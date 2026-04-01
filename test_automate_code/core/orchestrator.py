"""
orchestrator.py
---------------
Spins up N parallel Comet browser instances (via Playwright),
distributes POI rows across workers using a multiprocessing Queue,
writes results to a single CSV, and maintains a checkpoint file
so interrupted runs can resume from where they left off.
"""

import csv
import json
import logging
import multiprocessing
import os
import sys
import time
from pathlib import Path
from typing import List

# ── logging setup ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/run.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("orchestrator")

sys.path.insert(0, str(Path(__file__).parent.parent))

from cfg.settings import (
    INPUT_CSV, OUTPUT_CSV, DISTRICT_MAP, CATEGORY_MAP,
    PROGRESS_FILE, ERROR_LOG, BROWSER_INSTANCES,
)
from core.mapping_loader import load_district_mapping, load_category_mapping


# ──────────────────────────────────────────────────────────────
# Progress / checkpoint helpers
# ──────────────────────────────────────────────────────────────

def load_progress() -> set:
    """Return set of poi_codes already completed."""
    if not Path(PROGRESS_FILE).exists():
        return set()
    with open(PROGRESS_FILE, "r") as f:
        data = json.load(f)
    return set(data.get("completed", []))


def save_progress(completed: set):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"completed": list(completed)}, f)


# ──────────────────────────────────────────────────────────────
# CSV I/O
# ──────────────────────────────────────────────────────────────

def load_input_pois(csv_path: str, skip_codes: set) -> List[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("poi_code", "") not in skip_codes:
                rows.append(dict(row))
    logger.info(f"Loaded {len(rows)} POIs to process (skipped {len(skip_codes)} already done)")
    return rows


def get_output_fieldnames(sample_row: dict) -> List[str]:
    """Build ordered output column list."""
    original_fields = [
        "poi_code","name","address","district_code","country",
        "gmaps_url","latitude","longitude","latlong_used",
        "location_status","search_string","name_tags","ratings",
        "reviews_count","brands","brand_method","poi_type",
        "name_poi_types","category_poi_types","poi_type_cumulative",
        "final_poitype_distilbert","final_confidence_distilbert",
        "sequential_poi_type_source","google_category_tags",
        "area_tag","polygon","polygon_area_sqm","polygon_source",
        "parent_polygon_area_sqm","host_poi","tenant_pois","location_type",
    ]
    extracted_fields = [
        "extracted_name","extracted_address","extracted_country",
        "extracted_latitude","extracted_longitude",
        "extracted_location_status","extracted_final_location_status",
        "extracted_ratings","extracted_reviews_count",
        "extracted_google_category_tags",
        "extracted_district","resolved_district_code","resolved_poi_type",
        # comparison columns
        "name_match_pct","address_match_pct",
        "country_match",
        "distance_from_latlong_m",
        "location_status_match",
        "category_tags_subset",
        "extraction_status","extraction_timestamp","ratings_diff","reviews_count_diff",
    ]
    all_fields = original_fields + extracted_fields
    # Add any unexpected keys from sample_row
    for k in sample_row:
        if k not in all_fields:
            all_fields.append(k)
    return all_fields


# ──────────────────────────────────────────────────────────────
# Worker process entry point
# ──────────────────────────────────────────────────────────────

def worker_process(worker_id: int, task_queue, result_queue,
                   district_map: dict, category_map: dict):
    """
    Runs in a separate process.
    Pulls POI dicts from task_queue, processes them, pushes results to result_queue.
    Sentinel value: None → worker should exit.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return

    from core.browser_worker import POIBrowserWorker
    from cfg.settings import BROWSER_INSTANCES

    logger.info(f"[W{worker_id}] Starting browser")

    with sync_playwright() as pw:
        # ── Launch Comet/Chromium ─────────────────────────────
        # For Comet browser: replace executable_path with your Comet binary path
        # e.g. executable_path="/Applications/Comet.app/Contents/MacOS/Comet"
        browser = pw.chromium.launch(
            headless=True,
            # executable_path="/path/to/comet",   # ← set your Comet browser path here
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = context.new_page()

        poi_worker = POIBrowserWorker(
            page=page,
            config=None,          # config loaded internally via imports
            district_map=district_map,
            category_map=category_map,
            worker_id=worker_id,
        )

        while True:
            poi = task_queue.get()
            if poi is None:         # sentinel → shut down
                break
            result = poi_worker.process_poi(poi)
            result_queue.put(result)

        browser.close()
    logger.info(f"[W{worker_id}] Browser closed")


# ──────────────────────────────────────────────────────────────
# Main orchestration loop
# ──────────────────────────────────────────────────────────────

def run():
    Path("logs").mkdir(exist_ok=True)
    Path("output").mkdir(exist_ok=True)

    # Load mappings
    district_map  = load_district_mapping(DISTRICT_MAP)
    category_map  = load_category_mapping(CATEGORY_MAP)

    # Resume support
    completed_codes = load_progress()
    pois = load_input_pois(INPUT_CSV, skip_codes=completed_codes)

    if not pois:
        logger.info("No POIs to process. Exiting.")
        return

    # Determine output file mode
    output_exists = Path(OUTPUT_CSV).exists()
    fieldnames = get_output_fieldnames(pois[0])

    # ── Queues ────────────────────────────────────────────────
    task_queue   = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()

    # Enqueue all POIs
    for poi in pois:
        task_queue.put(poi)

    # Enqueue sentinels (one per worker)
    for _ in range(BROWSER_INSTANCES):
        task_queue.put(None)

    # ── Launch workers ────────────────────────────────────────
    workers = []
    for wid in range(BROWSER_INSTANCES):
        p = multiprocessing.Process(
            target=worker_process,
            args=(wid, task_queue, result_queue, district_map, category_map),
            daemon=True,
        )
        p.start()
        workers.append(p)
        logger.info(f"Started worker process {wid} (PID {p.pid})")

    # ── Collect results & write CSV ───────────────────────────
    total          = len(pois)
    processed      = 0
    success_count  = 0
    failed_count   = 0
    output_exists = Path(OUTPUT_CSV).exists()
    with open(OUTPUT_CSV, "a" if output_exists else "w", newline="", encoding="utf-8") as out_f, \
         open(ERROR_LOG,  "a", encoding="utf-8") as err_f:

        writer = csv.DictWriter(out_f, fieldnames=fieldnames, extrasaction="ignore")
        if not output_exists:
            writer.writeheader()

        while processed < total:
            result = result_queue.get(timeout=120)   # 2-min max wait per result
            writer.writerow(result)
            out_f.flush()

            status = result.get("extraction_status", "unknown")
            poi_code = result.get("poi_code", "?")

            if status == "success":
                success_count += 1
                completed_codes.add(poi_code)
            else:
                failed_count += 1
                err_f.write(f"{poi_code}\t{status}\t{result.get('gmaps_url','')}\n")

            processed += 1
            save_progress(completed_codes)

            pct = processed / total * 100
            logger.info(f"Progress: {processed}/{total} ({pct:.1f}%) | ✓{success_count} ✗{failed_count}")

    # ── Wait for workers to finish ────────────────────────────
    for p in workers:
        p.join(timeout=30)

    logger.info(f"\n{'='*60}")
    logger.info(f"Run complete: {success_count} success, {failed_count} failed")
    logger.info(f"Output → {OUTPUT_CSV}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    run()
