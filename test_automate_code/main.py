"""
main.py
-------
Entry point for the POI Extractor tool.

Usage:
    python main.py run          → start/resume extraction
    python main.py status       → show current progress
    python main.py reset        → clear checkpoint (start fresh)
    python main.py validate     → validate input CSV + mapping files
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("main")

import os
import csv
from cfg.settings import OUTPUT_CSV

COLUMNS = [
    'poi_code', 'name', 'address', 'district_code', 'country', 'gmaps_url',
    'latitude', 'longitude', 'latlong_used', 'location_status',
    'search_string', 'name_tags', 'ratings', 'reviews_count', 'brands',
    'brand_method', 'poi_type', 'name_poi_types', 'category_poi_types',
    'poi_type_cumulative', 'final_poitype_distilbert',
    'final_confidence_distilbert', 'sequential_poi_type_source',
    'google_category_tags', 'area_tag', 'polygon', 'polygon_area_sqm',
    'polygon_source', 'parent_polygon_area_sqm', 'host_poi', 'tenant_pois',
    'location_type', 'extracted_name', 'extracted_address',
    'extracted_country', 'extracted_latitude', 'extracted_longitude',
    'extracted_location_status', 'extracted_final_location_status',
    'extracted_ratings', 'extracted_reviews_count',
    'extracted_google_category_tags', 'extracted_district',
    'resolved_district_code', 'resolved_poi_type', 'name_match_pct',
    'address_match_pct', 'country_match', 'distance_from_latlong_m',
    'location_status_match', 'category_tags_subset', 'extraction_status',
    'extraction_timestamp', 'Unnamed: 53', 'Lat long', 'Polygon',
    'Area tag', 'Brands', 'Location status ', 'Rating Status',
    'Review status', 'Comment'
]

def ensure_output_csv():
    try:
        # Atomic file creation (only one thread/process succeeds)
        fd = os.open(OUTPUT_CSV, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)
        return True  # file was created

    except FileExistsError:
        return False  # already exists
    
def cmd_run():
    from core.orchestrator import run
    from cfg.settings import PROGRESS_FILE, INPUT_CSV, OUTPUT_CSV 
    #ensure_output_csv()
    run()


def cmd_status():
    from cfg.settings import PROGRESS_FILE, INPUT_CSV, OUTPUT_CSV

    print("\n── POI Extractor Status ──────────────────────────────")

    # Total input
    total = 0
    if Path(INPUT_CSV).exists():
        with open(INPUT_CSV, newline="", encoding="utf-8") as f:
            total = sum(1 for _ in csv.DictReader(f))
        print(f"  Input POIs      : {total:,}")
    else:
        print(f"  Input CSV       : NOT FOUND ({INPUT_CSV})")

    # Completed
    completed = 0
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
        completed = len(data.get("completed", []))
    remaining = total - completed
    pct = (completed / total * 100) if total else 0
    print(f"  Completed       : {completed:,} ({pct:.1f}%)")
    print(f"  Remaining       : {remaining:,}")

    # Output rows
    if Path(OUTPUT_CSV).exists():
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            out_rows = sum(1 for _ in csv.DictReader(f))
        print(f"  Output rows     : {out_rows:,}")
    else:
        print(f"  Output CSV      : not yet created")
    print()


def cmd_reset():
    from cfg.settings import PROGRESS_FILE
    if Path(PROGRESS_FILE).exists():
        os.remove(PROGRESS_FILE)
        print("Checkpoint cleared. Next run will start from scratch.")
    else:
        print("No checkpoint file found.")


def cmd_validate():
    from cfg.settings import INPUT_CSV, DISTRICT_MAP, CATEGORY_MAP

    ok = True
    print("\n── Validation ────────────────────────────────────────")

    for label, path in [("Input CSV", INPUT_CSV), ("District map", DISTRICT_MAP), ("Category map", CATEGORY_MAP)]:
        if Path(path).exists():
            print(f"  ✓  {label}: {path}")
        else:
            print(f"  ✗  {label} MISSING: {path}")
            ok = False

    # Check required columns in input CSV
    if Path(INPUT_CSV).exists():
        with open(INPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
        required = ["poi_code", "gmaps_url"]
        for col in required:
            if col in headers:
                print(f"  ✓  Column '{col}' present in input CSV")
            else:
                print(f"  ✗  Column '{col}' MISSING from input CSV")
                ok = False

    print(f"\n  {'All checks passed ✓' if ok else 'Fix the above issues before running.'}\n")


def main():
    parser = argparse.ArgumentParser(description="POI Extractor — Comet Browser Automation")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run",      help="Start or resume extraction")
    sub.add_parser("status",   help="Show progress summary")
    sub.add_parser("reset",    help="Clear checkpoint file")
    sub.add_parser("validate", help="Validate input files")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run()
    elif args.command == "status":
        cmd_status()
    elif args.command == "reset":
        cmd_reset()
    elif args.command == "validate":
        cmd_validate()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
