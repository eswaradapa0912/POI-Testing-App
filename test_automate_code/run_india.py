#!/usr/bin/env python3
"""
run_india.py
============
Single script to process India POI data end-to-end:
  1. Extract data from Google Maps (screenshots + matching metrics)
  2. Capture Kepler polygon screenshots
  3. Append results to SQLite (preserves existing USA data)

Usage:
    cd /mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code
    python run_india.py                  # Run all 3 steps
    python run_india.py --step extract   # Only Google Maps extraction
    python run_india.py --step polygon   # Only Kepler polygon screenshots
    python run_india.py --step sqlite    # Only import to SQLite

This script does NOT modify settings.py, csv_to_sqlite.py, or any existing
USA data. All India outputs go to separate directories.
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths — India-specific (no overlap with USA)
# ---------------------------------------------------------------------------
BASE_DIR = Path("/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code")
VALIDATION_DIR = BASE_DIR / "poi_validation_system"

# Input
INDIA_INPUT_CSV = str(VALIDATION_DIR / "input" / "India_sample (1).csv")

# Outputs — country-specific subdirectories
INDIA_OUTPUT_DIR = VALIDATION_DIR / "output" / "IND"
INDIA_OUTPUT_CSV = str(INDIA_OUTPUT_DIR / "poi_extracted.csv")
INDIA_SCREENSHOTS_DISPLAY = str(INDIA_OUTPUT_DIR / "screenshots_with_display")
INDIA_SCREENSHOTS_NO_DISPLAY = str(INDIA_OUTPUT_DIR / "screenshots_without_display")
INDIA_SCREENSHOTS_KEPLER = str(INDIA_OUTPUT_DIR / "screenshots_kepler")

# Progress — separate from USA
INDIA_PROGRESS_FILE = str(BASE_DIR / "logs" / "progress_india.json")

# Shared resources (read-only for this script)
DISTRICT_MAP = str(BASE_DIR / "input" / "Districts.csv")
DB_PATH = str(VALIDATION_DIR / "poi_data.db")

# Kepler
KEPLER_OUTPUT_JSON = str(BASE_DIR.parent / "get-started-vite" / "src" / "output.json")
KEPLER_URL = "http://localhost:8081/"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(BASE_DIR / "logs" / "run_india.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger("run_india")


# ===========================================================================
# STEP 1: Google Maps Extraction (equivalent to main.py run)
# ===========================================================================
def step_extract():
    """
    Monkey-patch cfg.settings so the orchestrator + browser_worker use
    India paths, then run the extraction. Original settings.py is unchanged.
    """
    logger.info("=" * 60)
    logger.info("STEP 1: Google Maps Extraction for India")
    logger.info("=" * 60)

    # Create output directories
    os.makedirs(INDIA_SCREENSHOTS_DISPLAY, exist_ok=True)
    os.makedirs(INDIA_SCREENSHOTS_NO_DISPLAY, exist_ok=True)
    os.makedirs(str(INDIA_OUTPUT_DIR), exist_ok=True)
    os.makedirs(str(BASE_DIR / "logs"), exist_ok=True)

    # ---- Monkey-patch cfg.settings BEFORE importing orchestrator ----
    sys.path.insert(0, str(BASE_DIR))
    import cfg.settings as settings

    # Save original values so we can restore if needed
    _orig_input = settings.INPUT_CSV
    _orig_output = settings.OUTPUT_CSV
    _orig_progress = settings.PROGRESS_FILE

    # Override with India paths
    settings.INPUT_CSV = INDIA_INPUT_CSV
    settings.OUTPUT_CSV = INDIA_OUTPUT_CSV
    settings.PROGRESS_FILE = INDIA_PROGRESS_FILE

    # ---- Monkey-patch browser_worker screenshot paths ----
    import core.browser_worker as bw

    _orig_navigate = bw.POIBrowserWorker._navigate_and_extract

    def _patched_navigate(self, url, poi_code):
        """Wrapper that redirects screenshots to India directories."""
        # Temporarily replace the hardcoded base path inside the method
        # We do this by patching os.makedirs and screenshot paths
        from cfg.settings import PAGE_LOAD_TIMEOUT, EXTRACTION_WAIT, SELECTORS

        # ── Navigation ──
        self.page.goto(url, timeout=PAGE_LOAD_TIMEOUT * 1000)
        self.page.wait_for_selector(SELECTORS["name"], timeout=PAGE_LOAD_TIMEOUT * 1000)
        self.page.reload()
        time.sleep(EXTRACTION_WAIT)

        # ── Info panel ──
        raw_info = self.extract_info()
        cleaned = self.transform_data2(raw_info)

        # ── Screenshots — INDIA paths ──
        os.makedirs(INDIA_SCREENSHOTS_NO_DISPLAY, exist_ok=True)
        os.makedirs(INDIA_SCREENSHOTS_DISPLAY, exist_ok=True)
        shot_no_panel = os.path.join(INDIA_SCREENSHOTS_NO_DISPLAY, f"{poi_code}.png")
        shot_panel = os.path.join(INDIA_SCREENSHOTS_DISPLAY, f"{poi_code}.png")

        self.page.screenshot(path=shot_no_panel, full_page=True)

        container = self.page.locator('div.gYkzb')
        container.locator('button[aria-label="Collapse side panel"]').click()
        time.sleep(EXTRACTION_WAIT)
        self.page.screenshot(path=shot_panel, full_page=True)

        # ── Name ──
        name = self._text(SELECTORS["name"])

        # ── Address ──
        address = self._text(SELECTORS["address"])

        # ── Rating ──
        rating = bw.clean_rating(self._text(SELECTORS["rating"]))

        # ── Reviews count ──
        reviews_raw = self._attr(SELECTORS["reviews_count"], "aria-label")
        if not reviews_raw:
            reviews_raw = self._text(SELECTORS["reviews_count"])
        reviews_count = bw.clean_reviews_count(reviews_raw)

        # ── Category tags ──
        category_elements = self.page.query_selector_all(SELECTORS["category_tags"])
        category_tags = [
            el.inner_text().strip()
            for el in category_elements
            if el.inner_text().strip()
        ]

        # ── Open / closed status ──
        import re
        status_raw = self._text(SELECTORS["open_status"]).strip()
        status_lower = status_raw.lower()

        if status_lower and any(kw in status_lower for kw in bw.CLOSED_KEYWORDS):
            extracted_final_location_status = status_raw
        else:
            extracted_final_location_status = "open"

        # ── Coordinates ──
        lat, lng = bw.parse_coords_from_url(self.page.url)

        # ── Country ──
        country = ""
        if address:
            parts = [p.strip() for p in address.split(",")]
            country = parts[-1] if parts else ""

        return {
            "extracted_name": name,
            "extracted_address": address,
            "extracted_country": country,
            "extracted_latitude": lat,
            "extracted_longitude": lng,
            "extracted_location_status": status_raw,
            "extracted_final_location_status": extracted_final_location_status,
            "extracted_ratings": rating,
            "extracted_reviews_count": reviews_count,
            "extracted_google_category_tags": "|".join(category_tags),
            "_category_tags_list": category_tags,
            **cleaned,
        }

    # Apply the monkey-patch
    bw.POIBrowserWorker._navigate_and_extract = _patched_navigate

    try:
        # ---- Run the orchestrator ----
        from core.orchestrator import run
        logger.info(f"Input CSV : {INDIA_INPUT_CSV}")
        logger.info(f"Output CSV: {INDIA_OUTPUT_CSV}")
        logger.info(f"Progress  : {INDIA_PROGRESS_FILE}")
        logger.info(f"Screenshots: {INDIA_SCREENSHOTS_DISPLAY}")
        run()
    finally:
        # ---- Restore original settings ----
        settings.INPUT_CSV = _orig_input
        settings.OUTPUT_CSV = _orig_output
        settings.PROGRESS_FILE = _orig_progress
        bw.POIBrowserWorker._navigate_and_extract = _orig_navigate
        logger.info("Restored original settings.py values")

    logger.info("STEP 1 COMPLETE: Extraction finished")


# ===========================================================================
# STEP 2: Kepler Polygon Screenshots (equivalent to capture_polygon.py)
# ===========================================================================
def step_polygon():
    """
    Fetch polygon data for India POIs from Trino, save to output.json,
    capture Kepler screenshots, then restore original output.json.
    """
    logger.info("=" * 60)
    logger.info("STEP 2: Kepler Polygon Screenshots for India")
    logger.info("=" * 60)

    os.makedirs(INDIA_SCREENSHOTS_KEPLER, exist_ok=True)

    # ---- Backup existing output.json (USA data) ----
    backup_path = KEPLER_OUTPUT_JSON + ".usa_backup"
    if os.path.exists(KEPLER_OUTPUT_JSON):
        import shutil
        shutil.copy2(KEPLER_OUTPUT_JSON, backup_path)
        logger.info(f"Backed up output.json → {backup_path}")

    try:
        # ---- Fetch India polygon data from Trino ----
        import pandas as pd
        import trino
        from trino.auth import BasicAuthentication

        logger.info("Reading India input CSV...")
        df_input = pd.read_csv(INDIA_INPUT_CSV)

        if "poi_code" not in df_input.columns:
            raise ValueError("CSV must contain 'poi_code' column")

        poi_codes = df_input["poi_code"].dropna().unique().tolist()
        if not poi_codes:
            logger.warning("No POI codes found in India CSV")
            return

        poi_codes_str = ",".join([f"'{code}'" for code in poi_codes])

        query = f"""
        SELECT poi_code, polygon, latitude, longitude, name, address
        FROM poi_data_5_0_10
        WHERE poi_code IN ({poi_codes_str})
        """

        logger.info("Connecting to Trino...")
        conn = trino.dbapi.connect(
            host='prestoazure.infiniteanalytics.com',
            port=443,
            user='application',
            catalog='delta',
            schema='default',
            auth=BasicAuthentication('application', 'knob#or53RainEin5teinTom168'),
            http_scheme='https',
            client_tags=['test']
        )

        cursor = conn.cursor()
        logger.info("Executing Trino query...")
        cursor.execute(query)

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        cursor.close()
        conn.close()

        if df.empty:
            logger.warning("No polygon data found for India POIs")
            return

        df = df.sort_values("poi_code")
        json_data = df.to_dict(orient="records")

        # ---- Write India polygon data to output.json ----
        os.makedirs(os.path.dirname(KEPLER_OUTPUT_JSON), exist_ok=True)
        with open(KEPLER_OUTPUT_JSON, "w") as f:
            json.dump(json_data, f, indent=2)
        logger.info(f"Saved {len(json_data)} India polygon records to output.json")

        time.sleep(2)

        # ---- Capture Kepler screenshots ----
        _capture_kepler_screenshots(json_data)

    finally:
        # ---- Restore original output.json (USA data) ----
        if os.path.exists(backup_path):
            import shutil
            shutil.copy2(backup_path, KEPLER_OUTPUT_JSON)
            os.remove(backup_path)
            logger.info("Restored original output.json (USA data)")

    logger.info("STEP 2 COMPLETE: Kepler screenshots captured")


def _capture_kepler_screenshots(poi_data: list):
    """Use Playwright to navigate Kepler UI and capture screenshots."""
    from playwright.sync_api import sync_playwright

    # ---- Load India-specific progress ----
    kepler_progress_file = str(BASE_DIR / "progress_kepler_india.json")
    progress = set()
    if os.path.exists(kepler_progress_file):
        with open(kepler_progress_file, "r") as f:
            progress = set(json.load(f))

    logger.info(f"Already captured: {len(progress)} Kepler screenshots")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})

        logger.info(f"Navigating to {KEPLER_URL}")
        page.goto(KEPLER_URL, wait_until="networkidle", timeout=30_000)
        time.sleep(5)

        btn_selector = 'button:has-text("NEXT POLYGON")'
        page.wait_for_selector(btn_selector, timeout=15_000)
        logger.info("NEXT POLYGON button found")

        total = len(poi_data)
        processed_count = 0

        for i, item in enumerate(poi_data):
            poi_code = item["poi_code"]

            if poi_code in progress:
                logger.info(f"[SKIP] {poi_code}")
                page.click(btn_selector)
                time.sleep(0.5)
                continue

            logger.info(f"[{i + 1}/{total}] Capturing {poi_code}")
            filename = os.path.join(INDIA_SCREENSHOTS_KEPLER, f"{poi_code}.png")
            page.screenshot(path=filename, full_page=False)

            progress.add(poi_code)
            with open(kepler_progress_file, "w") as f:
                json.dump(list(progress), f, indent=2)

            processed_count += 1
            page.click(btn_selector)
            time.sleep(1)

        browser.close()

    logger.info(f"Newly captured: {processed_count}, Total: {len(progress)}")


# ===========================================================================
# STEP 3: Append India data to SQLite (preserves existing USA data)
# ===========================================================================
def step_sqlite():
    """
    Append India POI input + metrics to the shared SQLite database.
    Does NOT delete existing USA rows — uses INSERT with country-aware logic.
    """
    logger.info("=" * 60)
    logger.info("STEP 3: Import India data to SQLite (APPEND mode)")
    logger.info("=" * 60)

    import pandas as pd

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ---- Ensure tables exist (same schema as csv_to_sqlite.py) ----
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS poi_input (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poi_code TEXT NOT NULL,
            name TEXT, address TEXT, district_code TEXT, country TEXT,
            gmaps_url TEXT, latitude REAL, longitude REAL, latlong_used TEXT,
            location_status TEXT, search_string TEXT, name_tags REAL,
            ratings REAL, reviews_count REAL, brands TEXT, brand_method TEXT,
            poi_type TEXT, name_poi_types TEXT, category_poi_types TEXT,
            poi_type_cumulative TEXT, final_poitype_distilbert TEXT,
            final_confidence_distilbert REAL, sequential_poi_type_source TEXT,
            google_category_tags TEXT, area_tag TEXT, polygon TEXT,
            polygon_area_sqm REAL, polygon_source TEXT,
            parent_polygon_area_sqm REAL, host_poi TEXT, tenant_pois TEXT,
            location_type TEXT, website_domain_name TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS poi_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poi_code TEXT NOT NULL,
            extracted_name TEXT, extracted_address TEXT, extracted_country TEXT,
            extracted_latitude REAL, extracted_longitude REAL,
            extracted_location_status TEXT, extracted_final_location_status TEXT,
            extracted_ratings REAL, extracted_reviews_count REAL,
            extracted_google_category_tags TEXT, extracted_district REAL,
            resolved_district_code REAL, resolved_poi_type REAL,
            name_match_pct REAL, address_match_pct REAL, country_match TEXT,
            distance_from_latlong_m REAL, location_status_match TEXT,
            category_tags_subset TEXT, extraction_status TEXT,
            extraction_timestamp TEXT, ratings_diff REAL, reviews_count_diff REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS validations (
            poi_code TEXT PRIMARY KEY,
            poi_type_validation TEXT DEFAULT '',
            correct_poi_type TEXT DEFAULT '',
            brand_validation TEXT DEFAULT '',
            polygon_area_validation TEXT DEFAULT '',
            polygon_validation TEXT DEFAULT '',
            comments TEXT DEFAULT '',
            timestamp TEXT DEFAULT '',
            validator_name TEXT DEFAULT ''
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_input_code ON poi_input(poi_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_input_country ON poi_input(country)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_input_type ON poi_input(poi_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_metrics_code ON poi_metrics(poi_code)")
    conn.commit()

    # ---- STEP 3a: Import India input CSV ----
    logger.info(f"Reading India input CSV: {INDIA_INPUT_CSV}")
    df_input = pd.read_csv(INDIA_INPUT_CSV)
    logger.info(f"  Found {len(df_input)} rows")

    input_columns = [
        'poi_code', 'name', 'address', 'district_code', 'country',
        'gmaps_url', 'latitude', 'longitude', 'latlong_used', 'location_status',
        'search_string', 'name_tags', 'ratings', 'reviews_count',
        'brands', 'brand_method', 'poi_type', 'name_poi_types',
        'category_poi_types', 'poi_type_cumulative', 'final_poitype_distilbert',
        'final_confidence_distilbert', 'sequential_poi_type_source',
        'google_category_tags', 'area_tag', 'polygon', 'polygon_area_sqm',
        'polygon_source', 'parent_polygon_area_sqm', 'host_poi',
        'tenant_pois', 'location_type', 'website_domain_name'
    ]

    available_input = [c for c in input_columns if c in df_input.columns]
    df_insert = df_input[available_input].copy()
    df_insert = df_insert.where(pd.notna(df_insert), None)

    # Delete only India rows (if re-running), preserve USA
    india_poi_codes = df_input['poi_code'].tolist()
    placeholders = ','.join(['?'] * len(india_poi_codes))
    cursor.execute(f"DELETE FROM poi_input WHERE poi_code IN ({placeholders})", india_poi_codes)
    logger.info(f"  Cleared {cursor.rowcount} existing India rows from poi_input")

    # Insert India rows
    col_names = ', '.join(available_input)
    ph = ', '.join(['?'] * len(available_input))
    sql = f"INSERT INTO poi_input ({col_names}) VALUES ({ph})"
    rows = [tuple(row[c] for c in available_input) for _, row in df_insert.iterrows()]
    cursor.executemany(sql, rows)
    conn.commit()
    logger.info(f"  Inserted {len(rows)} India rows into poi_input")

    # ---- STEP 3b: Import India extracted metrics ----
    if os.path.exists(INDIA_OUTPUT_CSV):
        logger.info(f"Reading India output CSV: {INDIA_OUTPUT_CSV}")
        df_output = pd.read_csv(INDIA_OUTPUT_CSV)
        logger.info(f"  Found {len(df_output)} rows")

        # Deduplicate: keep best row per poi_code
        best_rows = []
        for poi_code in df_output['poi_code'].unique():
            poi_rows = df_output[df_output['poi_code'] == poi_code]
            good_rows = poi_rows.dropna(subset=['name_match_pct', 'address_match_pct'])
            if not good_rows.empty:
                best_rows.append(good_rows.iloc[0])
            else:
                best_rows.append(poi_rows.iloc[0])
        df_dedup = pd.DataFrame(best_rows)
        logger.info(f"  Deduplicated: {len(df_output)} → {len(df_dedup)} unique POIs")

        metrics_columns = [
            'poi_code', 'extracted_name', 'extracted_address', 'extracted_country',
            'extracted_latitude', 'extracted_longitude', 'extracted_location_status',
            'extracted_final_location_status', 'extracted_ratings', 'extracted_reviews_count',
            'extracted_google_category_tags', 'extracted_district', 'resolved_district_code',
            'resolved_poi_type', 'name_match_pct', 'address_match_pct', 'country_match',
            'distance_from_latlong_m', 'location_status_match', 'category_tags_subset',
            'extraction_status', 'extraction_timestamp', 'ratings_diff', 'reviews_count_diff'
        ]

        available_metrics = [c for c in metrics_columns if c in df_dedup.columns]
        df_met = df_dedup[available_metrics].copy()
        df_met = df_met.where(pd.notna(df_met), None)

        # Delete only India metrics (if re-running)
        india_metric_codes = df_dedup['poi_code'].tolist()
        ph2 = ','.join(['?'] * len(india_metric_codes))
        cursor.execute(f"DELETE FROM poi_metrics WHERE poi_code IN ({ph2})", india_metric_codes)
        logger.info(f"  Cleared {cursor.rowcount} existing India rows from poi_metrics")

        col_names_m = ', '.join(available_metrics)
        ph_m = ', '.join(['?'] * len(available_metrics))
        sql_m = f"INSERT INTO poi_metrics ({col_names_m}) VALUES ({ph_m})"
        rows_m = [tuple(row[c] for c in available_metrics) for _, row in df_met.iterrows()]
        cursor.executemany(sql_m, rows_m)
        conn.commit()
        logger.info(f"  Inserted {len(rows_m)} India rows into poi_metrics")
    else:
        logger.warning(f"  India output CSV not found: {INDIA_OUTPUT_CSV}")
        logger.warning("  Run --step extract first, then re-run --step sqlite")

    # ---- Summary ----
    cursor.execute("SELECT COUNT(*) FROM poi_input")
    total_input = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM poi_input WHERE country = 'IND'")
    india_input = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM poi_input WHERE country = 'USA'")
    usa_input = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM poi_metrics")
    total_metrics = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM validations")
    total_vals = cursor.fetchone()[0]

    conn.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("SQLite Database Summary")
    logger.info("=" * 60)
    logger.info(f"  poi_input total : {total_input} rows")
    logger.info(f"    USA           : {usa_input} rows")
    logger.info(f"    IND           : {india_input} rows")
    logger.info(f"  poi_metrics     : {total_metrics} rows")
    logger.info(f"  validations     : {total_vals} rows (unchanged)")
    logger.info("=" * 60)


# ===========================================================================
# Main
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Process India POI data: extract → polygon → SQLite"
    )
    parser.add_argument(
        "--step",
        choices=["extract", "polygon", "sqlite", "all"],
        default="all",
        help="Which step to run (default: all)",
    )
    args = parser.parse_args()

    os.chdir(str(BASE_DIR))
    os.makedirs(str(BASE_DIR / "logs"), exist_ok=True)

    logger.info("=" * 60)
    logger.info("India POI Processing Pipeline")
    logger.info(f"Input  : {INDIA_INPUT_CSV}")
    logger.info(f"Output : {INDIA_OUTPUT_DIR}")
    logger.info(f"DB     : {DB_PATH}")
    logger.info("=" * 60)

    if args.step in ("extract", "all"):
        step_extract()

    if args.step in ("polygon", "all"):
        step_polygon()

    if args.step in ("sqlite", "all"):
        step_sqlite()

    logger.info("")
    logger.info("PIPELINE COMPLETE")


if __name__ == "__main__":
    main()
