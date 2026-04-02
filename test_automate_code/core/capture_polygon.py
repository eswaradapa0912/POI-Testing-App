"""
capture_polygon.py
------------------
Fetch POI polygon data, open UI, click NEXT POLYGON,
and save screenshots using poi_code as filename.

Now includes:
✅ progress.json tracking
✅ resume capability
"""

import argparse
import time
import os
import json
from playwright.sync_api import sync_playwright

import pandas as pd
import trino
from trino.auth import BasicAuthentication

from cfg.settings import INPUT_CSV

# ---- Progress file ----
PROGRESS_FILE = "/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/progress.json"
import os
import json

# ---- Paths ----
SCREENSHOT_DIR = "/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system/output/screenshots_kepler/"
PROGRESS_FILE = "/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/progress.json"

def build_progress_from_images():
    if not os.path.exists(SCREENSHOT_DIR):
        print("❌ Screenshot directory not found")
        return

    files = os.listdir(SCREENSHOT_DIR)

    # ---- Extract POI codes ----
    poi_codes = [
        f.replace(".png", "")
        for f in files
        if f.endswith(".png")
    ]

    poi_codes = sorted(set(poi_codes))  # remove duplicates

    # ---- Save progress.json ----
    with open(PROGRESS_FILE, "w") as f:
        json.dump(poi_codes, f, indent=2)

    print(f"✅ Created progress.json with {len(poi_codes)} POIs")
    print(f"📁 Saved at: {PROGRESS_FILE}")

# ==============================
# Progress Helpers
# ==============================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_progress(progress_set):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(list(progress_set), f, indent=2)


# ==============================
# Fetch POI Data
# ==============================
def capture_polygon_data():
    output_path = "/mnt/data/POI_Testing_Automation/version=5_0_2/get-started-vite/src/output.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print("Reading input CSV...")
    df_input = pd.read_csv(INPUT_CSV)

    if "poi_code" not in df_input.columns:
        raise ValueError("CSV must contain 'poi_code' column")

    poi_codes = df_input["poi_code"].dropna().unique().tolist()

    if not poi_codes:
        print("⚠️ No POI codes found in CSV")
        return None

    poi_codes_str = ",".join([f"'{code}'" for code in poi_codes])

    query = f"""
    SELECT
        poi_code,
        polygon,
        latitude,
        longitude,
        name,
        address
    FROM poi_data_5_0_10
    WHERE poi_code IN ({poi_codes_str})
    """

    print("Connecting to Trino...")
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

    print("Executing query...")
    cursor.execute(query)

    print("Fetching results...")
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=column_names)

    cursor.close()
    conn.close()

    if df.empty:
        print("⚠️ No matching data found")
        return None

    df = df.sort_values("poi_code")

    print("Converting to JSON...")
    json_data = df.to_dict(orient="records")

    print("Saving JSON...")
    with open(output_path, "w") as f:
        json.dump(json_data, f, indent=2)

    print(f"✅ Saved {len(json_data)} records to {output_path}")

    return json_data


# ==============================
# Screenshot Automation
# ==============================
def capture_polygon(url: str, poi_data: list, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    progress = load_progress()
    print(f"📌 Already processed: {len(progress)} POIs")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})

        print(f"→ Navigating to {url}")
        page.goto(url, wait_until="networkidle", timeout=30_000)

        time.sleep(5)

        btn_selector = 'button:has-text("NEXT POLYGON")'
        page.wait_for_selector(btn_selector, timeout=15_000)
        print("✓ NEXT POLYGON button found")

        total = len(poi_data)
        processed_count = 0

        for i, item in enumerate(poi_data):
            poi_code = item["poi_code"]

            # ---- Skip if already processed ----
            if poi_code in progress:
                print(f"[SKIP] {poi_code}")
                page.click(btn_selector)
                time.sleep(0.5)
                continue

            print(f"[{i+1}/{total}] Processing {poi_code}")

            filename = os.path.join(output_dir, f"{poi_code}.png")

            page.screenshot(path=filename, full_page=False)
            print(f"✓ Saved → {filename}")

            # ---- Update progress ----
            progress.add(poi_code)
            save_progress(progress)

            processed_count += 1

            # Move to next polygon
            page.click(btn_selector)
            time.sleep(1)

        browser.close()

    print(f"\n🎯 Done. Newly processed: {processed_count}")
    print(f"📊 Total completed: {len(progress)}")


# ==============================
# Main
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Automate polygon screenshots")

    parser.add_argument("--url", default="http://localhost:8081/")
    parser.add_argument(
        "--output",
        default="/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system/output/screenshots_kepler/",
    )
    
    build_progress_from_images()
    args = parser.parse_args()

    poi_data = capture_polygon_data()

    if not poi_data:
        print("❌ No data to process. Exiting.")
        return

    time.sleep(2)

    capture_polygon(args.url, poi_data, args.output)


if __name__ == "__main__":
    
    main()