"""
capture_polygon.py
------------------
Fetch POI polygon data, open UI, click NEXT POLYGON,
and save screenshots using poi_code as filename.

Requirements:
    pip install playwright pandas trino
    playwright install chromium
"""

import argparse
import time
import os
from playwright.sync_api import sync_playwright

import pandas as pd
import json
import trino
from trino.auth import BasicAuthentication

# Import INPUT_CSV from settings
from cfg.settings import INPUT_CSV


def capture_polygon_data():
    # ---- Paths ----
    output_path = "/mnt/data/POI_Testing_Automation/version=5_0_2/get-started-vite/src/output.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # ---- Step 1: Read CSV ----
    print("Reading input CSV...")
    df_input = pd.read_csv(INPUT_CSV)

    if "poi_code" not in df_input.columns:
        raise ValueError("CSV must contain 'poi_code' column")

    poi_codes = df_input["poi_code"].dropna().unique().tolist()

    if not poi_codes:
        print("⚠️ No POI codes found in CSV")
        return None

    # ---- Step 2: Create SQL IN clause ----
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

    # ---- Step 3: Connect to Trino ----
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

    # ---- Step 4: Execute Query ----
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

    # ---- Optional: Maintain consistent order ----
    df = df.sort_values("poi_code")

    # ---- Step 5: Convert to JSON ----
    print("Converting to JSON...")
    json_data = df.to_dict(orient="records")

    # ---- Step 6: Save JSON ----
    print("Saving JSON...")
    with open(output_path, "w") as f:
        json.dump(json_data, f, indent=2)

    print(f"✅ Saved {len(json_data)} records to {output_path}")

    return json_data


def capture_polygon(url: str, poi_data: list, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    poi_codes = [item["poi_code"] for item in poi_data]

    if not poi_codes:
        print("⚠️ No POI codes to process")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})

        print(f"→ Navigating to {url}")
        page.goto(url, wait_until="networkidle", timeout=30_000)

        time.sleep(5)

        # ---- Wait for button ----
        btn_selector = 'button:has-text("NEXT POLYGON")'
        page.wait_for_selector(btn_selector, timeout=15_000)
        print("✓ NEXT POLYGON button found")

        # ---- First screenshot ----
        first_code = poi_codes[0]
        filename = os.path.join(output_dir, f"{first_code}.png")
        page.screenshot(path=filename, full_page=False)
        print(f"✓ Saved → {filename}")

        # ---- Loop through remaining POIs ----
        for i in range(1, len(poi_codes)):
            print(f"[{i}/{len(poi_codes)-1}] Clicking NEXT POLYGON...")
            page.click(btn_selector)

            time.sleep(1)

            poi_code = poi_codes[i]
            filename = os.path.join(output_dir, f"{poi_code}.png")

            page.screenshot(path=filename, full_page=False)
            print(f"✓ Saved → {filename}")

        browser.close()

    print(f"\n🎯 Done. {len(poi_codes)} screenshots saved in {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Automate polygon screenshots")
    parser.add_argument("--url", default="http://localhost:8081/", help="Target URL")
    parser.add_argument(
        "--output",
        default="/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system/output/screenshots_kepler/",
        help="Output folder for screenshots"
    )

    args = parser.parse_args()

    # ---- Step 1: Fetch POI data ----
    poi_data = capture_polygon_data()

    if not poi_data:
        print("❌ No data to process. Exiting.")
        return

    time.sleep(2)

    # ---- Step 2: Capture screenshots ----
    capture_polygon(args.url, poi_data, args.output)


if __name__ == "__main__":
    main()