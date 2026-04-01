#!/usr/bin/env python3
"""
CSV to SQLite Converter
-----------------------
Converts input CSV and output CSV files into a SQLite database.
Also migrates existing validations from JSON into the database.

Usage:
    python3 csv_to_sqlite.py
    python3 csv_to_sqlite.py --input path/to/input.csv --output path/to/output.csv
    python3 csv_to_sqlite.py --input path/to/input.csv  (output CSV is optional)
"""

import argparse
import sqlite3
import pandas as pd
import json
import os
from pathlib import Path

BASE_DIR = Path("/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system")
DEFAULT_INPUT_CSV = BASE_DIR / "input" / "usa_sample - usa_sample.csv"
DEFAULT_OUTPUT_CSV = BASE_DIR / "output" / "poi_extracted.csv"
DEFAULT_DB_PATH = BASE_DIR / "poi_data.db"
DEFAULT_VALIDATIONS_JSON = BASE_DIR / "validations.json"


def create_database(db_path):
    """Create the SQLite database with the required tables"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Table for input POI data (all columns from input CSV)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS poi_input (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poi_code TEXT NOT NULL,
            name TEXT,
            address TEXT,
            district_code INTEGER,
            country TEXT,
            gmaps_url TEXT,
            latitude REAL,
            longitude REAL,
            latlong_used TEXT,
            location_status TEXT,
            search_string TEXT,
            name_tags REAL,
            ratings REAL,
            reviews_count REAL,
            brands TEXT,
            brand_method TEXT,
            poi_type TEXT,
            name_poi_types TEXT,
            category_poi_types TEXT,
            poi_type_cumulative TEXT,
            final_poitype_distilbert TEXT,
            final_confidence_distilbert REAL,
            sequential_poi_type_source TEXT,
            google_category_tags TEXT,
            area_tag TEXT,
            polygon TEXT,
            polygon_area_sqm REAL,
            polygon_source TEXT,
            parent_polygon_area_sqm REAL,
            host_poi TEXT,
            tenant_pois TEXT,
            location_type TEXT,
            website_domain_name TEXT
        )
    """)

    # Table for output/extracted metrics (extra columns from output CSV)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS poi_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poi_code TEXT NOT NULL,
            extracted_name TEXT,
            extracted_address TEXT,
            extracted_country TEXT,
            extracted_latitude REAL,
            extracted_longitude REAL,
            extracted_location_status TEXT,
            extracted_final_location_status TEXT,
            extracted_ratings REAL,
            extracted_reviews_count REAL,
            extracted_google_category_tags TEXT,
            extracted_district REAL,
            resolved_district_code REAL,
            resolved_poi_type REAL,
            name_match_pct REAL,
            address_match_pct REAL,
            country_match TEXT,
            distance_from_latlong_m REAL,
            location_status_match TEXT,
            category_tags_subset TEXT,
            extraction_status TEXT,
            extraction_timestamp TEXT,
            ratings_diff REAL,
            reviews_count_diff REAL
        )
    """)

    # Table for validations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS validations (
            poi_code TEXT PRIMARY KEY,
            poi_type_validation TEXT DEFAULT '',
            correct_poi_type TEXT DEFAULT '',
            brand_validation TEXT DEFAULT '',
            polygon_area_validation TEXT DEFAULT '',
            polygon_validation TEXT DEFAULT '',
            comments TEXT DEFAULT '',
            timestamp TEXT DEFAULT ''
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_input_code ON poi_input(poi_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_input_country ON poi_input(country)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_input_type ON poi_input(poi_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_poi_metrics_code ON poi_metrics(poi_code)")

    conn.commit()
    return conn


def import_input_csv(conn, csv_path):
    """Import input CSV into poi_input table"""
    print(f"Reading input CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"  Found {len(df)} rows, {len(df.columns)} columns")

    # Map CSV columns to table columns
    table_columns = [
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

    # Only use columns that exist in both CSV and table definition
    available = [c for c in table_columns if c in df.columns]
    df_insert = df[available].copy()

    # Fill NaN with None for proper SQL NULL
    df_insert = df_insert.where(pd.notna(df_insert), None)

    # Clear existing data
    conn.execute("DELETE FROM poi_input")

    # Insert rows
    placeholders = ', '.join(['?'] * len(available))
    col_names = ', '.join(available)
    sql = f"INSERT INTO poi_input ({col_names}) VALUES ({placeholders})"

    rows = [tuple(row[c] for c in available) for _, row in df_insert.iterrows()]
    conn.executemany(sql, rows)
    conn.commit()

    print(f"  Imported {len(rows)} rows into poi_input")
    return len(rows)


def import_output_csv(conn, csv_path):
    """Import output CSV into poi_metrics table (deduplicated - best row per poi_code)"""
    print(f"Reading output CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"  Found {len(df)} rows, {len(df.columns)} columns")

    # Deduplicate: for each poi_code, prefer rows with non-null matching metrics
    best_rows = []
    for poi_code in df['poi_code'].unique():
        poi_rows = df[df['poi_code'] == poi_code]
        good_rows = poi_rows.dropna(subset=['name_match_pct', 'address_match_pct'])
        if not good_rows.empty:
            best_rows.append(good_rows.iloc[0])
        else:
            best_rows.append(poi_rows.iloc[0])

    df_dedup = pd.DataFrame(best_rows)
    print(f"  Deduplicated: {len(df)} rows -> {len(df_dedup)} unique POIs")

    # Metrics-specific columns
    metrics_columns = [
        'poi_code', 'extracted_name', 'extracted_address', 'extracted_country',
        'extracted_latitude', 'extracted_longitude', 'extracted_location_status',
        'extracted_final_location_status', 'extracted_ratings', 'extracted_reviews_count',
        'extracted_google_category_tags', 'extracted_district', 'resolved_district_code',
        'resolved_poi_type', 'name_match_pct', 'address_match_pct', 'country_match',
        'distance_from_latlong_m', 'location_status_match', 'category_tags_subset',
        'extraction_status', 'extraction_timestamp', 'ratings_diff', 'reviews_count_diff'
    ]

    available = [c for c in metrics_columns if c in df_dedup.columns]
    df_insert = df_dedup[available].copy()
    df_insert = df_insert.where(pd.notna(df_insert), None)

    # Clear existing data
    conn.execute("DELETE FROM poi_metrics")

    placeholders = ', '.join(['?'] * len(available))
    col_names = ', '.join(available)
    sql = f"INSERT INTO poi_metrics ({col_names}) VALUES ({placeholders})"

    rows = [tuple(row[c] for c in available) for _, row in df_insert.iterrows()]
    conn.executemany(sql, rows)
    conn.commit()

    print(f"  Imported {len(rows)} rows into poi_metrics")
    return len(rows)


def import_validations_json(conn, json_path):
    """Import existing validations from JSON file"""
    if not os.path.exists(json_path):
        print(f"  No validations file found at {json_path}, skipping")
        return 0

    print(f"Reading validations JSON: {json_path}")
    with open(json_path, 'r') as f:
        validations = json.load(f)

    print(f"  Found {len(validations)} validations")

    for poi_code, v in validations.items():
        conn.execute("""
            INSERT OR REPLACE INTO validations
            (poi_code, poi_type_validation, correct_poi_type, brand_validation,
             polygon_area_validation, polygon_validation, comments, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            poi_code,
            v.get('poi_type_validation', ''),
            v.get('correct_poi_type', ''),
            v.get('brand_validation', ''),
            v.get('polygon_area_validation', ''),
            v.get('polygon_validation', ''),
            v.get('comments', ''),
            v.get('timestamp', '')
        ))

    conn.commit()
    print(f"  Imported {len(validations)} validations")
    return len(validations)


def main():
    parser = argparse.ArgumentParser(description="Convert CSV files to SQLite database")
    parser.add_argument('--input', default=str(DEFAULT_INPUT_CSV),
                        help='Path to input CSV file')
    parser.add_argument('--output', default=str(DEFAULT_OUTPUT_CSV),
                        help='Path to output/metrics CSV file (optional)')
    parser.add_argument('--db', default=str(DEFAULT_DB_PATH),
                        help='Path for SQLite database file')
    parser.add_argument('--validations', default=str(DEFAULT_VALIDATIONS_JSON),
                        help='Path to validations JSON file (optional)')
    parser.add_argument('--no-validations', action='store_true',
                        help='Skip importing validations')

    args = parser.parse_args()

    print("=" * 60)
    print("CSV to SQLite Converter")
    print("=" * 60)

    # Create database
    print(f"\nCreating database: {args.db}")
    conn = create_database(args.db)

    # Import input CSV
    if os.path.exists(args.input):
        import_input_csv(conn, args.input)
    else:
        print(f"  WARNING: Input CSV not found: {args.input}")

    # Import output CSV
    if os.path.exists(args.output):
        import_output_csv(conn, args.output)
    else:
        print(f"  NOTE: Output CSV not found: {args.output} (skipping metrics)")

    # Import validations
    if not args.no_validations:
        import_validations_json(conn, args.validations)

    # Print summary
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM poi_input")
    input_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM poi_metrics")
    metrics_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM validations")
    val_count = cursor.fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"Database ready: {args.db}")
    print(f"  poi_input:   {input_count} rows")
    print(f"  poi_metrics: {metrics_count} rows")
    print(f"  validations: {val_count} rows")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == '__main__':
    main()
