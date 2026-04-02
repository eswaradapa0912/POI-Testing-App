#!/usr/bin/env python3
"""
Get validation counts for a specific assignee or all assignees.

Usage:
    python3 assignee_counts.py                          # All assignees
    python3 assignee_counts.py "Aditya Mishra"          # Specific assignee
    python3 assignee_counts.py --csv path/to/file.csv   # Custom CSV path
"""

import argparse
import pandas as pd
from pathlib import Path

DEFAULT_CSV = Path(__file__).parent / "output" / "poi_validation_google_sheets.csv"


def get_counts(csv_path, assignee=None):
    df = pd.read_csv(csv_path)

    # Replace NaN with empty string for validation columns
    val_cols = ['poi_type_validation', 'brand_validation',
                'polygon_area_validation', 'polygon_validation']
    for col in val_cols:
        if col in df.columns:
            df[col] = df[col].fillna('')

    if 'validator_name' in df.columns:
        df['validator_name'] = df['validator_name'].fillna('')

    def compute(subset):
        total = len(subset)
        validated = subset[val_cols].apply(lambda row: any(row != ''), axis=1).sum()
        poi_type_correct = (subset['poi_type_validation'] == 'correct').sum()
        poi_type_incorrect = (subset['poi_type_validation'] == 'incorrect').sum()
        brand_correct = (subset['brand_validation'] == 'correct').sum()
        brand_incorrect = (subset['brand_validation'] == 'incorrect').sum()
        polygon_area_correct = (subset['polygon_area_validation'] == 'correct').sum()
        polygon_area_incorrect = (subset['polygon_area_validation'] == 'incorrect').sum()
        polygon_correct = (subset['polygon_validation'] == 'correct').sum()
        polygon_incorrect = (subset['polygon_validation'] == 'incorrect').sum()

        return {
            'Total POIs': total,
            'Validated': int(validated),
            'Not Validated': total - int(validated),
            'POI Type - Correct': int(poi_type_correct),
            'POI Type - Incorrect': int(poi_type_incorrect),
            'Brand - Correct': int(brand_correct),
            'Brand - Incorrect': int(brand_incorrect),
            'Polygon Area - Correct': int(polygon_area_correct),
            'Polygon Area - Incorrect': int(polygon_area_incorrect),
            'Polygon - Correct': int(polygon_correct),
            'Polygon - Incorrect': int(polygon_incorrect),
        }

    if assignee:
        subset = df[df['validator_name'] == assignee]
        if subset.empty:
            print(f"No validations found for '{assignee}'")
            return
        print(f"\n--- Counts for: {assignee} ---")
        for k, v in compute(subset).items():
            print(f"  {k}: {v}")
    else:
        # Show all assignees
        validators = sorted(df[df['validator_name'] != '']['validator_name'].unique())
        if not validators:
            print("No validator names found in the CSV.")
            return

        print(f"\n{'Assignee':<25} {'Validated':>10} {'POI Type':>12} {'Brand':>12} {'Poly Area':>12} {'Polygon':>12}")
        print("-" * 85)
        for name in validators:
            subset = df[df['validator_name'] == name]
            c = compute(subset)
            pt = f"{c['POI Type - Correct']}C/{c['POI Type - Incorrect']}I"
            br = f"{c['Brand - Correct']}C/{c['Brand - Incorrect']}I"
            pa = f"{c['Polygon Area - Correct']}C/{c['Polygon Area - Incorrect']}I"
            pg = f"{c['Polygon - Correct']}C/{c['Polygon - Incorrect']}I"
            print(f"{name:<25} {c['Validated']:>10} {pt:>12} {br:>12} {pa:>12} {pg:>12}")

        # Overall
        print("-" * 85)
        c = compute(df)
        pt = f"{c['POI Type - Correct']}C/{c['POI Type - Incorrect']}I"
        br = f"{c['Brand - Correct']}C/{c['Brand - Incorrect']}I"
        pa = f"{c['Polygon Area - Correct']}C/{c['Polygon Area - Incorrect']}I"
        pg = f"{c['Polygon - Correct']}C/{c['Polygon - Incorrect']}I"
        print(f"{'OVERALL':<25} {c['Validated']:>10} {pt:>12} {br:>12} {pa:>12} {pg:>12}")
        print(f"{'Total POIs':>25}: {c['Total POIs']}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Get validation counts per assignee")
    parser.add_argument('assignee', nargs='?', default=None, help='Assignee name (optional)')
    parser.add_argument('--csv', default=str(DEFAULT_CSV), help='Path to CSV file')
    args = parser.parse_args()

    get_counts(args.csv, args.assignee)
