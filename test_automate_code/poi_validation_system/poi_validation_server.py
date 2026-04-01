#!/usr/bin/env python3
"""
POI Validation Server
Provides API endpoints for POI data validation system
"""

from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_cors import CORS
import pandas as pd
import json
import os
import subprocess
import signal
from datetime import datetime
import glob
import ast
from pathlib import Path
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import trino
from trino.auth import BasicAuthentication

app = Flask(__name__)
CORS(app)

# Configuration
BASE_DIR = Path("/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system")
INPUT_CSV = BASE_DIR / "input" / "usa_sample - usa_sample.csv"
OUTPUT_CSV = BASE_DIR / "output" / "poi_extracted.csv"
SCREENSHOTS_DISPLAY = BASE_DIR / "output" / "screenshots_with_display"
SCREENSHOTS_KEPLER = BASE_DIR / "output" / "screenshots_kepler"
VALIDATION_FILE = BASE_DIR / "validations.json"
CONFIG_FILE = BASE_DIR / "config.json"
EXCEL_OUTPUT = BASE_DIR / "output" / "poi_validation_report.xlsx"
GOOGLE_SHEETS_CSV = BASE_DIR / "output" / "poi_validation_google_sheets.csv"

# Kepler configuration
KEPLER_VITE_DIR = Path("/mnt/data/POI_Testing_Automation/version=5_0_2/get-started-vite")
KEPLER_OUTPUT_JSON = KEPLER_VITE_DIR / "src" / "output.json"
KEPLER_PORT = 8082
kepler_process = None  # Track the running Kepler dev server

# Required fields from input CSV
INPUT_FIELDS = [
    'poi_code', 'name', 'address', 'district_code', 'country', 'latitude', 'longitude', 'gmaps_url',
    'brands', 'brand_method', 'poi_type', 'category_poi_types', 
    'poi_type_cumulative', 'final_poitype_distilbert', 'final_confidence_distilbert',
    'google_category_tags', 'area_tag', 'polygon_area_sqm', 'parent_polygon_area_sqm',
    'website_domain_name'
]

# Required fields from output CSV
OUTPUT_FIELDS = [
    'name_match_pct', 'address_match_pct', 'distance_from_latlong_m',
    'location_status_match', 'ratings_diff'
]

def safe_parse_list(value):
    """Safely parse list strings from CSV"""
    if pd.isna(value) or value == '' or value == 'nan':
        return []
    if isinstance(value, str):
        try:
            # Try to parse as Python literal
            if value.startswith('[') and value.endswith(']'):
                return ast.literal_eval(value)
            # If it's a simple string, return as single-item list
            return [value]
        except:
            # If parsing fails, treat as single string
            return [value] if value else []
    return value if isinstance(value, list) else [value]

def load_poi_data():
    """Load and merge POI data from input and output CSVs"""
    try:
        # Read input CSV
        input_df = pd.read_csv(INPUT_CSV)
        
        # Read output CSV if it exists
        if OUTPUT_CSV.exists():
            output_df = pd.read_csv(OUTPUT_CSV)
            
            # For each poi_code, find the best row (one with non-null matching metrics)
            best_rows = []
            for poi_code in output_df['poi_code'].unique():
                poi_rows = output_df[output_df['poi_code'] == poi_code]
                
                # Try to find a row with non-null matching metrics
                good_rows = poi_rows.dropna(subset=['name_match_pct', 'address_match_pct'])
                if not good_rows.empty:
                    best_rows.append(good_rows.iloc[0])
                else:
                    # If no good rows, take the first one
                    best_rows.append(poi_rows.iloc[0])
            
            output_df_dedup = pd.DataFrame(best_rows)
            print(f"Output CSV: {len(output_df)} rows -> {len(output_df_dedup)} unique POIs (preferring rows with matching metrics)")
            
            # Merge on poi_code
            merged_df = pd.merge(input_df, output_df_dedup[['poi_code'] + OUTPUT_FIELDS], 
                               on='poi_code', how='left')
        else:
            merged_df = input_df
        
        return merged_df
    except Exception as e:
        print(f"Error loading POI data: {e}")
        return pd.DataFrame()

def find_screenshot(poi_code, screenshot_dir):
    """Find screenshot file for given POI code"""
    pattern = f"{poi_code}*.png"
    files = list(screenshot_dir.glob(pattern))
    if files:
        return files[0].name
    return None

def load_validations():
    """Load existing validations from JSON file"""
    if VALIDATION_FILE.exists():
        try:
            with open(VALIDATION_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_validations(validations):
    """Save validations to JSON file"""
    with open(VALIDATION_FILE, 'w') as f:
        json.dump(validations, f, indent=2)

def load_config():
    """Load config from JSON file"""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"assignees": []}
    return {"assignees": []}

@app.route('/')
def index():
    """Serve the main HTML page"""
    return send_file(BASE_DIR / 'poi_validation_app.html')

@app.route('/logo.svg')
def serve_logo():
    """Serve the Sherlock logo SVG"""
    return send_file(BASE_DIR / 'sherlockLogo.svg', mimetype='image/svg+xml')

@app.route('/mycroft_logo.png')
def serve_mycroft_logo():
    """Serve the Mycroft logo PNG"""
    return send_file(BASE_DIR / 'MyCroft_logo.png', mimetype='image/png')

@app.route('/api/config')
def get_config():
    """Get configuration including assignee list"""
    return jsonify(load_config())

@app.route('/api/filters')
def get_filters():
    """Get unique values for filter dropdowns"""
    df = load_poi_data()
    if df.empty:
        return jsonify({'error': 'No data available'}), 404

    countries = sorted(df['country'].dropna().unique().tolist())
    poi_types = sorted(df['poi_type'].dropna().unique().tolist())

    # Extract level1 from poi_type (first segment before the dot)
    level1_values = sorted(
        df['poi_type'].dropna().apply(lambda x: str(x).split('.')[0]).unique().tolist()
    )

    return jsonify({
        'countries': countries,
        'poi_types': poi_types,
        'level1_values': level1_values
    })

@app.route('/api/poi_list')
def get_poi_list():
    """Get list of all POIs with optional filtering"""
    df = load_poi_data()
    if df.empty:
        return jsonify({'error': 'No data available'}), 404

    # Apply filters from query params
    country = request.args.get('country', '')
    poi_type = request.args.get('poi_type', '')
    level1 = request.args.get('level1', '')
    assignee = request.args.get('assignee', '')

    if country:
        df = df[df['country'] == country]

    if level1:
        df = df[df['poi_type'].fillna('').apply(lambda x: str(x).split('.')[0]) == level1]

    if poi_type:
        df = df[df['poi_type'] == poi_type]

    # Apply assignee split — deterministic split based on sorted poi_codes
    if assignee:
        config = load_config()
        assignees = config.get('assignees', [])
        if assignees and assignee in assignees:
            assignee_index = assignees.index(assignee)
            num_assignees = len(assignees)
            # Sort by poi_code for deterministic split
            df = df.sort_values('poi_code').reset_index(drop=True)
            df = df[df.index % num_assignees == assignee_index]

    poi_list = []
    for _, row in df.iterrows():
        poi_list.append({
            'poi_code': row['poi_code'],
            'name': row['name'] if pd.notna(row['name']) else 'Unknown'
        })

    return jsonify({'poi_list': poi_list})

@app.route('/api/poi_data/<poi_code>')
def get_poi_data(poi_code):
    """Get detailed data for a specific POI"""
    df = load_poi_data()
    if df.empty:
        return jsonify({'error': 'No data available'}), 404
    
    # Find the POI
    poi_row = df[df['poi_code'] == poi_code]
    if poi_row.empty:
        return jsonify({'error': f'POI {poi_code} not found'}), 404
    
    row = poi_row.iloc[0]
    
    # Prepare response data
    data = {'poi_code': poi_code}
    
    # Add input fields
    for field in INPUT_FIELDS:
        if field in row:
            value = row[field]
            # Handle list fields
            if field in ['category_poi_types', 'google_category_tags']:
                data[field] = safe_parse_list(value)
            elif pd.notna(value):
                # Convert numpy int64/float64 to Python native types
                if hasattr(value, 'item'):
                    data[field] = value.item()
                else:
                    data[field] = value
            else:
                data[field] = None
    
    # Add output fields
    for field in OUTPUT_FIELDS:
        if field in row:
            value = row[field]
            if pd.notna(value):
                # Convert numpy int64/float64 to Python native types
                if hasattr(value, 'item'):
                    data[field] = value.item()
                else:
                    data[field] = value
            else:
                data[field] = None
    
    # Find screenshots
    screenshot_display = find_screenshot(poi_code, SCREENSHOTS_DISPLAY)
    screenshot_kepler = find_screenshot(poi_code, SCREENSHOTS_KEPLER)
    
    if screenshot_display:
        data['screenshot_display'] = f"display/{screenshot_display}"
    if screenshot_kepler:
        data['screenshot_kepler'] = f"kepler/{screenshot_kepler}"
    
    return jsonify(data)

@app.route('/api/screenshot/display/<path:filename>')
def serve_screenshot_display(filename):
    """Serve screenshot files from display directory"""
    if (SCREENSHOTS_DISPLAY / filename).exists():
        return send_from_directory(SCREENSHOTS_DISPLAY, filename)
    else:
        return jsonify({'error': 'Display screenshot not found'}), 404

@app.route('/api/screenshot/kepler/<path:filename>')
def serve_screenshot_kepler(filename):
    """Serve screenshot files from kepler directory"""
    if (SCREENSHOTS_KEPLER / filename).exists():
        return send_from_directory(SCREENSHOTS_KEPLER, filename)
    else:
        return jsonify({'error': 'Kepler screenshot not found'}), 404

@app.route('/api/get_validations')
def get_validations():
    """Get all existing validations"""
    validations = load_validations()
    return jsonify(validations)

@app.route('/api/save_validation', methods=['POST'])
def save_validation():
    """Save validation for a POI"""
    try:
        validation_data = request.json
        poi_code = validation_data.get('poi_code')
        
        if not poi_code:
            return jsonify({'error': 'POI code is required'}), 400
        
        # Load existing validations
        validations = load_validations()
        
        # Update or add validation
        validations[poi_code] = validation_data
        
        # Save to file
        save_validations(validations)
        
        # Immediately update Excel file after each save
        print(f"Auto-saving validation for {poi_code} to Excel...")
        update_excel_report()
        
        # Also create/update Google Sheets compatible CSV
        update_google_sheets_csv()
        
        return jsonify({
            'success': True, 
            'message': f'Validation saved and Excel updated for {poi_code}'
        })
    except Exception as e:
        print(f"Error saving validation: {e}")
        return jsonify({'error': str(e)}), 500

def update_excel_report():
    """Update Excel report with all POI data and validations"""
    try:
        # Load POI data
        df = load_poi_data()
        if df.empty:
            return
        
        # Load validations
        validations = load_validations()
        
        # Add validation columns
        df['poi_type_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('poi_type_validation', ''))
        df['correct_poi_type'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('correct_poi_type', ''))
        df['brand_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('brand_validation', ''))
        df['polygon_area_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('polygon_area_validation', ''))
        df['polygon_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('polygon_validation', ''))
        df['validation_comments'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('comments', ''))
        df['validation_timestamp'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('timestamp', ''))

        # Create Excel writer
        with pd.ExcelWriter(EXCEL_OUTPUT, engine='openpyxl') as writer:
            # Write main data
            df.to_excel(writer, sheet_name='POI Validation Data', index=False)
            
            # Get workbook and worksheet
            workbook = writer.book
            worksheet = workbook['POI Validation Data']
            
            # Apply formatting
            header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
            header_font = Font(color='FFFFFF', bold=True)
            
            # Format headers
            for col in range(1, worksheet.max_column + 1):
                cell = worksheet.cell(row=1, column=col)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Apply conditional formatting for validation columns
            validation_fill_correct = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
            validation_fill_incorrect = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            
            validation_cols = ['poi_type_validation', 'brand_validation', 'polygon_area_validation', 'polygon_validation']
            for col_name in validation_cols:
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name) + 1
                    for row in range(2, worksheet.max_row + 1):
                        cell = worksheet.cell(row=row, column=col_idx)
                        if cell.value == 'correct':
                            cell.fill = validation_fill_correct
                        elif cell.value == 'incorrect':
                            cell.fill = validation_fill_incorrect
            
            # Add summary sheet
            summary_data = {
                'Metric': [
                    'Total POIs',
                    'POIs Validated',
                    'POI Type - Correct',
                    'POI Type - Incorrect',
                    'Brand - Correct',
                    'Brand - Incorrect',
                    'Polygon Area - Correct',
                    'Polygon Area - Incorrect',
                    'Polygon - Correct',
                    'Polygon - Incorrect'
                ],
                'Count': [
                    len(df),
                    len([v for v in validations.values() if any([v.get('poi_type_validation'), v.get('brand_validation'), v.get('polygon_area_validation'), v.get('polygon_validation')])]),
                    len([v for v in validations.values() if v.get('poi_type_validation') == 'correct']),
                    len([v for v in validations.values() if v.get('poi_type_validation') == 'incorrect']),
                    len([v for v in validations.values() if v.get('brand_validation') == 'correct']),
                    len([v for v in validations.values() if v.get('brand_validation') == 'incorrect']),
                    len([v for v in validations.values() if v.get('polygon_area_validation') == 'correct']),
                    len([v for v in validations.values() if v.get('polygon_area_validation') == 'incorrect']),
                    len([v for v in validations.values() if v.get('polygon_validation') == 'correct']),
                    len([v for v in validations.values() if v.get('polygon_validation') == 'incorrect'])
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Validation Summary', index=False)
            
            # Format summary sheet
            summary_sheet = workbook['Validation Summary']
            for col in range(1, 3):
                cell = summary_sheet.cell(row=1, column=col)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            summary_sheet.column_dimensions['A'].width = 25
            summary_sheet.column_dimensions['B'].width = 15
            
    except Exception as e:
        print(f"Error updating Excel report: {e}")

def update_google_sheets_csv():
    """Create/update Google Sheets compatible CSV with validation data"""
    try:
        # Load POI data
        df = load_poi_data()
        if df.empty:
            return
        
        # Load validations
        validations = load_validations()
        
        # Add validation columns
        df['poi_type_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('poi_type_validation', ''))
        df['correct_poi_type'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('correct_poi_type', ''))
        df['brand_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('brand_validation', ''))
        df['polygon_area_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('polygon_area_validation', ''))
        df['polygon_validation'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('polygon_validation', ''))
        df['validation_comments'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('comments', ''))
        df['validation_timestamp'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('timestamp', ''))
        df['validator_name'] = df['poi_code'].map(lambda x: validations.get(x, {}).get('validator', 'System'))

        # Select key columns for Google Sheets (avoid overwhelming with too many columns)
        google_sheets_columns = [
            'poi_code', 'name', 'address', 'district_code', 'country',
            'poi_type', 'final_poitype_distilbert', 'final_confidence_distilbert',
            'polygon_area_sqm', 'area_tag',
            'name_match_pct', 'address_match_pct', 'distance_from_latlong_m', 'location_status_match',
            'poi_type_validation', 'correct_poi_type', 'brand_validation',
            'polygon_area_validation', 'polygon_validation',
            'validation_comments', 'validation_timestamp', 'validator_name'
        ]
        
        # Filter columns that exist in the dataframe
        available_columns = [col for col in google_sheets_columns if col in df.columns]
        google_sheets_df = df[available_columns].copy()
        
        # Clean up data for CSV compatibility
        for col in google_sheets_df.columns:
            if google_sheets_df[col].dtype == 'object':
                # Handle list columns by converting to string
                google_sheets_df[col] = google_sheets_df[col].astype(str).replace(['nan', 'None'], '')
        
        # Save to CSV
        google_sheets_df.to_csv(GOOGLE_SHEETS_CSV, index=False)
        print(f"Google Sheets CSV updated: {GOOGLE_SHEETS_CSV}")
        print(f"Rows: {len(google_sheets_df)}, Columns: {len(available_columns)}")
        
    except Exception as e:
        print(f"Error updating Google Sheets CSV: {e}")


@app.route('/api/analytics')
def get_analytics():
    """Get analytics data for the dashboard"""
    df = load_poi_data()
    if df.empty:
        return jsonify({'error': 'No data available'}), 404

    validations = load_validations()
    config = load_config()
    assignees = config.get('assignees', [])

    all_poi_codes = df['poi_code'].tolist()
    all_poi_names = {row['poi_code']: row['name'] if pd.notna(row['name']) else 'Unknown'
                     for _, row in df.iterrows()}

    def compute_stats(poi_codes):
        total = len(poi_codes)
        tested = 0
        stats = {
            'total': total,
            'poi_type': {'correct': 0, 'incorrect': 0, 'untested': 0},
            'brand': {'correct': 0, 'incorrect': 0, 'untested': 0},
            'polygon_area': {'correct': 0, 'incorrect': 0, 'untested': 0},
            'polygon': {'correct': 0, 'incorrect': 0, 'untested': 0},
            'comments': []
        }
        for pc in poi_codes:
            v = validations.get(pc, {})
            has_any = bool(v.get('poi_type_validation') or v.get('brand_validation')
                          or v.get('polygon_area_validation') or v.get('polygon_validation'))
            if has_any:
                tested += 1

            for key, field in [('poi_type', 'poi_type_validation'), ('brand', 'brand_validation'),
                               ('polygon_area', 'polygon_area_validation'), ('polygon', 'polygon_validation')]:
                val = v.get(field, '')
                if val == 'correct':
                    stats[key]['correct'] += 1
                elif val == 'incorrect':
                    stats[key]['incorrect'] += 1
                else:
                    stats[key]['untested'] += 1

            comment = v.get('comments', '').strip()
            if comment:
                stats['comments'].append({
                    'poi_code': pc,
                    'name': all_poi_names.get(pc, 'Unknown'),
                    'comment': comment,
                    'timestamp': v.get('timestamp', ''),
                    'correct_poi_type': v.get('correct_poi_type', '')
                })

        stats['tested'] = tested
        stats['untested_count'] = total - tested
        return stats

    # Overall stats
    overall = compute_stats(all_poi_codes)

    # Per-assignee stats
    per_assignee = {}
    if assignees:
        sorted_codes = sorted(all_poi_codes)
        for i, assignee in enumerate(assignees):
            assignee_codes = [sorted_codes[j] for j in range(len(sorted_codes)) if j % len(assignees) == i]
            per_assignee[assignee] = compute_stats(assignee_codes)

    return jsonify({
        'overall': overall,
        'per_assignee': per_assignee,
        'assignees': assignees
    })


def fetch_poi_polygon_from_trino(poi_code):
    """Query Trino for a single POI's polygon data"""
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
    query = f"""
    SELECT poi_code, polygon, latitude, longitude, name, address
    FROM poi_data_5_0_10
    WHERE poi_code = '{poi_code}'
    """
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    if not results:
        return None

    df = pd.DataFrame(results, columns=column_names)
    return df.to_dict(orient="records")


def is_kepler_running():
    """Check if the Kepler Vite dev server is running on the expected port"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', KEPLER_PORT)) == 0


def start_kepler_server():
    """Start the Kepler Vite dev server if not already running"""
    global kepler_process
    if is_kepler_running():
        print(f"Kepler dev server already running on port {KEPLER_PORT}")
        return True

    try:
        print(f"Starting Kepler dev server in {KEPLER_VITE_DIR}...")
        kepler_process = subprocess.Popen(
            ['pnpm', 'dev', '--host'],
            cwd=str(KEPLER_VITE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )
        # Wait a bit for the server to start
        import time
        for _ in range(30):
            time.sleep(1)
            if is_kepler_running():
                print(f"Kepler dev server started on port {KEPLER_PORT}")
                return True
        print("Kepler dev server failed to start within 30 seconds")
        return False
    except Exception as e:
        print(f"Error starting Kepler dev server: {e}")
        return False


@app.route('/api/open_kepler/<poi_code>', methods=['POST'])
def open_kepler(poi_code):
    """Fetch polygon data for a POI from Trino, write to output.json, and start Kepler"""
    try:
        # Step 1: Fetch polygon data from Trino
        print(f"Fetching polygon data for {poi_code} from Trino...")
        poi_data = fetch_poi_polygon_from_trino(poi_code)

        if not poi_data:
            return jsonify({'error': f'No polygon data found for {poi_code} in Trino'}), 404

        # Step 2: Write to output.json for the Kepler Vite app
        with open(KEPLER_OUTPUT_JSON, 'w') as f:
            json.dump(poi_data, f, indent=2)
        print(f"Wrote {len(poi_data)} records to {KEPLER_OUTPUT_JSON}")

        # Step 3: Start Kepler dev server if not running
        kepler_started = start_kepler_server()

        if not kepler_started:
            return jsonify({
                'error': 'Could not start Kepler dev server. Please start it manually: cd get-started-vite && pnpm dev'
            }), 500

        kepler_url = f"http://localhost:{KEPLER_PORT}"
        return jsonify({
            'success': True,
            'message': f'Kepler ready for {poi_code}',
            'kepler_url': kepler_url
        })

    except Exception as e:
        print(f"Error opening Kepler for {poi_code}: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Ensure output directory exists
    (BASE_DIR / "output").mkdir(exist_ok=True)

    print(f"Starting POI Validation Server...")
    print(f"Base directory: {BASE_DIR}")
    print(f"Input CSV: {INPUT_CSV}")
    print(f"Output CSV: {OUTPUT_CSV}")
    print(f"Screenshots Display: {SCREENSHOTS_DISPLAY}")
    print(f"Screenshots Kepler: {SCREENSHOTS_KEPLER}")
    print(f"\nServer running at http://localhost:5002")
    print("Open your browser and navigate to http://localhost:5002 to use the application")

    app.run(debug=True, host='0.0.0.0', port=5002)