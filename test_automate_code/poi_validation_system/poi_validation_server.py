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
from datetime import datetime
import glob
import ast
from pathlib import Path
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

app = Flask(__name__)
CORS(app)

# Configuration
BASE_DIR = Path("/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system")
INPUT_CSV = BASE_DIR / "input" / "usa_sample_10.csv"
OUTPUT_CSV = BASE_DIR / "output" / "poi_extracted.csv"
SCREENSHOTS_DISPLAY = BASE_DIR / "output" / "screenshots_with_display"
SCREENSHOTS_KEPLER = BASE_DIR / "output" / "screenshots_kepler"
VALIDATION_FILE = BASE_DIR / "validations.json"
EXCEL_OUTPUT = BASE_DIR / "output" / "poi_validation_report.xlsx"
GOOGLE_SHEETS_CSV = BASE_DIR / "output" / "poi_validation_google_sheets.csv"

# Required fields from input CSV
INPUT_FIELDS = [
    'poi_code', 'name', 'address', 'district_code', 'country', 'gmaps_url',
    'brands', 'brand_method', 'poi_type', 'category_poi_types', 
    'poi_type_cumulative', 'final_poitype_distilbert', 'final_confidence_distilbert',
    'google_category_tags', 'area_tag', 'polygon_area_sqm', 'parent_polygon_area_sqm'
]

# Required fields from output CSV
OUTPUT_FIELDS = [
    'name_match_pct', 'address_match_pct', 'distance_from_latlong_m', 
    'location_status_match'
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

@app.route('/')
def index():
    """Serve the main HTML page"""
    return send_file(BASE_DIR / 'poi_validation_app.html')

@app.route('/api/poi_list')
def get_poi_list():
    """Get list of all POIs"""
    df = load_poi_data()
    if df.empty:
        return jsonify({'error': 'No data available'}), 404
    
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
            
            validation_cols = ['poi_type_validation', 'polygon_area_validation', 'polygon_validation']
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
                    'Polygon Area - Correct',
                    'Polygon Area - Incorrect',
                    'Polygon - Correct',
                    'Polygon - Incorrect'
                ],
                'Count': [
                    len(df),
                    len([v for v in validations.values() if any([v.get('poi_type_validation'), v.get('polygon_area_validation'), v.get('polygon_validation')])]),
                    len([v for v in validations.values() if v.get('poi_type_validation') == 'correct']),
                    len([v for v in validations.values() if v.get('poi_type_validation') == 'incorrect']),
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
            'poi_type_validation', 'polygon_area_validation', 'polygon_validation', 
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

@app.route('/api/export_excel')
def export_excel():
    """Export validation data to Excel"""
    try:
        # Update Excel file first
        update_excel_report()
        
        if EXCEL_OUTPUT.exists():
            return send_file(EXCEL_OUTPUT, as_attachment=True, 
                           download_name=f"poi_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        else:
            return jsonify({'error': 'Excel file not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export_google_sheets_csv')
def export_google_sheets_csv():
    """Export Google Sheets compatible CSV"""
    try:
        # Update CSV file first
        update_google_sheets_csv()
        
        if GOOGLE_SHEETS_CSV.exists():
            return send_file(GOOGLE_SHEETS_CSV, as_attachment=True, 
                           download_name=f"poi_validation_google_sheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        else:
            return jsonify({'error': 'Google Sheets CSV file not found'}), 404
    except Exception as e:
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