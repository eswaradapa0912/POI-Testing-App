# POI Validation System

A comprehensive web-based system for validating Point of Interest (POI) data with screenshots and automated Excel reporting.

## Features

- **Interactive Web Interface**: Review POI data with screenshots from two sources
- **Data Integration**: Merges data from input CSV and output CSV files
- **Visual Validation**: Side-by-side display of screenshots with zoom functionality
- **Validation Tracking**: Dropdown selections for POI type, polygon area, and polygon validation
- **Comments System**: Add detailed comments for each POI
- **Excel Export**: Automatic generation of formatted Excel reports with validation summary
- **Real-time Updates**: Validations are saved immediately and reflected in Excel exports

## Installation

### Prerequisites
- Python 3.6+
- Web browser (Chrome, Firefox, Safari, Edge)

### Required Python Packages
The launch script will automatically install:
- flask
- flask-cors
- pandas
- openpyxl

## File Structure

```
/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/
├── poi_validation_app.html      # Frontend HTML/JavaScript interface
├── poi_validation_server.py     # Backend Flask server
├── run_poi_validation.sh       # Launch script
├── input/
│   └── usa_sample_10.csv       # Input POI data
├── output/
│   ├── poi_extracted.csv       # Output data with matching metrics
│   ├── screenshots_with_display/  # Display screenshots
│   ├── screenshots_kepler/        # Kepler screenshots
│   └── poi_validation_report.xlsx # Generated Excel report
└── validations.json             # Stored validations
```

## Usage

### Starting the Application

1. Open a terminal
2. Navigate to the project directory:
   ```bash
   cd /mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code
   ```
3. Run the launch script:
   ```bash
   ./run_poi_validation.sh
   ```
4. Open your browser and go to: `http://localhost:5000`

### Using the Interface

1. **Select a POI**: Use the dropdown to select a POI from the list
2. **Load POI**: Click "Load POI" to display the data and screenshots
3. **Review Information**: 
   - Generic info (name, address, location)
   - Brand information
   - POI type details with confidence scores
   - Polygon area information
   - Matching metrics (name, address, distance)
4. **Validate**: 
   - Select validation status for POI type
   - Select validation status for polygon area
   - Select validation status for polygon
   - Add comments if needed
5. **Save**: Click "Save Validation" to store your validation
6. **Export**: Click "Export to Excel" to download the complete report

## Data Fields

### Input Data (usa_sample_10.csv)
- **Generic**: poi_code, name, address, district_code, country, gmaps_url
- **Brand**: brands, brand_method
- **POI Type**: poi_type, category_poi_types, poi_type_cumulative, final_poitype_distilbert, final_confidence_distilbert, google_category_tags
- **Polygon**: area_tag, polygon_area_sqm, parent_polygon_area_sqm

### Output Data (poi_extracted.csv)
- **Matching Metrics**: name_match_pct, address_match_pct, distance_from_latlong_m, location_status_match

### Validation Fields
- **poi_type_validation**: Correct/Incorrect
- **polygon_area_validation**: Correct/Incorrect
- **polygon_validation**: Correct/Incorrect
- **comments**: Free text field

## Excel Report

The exported Excel file contains:
1. **POI Validation Data**: Complete dataset with all fields and validations
2. **Validation Summary**: Statistics on validation progress and results

### Features:
- Color-coded validation cells (green for correct, red for incorrect)
- Formatted headers
- Auto-adjusted column widths
- Summary statistics sheet

## Troubleshooting

### Server won't start
- Check Python installation: `python3 --version`
- Verify port 5000 is available: `lsof -i :5000`
- Check file permissions: `ls -la *.py *.sh`

### Screenshots not loading
- Verify screenshot files exist in the directories
- Check file naming convention (should start with POI code)
- Ensure proper file permissions

### Data not loading
- Verify CSV files exist and are readable
- Check CSV format and required columns
- Look for error messages in terminal

## API Endpoints

- `GET /`: Main application page
- `GET /api/poi_list`: List of all POIs
- `GET /api/poi_data/<poi_code>`: Detailed POI data
- `GET /api/screenshot/<filename>`: Serve screenshot files
- `GET /api/get_validations`: Get all validations
- `POST /api/save_validation`: Save validation data
- `GET /api/export_excel`: Download Excel report

## Notes

- Screenshots are matched by POI code in filename
- Validations are persistent across sessions
- Excel report is regenerated on each export
- The system handles missing data gracefully with "N/A" placeholders

## Support

For issues or questions, check:
1. Terminal output for error messages
2. Browser console for JavaScript errors
3. Verify all required files are present
4. Ensure proper file permissions