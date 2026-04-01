# Google Sheets Integration Guide

## Automatic Excel & CSV Updates

The POI Validation System now automatically updates both Excel and CSV files whenever you click **"Save Validation"**.

### What Happens When You Save:
1. ✅ **Validation data saved** to `validations.json`
2. ✅ **Excel file updated** automatically (`poi_validation_report.xlsx`)
3. ✅ **Google Sheets CSV created/updated** (`poi_validation_google_sheets.csv`)
4. ✅ **Console confirmation** shows the update

### Export Options:

#### 1. Excel Export (Full Featured)
- Click **"Export to Excel"** button
- Downloads: `poi_validation_YYYYMMDD.xlsx`
- **Features:**
  - Multiple sheets (Data + Summary)
  - Color-coded validations (green/red)
  - Formatted headers and columns
  - Complete dataset with all fields

#### 2. Google Sheets CSV Export (Clean & Simple)
- Click **"Export to CSV (Google Sheets)"** button
- Downloads: `poi_validation_google_sheets_YYYYMMDD.csv`
- **Features:**
  - Streamlined columns for Google Sheets
  - Clean data (no formatting issues)
  - Ready to import into Google Sheets

### Google Sheets Import Steps:

1. **Download CSV**: Click "Export to CSV (Google Sheets)"
2. **Open Google Sheets**: Go to [sheets.google.com](https://sheets.google.com)
3. **Create New Sheet**: Click "Blank" or "+" 
4. **Import CSV**: 
   - File → Import → Upload tab
   - Select your downloaded CSV file
   - Choose "Replace spreadsheet" or "Insert new sheet(s)"
   - Click "Import data"

### Key Columns in Google Sheets CSV:
- `poi_code` - Unique identifier
- `name` - POI name
- `address` - POI address
- `poi_type` - POI type classification  
- `final_poitype_distilbert` - AI-predicted type
- `final_confidence_distilbert` - AI confidence score
- `polygon_area_sqm` - Area in square meters
- `name_match_pct` - Name matching percentage
- `address_match_pct` - Address matching percentage
- `poi_type_validation` - Your validation: correct/incorrect
- `polygon_area_validation` - Your validation: correct/incorrect
- `polygon_validation` - Your validation: correct/incorrect
- `validation_comments` - Your comments
- `validation_timestamp` - When validated

### Auto-Save Features:
- **Real-time Updates**: Files update immediately on each save
- **No Manual Export Needed**: Files are always current
- **Backup Safety**: Multiple formats ensure data preservation
- **Google Sheets Ready**: CSV optimized for Google Sheets import

### File Locations:
- Excel: `output/poi_validation_report.xlsx`
- Google Sheets CSV: `output/poi_validation_google_sheets.csv`
- Validations: `validations.json`

### Tips:
- Use CSV export for Google Sheets (cleaner data)
- Use Excel export for detailed analysis (more features)  
- Files update automatically - no need to export after every validation
- Google Sheets CSV has fewer columns for better performance