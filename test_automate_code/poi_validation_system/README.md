# POI Validation System

**Self-contained POI validation system with web interface, Excel/Google Sheets export, and visual validation indicators.**

## 🚀 Quick Start

1. **Launch the system:**
   ```bash
   cd /mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system
   ./run_poi_validation.sh
   ```

2. **Open your browser:**
   - Navigate to: `http://localhost:5002`

3. **Start validating:**
   - Select a POI from the dropdown
   - Review data and screenshots
   - Set validation status and add comments
   - Save validation (auto-updates Excel/CSV)

## 📁 File Structure

### **Core Application Files**
```
poi_validation_system/
├── poi_validation_server.py     # Flask backend server
├── poi_validation_app.html      # Web interface (HTML/CSS/JavaScript)
├── run_poi_validation.sh        # Launch script
└── validations.json            # Stored validation data (created automatically)
```

### **Data Files**
```
input/
└── usa_sample_10.csv           # Input POI data (9 POIs for validation)

output/
├── poi_extracted.csv           # Output data with matching metrics
├── poi_validation_report.xlsx  # Auto-generated Excel report
├── poi_validation_google_sheets.csv # Google Sheets compatible CSV
├── screenshots_with_display/   # Display screenshots (7 files)
└── screenshots_kepler/         # Kepler screenshots (7 files)
```

### **Documentation**
```
├── README.md                   # This file
├── README_POI_VALIDATION.md    # Detailed usage guide
├── GOOGLE_SHEETS_INTEGRATION.md # Google Sheets setup guide
└── VALIDATION_INDICATORS.md   # Visual indicators explanation
```

## 🎯 Key Features

- **Web Interface**: Modern, responsive design with real-time updates
- **Dual Screenshots**: Display and Kepler views for each POI  
- **Smart Deduplication**: Prioritizes rows with actual matching metrics
- **Visual Indicators**: Green checkmarks show validated POIs
- **Progress Tracking**: Live completion percentage in header
- **Auto-save**: Excel and CSV files update on each validation save
- **Color-coded Metrics**: Match percentages with visual indicators
- **Google Sheets Ready**: Clean CSV export for collaborative review

## 📊 Data Files Overview

| File | Purpose | Contains |
|------|---------|-----------|
| `input/usa_sample_10.csv` | Source POI data | 9 POIs with basic info, AI predictions, polygon data |
| `output/poi_extracted.csv` | Processed data | Same POIs + matching metrics (401 rows → 9 unique) |
| `validations.json` | Validation results | User validation decisions and comments |
| `output/poi_validation_report.xlsx` | Excel report | Complete dataset with formatting and summary |
| `output/poi_validation_google_sheets.csv` | Clean export | Streamlined data for Google Sheets import |

## 🔄 How to Update Data

### **To change input POIs:**
1. Replace `input/usa_sample_10.csv` with your data
2. Update `output/poi_extracted.csv` with matching results  
3. Update screenshot folders with new POI images
4. Restart the server

### **To add screenshots:**
- **Display screenshots**: Add to `output/screenshots_with_display/`
- **Kepler screenshots**: Add to `output/screenshots_kepler/`
- **Naming**: Use format `POI_CODE.png` (must match poi_code exactly)

### **File Requirements:**
- **CSV files**: Must have `poi_code` as the common key
- **Screenshots**: Filename must start with the exact poi_code
- **Encoding**: UTF-8 for CSV files

## 🛠 System Requirements

- **Python 3.6+**
- **Required packages** (auto-installed by launch script):
  - `flask` - Web server
  - `flask-cors` - Cross-origin requests
  - `pandas` - Data processing  
  - `openpyxl` - Excel file generation

## 💡 Usage Tips

1. **Focus on unvalidated POIs** (no green checkmark)
2. **Use color-coded metrics** to identify quality issues
3. **Add detailed comments** for complex validation decisions
4. **Export regularly** to backup validation progress
5. **Use Google Sheets CSV** for team collaboration

## 🏃 Running the System

### **Normal startup:**
```bash
./run_poi_validation.sh
```

### **Manual startup:**
```bash
python3 poi_validation_server.py
```

### **Background mode:**
```bash
nohup python3 poi_validation_server.py &
```

## 🔧 Configuration

All paths are configured in `poi_validation_server.py`:
- Input/output directories
- Screenshot locations
- File naming conventions
- Server port (default: 5002)

## 📝 Validation Workflow

1. **Select POI** → Dropdown shows validation status
2. **Review Data** → Check AI predictions, matching metrics  
3. **View Screenshots** → Display and Kepler comparisons
4. **Validate** → Set POI type, polygon area, polygon status
5. **Comment** → Add detailed notes
6. **Save** → Auto-updates Excel/CSV files
7. **Track Progress** → Header shows completion percentage

## 🎨 Visual Indicators

- **✓ Green POIs**: Already validated
- **White POIs**: Need validation
- **Progress bar**: Shows completion percentage
- **Color-coded metrics**: Green (high), orange (medium), red (low)

---

**Ready to validate POIs!** 🚀 Run `./run_poi_validation.sh` to get started.