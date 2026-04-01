# POI Extractor — Comet Browser Automation

Extracts POI data from Google Maps using parallel Comet browser instances.
Processes 10K POIs from a CSV, navigates each `gmaps_url`, scrapes live data,
resolves district codes and POI types via mapping files, and writes to a single output CSV.

---

## Project Structure

```
poi-extractor/
├── main.py                    # CLI entry point
├── requirements.txt
├── config/
│   └── config.py              # All settings (paths, timeouts, selectors, etc.)
├── core/
│   ├── orchestrator.py        # Parallel worker manager + output writer
│   ├── browser_worker.py      # Per-POI navigation, scraping, enrichment
│   └── mapping_loader.py      # District & category mapping helpers
├── ui/
│   └── dashboard.html         # Live monitoring dashboard (open in browser)
├── input/                     # ← Put your files here
│   ├── pois.csv
│   ├── district_mapping.csv
│   └── category_poi_mapping.csv
├── output/
│   └── poi_extracted.csv      # ← Final output lands here
└── logs/
    ├── run.log
    ├── progress.json          # Checkpoint file (resume support)
    └── errors.log
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Point to Comet browser (if not using system Chromium)

Edit `config/config.py` and uncomment + set `executable_path` in `orchestrator.py`:

```python
browser = pw.chromium.launch(
    executable_path="/Applications/Comet.app/Contents/MacOS/Comet",
    headless=True,
)
```

### 3. Prepare input files

**`input/pois.csv`** — your 10K POI subset. Must include at minimum:
```
poi_code, gmaps_url, [... all other fields ...]
```

**`input/district_mapping.csv`** — district name to code mapping:
```
district_name,district_code
Bandra,MH-BAN
Andheri,MH-AND
...
```

**`input/category_poi_mapping.csv`** — Google category to POI type mapping:
```
google_category,poi_type
restaurant,F&B
cafe,F&B
hospital,Healthcare
...
```

---

## Usage

```bash
# Validate your input files first
python main.py validate

# Start extraction (or resume if checkpoint exists)
python main.py run

# Check progress mid-run
python main.py status

# Clear checkpoint and start fresh
python main.py reset
```

---

## Output

`output/poi_extracted.csv` — one row per POI containing:

| Column | Source |
|--------|--------|
| All original fields | Passed through from input CSV |
| `extracted_name` | Scraped from Maps |
| `extracted_address` | Scraped from Maps |
| `extracted_country` | Parsed from address |
| `extracted_latitude` | Parsed from Maps URL |
| `extracted_longitude` | Parsed from Maps URL |
| `extracted_location_status` | Scraped from Maps |
| `extracted_ratings` | Scraped from Maps |
| `extracted_reviews_count` | Scraped from Maps |
| `extracted_google_category_tags` | Scraped from Maps (pipe-separated) |
| `resolved_district_code` | Mapped via district_mapping.csv |
| `resolved_poi_type` | Mapped via category_poi_mapping.csv |
| `extraction_status` | `success` / `failed` / `skipped_no_url` |
| `extraction_timestamp` | UTC ISO timestamp |

---

## Configuration (config/config.py)

| Setting | Default | Description |
|---------|---------|-------------|
| `BROWSER_INSTANCES` | 4 | Parallel Comet windows |
| `PAGE_LOAD_TIMEOUT` | 30s | Max wait for Maps page |
| `EXTRACTION_WAIT` | 3s | Settle time before scraping |
| `RATE_LIMIT_DELAY` | 1.5s | Delay between navigations per worker |
| `MAX_RETRIES` | 3 | Retries per failed POI |
| `RETRY_BACKOFF` | 5s | Wait between retries |

---

## Dashboard

Open `ui/dashboard.html` directly in any browser for a live monitoring view.
It shows worker status, progress, throughput chart, and live extraction log.

---

## Resume / Checkpoint

The tool saves progress to `logs/progress.json` after every completed POI.
If the run crashes or is stopped, simply re-run `python main.py run` and it
will skip already-completed POIs and continue from where it left off.
