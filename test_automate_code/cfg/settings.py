# ============================================================
# POI Extractor — Configuration
# ============================================================

import os

# --- Paths ---
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_CSV       = os.path.join(BASE_DIR, "input", "usa_sample - usa_sample.csv")
DISTRICT_MAP    = os.path.join(BASE_DIR, "input", "Districts.csv")
CATEGORY_MAP    = "" # os.path.join(BASE_DIR, "input", "category_poi_mapping.csv")
OUTPUT_CSV      = os.path.join(BASE_DIR, "poi_validation_system", "output", "poi_extracted.csv")
ERROR_LOG       = os.path.join(BASE_DIR, "logs",   "errors.log")
PROGRESS_FILE   = os.path.join(BASE_DIR, "logs",   "progress.json")

# --- Browser ---
BROWSER_INSTANCES   = 8          # parallel Comet browser workers
PAGE_LOAD_TIMEOUT   = 30         # seconds to wait for Maps page
EXTRACTION_WAIT     = 3          # seconds after load before scraping
RATE_LIMIT_DELAY    = 1.5        # seconds between navigations (per worker)

# --- Retry ---
MAX_RETRIES         = 3
RETRY_BACKOFF       = 5          # seconds between retries

# --- Fields extracted from Google Maps ---
FIELDS_TO_EXTRACT = [
    "name",
    "address",
    "district_code",        # resolved via mapping
    "country",
    "latitude",
    "longitude",
    "location_status",
    "ratings",
    "reviews_count",
    "brands",
    "poi_type",             # resolved via category mapping
    "google_category_tags",
]

# --- CSS Selectors for Google Maps DOM ---
# These target the rendered Maps place page
SELECTORS = {
    "name":             'h1.DUwDvf',
    "address":          'button[data-item-id="address"] .Io6YTe',
    "rating":           'div.F7nice span[aria-hidden="true"]',
    "reviews_count":    'div.F7nice span[aria-label*="reviews"]',
    "category_tags":    'button.DkEaL',
    "coordinates_url":  None,    # parsed from URL @lat,lng
    "open_status":      'span.ZDu9vd span',
}
