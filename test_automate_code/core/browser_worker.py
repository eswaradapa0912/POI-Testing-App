"""
browser_worker.py
-----------------
Handles a single Comet browser instance.
Each worker pulls POIs from a shared queue, navigates gmaps_url,
extracts data, resolves mappings, and writes to the output queue.

Changes vs previous version:
  1. clean_text()        — guards non-string input, returns None for blank
  2. transform_data2()   — field names clarified; keys match data-item-id values
  3. is_subset_tags()    — now correctly checks per-tag subset (pipe-split);
                           treats unparseable original_tags as single-item list
  4. _merge()            — no logic change; comments added for clarity
  5. General            — no core extraction / navigation logic altered
"""

import ast
import re
import time
import logging
import os
import datetime
from math import radians, sin, cos, sqrt, atan2
from difflib import SequenceMatcher
from typing import Optional

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# URL / STRING UTILITIES
# ══════════════════════════════════════════════════════════════

def parse_coords_from_url(url: str) -> tuple[Optional[float], Optional[float]]:
    """
    Extract ONLY the actual POI coordinates from a Google Maps URL
    using the !3d (latitude) and !4d (longitude) parameters.

    Returns:
        (lat, lng)  on success
        (None, None) on any failure
    """
    lat_match = re.search(r'!3d(-?\d+\.\d+)', url)
    lng_match = re.search(r'!4d(-?\d+\.\d+)', url)

    if lat_match and lng_match:
        try:
            return float(lat_match.group(1)), float(lng_match.group(1))
        except ValueError:
            pass

    return None, None


def clean_reviews_count(raw: str) -> Optional[int]:
    """Convert '(1,234 reviews)' or '1,234' or '1234' → integer.
    Returns None for empty / non-numeric input."""
    if not raw:
        return None
    digits = re.sub(r'[^\d]', '', raw)
    return int(digits) if digits else None


def clean_rating(raw: str) -> Optional[float]:
    """Convert '4.5' or '4,5' → float.
    Returns None for empty / non-numeric input."""
    if not raw:
        return None
    cleaned = raw.strip().replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return None


def haversine_meters(lat1, lon1, lat2, lon2) -> Optional[float]:
    """
    Return distance in metres between two lat/lng points.
    Returns None if any coordinate is missing or non-numeric.
    """
    try:
        R = 6_371_000
        lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1),
                                                float(lat2), float(lon2)])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        return round(R * 2 * atan2(sqrt(a), sqrt(1 - a)), 2)
    except (TypeError, ValueError):
        return None


def match_pct(a: str, b: str) -> Optional[float]:
    """
    Character-level similarity between two strings (0.0 – 100.0).
    Returns None if either string is empty after stripping.
    """
    a = str(a).strip().lower()
    b = str(b).strip().lower()
    if not a or not b:
        return None
    return round(SequenceMatcher(None, a, b).ratio() * 100, 2)

def rating_diff(orig, ext):
    if orig is None or ext is None:
        return None
    try:
        return round(float(ext) - float(orig), 2)  # IMPORTANT: extracted - original
    except Exception:
        return None
    
def reviews_diff(orig, ext):
    if orig is None or ext is None:
        return None
    try:
        return int(ext) - int(orig)  # extracted - original
    except Exception:
        return None

def is_subset_tags(extracted_tags_str: str, original_tags) -> Optional[bool]:
    """
    Check whether EVERY tag in extracted_tags_str (pipe-separated) is
    present in original_tags.

    original_tags may be:
      - a Python-literal string  → parsed with ast.literal_eval
      - a plain string           → treated as a single-item list
      - already a list           → used as-is

    Returns:
      True   — all extracted tags found in original set
      False  — at least one extracted tag missing
      None   — either side is empty / unparseable
    """
    ext_str = extracted_tags_str.strip().lower() if extracted_tags_str else ""

    # Normalise original_tags → list
    if isinstance(original_tags, str):
        try:
            original_tags = ast.literal_eval(original_tags)
        except Exception:
            # Treat as a single non-list string rather than discarding it
            original_tags = [original_tags] if original_tags.strip() else []

    orig_set = {str(t).strip().lower() for t in original_tags if str(t).strip()}

    if not ext_str or not orig_set:
        return None

    # Split pipe-joined extracted tags and check each one
    extracted_tags = [t.strip() for t in ext_str.split("|") if t.strip()]
    if not extracted_tags:
        return None

    return all(tag in orig_set for tag in extracted_tags)


def extract_district_from_address(address: str, district_map: dict) -> str:
    """Scan address string for any known district name (substring match)."""
    if not address or not district_map:
        return ""
    addr_lower = address.lower()
    for district_name in district_map:
        if district_name in addr_lower:
            return district_name.title()
    return ""


# ══════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════

CLOSED_KEYWORDS = ["temporarily closed", "permanently closed"]


# ══════════════════════════════════════════════════════════════
# WORKER CLASS
# ══════════════════════════════════════════════════════════════

class POIBrowserWorker:
    """
    One browser instance.  Call process_poi() per POI.

    `page` is a Playwright/Comet Page object passed in at construction.
    This keeps the worker browser-agnostic — swap the driver freely.
    """

    def __init__(
        self,
        page,
        config,
        district_map: dict,
        category_map: dict,
        worker_id: int = 0,
    ):
        self.page         = page
        self.cfg          = config
        self.district_map = district_map
        self.category_map = category_map
        self.worker_id    = worker_id

    # ──────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────

    def process_poi(self, poi: dict) -> dict:
        """
        Navigate to gmaps_url, extract fields, return enriched row.
        Retries up to MAX_RETRIES times on failure.
        """
        from cfg.settings import MAX_RETRIES, RETRY_BACKOFF

        url      = poi.get("gmaps_url", "").strip()
        poi_code = poi.get("poi_code", "?")

        if not url:
            logger.warning(f"[W{self.worker_id}] {poi_code}: empty gmaps_url, skipping")
            return self._merge(poi, {}, status="skipped_no_url")

        # Normalise zoom level before navigation
        updated_url = re.sub(r',\d+a(?=,13\.1y)', ',120m', url)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                extracted = self._navigate_and_extract(updated_url, poi_code)
                enriched  = self._enrich(extracted)
                return self._merge(poi, enriched, status="success")

            except Exception as exc:
                logger.warning(
                    f"[W{self.worker_id}] {poi_code} attempt {attempt}/{MAX_RETRIES} failed: {exc}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF)

        logger.error(f"[W{self.worker_id}] {poi_code}: all retries exhausted")
        return self._merge(poi, {}, status="failed")

    # ──────────────────────────────────────────────────────────
    # EXTRACTION HELPERS
    # ──────────────────────────────────────────────────────────

    def extract_attributes(self) -> dict:
        """
        Click the last tab, scroll through the attributes panel,
        and return a dict of { section_title: [aria-label values] }.
        """
        data = {}

        tabs = self.page.locator('button[role="tab"]')
        if tabs.count() == 0:
            return data

        tabs.nth(tabs.count() - 1).click()

        container = self.page.locator('div.m6QErb.DxyBCb.XiKgde').first
        container.wait_for(timeout=10_000)

        # Scroll to load all lazy sections
        for _ in range(5):
            self.page.mouse.wheel(0, 2000)
            self.page.wait_for_timeout(1000)

        sections = container.locator('div.iP2t7d')
        logger.debug(f"[W{self.worker_id}] attribute sections found: {sections.count()}")

        for i in range(sections.count()):
            sec = sections.nth(i)
            try:
                title = sec.locator('h2').inner_text().strip()
            except Exception:
                continue

            items  = sec.locator('li span[aria-label]')
            values = []
            for j in range(items.count()):
                val = items.nth(j).get_attribute("aria-label")
                if val:
                    values.append(val.strip())

            if title:
                data[title] = values

        return data

    def extract_info(self) -> dict:
        """
        Extract key/value pairs from elements with data-item-id inside
        the main scrollable container.

        Keys are normalised to the prefix before the first colon so that
        e.g. 'authority:https://…' becomes 'authority'.
        """
        data      = {}
        container = self.page.locator('div.m6QErb.XiKgde')
        items     = container.locator('[data-item-id]')

        for i in range(items.count()):
            el  = items.nth(i)
            key = el.get_attribute("data-item-id")
            if not key:
                continue
            key   = key.split(":")[0]          # normalise
            value = el.inner_text().strip()
            if value:
                data[key] = value

        logger.debug(f"[W{self.worker_id}] raw_info keys: {list(data.keys())}")
        return data

    def clean_text(self, value) -> Optional[str]:
        """
        Strip private-use unicode characters (icon glyphs) and whitespace.

        Edge-case handling vs previous version:
          - Accepts non-string input (converts via str())
          - Returns None (not '') for blank / whitespace-only values
        """
        if value is None:
            return None
        value = str(value)
        if not value.strip():
            return None
        cleaned = re.sub(r'[\ue000-\uf8ff]', '', value).strip()
        return cleaned or None

    def transform_data2(self, data: dict) -> dict:
        """
        Map raw data-item-id keys to output column names.

        Google Maps data-item-id prefixes (verified common values):
          'authority' → website URL
          'phone'     → phone number
          'oloc'      → plus code / open location code

        NOTE: Log raw_info keys once per environment to confirm these
        match what your Maps locale/version actually returns.
        """
        return {
            "extracted_website":   self.clean_text(data.get("authority")),
            "extracted_phone":     self.clean_text(data.get("phone")),
            "extracted_plus_code": self.clean_text(data.get("oloc")),
        }

    # ──────────────────────────────────────────────────────────
    # CORE NAVIGATION + EXTRACTION  (logic unchanged)
    # ──────────────────────────────────────────────────────────

    def _navigate_and_extract(self, url: str, poi_code: str) -> dict:
        from cfg.settings import PAGE_LOAD_TIMEOUT, EXTRACTION_WAIT, SELECTORS

        # ── Navigation ────────────────────────────────────────
        self.page.goto(url, timeout=PAGE_LOAD_TIMEOUT * 1000)
        self.page.wait_for_selector(SELECTORS["name"], timeout=PAGE_LOAD_TIMEOUT * 1000)
        self.page.reload()
        time.sleep(EXTRACTION_WAIT)

        # ── Info panel (website / phone / plus code) ──────────
        raw_info = self.extract_info()
        cleaned  = self.transform_data2(raw_info)

        # ── Screenshot setup ──────────────────────────────────
        base = "/mnt/data/POI_Testing_Automation/version=5_0_2/test_automate_code/poi_validation_system/output"
        os.makedirs(f"{base}/screenshots_without_display", exist_ok=True)
        os.makedirs(f"{base}/screenshots_with_display",    exist_ok=True)
        shot_no_panel = f"{base}/screenshots_without_display/{poi_code}.png"
        shot_panel    = f"{base}/screenshots_with_display/{poi_code}.png"

        # Screenshot BEFORE collapsing the side panel
        self.page.screenshot(path=shot_no_panel, full_page=True)

        # Collapse side panel then screenshot again
        container = self.page.locator('div.gYkzb')
        container.locator('button[aria-label="Collapse side panel"]').click()
        time.sleep(EXTRACTION_WAIT)
        self.page.screenshot(path=shot_panel, full_page=True)

        # ── Name ──────────────────────────────────────────────
        name = self._text(SELECTORS["name"])

        # ── Address ───────────────────────────────────────────
        address = self._text(SELECTORS["address"])

        # ── Rating ────────────────────────────────────────────
        rating = clean_rating(self._text(SELECTORS["rating"]))

        # ── Reviews count ─────────────────────────────────────
        reviews_raw = self._attr(SELECTORS["reviews_count"], "aria-label")
        if not reviews_raw:
            reviews_raw = self._text(SELECTORS["reviews_count"])
        reviews_count = clean_reviews_count(reviews_raw)

        # ── Category tags ─────────────────────────────────────
        category_elements = self.page.query_selector_all(SELECTORS["category_tags"])
        category_tags = [
            el.inner_text().strip()
            for el in category_elements
            if el.inner_text().strip()
        ]

        # ── Open / closed status ──────────────────────────────
        status_raw   = self._text(SELECTORS["open_status"]).strip()
        status_lower = status_raw.lower()

        if status_lower and any(kw in status_lower for kw in CLOSED_KEYWORDS):
            extracted_final_location_status = status_raw
        else:
            extracted_final_location_status = "open"

        # ── Coordinates (from final redirected URL) ───────────
        lat, lng = parse_coords_from_url(self.page.url)

        # ── Country (last comma-segment of address) ───────────
        country = ""
        if address:
            parts   = [p.strip() for p in address.split(",")]
            country = parts[-1] if parts else ""

        return {
            # Core fields
            "extracted_name":                   name,
            "extracted_address":                address,
            "extracted_country":                country,
            "extracted_latitude":               lat,
            "extracted_longitude":              lng,
            "extracted_location_status":        status_raw,
            "extracted_final_location_status":  extracted_final_location_status,
            "extracted_ratings":                rating,
            "extracted_reviews_count":          reviews_count,
            "extracted_google_category_tags":   "|".join(category_tags),
            "_category_tags_list":              category_tags,  # internal; removed in _enrich
            # Contact / location info from info panel
            **cleaned,          # extracted_website, extracted_phone, extracted_plus_code
        }

    # ──────────────────────────────────────────────────────────
    # ENRICHMENT  (logic unchanged)
    # ──────────────────────────────────────────────────────────

    def _enrich(self, extracted: dict) -> dict:
        """Resolve district, district_code, and poi_type via mapping tables."""
        from core.mapping_loader import resolve_district_code, resolve_poi_type

        address       = extracted.get("extracted_address", "")
        category_tags = extracted.get("_category_tags_list", [])

        extracted["extracted_district"]     = extract_district_from_address(address, self.district_map)
        extracted["resolved_district_code"] = resolve_district_code(address, self.district_map)
        extracted["resolved_poi_type"]      = resolve_poi_type(category_tags, self.category_map)

        extracted.pop("_category_tags_list", None)
        return extracted

    # ──────────────────────────────────────────────────────────
    # MERGE + COMPARISON  (logic unchanged; comments added)
    # ──────────────────────────────────────────────────────────

    def _merge(self, original: dict, extracted: dict, status: str) -> dict:
        """
        Merge the original POI row with all extracted fields and
        append comparison / QA columns.

        Output columns added here:
          name_match_pct            float | None
          address_match_pct         float | None
          country_match             bool  | None
          distance_from_latlong_m   float | None
          location_status_match     bool  | None
          category_tags_subset      bool  | None
          extraction_status         str
          extraction_timestamp      str  (ISO-8601 UTC)

        Contact fields arriving via **cleaned in _navigate_and_extract:
          extracted_website         str | None
          extracted_phone           str | None
          extracted_plus_code       str | None
        """
        row = dict(original)
        row.update(extracted)       # all extracted + cleaned fields land here

        # ── Name similarity ───────────────────────────────────
        row["name_match_pct"] = match_pct(
            original.get("name", ""),
            extracted.get("extracted_name", ""),
        )

        # ── Address similarity ────────────────────────────────
        row["address_match_pct"] = match_pct(
            original.get("address", ""),
            extracted.get("extracted_address", ""),
        )

        # ── Country equality ──────────────────────────────────
        orig_country = str(original.get("country", "")).strip().lower()
        ext_country  = str(extracted.get("extracted_country", "")).strip().lower()
        row["country_match"] = (
            (orig_country == ext_country) if (orig_country and ext_country) else None
        )

        # ── Distance between original and extracted coords ────
        row["distance_from_latlong_m"] = haversine_meters(
            original.get("latitude"),
            original.get("longitude"),
            extracted.get("extracted_latitude"),
            extracted.get("extracted_longitude"),
        )

        # ── Location status equality ──────────────────────────
        orig_status = str(original.get("location_status", "")).strip().lower()
        ext_status  = str(extracted.get("extracted_final_location_status", "")).strip().lower()
        row["location_status_match"] = (
            (orig_status == ext_status) if (orig_status and ext_status) else None
        )

        # ── Category tags subset check ────────────────────────
        row["category_tags_subset"] = is_subset_tags(
            extracted.get("extracted_google_category_tags", ""),
            original.get("google_category_tags", ""),
        )

        # ── Ratings diff (+/-) ──────────────────────────────
        row["ratings_diff"] = rating_diff(
            original.get("ratings"),
            extracted.get("extracted_ratings"),
        )

        # ── Reviews count diff (+/-) ───────────────────────
        row["reviews_count_diff"] = reviews_diff(
            original.get("reviews_count"),
            extracted.get("extracted_reviews_count"),
        )
        # ── Metadata ──────────────────────────────────────────
        row["extraction_status"]    = status
        row["extraction_timestamp"] = datetime.datetime.utcnow().isoformat()

        return row

    # ──────────────────────────────────────────────────────────
    # LOW-LEVEL PAGE HELPERS  (unchanged)
    # ──────────────────────────────────────────────────────────

    def _text(self, selector: str) -> str:
        """Safe text extractor — returns '' on any failure."""
        try:
            el = self.page.query_selector(selector)
            return el.inner_text().strip() if el else ""
        except Exception:
            return ""

    def _attr(self, selector: str, attr: str) -> str:
        """Safe attribute extractor — returns '' on any failure."""
        try:
            el = self.page.query_selector(selector)
            return (el.get_attribute(attr) or "") if el else ""
        except Exception:
            return ""