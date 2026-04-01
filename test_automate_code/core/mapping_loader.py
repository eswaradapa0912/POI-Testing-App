"""
mapping_loader.py
-----------------
Loads and exposes:
  - district_name  → district_code
  - google_category → poi_type
"""

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_district_mapping(csv_path: str) -> dict:
    """
    Expects CSV with columns: district_name, district_code
    Returns: { "district name (lowercase)": "CODE" }
    """
    if not csv_path:          # ← add this
        return {}
    mapping = {}
    path = Path(csv_path)
    if not path.exists():
        logger.warning(f"District mapping file not found: {csv_path}")
        return mapping

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("district_name", "").strip().lower()
            code = row.get("district_code", "").strip()
            if name and code:
                mapping[name] = code

    logger.info(f"Loaded {len(mapping)} district mappings from {csv_path}")
    return mapping


def load_category_mapping(csv_path: str) -> dict:
    """
    Expects CSV with columns: google_category, poi_type
    Returns: { "google category (lowercase)": "POI_TYPE" }
    """
    if not csv_path:          # ← add this
        return {}

    mapping = {}
    path = Path(csv_path)
    if not path.exists():
        logger.warning(f"Category mapping file not found: {csv_path}")
        return mapping

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat  = row.get("google_category", "").strip().lower()
            ptype = row.get("poi_type", "").strip()
            if cat and ptype:
                mapping[cat] = ptype

    logger.info(f"Loaded {len(mapping)} category→poi_type mappings from {csv_path}")
    return mapping


def resolve_district_code(address: str, mapping: dict) -> str:
    """
    Scan address string for any known district name (substring match).
    Returns matched district_code or empty string.
    """
    if not address or not mapping:
        return ""
    addr_lower = address.lower()
    for district_name, code in mapping.items():
        if district_name in addr_lower:
            return code
    return ""


def resolve_poi_type(category_tags: list, mapping: dict) -> str:
    """
    Given a list of Google category tag strings, return the first
    matched poi_type from the mapping (priority: first match wins).
    Returns empty string if none found.
    """
    if not category_tags or not mapping:
        return ""
    for tag in category_tags:
        tag_lower = tag.strip().lower()
        if tag_lower in mapping:
            return mapping[tag_lower]
    return ""
