#!/usr/bin/env python3
"""
Geocode Dutch schools using Nominatim (OpenStreetMap) with postal code + street.

DUO data includes addresses but no coordinates. This script geocodes each school
using the free Nominatim API with aggressive caching to avoid repeated calls.

Rate limit: 1 request/second (Nominatim usage policy).
Fallback: postal code centroid when street-level geocoding fails.

Input:  data_nl/intermediate/nl_school_master_base.csv
Output: data_nl/intermediate/nl_school_master_geocoded.csv

Usage:
    python geocode_schools.py
    python geocode_schools.py --force   # Re-geocode all (ignore cache)
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"
CACHE_FILE = CACHE_DIR / "geocode_cache.json"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Nominatim config
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {"User-Agent": "SchoolNossa/1.0 (school comparison platform; contact@schoolnossa.com)"}
RATE_LIMIT_SECONDS = 1.1  # Nominatim requires max 1 req/sec


def load_cache() -> dict:
    """Load geocode cache from disk."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    """Save geocode cache to disk."""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def geocode_address(street: str, postal_code: str, city: str, cache: dict) -> tuple:
    """
    Geocode an address via Nominatim. Returns (lat, lon) or (None, None).
    Tries street-level first, falls back to postal code centroid.
    """
    # Cache key
    key = f"{postal_code}|{street}|{city}"
    if key in cache:
        result = cache[key]
        return result.get("lat"), result.get("lon")

    lat, lon = None, None

    # Attempt 1: Full address
    if street and postal_code:
        params = {
            "q": f"{street}, {postal_code} {city}, Netherlands",
            "format": "json",
            "limit": 1,
            "countrycodes": "nl",
        }
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            resp = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            results = resp.json()
            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
        except Exception as e:
            logger.debug(f"  Geocode failed for {key}: {e}")

    # Attempt 2: Postal code only (centroid)
    if lat is None and postal_code:
        params = {
            "postalcode": postal_code.replace(" ", ""),
            "country": "Netherlands",
            "format": "json",
            "limit": 1,
        }
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            resp = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            results = resp.json()
            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
        except Exception as e:
            logger.debug(f"  Postal code geocode failed for {postal_code}: {e}")

    # Cache result (even if None)
    cache[key] = {"lat": lat, "lon": lon}
    return lat, lon


def main(force: bool = False):
    """Geocode all schools in the master base file."""
    input_path = INTERMEDIATE_DIR / "nl_school_master_base.csv"
    output_path = INTERMEDIATE_DIR / "nl_school_master_geocoded.csv"

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        logger.error("Run duo_school_registry.py first (Phase 1a)")
        sys.exit(1)

    df = pd.read_csv(input_path, dtype=str)
    logger.info(f"Loaded {len(df)} schools from {input_path.name}")

    cache = {} if force else load_cache()
    logger.info(f"Geocode cache: {len(cache)} entries")

    # Geocode each school
    latitudes = []
    longitudes = []
    cached_count = 0
    geocoded_count = 0
    failed_count = 0

    for idx, row in df.iterrows():
        street = row.get("street_address", "")
        postal_code = row.get("postal_code", "")
        city = row.get("city", "")

        key = f"{postal_code}|{street}|{city}"
        was_cached = key in cache

        lat, lon = geocode_address(street, postal_code, city, cache)
        latitudes.append(lat)
        longitudes.append(lon)

        if was_cached:
            cached_count += 1
        elif lat is not None:
            geocoded_count += 1
        else:
            failed_count += 1

        # Progress log every 100 schools
        if (idx + 1) % 100 == 0:
            logger.info(
                f"  Progress: {idx + 1}/{len(df)} "
                f"(cached: {cached_count}, geocoded: {geocoded_count}, failed: {failed_count})"
            )
            save_cache(cache)  # Periodic save

    df["latitude"] = latitudes
    df["longitude"] = longitudes

    # Final save
    save_cache(cache)

    # Convert numeric columns back
    for col in df.columns:
        if col.startswith("students_") or col.startswith("teachers_") or col.startswith("exam_"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_csv(output_path, index=False)

    # Report
    total = len(df)
    with_coords = df["latitude"].notna().sum()
    logger.info(f"\n{'='*60}")
    logger.info(f"GEOCODING COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"  Total schools: {total}")
    logger.info(f"  With coordinates: {with_coords} ({with_coords/total*100:.1f}%)")
    logger.info(f"  From cache: {cached_count}")
    logger.info(f"  Newly geocoded: {geocoded_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"  Saved: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geocode Dutch schools via Nominatim")
    parser.add_argument("--force", action="store_true", help="Ignore cache, re-geocode all")
    args = parser.parse_args()
    main(force=args.force)
