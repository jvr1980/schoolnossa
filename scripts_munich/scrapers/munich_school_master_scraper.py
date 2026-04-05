#!/usr/bin/env python3
"""
Phase 1: Munich School Master Data Scraper

Downloads and processes school data from jedeschule.codefor.de (primary source)
with coordinates already included (WKB format).

Data Source: https://jedeschule.codefor.de/csv-data/jedeschule-data-2025-01-04.csv
Format: CSV (comma-separated, UTF-8)
Coordinates: WKB hex format (EPSG:4326 Point with SRID)
License: CC0 (public domain)

This script:
1. Downloads jedeschule.codefor.de CSV (31k+ German schools with coords)
2. Filters for München (city field)
3. Filters for secondary school types (Gymnasien, Realschulen, Mittelschulen, etc.)
4. Decodes WKB hex coordinates to lat/lon
5. Geocodes remaining schools without coordinates via Nominatim
6. Normalizes columns and outputs to intermediate/

Input: jedeschule.codefor.de CSV (web download)
Output: data_munich/intermediate/munich_secondary_schools.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
Updated: 2026-04-04 — Switched to jedeschule.codefor.de as primary source
"""

import requests
import pandas as pd
import numpy as np
import logging
import sys
import struct
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# jedeschule.codefor.de — weekly scraped school data with coordinates
JEDESCHULE_CSV_URL = "https://jedeschule.codefor.de/csv-data/jedeschule-data-2025-01-04.csv"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Munich school data pipeline, educational project)',
}

# Secondary school types in Bavaria (as they appear in jedeschule data)
SECONDARY_TYPE_PATTERNS = [
    'Gymnasium', 'Gymnasien',
    'Realschule', 'Realschulen',
    'Mittelschule', 'Mittelschulen',
    'Wirtschaftsschule', 'Wirtschaftsschulen',
    'Förderzentrum', 'Förderzentren', 'Förderschule',
    'Waldorfschule', 'Freie Waldorfschule',
    'Gesamtschule',
    'Fachoberschule', 'Fachoberschulen',
    'Berufsoberschule', 'Berufsoberschulen',
]


def ensure_directories():
    for d in [RAW_DIR, INTERMEDIATE_DIR, CACHE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def download_jedeschule_csv() -> pd.DataFrame:
    """Download and parse jedeschule.codefor.de CSV."""
    cache_file = CACHE_DIR / "jedeschule_data.csv"

    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 30 * 86400:  # 30-day cache
            logger.info("Loading jedeschule.codefor.de data from cache...")
            return pd.read_csv(cache_file, dtype=str)

    logger.info(f"Downloading jedeschule.codefor.de CSV from {JEDESCHULE_CSV_URL}")
    try:
        response = requests.get(JEDESCHULE_CSV_URL, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.1f} MB")

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'wb') as f:
            f.write(response.content)

        return pd.read_csv(cache_file, dtype=str)
    except requests.RequestException as e:
        logger.error(f"Failed to download: {e}")
        raise


def decode_wkb_point(wkb_hex: str) -> Optional[tuple]:
    """Decode WKB hex Point to (latitude, longitude).

    WKB format for Point with SRID:
    - 1 byte: byte order (01 = little-endian)
    - 4 bytes: geometry type (01000020 = Point with SRID)
    - 4 bytes: SRID (E6100000 = 4326)
    - 8 bytes: X (latitude in this dataset's convention)
    - 8 bytes: Y (longitude)
    """
    if not wkb_hex or wkb_hex == 'nan' or len(wkb_hex) < 50:
        return None
    try:
        wkb = bytes.fromhex(wkb_hex)
        # X and Y are at offset 9 and 17 (after byte_order + type + SRID)
        x = struct.unpack('<d', wkb[9:17])[0]
        y = struct.unpack('<d', wkb[17:25])[0]
        # In this dataset: X=lat, Y=lon (verified empirically)
        lat, lon = x, y
        # Sanity check: should be in Germany
        if 47.0 <= lat <= 55.0 and 5.0 <= lon <= 16.0:
            return (round(lat, 6), round(lon, 6))
    except (ValueError, struct.error):
        pass
    return None


def filter_munich_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for München schools using city name AND PLZ prefix.

    The jedeschule 'city' field includes suburban schools under München's
    Regierungsbezirk. We tighten to PLZ 80xxx/81xxx (actual Munich city postal codes)
    combined with city name to avoid including distant suburbs.
    """
    logger.info("Filtering for München schools...")

    city_mask = df['city'].str.contains('München', case=False, na=False)
    plz_mask = df['zip'].astype(str).str.strip().str.startswith(('80', '81'))

    # Require BOTH city name match AND Munich PLZ prefix
    mask = city_mask & plz_mask
    filtered = df[mask].copy()

    # Also include schools explicitly named "München" even if PLZ is different
    name_mask = df['name'].str.contains('München', case=False, na=False) & ~mask
    if name_mask.any():
        extra = df[name_mask]
        logger.info(f"  Adding {len(extra)} schools with 'München' in name but non-80/81 PLZ")
        filtered = pd.concat([filtered, extra], ignore_index=True)

    logger.info(f"Filtered from {len(df)} to {len(filtered)} München schools")
    return filtered


def filter_secondary_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for secondary school types."""
    logger.info("Filtering for secondary school types...")

    mask = pd.Series(False, index=df.index)
    for pattern in SECONDARY_TYPE_PATTERNS:
        mask |= df['school_type'].str.contains(pattern, case=False, na=False)

    filtered = df[mask].copy()
    logger.info(f"Filtered from {len(df)} to {len(filtered)} secondary schools")

    if not filtered.empty:
        for st, count in filtered['school_type'].value_counts().items():
            logger.info(f"  - {st}: {count}")

    return filtered


def decode_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Decode WKB hex coordinates to lat/lon."""
    logger.info("Decoding WKB coordinates...")

    df = df.copy()
    df['latitude'] = None
    df['longitude'] = None

    decoded = 0
    for idx, row in df.iterrows():
        wkb_hex = row.get('location', '')
        if pd.isna(wkb_hex):
            continue
        coords = decode_wkb_point(str(wkb_hex))
        if coords:
            df.at[idx, 'latitude'] = coords[0]
            df.at[idx, 'longitude'] = coords[1]
            decoded += 1

    logger.info(f"Decoded coordinates for {decoded}/{len(df)} schools")
    return df


def geocode_remaining(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode schools without coordinates via Nominatim."""
    missing = df['latitude'].isna()
    missing_count = missing.sum()

    if missing_count == 0:
        logger.info("All schools have coordinates")
        return df

    logger.info(f"Geocoding {missing_count} schools via Nominatim...")

    geocode_cache_file = CACHE_DIR / "geocode_cache.json"
    geocode_cache = {}
    if geocode_cache_file.exists():
        with open(geocode_cache_file) as f:
            geocode_cache = json.load(f)

    geocoded = 0
    for idx in df[missing].index:
        row = df.loc[idx]
        address = str(row.get('address', '')).strip()
        zip_code = str(row.get('zip', '')).strip()
        city = str(row.get('city', 'München')).strip()

        cache_key = f"{address}|{zip_code}|{city}"
        if cache_key in geocode_cache:
            coords = geocode_cache[cache_key]
            if coords:
                df.at[idx, 'latitude'] = coords['lat']
                df.at[idx, 'longitude'] = coords['lon']
                geocoded += 1
            continue

        query = f"{address}, {zip_code} {city}, Germany"
        try:
            response = requests.get(
                NOMINATIM_URL,
                params={'q': query, 'format': 'json', 'limit': 1, 'countrycodes': 'de'},
                headers={'User-Agent': 'SchoolNossa/1.0 (educational research)'},
                timeout=10
            )
            response.raise_for_status()
            results = response.json()
            if results:
                coords = {'lat': float(results[0]['lat']), 'lon': float(results[0]['lon'])}
                geocode_cache[cache_key] = coords
                df.at[idx, 'latitude'] = coords['lat']
                df.at[idx, 'longitude'] = coords['lon']
                geocoded += 1
            else:
                geocode_cache[cache_key] = None
        except Exception as e:
            logger.debug(f"Geocoding failed for {query}: {e}")
            geocode_cache[cache_key] = None

        time.sleep(1.1)  # Nominatim rate limit

    with open(geocode_cache_file, 'w') as f:
        json.dump(geocode_cache, f)

    logger.info(f"Geocoded {geocoded} additional schools ({missing_count - geocoded} still missing)")
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize jedeschule columns to pipeline standard."""
    df = df.copy()

    # Rename jedeschule columns to pipeline standard
    rename_map = {
        'id': 'schulnummer',
        'name': 'schulname',
        'address': 'strasse',
        'zip': 'plz',
        'city': 'ort',
        'school_type': 'school_type',
        'legal_status': 'traegerschaft',
        'provider': 'traeger',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Clean website URLs
    if 'website' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).strip() in ['', 'nan']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    # Build full address
    if 'strasse' in df.columns and 'plz' in df.columns:
        df['adresse'] = df['strasse'].fillna('') + ', ' + df['plz'].fillna('') + ' ' + df['ort'].fillna('München')

    # Metadata
    df['data_source'] = 'jedeschule.codefor.de (CC0, scraped from km.bayern.de)'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')
    df['bundesland'] = 'Bayern'
    df['stadt'] = 'München'

    # Drop the raw JSON column and WKB location to save space
    drop_cols = ['raw', 'location', 'update_timestamp', 'address2']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

    return df


def save_outputs(df: pd.DataFrame):
    """Save processed data."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    output_path = INTERMEDIATE_DIR / "munich_secondary_schools.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path} ({len(df)} schools)")

    raw_path = RAW_DIR / "munich_secondary_schools_raw.csv"
    df.to_csv(raw_path, index=False, encoding='utf-8-sig')


def print_summary(df: pd.DataFrame):
    print(f"\n{'='*70}")
    print("MUNICH SCHOOL MASTER DATA SCRAPER - COMPLETE")
    print(f"{'='*70}")
    print(f"\nTotal secondary schools: {len(df)}")

    if 'school_type' in df.columns:
        print("\nBy school type:")
        for st, count in df['school_type'].value_counts().items():
            print(f"  - {st}: {count}")

    if 'latitude' in df.columns:
        coord_count = df['latitude'].notna().sum()
        pct = 100 * coord_count / len(df) if len(df) > 0 else 0
        print(f"\nCoordinates: {coord_count}/{len(df)} ({pct:.0f}%)")

    if 'website' in df.columns:
        web_count = df['website'].notna().sum()
        pct = 100 * web_count / len(df) if len(df) > 0 else 0
        print(f"Websites: {web_count}/{len(df)} ({pct:.0f}%)")

    if 'email' in df.columns:
        email_count = df['email'].notna().sum()
        pct = 100 * email_count / len(df) if len(df) > 0 else 0
        print(f"Emails: {email_count}/{len(df)} ({pct:.0f}%)")

    if 'phone' in df.columns:
        phone_count = df['phone'].notna().sum()
        pct = 100 * phone_count / len(df) if len(df) > 0 else 0
        print(f"Phone: {phone_count}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'='*70}")


def main():
    logger.info("=" * 60)
    logger.info("Starting Munich School Master Data Scraper")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # Step 1: Download jedeschule data
        all_schools = download_jedeschule_csv()
        logger.info(f"Total German schools: {len(all_schools)}")

        # Step 2: Filter for München
        munich_schools = filter_munich_schools(all_schools)

        # Step 3: Filter for secondary schools
        secondary = filter_secondary_schools(munich_schools)

        if len(secondary) == 0:
            logger.error("No secondary schools found in München!")
            sys.exit(1)

        # Step 4: Decode WKB coordinates
        secondary = decode_coordinates(secondary)

        # Step 5: Geocode remaining via Nominatim
        secondary = geocode_remaining(secondary)

        # Step 6: Normalize columns
        secondary = normalize_columns(secondary)

        # Step 7: Save
        save_outputs(secondary)

        # Print summary
        print_summary(secondary)

        logger.info("Munich School Master Data Scraper complete!")
        return secondary

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
