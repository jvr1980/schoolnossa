#!/usr/bin/env python3
"""
Phase 1: Dresden School Master Data Scraper

Downloads school data from Sächsische Schuldatenbank API.

Source: https://schuldatenbank.sachsen.de/api/v1/schools
Format: CSV, comma-separated, UTF-8
Coordinates: WGS84 (latitude/longitude)
Access: Free, no authentication required

This script:
1. Queries the Schuldatenbank API for all Dresden schools
2. Filters for general education schools (Grundschule, Oberschule, Gymnasium, etc.)
3. Normalizes columns for pipeline compatibility
4. Saves raw output for downstream enrichment phases

Author: Dresden School Data Pipeline
Created: 2026-04-07
"""

import requests
import pandas as pd
import logging
import sys
import io
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API endpoint
API_BASE = "https://schuldatenbank.sachsen.de/api/v1/schools"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/csv, */*',
}

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
RAW_DIR = DATA_DIR / "raw"

# School category and type mappings
# school_category_key=10 → Allgemeinbildende Schulen
SCHOOL_TYPE_KEYS = {
    '11': 'Grundschule',
    '12': 'Oberschule',
    '13': 'Gymnasium',
    '14': 'Förderschule',
    '15': 'Gemeinschaftsschule',
    '16': 'Abendgymnasium',
    '17': 'Abendoberschule',
}

# Which types are primary vs secondary
PRIMARY_TYPES = ['11']  # Grundschule
SECONDARY_TYPES = ['12', '13', '15']  # Oberschule, Gymnasium, Gemeinschaftsschule

LEGAL_STATUS_MAP = {
    '01': 'Öffentlich',
    '02': 'Privat',
}


def ensure_directories():
    """Create necessary directories."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_schools_csv(school_type_key: str = None) -> str:
    """Fetch school data from Schuldatenbank API as CSV."""
    params = {
        'format': 'csv',
        'filter_block_operator': 'AND',
        'order': 'name',
        'pre_registered': 'yes',
        'only_schools': 'yes',
        'address': 'Dresden',
        'school_category_key': '10',  # Allgemeinbildende Schulen
    }
    if school_type_key:
        params['school_type_key'] = school_type_key

    logger.info(f"Fetching schools from API (type_key={school_type_key or 'all'})...")

    response = requests.get(API_BASE, params=params, headers=HEADERS, timeout=60)
    response.raise_for_status()

    logger.info(f"Received {len(response.content):,} bytes")
    return response.text


def fetch_all_school_types() -> pd.DataFrame:
    """Fetch all general education school types from the API."""
    all_dfs = []

    for type_key, type_name in SCHOOL_TYPE_KEYS.items():
        try:
            csv_text = fetch_schools_csv(school_type_key=type_key)
            df = pd.read_csv(io.StringIO(csv_text), dtype=str)

            if len(df) > 0:
                df['school_type_key'] = type_key
                df['school_type_name'] = type_name
                logger.info(f"  {type_name} (key={type_key}): {len(df)} schools")
                all_dfs.append(df)
            else:
                logger.info(f"  {type_name} (key={type_key}): 0 schools")
        except Exception as e:
            logger.warning(f"  Failed to fetch {type_name}: {e}")

    if not all_dfs:
        raise RuntimeError("No school data fetched from API")

    combined = pd.concat(all_dfs, ignore_index=True)

    # Remove duplicates (schools with multiple buildings appear multiple times)
    if 'institution_key' in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=['institution_key'], keep='first')
        if before != len(combined):
            logger.info(f"Removed {before - len(combined)} duplicate entries (multi-building schools)")

    logger.info(f"Total: {len(combined)} unique schools")
    return combined


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API columns to pipeline-standard names."""
    df = df.copy()

    # Rename API columns to pipeline standard
    rename_map = {
        'institution_key': 'schulnummer',
        'name': 'schulname',
        'street': 'strasse',
        'postcode': 'plz',
        'community': 'ort',
        'phone': 'telefon',
        'fax': 'fax',
        'email': 'email',
        'homepage': 'website',
        'latitude': 'latitude',
        'longitude': 'longitude',
        'headmaster': 'schulleiter',
        'legal_status_key': 'legal_status_key',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Decode legal status
    if 'legal_status_key' in df.columns:
        df['traegerschaft'] = df['legal_status_key'].map(LEGAL_STATUS_MAP).fillna('Unbekannt')

    # Clean website URLs
    if 'website' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).strip() in ['', 'null', 'nan']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    # Convert coordinates to float
    for col in ['latitude', 'longitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Classify as primary or secondary
    if 'school_type_key' in df.columns:
        def classify(key):
            if key in PRIMARY_TYPES:
                return 'primary'
            elif key in SECONDARY_TYPES:
                return 'secondary'
            else:
                return 'other'
        df['school_category'] = df['school_type_key'].apply(classify)

    # Determine city from ort
    df['stadt'] = 'Dresden'

    # Add metadata
    df['data_source'] = 'Sächsische Schuldatenbank API'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')
    df['bundesland'] = 'Sachsen'

    return df


def save_outputs(df: pd.DataFrame):
    """Save processed data — all schools combined, plus primary/secondary splits."""
    logger.info("Saving output files...")

    # Save all schools
    all_path = RAW_DIR / "dresden_schools_raw.csv"
    df.to_csv(all_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {all_path} ({len(df)} schools)")

    # Split and save primary
    if 'school_category' in df.columns:
        primary_df = df[df['school_category'] == 'primary'].copy()
        primary_path = RAW_DIR / "dresden_primary_schools.csv"
        primary_df.to_csv(primary_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {primary_path} ({len(primary_df)} primary schools)")

        secondary_df = df[df['school_category'] == 'secondary'].copy()
        secondary_path = RAW_DIR / "dresden_secondary_schools.csv"
        secondary_df.to_csv(secondary_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {secondary_path} ({len(secondary_df)} secondary schools)")


def print_summary(df: pd.DataFrame):
    """Print summary statistics."""
    print("\n" + "=" * 70)
    print("DRESDEN SCHOOL MASTER DATA SCRAPER - COMPLETE")
    print("=" * 70)

    print(f"\nTotal schools: {len(df)}")

    if 'school_type_name' in df.columns:
        print("\nBy school type:")
        for st, count in df['school_type_name'].value_counts().items():
            print(f"  - {st}: {count}")

    if 'traegerschaft' in df.columns:
        print("\nBy operator:")
        for t, count in df['traegerschaft'].value_counts().items():
            print(f"  - {t}: {count}")

    if 'latitude' in df.columns:
        coord_count = df['latitude'].notna().sum()
        pct = 100 * coord_count / len(df) if len(df) > 0 else 0
        print(f"\nCoordinates: {coord_count}/{len(df)} ({pct:.0f}%)")

    if 'website' in df.columns:
        web_count = df['website'].notna().sum()
        pct = 100 * web_count / len(df) if len(df) > 0 else 0
        print(f"Websites: {web_count}/{len(df)} ({pct:.0f}%)")

    if 'school_category' in df.columns:
        print("\nBy category:")
        for cat, count in df['school_category'].value_counts().items():
            print(f"  - {cat}: {count}")

    print("\n" + "=" * 70)


def main():
    """Main function."""
    logger.info("=" * 60)
    logger.info("Starting Dresden School Master Data Scraper")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # Fetch all school types from API
        schools_df = fetch_all_school_types()

        # Save raw API response
        raw_api_path = RAW_DIR / "dresden_schuldatenbank_api_raw.csv"
        schools_df.to_csv(raw_api_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved raw API response: {raw_api_path}")

        # Normalize columns
        schools_df = normalize_columns(schools_df)

        # Save outputs
        save_outputs(schools_df)

        # Print summary
        print_summary(schools_df)

        logger.info("Dresden School Master Data Scraper complete!")
        return schools_df

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
