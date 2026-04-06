#!/usr/bin/env python3
"""
Stuttgart School Data Scraper
Builds the school master list from jedeschule.codefor.de CSV data + Nominatim geocoding.

Data Sources:
- jedeschule.codefor.de CSV dump (pre-geocoded, with contact info)
- Nominatim fallback geocoding for schools missing coordinates

The jedeschule data covers ~160 primary + ~180 secondary schools in Stuttgart
with coordinates, websites, emails, phone numbers, and principal names.

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import pandas as pd
import requests
import re
import math
import logging
import time
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

JEDESCHULE_CSV_URL = "https://jedeschule.codefor.de/csv-data/latest.csv"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Stuttgart school data pipeline)',
}

GEOCODE_DELAY = 1.1  # seconds between Nominatim requests

# Stuttgart PLZ range
STUTTGART_PLZ_PREFIX = '70'

# School type classification
PRIMARY_KEYWORDS = ['grundschule']
SECONDARY_KEYWORDS = [
    'gymnasium', 'realschule', 'gemeinschaftsschule', 'werkrealschule',
    'hauptschule', 'gesamtschule',
]
SBBZ_KEYWORDS = [
    'sonderpädagogisch', 'sbbz', 'förderschule', 'förderschwerpunkt',
    'bildungs- und beratungszentrum',
]
VOCATIONAL_KEYWORDS = [
    'berufsschule', 'berufsfachschule', 'berufskolleg', 'gewerbliche schule',
    'kaufmännische schule', 'hauswirtschaftliche schule', 'technische schule',
]
EXCLUDE_KEYWORDS = [
    'volkshochschule', 'musikschule', 'kunstschule', 'tanzschule',
    'fahrschule', 'sprachschule', 'nachhilfe', 'studienkolleg',
    'studienseminar', 'regierungspräsidium', 'schulamt', 'seminar',
    'verwaltung', 'dienststelle', 'kindergarten', 'kindertagesstätte',
    'abteilung', 'kolping berufsbildung',
]

# Stuttgart Stadtbezirke (23 districts) with PLZ mapping
STUTTGART_PLZ_BEZIRK = {
    '70173': 'Mitte', '70174': 'Mitte', '70178': 'Süd', '70176': 'West',
    '70180': 'Süd', '70182': 'Ost', '70184': 'Ost', '70186': 'Ost',
    '70188': 'Ost', '70190': 'Nord', '70191': 'Nord', '70192': 'Nord',
    '70193': 'West', '70195': 'Botnang', '70197': 'West', '70199': 'Süd',
    '70327': 'Untertürkheim', '70329': 'Obertürkheim',
    '70372': 'Bad Cannstatt', '70374': 'Bad Cannstatt', '70376': 'Bad Cannstatt',
    '70378': 'Mühlhausen', '70435': 'Zuffenhausen', '70437': 'Zuffenhausen',
    '70439': 'Stammheim', '70469': 'Feuerbach',
    '70499': 'Weilimdorf',
    '70563': 'Vaihingen', '70565': 'Vaihingen', '70567': 'Möhringen',
    '70569': 'Vaihingen', '70597': 'Degerloch',
    '70599': 'Plieningen', '70619': 'Sillenbuch',
    '70629': 'Birkach',
}


def download_jedeschule_data() -> pd.DataFrame:
    """Download and filter jedeschule CSV for Stuttgart schools."""
    cache_path = CACHE_DIR / "jedeschule_bw_stuttgart.csv"

    if cache_path.exists():
        age_days = (datetime.now().timestamp() - cache_path.stat().st_mtime) / 86400
        if age_days < 30:
            logger.info(f"Using cached jedeschule data ({age_days:.0f} days old)")
            return pd.read_csv(cache_path)

    logger.info("Downloading jedeschule CSV dump (52MB, may take a minute)...")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    try:
        r = requests.get(JEDESCHULE_CSV_URL, headers=HEADERS, timeout=300)
        r.raise_for_status()
        logger.info(f"Downloaded {len(r.content) / 1024 / 1024:.1f} MB")

        # Filter for Stuttgart entries
        lines = r.text.split('\n')
        header = lines[0]
        stuttgart_lines = [l for l in lines[1:] if 'Stuttgart' in l or 'stuttgart' in l]

        logger.info(f"Filtered {len(stuttgart_lines)} Stuttgart entries from {len(lines)} total")

        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(header + '\n')
            for l in stuttgart_lines:
                f.write(l + '\n')

        return pd.read_csv(cache_path)

    except Exception as e:
        if cache_path.exists():
            logger.warning(f"Download failed ({e}), using stale cache")
            return pd.read_csv(cache_path)
        raise


def classify_school(name: str, school_type_raw: str) -> Optional[str]:
    """Classify a school entry into primary/secondary/sbbz/vocational or None (exclude)."""
    name_lower = name.lower()

    # Exclude non-school entries
    if any(kw in name_lower for kw in EXCLUDE_KEYWORDS):
        return None

    # SBBZ (special education)
    if any(kw in name_lower for kw in SBBZ_KEYWORDS):
        return 'sbbz'

    # Vocational
    if any(kw in name_lower for kw in VOCATIONAL_KEYWORDS):
        return 'vocational'

    # Primary
    if school_type_raw == 'primaryEducation':
        return 'primary'
    if any(kw in name_lower for kw in PRIMARY_KEYWORDS):
        # Check it's not also secondary (Grund- und Werkrealschule)
        if any(kw in name_lower for kw in SECONDARY_KEYWORDS):
            return 'secondary'  # combined schools go to secondary
        return 'primary'

    # Secondary
    if school_type_raw in ('upperSecondaryEducation', 'lowerSecondaryEduction'):
        return 'secondary'
    if any(kw in name_lower for kw in SECONDARY_KEYWORDS):
        return 'secondary'

    # Waldorfschule → secondary (they span all grades)
    if 'waldorf' in name_lower:
        return 'secondary'

    # Generic "education" — try to classify by name
    if school_type_raw == 'education':
        if 'schule' in name_lower:
            return 'secondary'  # default for generic schools
        return None  # not clearly a school

    return None


def geocode_address(strasse: str, plz: str, city: str = "Stuttgart") -> Optional[Tuple[float, float]]:
    """Geocode an address using Nominatim."""
    query = f"{strasse}, {plz} {city}, Deutschland"

    try:
        r = requests.get(
            NOMINATIM_URL,
            params={'q': query, 'format': 'json', 'limit': 1},
            headers={'User-Agent': 'SchoolNossa/1.0 (stuttgart@schoolnossa.com)'},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            if data:
                return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        logger.warning(f"Geocoding failed for {query}: {e}")

    return None


def process_jedeschule_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process jedeschule data into primary and secondary school DataFrames."""
    logger.info(f"Processing {len(df)} jedeschule entries...")

    # Normalize PLZ
    df['zip_str'] = df['zip'].astype(str).str.split('.').str[0].str.strip()

    # Filter to Stuttgart PLZ (70xxx)
    df = df[df['zip_str'].str.startswith(STUTTGART_PLZ_PREFIX)].copy()
    logger.info(f"Stuttgart PLZ filter: {len(df)} entries")

    # Classify schools
    df['classification'] = df.apply(
        lambda r: classify_school(str(r.get('name', '')), str(r.get('school_type', ''))),
        axis=1
    )

    # Log classification results
    for cls, count in df['classification'].value_counts(dropna=False).items():
        logger.info(f"  {cls}: {count}")

    # Filter to primary and secondary only
    primary_df = df[df['classification'] == 'primary'].copy()
    secondary_df = df[df['classification'] == 'secondary'].copy()

    logger.info(f"Primary schools: {len(primary_df)}")
    logger.info(f"Secondary schools: {len(secondary_df)}")

    # Deduplicate by name (keep first, which typically has best data)
    primary_df = primary_df.drop_duplicates(subset=['name'], keep='first')
    secondary_df = secondary_df.drop_duplicates(subset=['name'], keep='first')

    logger.info(f"After dedup — Primary: {len(primary_df)}, Secondary: {len(secondary_df)}")

    # Transform to our schema
    primary_out = _transform_to_schema(primary_df, 'primary')
    secondary_out = _transform_to_schema(secondary_df, 'secondary')

    return primary_out, secondary_out


def _transform_to_schema(df: pd.DataFrame, school_type: str) -> pd.DataFrame:
    """Transform jedeschule data to SchoolNossa schema."""
    records = []

    for i, (_, row) in enumerate(df.iterrows()):
        name = str(row.get('name', '')).strip()
        plz = str(row.get('zip_str', '')).strip()

        # Clean website
        website = str(row.get('website', ''))
        if website == 'nan' or not website:
            website = ''
        elif website and not website.startswith('http'):
            website = f"https://{website}"

        # Clean email
        email = str(row.get('email', ''))
        if email == 'nan':
            email = ''

        # Clean phone
        phone = str(row.get('phone', ''))
        if phone == 'nan':
            phone = ''

        # Clean fax
        fax = str(row.get('fax', ''))
        if fax == 'nan':
            fax = ''

        # Director / Schulleitung
        director = str(row.get('director', ''))
        if director == 'nan':
            director = ''

        # Legal status
        legal = str(row.get('legal_status', ''))
        if 'privat' in legal.lower() or 'frei' in legal.lower():
            traegerschaft = 'Privat'
        else:
            traegerschaft = 'Öffentlich'

        # Also check name for private school indicators
        if any(kw in name.lower() for kw in ['freie', 'waldorf', 'montessori', 'privat']):
            traegerschaft = 'Privat'

        # District from PLZ
        ortsteil = STUTTGART_PLZ_BEZIRK.get(plz, '')

        # Coordinates
        lat = row.get('latitude')
        lon = row.get('longitude')
        if pd.isna(lat):
            lat = None
        if pd.isna(lon):
            lon = None

        # Determine more specific school type
        name_lower = name.lower()
        if 'gymnasium' in name_lower:
            schulart = 'Gymnasium'
        elif 'realschule' in name_lower:
            schulart = 'Realschule'
        elif 'gemeinschaftsschule' in name_lower:
            schulart = 'Gemeinschaftsschule'
        elif 'werkrealschule' in name_lower:
            schulart = 'Werkrealschule'
        elif 'grundschule' in name_lower or school_type == 'primary':
            schulart = 'Grundschule'
        elif 'waldorf' in name_lower:
            schulart = 'Waldorfschule'
        else:
            schulart = 'Schule'

        record = {
            'schulnummer': f"STG-{school_type[0].upper()}-{i + 1:04d}",
            'schulname': name,
            'school_type': school_type,
            'schulart': schulart,
            'strasse': str(row.get('address', '')).strip() if str(row.get('address', '')) != 'nan' else '',
            'plz': plz,
            'ort': 'Stuttgart',
            'ortsteil': ortsteil,
            'bundesland': 'Baden-Württemberg',
            'stadt': 'Stuttgart',
            'latitude': lat,
            'longitude': lon,
            'telefon': phone,
            'fax': fax,
            'email': email,
            'website': website,
            'schulleitung': director,
            'traegerschaft': traegerschaft,
            'data_source': 'jedeschule.codefor.de',
            'data_retrieved': datetime.now().strftime('%Y-%m-%d'),
        }
        records.append(record)

    return pd.DataFrame(records)


def geocode_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode schools that are missing coordinates."""
    missing = df[df['latitude'].isna() & (df['strasse'] != '')].copy()

    if len(missing) == 0:
        logger.info("All schools have coordinates")
        return df

    logger.info(f"Geocoding {len(missing)} schools missing coordinates...")

    geocode_cache_file = CACHE_DIR / "stuttgart_geocode_cache.json"
    cache = {}
    if geocode_cache_file.exists():
        try:
            cache = json.loads(geocode_cache_file.read_text(encoding='utf-8'))
        except Exception:
            pass

    geocoded = 0
    for idx, row in missing.iterrows():
        strasse = row['strasse']
        plz = row['plz']
        cache_key = f"{strasse}|{plz}"

        if cache_key in cache:
            lat, lon = cache[cache_key]
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lon
            continue

        coords = geocode_address(strasse, plz)
        if coords:
            df.at[idx, 'latitude'] = coords[0]
            df.at[idx, 'longitude'] = coords[1]
            cache[cache_key] = coords
            geocoded += 1

        time.sleep(GEOCODE_DELAY)

    geocode_cache_file.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    logger.info(f"Geocoded {geocoded} additional schools")

    return df


def save_output(df: pd.DataFrame, school_type: str):
    """Save school data to CSV."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RAW_DIR / f"stuttgart_{school_type}_schools.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame, school_type: str):
    """Print summary of scraped data."""
    print(f"\n{'=' * 70}")
    print(f"STUTTGART {school_type.upper()} SCHOOLS - SCRAPE COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")
    print(f"With coordinates: {df['latitude'].notna().sum()}/{len(df)} "
          f"({100 * df['latitude'].notna().sum() / len(df):.0f}%)")
    print(f"With phone: {(df['telefon'] != '').sum()}/{len(df)}")
    print(f"With website: {(df['website'] != '').sum()}/{len(df)}")
    print(f"With email: {(df['email'] != '').sum()}/{len(df)}")
    print(f"With principal: {(df['schulleitung'] != '').sum()}/{len(df)}")

    if 'schulart' in df.columns:
        print(f"\nBy school type:")
        for t, count in df['schulart'].value_counts().items():
            print(f"  {t}: {count}")

    if 'ortsteil' in df.columns:
        filled = (df['ortsteil'] != '').sum()
        print(f"\nDistrict coverage: {filled}/{len(df)}")
        if filled > 0:
            top = df[df['ortsteil'] != '']['ortsteil'].value_counts().head(10)
            for district, count in top.items():
                print(f"  {district}: {count}")

    if 'traegerschaft' in df.columns:
        print(f"\nBy operator:")
        for t, count in df['traegerschaft'].value_counts().items():
            print(f"  {t}: {count}")

    print(f"{'=' * 70}")


def scrape_and_save(school_type: str = 'primary') -> pd.DataFrame:
    """Main entry point for a single school type (called by orchestrator)."""
    # This gets called after main() has already done the work
    csv_path = RAW_DIR / f"stuttgart_{school_type}_schools.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)
    # If not yet scraped, run main
    primary, secondary = main()
    if school_type == 'primary':
        return primary
    return secondary


def main() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape both primary and secondary schools."""
    logger.info("=" * 60)
    logger.info("Starting Stuttgart School Data Scraper (jedeschule + Nominatim)")
    logger.info("=" * 60)

    # Download jedeschule data
    raw_df = download_jedeschule_data()

    # Process into primary/secondary
    primary_df, secondary_df = process_jedeschule_data(raw_df)

    # Geocode missing coordinates
    primary_df = geocode_missing(primary_df)
    secondary_df = geocode_missing(secondary_df)

    # Save
    save_output(primary_df, 'primary')
    save_output(secondary_df, 'secondary')

    # Summaries
    print_summary(primary_df, 'primary')
    print_summary(secondary_df, 'secondary')

    return primary_df, secondary_df


if __name__ == "__main__":
    main()
