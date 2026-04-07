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
3. Filters for school type (primary=Grundschulen or secondary)
4. Decodes WKB hex coordinates to lat/lon
5. Geocodes remaining schools without coordinates via Nominatim
6. Normalizes columns and outputs to intermediate/

Input: jedeschule.codefor.de CSV (web download)
Output: data_munich/intermediate/munich_{school_type}_schools.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
Updated: 2026-04-04 — Switched to jedeschule.codefor.de as primary source
Updated: 2026-04-07 — Added primary school (Grundschule) support
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

# Primary school types in Bavaria
PRIMARY_TYPE_PATTERNS = [
    'Grundschule', 'Grundschulen',
    'Volksschule',  # older term, sometimes still in data
]

# Keywords that indicate a private school operator in OSM data
PRIVATE_OPERATOR_KEYWORDS = [
    'ggmbh', 'gag', 'e.v.', 'ev ', 'stiftung', 'verein', 'schulverein',
    'phorms', 'montessori', 'waldorf', 'steiner',
]

# Keywords in school name that indicate private
PRIVATE_NAME_KEYWORDS = [
    'privat', 'privatschule', 'private ', 'freie schule', 'freie ',
    'montessori', 'waldorf', 'rudolf steiner', 'rudolf-steiner',
    'international school', 'phorms', 'lukas-schule', 'lukas-',
    'nymphenburger', 'isar gymnasium', 'isar grundschule',
    'bavarian international', 'european school', 'munich international',
    'obermenzinger', 'sabel', 'begemann',
    'parzival', 'christophorus', 'samuel-heinicke',
]

# Public operator keywords — exclude these from private detection
PUBLIC_OPERATOR_KEYWORDS = [
    'freistaat bayern', 'landeshauptstadt münchen', 'stadt münchen',
    'staatlich', 'städtisch',
]

OVERPASS_URL_SCRAPER = "https://overpass-api.de/api/interpreter"


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


def filter_schools_by_type(df: pd.DataFrame, school_type: str = 'secondary') -> pd.DataFrame:
    """Filter for primary or secondary school types."""
    if school_type == 'primary':
        patterns = PRIMARY_TYPE_PATTERNS
        label = 'primary'
    else:
        patterns = SECONDARY_TYPE_PATTERNS
        label = 'secondary'

    logger.info(f"Filtering for {label} school types...")

    mask = pd.Series(False, index=df.index)
    for pattern in patterns:
        mask |= df['school_type'].str.contains(pattern, case=False, na=False)

    # For primary: exclude combined schools that are primarily secondary
    # (e.g., "Grund- und Mittelschule" should appear in BOTH primary and secondary)
    if school_type == 'primary':
        # Include schools that mention Grundschule even if they also mention secondary types
        pass

    filtered = df[mask].copy()
    logger.info(f"Filtered from {len(df)} to {len(filtered)} {label} schools")

    if not filtered.empty:
        for st, count in filtered['school_type'].value_counts().items():
            logger.info(f"  - {st}: {count}")

    return filtered


def filter_secondary_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for secondary school types (backward compat)."""
    return filter_schools_by_type(df, 'secondary')


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


def save_outputs(df: pd.DataFrame, school_type: str = 'secondary'):
    """Save processed data."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    output_path = INTERMEDIATE_DIR / f"munich_{school_type}_schools.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path} ({len(df)} schools)")

    raw_path = RAW_DIR / f"munich_{school_type}_schools_raw.csv"
    df.to_csv(raw_path, index=False, encoding='utf-8-sig')


def print_summary(df: pd.DataFrame, school_type: str = 'secondary'):
    print(f"\n{'='*70}")
    print(f"MUNICH SCHOOL MASTER DATA SCRAPER - COMPLETE ({school_type.upper()})")
    print(f"{'='*70}")
    print(f"\nTotal {school_type} schools: {len(df)}")

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


def is_private_school(name: str, operator: str) -> bool:
    """Determine if a school is private based on OSM name and operator."""
    name_l = name.lower()
    op_l = operator.lower()

    # Exclude explicitly public schools
    if any(kw in op_l for kw in PUBLIC_OPERATOR_KEYWORDS):
        return False
    if any(kw in name_l for kw in ['staatlich', 'städtisch']):
        return False

    # Check operator for private indicators
    if any(kw in op_l for kw in PRIVATE_OPERATOR_KEYWORDS):
        return True

    # Check name for private indicators
    if any(kw in name_l for kw in PRIVATE_NAME_KEYWORDS):
        return True

    return False


def classify_osm_school_type(tags: dict) -> Optional[str]:
    """Classify an OSM school into primary or secondary based on tags and name."""
    name = tags.get('name', '').lower()
    isced = tags.get('isced:level', '')
    school_type_tag = tags.get('school:type', '').lower()

    # ISCED levels: 1 = primary, 2 = lower secondary, 3 = upper secondary
    if '1' in isced and '2' not in isced and '3' not in isced:
        return 'primary'
    if '2' in isced or '3' in isced:
        return 'secondary'

    # Name-based classification
    primary_kw = ['grundschule', 'primary school', 'elementary']
    secondary_kw = ['gymnasium', 'realschule', 'mittelschule', 'förderzentrum',
                    'fachoberschule', 'berufsoberschule', 'gesamtschule',
                    'secondary', 'high school', 'oberschule']

    for kw in primary_kw:
        if kw in name:
            return 'primary'
    for kw in secondary_kw:
        if kw in name:
            return 'secondary'

    # Schools with mixed ISCED (e.g., "1;2") — appear in both
    if '1' in isced:
        return 'both'

    return None


def fetch_osm_private_schools() -> pd.DataFrame:
    """Fetch private schools from OSM that aren't in jedeschule data."""
    cache_file = CACHE_DIR / "osm_private_schools.json"
    if cache_file.exists():
        age_days = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
        if age_days < 30:
            logger.info("Loading OSM private schools from cache...")
            with open(cache_file) as f:
                elements = json.load(f)
            return _parse_osm_schools(elements)

    logger.info("Fetching private schools from OSM Overpass API...")
    query = '''
    [out:json][timeout:60];
    area["name"="München"]["admin_level"="6"]->.searchArea;
    (
      node["amenity"="school"](area.searchArea);
      way["amenity"="school"](area.searchArea);
      relation["amenity"="school"](area.searchArea);
    );
    out center body;
    '''
    try:
        resp = requests.post(OVERPASS_URL_SCRAPER, data={'data': query}, timeout=60)
        resp.raise_for_status()
        elements = resp.json().get('elements', [])
        logger.info(f"Fetched {len(elements)} OSM schools")

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(elements, f)

        return _parse_osm_schools(elements)
    except Exception as e:
        logger.warning(f"OSM query failed: {e}")
        return pd.DataFrame()


def _parse_osm_schools(elements: list) -> pd.DataFrame:
    """Parse OSM elements into a DataFrame of private schools."""
    schools = []
    for elem in elements:
        tags = elem.get('tags', {})
        name = tags.get('name', '')
        operator = tags.get('operator', '')

        if not name:
            continue
        if not is_private_school(name, operator):
            continue

        # Get coordinates (center for ways/relations)
        lat = elem.get('lat') or elem.get('center', {}).get('lat')
        lon = elem.get('lon') or elem.get('center', {}).get('lon')

        school_type = classify_osm_school_type(tags)

        schools.append({
            'schulnummer': f"osm_{elem.get('id', '')}",
            'schulname': name,
            'strasse': tags.get('addr:street', ''),
            'plz': tags.get('addr:postcode', ''),
            'ort': tags.get('addr:city', 'München'),
            'school_type': name,  # will be overridden by classify
            'traegerschaft': 'privat',
            'traeger': operator or 'Privatschule',
            'website': tags.get('website', tags.get('contact:website', '')),
            'phone': tags.get('phone', tags.get('contact:phone', '')),
            'email': tags.get('email', tags.get('contact:email', '')),
            'latitude': lat,
            'longitude': lon,
            'data_source': 'OpenStreetMap (OSM)',
            '_osm_school_type': school_type,
        })

    df = pd.DataFrame(schools)
    if not df.empty:
        logger.info(f"Found {len(df)} private schools in OSM")
    return df


def merge_osm_private_schools(jedeschule_df: pd.DataFrame, school_type: str) -> pd.DataFrame:
    """Merge OSM private schools into the jedeschule dataset."""
    osm = fetch_osm_private_schools()
    if osm.empty:
        return jedeschule_df

    # Filter OSM schools by school type
    if school_type == 'primary':
        osm = osm[osm['_osm_school_type'].isin(['primary', 'both'])].copy()
    else:
        osm = osm[osm['_osm_school_type'].isin(['secondary', 'both'])].copy()

    if osm.empty:
        logger.info(f"No OSM private {school_type} schools to merge")
        return jedeschule_df

    # Deduplicate: remove OSM schools that are already in jedeschule (by name similarity)
    existing_names = set(jedeschule_df['schulname'].str.lower().str.strip())
    new_schools = []
    for _, row in osm.iterrows():
        osm_name = row['schulname'].lower().strip()
        # Check if any existing name is a substring match
        is_dup = any(osm_name in existing or existing in osm_name
                     for existing in existing_names if len(existing) > 10)
        if not is_dup:
            new_schools.append(row)

    if not new_schools:
        logger.info(f"All OSM private schools already in dataset")
        return jedeschule_df

    osm_new = pd.DataFrame(new_schools)
    osm_new = osm_new.drop(columns=['_osm_school_type'], errors='ignore')

    # Set proper school_type based on name
    for idx, row in osm_new.iterrows():
        name = row['schulname']
        if school_type == 'primary':
            osm_new.at[idx, 'school_type'] = 'Grundschule (privat)'
        else:
            # Try to classify more specifically
            name_l = name.lower()
            if 'gymnasium' in name_l:
                osm_new.at[idx, 'school_type'] = 'Gymnasium (privat)'
            elif 'realschule' in name_l:
                osm_new.at[idx, 'school_type'] = 'Realschule (privat)'
            elif 'mittelschule' in name_l:
                osm_new.at[idx, 'school_type'] = 'Mittelschule (privat)'
            elif 'förderzentrum' in name_l or 'förderschule' in name_l:
                osm_new.at[idx, 'school_type'] = 'Förderzentrum (privat)'
            else:
                osm_new.at[idx, 'school_type'] = f'{school_type.title()}schule (privat)'

    # Also tag existing jedeschule schools as public
    jedeschule_df = jedeschule_df.copy()
    if 'traegerschaft' not in jedeschule_df.columns:
        jedeschule_df['traegerschaft'] = None
    jedeschule_df['traegerschaft'] = jedeschule_df['traegerschaft'].fillna('öffentlich')

    logger.info(f"Adding {len(osm_new)} private {school_type} schools from OSM")
    for _, row in osm_new.iterrows():
        logger.info(f"  + {row['schulname']}")

    # Add metadata columns that OSM schools might be missing
    osm_new['bundesland'] = 'Bayern'
    osm_new['stadt'] = 'München'
    osm_new['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')

    merged = pd.concat([jedeschule_df, osm_new], ignore_index=True)
    return merged


def scrape_school_type(school_type: str = 'secondary') -> pd.DataFrame:
    """Scrape and process schools of a given type."""
    logger.info(f"Processing {school_type} schools...")

    ensure_directories()

    # Step 1: Download jedeschule data
    all_schools = download_jedeschule_csv()
    logger.info(f"Total German schools: {len(all_schools)}")

    # Step 2: Filter for München
    munich_schools = filter_munich_schools(all_schools)

    # Step 3: Filter for school type
    filtered = filter_schools_by_type(munich_schools, school_type)

    if len(filtered) == 0:
        logger.error(f"No {school_type} schools found in München!")
        return pd.DataFrame()

    # Step 4: Decode WKB coordinates
    filtered = decode_coordinates(filtered)

    # Step 5: Geocode remaining via Nominatim
    filtered = geocode_remaining(filtered)

    # Step 6: Normalize columns
    filtered = normalize_columns(filtered)

    # Step 7: Save
    save_outputs(filtered, school_type)

    # Print summary
    print_summary(filtered, school_type)

    return filtered


def main(school_type: str = 'secondary'):
    logger.info("=" * 60)
    logger.info(f"Starting Munich School Master Data Scraper ({school_type})")
    logger.info("=" * 60)

    try:
        result = scrape_school_type(school_type)
        if result.empty:
            sys.exit(1)
        logger.info("Munich School Master Data Scraper complete!")
        return result
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
