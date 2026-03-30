#!/usr/bin/env python3
"""
Frankfurt School Master Data Scraper
Downloads and processes school data from Hessisches Statistisches Landesamt.

Primary source: Verzeichnis 6 (Excel) — all Hessen schools
Secondary source: jedeschule.codefor.de (CSV) — pre-geocoded coordinates
Fallback geocoding: Nominatim/OpenStreetMap

This script:
1. Downloads the Hessen school directory Excel (Verzeichnis 6)
2. Parses multi-level headers and student count columns
3. Filters for Frankfurt am Main (Landkreis 412)
4. Downloads jedeschule.codefor.de CSV for coordinates
5. Geocodes remaining schools via Nominatim
6. Splits into primary and secondary school outputs

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
"""

import pandas as pd
import requests
import time
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_frankfurt"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Data source URLs
HESSEN_VERZ6_URL = "https://statistik.hessen.de/sites/statistik.hessen.de/files/2025-09/verz-6_25_0.xlsx"
JEDESCHULE_CSV_URL = "https://jedeschule.codefor.de/csv-data/jedeschule-data-2026-03-28.csv"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Frankfurt school data pipeline, educational project)',
}

# Frankfurt administrative codes
FRANKFURT_LANDKREIS = '412'
FRANKFURT_AGS_PREFIX = '06412'

# School type classification
# From Hessen Verzeichnis 6 column structure:
# Primary = Grundschule student count > 0 and no secondary students
# Secondary = Gymnasium, Realschule, Hauptschule, IGS, Mittelstufenschule students > 0
PRIMARY_TYPES = ['Grundschule']
SECONDARY_TYPES = ['Gymnasium', 'Realschule', 'Hauptschule', 'Gesamtschule',
                   'Integrierte Gesamtschule', 'Kooperative Gesamtschule',
                   'Mittelstufenschule', 'Förderstufe']


def ensure_directories():
    """Create necessary directories."""
    for d in [RAW_DIR, INTERMEDIATE_DIR, CACHE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def download_hessen_verzeichnis() -> pd.DataFrame:
    """Download and parse Hessen Schulverzeichnis 6 (Excel)."""
    cache_file = CACHE_DIR / "hessen_verz6.xlsx"

    # Check cache (7-day validity)
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 7 * 86400:
            logger.info("Loading Hessen Verzeichnis 6 from cache...")
            return _parse_verzeichnis_excel(cache_file)

    logger.info(f"Downloading Hessen Verzeichnis 6 from {HESSEN_VERZ6_URL}")
    try:
        response = requests.get(HESSEN_VERZ6_URL, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content) / 1024:.0f} KB")

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'wb') as f:
            f.write(response.content)

        return _parse_verzeichnis_excel(cache_file)
    except requests.RequestException as e:
        logger.error(f"Failed to download Hessen Verzeichnis 6: {e}")
        raise


def _parse_verzeichnis_excel(filepath: Path) -> pd.DataFrame:
    """Parse the multi-header Excel file from Hessen Statistik."""
    logger.info(f"Parsing Excel file: {filepath.name}")

    # Read with header=None to handle multi-level headers manually
    # The sheet "Schulverzeichnis" has row 0 = main headers, row 1 = sub-headers
    try:
        df_raw = pd.read_excel(filepath, sheet_name='Schulverzeichnis', header=None, dtype=str)
    except Exception:
        # Try first sheet if 'Schulverzeichnis' not found
        df_raw = pd.read_excel(filepath, sheet_name=0, header=None, dtype=str)

    logger.info(f"Raw shape: {df_raw.shape}")

    # Identify header rows: rows 0 and 1 are headers, data from row 2
    # Build column names from the two header rows
    row0 = df_raw.iloc[0].fillna('')
    row1 = df_raw.iloc[1].fillna('')

    columns = []
    for i in range(len(row0)):
        h0 = str(row0.iloc[i]).strip()
        h1 = str(row1.iloc[i]).strip()
        if h0 and h1:
            col = f"{h0}_{h1}"
        elif h0:
            col = h0
        elif h1:
            col = h1
        else:
            col = f"col_{i}"
        # Clean up
        col = col.replace('\n', ' ').replace('  ', ' ').strip()
        columns.append(col)

    # Drop header rows
    df = df_raw.iloc[2:].copy()
    df.columns = columns
    df = df.reset_index(drop=True)

    logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
    logger.info(f"First 10 columns: {columns[:10]}")

    return df


def filter_frankfurt(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for Frankfurt am Main schools."""
    logger.info("Filtering for Frankfurt am Main...")

    # Try multiple approaches to find Frankfurt schools
    filtered = pd.DataFrame()

    # Approach 1: Landkreis column
    for col in df.columns:
        if 'landkreis' in col.lower() or 'kreis' in col.lower():
            mask = df[col].astype(str).str.strip() == FRANKFURT_LANDKREIS
            if mask.any():
                filtered = df[mask].copy()
                logger.info(f"Filtered by '{col}' == {FRANKFURT_LANDKREIS}: {len(filtered)} schools")
                break

    # Approach 2: Schulort contains Frankfurt
    if filtered.empty:
        for col in df.columns:
            if 'ort' in col.lower() or 'stadt' in col.lower():
                mask = df[col].astype(str).str.contains('Frankfurt', case=False, na=False)
                if mask.any():
                    filtered = df[mask].copy()
                    logger.info(f"Filtered by '{col}' contains 'Frankfurt': {len(filtered)} schools")
                    break

    if filtered.empty:
        logger.error("Could not filter Frankfurt schools! Check column names.")
        logger.info(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    return filtered


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and extract key fields."""
    logger.info("Normalizing columns...")
    df = df.copy()

    # Map known columns to standardized names
    col_map = {}
    for col in df.columns:
        cl = col.lower()
        if 'schulnummer' in cl or 'schul-nr' in cl or col.lower().startswith('schulnr'):
            col_map[col] = 'schulnummer'
        elif 'name der schule' in cl or 'schulname' in cl:
            col_map[col] = 'schulname'
        elif cl.startswith('plz') or 'postleitzahl' in cl:
            col_map[col] = 'plz'
        elif 'schulort' in cl:
            col_map[col] = 'ort'
        elif 'straße' in cl or 'strasse' in cl or 'hausnummer' in cl:
            col_map[col] = 'strasse'
        elif 'telefonvorwahl' in cl:
            col_map[col] = 'telefonvorwahl'
        elif 'telefonnummer' in cl:
            col_map[col] = 'telefonnummer'
        elif cl == 'fax' or 'fax' in cl:
            if 'fax' not in col_map.values():
                col_map[col] = 'fax'
        elif 'e-mail' in cl or 'email' in cl:
            col_map[col] = 'email'
        elif 'rechtsform' in cl:
            col_map[col] = 'rechtsform'
        elif 'gesamtschule' in cl and 'schüler' not in cl:
            col_map[col] = 'gesamtschultyp'

    df = df.rename(columns=col_map)
    logger.info(f"Renamed {len(col_map)} columns")

    # Build phone number
    if 'telefonvorwahl' in df.columns and 'telefonnummer' in df.columns:
        df['telefon'] = df['telefonvorwahl'].fillna('') + df['telefonnummer'].fillna('')
        df['telefon'] = df['telefon'].str.strip()
        df.loc[df['telefon'] == '', 'telefon'] = None

    # Determine school operator
    if 'rechtsform' in df.columns:
        df['traegerschaft'] = df['rechtsform'].map({
            '1': 'Öffentlich',
            '2': 'Privat',
        }).fillna('Unbekannt')

    # Extract total student count
    # Look for the "insgesamt" column
    total_col = None
    ndh_col = None
    for col in df.columns:
        cl = col.lower()
        if 'insgesamt' in cl and 'schüler' in cl:
            total_col = col
        elif 'nichtdeutsch' in cl or 'ndh' in cl or 'herkunftssprache' in cl:
            ndh_col = col

    if total_col:
        df['schueler_gesamt'] = pd.to_numeric(df[total_col], errors='coerce')
        logger.info(f"Total students column: '{total_col}'")

    if ndh_col:
        df['ndh_count'] = pd.to_numeric(df[ndh_col], errors='coerce')
        logger.info(f"ndH column: '{ndh_col}'")

    # Classify school type from student count columns
    df = _classify_school_type(df)

    # Clean PLZ
    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.zfill(5)

    # Add metadata
    df['stadt'] = 'Frankfurt am Main'
    df['bundesland'] = 'Hessen'
    df['data_source'] = 'Hessisches Statistisches Landesamt Verzeichnis 6'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')

    return df


def _classify_school_type(df: pd.DataFrame) -> pd.DataFrame:
    """Classify each school as primary, secondary, or other based on student count columns."""
    logger.info("Classifying school types from student count columns...")

    df = df.copy()

    # Find columns containing student counts for each type
    grundschule_cols = []
    secondary_cols = []
    foerderschule_cols = []

    for col in df.columns:
        cl = col.lower()
        if 'grundschule' in cl or 'eingangsstufe' in cl:
            grundschule_cols.append(col)
        elif any(t in cl for t in ['gymnasium', 'gymnasien', 'realschule', 'hauptschule',
                                     'gesamtschule', 'mittelstufenschule', 'förderstufe',
                                     'mittelstufe', 'oberstufe']):
            if 'förderschule' not in cl:
                secondary_cols.append(col)
        elif 'förderschule' in cl or 'foerderschule' in cl:
            foerderschule_cols.append(col)

    logger.info(f"Found student count columns: {len(grundschule_cols)} primary, {len(secondary_cols)} secondary")

    # Calculate totals per type
    def safe_sum(row, cols):
        total = 0
        for c in cols:
            val = pd.to_numeric(row.get(c, 0), errors='coerce')
            if pd.notna(val):
                total += val
        return total

    df['_grundschule_students'] = df.apply(lambda r: safe_sum(r, grundschule_cols), axis=1)
    df['_secondary_students'] = df.apply(lambda r: safe_sum(r, secondary_cols), axis=1)
    df['_foerderschule_students'] = df.apply(lambda r: safe_sum(r, foerderschule_cols), axis=1)

    # Classify
    def classify(row):
        gs = row['_grundschule_students']
        sec = row['_secondary_students']
        fs = row['_foerderschule_students']
        name = str(row.get('schulname', '')).lower()
        gst = row.get('gesamtschultyp', '')

        if gs > 0 and sec == 0:
            return 'Grundschule'
        elif sec > 0 and gs == 0:
            return _classify_secondary_type_from_name(name, gst)
        elif gs > 0 and sec > 0:
            return 'Gesamtschule'  # Combined primary+secondary
        elif fs > 0:
            return 'Förderschule'
        else:
            # Fallback: classify from school name
            return _classify_secondary_type_from_name(name, gst)

    df['school_type'] = df.apply(classify, axis=1)

    # Clean up temp columns
    df = df.drop(columns=['_grundschule_students', '_secondary_students', '_foerderschule_students'])

    type_counts = df['school_type'].value_counts()
    for t, c in type_counts.items():
        logger.info(f"  {t}: {c}")

    return df


def _classify_secondary_type_from_name(name: str, gesamtschultyp) -> str:
    """Classify school type from name and gesamtschul indicator."""
    name = name.lower()

    # Check Gesamtschule type indicator (1=KGS, 2=IGS)
    gst_str = str(gesamtschultyp).strip()
    if gst_str in ('1', '1.0'):
        return 'Gesamtschule'  # KGS
    elif gst_str in ('2', '2.0'):
        return 'Gesamtschule'  # IGS

    # Check name
    if 'gymnasium' in name:
        return 'Gymnasium'
    elif 'igs' in name or 'gesamtschule' in name:
        return 'Gesamtschule'
    elif 'realschule' in name:
        return 'Realschule'
    elif 'hauptschule' in name:
        return 'Hauptschule'
    elif 'mittelstufenschule' in name:
        return 'Mittelstufenschule'
    elif 'oberstufe' in name or 'kolleg' in name:
        return 'Gymnasium'  # Oberstufe / Abendgymnasium
    elif 'schule' in name:
        # Many Frankfurt secondary schools just have "Schule" in the name
        # (e.g., Wöhlerschule, Elisabethenschule — these are Gymnasien)
        return 'Weiterführende Schule'
    return 'Sonstige'


def _classify_secondary_type(row: pd.Series) -> str:
    """Determine the specific secondary school type."""
    for col in row.index:
        cl = col.lower()
        raw = row.get(col, 0)
        # Handle Series/array values by taking first element
        if hasattr(raw, '__len__') and not isinstance(raw, str):
            try:
                raw = raw.iloc[0] if hasattr(raw, 'iloc') else raw[0]
            except (IndexError, KeyError):
                continue
        val = pd.to_numeric(raw, errors='coerce')
        if pd.isna(val) or val == 0:
            continue
        if 'gymnasium' in cl and 'oberstufe' not in cl:
            return 'Gymnasium'
        elif 'realschule' in cl:
            return 'Realschule'
        elif 'hauptschule' in cl:
            return 'Hauptschule'
        elif 'gesamtschule' in cl:
            return 'Gesamtschule'
        elif 'mittelstufenschule' in cl:
            return 'Mittelstufenschule'
    return 'Weiterführende Schule'


def add_coordinates_jedeschule(df: pd.DataFrame) -> pd.DataFrame:
    """Add coordinates from jedeschule.codefor.de CSV."""
    cache_file = CACHE_DIR / "jedeschule_frankfurt.csv"

    # Check cache
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 7 * 86400:
            logger.info("Loading jedeschule coordinates from cache...")
            js_df = pd.read_csv(cache_file)
            return _merge_coordinates(df, js_df)

    logger.info(f"Downloading jedeschule.codefor.de data...")
    try:
        # Download full CSV (large file — use streaming)
        response = requests.get(JEDESCHULE_CSV_URL, headers=HEADERS, timeout=300, stream=True)
        response.raise_for_status()

        # Read and filter for Frankfurt in chunks
        chunks = []
        for chunk in pd.read_csv(
            response.raw, chunksize=10000, encoding='utf-8',
            dtype=str, on_bad_lines='skip'
        ):
            frankfurt_chunk = chunk[
                chunk['city'].str.contains('Frankfurt', case=False, na=False) &
                chunk['zip'].str.startswith('6', na=False)
            ]
            if len(frankfurt_chunk) > 0:
                chunks.append(frankfurt_chunk)

        if chunks:
            js_df = pd.concat(chunks, ignore_index=True)
            logger.info(f"Found {len(js_df)} Frankfurt schools in jedeschule data")

            # Cache
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            js_df.to_csv(cache_file, index=False)

            return _merge_coordinates(df, js_df)
        else:
            logger.warning("No Frankfurt schools found in jedeschule data")
            return df

    except Exception as e:
        logger.warning(f"Could not download jedeschule data: {e}")
        return df


def _merge_coordinates(df: pd.DataFrame, js_df: pd.DataFrame) -> pd.DataFrame:
    """Merge coordinates from jedeschule data into main dataframe."""
    logger.info("Merging jedeschule coordinates...")

    df = df.copy()
    if 'latitude' not in df.columns:
        df['latitude'] = None
    if 'longitude' not in df.columns:
        df['longitude'] = None

    matched = 0
    for idx, row in df.iterrows():
        if pd.notna(row.get('latitude')):
            continue

        name = str(row.get('schulname', '')).lower().strip()
        plz = str(row.get('plz', '')).strip()

        # Try matching by name + PLZ
        for _, js_row in js_df.iterrows():
            js_name = str(js_row.get('name', '')).lower().strip()
            js_plz = str(js_row.get('zip', '')).strip()

            if plz == js_plz and (name in js_name or js_name in name):
                lat = pd.to_numeric(js_row.get('latitude'), errors='coerce')
                lon = pd.to_numeric(js_row.get('longitude'), errors='coerce')
                if pd.notna(lat) and pd.notna(lon) and lat != 0 and lon != 0:
                    df.at[idx, 'latitude'] = lat
                    df.at[idx, 'longitude'] = lon
                    matched += 1
                    break

    logger.info(f"Matched {matched} coordinates from jedeschule data")
    return df


def geocode_remaining(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode schools without coordinates using Nominatim."""
    missing = df[df['latitude'].isna()].copy()
    if len(missing) == 0:
        logger.info("All schools have coordinates")
        return df

    logger.info(f"Geocoding {len(missing)} schools via Nominatim...")

    geocode_cache_file = CACHE_DIR / "frankfurt_geocode_cache.json"
    cache = {}
    if geocode_cache_file.exists():
        with open(geocode_cache_file, 'r') as f:
            cache = json.load(f)

    geocoded = 0
    for idx, row in missing.iterrows():
        strasse = str(row.get('strasse', '')).strip()
        plz = str(row.get('plz', '')).strip()
        ort = 'Frankfurt am Main'

        # Build query
        query = f"{strasse}, {plz} {ort}, Germany"
        cache_key = query.lower()

        if cache_key in cache:
            lat, lon = cache[cache_key]
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lon
            geocoded += 1
            continue

        try:
            response = requests.get(
                NOMINATIM_URL,
                params={
                    'q': query,
                    'format': 'json',
                    'limit': 1,
                    'countrycodes': 'de',
                },
                headers={'User-Agent': 'SchoolNossa/1.0 (educational project)'},
                timeout=10
            )
            response.raise_for_status()
            results = response.json()

            if results:
                lat = float(results[0]['lat'])
                lon = float(results[0]['lon'])
                df.at[idx, 'latitude'] = lat
                df.at[idx, 'longitude'] = lon
                cache[cache_key] = [lat, lon]
                geocoded += 1
            else:
                logger.warning(f"  No geocoding result for: {query}")

            time.sleep(1.1)  # Nominatim rate limit

        except Exception as e:
            logger.warning(f"  Geocoding failed for {query}: {e}")
            time.sleep(1.1)

    # Save cache
    with open(geocode_cache_file, 'w') as f:
        json.dump(cache, f)

    logger.info(f"Geocoded {geocoded} additional schools")
    total_coords = df['latitude'].notna().sum()
    logger.info(f"Total with coordinates: {total_coords}/{len(df)} ({100*total_coords/len(df):.0f}%)")

    return df


def split_by_school_type(df: pd.DataFrame):
    """Split into primary and secondary school DataFrames."""
    logger.info("Splitting by school type...")

    primary_df = df[df['school_type'] == 'Grundschule'].copy()
    secondary_types = [
        'Gymnasium', 'Realschule', 'Hauptschule', 'Gesamtschule',
        'Mittelstufenschule', 'Weiterführende Schule'
    ]
    secondary_mask = df['school_type'].isin(secondary_types)
    secondary_df = df[secondary_mask].copy()

    # Schools that are both (e.g., IGS with Grundstufe) — include in both
    both_mask = df['school_type'] == 'Gesamtschule'
    # Check if these also have Grundschule students — if so, also in primary
    # For now, Gesamtschule goes to secondary only

    logger.info(f"Primary (Grundschulen): {len(primary_df)}")
    logger.info(f"Secondary: {len(secondary_df)}")

    if not secondary_df.empty:
        for st, count in secondary_df['school_type'].value_counts().items():
            logger.info(f"  - {st}: {count}")

    return primary_df, secondary_df


def save_outputs(all_df, primary_df, secondary_df):
    """Save processed data."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Save all schools
    all_path = RAW_DIR / "frankfurt_schools_all.csv"
    all_df.to_csv(all_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {all_path} ({len(all_df)} schools)")

    # Save primary
    primary_path = RAW_DIR / "frankfurt_primary_schools.csv"
    primary_df.to_csv(primary_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {primary_path} ({len(primary_df)} schools)")

    # Save secondary
    secondary_path = RAW_DIR / "frankfurt_secondary_schools.csv"
    secondary_df.to_csv(secondary_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {secondary_path} ({len(secondary_df)} schools)")


def print_summary(primary_df, secondary_df):
    """Print summary."""
    print(f"\n{'=' * 70}")
    print("FRANKFURT SCHOOL MASTER DATA SCRAPER - COMPLETE")
    print(f"{'=' * 70}")
    print(f"\nPrimary schools (Grundschulen): {len(primary_df)}")
    print(f"Secondary schools: {len(secondary_df)}")

    for label, df in [("Primary", primary_df), ("Secondary", secondary_df)]:
        print(f"\n--- {label} Schools ---")
        if 'school_type' in df.columns:
            for st, count in df['school_type'].value_counts().items():
                print(f"  - {st}: {count}")
        if 'traegerschaft' in df.columns:
            for t, count in df['traegerschaft'].value_counts().items():
                print(f"  - {t}: {count}")
        if 'latitude' in df.columns:
            c = df['latitude'].notna().sum()
            print(f"  Coordinates: {c}/{len(df)} ({100*c/len(df):.0f}%)")
        if 'ndh_count' in df.columns:
            c = df['ndh_count'].notna().sum()
            print(f"  ndH data: {c}/{len(df)} ({100*c/len(df):.0f}%)")

    print(f"\n{'=' * 70}")


def main():
    """Main function."""
    logger.info("=" * 60)
    logger.info("Starting Frankfurt School Master Data Scraper")
    logger.info("=" * 60)

    ensure_directories()

    # Download and parse Hessen Verzeichnis 6
    schools_df = download_hessen_verzeichnis()

    # Filter for Frankfurt
    schools_df = filter_frankfurt(schools_df)

    # Normalize columns
    schools_df = normalize_columns(schools_df)

    # Add coordinates from jedeschule
    schools_df = add_coordinates_jedeschule(schools_df)

    # Geocode remaining
    schools_df = geocode_remaining(schools_df)

    # Split
    primary_df, secondary_df = split_by_school_type(schools_df)

    # Save
    save_outputs(schools_df, primary_df, secondary_df)

    # Summary
    print_summary(primary_df, secondary_df)

    return primary_df, secondary_df


if __name__ == "__main__":
    main()
