#!/usr/bin/env python3
"""
NRW School Master Data Scraper
Downloads and processes school data from NRW Schulministerium Open Data.

Data Source: https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/schuldaten.csv
Format: CSV (semicolon-separated, UTF-8)

This script:
1. Downloads the school master CSV from NRW open data portal
2. Downloads the Schulsozialindex CSV
3. Parses and normalizes the data
4. Converts UTM coordinates (EPSG:25832) to WGS84 lat/lon
5. Filters for Cologne and Düsseldorf schools
6. Splits into primary (Grundschule) and secondary school outputs
7. Merges Schulsozialindex data

Author: NRW School Data Pipeline
Created: 2026-02-15
"""

import requests
import pandas as pd
import logging
import sys
import io
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from pyproj import Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data source URLs
SCHULDATEN_CSV_URL = "https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/schuldaten.csv"
SCHULSOZIALINDEX_CSV_URL = "https://www.schulministerium.nrw/system/files/media/document/file/schulliste_sj_25_26_open_data.csv"
KEY_SCHULFORM_URL = "https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/key_schulformschluessel.csv"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

# NRW school type codes
SCHULFORM_CODES = {
    '02': 'Grundschule',
    '04': 'Hauptschule',
    '06': 'Volksschule',
    '08': 'Förderschule',
    '10': 'Realschule',
    '13': 'Primus (Schulversuch)',
    '14': 'Sekundarschule',
    '15': 'Gesamtschule',
    '17': 'Waldorfschule',
    '18': 'Hiberniaschule',
    '20': 'Gymnasium',
    '25': 'Weiterbildungskolleg',
    '30': 'Berufskolleg',
}

# Rechtsform codes
RECHTSFORM_CODES = {
    '1': 'Öffentlich',
    '2': 'Privat',
}

# Bezirksregierung codes
BEZIRKSREGIERUNG_CODES = {
    '1': 'Düsseldorf',
    '3': 'Köln',
    '5': 'Münster',
    '7': 'Detmold',
    '9': 'Arnsberg',
}

# Primary school types
PRIMARY_SCHULFORM_CODES = ['02']

# Secondary school types
SECONDARY_SCHULFORM_CODES = ['04', '10', '14', '15', '17', '20']

# Target cities (Cologne and Düsseldorf metro area)
TARGET_CITIES_COLOGNE = [
    'Köln', 'Koeln',
]
TARGET_CITIES_DUESSELDORF = [
    'Düsseldorf', 'Duesseldorf',
]
TARGET_CITIES = TARGET_CITIES_COLOGNE + TARGET_CITIES_DUESSELDORF


def ensure_directories():
    """Create necessary directories."""
    for dir_path in [RAW_DIR, INTERMEDIATE_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)


def download_file(url: str, description: str) -> bytes:
    """Download a file from URL."""
    logger.info(f"Downloading {description} from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {description} ({len(response.content):,} bytes)")
        return response.content
    except requests.RequestException as e:
        logger.error(f"Failed to download {description}: {e}")
        raise


def parse_schuldaten_csv(content: bytes) -> pd.DataFrame:
    """Parse the NRW school data CSV."""
    logger.info("Parsing NRW school data CSV...")

    # The file starts with 'sep=;' hint line, then header
    text = content.decode('utf-8')

    # Skip the 'sep=;' line if present
    lines = text.split('\n')
    if lines[0].strip().startswith('sep='):
        text = '\n'.join(lines[1:])

    df = pd.read_csv(
        io.StringIO(text),
        sep=';',
        quotechar='"',
        dtype=str,
        on_bad_lines='skip'
    )

    logger.info(f"Parsed {len(df)} schools with {len(df.columns)} columns")
    logger.info(f"Columns: {list(df.columns)}")

    return df


def parse_schulsozialindex_csv(content: bytes) -> pd.DataFrame:
    """Parse the Schulsozialindex CSV (cp850 encoding)."""
    logger.info("Parsing Schulsozialindex CSV...")

    # Try cp850 first, then fallback
    for encoding in ['cp850', 'latin-1', 'utf-8', 'utf-8-sig']:
        try:
            text = content.decode(encoding)
            df = pd.read_csv(
                io.StringIO(text),
                sep=';',
                dtype=str,
                on_bad_lines='skip'
            )
            if len(df.columns) >= 5:
                logger.info(f"Parsed Schulsozialindex with encoding {encoding}: {len(df)} schools")
                logger.info(f"Columns: {list(df.columns)}")
                return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    logger.warning("Could not parse Schulsozialindex CSV")
    return pd.DataFrame()


def convert_utm_to_wgs84(df: pd.DataFrame) -> pd.DataFrame:
    """Convert UTM coordinates (EPSG:25832) to WGS84 lat/lon."""
    logger.info("Converting UTM coordinates to WGS84...")

    df = df.copy()
    df['latitude'] = None
    df['longitude'] = None

    if not PYPROJ_AVAILABLE:
        logger.warning("pyproj not installed. Cannot convert coordinates.")
        logger.info("Install with: pip install pyproj")
        return df

    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)

    converted = 0
    for idx, row in df.iterrows():
        easting = row.get('UTMRechtswert', 'null')
        northing = row.get('UTMHochwert', 'null')

        if easting == 'null' or northing == 'null' or pd.isna(easting) or pd.isna(northing):
            continue

        try:
            e = float(str(easting).replace(',', '.'))
            n = float(str(northing).replace(',', '.'))

            if e > 0 and n > 0:
                lon, lat = transformer.transform(e, n)
                df.at[idx, 'latitude'] = round(lat, 6)
                df.at[idx, 'longitude'] = round(lon, 6)
                converted += 1
        except (ValueError, TypeError):
            continue

    logger.info(f"Converted coordinates for {converted}/{len(df)} schools")
    return df


def filter_active_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for active schools only."""
    logger.info("Filtering for active schools...")

    if 'Schulbetriebsschluessel' in df.columns:
        active_mask = df['Schulbetriebsschluessel'].isin(['1', '6'])
        filtered = df[active_mask].copy()
        logger.info(f"Filtered from {len(df)} to {len(filtered)} active schools")
        return filtered

    return df


def filter_target_cities(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for Cologne and Düsseldorf."""
    logger.info("Filtering for Cologne and Düsseldorf...")

    if 'Ort' not in df.columns:
        logger.warning("No 'Ort' column found")
        return df

    # Normalize city names for matching
    mask = df['Ort'].str.strip().isin(TARGET_CITIES)
    filtered = df[mask].copy()

    logger.info(f"Filtered from {len(df)} to {len(filtered)} schools in target cities")

    if 'Ort' in filtered.columns:
        for city, count in filtered['Ort'].value_counts().items():
            logger.info(f"  - {city}: {count} schools")

    return filtered


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and enrich the dataframe with standardized columns."""
    logger.info("Normalizing columns...")

    df = df.copy()

    # Add decoded school type
    if 'Schulform' in df.columns:
        df['schulform_name'] = df['Schulform'].map(SCHULFORM_CODES).fillna('Sonstige')

    # Add decoded Rechtsform
    if 'Rechtsform' in df.columns:
        df['traegerschaft'] = df['Rechtsform'].map(RECHTSFORM_CODES).fillna('Unbekannt')

    # Add decoded Bezirksregierung
    if 'Bezirksregierung' in df.columns:
        df['bezirksregierung_name'] = df['Bezirksregierung'].map(BEZIRKSREGIERUNG_CODES).fillna('Unbekannt')

    # Build full school name
    name_parts = []
    for col in ['Schulbezeichnung_1', 'Schulbezeichnung_2', 'Schulbezeichnung_3']:
        if col in df.columns:
            name_parts.append(col)

    if name_parts:
        df['schulname'] = df[name_parts].fillna('').apply(
            lambda x: ' '.join(x.str.strip()).strip(), axis=1
        )

    # Standardize column names for compatibility
    rename_map = {
        'Schulnummer': 'schulnummer',
        'PLZ': 'plz',
        'Ort': 'ort',
        'Strasse': 'strasse',
        'E-Mail': 'email',
        'Homepage': 'website',
        'Kurzbezeichnung': 'kurzbezeichnung',
        'Gemeindeschluessel': 'gemeindeschluessel',
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Build phone number
    if 'Telefonvorwahl' in df.columns and 'Telefon' in df.columns:
        df['telefon'] = df['Telefonvorwahl'].fillna('') + df['Telefon'].fillna('')
        df['telefon'] = df['telefon'].str.strip()
        df.loc[df['telefon'] == '', 'telefon'] = None

    # Clean website URLs
    if 'website' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).strip() in ['', 'null']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    # Determine city/bezirk
    if 'ort' in df.columns:
        df['stadt'] = df['ort'].str.strip()

    # Add metadata
    df['data_source'] = 'Schulministerium NRW Open Data'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')
    df['bundesland'] = 'Nordrhein-Westfalen'

    return df


def merge_schulsozialindex(df: pd.DataFrame, ssi_df: pd.DataFrame) -> pd.DataFrame:
    """Merge Schulsozialindex into school data."""
    if ssi_df.empty:
        logger.warning("No Schulsozialindex data to merge")
        return df

    logger.info("Merging Schulsozialindex...")

    # Find the Schulnummer column in SSI data
    ssi_schulnr_col = None
    for col in ssi_df.columns:
        if 'schulnummer' in col.lower():
            ssi_schulnr_col = col
            break

    if ssi_schulnr_col is None:
        logger.warning("No Schulnummer column found in Schulsozialindex data")
        return df

    # Find the Sozialindex column
    ssi_index_col = None
    for col in ssi_df.columns:
        if 'sozialindex' in col.lower():
            ssi_index_col = col
            break

    if ssi_index_col is None:
        logger.warning("No Sozialindexstufe column found")
        return df

    # Prepare merge
    ssi_merge = ssi_df[[ssi_schulnr_col, ssi_index_col]].copy()
    ssi_merge.columns = ['schulnummer', 'sozialindexstufe']
    ssi_merge['schulnummer'] = ssi_merge['schulnummer'].astype(str).str.strip()

    # Convert sozialindex - 'ohne' becomes NaN
    ssi_merge['sozialindexstufe'] = pd.to_numeric(
        ssi_merge['sozialindexstufe'].replace('ohne', None),
        errors='coerce'
    )

    df['schulnummer'] = df['schulnummer'].astype(str).str.strip()

    merged = df.merge(ssi_merge, on='schulnummer', how='left')

    match_count = merged['sozialindexstufe'].notna().sum()
    logger.info(f"Matched Schulsozialindex for {match_count}/{len(merged)} schools")

    return merged


def split_by_school_type(df: pd.DataFrame):
    """Split into primary and secondary school DataFrames."""
    logger.info("Splitting by school type...")

    primary_mask = df['Schulform'].isin(PRIMARY_SCHULFORM_CODES)
    secondary_mask = df['Schulform'].isin(SECONDARY_SCHULFORM_CODES)

    primary_df = df[primary_mask].copy()
    secondary_df = df[secondary_mask].copy()

    # Add school_type column
    primary_df['school_type'] = 'Grundschule'

    def classify_secondary(schulform):
        return SCHULFORM_CODES.get(schulform, 'Sonstige')

    secondary_df['school_type'] = secondary_df['Schulform'].apply(classify_secondary)

    logger.info(f"Primary schools (Grundschulen): {len(primary_df)}")
    logger.info(f"Secondary schools: {len(secondary_df)}")

    if not secondary_df.empty and 'school_type' in secondary_df.columns:
        for st, count in secondary_df['school_type'].value_counts().items():
            logger.info(f"  - {st}: {count}")

    return primary_df, secondary_df


def save_outputs(all_df: pd.DataFrame, primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    """Save processed data."""
    logger.info("Saving output files...")

    # Save all schools (raw)
    all_path = RAW_DIR / "nrw_schools_cologne_duesseldorf_all.csv"
    all_df.to_csv(all_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {all_path} ({len(all_df)} schools)")

    # Save primary schools
    primary_path = RAW_DIR / "nrw_primary_schools.csv"
    primary_df.to_csv(primary_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {primary_path} ({len(primary_df)} schools)")

    # Save secondary schools
    secondary_path = RAW_DIR / "nrw_secondary_schools.csv"
    secondary_df.to_csv(secondary_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {secondary_path} ({len(secondary_df)} schools)")


def print_summary(primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    """Print summary statistics."""
    print("\n" + "=" * 70)
    print("NRW SCHOOL MASTER DATA SCRAPER - COMPLETE")
    print("=" * 70)

    print(f"\nPrimary schools (Grundschulen): {len(primary_df)}")
    print(f"Secondary schools: {len(secondary_df)}")

    for label, df in [("Primary", primary_df), ("Secondary", secondary_df)]:
        print(f"\n--- {label} Schools ---")

        if 'stadt' in df.columns:
            print("  By city:")
            for city, count in df['stadt'].value_counts().items():
                print(f"    - {city}: {count}")

        if 'school_type' in df.columns:
            print("  By type:")
            for st, count in df['school_type'].value_counts().items():
                print(f"    - {st}: {count}")

        if 'traegerschaft' in df.columns:
            print("  By operator:")
            for t, count in df['traegerschaft'].value_counts().items():
                print(f"    - {t}: {count}")

        if 'latitude' in df.columns:
            coord_count = df['latitude'].notna().sum()
            print(f"  Coordinates: {coord_count}/{len(df)} ({100 * coord_count / len(df):.0f}%)")

        if 'sozialindexstufe' in df.columns:
            ssi_count = df['sozialindexstufe'].notna().sum()
            print(f"  Schulsozialindex: {ssi_count}/{len(df)} ({100 * ssi_count / len(df):.0f}%)")

    print("\n" + "=" * 70)


def main():
    """Main function."""
    logger.info("=" * 60)
    logger.info("Starting NRW School Master Data Scraper")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # Download school data
        school_content = download_file(SCHULDATEN_CSV_URL, "NRW school data")

        # Save raw download
        raw_download = RAW_DIR / "nrw_schuldaten_raw.csv"
        with open(raw_download, 'wb') as f:
            f.write(school_content)

        # Parse school data
        schools_df = parse_schuldaten_csv(school_content)

        # Download and parse Schulsozialindex
        ssi_df = pd.DataFrame()
        try:
            ssi_content = download_file(SCHULSOZIALINDEX_CSV_URL, "Schulsozialindex")
            raw_ssi = RAW_DIR / "nrw_schulsozialindex_raw.csv"
            with open(raw_ssi, 'wb') as f:
                f.write(ssi_content)
            ssi_df = parse_schulsozialindex_csv(ssi_content)
        except Exception as e:
            logger.warning(f"Could not download Schulsozialindex: {e}")

        # Filter active schools
        schools_df = filter_active_schools(schools_df)

        # Filter for target cities
        schools_df = filter_target_cities(schools_df)

        # Convert coordinates
        schools_df = convert_utm_to_wgs84(schools_df)

        # Normalize columns
        schools_df = normalize_columns(schools_df)

        # Merge Schulsozialindex
        schools_df = merge_schulsozialindex(schools_df, ssi_df)

        # Split by school type
        primary_df, secondary_df = split_by_school_type(schools_df)

        # Save outputs
        save_outputs(schools_df, primary_df, secondary_df)

        # Print summary
        print_summary(primary_df, secondary_df)

        logger.info("NRW School Master Data Scraper complete!")
        return primary_df, secondary_df

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
