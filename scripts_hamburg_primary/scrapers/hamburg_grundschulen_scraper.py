#!/usr/bin/env python3
"""
Hamburg Grundschulen (Primary School) Scraper
Phase 1: Downloads school data from Hamburg Transparenzportal and creates base master table
for primary schools (Grundschulen).

Data Source: https://suche.transparenz.hamburg.de/dataset/schulstammdaten-und-schuelerzahlen-der-hamburger-schulen16
Formats: CSV and GeoJSON from Hamburg's WFS service

This script:
1. Downloads the CSV and GeoJSON data from Hamburg's open data portal
2. Parses and normalizes the data
3. Filters for Grundschulen only (excludes Stadtteilschulen and Gymnasien)
4. Extracts coordinates from GeoJSON
5. Outputs: hamburg_grundschulen.csv and hamburg_grundschulen_raw.csv

Expected output: ~237 standalone Grundschulen (208 state + 29 private)

Author: Hamburg School Data Pipeline
Created: 2026-04-04
"""

import requests
import pandas as pd
import json
import logging
import os
import sys
import zipfile
import io
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data source URLs
CSV_URL = "https://geodienste.hamburg.de/download?url=https://geodienste.hamburg.de/HH_WFS_Schulen&f=csv"
GEOJSON_URL = "https://geodienste.hamburg.de/download?url=https://geodienste.hamburg.de/HH_WFS_Schulen&f=json"

# Headers for requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg_primary"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"


def ensure_directories():
    """Create necessary directories if they don't exist."""
    for dir_path in [RAW_DIR, INTERMEDIATE_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {dir_path}")


def download_file(url: str, description: str) -> bytes:
    """Download a file from URL and return its content."""
    logger.info(f"Downloading {description} from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        logger.info(f"Successfully downloaded {description} ({len(response.content)} bytes)")
        return response.content
    except requests.RequestException as e:
        logger.error(f"Failed to download {description}: {e}")
        raise


def extract_csv_from_zip(zip_content: bytes) -> pd.DataFrame:
    """Extract and combine CSV files from a ZIP archive."""
    logger.info("Extracting CSV files from ZIP archive...")

    all_dfs = []

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        file_list = zf.namelist()
        logger.info(f"ZIP contains {len(file_list)} files: {file_list}")

        # Look for the main school CSV files (EPSG_4326 for WGS84 coordinates)
        target_files = [
            'de_hh_up_staatliche_schulen_EPSG_4326.csv',      # State schools
            'de_hh_up_nicht_staatliche_schulen_EPSG_4326.csv', # Private schools
        ]

        for target_file in target_files:
            if target_file in file_list:
                logger.info(f"Extracting: {target_file}")
                with zf.open(target_file) as f:
                    csv_content = f.read()
                    df = parse_single_csv(csv_content, target_file)
                    if df is not None and not df.empty:
                        # Add source file info
                        if 'nicht_staatliche' in target_file:
                            df['traegerschaft'] = 'Privat/Freie Träger'
                        else:
                            df['traegerschaft'] = 'Staatlich'
                        all_dfs.append(df)

    if not all_dfs:
        raise ValueError("No valid CSV data found in ZIP archive")

    # Combine all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Combined {len(all_dfs)} CSV files into {len(combined_df)} total rows")

    return combined_df


def parse_single_csv(csv_content: bytes, filename: str) -> pd.DataFrame:
    """Parse a single CSV file content into DataFrame."""
    logger.info(f"Parsing CSV: {filename}...")

    best_df = None
    best_cols = 0

    # Try different encodings and separators
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
    separators = [';', ',', '\t']  # Semicolon first (common in German CSVs)

    for encoding in encodings:
        try:
            csv_text = csv_content.decode(encoding)
        except UnicodeDecodeError:
            continue

        for sep in separators:
            try:
                df = pd.read_csv(io.StringIO(csv_text), sep=sep, on_bad_lines='skip')
                if len(df.columns) > best_cols:
                    best_df = df
                    best_cols = len(df.columns)
                    best_encoding = encoding
                    best_sep = sep
                    # If we found many columns, this is likely correct
                    if len(df.columns) > 10:
                        logger.info(f"Successfully parsed {filename}")
                        logger.info(f"  Encoding: {encoding}, Separator: '{sep}'")
                        logger.info(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
                        logger.info(f"  Columns: {list(df.columns)[:5]}...")
                        return df
            except pd.errors.ParserError:
                continue

    if best_df is not None and best_cols > 1:
        logger.info(f"Best parse for {filename}: {best_cols} columns with {best_encoding}/{best_sep}")
        return best_df

    logger.warning(f"Could not parse {filename} with any encoding/separator")
    return None


def parse_csv_data(csv_content: bytes) -> pd.DataFrame:
    """Parse CSV content - handles both ZIP archives and raw CSV."""
    logger.info("Parsing CSV data...")

    # Check if content is a ZIP file
    if csv_content[:4] == b'PK\x03\x04':
        logger.info("Detected ZIP archive format")
        return extract_csv_from_zip(csv_content)

    # Otherwise try as raw CSV
    return parse_single_csv(csv_content, "raw_csv")


def extract_geojson_from_zip(zip_content: bytes) -> dict:
    """Extract GeoJSON files from a ZIP archive and combine them."""
    logger.info("Extracting GeoJSON files from ZIP archive...")

    all_features = []

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        file_list = zf.namelist()
        logger.info(f"ZIP contains {len(file_list)} files")

        # Look for GeoJSON files (EPSG_4326 for WGS84 coordinates)
        for filename in file_list:
            if 'EPSG_4326' in filename and filename.endswith('.json'):
                logger.info(f"Extracting: {filename}")
                with zf.open(filename) as f:
                    content = f.read()
                    try:
                        geojson_data = json.loads(content.decode('utf-8'))
                        if 'features' in geojson_data:
                            all_features.extend(geojson_data['features'])
                            logger.info(f"  Found {len(geojson_data['features'])} features in {filename}")
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Failed to parse {filename}: {e}")

    logger.info(f"Total features extracted: {len(all_features)}")
    return {'type': 'FeatureCollection', 'features': all_features}


def parse_geojson_data(geojson_content: bytes) -> dict:
    """Parse GeoJSON content and extract coordinates."""
    logger.info("Parsing GeoJSON data...")

    # Check if content is a ZIP file
    if geojson_content[:4] == b'PK\x03\x04':
        logger.info("Detected ZIP archive format for GeoJSON")
        return extract_geojson_from_zip(geojson_content)

    # Otherwise try as raw GeoJSON
    try:
        geojson_text = geojson_content.decode('utf-8')
        geojson_data = json.loads(geojson_text)

        if 'features' in geojson_data:
            logger.info(f"GeoJSON contains {len(geojson_data['features'])} features")

        return geojson_data
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Failed to parse GeoJSON: {e}")
        raise


def extract_coordinates_from_geojson(geojson_data: dict) -> pd.DataFrame:
    """Extract school IDs and coordinates from GeoJSON features."""
    logger.info("Extracting coordinates from GeoJSON...")

    coords_list = []

    for feature in geojson_data.get('features', []):
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})

        # Get school identifier (try different possible field names)
        school_id = None
        for id_field in ['schul_id', 'schulnummer', 'id', 'schulid', 'schule_id', 'snr']:
            if id_field in properties:
                school_id = properties[id_field]
                break

        # Get school name as fallback identifier
        school_name = None
        for name_field in ['schulname', 'name', 'schule', 'bezeichnung']:
            if name_field in properties:
                school_name = properties[name_field]
                break

        # Extract coordinates
        lat, lon = None, None
        if geometry.get('type') == 'Point':
            coordinates = geometry.get('coordinates', [])
            if len(coordinates) >= 2:
                # GeoJSON uses [longitude, latitude] order
                lon, lat = coordinates[0], coordinates[1]

        if school_name or school_id:
            coords_list.append({
                'geojson_school_id': school_id,
                'geojson_school_name': school_name,
                'latitude': lat,
                'longitude': lon,
                'geojson_properties': properties  # Keep all properties for debugging
            })

    df = pd.DataFrame(coords_list)
    logger.info(f"Extracted coordinates for {len(df)} schools")
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to consistent format."""
    logger.info("Normalizing column names...")

    # Create a mapping of common variations
    column_mapping = {
        # School identification
        'schul_id': 'schulnummer',
        'schulid': 'schulnummer',
        'snr': 'schulnummer',
        'schule_id': 'schulnummer',

        # School name
        'schulname': 'schulname',
        'name': 'schulname',
        'schule': 'schulname',
        'bezeichnung': 'schulname',

        # School type - map schulform to schulart, keep schultyp separate
        'schulform': 'schulart',

        # Address
        'strasse': 'strasse',
        'straße': 'strasse',
        'str': 'strasse',
        'adresse': 'strasse',

        # Postal code
        'plz': 'plz',
        'postleitzahl': 'plz',

        # District
        'stadtteil': 'stadtteil',
        'ortsteil': 'stadtteil',

        # Bezirk
        'bezirk': 'bezirk',

        # Contact
        'telefon': 'telefon',
        'tel': 'telefon',
        'phone': 'telefon',

        'email': 'email',
        'e-mail': 'email',
        'mail': 'email',

        'homepage': 'website',
        'website': 'website',
        'url': 'website',
        'www': 'website',

        # Leadership
        'schulleiter': 'leitung',
        'schulleitung': 'leitung',
        'leiter': 'leitung',

        # Student counts
        'schueler': 'schueler_gesamt',
        'schuelerzahl': 'schueler_gesamt',
        'anzahl_schueler': 'schueler_gesamt',

        # Operator/Traegerschaft
        'traeger': 'traegerschaft',
        'träger': 'traegerschaft',
        'traegerschaft': 'traegerschaft',
    }

    # Apply mapping (case-insensitive)
    new_columns = {}
    seen_names = set()

    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in column_mapping:
            target_name = column_mapping[col_lower]
        else:
            # Keep original but lowercase with underscores
            target_name = col_lower.replace(' ', '_').replace('-', '_')

        # Handle duplicates by adding suffix
        if target_name in seen_names:
            # If duplicate, add a suffix based on original column name
            suffix = 2
            while f"{target_name}_{suffix}" in seen_names:
                suffix += 1
            target_name = f"{target_name}_{suffix}"

        seen_names.add(target_name)
        new_columns[col] = target_name

    df = df.rename(columns=new_columns)
    logger.info(f"Normalized columns: {list(df.columns)}")
    return df


def filter_primary_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame to keep only Grundschulen (excluding Stadtteilschulen and Gymnasien)."""
    logger.info("Filtering for primary schools (Grundschulen)...")

    # Find the school type column
    type_col = None
    for col in ['schulart', 'schulform', 'schultyp', 'type', 'art']:
        if col in df.columns:
            type_col = col
            break

    if type_col is None:
        logger.warning("No school type column found. Keeping all schools.")
        logger.info(f"Available columns: {list(df.columns)}")
        return df

    # Get unique school types before filtering
    unique_types = df[type_col].unique()
    logger.info(f"Found school types: {list(unique_types)}")

    # Create filter mask: any school that contains "Grundschule" in its schulart
    # This includes both standalone Grundschulen and combined schools
    # (e.g., Grundschule|Stadtteilschule) that also serve primary grades
    mask = df[type_col].str.lower().str.contains('grundschule', na=False)

    filtered_df = df[mask].copy()

    logger.info(f"Filtered from {len(df)} to {len(filtered_df)} primary schools (Grundschulen)")

    # Log breakdown by type
    type_counts = filtered_df[type_col].value_counts()
    for school_type, count in type_counts.items():
        logger.info(f"  - {school_type}: {count} schools")

    return filtered_df


def clean_and_enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data and add derived fields."""
    logger.info("Cleaning and enriching data...")

    # Add school_type standardized column - all primary schools are Grundschule
    if 'schulart' in df.columns:
        def categorize_school_type(art):
            if pd.isna(art):
                return 'Unknown'
            return 'Grundschule'

        df['school_type'] = df['schulart'].apply(categorize_school_type)

    # Clean PLZ - ensure 5-digit format
    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip()
        # Pad with zeros if needed
        df['plz'] = df['plz'].apply(lambda x: x.zfill(5) if x.isdigit() else x)

    # Clean phone numbers
    if 'telefon' in df.columns:
        df['telefon'] = df['telefon'].astype(str).str.strip()

    # Clean email
    if 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.strip().str.lower()

    # Clean website URLs
    if 'website' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).lower() in ['nan', 'none', '']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    # Add traegerschaft classification
    if 'traegerschaft' in df.columns:
        def classify_traeger(traeger):
            if pd.isna(traeger):
                return 'Unknown'
            traeger_lower = str(traeger).lower()
            if 'privat' in traeger_lower or 'frei' in traeger_lower:
                return 'Privat'
            elif 'stadt' in traeger_lower or 'öffentlich' in traeger_lower or 'hamburg' in traeger_lower:
                return 'Öffentlich'
            else:
                return 'Sonstige'
        df['traeger_typ'] = df['traegerschaft'].apply(classify_traeger)

    # Add metadata
    df['data_source'] = 'Transparenzportal Hamburg'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')

    return df


def merge_with_coordinates(schools_df: pd.DataFrame, coords_df: pd.DataFrame) -> pd.DataFrame:
    """Merge school data with coordinates from GeoJSON."""
    logger.info("Merging with coordinate data...")

    if coords_df.empty:
        logger.warning("No coordinate data to merge")
        return schools_df

    # Try to match on school name if IDs don't work
    if 'geojson_school_name' in coords_df.columns and 'schulname' in schools_df.columns:
        # Clean names for matching
        schools_df['_match_name'] = schools_df['schulname'].str.lower().str.strip()
        coords_df['_match_name'] = coords_df['geojson_school_name'].str.lower().str.strip()

        # Merge on cleaned names
        merged = schools_df.merge(
            coords_df[['_match_name', 'latitude', 'longitude']],
            on='_match_name',
            how='left'
        )

        # Drop temporary columns
        merged = merged.drop(columns=['_match_name'], errors='ignore')

        # Count successful coordinate matches
        coord_count = merged['latitude'].notna().sum()
        logger.info(f"Successfully matched coordinates for {coord_count}/{len(merged)} schools")

        return merged

    return schools_df


def save_outputs(df: pd.DataFrame):
    """Save the processed data to various formats."""
    logger.info("Saving output files...")

    # Save raw CSV (all data before filtering)
    raw_csv_path = RAW_DIR / "hamburg_grundschulen_raw.csv"
    df.to_csv(raw_csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {raw_csv_path}")

    # Save as parquet for efficient storage
    raw_parquet_path = RAW_DIR / "hamburg_grundschulen_raw.parquet"
    df.to_parquet(raw_parquet_path, index=False)
    logger.info(f"Saved: {raw_parquet_path}")

    # Also save primary schools only
    if 'school_type' in df.columns:
        primary_only = df[df['school_type'] == 'Grundschule']
        primary_csv_path = RAW_DIR / "hamburg_grundschulen.csv"
        primary_only.to_csv(primary_csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {primary_csv_path} ({len(primary_only)} schools)")

    return raw_csv_path


def print_summary(df: pd.DataFrame):
    """Print summary statistics of the scraped data."""
    print("\n" + "="*70)
    print("HAMBURG GRUNDSCHULEN SCRAPER - PHASE 1 COMPLETE")
    print("="*70)

    print(f"\nTotal primary schools: {len(df)}")

    if 'school_type' in df.columns:
        print("\nSchools by type:")
        for school_type, count in df['school_type'].value_counts().items():
            print(f"  - {school_type}: {count}")

    if 'traeger_typ' in df.columns:
        print("\nSchools by operator type:")
        for traeger, count in df['traeger_typ'].value_counts().items():
            print(f"  - {traeger}: {count}")

    if 'bezirk' in df.columns:
        print("\nSchools by Bezirk:")
        for bezirk, count in df['bezirk'].value_counts().head(10).items():
            print(f"  - {bezirk}: {count}")

    # Check coordinate coverage
    if 'latitude' in df.columns:
        coord_count = df['latitude'].notna().sum()
        print(f"\nCoordinate coverage: {coord_count}/{len(df)} ({100*coord_count/len(df):.1f}%)")

    print(f"\nColumns in output: {len(df.columns)}")
    print(f"Columns: {list(df.columns)}")

    print("\nSample data (first 3 schools):")
    display_cols = ['schulnummer', 'schulname', 'school_type', 'bezirk', 'plz']
    display_cols = [c for c in display_cols if c in df.columns]
    print(df[display_cols].head(3).to_string())

    print("\n" + "="*70)


def main():
    """Main function to run the Hamburg Grundschulen scraper."""
    logger.info("="*60)
    logger.info("Starting Hamburg Grundschulen Scraper (Phase 1)")
    logger.info("="*60)

    try:
        # Ensure output directories exist
        ensure_directories()

        # Download data
        csv_content = download_file(CSV_URL, "CSV data")
        geojson_content = download_file(GEOJSON_URL, "GeoJSON data")

        # Save raw downloads
        raw_csv_download = RAW_DIR / "hamburg_schools_download.csv"
        raw_geojson_download = RAW_DIR / "hamburg_schools_download.geojson"

        with open(raw_csv_download, 'wb') as f:
            f.write(csv_content)
        logger.info(f"Saved raw CSV download: {raw_csv_download}")

        with open(raw_geojson_download, 'wb') as f:
            f.write(geojson_content)
        logger.info(f"Saved raw GeoJSON download: {raw_geojson_download}")

        # Parse data
        schools_df = parse_csv_data(csv_content)
        geojson_data = parse_geojson_data(geojson_content)
        coords_df = extract_coordinates_from_geojson(geojson_data)

        # Normalize column names
        schools_df = normalize_column_names(schools_df)

        # Merge with coordinates
        schools_df = merge_with_coordinates(schools_df, coords_df)

        # Clean and enrich
        schools_df = clean_and_enrich_data(schools_df)

        # Filter for primary schools (Grundschulen)
        primary_df = filter_primary_schools(schools_df)

        # Save outputs
        save_outputs(primary_df)

        # Print summary
        print_summary(primary_df)

        logger.info("Phase 1 complete!")
        return primary_df

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
