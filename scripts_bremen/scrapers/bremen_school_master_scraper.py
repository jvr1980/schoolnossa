#!/usr/bin/env python3
"""
Phase 1: Bremen School Master Data Scraper

Downloads and merges two data sources:
1. GeoBremen Schulstandorte Shapefile (coordinates + school type, EPSG:25832)
   URL: https://gdi2.geo.bremen.de/inspire/download/Schulstandorte/data/Schulstandorte_HB_BHV.zip
2. Schulwegweiser Excel from bildung.bremen.de (school details)
   URL: https://www.bildung.bremen.de/schulwegweiser-3714

Pipeline: Download both → Parse SHP → Convert EPSG:25832→WGS84 → Parse Excel →
          Join on school name/address → Normalize → Output CSV

Output: data_bremen/raw/bremen_school_master.csv

Author: Bremen School Data Pipeline
Created: 2026-04-07
"""

import requests
import pandas as pd
import numpy as np
import logging
import sys
import io
import re
import zipfile
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

try:
    from pyproj import Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False

try:
    import geopandas as gpd
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_bremen"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Data source URLs
SHAPEFILE_URL = "https://gdi2.geo.bremen.de/inspire/download/Schulstandorte/data/Schulstandorte_HB_BHV.zip"
# The Schulwegweiser Excel download link — may need updating if the site changes
SCHULWEGWEISER_PAGE_URL = "https://www.bildung.bremen.de/schulwegweiser-3714"
# PDF school directory as fallback
SCHULVERZEICHNIS_PDF_URL = "https://www.bildung.bremen.de/sixcms/media.php/13/schulverzeichnis.pdf"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Bremen school type classification
SCHULFORM_MAP = {
    'grundschule': 'Grundschule',
    'oberschule': 'Oberschule',
    'gymnasium': 'Gymnasium',
    'werkschule': 'Werkschule',
    'förderzentrum': 'Förderzentrum',
    'beratungszentrum': 'Beratungszentrum',
    'bildungszentrum': 'Bildungszentrum',
    'freie schule': 'Freie Schule',
    'waldorfschule': 'Waldorfschule',
    'beruf': 'Berufsbildende Schule',
}

# School type grouping for pipeline
PRIMARY_TYPES = ['Grundschule']
SECONDARY_TYPES = ['Oberschule', 'Gymnasium']


def ensure_directories():
    """Create necessary directories."""
    for dir_path in [RAW_DIR, INTERMEDIATE_DIR, CACHE_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)


def download_file(url: str, description: str) -> bytes:
    """Download a file from URL with retry logic."""
    logger.info(f"Downloading {description} from {url}")
    for attempt in range(3):
        try:
            response = requests.get(url, headers=HEADERS, timeout=120)
            response.raise_for_status()
            logger.info(f"Downloaded {description} ({len(response.content):,} bytes)")
            return response.content
        except requests.RequestException as e:
            if attempt == 2:
                logger.error(f"Failed to download {description} after 3 attempts: {e}")
                raise
            logger.warning(f"Download attempt {attempt + 1} failed: {e}")


def parse_shapefile(zip_content: bytes) -> pd.DataFrame:
    """Parse the GeoBremen Schulstandorte Shapefile ZIP.

    Extracts school locations with coordinates in EPSG:25832,
    then converts to WGS84 (EPSG:4326).
    """
    logger.info("Parsing Schulstandorte Shapefile...")

    # Save ZIP for debugging
    zip_path = RAW_DIR / "bremen_schulstandorte.zip"
    with open(zip_path, 'wb') as f:
        f.write(zip_content)

    if GEOPANDAS_AVAILABLE:
        return _parse_shapefile_geopandas(zip_path)
    else:
        return _parse_shapefile_manual(zip_content)


def _parse_shapefile_geopandas(zip_path: Path) -> pd.DataFrame:
    """Parse shapefile using geopandas (preferred)."""
    logger.info("Using geopandas to read shapefile...")

    # geopandas can read directly from ZIP
    gdf = gpd.read_file(f"zip://{zip_path}")
    logger.info(f"Read {len(gdf)} features with CRS: {gdf.crs}")
    logger.info(f"Columns: {list(gdf.columns)}")

    # Convert to WGS84
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
        logger.info("Converted coordinates to WGS84")

    # Extract lat/lon from geometry
    df = pd.DataFrame(gdf.drop(columns='geometry'))
    df['latitude'] = gdf.geometry.y
    df['longitude'] = gdf.geometry.x

    coord_count = df['latitude'].notna().sum()
    logger.info(f"Extracted coordinates for {coord_count}/{len(df)} schools")

    return df


def _parse_shapefile_manual(zip_content: bytes) -> pd.DataFrame:
    """Fallback: parse shapefile using pyproj for coordinate conversion only.

    Reads the DBF file from the ZIP for attributes and extracts coordinates
    from the SHX/SHP files manually, or uses pyproj for conversion.
    """
    logger.warning("geopandas not available. Using manual shapefile parsing.")
    logger.info("Install geopandas for better shapefile support: pip install geopandas")

    # Without geopandas, we try to read the DBF file for attributes
    # and use the Overpass/Nominatim API for geocoding as fallback
    records = []

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        file_list = zf.namelist()
        logger.info(f"ZIP contains: {file_list}")

        # Try to find and read the DBF file (attribute data)
        dbf_files = [f for f in file_list if f.lower().endswith('.dbf')]
        if not dbf_files:
            logger.warning("No DBF file found in ZIP")
            return pd.DataFrame()

        # Try reading with pandas if simpledbf is available
        try:
            from simpledbf import Dbf5
            dbf_path = RAW_DIR / "temp_schulstandorte.dbf"
            with zf.open(dbf_files[0]) as src:
                with open(dbf_path, 'wb') as dst:
                    dst.write(src.read())
            dbf = Dbf5(str(dbf_path))
            df = dbf.to_dataframe()
            logger.info(f"Parsed DBF with {len(df)} records")
            dbf_path.unlink(missing_ok=True)
            return df
        except ImportError:
            logger.warning("simpledbf not available. Cannot parse DBF directly.")
            logger.info("Install with: pip install simpledbf")

    logger.warning("Could not parse shapefile without geopandas or simpledbf.")
    logger.info("Falling back to geocoding from addresses.")
    return pd.DataFrame()


def find_and_download_excel(page_url: str) -> Optional[bytes]:
    """Try to find and download the Schulwegweiser Excel file from the page."""
    logger.info(f"Searching for Excel download on {page_url}")

    try:
        response = requests.get(page_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        page_html = response.text

        # Look for Excel file links
        excel_patterns = [
            r'href=["\']([^"\']*\.xlsx?[^"\']*)["\']',
            r'href=["\']([^"\']*schuldatei[^"\']*)["\']',
            r'href=["\']([^"\']*schulverzeichnis[^"\']*\.xls[^"\']*)["\']',
        ]

        for pattern in excel_patterns:
            matches = re.findall(pattern, page_html, re.IGNORECASE)
            for match in matches:
                # Build full URL if relative
                if match.startswith('/'):
                    excel_url = f"https://www.bildung.bremen.de{match}"
                elif match.startswith('http'):
                    excel_url = match
                else:
                    excel_url = f"https://www.bildung.bremen.de/{match}"

                logger.info(f"Found Excel link: {excel_url}")
                try:
                    content = download_file(excel_url, "Schulwegweiser Excel")
                    return content
                except Exception as e:
                    logger.warning(f"Could not download {excel_url}: {e}")

        logger.warning("No Excel download link found on page")
        return None

    except requests.RequestException as e:
        logger.warning(f"Could not access Schulwegweiser page: {e}")
        return None


def parse_excel(content: bytes) -> pd.DataFrame:
    """Parse the Schulwegweiser Excel file."""
    logger.info("Parsing Schulwegweiser Excel...")

    # Save raw download
    excel_path = RAW_DIR / "bremen_schulwegweiser.xlsx"
    with open(excel_path, 'wb') as f:
        f.write(content)

    # Try reading with different engines
    for engine in ['openpyxl', 'xlrd']:
        try:
            # Read all sheets, pick the one with the most rows
            xlsx = pd.ExcelFile(excel_path, engine=engine)
            best_df = None
            best_rows = 0

            for sheet_name in xlsx.sheet_names:
                df = xlsx.parse(sheet_name, dtype=str)
                if len(df) > best_rows:
                    best_df = df
                    best_rows = len(df)
                    logger.info(f"Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")

            if best_df is not None:
                logger.info(f"Using sheet with {best_rows} rows")
                logger.info(f"Columns: {list(best_df.columns)}")
                return best_df

        except Exception as e:
            logger.warning(f"Could not read Excel with {engine}: {e}")

    logger.warning("Could not parse Excel file")
    return pd.DataFrame()


def classify_schulform(name_or_type: str) -> str:
    """Classify a school by its name or type string into a standard Schulform."""
    if pd.isna(name_or_type):
        return 'Sonstige'

    text = str(name_or_type).lower()

    for key, value in SCHULFORM_MAP.items():
        if key in text:
            return value

    return 'Sonstige'


def classify_school_group(schulform: str) -> str:
    """Classify a school as primary, secondary, or other."""
    if schulform in PRIMARY_TYPES:
        return 'primary'
    elif schulform in SECONDARY_TYPES:
        return 'secondary'
    else:
        return 'other'


def extract_stadtteil_from_address(address: str) -> Optional[str]:
    """Try to extract Stadtteil from address string."""
    if pd.isna(address):
        return None
    # Bremen addresses sometimes include the Stadtteil after the PLZ/Ort
    return None  # Will be filled from shapefile data or separate lookup


def normalize_name(name: str) -> str:
    """Normalize a school name for matching purposes."""
    if pd.isna(name):
        return ''
    name = str(name).lower().strip()
    # Remove common prefixes/suffixes
    name = re.sub(r'\s+', ' ', name)
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    return name


def join_data_sources(shp_df: pd.DataFrame, excel_df: pd.DataFrame) -> pd.DataFrame:
    """Join the Shapefile (coordinates) with the Excel (details) data.

    Strategy:
    1. Try matching on school name (fuzzy)
    2. Try matching on address
    3. Fall back to coordinate-only or detail-only records
    """
    logger.info("Joining Shapefile and Excel data sources...")

    if shp_df.empty and excel_df.empty:
        raise ValueError("Both data sources are empty — cannot proceed")

    if shp_df.empty:
        logger.warning("No shapefile data — using Excel only (no coordinates)")
        return excel_df

    if excel_df.empty:
        logger.warning("No Excel data — using Shapefile only (limited details)")
        return shp_df

    # Normalize names for matching
    # Find the school name column in each dataframe
    shp_name_col = None
    for col in shp_df.columns:
        if 'name' in col.lower() or 'bezeichnung' in col.lower() or 'schule' in col.lower():
            shp_name_col = col
            break

    excel_name_col = None
    for col in excel_df.columns:
        if 'name' in col.lower() or 'bezeichnung' in col.lower() or 'schule' in col.lower():
            excel_name_col = col
            break

    if shp_name_col and excel_name_col:
        shp_df['_match_key'] = shp_df[shp_name_col].apply(normalize_name)
        excel_df['_match_key'] = excel_df[excel_name_col].apply(normalize_name)

        # Merge on normalized name
        merged = excel_df.merge(
            shp_df[['_match_key', 'latitude', 'longitude']].drop_duplicates(subset='_match_key'),
            on='_match_key',
            how='left'
        )
        merged = merged.drop(columns=['_match_key'], errors='ignore')

        matched = merged['latitude'].notna().sum()
        logger.info(f"Name matching: {matched}/{len(merged)} schools got coordinates")

        # For unmatched schools, try address-based matching
        unmatched_count = merged['latitude'].isna().sum()
        if unmatched_count > 0:
            logger.info(f"{unmatched_count} schools still need coordinates (will geocode later)")

        return merged
    else:
        logger.warning("Could not identify name columns for matching")
        # Return shapefile data as primary (it has coordinates)
        return shp_df


def geocode_missing_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode schools that are missing coordinates using Nominatim."""
    missing = df['latitude'].isna()
    missing_count = missing.sum()

    if missing_count == 0:
        logger.info("All schools have coordinates — no geocoding needed")
        return df

    logger.info(f"Geocoding {missing_count} schools with missing coordinates via Nominatim...")

    # Build address strings for geocoding
    addr_cols = [c for c in ['strasse', 'plz', 'ort', 'adresse'] if c in df.columns]

    geocoded = 0
    for idx in df[missing].index:
        # Build search address
        parts = []
        for col in addr_cols:
            val = df.at[idx, col]
            if pd.notna(val) and str(val).strip():
                parts.append(str(val).strip())

        if not parts:
            continue

        search_addr = ', '.join(parts) + ', Bremen, Germany'

        try:
            import time
            time.sleep(1.1)  # Nominatim rate limit: 1 request/second

            response = requests.get(
                'https://nominatim.openstreetmap.org/search',
                params={
                    'q': search_addr,
                    'format': 'json',
                    'limit': 1,
                    'countrycodes': 'de',
                },
                headers={'User-Agent': 'SchoolNossa/1.0 (school-data-pipeline)'},
                timeout=10,
            )
            response.raise_for_status()
            results = response.json()

            if results:
                df.at[idx, 'latitude'] = float(results[0]['lat'])
                df.at[idx, 'longitude'] = float(results[0]['lon'])
                geocoded += 1

                if geocoded % 20 == 0:
                    logger.info(f"Geocoded {geocoded}/{missing_count} schools...")

        except Exception as e:
            logger.warning(f"Geocoding failed for '{search_addr}': {e}")

    logger.info(f"Geocoded {geocoded}/{missing_count} schools via Nominatim")
    final_missing = df['latitude'].isna().sum()
    if final_missing > 0:
        logger.warning(f"{final_missing} schools still have no coordinates")

    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and standardize column names."""
    logger.info("Normalizing columns...")

    df = df.copy()

    # Attempt column renaming based on common patterns
    rename_map = {}
    for col in df.columns:
        col_lower = col.lower().strip()

        if col_lower in ('schulnummer', 'schulnr', 'snr', 'schul_nr', 'schul_id', 'schulid'):
            rename_map[col] = 'schulnummer'
        elif col_lower in ('schulname', 'name', 'schule', 'bezeichnung', 'schulbezeichnung'):
            rename_map[col] = 'schulname'
        elif col_lower in ('schulform', 'schulart', 'schultyp', 'art'):
            rename_map[col] = 'schulform_raw'
        elif col_lower in ('strasse', 'straße', 'str', 'adresse_strasse'):
            rename_map[col] = 'strasse'
        elif col_lower in ('plz', 'postleitzahl'):
            rename_map[col] = 'plz'
        elif col_lower in ('ort', 'stadt', 'city'):
            rename_map[col] = 'ort'
        elif col_lower in ('stadtteil', 'ortsteil'):
            rename_map[col] = 'stadtteil'
        elif col_lower in ('bezirk',):
            rename_map[col] = 'bezirk'
        elif col_lower in ('telefon', 'tel', 'phone', 'telefonvorwahl'):
            rename_map[col] = 'telefon'
        elif col_lower in ('email', 'e-mail', 'mail'):
            rename_map[col] = 'email'
        elif col_lower in ('homepage', 'website', 'url', 'www', 'web'):
            rename_map[col] = 'website'
        elif col_lower in ('schulleiter', 'schulleitung', 'leitung', 'leiter'):
            rename_map[col] = 'leitung'
        elif col_lower in ('ganztagsschule', 'ganztag', 'ganztagsbetrieb'):
            rename_map[col] = 'ganztagsschule'
        elif col_lower in ('traeger', 'träger', 'traegerschaft', 'rechtsform'):
            rename_map[col] = 'traegerschaft'

    df = df.rename(columns=rename_map)

    # Generate schulnummer if missing
    if 'schulnummer' not in df.columns:
        df['schulnummer'] = [f"HB{i:04d}" for i in range(1, len(df) + 1)]
        logger.info("Generated synthetic schulnummer values (HBxxxx)")

    # Ensure schulnummer is string
    df['schulnummer'] = df['schulnummer'].astype(str).str.strip()

    # Classify Schulform
    schulform_source = 'schulform_raw' if 'schulform_raw' in df.columns else 'schulname'
    if schulform_source in df.columns:
        df['schulform'] = df[schulform_source].apply(classify_schulform)
        df['school_group'] = df['schulform'].apply(classify_school_group)
    else:
        df['schulform'] = 'Sonstige'
        df['school_group'] = 'other'

    # Classify Traegerschaft
    if 'traegerschaft' not in df.columns and 'schulname' in df.columns:
        def infer_traeger(name):
            if pd.isna(name):
                return 'Öffentlich'
            name_lower = str(name).lower()
            if any(w in name_lower for w in ['privat', 'frei', 'waldorf', 'montessori']):
                return 'Privat'
            return 'Öffentlich'
        df['traegerschaft'] = df['schulname'].apply(infer_traeger)

    # Clean URLs
    if 'website' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).strip() in ('', 'nan', 'None', 'null'):
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    # Clean PLZ
    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip()
        df['plz'] = df['plz'].apply(lambda x: x.zfill(5) if x.replace('.0', '').isdigit() else x)

    # Ensure latitude/longitude exist
    if 'latitude' not in df.columns:
        df['latitude'] = None
    if 'longitude' not in df.columns:
        df['longitude'] = None

    # Default ort to Bremen
    if 'ort' not in df.columns:
        df['ort'] = 'Bremen'

    # Add metadata
    df['bundesland'] = 'Bremen'
    df['data_source'] = 'GeoBremen + Schulwegweiser Bremen'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')

    return df


def save_outputs(df: pd.DataFrame):
    """Save processed data to CSV files."""
    logger.info("Saving output files...")

    # Save all schools
    all_path = RAW_DIR / "bremen_school_master.csv"
    df.to_csv(all_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {all_path} ({len(df)} schools)")

    # Save by school group
    for group in ['primary', 'secondary', 'other']:
        group_df = df[df['school_group'] == group]
        if not group_df.empty:
            group_path = RAW_DIR / f"bremen_{group}_schools.csv"
            group_df.to_csv(group_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved: {group_path} ({len(group_df)} schools)")


def print_summary(df: pd.DataFrame):
    """Print summary statistics."""
    print("\n" + "=" * 70)
    print("BREMEN SCHOOL MASTER DATA SCRAPER - PHASE 1 COMPLETE")
    print("=" * 70)

    print(f"\nTotal schools: {len(df)}")

    if 'schulform' in df.columns:
        print("\nSchools by Schulform:")
        for sf, count in df['schulform'].value_counts().items():
            print(f"  - {sf}: {count}")

    if 'school_group' in df.columns:
        print("\nSchools by group:")
        for group, count in df['school_group'].value_counts().items():
            print(f"  - {group}: {count}")

    if 'traegerschaft' in df.columns:
        print("\nSchools by Traegerschaft:")
        for t, count in df['traegerschaft'].value_counts().items():
            print(f"  - {t}: {count}")

    if 'stadtteil' in df.columns:
        non_null = df['stadtteil'].notna().sum()
        print(f"\nStadtteil coverage: {non_null}/{len(df)} ({100 * non_null / max(len(df), 1):.0f}%)")

    if 'latitude' in df.columns:
        coord_count = df['latitude'].notna().sum()
        print(f"Coordinate coverage: {coord_count}/{len(df)} ({100 * coord_count / max(len(df), 1):.0f}%)")

    if 'website' in df.columns:
        web_count = df['website'].notna().sum()
        print(f"Website coverage: {web_count}/{len(df)} ({100 * web_count / max(len(df), 1):.0f}%)")

    print(f"\nColumns ({len(df.columns)}): {list(df.columns)}")
    print("\nSample data (first 3 schools):")
    display_cols = [c for c in ['schulnummer', 'schulname', 'schulform', 'stadtteil', 'plz'] if c in df.columns]
    if display_cols:
        print(df[display_cols].head(3).to_string())

    print("\n" + "=" * 70)


def main():
    """Main entry point called by orchestrator."""
    logger.info("=" * 60)
    logger.info("Starting Bremen School Master Data Scraper (Phase 1)")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # Step 1: Download and parse Shapefile (coordinates)
        shp_df = pd.DataFrame()
        try:
            shp_content = download_file(SHAPEFILE_URL, "Schulstandorte Shapefile")
            shp_df = parse_shapefile(shp_content)
            logger.info(f"Shapefile: {len(shp_df)} school locations")
        except Exception as e:
            logger.warning(f"Could not process Shapefile: {e}")
            logger.info("Will proceed without shapefile coordinates")

        # Step 2: Download and parse Schulwegweiser Excel (details)
        excel_df = pd.DataFrame()
        try:
            excel_content = find_and_download_excel(SCHULWEGWEISER_PAGE_URL)
            if excel_content:
                excel_df = parse_excel(excel_content)
                logger.info(f"Excel: {len(excel_df)} schools")
        except Exception as e:
            logger.warning(f"Could not process Excel: {e}")

        # Step 3: Join data sources
        if not shp_df.empty and not excel_df.empty:
            df = join_data_sources(shp_df, excel_df)
        elif not shp_df.empty:
            df = shp_df
        elif not excel_df.empty:
            df = excel_df
        else:
            raise ValueError("No data sources could be loaded")

        logger.info(f"Combined: {len(df)} schools")

        # Step 4: Normalize columns
        df = normalize_columns(df)

        # Step 5: Geocode missing coordinates
        if df['latitude'].isna().any():
            df = geocode_missing_coordinates(df)

        # Step 6: Save outputs
        save_outputs(df)

        # Step 7: Summary
        print_summary(df)

        logger.info("Phase 1 complete!")
        return str(RAW_DIR / "bremen_school_master.csv")

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
