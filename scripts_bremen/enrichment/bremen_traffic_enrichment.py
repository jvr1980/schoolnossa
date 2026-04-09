#!/usr/bin/env python3
"""
Phase 2: Bremen Traffic Enrichment (Unfallatlas)

Enriches schools with traffic accident data from the national Unfallatlas.
Filters for ULAND='04' (Bremen). Same approach as NRW pipeline.

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig, German comma decimal separator)

Input (fallback chain):
    1. data_bremen/raw/bremen_school_master.csv

Output:
    - data_bremen/intermediate/bremen_schools_with_traffic.csv

Reference: scripts_nrw/enrichment/nrw_traffic_enrichment.py
Author: Bremen School Data Pipeline
Created: 2026-04-07
"""

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import math
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

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

# Unfallatlas download URL pattern
UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"

# Configuration
SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
BREMEN_ULAND_CODE = '04'

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Bremen school data enrichment)',
}


def find_input_file() -> Path:
    """Find the most-enriched input file, falling back through the chain."""
    fallback_chain = [
        ("bremen_school_master.csv", RAW_DIR),
    ]
    for filename, directory in fallback_chain:
        path = directory / filename
        if path.exists():
            logger.info(f"Using input: {path.name}")
            return path
    raise FileNotFoundError("No input file found for Bremen schools")


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great circle distance between two points in meters."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def download_unfallatlas(year: int) -> pd.DataFrame:
    """Download and parse Unfallatlas data for a given year, filtered for Bremen."""
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_bremen.parquet"

    # Check cache
    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 30 * 86400:  # 30 days
            logger.info(f"Loading {year} accident data from cache...")
            return pd.read_parquet(cache_file)

    logger.info(f"Downloading Unfallatlas {year} from {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.1f} MB")

        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv') and 'LinRef' in f]
            if not csv_files:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]

            if not csv_files:
                logger.error(f"No CSV file found in ZIP for {year}")
                return pd.DataFrame()

            csv_file = csv_files[0]
            logger.info(f"Extracting: {csv_file}")

            with zf.open(csv_file) as f:
                content = f.read()

            df = pd.read_csv(
                io.BytesIO(content),
                sep=';',
                encoding='utf-8-sig',
                dtype=str,
                on_bad_lines='skip'
            )

        logger.info(f"Parsed {len(df)} accidents for {year}")

        # Filter for Bremen
        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == BREMEN_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} Bremen accidents")

        # Convert coordinates (German comma decimal separator)
        for coord_col in ['XGCSWGS84', 'YGCSWGS84']:
            if coord_col in df.columns:
                df[coord_col] = df[coord_col].str.replace(',', '.').astype(float, errors='ignore')

        df = df.rename(columns={
            'XGCSWGS84': 'accident_lon',
            'YGCSWGS84': 'accident_lat',
        })

        # Cache
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_file, index=False)
        logger.info(f"Cached to {cache_file}")

        return df

    except Exception as e:
        logger.warning(f"Failed to download Unfallatlas {year}: {e}")
        return pd.DataFrame()


def load_all_accidents() -> pd.DataFrame:
    """Load accident data for all configured years."""
    logger.info("Loading accident data for all years...")

    all_dfs = []
    for year in YEARS_TO_DOWNLOAD:
        df = download_unfallatlas(year)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("No accident data loaded!")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total accidents loaded: {len(combined)} across {len(all_dfs)} years")
    return combined


def count_accidents_near_schools(
    schools_df: pd.DataFrame,
    accidents_df: pd.DataFrame,
    radius_m: int = SEARCH_RADIUS_M
) -> Dict[str, Dict]:
    """Count accidents within radius of each school."""
    logger.info(f"Counting accidents within {radius_m}m of each school...")

    if accidents_df.empty:
        return {}

    # Filter valid accident coordinates
    valid_accidents = accidents_df[
        accidents_df['accident_lat'].notna() & accidents_df['accident_lon'].notna()
    ].copy()
    valid_accidents['accident_lat'] = pd.to_numeric(valid_accidents['accident_lat'], errors='coerce')
    valid_accidents['accident_lon'] = pd.to_numeric(valid_accidents['accident_lon'], errors='coerce')
    valid_accidents = valid_accidents.dropna(subset=['accident_lat', 'accident_lon'])

    logger.info(f"Valid accidents with coordinates: {len(valid_accidents)}")

    acc_lats = valid_accidents['accident_lat'].values
    acc_lons = valid_accidents['accident_lon'].values

    schools_with_coords = schools_df[
        schools_df['latitude'].notna() & schools_df['longitude'].notna()
    ]
    logger.info(f"Schools with coordinates: {len(schools_with_coords)}")

    deg_offset = radius_m / 111000 * 1.5
    school_results = {}

    iterator = schools_with_coords.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Matching accidents to schools")

    for idx, school in iterator:
        schulnummer = str(school.get('schulnummer', idx))
        school_lat = float(school['latitude'])
        school_lon = float(school['longitude'])

        # Bounding box pre-filter
        lat_mask = (acc_lats >= school_lat - deg_offset) & (acc_lats <= school_lat + deg_offset)
        lon_mask = (acc_lons >= school_lon - deg_offset) & (acc_lons <= school_lon + deg_offset)
        nearby_accidents = valid_accidents[lat_mask & lon_mask]

        counts = {
            'total': 0, 'fatal': 0, 'serious_injury': 0, 'minor_injury': 0,
            'involving_bicycle': 0, 'involving_pedestrian': 0, 'involving_car': 0,
            'school_hours': 0,
        }
        nearest_distance = float('inf')

        for _, acc in nearby_accidents.iterrows():
            try:
                dist = haversine_distance(
                    school_lat, school_lon,
                    float(acc['accident_lat']), float(acc['accident_lon'])
                )
            except (ValueError, TypeError):
                continue

            if dist <= radius_m:
                counts['total'] += 1
                if dist < nearest_distance:
                    nearest_distance = dist

                severity = str(acc.get('UKATEGORIE', ''))
                if severity == '1':
                    counts['fatal'] += 1
                elif severity == '2':
                    counts['serious_injury'] += 1
                elif severity == '3':
                    counts['minor_injury'] += 1

                if str(acc.get('IstRad', '0')) == '1':
                    counts['involving_bicycle'] += 1
                if str(acc.get('IstFuss', '0')) == '1':
                    counts['involving_pedestrian'] += 1
                if str(acc.get('IstPKW', '0')) == '1':
                    counts['involving_car'] += 1

                try:
                    hour = int(acc.get('USTUNDE', -1))
                    weekday = int(acc.get('UWOCHENTAG', 0))
                    if 7 <= hour <= 17 and weekday in [2, 3, 4, 5, 6]:
                        counts['school_hours'] += 1
                except (ValueError, TypeError):
                    pass

        num_years = len(YEARS_TO_DOWNLOAD)
        school_results[schulnummer] = {
            'traffic_accidents_total': counts['total'],
            'traffic_accidents_per_year': round(counts['total'] / num_years, 1),
            'traffic_accidents_fatal': counts['fatal'],
            'traffic_accidents_serious': counts['serious_injury'],
            'traffic_accidents_minor': counts['minor_injury'],
            'traffic_accidents_bicycle': counts['involving_bicycle'],
            'traffic_accidents_pedestrian': counts['involving_pedestrian'],
            'traffic_accidents_school_hours': counts['school_hours'],
            'traffic_nearest_accident_m': round(nearest_distance) if nearest_distance < float('inf') else None,
            'traffic_data_years': ','.join(str(y) for y in YEARS_TO_DOWNLOAD),
            'traffic_data_source': 'Unfallatlas',
        }

    schools_with_accidents = sum(1 for v in school_results.values() if v['traffic_accidents_total'] > 0)
    logger.info(f"Schools with nearby accidents: {schools_with_accidents}/{len(school_results)}")

    return school_results


def merge_traffic_data(schools_df: pd.DataFrame, traffic_data: Dict[str, Dict]) -> pd.DataFrame:
    """Merge traffic accident data into school dataframe."""
    logger.info("Merging traffic data...")
    df = schools_df.copy()

    traffic_columns = [
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'traffic_accidents_fatal', 'traffic_accidents_serious', 'traffic_accidents_minor',
        'traffic_accidents_bicycle', 'traffic_accidents_pedestrian',
        'traffic_accidents_school_hours', 'traffic_nearest_accident_m',
        'traffic_data_years', 'traffic_data_source',
    ]

    for col in traffic_columns:
        if col not in df.columns:
            df[col] = None

    for idx, row in df.iterrows():
        schulnummer = str(row.get('schulnummer', idx))
        if schulnummer in traffic_data:
            for col in traffic_columns:
                df.at[idx, col] = traffic_data[schulnummer].get(col)

    return df


def main() -> str:
    """Main entry point called by orchestrator."""
    logger.info("=" * 60)
    logger.info("Starting Bremen Traffic Enrichment (Unfallatlas, ULAND=04)")
    logger.info("=" * 60)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    input_file = find_input_file()
    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} schools from {input_file.name}")

    # Load accidents
    accidents_df = load_all_accidents()

    # Count and merge
    traffic_data = count_accidents_near_schools(schools_df, accidents_df)
    enriched_df = merge_traffic_data(schools_df, traffic_data)

    # Save
    output_path = INTERMEDIATE_DIR / "bremen_schools_with_traffic.csv"
    enriched_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path} ({len(enriched_df)} schools)")

    # Summary
    if 'traffic_accidents_total' in enriched_df.columns:
        with_data = enriched_df['traffic_accidents_total'].notna().sum()
        total_acc = enriched_df['traffic_accidents_total'].sum()
        logger.info(f"Schools with traffic data: {with_data}/{len(enriched_df)}")
        logger.info(f"Total accidents matched: {total_acc:.0f}")

    return str(output_path)


if __name__ == "__main__":
    main()
