#!/usr/bin/env python3
"""
NRW Traffic Data Enrichment (Unfallatlas)
Enriches school data with traffic accident statistics from the national Unfallatlas.

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig), German decimal separator (comma)

This script:
1. Downloads Unfallatlas CSV data for recent years
2. Filters for NRW (ULAND=05)
3. Counts accidents within configurable radius of each school
4. Classifies by severity and vehicle type
5. Merges accident metrics into school data

Author: NRW School Data Pipeline
Created: 2026-02-15
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
from typing import Dict, List, Tuple

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
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Unfallatlas download URL pattern
UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"

# Configuration
SEARCH_RADIUS_M = 500  # Radius around school for accident counting
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]  # Most recent years
NRW_ULAND_CODE = '05'  # NRW Bundesland code

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (NRW school data enrichment)',
}

# Accident severity codes
SEVERITY_MAP = {
    '1': 'fatal',
    '2': 'serious_injury',
    '3': 'minor_injury',
}


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
    """Download and parse Unfallatlas data for a given year."""
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_nrw.parquet"

    # Check cache
    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 30 * 86400:  # 30 days cache
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

            # Parse CSV with German decimal separator
            df = pd.read_csv(
                io.BytesIO(content),
                sep=';',
                encoding='utf-8-sig',
                dtype=str,
                on_bad_lines='skip'
            )

        logger.info(f"Parsed {len(df)} accidents for {year}")

        # Filter for NRW only
        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == NRW_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} NRW accidents")

        # Convert coordinates (handle German comma decimal separator)
        for coord_col in ['XGCSWGS84', 'YGCSWGS84']:
            if coord_col in df.columns:
                df[coord_col] = df[coord_col].str.replace(',', '.').astype(float, errors='ignore')

        # Rename for clarity
        df = df.rename(columns={
            'XGCSWGS84': 'accident_lon',
            'YGCSWGS84': 'accident_lat',
        })

        # Cache the NRW-filtered result
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

    # Filter accidents with valid coordinates
    valid_accidents = accidents_df[
        accidents_df['accident_lat'].notna() &
        accidents_df['accident_lon'].notna()
    ].copy()

    valid_accidents['accident_lat'] = pd.to_numeric(valid_accidents['accident_lat'], errors='coerce')
    valid_accidents['accident_lon'] = pd.to_numeric(valid_accidents['accident_lon'], errors='coerce')
    valid_accidents = valid_accidents.dropna(subset=['accident_lat', 'accident_lon'])

    logger.info(f"Valid accidents with coordinates: {len(valid_accidents)}")

    # Pre-filter: create bounding box for faster filtering
    acc_lats = valid_accidents['accident_lat'].values
    acc_lons = valid_accidents['accident_lon'].values

    school_results = {}
    schools_with_coords = schools_df[
        schools_df['latitude'].notna() & schools_df['longitude'].notna()
    ]

    logger.info(f"Schools with coordinates: {len(schools_with_coords)}")

    # Rough degree offset for pre-filtering (0.01 degree ~ 1.1 km)
    deg_offset = radius_m / 111000 * 1.5

    iterator = schools_with_coords.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Matching accidents to schools")

    for idx, school in iterator:
        schulnummer = str(school.get('schulnummer', idx))
        school_lat = float(school['latitude'])
        school_lon = float(school['longitude'])

        # Pre-filter by bounding box
        lat_mask = (acc_lats >= school_lat - deg_offset) & (acc_lats <= school_lat + deg_offset)
        lon_mask = (acc_lons >= school_lon - deg_offset) & (acc_lons <= school_lon + deg_offset)
        nearby_mask = lat_mask & lon_mask
        nearby_accidents = valid_accidents[nearby_mask]

        # Precise distance calculation
        counts = {
            'total': 0,
            'fatal': 0,
            'serious_injury': 0,
            'minor_injury': 0,
            'involving_bicycle': 0,
            'involving_pedestrian': 0,
            'involving_car': 0,
            'school_hours': 0,  # 7-17 Uhr weekdays
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

                # Severity
                severity = str(acc.get('UKATEGORIE', ''))
                if severity == '1':
                    counts['fatal'] += 1
                elif severity == '2':
                    counts['serious_injury'] += 1
                elif severity == '3':
                    counts['minor_injury'] += 1

                # Vehicle type
                if str(acc.get('IstRad', '0')) == '1':
                    counts['involving_bicycle'] += 1
                if str(acc.get('IstFuss', '0')) == '1':
                    counts['involving_pedestrian'] += 1
                if str(acc.get('IstPKW', '0')) == '1':
                    counts['involving_car'] += 1

                # School hours (7-17, weekdays Mon-Fri)
                try:
                    hour = int(acc.get('USTUNDE', -1))
                    weekday = int(acc.get('UWOCHENTAG', 0))
                    if 7 <= hour <= 17 and weekday in [2, 3, 4, 5, 6]:  # Mon=2 to Fri=6
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

    # Statistics
    schools_with_accidents = sum(1 for v in school_results.values() if v['traffic_accidents_total'] > 0)
    total_acc = sum(v['traffic_accidents_total'] for v in school_results.values())
    logger.info(f"Schools with nearby accidents: {schools_with_accidents}/{len(school_results)}")
    logger.info(f"Total accidents matched: {total_acc}")

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


def save_output(df: pd.DataFrame, school_type: str):
    """Save enriched data."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_traffic.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame, school_type: str):
    """Print enrichment summary."""
    print(f"\n{'=' * 70}")
    print(f"NRW TRAFFIC ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")

    if 'traffic_accidents_total' in df.columns:
        with_data = df['traffic_accidents_total'].notna().sum()
        print(f"Schools with traffic data: {with_data}/{len(df)}")

        if with_data > 0:
            subset = df[df['traffic_accidents_total'].notna()]
            print(f"\nAccident statistics (within {SEARCH_RADIUS_M}m):")
            print(f"  Total accidents: {subset['traffic_accidents_total'].sum():.0f}")
            print(f"  Avg per school: {subset['traffic_accidents_total'].mean():.1f}")
            print(f"  Avg per year per school: {subset['traffic_accidents_per_year'].mean():.1f}")

            if 'traffic_accidents_bicycle' in df.columns:
                print(f"  Bicycle accidents: {subset['traffic_accidents_bicycle'].sum():.0f}")
            if 'traffic_accidents_pedestrian' in df.columns:
                print(f"  Pedestrian accidents: {subset['traffic_accidents_pedestrian'].sum():.0f}")

    print(f"\n{'=' * 70}")


def enrich_schools(school_type: str = "secondary") -> pd.DataFrame:
    """Run traffic enrichment for a specific school type."""
    logger.info(f"Running traffic enrichment for {school_type} schools...")

    # Load school data
    if school_type == "primary":
        input_file = RAW_DIR / "nrw_primary_schools.csv"
    else:
        input_file = RAW_DIR / "nrw_secondary_schools.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"School data not found: {input_file}")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} {school_type} schools")

    # Load accidents
    accidents_df = load_all_accidents()

    # Count accidents near schools
    traffic_data = count_accidents_near_schools(schools_df, accidents_df)

    # Merge
    enriched_df = merge_traffic_data(schools_df, traffic_data)

    # Save
    save_output(enriched_df, school_type)

    # Summary
    print_summary(enriched_df, school_type)

    return enriched_df


def main():
    """Main entry point - enriches both primary and secondary."""
    logger.info("=" * 60)
    logger.info("Starting NRW Traffic Data Enrichment (Unfallatlas)")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Check which school files exist and enrich them
    for school_type in ['secondary', 'primary']:
        input_file = RAW_DIR / f"nrw_{school_type}_schools.csv"
        if input_file.exists():
            enrich_schools(school_type)
        else:
            logger.warning(f"No {school_type} school data found at {input_file}")


if __name__ == "__main__":
    main()
