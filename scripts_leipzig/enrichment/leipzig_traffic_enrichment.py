#!/usr/bin/env python3
"""
Phase 2: Leipzig Traffic Data Enrichment (Unfallatlas)
======================================================

Enriches school data with traffic accident statistics from the federal Unfallatlas.
Adapted from NRW traffic enrichment -- same data source, different ULAND filter.

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig), German decimal separator (comma)
Coordinates: EPSG:25832 (UTM Zone 32N) -- requires conversion to WGS84

Filtering: ULAND=14 (Sachsen), then spatial bounding box for Leipzig area

This script:
1. Downloads Unfallatlas CSV for recent years
2. Filters for Sachsen (ULAND=14) and Leipzig bounding box
3. Converts coordinates from EPSG:25832 (UTM) to WGS84
4. Counts accidents within 500m and 1000m radius of each school
5. Classifies by severity (UKATEGORIE) and vehicle type
6. Calculates traffic safety score
7. Merges accident metrics into school data

Output columns (Berlin schema compatible):
    traffic_accidents_500m, traffic_accidents_1000m
    traffic_severity_1_count, traffic_severity_2_count, traffic_severity_3_count
    traffic_pedestrian_accidents, traffic_bicycle_accidents
    traffic_safety_score, traffic_accidents_per_year
    traffic_accidents_school_hours, traffic_nearest_accident_m
    traffic_data_years, traffic_data_source

Input: data_leipzig/raw/leipzig_schools_raw.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_traffic.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
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
    from pyproj import Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False

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
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Unfallatlas download URL pattern
UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"

# Configuration
SEARCH_RADIUS_500 = 500   # Inner radius for accident counting
SEARCH_RADIUS_1000 = 1000  # Outer radius for accident counting
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
SACHSEN_ULAND_CODE = '14'  # Sachsen Bundesland code

# Leipzig bounding box (approximate city limits)
LEIPZIG_BBOX = {
    'lat_min': 51.24,
    'lat_max': 51.45,
    'lon_min': 12.20,
    'lon_max': 12.55,
}

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Leipzig school data enrichment)',
}

# Fallback input chain (most-enriched first, raw last)
INPUT_FALLBACKS = [
    RAW_DIR / "leipzig_schools_raw.csv",
]


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


def convert_utm_to_wgs84(easting: float, northing: float) -> Tuple[float, float]:
    """Convert EPSG:25832 (UTM Zone 32N) coordinates to WGS84 (lat, lon).

    Returns (latitude, longitude) tuple.
    """
    if not PYPROJ_AVAILABLE:
        raise ImportError(
            "pyproj is required for UTM coordinate conversion. "
            "Install with: pip install pyproj"
        )
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(easting, northing)
    return lat, lon


def download_unfallatlas(year: int) -> pd.DataFrame:
    """Download and parse Unfallatlas data for a given year, filtered to Sachsen/Leipzig."""
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_leipzig.parquet"

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

        # Filter for Sachsen only (ULAND=14)
        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == SACHSEN_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} Sachsen accidents")
        else:
            logger.warning("ULAND column not found -- cannot filter by Bundesland")
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"No Sachsen accidents found for {year}")
            return pd.DataFrame()

        # Convert UTM coordinates to WGS84
        # The EPSG:25832 CSV has XGCSWGS84/YGCSWGS84 columns with German comma decimals
        # AND/OR raw UTM columns (LINREFX, LINREFY or similar)
        # First try WGS84 columns (already present in some years)
        has_wgs84 = 'XGCSWGS84' in df.columns and 'YGCSWGS84' in df.columns

        if has_wgs84:
            # Convert German decimal separator
            for coord_col in ['XGCSWGS84', 'YGCSWGS84']:
                df[coord_col] = df[coord_col].str.replace(',', '.').astype(float, errors='ignore')

            df = df.rename(columns={
                'XGCSWGS84': 'accident_lon',
                'YGCSWGS84': 'accident_lat',
            })
            df['accident_lat'] = pd.to_numeric(df['accident_lat'], errors='coerce')
            df['accident_lon'] = pd.to_numeric(df['accident_lon'], errors='coerce')
        else:
            # Fall back to UTM conversion using LINREFX/LINREFY
            utm_x_col = None
            utm_y_col = None
            for col in df.columns:
                if 'LINREFX' in col.upper() or 'RECHTS' in col.upper():
                    utm_x_col = col
                if 'LINREFY' in col.upper() or 'HOCH' in col.upper():
                    utm_y_col = col

            if utm_x_col and utm_y_col:
                logger.info(f"Converting UTM coordinates from {utm_x_col}/{utm_y_col}")
                df[utm_x_col] = df[utm_x_col].str.replace(',', '.').astype(float, errors='ignore')
                df[utm_y_col] = df[utm_y_col].str.replace(',', '.').astype(float, errors='ignore')
                df[utm_x_col] = pd.to_numeric(df[utm_x_col], errors='coerce')
                df[utm_y_col] = pd.to_numeric(df[utm_y_col], errors='coerce')

                valid_mask = df[utm_x_col].notna() & df[utm_y_col].notna()
                logger.info(f"Valid UTM coordinates: {valid_mask.sum()}/{len(df)}")

                lats = []
                lons = []
                for _, row in df.iterrows():
                    if pd.notna(row[utm_x_col]) and pd.notna(row[utm_y_col]):
                        try:
                            lat, lon = convert_utm_to_wgs84(row[utm_x_col], row[utm_y_col])
                            lats.append(lat)
                            lons.append(lon)
                        except Exception:
                            lats.append(np.nan)
                            lons.append(np.nan)
                    else:
                        lats.append(np.nan)
                        lons.append(np.nan)

                df['accident_lat'] = lats
                df['accident_lon'] = lons
            else:
                logger.error("No coordinate columns found in Unfallatlas data")
                return pd.DataFrame()

        # Drop rows without valid coordinates
        df = df.dropna(subset=['accident_lat', 'accident_lon'])

        # Spatial filter: Leipzig bounding box
        df = df[
            (df['accident_lat'] >= LEIPZIG_BBOX['lat_min']) &
            (df['accident_lat'] <= LEIPZIG_BBOX['lat_max']) &
            (df['accident_lon'] >= LEIPZIG_BBOX['lon_min']) &
            (df['accident_lon'] <= LEIPZIG_BBOX['lon_max'])
        ].copy()
        logger.info(f"After Leipzig bounding box filter: {len(df)} accidents")

        # Cache the Leipzig-filtered result
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
    logger.info(f"Total Leipzig accidents loaded: {len(combined)} across {len(all_dfs)} years")

    return combined


def count_accidents_near_school(
    school_lat: float,
    school_lon: float,
    accidents_df: pd.DataFrame,
    acc_lats: np.ndarray,
    acc_lons: np.ndarray,
) -> Dict:
    """Count accidents within 500m and 1000m of a single school."""
    # Rough degree offset for pre-filtering (0.01 degree ~ 1.1 km)
    deg_offset_1000 = 1000 / 111000 * 1.5

    # Pre-filter by bounding box (1000m)
    lat_mask = (acc_lats >= school_lat - deg_offset_1000) & (acc_lats <= school_lat + deg_offset_1000)
    lon_mask = (acc_lons >= school_lon - deg_offset_1000) & (acc_lons <= school_lon + deg_offset_1000)
    nearby_mask = lat_mask & lon_mask
    nearby_accidents = accidents_df[nearby_mask]

    counts = {
        'total_500m': 0,
        'total_1000m': 0,
        'fatal': 0,
        'serious_injury': 0,
        'minor_injury': 0,
        'involving_bicycle': 0,
        'involving_pedestrian': 0,
        'involving_car': 0,
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

        if dist <= SEARCH_RADIUS_1000:
            counts['total_1000m'] += 1

            if dist < nearest_distance:
                nearest_distance = dist

            if dist <= SEARCH_RADIUS_500:
                counts['total_500m'] += 1

            # Severity (UKATEGORIE: 1=fatal, 2=severe, 3=minor)
            severity = str(acc.get('UKATEGORIE', ''))
            if severity == '1':
                counts['fatal'] += 1
            elif severity == '2':
                counts['serious_injury'] += 1
            elif severity == '3':
                counts['minor_injury'] += 1

            # Vehicle/participant type
            if str(acc.get('IstRad', '0')) == '1':
                counts['involving_bicycle'] += 1
            if str(acc.get('IstFuss', '0')) == '1':
                counts['involving_pedestrian'] += 1
            if str(acc.get('IstPKW', '0')) == '1':
                counts['involving_car'] += 1

            # School hours (7-17, weekdays Mon=2 to Fri=6 in Unfallatlas encoding)
            try:
                hour = int(acc.get('USTUNDE', -1))
                weekday = int(acc.get('UWOCHENTAG', 0))
                if 7 <= hour <= 17 and weekday in [2, 3, 4, 5, 6]:
                    counts['school_hours'] += 1
            except (ValueError, TypeError):
                pass

    return counts, nearest_distance


def calculate_safety_score(row: pd.Series) -> float:
    """Calculate a traffic safety score (0-100, higher = safer).

    Based on accident counts within 500m, severity weighting,
    and pedestrian/bicycle involvement.
    """
    acc_500 = row.get('traffic_accidents_500m', 0) or 0
    acc_1000 = row.get('traffic_accidents_1000m', 0) or 0
    fatal = row.get('traffic_severity_1_count', 0) or 0
    serious = row.get('traffic_severity_2_count', 0) or 0
    pedestrian = row.get('traffic_pedestrian_accidents', 0) or 0
    bicycle = row.get('traffic_bicycle_accidents', 0) or 0

    # Weighted penalty: fatal=10, serious=5, minor=1, ped/bike=2 extra
    severity_penalty = fatal * 10 + serious * 5 + (acc_1000 - fatal - serious) * 1
    vulnerable_penalty = (pedestrian + bicycle) * 2

    # Total penalty, normalized against typical urban values
    total_penalty = severity_penalty + vulnerable_penalty + acc_500 * 0.5

    # Sigmoid-like mapping to 0-100 scale
    # A school with 0 accidents scores 100; ~50 weighted penalty scores ~50
    score = 100 * math.exp(-total_penalty / 40)
    return round(max(0, min(100, score)), 1)


def enrich_schools_with_traffic(schools_df: pd.DataFrame, accidents_df: pd.DataFrame) -> pd.DataFrame:
    """Count accidents near each school and merge traffic metrics."""
    logger.info(f"Enriching {len(schools_df)} schools with traffic data...")

    df = schools_df.copy()
    num_years = len(YEARS_TO_DOWNLOAD)

    if accidents_df.empty:
        logger.warning("No accident data available -- adding empty traffic columns")
        for col in [
            'traffic_accidents_500m', 'traffic_accidents_1000m',
            'traffic_accidents_per_year',
            'traffic_severity_1_count', 'traffic_severity_2_count', 'traffic_severity_3_count',
            'traffic_pedestrian_accidents', 'traffic_bicycle_accidents',
            'traffic_accidents_school_hours', 'traffic_nearest_accident_m',
            'traffic_safety_score', 'traffic_data_years', 'traffic_data_source',
        ]:
            df[col] = None
        return df

    # Pre-extract coordinate arrays for fast bounding-box filtering
    valid_accidents = accidents_df[
        accidents_df['accident_lat'].notna() &
        accidents_df['accident_lon'].notna()
    ].copy()
    valid_accidents['accident_lat'] = pd.to_numeric(valid_accidents['accident_lat'], errors='coerce')
    valid_accidents['accident_lon'] = pd.to_numeric(valid_accidents['accident_lon'], errors='coerce')
    valid_accidents = valid_accidents.dropna(subset=['accident_lat', 'accident_lon'])

    acc_lats = valid_accidents['accident_lat'].values
    acc_lons = valid_accidents['accident_lon'].values
    logger.info(f"Valid accidents with coordinates: {len(valid_accidents)}")

    # Normalize coordinate column names
    if 'lat' in df.columns and 'latitude' not in df.columns:
        df = df.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
    schools_with_coords = df[df['latitude'].notna() & df['longitude'].notna()]
    logger.info(f"Schools with coordinates: {len(schools_with_coords)}")

    # Initialize result columns
    result_cols = {
        'traffic_accidents_500m': [],
        'traffic_accidents_1000m': [],
        'traffic_accidents_per_year': [],
        'traffic_severity_1_count': [],
        'traffic_severity_2_count': [],
        'traffic_severity_3_count': [],
        'traffic_pedestrian_accidents': [],
        'traffic_bicycle_accidents': [],
        'traffic_accidents_school_hours': [],
        'traffic_nearest_accident_m': [],
    }
    result_indices = []

    iterator = schools_with_coords.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Matching accidents to schools")

    for idx, school in iterator:
        school_lat = float(school['latitude'])
        school_lon = float(school['longitude'])

        counts, nearest_dist = count_accidents_near_school(
            school_lat, school_lon, valid_accidents, acc_lats, acc_lons
        )

        result_indices.append(idx)
        result_cols['traffic_accidents_500m'].append(counts['total_500m'])
        result_cols['traffic_accidents_1000m'].append(counts['total_1000m'])
        result_cols['traffic_accidents_per_year'].append(
            round(counts['total_1000m'] / num_years, 1)
        )
        result_cols['traffic_severity_1_count'].append(counts['fatal'])
        result_cols['traffic_severity_2_count'].append(counts['serious_injury'])
        result_cols['traffic_severity_3_count'].append(counts['minor_injury'])
        result_cols['traffic_pedestrian_accidents'].append(counts['involving_pedestrian'])
        result_cols['traffic_bicycle_accidents'].append(counts['involving_bicycle'])
        result_cols['traffic_accidents_school_hours'].append(counts['school_hours'])
        result_cols['traffic_nearest_accident_m'].append(
            round(nearest_dist) if nearest_dist < float('inf') else None
        )

    # Assign results back to dataframe
    for col, values in result_cols.items():
        df[col] = None
        df.loc[result_indices, col] = values

    # Add metadata columns
    df['traffic_data_years'] = ','.join(str(y) for y in YEARS_TO_DOWNLOAD)
    df['traffic_data_source'] = 'Unfallatlas'

    # Calculate safety score
    df['traffic_safety_score'] = df.apply(calculate_safety_score, axis=1)

    # Statistics
    with_500m = (df['traffic_accidents_500m'].fillna(0) > 0).sum()
    with_1000m = (df['traffic_accidents_1000m'].fillna(0) > 0).sum()
    total_500 = df['traffic_accidents_500m'].fillna(0).sum()
    total_1000 = df['traffic_accidents_1000m'].fillna(0).sum()
    logger.info(f"Schools with accidents within 500m: {with_500m}/{len(schools_with_coords)}")
    logger.info(f"Schools with accidents within 1000m: {with_1000m}/{len(schools_with_coords)}")
    logger.info(f"Total accidents matched (500m): {total_500:.0f}")
    logger.info(f"Total accidents matched (1000m): {total_1000:.0f}")

    return df


def load_school_data() -> pd.DataFrame:
    """Load school data using fallback input chain."""
    for input_path in INPUT_FALLBACKS:
        if input_path.exists():
            logger.info(f"Loading school data from: {input_path}")
            df = pd.read_csv(input_path)
            logger.info(f"Loaded {len(df)} schools")
            return df

    raise FileNotFoundError(
        f"No school data found. Tried:\n" +
        "\n".join(f"  - {p}" for p in INPUT_FALLBACKS)
    )


def print_summary(df: pd.DataFrame):
    """Print enrichment summary."""
    print(f"\n{'=' * 70}")
    print("LEIPZIG TRAFFIC ENRICHMENT - COMPLETE")
    print(f"{'=' * 70}")
    print(f"\nTotal schools: {len(df)}")

    if 'traffic_accidents_500m' in df.columns:
        with_data = df['traffic_accidents_500m'].notna().sum()
        print(f"Schools with traffic data: {with_data}/{len(df)}")

        if with_data > 0:
            subset = df[df['traffic_accidents_500m'].notna()]
            print(f"\nAccident statistics:")
            print(f"  Within 500m  -- total: {subset['traffic_accidents_500m'].sum():.0f}, "
                  f"avg: {subset['traffic_accidents_500m'].mean():.1f}")
            print(f"  Within 1000m -- total: {subset['traffic_accidents_1000m'].sum():.0f}, "
                  f"avg: {subset['traffic_accidents_1000m'].mean():.1f}")

            if 'traffic_severity_1_count' in df.columns:
                print(f"  Fatal (cat 1): {subset['traffic_severity_1_count'].sum():.0f}")
                print(f"  Serious (cat 2): {subset['traffic_severity_2_count'].sum():.0f}")
                print(f"  Minor (cat 3): {subset['traffic_severity_3_count'].sum():.0f}")

            if 'traffic_bicycle_accidents' in df.columns:
                print(f"  Bicycle: {subset['traffic_bicycle_accidents'].sum():.0f}")
            if 'traffic_pedestrian_accidents' in df.columns:
                print(f"  Pedestrian: {subset['traffic_pedestrian_accidents'].sum():.0f}")

            if 'traffic_safety_score' in df.columns:
                print(f"\nSafety score: mean={subset['traffic_safety_score'].mean():.1f}, "
                      f"min={subset['traffic_safety_score'].min():.1f}, "
                      f"max={subset['traffic_safety_score'].max():.1f}")

    print(f"\nYears: {', '.join(str(y) for y in YEARS_TO_DOWNLOAD)}")
    print(f"{'=' * 70}")


def main():
    """Enrich Leipzig schools with Unfallatlas traffic accident data."""
    logger.info("=" * 60)
    logger.info("Starting Leipzig Traffic Data Enrichment (Unfallatlas)")
    logger.info("=" * 60)

    # Ensure directories exist
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Load school data
    schools_df = load_school_data()

    # Load all accident data (filtered to Sachsen + Leipzig bbox)
    accidents_df = load_all_accidents()

    # Enrich schools with traffic data
    enriched_df = enrich_schools_with_traffic(schools_df, accidents_df)

    # Save output
    output_path = INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv"
    enriched_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path}")

    # Print summary
    print_summary(enriched_df)

    return enriched_df


if __name__ == "__main__":
    main()
