#!/usr/bin/env python3
"""
Phase 2: Munich Traffic Data Enrichment (Unfallatlas)

Enriches school data with traffic accident statistics from the national Unfallatlas.
Same approach as NRW pipeline — accident-based, not sensor-based.

Data Source: https://unfallatlas.statistikportal.de/
Format: CSV (semicolon, UTF-8-sig), German comma decimal separator
Filter: ULAND=09 (Bayern), then München bounding box
Coordinate system: WGS84 (EPSG:4326)

Input: data_munich/intermediate/munich_secondary_schools.csv
Output: data_munich/intermediate/munich_secondary_schools_with_traffic.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Unfallatlas download URL pattern — NRW hosts these files; same data nationwide
UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"

SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
BAYERN_ULAND_CODE = '09'

# Munich bounding box for pre-filtering
MUNICH_BBOX = {
    'lat_min': 48.06, 'lat_max': 48.25,
    'lon_min': 11.36, 'lon_max': 11.72,
}

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Munich school data enrichment)',
}

SEVERITY_MAP = {'1': 'fatal', '2': 'serious_injury', '3': 'minor_injury'}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in meters between two WGS84 points."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def download_unfallatlas(year: int) -> pd.DataFrame:
    """Download and parse Unfallatlas data for a given year, filtered to Bayern/Munich."""
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_munich.parquet"

    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 30 * 86400:
            logger.info(f"Loading {year} accident data from cache...")
            return pd.read_parquet(cache_file)

    logger.info(f"Downloading Unfallatlas {year} from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.1f} MB")

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            # Prefer LinRef CSV if available
            linref = [f for f in csv_files if 'LinRef' in f]
            csv_file = linref[0] if linref else csv_files[0]

            with zf.open(csv_file) as f:
                content = f.read()

            df = pd.read_csv(
                io.BytesIO(content), sep=';', encoding='utf-8-sig',
                dtype=str, on_bad_lines='skip'
            )

        logger.info(f"Parsed {len(df)} accidents for {year}")

        # Filter for Bayern
        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == BAYERN_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} Bayern accidents")

        # Convert coordinates (German comma decimal separator)
        for col in ['XGCSWGS84', 'YGCSWGS84']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', '.'), errors='coerce')

        df = df.rename(columns={'XGCSWGS84': 'accident_lon', 'YGCSWGS84': 'accident_lat'})

        # Filter to Munich bounding box
        df = df[
            (df['accident_lat'] >= MUNICH_BBOX['lat_min']) &
            (df['accident_lat'] <= MUNICH_BBOX['lat_max']) &
            (df['accident_lon'] >= MUNICH_BBOX['lon_min']) &
            (df['accident_lon'] <= MUNICH_BBOX['lon_max'])
        ].copy()
        logger.info(f"Munich area: {len(df)} accidents")

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_file, index=False)
        return df

    except Exception as e:
        logger.warning(f"Failed to download Unfallatlas {year}: {e}")
        return pd.DataFrame()


def load_all_accidents() -> pd.DataFrame:
    """Load accident data for all configured years."""
    all_dfs = []
    for year in YEARS_TO_DOWNLOAD:
        df = download_unfallatlas(year)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("No accident data loaded!")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total accidents: {len(combined)} across {len(all_dfs)} years")
    return combined


def count_accidents_near_schools(schools_df, accidents_df, radius_m=SEARCH_RADIUS_M):
    """Count accidents within radius of each school."""
    if accidents_df.empty:
        return {}

    valid = accidents_df.dropna(subset=['accident_lat', 'accident_lon']).copy()
    valid['accident_lat'] = pd.to_numeric(valid['accident_lat'], errors='coerce')
    valid['accident_lon'] = pd.to_numeric(valid['accident_lon'], errors='coerce')
    valid = valid.dropna(subset=['accident_lat', 'accident_lon'])

    acc_lats = valid['accident_lat'].values
    acc_lons = valid['accident_lon'].values
    deg_offset = radius_m / 111000 * 1.5

    results = {}
    with_coords = schools_df[schools_df['latitude'].notna() & schools_df['longitude'].notna()]

    iterator = list(with_coords.iterrows())
    if TQDM_AVAILABLE:
        iterator = tqdm(iterator, desc="Matching accidents")

    for idx, school in iterator:
        key = str(school.get('schulnummer', idx))
        slat, slon = float(school['latitude']), float(school['longitude'])

        nearby_mask = (
            (acc_lats >= slat - deg_offset) & (acc_lats <= slat + deg_offset) &
            (acc_lons >= slon - deg_offset) & (acc_lons <= slon + deg_offset)
        )
        nearby = valid[nearby_mask]

        counts = {'total': 0, 'fatal': 0, 'serious_injury': 0, 'minor_injury': 0,
                  'involving_bicycle': 0, 'involving_pedestrian': 0}

        for _, acc in nearby.iterrows():
            try:
                dist = haversine_distance(slat, slon, float(acc['accident_lat']), float(acc['accident_lon']))
            except (ValueError, TypeError):
                continue
            if dist <= radius_m:
                counts['total'] += 1
                sev = SEVERITY_MAP.get(str(acc.get('UKATEGORIE', '')), '')
                if sev:
                    counts[sev] = counts.get(sev, 0) + 1
                istrad = str(acc.get('IstRad', '0'))
                if istrad == '1':
                    counts['involving_bicycle'] += 1
                istfuss = str(acc.get('IstFuss', '0'))
                if istfuss == '1':
                    counts['involving_pedestrian'] += 1

        results[key] = counts

    return results


def find_input_file(school_type='secondary'):
    """Find the best available input file (fallback chain)."""
    candidates = [
        INTERMEDIATE_DIR / f"munich_{school_type}_schools.csv",
    ]
    for f in candidates:
        if f.exists():
            return f
    raise FileNotFoundError(f"No {school_type} school data found. Run Phase 1 first.")


def enrich_schools(school_type='secondary'):
    logger.info(f"Enriching {school_type} schools with traffic data...")

    input_file = find_input_file(school_type)
    logger.info(f"Input: {input_file}")
    schools = pd.read_csv(input_file, dtype=str)
    schools['latitude'] = pd.to_numeric(schools['latitude'], errors='coerce')
    schools['longitude'] = pd.to_numeric(schools['longitude'], errors='coerce')

    logger.info(f"Loaded {len(schools)} schools ({schools['latitude'].notna().sum()} with coordinates)")

    accidents = load_all_accidents()
    results = count_accidents_near_schools(schools, accidents)

    # Add traffic columns
    for col in ['traffic_accidents_total', 'traffic_accidents_fatal',
                'traffic_accidents_serious_injury', 'traffic_accidents_minor_injury',
                'traffic_accidents_bicycle', 'traffic_accidents_pedestrian']:
        schools[col] = None

    for idx, row in schools.iterrows():
        key = str(row.get('schulnummer', idx))
        if key in results:
            c = results[key]
            schools.at[idx, 'traffic_accidents_total'] = c['total']
            schools.at[idx, 'traffic_accidents_fatal'] = c['fatal']
            schools.at[idx, 'traffic_accidents_serious_injury'] = c['serious_injury']
            schools.at[idx, 'traffic_accidents_minor_injury'] = c['minor_injury']
            schools.at[idx, 'traffic_accidents_bicycle'] = c['involving_bicycle']
            schools.at[idx, 'traffic_accidents_pedestrian'] = c['involving_pedestrian']

    # Safety score
    if 'traffic_accidents_total' in schools.columns:
        total = pd.to_numeric(schools['traffic_accidents_total'], errors='coerce')
        schools['traffic_safety_score'] = np.where(
            total.notna(),
            np.clip(100 - total * 2, 0, 100).round(1),
            None
        )

    output_path = INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_traffic.csv"
    schools.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path} ({len(schools)} schools)")

    enriched = schools['traffic_accidents_total'].notna().sum()
    print(f"\nTraffic enrichment ({school_type}): {enriched}/{len(schools)} schools enriched")
    print(f"Total accidents within {SEARCH_RADIUS_M}m: {pd.to_numeric(schools['traffic_accidents_total'], errors='coerce').sum():.0f}")

    return schools


def main(school_type='secondary'):
    logger.info("=" * 60)
    logger.info(f"Phase 2: Munich Traffic Enrichment ({school_type})")
    logger.info("=" * 60)
    return enrich_schools(school_type)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
