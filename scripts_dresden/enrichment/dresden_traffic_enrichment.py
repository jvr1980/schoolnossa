#!/usr/bin/env python3
"""
Phase 2: Dresden Traffic Enrichment (Unfallatlas)

Enriches school data with traffic accident statistics from the national Unfallatlas.
Identical approach to NRW pipeline, but filters for Sachsen (ULAND=14).

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig), German decimal separator (comma)
Coordinates: UTM EPSG:25832 in source, but XGCSWGS84/YGCSWGS84 columns provide WGS84

Author: Dresden School Data Pipeline
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"
SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
SACHSEN_ULAND_CODE = '14'

HEADERS = {'User-Agent': 'SchoolNossa/1.0 (Dresden school data enrichment)'}


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def download_unfallatlas(year: int) -> pd.DataFrame:
    """Download and parse Unfallatlas data for a given year, filtered for Sachsen."""
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_sachsen.parquet"

    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 30 * 86400:
            logger.info(f"Loading {year} accident data from cache...")
            return pd.read_parquet(cache_file)

    logger.info(f"Downloading Unfallatlas {year}...")

    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content)/1024/1024:.1f} MB")

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            # Prefer LinRef file if available
            linref = [f for f in csv_files if 'LinRef' in f]
            csv_file = linref[0] if linref else csv_files[0]
            logger.info(f"Extracting: {csv_file}")

            with zf.open(csv_file) as f:
                content = f.read()

            df = pd.read_csv(io.BytesIO(content), sep=';', encoding='utf-8-sig', dtype=str, on_bad_lines='skip')

        logger.info(f"Parsed {len(df)} accidents for {year}")

        # Filter for Sachsen
        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == SACHSEN_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} Sachsen accidents")

        # Convert WGS84 coordinate columns (German comma decimal separator)
        for col in ['XGCSWGS84', 'YGCSWGS84']:
            if col in df.columns:
                df[col] = df[col].str.replace(',', '.').astype(float, errors='ignore')

        df = df.rename(columns={'XGCSWGS84': 'accident_lon', 'YGCSWGS84': 'accident_lat'})

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_file, index=False)
        return df

    except Exception as e:
        logger.warning(f"Failed to download Unfallatlas {year}: {e}")
        return pd.DataFrame()


def count_accidents_near_schools(schools_df, accidents_df, radius_m=SEARCH_RADIUS_M):
    """Count accidents within radius of each school."""
    logger.info(f"Counting accidents within {radius_m}m of each school...")

    if accidents_df.empty:
        return {}

    valid = accidents_df[accidents_df['accident_lat'].notna() & accidents_df['accident_lon'].notna()].copy()
    valid['accident_lat'] = pd.to_numeric(valid['accident_lat'], errors='coerce')
    valid['accident_lon'] = pd.to_numeric(valid['accident_lon'], errors='coerce')
    valid = valid.dropna(subset=['accident_lat', 'accident_lon'])

    acc_lats = valid['accident_lat'].values
    acc_lons = valid['accident_lon'].values
    deg_offset = radius_m / 111000 * 1.5

    schools_with_coords = schools_df[schools_df['latitude'].notna() & schools_df['longitude'].notna()]
    results = {}

    iterator = schools_with_coords.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Matching accidents to schools")

    for idx, school in iterator:
        key = str(school.get('schulnummer', idx))
        slat, slon = float(school['latitude']), float(school['longitude'])

        lat_mask = (acc_lats >= slat - deg_offset) & (acc_lats <= slat + deg_offset)
        lon_mask = (acc_lons >= slon - deg_offset) & (acc_lons <= slon + deg_offset)
        nearby = valid[lat_mask & lon_mask]

        counts = {'total': 0, 'fatal': 0, 'serious_injury': 0, 'minor_injury': 0,
                  'involving_bicycle': 0, 'involving_pedestrian': 0, 'involving_car': 0, 'school_hours': 0}
        nearest = float('inf')

        for _, acc in nearby.iterrows():
            try:
                dist = haversine_distance(slat, slon, float(acc['accident_lat']), float(acc['accident_lon']))
            except (ValueError, TypeError):
                continue

            if dist <= radius_m:
                counts['total'] += 1
                if dist < nearest:
                    nearest = dist

                sev = str(acc.get('UKATEGORIE', ''))
                if sev == '1': counts['fatal'] += 1
                elif sev == '2': counts['serious_injury'] += 1
                elif sev == '3': counts['minor_injury'] += 1

                if str(acc.get('IstRad', '0')) == '1': counts['involving_bicycle'] += 1
                if str(acc.get('IstFuss', '0')) == '1': counts['involving_pedestrian'] += 1
                if str(acc.get('IstPKW', '0')) == '1': counts['involving_car'] += 1

                try:
                    hour = int(acc.get('USTUNDE', -1))
                    weekday = int(acc.get('UWOCHENTAG', 0))
                    if 7 <= hour <= 17 and weekday in [2, 3, 4, 5, 6]:
                        counts['school_hours'] += 1
                except (ValueError, TypeError):
                    pass

        num_years = len(YEARS_TO_DOWNLOAD)
        results[key] = {
            'traffic_accidents_total': counts['total'],
            'traffic_accidents_per_year': round(counts['total'] / num_years, 1),
            'traffic_accidents_fatal': counts['fatal'],
            'traffic_accidents_serious': counts['serious_injury'],
            'traffic_accidents_minor': counts['minor_injury'],
            'traffic_accidents_bicycle': counts['involving_bicycle'],
            'traffic_accidents_pedestrian': counts['involving_pedestrian'],
            'traffic_accidents_school_hours': counts['school_hours'],
            'traffic_nearest_accident_m': round(nearest) if nearest < float('inf') else None,
            'traffic_data_years': ','.join(str(y) for y in YEARS_TO_DOWNLOAD),
            'traffic_data_source': 'Unfallatlas',
        }

    schools_with = sum(1 for v in results.values() if v['traffic_accidents_total'] > 0)
    logger.info(f"Schools with nearby accidents: {schools_with}/{len(results)}")
    return results


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Traffic Data Enrichment (Unfallatlas)")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Load school data (fallback chain)
    input_file = RAW_DIR / "dresden_schools_raw.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"School data not found: {input_file}")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} schools")

    # Load accidents
    all_dfs = [download_unfallatlas(y) for y in YEARS_TO_DOWNLOAD]
    all_dfs = [d for d in all_dfs if not d.empty]
    if not all_dfs:
        logger.warning("No accident data loaded!")
        return
    accidents_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total Sachsen accidents: {len(accidents_df)}")

    # Count accidents near schools
    traffic_data = count_accidents_near_schools(schools_df, accidents_df)

    # Merge into school data
    traffic_cols = list(next(iter(traffic_data.values())).keys()) if traffic_data else []
    for col in traffic_cols:
        schools_df[col] = None
    for idx, row in schools_df.iterrows():
        key = str(row.get('schulnummer', idx))
        if key in traffic_data:
            for col in traffic_cols:
                schools_df.at[idx, col] = traffic_data[key].get(col)

    # Save
    out_path = INTERMEDIATE_DIR / "dresden_schools_with_traffic.csv"
    schools_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out_path}")

    # Summary
    if 'traffic_accidents_total' in schools_df.columns:
        with_data = schools_df['traffic_accidents_total'].notna().sum()
        total_acc = schools_df['traffic_accidents_total'].sum()
        print(f"\n{'='*70}")
        print(f"DRESDEN TRAFFIC ENRICHMENT - COMPLETE")
        print(f"{'='*70}")
        print(f"Schools with traffic data: {with_data}/{len(schools_df)}")
        print(f"Total accidents matched: {total_acc:.0f}")
        print(f"Avg per school: {schools_df['traffic_accidents_total'].mean():.1f}")
        print(f"{'='*70}")


if __name__ == "__main__":
    main()
