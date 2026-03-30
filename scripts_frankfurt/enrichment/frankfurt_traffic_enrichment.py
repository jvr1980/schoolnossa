#!/usr/bin/env python3
"""
Frankfurt Traffic Data Enrichment (Unfallatlas)
Enriches school data with traffic accident statistics from the national Unfallatlas.

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig), German decimal separator (comma)
Filter: ULAND=06 (Hessen), UKREIS=12 (Frankfurt am Main)

Same data source and format as NRW pipeline, just different geographic filter.

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
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
DATA_DIR = PROJECT_ROOT / "data_frankfurt"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"
SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
HESSEN_ULAND_CODE = '06'
FRANKFURT_UKREIS_CODE = '12'

HEADERS = {'User-Agent': 'SchoolNossa/1.0 (Frankfurt school data enrichment)'}


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def download_unfallatlas(year: int) -> pd.DataFrame:
    """Download and parse Unfallatlas data, filtered for Hessen."""
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_hessen.parquet"

    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 30 * 86400:
            logger.info(f"Loading {year} accident data from cache...")
            return pd.read_parquet(cache_file)

    logger.info(f"Downloading Unfallatlas {year}...")
    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content)/1024/1024:.1f} MB")

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                return pd.DataFrame()
            with zf.open(csv_files[0]) as f:
                content = f.read()

            df = pd.read_csv(io.BytesIO(content), sep=';', encoding='utf-8-sig',
                             dtype=str, on_bad_lines='skip')

        # Filter for Hessen
        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == HESSEN_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} Hessen accidents")

        # Convert WGS84 coordinates
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


def load_all_accidents() -> pd.DataFrame:
    """Load and combine accident data for all years, filtered to Frankfurt."""
    all_dfs = []
    for year in YEARS_TO_DOWNLOAD:
        df = download_unfallatlas(year)
        if not df.empty:
            # Further filter to Frankfurt (UKREIS=12)
            if 'UKREIS' in df.columns:
                df = df[df['UKREIS'] == FRANKFURT_UKREIS_CODE].copy()
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total Frankfurt accidents: {len(combined)} across {len(all_dfs)} years")
    return combined


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

    results = {}
    schools_with_coords = schools_df[schools_df['latitude'].notna() & schools_df['longitude'].notna()]
    deg_offset = radius_m / 111000 * 1.5

    iterator = schools_with_coords.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Matching accidents")

    for idx, school in iterator:
        schulnummer = str(school.get('schulnummer', idx))
        slat, slon = float(school['latitude']), float(school['longitude'])

        lat_mask = (acc_lats >= slat - deg_offset) & (acc_lats <= slat + deg_offset)
        lon_mask = (acc_lons >= slon - deg_offset) & (acc_lons <= slon + deg_offset)
        nearby = valid[lat_mask & lon_mask]

        counts = {'total': 0, 'fatal': 0, 'serious_injury': 0, 'minor_injury': 0,
                  'involving_bicycle': 0, 'involving_pedestrian': 0, 'involving_car': 0,
                  'school_hours': 0}
        nearest = float('inf')

        for _, acc in nearby.iterrows():
            try:
                dist = haversine_distance(slat, slon, float(acc['accident_lat']), float(acc['accident_lon']))
            except (ValueError, TypeError):
                continue
            if dist <= radius_m:
                counts['total'] += 1
                nearest = min(nearest, dist)
                sev = str(acc.get('UKATEGORIE', ''))
                if sev == '1': counts['fatal'] += 1
                elif sev == '2': counts['serious_injury'] += 1
                elif sev == '3': counts['minor_injury'] += 1
                if str(acc.get('IstRad', '0')) == '1': counts['involving_bicycle'] += 1
                if str(acc.get('IstFuss', '0')) == '1': counts['involving_pedestrian'] += 1
                if str(acc.get('IstPKW', '0')) == '1': counts['involving_car'] += 1
                try:
                    h = int(acc.get('USTUNDE', -1))
                    wd = int(acc.get('UWOCHENTAG', 0))
                    if 7 <= h <= 17 and wd in [2,3,4,5,6]:
                        counts['school_hours'] += 1
                except (ValueError, TypeError):
                    pass

        ny = len(YEARS_TO_DOWNLOAD)
        results[schulnummer] = {
            'traffic_accidents_total': counts['total'],
            'traffic_accidents_per_year': round(counts['total'] / ny, 1),
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

    with_acc = sum(1 for v in results.values() if v['traffic_accidents_total'] > 0)
    logger.info(f"Schools with nearby accidents: {with_acc}/{len(results)}")
    return results


def enrich_schools(school_type="secondary"):
    """Run traffic enrichment for a school type."""
    input_file = RAW_DIR / f"frankfurt_{school_type}_schools.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Not found: {input_file}")

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} {school_type} schools")

    accidents = load_all_accidents()
    traffic_data = count_accidents_near_schools(df, accidents)

    # Merge
    traffic_cols = list(next(iter(traffic_data.values())).keys()) if traffic_data else []
    for col in traffic_cols:
        if col not in df.columns:
            df[col] = None
    for idx, row in df.iterrows():
        sn = str(row.get('schulnummer', idx))
        if sn in traffic_data:
            for col in traffic_cols:
                df.at[idx, col] = traffic_data[sn].get(col)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_traffic.csv"
    df.to_csv(out, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out}")

    print(f"\n{'='*70}\nFRANKFURT TRAFFIC ENRICHMENT ({school_type.upper()}) - COMPLETE\n{'='*70}")
    print(f"Schools: {len(df)}")
    if 'traffic_accidents_total' in df.columns:
        wd = df['traffic_accidents_total'].notna().sum()
        print(f"With traffic data: {wd}/{len(df)}")
        if wd > 0:
            print(f"Avg accidents/year: {df['traffic_accidents_per_year'].mean():.1f}")
    print(f"{'='*70}")
    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Frankfurt Traffic Enrichment (Unfallatlas)")
    logger.info("=" * 60)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for st in ['secondary', 'primary']:
        f = RAW_DIR / f"frankfurt_{st}_schools.csv"
        if f.exists():
            enrich_schools(st)
        else:
            logger.warning(f"No {st} data at {f}")


if __name__ == "__main__":
    main()
