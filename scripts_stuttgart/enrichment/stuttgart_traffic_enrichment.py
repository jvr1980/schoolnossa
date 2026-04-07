#!/usr/bin/env python3
"""
Stuttgart Traffic Data Enrichment (Unfallatlas)
Enriches school data with traffic accident statistics.

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig), German decimal separator
Filter: ULAND=08 (Baden-Württemberg)

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"

SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
BW_ULAND_CODE = '08'  # Baden-Württemberg

HEADERS = {'User-Agent': 'SchoolNossa/1.0 (Stuttgart school data enrichment)'}


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def download_unfallatlas(year):
    url = UNFALLATLAS_URL_TEMPLATE.format(year=year)
    cache_file = CACHE_DIR / f"unfallatlas_{year}_bw.parquet"

    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 30 * 86400:
            logger.info(f"Loading {year} from cache...")
            return pd.read_parquet(cache_file)

    logger.info(f"Downloading Unfallatlas {year}...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=120)
        r.raise_for_status()
        logger.info(f"Downloaded {len(r.content)/1024/1024:.1f} MB")

        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv') and 'LinRef' in f]
            if not csv_files:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                return pd.DataFrame()

            with zf.open(csv_files[0]) as f:
                content = f.read()

            df = pd.read_csv(io.BytesIO(content), sep=';', encoding='utf-8-sig',
                             dtype=str, on_bad_lines='skip')

        logger.info(f"Parsed {len(df)} accidents for {year}")

        if 'ULAND' in df.columns:
            df = df[df['ULAND'] == BW_ULAND_CODE].copy()
            logger.info(f"Filtered to {len(df)} BW accidents")

        for col in ['XGCSWGS84', 'YGCSWGS84']:
            if col in df.columns:
                df[col] = df[col].str.replace(',', '.').astype(float, errors='ignore')

        df = df.rename(columns={'XGCSWGS84': 'accident_lon', 'YGCSWGS84': 'accident_lat'})

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_file, index=False)
        return df

    except Exception as e:
        logger.warning(f"Failed for {year}: {e}")
        return pd.DataFrame()


def load_all_accidents():
    all_dfs = []
    for year in YEARS_TO_DOWNLOAD:
        df = download_unfallatlas(year)
        if not df.empty:
            all_dfs.append(df)
    if not all_dfs:
        return pd.DataFrame()
    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total accidents: {len(combined)}")
    return combined


def count_accidents_near_schools(schools_df, accidents_df, radius_m=SEARCH_RADIUS_M):
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
        iterator = tqdm(list(iterator), desc="Matching accidents")

    for idx, school in iterator:
        schulnummer = str(school.get('schulnummer', idx))
        s_lat, s_lon = float(school['latitude']), float(school['longitude'])

        lat_mask = (acc_lats >= s_lat - deg_offset) & (acc_lats <= s_lat + deg_offset)
        lon_mask = (acc_lons >= s_lon - deg_offset) & (acc_lons <= s_lon + deg_offset)
        nearby = valid[lat_mask & lon_mask]

        counts = {'total': 0, 'fatal': 0, 'serious_injury': 0, 'minor_injury': 0,
                  'involving_bicycle': 0, 'involving_pedestrian': 0, 'school_hours': 0}
        nearest = float('inf')

        for _, acc in nearby.iterrows():
            try:
                dist = haversine_distance(s_lat, s_lon, float(acc['accident_lat']), float(acc['accident_lon']))
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
                try:
                    hour = int(acc.get('USTUNDE', -1))
                    wday = int(acc.get('UWOCHENTAG', 0))
                    if 7 <= hour <= 17 and wday in [2,3,4,5,6]:
                        counts['school_hours'] += 1
                except (ValueError, TypeError):
                    pass

        n_years = len(YEARS_TO_DOWNLOAD)
        results[schulnummer] = {
            'traffic_accidents_total': counts['total'],
            'traffic_accidents_per_year': round(counts['total'] / n_years, 1),
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

    matched = sum(1 for v in results.values() if v['traffic_accidents_total'] > 0)
    logger.info(f"Schools with nearby accidents: {matched}/{len(results)}")
    return results


def merge_traffic_data(schools_df, traffic_data):
    df = schools_df.copy()
    cols = ['traffic_accidents_total', 'traffic_accidents_per_year',
            'traffic_accidents_fatal', 'traffic_accidents_serious', 'traffic_accidents_minor',
            'traffic_accidents_bicycle', 'traffic_accidents_pedestrian',
            'traffic_accidents_school_hours', 'traffic_nearest_accident_m',
            'traffic_data_years', 'traffic_data_source']
    for col in cols:
        if col not in df.columns:
            df[col] = None
    for idx, row in df.iterrows():
        key = str(row.get('schulnummer', idx))
        if key in traffic_data:
            for col in cols:
                df.at[idx, col] = traffic_data[key].get(col)
    return df


def enrich_schools(school_type='secondary'):
    input_file = RAW_DIR / f"stuttgart_{school_type}_schools.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Not found: {input_file}")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} {school_type} schools")

    accidents_df = load_all_accidents()
    traffic_data = count_accidents_near_schools(schools_df, accidents_df)
    enriched = merge_traffic_data(schools_df, traffic_data)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_traffic.csv"
    enriched.to_csv(out, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out}")

    print(f"\n{'='*70}")
    print(f"STUTTGART TRAFFIC ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(enriched)}")
    if 'traffic_accidents_total' in enriched.columns:
        with_data = enriched['traffic_accidents_total'].notna().sum()
        print(f"With traffic data: {with_data}/{len(enriched)}")
        if with_data > 0:
            sub = enriched[enriched['traffic_accidents_total'].notna()]
            print(f"Total accidents: {sub['traffic_accidents_total'].sum():.0f}")
            print(f"Avg per school: {sub['traffic_accidents_total'].mean():.1f}")
    print(f"{'='*70}")

    return enriched


def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for st in ['primary', 'secondary']:
        if (RAW_DIR / f"stuttgart_{st}_schools.csv").exists():
            enrich_schools(st)


if __name__ == "__main__":
    main()
