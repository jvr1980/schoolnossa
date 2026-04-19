#!/usr/bin/env python3
"""
Stuttgart School Data Combiner
Combines all enriched data into a unified master table.

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"


def find_most_enriched_file(school_type):
    candidates = [
        INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_metadata.csv",
        INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_pois.csv",
        INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_crime.csv",
        INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_transit.csv",
        INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_traffic.csv",
        RAW_DIR / f"stuttgart_{school_type}_schools.csv",
    ]
    for fp in candidates:
        if fp.exists():
            df = pd.read_csv(fp)
            logger.info(f"Loaded {len(df)} from {fp.name} ({len(df.columns)} cols)")
            return df
    raise FileNotFoundError(f"No {school_type} data found")


def standardize_columns(df):
    preferred = [
        'schulnummer', 'schulname', 'school_type', 'schulart',
        'strasse', 'plz', 'ort', 'ortsteil', 'stadt', 'bundesland',
        'latitude', 'longitude',
        'telefon', 'fax', 'email', 'website',
        'schulleitung', 'traegerschaft', 'leitung',
        'schueler_2024_25', 'lehrer_2024_25', 'sprachen',
        'gruendungsjahr', 'besonderheiten',
        'transit_stops_500m', 'transit_stop_count_1000m',
        'transit_rail_01_name', 'transit_rail_01_distance_m',
        'transit_rail_01_latitude', 'transit_rail_01_longitude', 'transit_rail_01_lines',
        'transit_rail_02_name', 'transit_rail_02_distance_m',
        'transit_rail_02_latitude', 'transit_rail_02_longitude', 'transit_rail_02_lines',
        'transit_rail_03_name', 'transit_rail_03_distance_m',
        'transit_rail_03_latitude', 'transit_rail_03_longitude', 'transit_rail_03_lines',
        'transit_tram_01_name', 'transit_tram_01_distance_m',
        'transit_tram_01_latitude', 'transit_tram_01_longitude', 'transit_tram_01_lines',
        'transit_tram_02_name', 'transit_tram_02_distance_m',
        'transit_tram_02_latitude', 'transit_tram_02_longitude', 'transit_tram_02_lines',
        'transit_tram_03_name', 'transit_tram_03_distance_m',
        'transit_tram_03_latitude', 'transit_tram_03_longitude', 'transit_tram_03_lines',
        'transit_bus_01_name', 'transit_bus_01_distance_m',
        'transit_bus_01_latitude', 'transit_bus_01_longitude', 'transit_bus_01_lines',
        'transit_bus_02_name', 'transit_bus_02_distance_m',
        'transit_bus_02_latitude', 'transit_bus_02_longitude', 'transit_bus_02_lines',
        'transit_bus_03_name', 'transit_bus_03_distance_m',
        'transit_bus_03_latitude', 'transit_bus_03_longitude', 'transit_bus_03_lines',
        'transit_all_lines_1000m', 'transit_accessibility_score',
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'traffic_accidents_fatal', 'traffic_accidents_serious', 'traffic_accidents_minor',
        'traffic_accidents_bicycle', 'traffic_accidents_pedestrian',
        'traffic_accidents_school_hours', 'traffic_nearest_accident_m',
        'crime_bezirk', 'crime_stadt',
        'crime_straftaten_2023', 'crime_haeufigkeitszahl_2023',
        'crime_aufklaerungsquote_2023', 'crime_bezirk_index',
        'data_source', 'data_retrieved',
    ]
    ordered = [c for c in preferred if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    return df[ordered + remaining]


def clean_data(df):
    df = df.copy()
    if 'schulnummer' in df.columns:
        orig = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < orig:
            logger.info(f"Removed {orig - len(df)} duplicates")

    # Normalize school_type: must be a specific German type (Gymnasium,
    # Grundschule, etc.), never the generic 'primary'/'secondary' bucket.
    # Older raw CSVs (pre-commit d6819cd) stored the generic value; copy
    # from schulart to recover.
    if 'school_type' in df.columns and 'schulart' in df.columns:
        generic = df['school_type'].astype(str).str.strip().str.lower().isin(
            ['primary', 'secondary', 'nan', 'none', '']
        )
        if generic.any():
            logger.info(f"Normalizing school_type from schulart for "
                        f"{int(generic.sum())} rows")
            df.loc[generic, 'school_type'] = df.loc[generic, 'schulart']

    numeric_cols = ['latitude', 'longitude', 'transit_stops_500m', 'transit_stop_count_1000m',
                    'transit_accessibility_score', 'traffic_accidents_total',
                    'traffic_accidents_per_year', 'crime_haeufigkeitszahl_2023', 'crime_bezirk_index']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.zfill(5).replace('0000n', '')

    return df


def combine_school_type(school_type):
    df = find_most_enriched_file(school_type)
    df = clean_data(df)
    df = standardize_columns(df)

    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = FINAL_DIR / f"stuttgart_{school_type}_school_master_table.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    parquet_path = FINAL_DIR / f"stuttgart_{school_type}_school_master_table.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {csv_path}")

    print(f"\n{'='*70}")
    print(f"STUTTGART DATA COMBINER ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(df)}, Columns: {len(df.columns)}")
    coverage = {
        'schulnummer': 'ID', 'latitude': 'Coordinates', 'telefon': 'Phone',
        'website': 'Website', 'email': 'Email', 'schulleitung': 'Principal',
        'transit_accessibility_score': 'Transit', 'traffic_accidents_total': 'Traffic',
        'crime_haeufigkeitszahl_2023': 'Crime',
    }
    for col, label in coverage.items():
        if col in df.columns:
            n = df[col].notna().sum() if col != 'telefon' else (df[col] != '').sum()
            print(f"  {label}: {n}/{len(df)} ({100*n/len(df):.0f}%)")
    print(f"{'='*70}")

    return df


def main():
    for st in ['primary', 'secondary']:
        try:
            combine_school_type(st)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
