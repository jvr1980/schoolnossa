#!/usr/bin/env python3
"""
Phase 7: Dresden Data Combiner

Finds the most-enriched intermediate file and produces the combined master table.
Reference: scripts_nrw/processing/nrw_data_combiner.py

Input: Most enriched file in data_dresden/intermediate/
Output: data_dresden/final/dresden_school_master_table_final.csv
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"


def find_most_enriched_file() -> pd.DataFrame:
    """Find the most enriched intermediate file."""
    candidates = [
        INTERMEDIATE_DIR / "dresden_schools_with_website_metadata.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_poi.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_crime.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_transit.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_traffic.csv",
        RAW_DIR / "dresden_schools_raw.csv",
    ]

    for filepath in candidates:
        if filepath.exists():
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} schools from {filepath.name} ({len(df.columns)} cols)")
            return df

    raise FileNotFoundError("No school data found")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate data."""
    df = df.copy()

    # Remove duplicates
    if 'schulnummer' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < before:
            logger.info(f"Removed {before - len(df)} duplicate schools")

    # Ensure numeric columns
    numeric_cols = ['latitude', 'longitude', 'transit_stops_500m', 'transit_stop_count_1000m',
                    'transit_accessibility_score', 'traffic_accidents_total', 'traffic_accidents_per_year']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Clean PLZ
    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.zfill(5)

    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Data Combiner")
    logger.info("=" * 60)

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    df = find_most_enriched_file()
    df = clean_data(df)

    # Save final outputs
    csv_path = FINAL_DIR / "dresden_school_master_table_final.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")

    parquet_path = FINAL_DIR / "dresden_school_master_table_final.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    print(f"\n{'='*70}")
    print("DRESDEN DATA COMBINER - COMPLETE")
    print(f"{'='*70}")
    print(f"Total schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    if 'school_type_name' in df.columns:
        for st, count in df['school_type_name'].value_counts().items():
            print(f"  {st}: {count}")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
