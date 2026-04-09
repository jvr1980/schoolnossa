#!/usr/bin/env python3
"""
Phase 7: Bremen Data Combiner

Merges all enrichment outputs into a single master table.
Sequential merge by schulnummer, following the Hamburg pattern.
Auto-detects which intermediate files exist and merges accordingly.

Input:
    - data_bremen/raw/bremen_school_master.csv (base)
    - data_bremen/intermediate/bremen_schools_with_traffic.csv
    - data_bremen/intermediate/bremen_schools_with_transit.csv
    - data_bremen/intermediate/bremen_schools_with_crime.csv
    - data_bremen/intermediate/bremen_schools_with_poi.csv
    - data_bremen/intermediate/bremen_schools_with_website_metadata.csv

Output:
    - data_bremen/final/bremen_school_master_table_final.csv
    - data_bremen/final/bremen_school_master_table_final.parquet

Reference: scripts_hamburg/processing/hamburg_data_combiner.py
Author: Bremen School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime

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
FINAL_DIR = DATA_DIR / "final"


def find_input_file() -> Path:
    """Find the base school data."""
    candidates = [
        RAW_DIR / "bremen_school_master.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("No base school data found (bremen_school_master.csv)")


def load_enrichment_file(filename: str) -> pd.DataFrame:
    """Load an intermediate enrichment file if it exists."""
    path = INTERMEDIATE_DIR / filename
    if not path.exists():
        logger.warning(f"  Enrichment file not found: {filename}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    logger.info(f"  Loaded {filename}: {len(df)} rows, {len(df.columns)} columns")
    return df


def extract_new_columns(enrichment_df: pd.DataFrame, base_df: pd.DataFrame,
                        enrichment_name: str) -> pd.DataFrame:
    """
    Extract only columns that are new in the enrichment file
    (not already present in base), plus schulnummer for joining.
    """
    if enrichment_df.empty:
        return pd.DataFrame()

    if "schulnummer" not in enrichment_df.columns:
        logger.warning(f"  {enrichment_name}: no schulnummer column, skipping")
        return pd.DataFrame()

    new_cols = [c for c in enrichment_df.columns
                if c not in base_df.columns or c == "schulnummer"]

    if len(new_cols) <= 1:  # Only schulnummer
        logger.info(f"  {enrichment_name}: no new columns to merge")
        return pd.DataFrame()

    logger.info(f"  {enrichment_name}: {len(new_cols) - 1} new columns")
    return enrichment_df[new_cols]


def merge_enrichment(base_df: pd.DataFrame, enrichment_df: pd.DataFrame,
                     enrichment_name: str) -> pd.DataFrame:
    """Merge enrichment data into base on schulnummer."""
    if enrichment_df.empty:
        logger.warning(f"No {enrichment_name} data to merge")
        return base_df

    logger.info(f"Merging {enrichment_name} data...")

    # Extract only new columns
    new_data = extract_new_columns(enrichment_df, base_df, enrichment_name)
    if new_data.empty:
        return base_df

    # Ensure schulnummer types match
    base_df["schulnummer"] = base_df["schulnummer"].astype(str).str.strip()
    new_data["schulnummer"] = new_data["schulnummer"].astype(str).str.strip()

    merged = base_df.merge(new_data, on="schulnummer", how="left")
    logger.info(f"  Merged: {len(merged)} rows")

    return merged


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the combined data."""
    logger.info("Cleaning combined data...")
    df = df.copy()

    # Remove duplicate rows based on schulnummer
    if "schulnummer" in df.columns:
        original_len = len(df)
        df = df.drop_duplicates(subset=["schulnummer"], keep="first")
        if len(df) < original_len:
            logger.info(f"  Removed {original_len - len(df)} duplicate schools")

    # Clean website URLs
    if "website" in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).lower() in ['nan', 'none', '']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    # Ensure numeric columns are numeric
    numeric_cols = ['latitude', 'longitude', 'schueler_2024_25', 'lehrer_2024_25',
                    'transit_accessibility_score', 'crime_total',
                    'crime_safety_score', 'crime_safety_rank']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns in a logical order."""
    logger.info("Standardizing column order...")

    preferred_order = [
        # Core identification
        'schulnummer', 'schulname', 'schulform', 'school_type',
        # Location
        'strasse', 'plz', 'stadt', 'stadtteil', 'bezirk',
        'latitude', 'longitude',
        # Contact
        'telefon', 'email', 'website', 'fax',
        # Leadership
        'leitung',
        # Operator
        'traegerschaft',
        # Student/Teacher data
        'schueler_2024_25', 'lehrer_2024_25',
        # Languages & programs
        'sprachen', 'besonderheiten', 'gruendungsjahr',
        # Traffic
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'traffic_accidents_with_children', 'traffic_severity_index',
        'traffic_safety_score', 'traffic_safety_category',
        # Transit
        'transit_rail_01_name', 'transit_rail_01_distance_m',
        'transit_tram_01_name', 'transit_tram_01_distance_m',
        'transit_bus_01_name', 'transit_bus_01_distance_m',
        'transit_stop_count_1000m', 'transit_accessibility_score',
        # Crime
        'crime_beirat', 'crime_beirat_population',
        'crime_total', 'crime_sexual', 'crime_robbery',
        'crime_assault', 'crime_burglary', 'crime_theft', 'crime_drugs',
        'crime_safety_score', 'crime_safety_category', 'crime_safety_rank',
        'crime_data_source',
        # POI
    ]

    # Collect columns in preferred order
    ordered_cols = []
    for col in preferred_order:
        if col in df.columns:
            ordered_cols.append(col)

    # Add POI columns (sorted)
    poi_cols = sorted([c for c in df.columns if c.startswith('poi_')])
    ordered_cols.extend(poi_cols)

    # Add description columns
    desc_cols = ['description', 'description_de', 'summary_en', 'summary_de', 'description_auto']
    for col in desc_cols:
        if col in df.columns and col not in ordered_cols:
            ordered_cols.append(col)

    # Add remaining columns not yet included
    remaining = [c for c in df.columns if c not in ordered_cols]
    ordered_cols.extend(remaining)

    return df[ordered_cols]


def save_outputs(df: pd.DataFrame):
    """Save the combined master table."""
    logger.info("Saving output files...")

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Save CSV
    csv_path = FINAL_DIR / "bremen_school_master_table_final.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"  Saved: {csv_path}")

    # Save parquet
    parquet_path = FINAL_DIR / "bremen_school_master_table_final.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"  Saved: {parquet_path}")


def print_summary(df: pd.DataFrame):
    """Print summary of combined data."""
    print(f"\n{'=' * 70}")
    print("BREMEN DATA COMBINER - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'schulform' in df.columns:
        print("\nSchools by Schulform:")
        for t, count in df['schulform'].value_counts().items():
            print(f"  - {t}: {count}")

    # Data coverage
    print("\nData coverage:")
    coverage_cols = {
        'schulnummer': 'School ID',
        'schulname': 'School Name',
        'latitude': 'Coordinates',
        'schueler_2024_25': 'Student Count',
        'lehrer_2024_25': 'Teacher Count',
        'transit_accessibility_score': 'Transit Score',
        'traffic_accidents_total': 'Traffic Data',
        'crime_total': 'Crime Data',
        'crime_safety_score': 'Crime Safety Score',
        'description': 'Description',
        'sprachen': 'Languages',
        'besonderheiten': 'Special Programs',
    }

    for col, label in coverage_cols.items():
        if col in df.columns:
            count = df[col].notna().sum()
            pct = 100 * count / len(df)
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    # Count POI columns
    poi_cols = [c for c in df.columns if c.startswith('poi_')]
    if poi_cols:
        print(f"  - POI columns: {len(poi_cols)}")

    print(f"\n{'=' * 70}")


def main():
    """Main function to combine all Bremen data."""
    logger.info("=" * 60)
    logger.info("Starting Bremen School Data Combiner")
    logger.info("=" * 60)

    try:
        # Load base data
        base_path = find_input_file()
        base_df = pd.read_csv(base_path)
        logger.info(f"Loaded base data: {len(base_df)} schools from {base_path.name}")

        # Define enrichment files in pipeline order
        enrichments = [
            ("bremen_schools_with_traffic.csv", "traffic"),
            ("bremen_schools_with_transit.csv", "transit"),
            ("bremen_schools_with_crime.csv", "crime"),
            ("bremen_schools_with_poi.csv", "POI"),
            ("bremen_schools_with_website_metadata.csv", "website_metadata"),
        ]

        # Sequential merge
        combined_df = base_df
        for filename, name in enrichments:
            enrichment_df = load_enrichment_file(filename)
            combined_df = merge_enrichment(combined_df, enrichment_df, name)

        # Clean and standardize
        combined_df = clean_data(combined_df)
        combined_df = standardize_columns(combined_df)

        # Save
        save_outputs(combined_df)
        print_summary(combined_df)

        logger.info("Data combination complete!")
        return combined_df

    except Exception as e:
        logger.error(f"Data combination failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
