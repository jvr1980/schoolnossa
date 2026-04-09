#!/usr/bin/env python3
"""
Phase 7: Dresden Data Combiner

Finds the most-enriched intermediate file and produces:
1. Combined master table (all schools)
2. Primary-only file (Grundschulen + cross-level schools)
3. Secondary-only file (Oberschulen/Gymnasien + cross-level schools)

Cross-level schools (Waldorf, International, Gemeinschaftsschulen) appear in
BOTH primary and secondary files with adjusted school_type/school_category.

Reference: scripts_nrw/processing/nrw_data_combiner.py
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

# Schools that span both primary and secondary (years 1-12/13)
# These get duplicated into both output files
CROSS_LEVEL_SCHULNUMMERN = {
    '4310025': {'name': 'Freie Waldorfschule Dresden', 'secondary_type': 'Waldorfschule'},
    '4313714': {'name': 'Interkulturelle Waldorfschule Dresden', 'secondary_type': 'Waldorfschule'},
    '4313599': {'name': 'Neue Waldorfschule Dresden', 'secondary_type': 'Waldorfschule'},
    '4312351': {'name': 'Internationale Schule Dresden', 'secondary_type': 'Internationale Schule'},
    '4370029': {'name': 'Gemeinschaftsschule Campus Cordis', 'secondary_type': 'Gemeinschaftsschule'},
    '4370011': {'name': 'Universitätsgemeinschaftsschule', 'secondary_type': 'Gemeinschaftsschule'},
}


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

    if 'schulnummer' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < before:
            logger.info(f"Removed {before - len(df)} duplicate schools")

    numeric_cols = ['latitude', 'longitude', 'transit_stops_500m', 'transit_stop_count_1000m',
                    'transit_accessibility_score', 'traffic_accidents_total', 'traffic_accidents_per_year']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.zfill(5)

    return df


def split_primary_secondary(df: pd.DataFrame):
    """Split into primary and secondary DataFrames, duplicating cross-level schools."""
    df = df.copy()
    df['schulnummer'] = df['schulnummer'].astype(str).str.strip()

    cross_level_ids = set(CROSS_LEVEL_SCHULNUMMERN.keys())

    # --- Primary file ---
    # All Grundschulen + cross-level schools
    primary_mask = df['school_category'] == 'primary'
    primary_df = df[primary_mask].copy()

    # Ensure all primary schools have schultyp set
    schultyp_col = 'schultyp' if 'schultyp' in primary_df.columns else None
    if schultyp_col:
        primary_df.loc[primary_df[schultyp_col].isna(), schultyp_col] = 'Grundschule'
    else:
        primary_df['schultyp'] = 'Grundschule'

    # Add cross-level schools that aren't already primary (Gemeinschaftsschulen are "other")
    cross_not_primary = df[
        df['schulnummer'].isin(cross_level_ids) & (df['school_category'] != 'primary')
    ].copy()
    if not cross_not_primary.empty:
        cross_not_primary['school_category'] = 'primary'
        cross_not_primary['schultyp'] = 'Grundschule'
        primary_df = pd.concat([primary_df, cross_not_primary], ignore_index=True)

    # --- Secondary file ---
    # All Oberschulen/Gymnasien + cross-level schools
    secondary_mask = df['school_category'] == 'secondary'
    secondary_df = df[secondary_mask].copy()

    # Add cross-level schools that aren't already secondary
    cross_not_secondary = df[
        df['schulnummer'].isin(cross_level_ids) & (df['school_category'] != 'secondary')
    ].copy()
    if not cross_not_secondary.empty:
        cross_not_secondary['school_category'] = 'secondary'
        # Set appropriate secondary school type
        for idx, row in cross_not_secondary.iterrows():
            snr = str(row['schulnummer']).strip()
            if snr in CROSS_LEVEL_SCHULNUMMERN:
                cross_not_secondary.at[idx, 'schultyp'] = CROSS_LEVEL_SCHULNUMMERN[snr]['secondary_type']
        secondary_df = pd.concat([secondary_df, cross_not_secondary], ignore_index=True)

    # Mark cross-level schools in both files
    for split_df in [primary_df, secondary_df]:
        split_df['cross_level_school'] = split_df['schulnummer'].isin(cross_level_ids)

    logger.info(f"Primary schools: {len(primary_df)} (incl. {len(cross_not_primary)} cross-level)")
    logger.info(f"Secondary schools: {len(secondary_df)} (incl. {len(cross_not_secondary)} cross-level)")

    return primary_df, secondary_df


def save_outputs(df: pd.DataFrame, primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    """Save all output files."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Combined (all schools)
    combined_csv = FINAL_DIR / "dresden_school_master_table_final.csv"
    df.to_csv(combined_csv, index=False, encoding='utf-8-sig')
    df.to_parquet(FINAL_DIR / "dresden_school_master_table_final.parquet", index=False)
    logger.info(f"Combined: {combined_csv} ({len(df)} schools)")

    # Primary
    primary_csv = FINAL_DIR / "dresden_primary_school_master_table_final.csv"
    primary_df.to_csv(primary_csv, index=False, encoding='utf-8-sig')
    primary_df.to_parquet(FINAL_DIR / "dresden_primary_school_master_table_final.parquet", index=False)
    logger.info(f"Primary:  {primary_csv} ({len(primary_df)} schools)")

    # Secondary
    secondary_csv = FINAL_DIR / "dresden_secondary_school_master_table_final.csv"
    secondary_df.to_csv(secondary_csv, index=False, encoding='utf-8-sig')
    secondary_df.to_parquet(FINAL_DIR / "dresden_secondary_school_master_table_final.parquet", index=False)
    logger.info(f"Secondary: {secondary_csv} ({len(secondary_df)} schools)")


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Data Combiner")
    logger.info("=" * 60)

    df = find_most_enriched_file()
    df = clean_data(df)

    # Split into primary + secondary (with cross-level duplication)
    primary_df, secondary_df = split_primary_secondary(df)

    # Save all outputs
    save_outputs(df, primary_df, secondary_df)

    # Summary
    cross_ids = set(CROSS_LEVEL_SCHULNUMMERN.keys())
    print(f"\n{'='*70}")
    print("DRESDEN DATA COMBINER - COMPLETE")
    print(f"{'='*70}")
    print(f"Combined:  {len(df)} schools, {len(df.columns)} columns")
    print(f"Primary:   {len(primary_df)} schools (incl. {primary_df['schulnummer'].isin(cross_ids).sum()} cross-level)")
    print(f"Secondary: {len(secondary_df)} schools (incl. {secondary_df['schulnummer'].isin(cross_ids).sum()} cross-level)")
    print(f"\nCross-level schools (in both files):")
    for snr, info in CROSS_LEVEL_SCHULNUMMERN.items():
        print(f"  {snr} — {info['name']} → secondary as {info['secondary_type']}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
