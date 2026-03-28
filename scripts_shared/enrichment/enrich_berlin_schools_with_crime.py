#!/usr/bin/env python3
"""
Berlin Crime Data Enrichment Script
====================================

Adds district-level crime statistics to Berlin school data.

This script performs ADDITIVE enrichment:
- Reads existing school data (parquet or CSV)
- Adds crime columns without removing existing data
- Can be run standalone or as part of the pipeline

Usage:
    # Add crime data to existing primary school parquet (in-place)
    python3 enrich_berlin_schools_with_crime.py \
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

    # Add crime data to existing secondary school parquet (in-place)
    python3 enrich_berlin_schools_with_crime.py \
        --input data_berlin/final/school_master_table_final_with_embeddings.parquet

    # Specify custom output path
    python3 enrich_berlin_schools_with_crime.py \
        --input data_berlin_primary/intermediate/combined_grundschulen_with_metadata.csv \
        --output data_berlin_primary/intermediate/combined_grundschulen_with_metadata.csv

Data Source:
    Crime data: data_berlin/raw/bezirk_crime_statistics.csv
    Contains district-level crime statistics for all 12 Berlin Bezirke
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script directory and project root
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Default crime data path
DEFAULT_CRIME_DATA_PATH = PROJECT_ROOT / "data_berlin" / "raw" / "bezirk_crime_statistics.csv"

# Berlin Bezirk mapping from schulnummer prefix
# First 2 digits of schulnummer indicate the Bezirk
BEZIRK_MAP = {
    '01': 'Mitte',
    '02': 'Friedrichshain-Kreuzberg',
    '03': 'Pankow',
    '04': 'Charlottenburg-Wilmersdorf',
    '05': 'Spandau',
    '06': 'Steglitz-Zehlendorf',
    '07': 'Tempelhof-Schöneberg',
    '08': 'Neukölln',
    '09': 'Treptow-Köpenick',
    '10': 'Marzahn-Hellersdorf',
    '11': 'Lichtenberg',
    '12': 'Reinickendorf'
}


def derive_bezirk_from_schulnummer(schulnummer: str) -> Optional[str]:
    """
    Derive the Bezirk name from the schulnummer prefix.

    Args:
        schulnummer: School number (e.g., '01K01', '11G07')

    Returns:
        Bezirk name or None if not derivable
    """
    if not schulnummer or pd.isna(schulnummer):
        return None

    schulnummer = str(schulnummer).strip()
    if len(schulnummer) >= 2:
        prefix = schulnummer[:2]
        return BEZIRK_MAP.get(prefix)

    return None


def load_school_data(input_path: Path) -> pd.DataFrame:
    """
    Load school data from parquet or CSV file.

    Args:
        input_path: Path to the school data file

    Returns:
        DataFrame with school data
    """
    input_path = Path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info(f"Loading school data from: {input_path}")

    if input_path.suffix == '.parquet':
        df = pd.read_parquet(input_path)
    elif input_path.suffix == '.csv':
        df = pd.read_csv(input_path)
    else:
        raise ValueError(f"Unsupported file format: {input_path.suffix}")

    logger.info(f"  Loaded {len(df)} schools with {len(df.columns)} columns")
    return df


def load_crime_data(crime_data_path: Path) -> pd.DataFrame:
    """
    Load crime statistics data.

    Args:
        crime_data_path: Path to the crime data CSV

    Returns:
        DataFrame with crime data indexed by bezirk
    """
    crime_data_path = Path(crime_data_path)

    if not crime_data_path.exists():
        raise FileNotFoundError(f"Crime data file not found: {crime_data_path}")

    logger.info(f"Loading crime data from: {crime_data_path}")

    df_crime = pd.read_csv(crime_data_path)
    logger.info(f"  Loaded {len(df_crime)} bezirke with {len(df_crime.columns)} columns")

    return df_crime


def enrich_with_crime(
    input_path: Path,
    output_path: Optional[Path] = None,
    crime_data_path: Optional[Path] = None
) -> pd.DataFrame:
    """
    Add crime data to school data.

    This function performs ADDITIVE enrichment:
    - Preserves all existing columns
    - Adds/updates only crime-related columns
    - Derives bezirk from schulnummer if bezirk column is empty/missing

    Args:
        input_path: Path to school data (parquet or CSV)
        output_path: Path to save enriched data (default: overwrite input)
        crime_data_path: Path to crime CSV (default: data_berlin/raw/bezirk_crime_statistics.csv)

    Returns:
        Enriched DataFrame
    """
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path else input_path
    crime_data_path = Path(crime_data_path) if crime_data_path else DEFAULT_CRIME_DATA_PATH

    # Load data
    df = load_school_data(input_path)
    df_crime = load_crime_data(crime_data_path)

    # Record original column count for verification
    original_cols = set(df.columns)
    original_crime_cols = [c for c in df.columns if c.startswith('crime_')]

    if original_crime_cols:
        logger.info(f"  Found {len(original_crime_cols)} existing crime columns - will update")
        # Remove existing crime columns to avoid duplicates
        df = df.drop(columns=original_crime_cols)

    # Ensure bezirk column exists and is populated
    if 'bezirk' not in df.columns:
        df['bezirk'] = None

    # Derive bezirk from schulnummer where bezirk is missing
    empty_bezirk_mask = df['bezirk'].isna() | (df['bezirk'] == '')
    empty_count = empty_bezirk_mask.sum()

    if empty_count > 0:
        logger.info(f"  Deriving bezirk from schulnummer for {empty_count} schools")
        df.loc[empty_bezirk_mask, 'bezirk'] = df.loc[empty_bezirk_mask, 'schulnummer'].apply(
            derive_bezirk_from_schulnummer
        )

    # Verify bezirk derivation
    still_empty = df['bezirk'].isna().sum()
    if still_empty > 0:
        logger.warning(f"  {still_empty} schools still have no bezirk after derivation")

    # Prepare crime data for merging
    # Rename columns to add 'crime_' prefix
    crime_columns_to_add = [c for c in df_crime.columns if c not in ['lor_code', 'bezirk_name']]

    crime_rename_map = {col: f'crime_{col}' for col in crime_columns_to_add}
    df_crime_renamed = df_crime.rename(columns=crime_rename_map)

    # Merge crime data on bezirk
    logger.info("  Merging crime data with school data on bezirk")

    df_enriched = pd.merge(
        df,
        df_crime_renamed[['bezirk_name'] + list(crime_rename_map.values())],
        left_on='bezirk',
        right_on='bezirk_name',
        how='left'
    )

    # Drop the redundant bezirk_name column from merge
    if 'bezirk_name' in df_enriched.columns:
        df_enriched = df_enriched.drop(columns=['bezirk_name'])

    # Count successful matches
    new_crime_cols = [c for c in df_enriched.columns if c.startswith('crime_')]
    matched_count = df_enriched['crime_total_crimes_2023'].notna().sum()

    logger.info(f"  Added {len(new_crime_cols)} crime columns")
    logger.info(f"  Successfully matched {matched_count}/{len(df_enriched)} schools ({100*matched_count/len(df_enriched):.1f}%)")

    # Save output
    logger.info(f"Saving enriched data to: {output_path}")

    if output_path.suffix == '.parquet':
        df_enriched.to_parquet(output_path, index=False)
    elif output_path.suffix == '.csv':
        df_enriched.to_csv(output_path, index=False)
    else:
        # Default to same format as input
        if input_path.suffix == '.parquet':
            df_enriched.to_parquet(output_path, index=False)
        else:
            df_enriched.to_csv(output_path, index=False)

    logger.info(f"  Final dataset: {len(df_enriched)} rows, {len(df_enriched.columns)} columns")

    return df_enriched


def print_summary(df: pd.DataFrame):
    """Print summary statistics of the enriched data."""
    print("\n" + "="*60)
    print("CRIME DATA ENRICHMENT SUMMARY")
    print("="*60)

    # Crime column statistics
    crime_cols = [c for c in df.columns if c.startswith('crime_')]
    print(f"\nCrime columns added: {len(crime_cols)}")

    # Coverage by bezirk
    print("\nCoverage by Bezirk:")
    bezirk_counts = df['bezirk'].value_counts()
    for bezirk, count in bezirk_counts.items():
        print(f"  - {bezirk}: {count} schools")

    # Safety category distribution
    if 'crime_safety_category' in df.columns:
        print("\nSafety Category Distribution:")
        safety_counts = df['crime_safety_category'].value_counts()
        for category, count in safety_counts.items():
            pct = 100 * count / len(df)
            print(f"  - {category}: {count} schools ({pct:.1f}%)")

    # Key crime metrics
    if 'crime_total_crimes_avg' in df.columns:
        print("\nCrime Metrics by Bezirk (avg total crimes):")
        bezirk_crime = df.groupby('bezirk')['crime_total_crimes_avg'].first().sort_values()
        for bezirk, crimes in bezirk_crime.items():
            print(f"  - {bezirk}: {crimes:.0f}")

    print("\n" + "="*60)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Add Berlin district crime statistics to school data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Add crime data to primary school parquet (in-place)
    python3 enrich_berlin_schools_with_crime.py \\
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

    # Add crime data to secondary school parquet (in-place)
    python3 enrich_berlin_schools_with_crime.py \\
        --input data_berlin/final/school_master_table_final_with_embeddings.parquet

    # Specify custom output path
    python3 enrich_berlin_schools_with_crime.py \\
        --input data_berlin_primary/intermediate/combined_grundschulen_with_metadata.csv \\
        --output data_berlin_primary/intermediate/enriched_with_crime.csv
        """
    )

    parser.add_argument(
        "--input", "-i",
        type=str,
        required=True,
        help="Path to school data file (parquet or CSV)"
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Path to save enriched data (default: overwrite input)"
    )

    parser.add_argument(
        "--crime-data",
        type=str,
        default=None,
        help=f"Path to crime data CSV (default: {DEFAULT_CRIME_DATA_PATH})"
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip printing summary statistics"
    )

    args = parser.parse_args()

    try:
        # Resolve paths relative to project root if not absolute
        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = PROJECT_ROOT / input_path

        output_path = None
        if args.output:
            output_path = Path(args.output)
            if not output_path.is_absolute():
                output_path = PROJECT_ROOT / output_path

        crime_data_path = None
        if args.crime_data:
            crime_data_path = Path(args.crime_data)
            if not crime_data_path.is_absolute():
                crime_data_path = PROJECT_ROOT / crime_data_path

        # Run enrichment
        df_enriched = enrich_with_crime(
            input_path=input_path,
            output_path=output_path,
            crime_data_path=crime_data_path
        )

        # Print summary
        if not args.no_summary:
            print_summary(df_enriched)

        logger.info("Crime data enrichment completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Error during enrichment: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
