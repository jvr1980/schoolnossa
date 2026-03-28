#!/usr/bin/env python3
"""
Berlin School Statistics Enrichment Script
==========================================

Adds student and teacher statistics from bildungsstatistik.berlin.de to Berlin school data.

This script performs ADDITIVE enrichment:
- Reads existing school data (parquet or CSV)
- Loads bildungsstatistik CSV files (downloaded separately)
- Joins on schulnummer (BSN column in source)
- Adds student/teacher columns without removing existing data
- Can be run standalone or as part of the pipeline

Usage:
    # Add statistics to existing primary school parquet (in-place)
    python3 enrich_berlin_schools_with_statistics.py \
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

    # Add statistics to existing secondary school parquet (in-place)
    python3 enrich_berlin_schools_with_statistics.py \
        --input data_berlin/final/school_master_table_final_with_embeddings.parquet

    # Specify custom output path
    python3 enrich_berlin_schools_with_statistics.py \
        --input data_berlin_primary/intermediate/combined_grundschulen_with_metadata.csv \
        --output data_berlin_primary/intermediate/combined_grundschulen_with_metadata.csv

Data Source:
    Statistics data: data_berlin/raw/bildungsstatistik_2024_25.csv
                    data_berlin/raw/bildungsstatistik_2023_24.csv
    Downloaded from: https://www.bildungsstatistik.berlin.de/statistik/ListGen/SVZ_Fakt5.aspx
    Contains student/teacher statistics for all Berlin public and private schools
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

# Default statistics data paths
DEFAULT_STATS_2024_25_PATH = PROJECT_ROOT / "data_berlin" / "raw" / "bildungsstatistik_2024_25.csv"
DEFAULT_STATS_2023_24_PATH = PROJECT_ROOT / "data_berlin" / "raw" / "bildungsstatistik_2023_24.csv"

# Column mapping from bildungsstatistik CSV to our schema
# Source columns are German with special characters
# These match the Berlin secondary school schema (schueler_YYYY_YY, lehrer_YYYY_YY)
COLUMN_MAPPING_2024_25_CORE = {
    'Schüler (m/w/d)': 'schueler_2024_25',
    'Lehrkräfte (m,w,d)': 'lehrer_2024_25',
}

COLUMN_MAPPING_2023_24_CORE = {
    'Schüler (m/w/d)': 'schueler_2023_24',
    'Lehrkräfte (m,w,d)': 'lehrer_2023_24',
}

# Extended column mapping including gender breakdown (optional)
COLUMN_MAPPING_2024_25_EXTENDED = {
    'Schüler (m/w/d)': 'schueler_2024_25',
    'Schüler (w)': 'schueler_w_2024_25',
    'Schüler (m)': 'schueler_m_2024_25',
    'Lehrkräfte (m,w,d)': 'lehrer_2024_25',
    'Lehrkräfte (w)': 'lehrer_w_2024_25',
    'Lehrkräfte (m)': 'lehrer_m_2024_25',
}

COLUMN_MAPPING_2023_24_EXTENDED = {
    'Schüler (m/w/d)': 'schueler_2023_24',
    'Schüler (w)': 'schueler_w_2023_24',
    'Schüler (m)': 'schueler_m_2023_24',
    'Lehrkräfte (m,w,d)': 'lehrer_2023_24',
    'Lehrkräfte (w)': 'lehrer_w_2023_24',
    'Lehrkräfte (m)': 'lehrer_m_2023_24',
}


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


def load_statistics_data(stats_path: Path, year_suffix: str, column_mapping: dict) -> pd.DataFrame:
    """
    Load and prepare statistics data from bildungsstatistik CSV.

    Args:
        stats_path: Path to the bildungsstatistik CSV file
        year_suffix: Year suffix for column names (e.g., '2024_25')
        column_mapping: Mapping from source columns to target columns

    Returns:
        DataFrame with statistics data, indexed by schulnummer
    """
    stats_path = Path(stats_path)

    if not stats_path.exists():
        logger.warning(f"Statistics file not found: {stats_path}")
        return None

    logger.info(f"Loading statistics data from: {stats_path}")

    # The CSV uses semicolon delimiter
    df_stats = pd.read_csv(stats_path, sep=';', encoding='utf-8')
    logger.info(f"  Loaded {len(df_stats)} schools from statistics file")

    # Rename BSN to schulnummer for joining
    if 'BSN' in df_stats.columns:
        df_stats = df_stats.rename(columns={'BSN': 'schulnummer_stats'})
    else:
        logger.error("BSN column not found in statistics file")
        return None

    # Rename statistics columns to our schema
    df_stats = df_stats.rename(columns=column_mapping)

    # Select only the columns we need
    columns_to_keep = ['schulnummer_stats'] + list(column_mapping.values())
    available_columns = [c for c in columns_to_keep if c in df_stats.columns]
    df_stats = df_stats[available_columns]

    # Convert numeric columns (they may have been read as strings)
    for col in list(column_mapping.values()):
        if col in df_stats.columns:
            df_stats[col] = pd.to_numeric(df_stats[col], errors='coerce')

    logger.info(f"  Prepared {len(available_columns)} columns for merging")

    return df_stats


def normalize_schulnummer(schulnummer: str) -> Optional[str]:
    """
    Normalize schulnummer for consistent joining.

    Args:
        schulnummer: School number (e.g., '01G01', '11G07')

    Returns:
        Normalized schulnummer or None
    """
    if pd.isna(schulnummer):
        return None

    # Convert to string and strip whitespace
    schulnummer = str(schulnummer).strip().upper()

    # Remove any leading/trailing special characters
    schulnummer = schulnummer.strip()

    return schulnummer if schulnummer else None


def enrich_with_statistics(
    input_path: Path,
    output_path: Optional[Path] = None,
    stats_2024_25_path: Optional[Path] = None,
    stats_2023_24_path: Optional[Path] = None,
    include_gender_breakdown: bool = False
) -> pd.DataFrame:
    """
    Add statistics data to school data.

    This function performs ADDITIVE enrichment:
    - Preserves all existing columns
    - Adds/updates only statistics-related columns
    - Joins on schulnummer
    - By default uses core columns matching Berlin secondary school schema

    Args:
        input_path: Path to school data (parquet or CSV)
        output_path: Path to save enriched data (default: overwrite input)
        stats_2024_25_path: Path to 2024/25 statistics CSV (default: data_berlin/raw/bildungsstatistik_2024_25.csv)
        stats_2023_24_path: Path to 2023/24 statistics CSV (default: data_berlin/raw/bildungsstatistik_2023_24.csv)
        include_gender_breakdown: If True, include gender breakdown columns (schueler_w_*, schueler_m_*, etc.)

    Returns:
        Enriched DataFrame
    """
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path else input_path
    stats_2024_25_path = Path(stats_2024_25_path) if stats_2024_25_path else DEFAULT_STATS_2024_25_PATH
    stats_2023_24_path = Path(stats_2023_24_path) if stats_2023_24_path else DEFAULT_STATS_2023_24_PATH

    # Select column mappings based on whether gender breakdown is requested
    if include_gender_breakdown:
        logger.info("  Including gender breakdown columns (extended schema)")
        column_mapping_2024 = COLUMN_MAPPING_2024_25_EXTENDED
        column_mapping_2023 = COLUMN_MAPPING_2023_24_EXTENDED
    else:
        logger.info("  Using core columns only (matches Berlin secondary school schema)")
        column_mapping_2024 = COLUMN_MAPPING_2024_25_CORE
        column_mapping_2023 = COLUMN_MAPPING_2023_24_CORE

    # Load school data
    df = load_school_data(input_path)

    # Record original column count for verification
    original_cols = set(df.columns)

    # Check for existing statistics columns and remove them to avoid duplicates
    existing_stats_cols = [c for c in df.columns if c.startswith('schueler_') or c.startswith('lehrer_')]
    if existing_stats_cols:
        logger.info(f"  Found {len(existing_stats_cols)} existing statistics columns - will update")
        df = df.drop(columns=existing_stats_cols)

    # Normalize schulnummer in school data for joining
    if 'schulnummer' not in df.columns:
        logger.error("schulnummer column not found in school data")
        return df

    df['schulnummer_normalized'] = df['schulnummer'].apply(normalize_schulnummer)

    # Load and merge 2024/25 statistics
    df_stats_2024 = load_statistics_data(stats_2024_25_path, '2024_25', column_mapping_2024)
    if df_stats_2024 is not None:
        df_stats_2024['schulnummer_normalized'] = df_stats_2024['schulnummer_stats'].apply(normalize_schulnummer)

        logger.info("  Merging 2024/25 statistics with school data")
        df = pd.merge(
            df,
            df_stats_2024.drop(columns=['schulnummer_stats']),
            on='schulnummer_normalized',
            how='left'
        )

        # Count matches
        matched_2024 = df['schueler_2024_25'].notna().sum()
        logger.info(f"  Matched {matched_2024}/{len(df)} schools ({100*matched_2024/len(df):.1f}%) with 2024/25 data")

    # Load and merge 2023/24 statistics
    df_stats_2023 = load_statistics_data(stats_2023_24_path, '2023_24', column_mapping_2023)
    if df_stats_2023 is not None:
        df_stats_2023['schulnummer_normalized'] = df_stats_2023['schulnummer_stats'].apply(normalize_schulnummer)

        logger.info("  Merging 2023/24 statistics with school data")
        df = pd.merge(
            df,
            df_stats_2023.drop(columns=['schulnummer_stats']),
            on='schulnummer_normalized',
            how='left'
        )

        # Count matches
        matched_2023 = df['schueler_2023_24'].notna().sum()
        logger.info(f"  Matched {matched_2023}/{len(df)} schools ({100*matched_2023/len(df):.1f}%) with 2023/24 data")

    # Drop the temporary normalized column
    if 'schulnummer_normalized' in df.columns:
        df = df.drop(columns=['schulnummer_normalized'])

    # Count final statistics columns
    final_stats_cols = [c for c in df.columns if c.startswith('schueler_') or c.startswith('lehrer_')]
    logger.info(f"  Added {len(final_stats_cols)} statistics columns")

    # Save output
    logger.info(f"Saving enriched data to: {output_path}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix == '.parquet':
        df.to_parquet(output_path, index=False)
    elif output_path.suffix == '.csv':
        df.to_csv(output_path, index=False)
    else:
        # Default to same format as input
        if input_path.suffix == '.parquet':
            df.to_parquet(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)

    logger.info(f"  Final dataset: {len(df)} rows, {len(df.columns)} columns")

    return df


def print_summary(df: pd.DataFrame):
    """Print summary statistics of the enriched data."""
    print("\n" + "="*60)
    print("STATISTICS ENRICHMENT SUMMARY")
    print("="*60)

    # Statistics column coverage
    stats_cols = [c for c in df.columns if c.startswith('schueler_') or c.startswith('lehrer_')]
    print(f"\nStatistics columns added: {len(stats_cols)}")

    # Coverage for key columns
    print("\nColumn Coverage:")
    key_cols = ['schueler_2024_25', 'lehrer_2024_25', 'schueler_2023_24', 'lehrer_2023_24']
    for col in key_cols:
        if col in df.columns:
            count = df[col].notna().sum()
            pct = 100 * count / len(df)
            print(f"  - {col}: {count}/{len(df)} ({pct:.1f}%)")

    # Summary statistics for student counts
    if 'schueler_2024_25' in df.columns:
        print("\nStudent Statistics (2024/25):")
        valid_data = df[df['schueler_2024_25'].notna()]['schueler_2024_25']
        if len(valid_data) > 0:
            print(f"  - Total students: {valid_data.sum():,.0f}")
            print(f"  - Mean per school: {valid_data.mean():.1f}")
            print(f"  - Min: {valid_data.min():.0f}")
            print(f"  - Max: {valid_data.max():.0f}")

    # Summary statistics for teacher counts
    if 'lehrer_2024_25' in df.columns:
        print("\nTeacher Statistics (2024/25):")
        valid_data = df[df['lehrer_2024_25'].notna()]['lehrer_2024_25']
        if len(valid_data) > 0:
            print(f"  - Total teachers: {valid_data.sum():,.0f}")
            print(f"  - Mean per school: {valid_data.mean():.1f}")
            print(f"  - Min: {valid_data.min():.0f}")
            print(f"  - Max: {valid_data.max():.0f}")

    # Student-teacher ratio
    if 'schueler_2024_25' in df.columns and 'lehrer_2024_25' in df.columns:
        mask = (df['schueler_2024_25'].notna()) & (df['lehrer_2024_25'].notna()) & (df['lehrer_2024_25'] > 0)
        if mask.sum() > 0:
            df_valid = df[mask]
            ratio = df_valid['schueler_2024_25'] / df_valid['lehrer_2024_25']
            print("\nStudent-Teacher Ratio (2024/25):")
            print(f"  - Mean: {ratio.mean():.1f}")
            print(f"  - Min: {ratio.min():.1f}")
            print(f"  - Max: {ratio.max():.1f}")

    # Sample matched schools
    print("\nSample Schools with Statistics:")
    sample_cols = ['schulname', 'schulnummer', 'schueler_2024_25', 'lehrer_2024_25']
    available_cols = [c for c in sample_cols if c in df.columns]
    if 'schueler_2024_25' in df.columns:
        sample = df[df['schueler_2024_25'].notna()][available_cols].head(5)
        print(sample.to_string(index=False))

    print("\n" + "="*60)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Add Berlin school statistics (student/teacher counts) to school data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Add statistics to primary school parquet (in-place)
    python3 enrich_berlin_schools_with_statistics.py \\
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

    # Add statistics to secondary school parquet (in-place)
    python3 enrich_berlin_schools_with_statistics.py \\
        --input data_berlin/final/school_master_table_final_with_embeddings.parquet

    # Specify custom output path
    python3 enrich_berlin_schools_with_statistics.py \\
        --input data_berlin_primary/intermediate/combined_grundschulen_with_metadata.csv \\
        --output data_berlin_primary/intermediate/enriched_with_statistics.csv
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
        "--stats-2024-25",
        type=str,
        default=None,
        help=f"Path to 2024/25 statistics CSV (default: {DEFAULT_STATS_2024_25_PATH})"
    )

    parser.add_argument(
        "--stats-2023-24",
        type=str,
        default=None,
        help=f"Path to 2023/24 statistics CSV (default: {DEFAULT_STATS_2023_24_PATH})"
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip printing summary statistics"
    )

    parser.add_argument(
        "--include-gender-breakdown",
        action="store_true",
        help="Include gender breakdown columns (schueler_w_*, schueler_m_*, lehrer_w_*, lehrer_m_*). "
             "By default, only core columns matching Berlin secondary school schema are added."
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

        stats_2024_25_path = None
        if args.stats_2024_25:
            stats_2024_25_path = Path(args.stats_2024_25)
            if not stats_2024_25_path.is_absolute():
                stats_2024_25_path = PROJECT_ROOT / stats_2024_25_path

        stats_2023_24_path = None
        if args.stats_2023_24:
            stats_2023_24_path = Path(args.stats_2023_24)
            if not stats_2023_24_path.is_absolute():
                stats_2023_24_path = PROJECT_ROOT / stats_2023_24_path

        # Run enrichment
        df_enriched = enrich_with_statistics(
            input_path=input_path,
            output_path=output_path,
            stats_2024_25_path=stats_2024_25_path,
            stats_2023_24_path=stats_2023_24_path,
            include_gender_breakdown=args.include_gender_breakdown
        )

        # Print summary
        if not args.no_summary:
            print_summary(df_enriched)

        logger.info("Statistics enrichment completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Error during enrichment: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
