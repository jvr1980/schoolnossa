#!/usr/bin/env python3
"""
Phase 9: Dresden to Berlin Schema Transformer

Transforms Dresden pipeline output to match the Berlin reference schema
for frontend compatibility. Processes BOTH primary and secondary files separately,
using the correct Berlin reference schema for each.

Reference: scripts_nrw/nrw_to_berlin_schema.py
Berlin schema: data_berlin/final/school_master_table_final_with_embeddings.parquet

Author: Dresden School Data Pipeline
Created: 2026-04-07
Updated: 2026-04-09 — process primary + secondary files separately
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
FINAL_DIR = DATA_DIR / "final"

# Berlin reference schemas
BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
BERLIN_PRI_REF = PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet"

# Dresden → Berlin column renames
COLUMN_RENAMES = {
    'school_type_name': 'schultyp',
    'crime_stadtteil': 'crime_bezirk',
    'crime_cases_total': 'crime_straftaten_gesamt',
}


def get_berlin_schema(school_type: str) -> list:
    """Load the exact column list from the Berlin reference parquet."""
    ref = BERLIN_SEC_REF if school_type == "secondary" else BERLIN_PRI_REF

    if not ref.exists():
        logger.warning(f"Berlin reference not found: {ref}")
        return []

    berlin_df = pd.read_parquet(ref, columns=[])
    cols = list(berlin_df.columns)
    logger.info(f"Berlin {school_type} schema: {len(cols)} columns")
    return cols


def transform_to_berlin_schema(df: pd.DataFrame, school_type: str) -> pd.DataFrame:
    """Transform Dresden data to Berlin schema."""
    logger.info(f"Transforming {len(df)} {school_type} schools to Berlin schema...")

    df = df.copy()

    # Only rename if the target column doesn't already exist
    for old, new in COLUMN_RENAMES.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
        elif old in df.columns and new in df.columns:
            # Target already exists — drop the source column
            df = df.drop(columns=[old])

    df['metadata_source'] = 'Sächsische Schuldatenbank'
    df['stadt'] = 'Dresden'
    df['bundesland'] = 'Sachsen'

    berlin_cols = get_berlin_schema(school_type)

    if berlin_cols:
        for col in berlin_cols:
            if col not in df.columns:
                df[col] = None

        extra_cols = [c for c in df.columns if c not in berlin_cols]
        ordered = [c for c in berlin_cols if c in df.columns] + extra_cols
        df = df[ordered]

        logger.info(f"Berlin columns present: {sum(1 for c in berlin_cols if c in df.columns)}/{len(berlin_cols)}")
        logger.info(f"Dresden extra columns: {len(extra_cols)}")
    else:
        logger.warning(f"No Berlin {school_type} reference available — keeping Dresden schema as-is")

    return df


def process_school_type(school_type: str):
    """Process a single school type file."""
    input_csv = FINAL_DIR / f"dresden_{school_type}_school_master_table_final.csv"
    if not input_csv.exists():
        logger.warning(f"File not found: {input_csv}")
        return None

    df = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(df)} {school_type} schools from {input_csv.name}")

    df = transform_to_berlin_schema(df, school_type)

    # Overwrite the _final files with Berlin-aligned schema
    df.to_csv(input_csv, index=False, encoding='utf-8-sig')
    df.to_parquet(input_csv.with_suffix('.parquet'), index=False)
    logger.info(f"Saved: {input_csv} ({len(df)} schools, {len(df.columns)} cols)")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden → Berlin Schema Enforcement")
    logger.info("=" * 60)

    results = {}

    # Process primary and secondary files separately
    for school_type in ['primary', 'secondary']:
        df = process_school_type(school_type)
        if df is not None:
            results[school_type] = len(df)

    # Also process the combined file (using secondary schema as it covers more columns)
    combined_csv = FINAL_DIR / "dresden_school_master_table_final.csv"
    if combined_csv.exists():
        df = pd.read_csv(combined_csv)
        df = transform_to_berlin_schema(df, 'secondary')
        df.to_csv(combined_csv, index=False, encoding='utf-8-sig')
        df.to_parquet(combined_csv.with_suffix('.parquet'), index=False)
        results['combined'] = len(df)

    print(f"\n{'='*70}")
    print("DRESDEN → BERLIN SCHEMA ENFORCEMENT - COMPLETE")
    print(f"{'='*70}")
    for key, count in results.items():
        print(f"  {key}: {count} schools")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
