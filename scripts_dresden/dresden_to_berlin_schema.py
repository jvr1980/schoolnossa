#!/usr/bin/env python3
"""
Phase 9: Dresden to Berlin Schema Transformer

Transforms Dresden pipeline output to match the Berlin reference schema
for frontend compatibility. Reads Berlin's parquet to get the exact column list,
then ensures all Berlin columns exist (adds as NULL if missing), renames
Dresden-specific columns to their Berlin equivalents, and preserves any
Dresden-specific extras.

Reference: scripts_nrw/nrw_to_berlin_schema.py
Berlin schema: data_berlin/final/school_master_table_final_with_embeddings.parquet

Author: Dresden School Data Pipeline
Created: 2026-04-07
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
    logger.info(f"Transforming {len(df)} schools to Berlin schema...")

    df = df.copy()

    # Apply renames
    df = df.rename(columns={k: v for k, v in COLUMN_RENAMES.items() if k in df.columns})

    # Set constant metadata
    df['metadata_source'] = 'Sächsische Schuldatenbank'
    df['stadt'] = 'Dresden'
    df['bundesland'] = 'Sachsen'

    # Get Berlin column list
    berlin_cols = get_berlin_schema(school_type)

    if berlin_cols:
        # Ensure all Berlin columns exist
        for col in berlin_cols:
            if col not in df.columns:
                df[col] = None

        # Order: Berlin columns first, then Dresden-specific extras
        extra_cols = [c for c in df.columns if c not in berlin_cols]
        ordered = [c for c in berlin_cols if c in df.columns] + extra_cols
        df = df[ordered]

        logger.info(f"Berlin columns present: {sum(1 for c in berlin_cols if c in df.columns)}/{len(berlin_cols)}")
        logger.info(f"Dresden extra columns: {len(extra_cols)}")
    else:
        logger.warning("No Berlin reference available — keeping Dresden schema as-is")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden → Berlin Schema Enforcement")
    logger.info("=" * 60)

    # Process the combined file
    final_csv = FINAL_DIR / "dresden_school_master_table_final.csv"
    if not final_csv.exists():
        raise FileNotFoundError(f"Final table not found: {final_csv}")

    df = pd.read_csv(final_csv)
    logger.info(f"Loaded {len(df)} schools")

    # Determine school types present
    if 'school_category' in df.columns:
        categories = df['school_category'].unique()
        logger.info(f"School categories: {list(categories)}")
    else:
        categories = ['secondary']

    # Transform using secondary schema (covers most columns)
    df = transform_to_berlin_schema(df, 'secondary')

    # Save
    out_csv = FINAL_DIR / "dresden_school_master_table_berlin_schema.csv"
    df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out_csv}")

    out_parquet = FINAL_DIR / "dresden_school_master_table_berlin_schema.parquet"
    df.to_parquet(out_parquet, index=False)
    logger.info(f"Saved: {out_parquet}")

    # Also overwrite the _final files so downstream tools find them
    df.to_csv(final_csv, index=False, encoding='utf-8-sig')
    final_parquet = FINAL_DIR / "dresden_school_master_table_final.parquet"
    df.to_parquet(final_parquet, index=False)

    print(f"\n{'='*70}")
    print("DRESDEN → BERLIN SCHEMA ENFORCEMENT - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
