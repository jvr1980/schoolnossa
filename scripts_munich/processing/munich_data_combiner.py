#!/usr/bin/env python3
"""
Phase 7: Munich Data Combiner

Finds the most-enriched intermediate file and outputs it as the master table.
All enrichment phases chain their outputs, so the last file contains all columns.

Input: data_munich/intermediate/munich_secondary_schools_with_*.csv
Output: data_munich/final/munich_secondary_school_master_table.csv
        data_munich/final/munich_secondary_school_master_table.parquet

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"


def find_most_enriched_file(school_type='secondary'):
    """Find the most enriched intermediate file (last in the chain)."""
    candidates = [
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_metadata.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_pois.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_crime.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_transit.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_traffic.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools.csv",
    ]
    for fp in candidates:
        if fp.exists():
            df = pd.read_csv(fp)
            logger.info(f"Loaded {len(df)} schools from {fp.name} ({len(df.columns)} columns)")
            return df
    raise FileNotFoundError(f"No {school_type} school data found in intermediate/")


def _carry_forward_paid_columns(df, school_type):
    """Preserve paid-enrichment columns (POI, descriptions, embeddings) across a
    base-data refresh by merging them back from the previous final parquet on
    schulnummer. Fill-gaps only — fresh values always win. See
    scripts_shared/processing/merge_enriched_columns.py."""
    import sys
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from scripts_shared.processing.merge_enriched_columns import merge_enriched_columns

    prev_path = FINAL_DIR / f"munich_{school_type}_school_master_table_final_with_embeddings.parquet"
    if not prev_path.exists():
        logger.info(f"No previous final parquet at {prev_path.name} — skipping merge-back")
        return df
    prev = pd.read_parquet(prev_path)
    return merge_enriched_columns(df, prev)


MUNICH_MUNICIPALITY = 'münchen'


def _drop_non_munich_rows(df):
    """Guard against non-Munich rows that reached the intermediates through an
    earlier substring filter (Waldmünchen, Schwabmünchen, ... 'b.München').
    Only the jedeschule base is checked: rows without an `ort` value and the
    researched private schools (MUCPRIV_*, re-ingested after the combiner,
    some deliberately in Landkreis München) are left alone."""
    if 'ort' not in df.columns:
        return df
    ort = (df['ort'].astype(str).str.strip().str.lower()
           .str.replace('muenchen', 'münchen', regex=False))
    is_private = df['schulnummer'].astype(str).str.startswith('MUCPRIV') \
        if 'schulnummer' in df.columns else pd.Series(False, index=df.index)
    drop = df['ort'].notna() & ort.ne(MUNICH_MUNICIPALITY) & ~is_private
    if drop.any():
        names = list(df.loc[drop, 'schulname'].astype(str).str.slice(0, 40)) \
            if 'schulname' in df.columns else []
        logger.warning(f"Dropping {int(drop.sum())} non-Munich rows (ort != München): {names}")
        df = df[~drop].copy()
    return df


def combine_school_type(school_type='secondary'):
    logger.info(f"Combining {school_type} school data...")

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    df = find_most_enriched_file(school_type)
    df = _drop_non_munich_rows(df)
    df = _carry_forward_paid_columns(df, school_type)

    csv_path = FINAL_DIR / f"munich_{school_type}_school_master_table.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved CSV: {csv_path}")

    parquet_path = FINAL_DIR / f"munich_{school_type}_school_master_table.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved parquet: {parquet_path}")

    print(f"\n{'='*70}")
    print(f"MUNICH DATA COMBINER ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Output: {csv_path.name}")
    print(f"{'='*70}")

    return df


def main(school_type='secondary'):
    logger.info("=" * 60)
    logger.info(f"Phase 7: Munich Data Combiner ({school_type})")
    logger.info("=" * 60)
    return combine_school_type(school_type)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
