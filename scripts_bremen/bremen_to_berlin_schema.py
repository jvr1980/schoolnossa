#!/usr/bin/env python3
"""
Phase 9: Transform Bremen school data to match Berlin schema exactly.

This script takes the Bremen school master table and transforms it to have
identical columns to the Berlin schema, enabling use with the same frontend.

Transformations:
1. Rename columns that have semantic equivalents
2. Map Bremen transit data to Berlin transit format
3. Add missing Berlin columns with NULL values
4. Drop Bremen-only columns
5. Reorder columns to match Berlin exactly

Input:
    - data_bremen/final/bremen_school_master_table_final.csv
    - data_bremen/final/bremen_school_master_table_final_with_embeddings.parquet
    - data_berlin/final/school_master_table_final_with_embeddings.parquet (reference)

Output:
    - data_bremen/final/bremen_school_master_table_berlin_schema.parquet
    - data_bremen/final/bremen_school_master_table_berlin_schema.csv

Reference: scripts_hamburg/hamburg_to_berlin_schema.py
Author: Bremen School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
BREMEN_CSV = PROJECT_ROOT / "data_bremen" / "final" / "bremen_school_master_table_final.csv"
BREMEN_EMBEDDINGS = PROJECT_ROOT / "data_bremen" / "final" / "bremen_school_master_table_final_with_embeddings.parquet"
BERLIN_REFERENCE = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
OUTPUT_DIR = PROJECT_ROOT / "data_bremen" / "final"


def extract_plz(addr):
    """Extract 5-digit PLZ from address string."""
    if pd.isna(addr):
        return None
    match = re.search(r'\b(\d{5})\b', str(addr))
    return match.group(1) if match else None


def transform_bremen_to_berlin_schema():
    """Transform Bremen data to match Berlin schema."""

    print("Loading data...")

    # Load Bremen data
    if not BREMEN_CSV.exists():
        raise FileNotFoundError(f"Bremen CSV not found: {BREMEN_CSV}")

    bremen = pd.read_csv(BREMEN_CSV, low_memory=False)

    # Load Berlin reference schema
    if not BERLIN_REFERENCE.exists():
        raise FileNotFoundError(f"Berlin reference not found: {BERLIN_REFERENCE}")

    berlin = pd.read_parquet(BERLIN_REFERENCE)

    # Merge embeddings from parquet if available
    if BREMEN_EMBEDDINGS.exists():
        emb_df = pd.read_parquet(BREMEN_EMBEDDINGS)
        if 'embedding' in emb_df.columns and 'schulnummer' in emb_df.columns:
            bremen['schulnummer'] = bremen['schulnummer'].astype(str)
            emb_df['schulnummer'] = emb_df['schulnummer'].astype(str)
            emb_cols = ['schulnummer', 'embedding']
            for c in ['most_similar_school_01', 'most_similar_school_02', 'most_similar_school_03']:
                if c in emb_df.columns:
                    emb_cols.append(c)
            bremen = bremen.merge(emb_df[emb_cols], on='schulnummer', how='left', suffixes=('', '_emb'))
            print(f"  Merged embeddings ({bremen['embedding'].notna().sum()} schools)")

    print(f"Bremen: {len(bremen)} schools, {len(bremen.columns)} columns")
    print(f"Berlin reference: {len(berlin)} schools, {len(berlin.columns)} columns")

    # Create output dataframe
    output = pd.DataFrame()

    # =========================================================================
    # STEP 1: Direct column mappings (rename Bremen -> Berlin)
    # =========================================================================
    print("\nStep 1: Applying column renames...")

    column_renames = {
        # Contact
        'email': 'email',
        'telefon': 'telefon',
        'website': 'website',
        # Location
        'stadtteil': 'ortsteil',
        # Leadership
        'leitung': 'leitung',
        # Similar schools
        'most_similar_school_01': 'most_similar_school_no_01',
        'most_similar_school_02': 'most_similar_school_no_02',
        'most_similar_school_03': 'most_similar_school_no_03',
        # Crime mappings: Bremen has crime_total, crime_assault, etc.
        'crime_total': 'crime_total_crimes_2023',
        'crime_assault': 'crime_assault_2023',
        'crime_robbery': 'crime_robbery_2023',
        'crime_burglary': 'crime_burglary_2023',
        'crime_theft': 'crime_theft_2023',
        'crime_drugs': 'crime_drugs_2023',
        'crime_sexual': 'crime_sexual_offenses_2023',
        'crime_beirat': 'crime_bezirk',
    }

    bremen_renamed = bremen.rename(columns=column_renames)

    for old, new in column_renames.items():
        if old in bremen.columns:
            print(f"  Renamed: {old} -> {new}")

    # Fill description gaps from summary_* fallback (Bremen's description pipeline
    # populated summary_en/summary_de more consistently than description_en/de)
    for desc_col, src_col in [('description_en', 'summary_en'), ('description_de', 'summary_de')]:
        if src_col in bremen_renamed.columns:
            if desc_col not in bremen_renamed.columns:
                bremen_renamed[desc_col] = bremen_renamed[src_col]
                filled = bremen_renamed[desc_col].notna().sum()
                print(f"  Filled {desc_col} from {src_col}: {filled}")
            else:
                mask = bremen_renamed[desc_col].isna() | (
                    bremen_renamed[desc_col].astype(str).str.strip().isin(['', 'None', 'nan'])
                )
                filled_before = bremen_renamed[desc_col].notna().sum()
                bremen_renamed.loc[mask, desc_col] = bremen_renamed.loc[mask, src_col]
                filled_after = bremen_renamed[desc_col].notna().sum()
                print(f"  Filled {desc_col} from {src_col}: {filled_before} -> {filled_after}")

    # =========================================================================
    # STEP 2: Extract/transform specific columns
    # =========================================================================
    print("\nStep 2: Extracting/transforming data...")

    # Ensure PLZ column exists (may already be in raw data)
    if 'plz' not in bremen_renamed.columns or bremen_renamed['plz'].isna().all():
        if 'strasse' in bremen.columns:
            bremen_renamed['plz'] = bremen['strasse'].apply(extract_plz)
            count = bremen_renamed['plz'].notna().sum()
            print(f"  Extracted PLZ: {count}/{len(bremen_renamed)} schools")

    # Map transit columns to Berlin format
    # Bremen transit enrichment already produces transit_rail_01, transit_bus_01 etc.
    # which match Berlin format. Just verify they exist.
    transit_cols_to_check = [
        'transit_rail_01_name', 'transit_rail_01_distance_m',
        'transit_bus_01_name', 'transit_bus_01_distance_m',
        'transit_tram_01_name', 'transit_tram_01_distance_m',
        'transit_stop_count_1000m', 'transit_accessibility_score',
    ]
    for col in transit_cols_to_check:
        if col in bremen_renamed.columns:
            count = bremen_renamed[col].notna().sum()
            if count > 0:
                print(f"  Transit column {col}: {count} values")

    # Map crime safety score/category/rank
    if 'crime_safety_score' in bremen_renamed.columns:
        print(f"  crime_safety_score: {bremen_renamed['crime_safety_score'].notna().sum()} values")
    if 'crime_safety_category' in bremen_renamed.columns:
        print(f"  crime_safety_category: {bremen_renamed['crime_safety_category'].notna().sum()} values")
    if 'crime_safety_rank' in bremen_renamed.columns:
        print(f"  crime_safety_rank: {bremen_renamed['crime_safety_rank'].notna().sum()} values")

    # =========================================================================
    # STEP 3: Build output with exact Berlin schema
    # =========================================================================
    print("\nStep 3: Building output with Berlin schema...")

    berlin_columns = list(berlin.columns)

    for col in berlin_columns:
        if col in bremen_renamed.columns:
            output[col] = bremen_renamed[col]
        else:
            output[col] = None

    # Preserve Bremen-specific columns the Supabase uploader expects but that
    # aren't in the Berlin reference (e.g. description_en).
    preserve_extras = ['description_en']
    for col in preserve_extras:
        if col in bremen_renamed.columns and col not in output.columns:
            output[col] = bremen_renamed[col].values

    # Count populated vs empty
    populated = sum(1 for col in berlin_columns if col in bremen_renamed.columns)
    print(f"  Columns from Bremen data: {populated}/{len(berlin_columns)}")
    print(f"  Columns set to NULL: {len(berlin_columns) - populated}/{len(berlin_columns)}")

    # =========================================================================
    # STEP 4: Verify schema match
    # =========================================================================
    print("\nStep 4: Verifying schema...")

    output_berlin_cols = list(output.columns[:len(berlin_columns)])
    assert output_berlin_cols == berlin_columns, "Berlin column order mismatch!"
    extras_present = [c for c in output.columns if c not in berlin_columns]
    print(f"  Schema verified: {len(berlin_columns)} Berlin cols + {len(extras_present)} extras")

    # =========================================================================
    # STEP 5: Data quality report
    # =========================================================================
    print("\nStep 5: Data quality report...")

    key_cols = [
        'schulnummer', 'schulname', 'latitude', 'longitude',
        'bezirk', 'ortsteil', 'plz', 'strasse',
        'email', 'telefon', 'website',
        'leitung', 'traegerschaft',
        'schueler_2024_25', 'lehrer_2024_25',
        'sprachen', 'besonderheiten',
        'transit_accessibility_score',
        'traffic_safety_score', 'traffic_accidents_total',
        'crime_safety_category', 'crime_safety_rank',
        'crime_total_crimes_2023',
        'description', 'embedding',
    ]

    print("\n  Key column population:")
    for col in key_cols:
        if col in output.columns:
            if col == 'embedding':
                non_null = output[col].apply(
                    lambda x: x is not None and (isinstance(x, list) and len(x) > 0)
                    if not isinstance(x, float) else False
                ).sum()
            else:
                non_null = output[col].notna().sum()
            pct = non_null / len(output) * 100
            status = "+" if pct > 50 else "o" if pct > 0 else "x"
            print(f"    [{status}] {col}: {non_null}/{len(output)} ({pct:.0f}%)")

    # =========================================================================
    # STEP 6: Save output
    # =========================================================================
    print("\nStep 6: Saving output...")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save as parquet
    output_parquet = OUTPUT_DIR / "bremen_school_master_table_berlin_schema.parquet"
    output.to_parquet(output_parquet, index=False)
    print(f"  Saved: {output_parquet}")

    # Save as CSV (without embedding for readability)
    output_csv = OUTPUT_DIR / "bremen_school_master_table_berlin_schema.csv"
    csv_output = output.drop(columns=['embedding'], errors='ignore')
    csv_output.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"  Saved: {output_csv}")

    # Primary/secondary split (refresh the split files consumed by Supabase uploader)
    def _bucket(v):
        if pd.isna(v):
            return 'secondary'
        return 'primary' if 'grund' in str(v).strip().lower() else 'secondary'

    type_col = 'school_type' if 'school_type' in csv_output.columns else 'schulart'
    if type_col in csv_output.columns:
        buckets = csv_output[type_col].apply(_bucket)
        for label in ('primary', 'secondary'):
            sub_csv = csv_output[buckets == label]
            sub_pq = output[buckets.values == label]
            if sub_csv.empty:
                continue
            split_csv = OUTPUT_DIR / f"bremen_{label}_school_master_table_berlin_schema.csv"
            split_pq = OUTPUT_DIR / f"bremen_{label}_school_master_table_berlin_schema.parquet"
            sub_csv.to_csv(split_csv, index=False, encoding='utf-8-sig')
            sub_pq.to_parquet(split_pq, index=False)
            print(f"  Saved: {split_csv.name} ({len(sub_csv)} schools)")

    # =========================================================================
    # Summary
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("TRANSFORMATION COMPLETE")
    print(f"{'=' * 60}")
    print(f"Input:  {len(bremen)} Bremen schools, {len(bremen.columns)} columns")
    print(f"Output: {len(output)} schools, {len(output.columns)} columns (Berlin schema)")
    print(f"\nOutput files:")
    print(f"  - {output_parquet}")
    print(f"  - {output_csv}")

    return output


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting Bremen-to-Berlin Schema Transformation")
    logger.info("=" * 60)

    transform_bremen_to_berlin_schema()


if __name__ == "__main__":
    main()
