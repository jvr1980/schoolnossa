#!/usr/bin/env python3
"""
Phase 8: Enforce Berlin Schema on NRW Data

This script takes the NRW final parquet files (with embeddings) and transforms
them to have ALL Berlin columns (in the same order) PLUS any NRW-specific columns
appended at the end.

The frontend expects all Berlin columns to be present. NRW-specific columns
(like crime_bezirk_population, crime_bezirk_index, crime_*_rate_per_100k) are
kept as extra columns for richer NRW analysis.

For secondary schools: match data_berlin/final/school_master_table_final_with_embeddings.parquet
For primary schools:   match data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

Transformations:
1. Rename NRW columns to their Berlin equivalents
2. Map NRW crime data (German names) to Berlin crime columns
3. Map NRW metadata fields (sozialindexstufe → belastungsstufe, etc.)
4. Compute derived crime metrics (safety category/rank from bezirk_index)
5. Set constant fields (metadata_source, leistungsdaten_quelle, tuition_display)
6. Ensure all Berlin columns exist (add as NULL if missing)
7. Order: Berlin columns first (exact order), then NRW-specific extras
8. Assert all Berlin columns present
9. Overwrite the *_final* files

Author: NRW School Data Pipeline
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
NRW_DATA_DIR = PROJECT_ROOT / "data_nrw" / "final"

# Berlin reference schemas (the ground truth)
BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
BERLIN_PRI_REF = PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet"


def get_berlin_schema(school_type: str) -> list:
    """Load the exact column list from the Berlin reference parquet."""
    if school_type == "secondary":
        ref_path = BERLIN_SEC_REF
    else:
        ref_path = BERLIN_PRI_REF

    if not ref_path.exists():
        raise FileNotFoundError(f"Berlin reference not found: {ref_path}")

    berlin = pd.read_parquet(ref_path)
    return list(berlin.columns)


def transform_to_berlin_schema(school_type: str):
    """Transform NRW data to match Berlin schema with zero deviation."""

    print(f"\n{'=' * 70}")
    print(f"PHASE 8: ENFORCE BERLIN SCHEMA ON NRW {school_type.upper()} DATA")
    print(f"{'=' * 70}")

    # Load NRW data (the final parquet with embeddings)
    nrw_parquet = NRW_DATA_DIR / f"nrw_{school_type}_school_master_table_final_with_embeddings.parquet"
    if not nrw_parquet.exists():
        print(f"  NRW {school_type} parquet not found: {nrw_parquet}")
        return None

    nrw = pd.read_parquet(nrw_parquet)
    berlin_columns = get_berlin_schema(school_type)

    print(f"  NRW input:      {len(nrw)} schools, {len(nrw.columns)} columns")
    print(f"  Berlin target:  {len(berlin_columns)} columns")

    # Work on a copy
    df = nrw.copy()

    # =========================================================================
    # STEP 1: Direct column renames (NRW name → Berlin name)
    # =========================================================================
    print("\n  Step 1: Column renames...")

    renames = {
        'ort': 'ortsteil',
        'crime_bezirk': 'bezirk',
        'most_similar_school_01': 'most_similar_school_no_01',
        'most_similar_school_02': 'most_similar_school_no_02',
        'most_similar_school_03': 'most_similar_school_no_03',
    }

    applied = 0
    for old, new in renames.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
            applied += 1
    print(f"    Applied {applied} renames")

    # =========================================================================
    # STEP 2: Map NRW crime data to Berlin crime columns
    # =========================================================================
    print("  Step 2: Crime data mapping...")

    crime_mappings = {
        'crime_straftaten_2023': 'crime_total_crimes_2023',
        'crime_strassenraub_2023': 'crime_street_robbery_2023',
        'crime_koerperverletzung_2023': 'crime_assault_2023',
        'crime_diebstahl_fahrrad_2023': 'crime_bike_theft_2023',
    }

    for nrw_col, berlin_col in crime_mappings.items():
        if nrw_col in df.columns:
            df[berlin_col] = df[nrw_col]

    # Compute crime safety category from bezirk_index
    if 'crime_bezirk_index' in df.columns:
        def index_to_category(idx):
            if pd.isna(idx):
                return None
            if idx <= 0.7:
                return 'Sehr sicher'
            elif idx <= 0.9:
                return 'Sicher'
            elif idx <= 1.1:
                return 'Durchschnittlich'
            elif idx <= 1.5:
                return 'Belastet'
            else:
                return 'Stark belastet'

        df['crime_safety_category'] = df['crime_bezirk_index'].apply(index_to_category)
        df['crime_safety_rank'] = df['crime_bezirk_index'].rank(method='dense', ascending=True)

    print(f"    Mapped {len(crime_mappings)} crime columns + safety category/rank")

    # =========================================================================
    # STEP 3: Map NRW metadata fields
    # =========================================================================
    print("  Step 3: Metadata field mapping...")

    # sozialindexstufe → belastungsstufe
    if 'sozialindexstufe' in df.columns:
        df['belastungsstufe'] = df['sozialindexstufe']

    # schulform_name → schulart
    if 'schulform_name' in df.columns and 'schulart' not in df.columns:
        df['schulart'] = df['schulform_name']

    # Set constant metadata
    df['metadata_source'] = 'NRW Schulministerium Open Data'
    df['leistungsdaten_quelle'] = 'Schulsozialindex NRW'

    # Generate tuition_display from traegerschaft
    if 'traegerschaft' in df.columns:
        df['tuition_display'] = df['traegerschaft'].apply(
            lambda x: 'Kostenfrei (öffentliche Schule)' if pd.notna(x) and 'öffentlich' in str(x).lower()
            else 'Privat (Details auf Schulwebsite)' if pd.notna(x) and 'privat' in str(x).lower()
            else None
        )

    print("    Done")

    # =========================================================================
    # STEP 4: Build output — Berlin columns first, then NRW extras
    # =========================================================================
    print("  Step 4: Building output (Berlin columns + NRW extras)...")

    output = pd.DataFrame(index=range(len(df)))

    # First: all Berlin columns in exact order
    populated = 0
    added_null = 0
    for col in berlin_columns:
        if col in df.columns:
            output[col] = df[col].values
            populated += 1
        else:
            output[col] = None
            added_null += 1

    # Second: NRW-specific extra columns (not in Berlin schema)
    nrw_extras = sorted(set(df.columns) - set(berlin_columns))
    for col in nrw_extras:
        output[col] = df[col].values

    print(f"    Berlin columns from NRW data:  {populated}/{len(berlin_columns)}")
    print(f"    Berlin columns added as NULL:   {added_null}/{len(berlin_columns)}")
    print(f"    NRW extra columns kept:         {len(nrw_extras)}")

    if nrw_extras:
        print(f"    Extras: {', '.join(nrw_extras[:15])}{'...' if len(nrw_extras) > 15 else ''}")

    # =========================================================================
    # STEP 5: Schema verification (all Berlin columns present)
    # =========================================================================
    print("  Step 5: Schema verification...")

    output_cols = list(output.columns)
    berlin_in_output = output_cols[:len(berlin_columns)]
    assert berlin_in_output == berlin_columns, (
        f"SCHEMA MISMATCH! Berlin columns not in correct order.\n"
        f"  Expected first {len(berlin_columns)} columns to match Berlin schema.\n"
        f"  Missing: {set(berlin_columns) - set(output_cols)}\n"
        f"  First mismatch at index: {next(i for i, (a, b) in enumerate(zip(berlin_in_output, berlin_columns)) if a != b)}"
    )
    print(f"    PASS: {len(berlin_columns)} Berlin columns present + {len(nrw_extras)} NRW extras")

    # =========================================================================
    # STEP 6: Save — overwrite the *_final* files
    # =========================================================================
    print("  Step 6: Saving output...")

    # Save parquet (with embeddings)
    out_parquet = NRW_DATA_DIR / f"nrw_{school_type}_school_master_table_final_with_embeddings.parquet"
    output.to_parquet(out_parquet, index=False)
    print(f"    Saved: {out_parquet}")

    # Save CSV (without embeddings)
    out_csv = NRW_DATA_DIR / f"nrw_{school_type}_school_master_table_final.csv"
    csv_output = output.drop(columns=['embedding'], errors='ignore')
    csv_output.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f"    Saved: {out_csv}")

    # Also save the berlin_schema files for backwards compatibility
    out_bs_parquet = NRW_DATA_DIR / f"nrw_{school_type}_school_master_table_berlin_schema.parquet"
    output.to_parquet(out_bs_parquet, index=False)

    out_bs_csv = NRW_DATA_DIR / f"nrw_{school_type}_school_master_table_berlin_schema.csv"
    csv_output.to_csv(out_bs_csv, index=False, encoding='utf-8-sig')

    # =========================================================================
    # STEP 7: Split by city
    # =========================================================================
    print("  Step 7: Splitting by city...")

    city_col = 'stadt' if 'stadt' in output.columns else None
    city_counts = {}

    if city_col:
        city_map = {
            'Köln': 'koeln',
            'Düsseldorf': 'duesseldorf',
        }
        for city_name, city_slug in city_map.items():
            city_df = output[output[city_col] == city_name].copy()
            if len(city_df) == 0:
                continue

            city_counts[city_name] = len(city_df)

            # Parquet (with embeddings)
            city_parquet = NRW_DATA_DIR / f"{city_slug}_{school_type}_school_master_table_final_with_embeddings.parquet"
            city_df.to_parquet(city_parquet, index=False)

            # CSV (without embeddings)
            city_csv = NRW_DATA_DIR / f"{city_slug}_{school_type}_school_master_table_final.csv"
            city_csv_df = city_df.drop(columns=['embedding'], errors='ignore')
            city_csv_df.to_csv(city_csv, index=False, encoding='utf-8-sig')

            print(f"    {city_name}: {len(city_df)} schools → {city_parquet.name}")

        # Check for schools not in any known city
        known_cities = set(city_map.keys())
        unknown = output[~output[city_col].isin(known_cities)]
        if len(unknown) > 0:
            print(f"    WARNING: {len(unknown)} schools with unknown city: {unknown[city_col].unique().tolist()}")
    else:
        print("    WARNING: No 'stadt' column found — cannot split by city")

    # =========================================================================
    # STEP 8: Data quality report
    # =========================================================================
    print("\n  Data quality report:")

    key_cols = [
        'schulnummer', 'schulname', 'school_type', 'traegerschaft',
        'strasse', 'plz', 'ortsteil', 'bezirk',
        'latitude', 'longitude',
        'email', 'telefon', 'website',
        'belastungsstufe',
        'transit_accessibility_score', 'transit_rail_01_name', 'transit_bus_01_name',
        'transit_stop_count_1000m',
        'crime_total_crimes_2023', 'crime_safety_category',
        'poi_supermarket_count_500m', 'poi_kita_count_500m',
        'description', 'embedding',
        'tuition_display', 'metadata_source',
    ]

    for col in key_cols:
        if col in output.columns:
            if col == 'embedding':
                non_null = output[col].apply(lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0).sum()
            else:
                non_null = output[col].notna().sum()
            pct = non_null / len(output) * 100
            status = "+" if pct > 50 else "~" if pct > 0 else "-"
            print(f"    {status} {col}: {non_null}/{len(output)} ({pct:.0f}%)")

    print(f"\n{'=' * 70}")
    print(f"  NRW {school_type}: {len(output)} schools, {len(output.columns)} columns "
          f"({len(berlin_columns)} Berlin + {len(nrw_extras)} NRW extras)")
    if city_counts:
        for city_name, count in city_counts.items():
            print(f"    → {city_name}: {count} schools")
    print(f"{'=' * 70}")

    return output


def main():
    """Transform both primary and secondary NRW data to Berlin schema."""
    print("\n" + "=" * 70)
    print("NRW → BERLIN SCHEMA ENFORCEMENT")
    print("=" * 70)

    results = {}
    for school_type in ['secondary', 'primary']:
        try:
            result = transform_to_berlin_schema(school_type)
            if result is not None:
                results[school_type] = len(result)
        except Exception as e:
            import traceback
            print(f"\n  FAILED for {school_type}: {e}")
            traceback.print_exc()

    print("\n" + "=" * 70)
    print("SCHEMA ENFORCEMENT COMPLETE")
    print("=" * 70)
    for st, count in results.items():
        print(f"  {st}: {count} schools → Berlin schema")
    print("=" * 70)


if __name__ == "__main__":
    main()
