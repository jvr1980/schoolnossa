#!/usr/bin/env python3
"""
Phase 9: Enforce Berlin Schema on Leipzig Data
================================================

Transforms Leipzig school data to match the Berlin schema exactly,
enabling use with the same frontend application.

Adapted from NRW/Hamburg schema transformers.

Transformations:
1. Rename Leipzig columns to their Berlin equivalents
2. Map Leipzig crime data (Ortsteil-level) to Berlin crime columns
3. Map Leipzig-specific fields (Ortsteil demographics, student counts)
4. Set constant fields (metadata_source, city)
5. Ensure all Berlin columns exist (add as NULL if missing)
6. Order: Berlin columns first (exact order), then Leipzig-specific extras
7. Assert all Berlin columns present
8. Overwrite the final files (both CSV and parquet)

Input: data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet
Reference: data_berlin/final/school_master_table_final_with_embeddings.parquet
Output: overwrites input with Berlin-aligned schema

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
LEIPZIG_DATA_DIR = PROJECT_ROOT / "data_leipzig" / "final"

# Berlin reference schemas (the ground truth)
BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
BERLIN_PRI_REF = PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet"


def get_berlin_schema(school_type: str = "secondary") -> list:
    """Load the exact column list from the Berlin reference parquet.

    Falls back to any available city's final parquet, or returns None if
    no reference is available (schema enforcement will be partial).
    """
    if school_type == "secondary":
        ref_path = BERLIN_SEC_REF
    else:
        ref_path = BERLIN_PRI_REF

    if ref_path.exists():
        berlin = pd.read_parquet(ref_path)
        return list(berlin.columns)

    # Try other cities as fallback references
    for city in ['hamburg', 'nrw', 'munich', 'stuttgart', 'frankfurt']:
        fallback = PROJECT_ROOT / f"data_{city}" / "final"
        for f in fallback.glob("*master_table_final_with_embeddings.parquet"):
            logger.info(f"Berlin reference not found, using {f.name} as schema reference")
            return list(pd.read_parquet(f).columns)

    logger.warning("No Berlin reference parquet found — schema enforcement will be partial")
    return None


def transform_to_berlin_schema():
    """Transform Leipzig data to match Berlin schema with zero deviation."""

    print(f"\n{'=' * 70}")
    print("PHASE 9: ENFORCE BERLIN SCHEMA ON LEIPZIG DATA")
    print(f"{'=' * 70}")

    # Load Leipzig data (the final parquet with embeddings)
    leipzig_parquet = LEIPZIG_DATA_DIR / "leipzig_school_master_table_final_with_embeddings.parquet"
    if not leipzig_parquet.exists():
        # Fall back to CSV + no embeddings
        leipzig_csv = LEIPZIG_DATA_DIR / "leipzig_school_master_table_final.csv"
        if not leipzig_csv.exists():
            print(f"  Leipzig data not found. Run phases 7-8 first.")
            print(f"  Expected: {leipzig_parquet}")
            return None
        print(f"  Parquet not found, loading from CSV (no embeddings)")
        leipzig = pd.read_csv(leipzig_csv, low_memory=False)
    else:
        leipzig = pd.read_parquet(leipzig_parquet)

    berlin_columns = get_berlin_schema("secondary")

    print(f"  Leipzig input:  {len(leipzig)} schools, {len(leipzig.columns)} columns")
    if berlin_columns:
        print(f"  Berlin target:  {len(berlin_columns)} columns")
    else:
        print(f"  Berlin target:  No reference available — partial enforcement only")

    # Work on a copy
    df = leipzig.copy()

    # =========================================================================
    # STEP 1: Direct column renames (Leipzig name -> Berlin name)
    # =========================================================================
    print("\n  Step 1: Column renames...")

    renames = {
        # Location
        'ortsteil': 'ortsteil',          # Same name, keep it
        'stadtbezirk': 'bezirk',         # Leipzig Stadtbezirk -> Berlin Bezirk
        'adresse': 'strasse',            # Address field
        'ort': 'ortsteil',               # If 'ort' used instead of ortsteil

        # Contact
        'telefon': 'telefon',            # Same
        'email': 'email',                # Same
        'website': 'website',            # Same

        # Leadership
        'schulleitung': 'leitung',       # School principal

        # School type
        'schultyp': 'schulart',          # School type

        # Operator
        'traeger': 'traegerschaft',      # Carrier/operator

        # Similar schools
        'most_similar_school_01': 'most_similar_school_no_01',
        'most_similar_school_02': 'most_similar_school_no_02',
        'most_similar_school_03': 'most_similar_school_no_03',
    }

    applied = 0
    for old, new in renames.items():
        if old in df.columns and new not in df.columns and old != new:
            df = df.rename(columns={old: new})
            applied += 1
    print(f"    Applied {applied} renames")

    # =========================================================================
    # STEP 2: Map Leipzig crime data to Berlin crime columns
    # =========================================================================
    print("  Step 2: Crime data mapping...")

    crime_mappings = {
        'crime_total': 'crime_total_crimes_2023',
        'crime_theft': 'crime_theft_2023',
        'crime_assault': 'crime_assault_2023',
        'crime_property': 'crime_property_damage_2023',
        'crime_per_1000_residents': 'crime_haeufigkeitszahl_2023',
    }

    crime_mapped = 0
    for leipzig_col, berlin_col in crime_mappings.items():
        if leipzig_col in df.columns:
            df[berlin_col] = df[leipzig_col]
            crime_mapped += 1

    # Map crime_ortsteil to crime_bezirk (Berlin uses bezirk)
    if 'crime_ortsteil' in df.columns and 'crime_bezirk' not in df.columns:
        df['crime_bezirk'] = df['crime_ortsteil']

    # Ensure crime_safety_category and crime_safety_rank exist
    if 'crime_safety_category' not in df.columns and 'crime_per_1000_residents' in df.columns:
        def rate_to_category(rate):
            if pd.isna(rate):
                return None
            if rate <= 50:
                return 'Sehr sicher'
            elif rate <= 80:
                return 'Sicher'
            elif rate <= 120:
                return 'Durchschnittlich'
            elif rate <= 180:
                return 'Belastet'
            else:
                return 'Stark belastet'
        df['crime_safety_category'] = df['crime_per_1000_residents'].apply(rate_to_category)

    if 'crime_safety_rank' not in df.columns and 'crime_per_1000_residents' in df.columns:
        df['crime_safety_rank'] = df['crime_per_1000_residents'].rank(
            method='dense', ascending=True
        )

    print(f"    Mapped {crime_mapped} crime columns + safety category/rank")

    # =========================================================================
    # STEP 3: Map Leipzig metadata fields
    # =========================================================================
    print("  Step 3: Metadata field mapping...")

    # Set constant metadata
    df['metadata_source'] = 'schuldatenbank_sachsen'
    df['city'] = 'Leipzig'

    # Generate tuition_display from traegerschaft
    traeger_col = 'traegerschaft' if 'traegerschaft' in df.columns else 'traeger'
    if traeger_col in df.columns and 'tuition_display' not in df.columns:
        def map_tuition(traeger):
            if pd.isna(traeger):
                return None
            traeger_str = str(traeger).lower()
            if any(kw in traeger_str for kw in ['stadt', 'kommune', 'oeffentlich', 'öffentlich', 'staatlich']):
                return 'Kostenfrei (oeffentliche Schule)'
            elif any(kw in traeger_str for kw in ['privat', 'frei', 'ev.', 'kath.', 'kirch']):
                return 'Privat (Details auf Schulwebsite)'
            return None
        df['tuition_display'] = df[traeger_col].apply(map_tuition)

    # Map student count if available
    if 'schueler_gesamt' in df.columns and 'schueler_2024_25' not in df.columns:
        df['schueler_2024_25'] = df['schueler_gesamt']

    # Map lehrer if available
    if 'lehrer_gesamt' in df.columns and 'lehrer_2024_25' not in df.columns:
        df['lehrer_2024_25'] = df['lehrer_gesamt']

    # Map sprachen if available
    if 'sprachen' not in df.columns:
        # Try to extract from besonderheiten or other fields
        pass

    print("    Done")

    # =========================================================================
    # STEP 4: Build output -- Berlin columns first, then Leipzig extras
    # =========================================================================
    print("  Step 4: Building output (Berlin columns + Leipzig extras)...")

    if berlin_columns:
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

        # Second: Leipzig-specific extra columns (not in Berlin schema)
        leipzig_extras = sorted(set(df.columns) - set(berlin_columns))
        for col in leipzig_extras:
            output[col] = df[col].values

        print(f"    Berlin columns from Leipzig data: {populated}/{len(berlin_columns)}")
        print(f"    Berlin columns added as NULL:     {added_null}/{len(berlin_columns)}")
        print(f"    Leipzig extra columns kept:        {len(leipzig_extras)}")

        if leipzig_extras:
            print(f"    Extras: {', '.join(leipzig_extras[:15])}{'...' if len(leipzig_extras) > 15 else ''}")

        # Schema verification
        print("  Step 5: Schema verification...")
        output_cols = list(output.columns)
        berlin_in_output = output_cols[:len(berlin_columns)]
        assert berlin_in_output == berlin_columns, (
            f"SCHEMA MISMATCH! Berlin columns not in correct order.\n"
            f"  Expected first {len(berlin_columns)} columns to match Berlin schema.\n"
            f"  Missing: {set(berlin_columns) - set(output_cols)}"
        )
        print(f"    PASS: {len(berlin_columns)} Berlin columns present + {len(leipzig_extras)} Leipzig extras")
    else:
        # No Berlin reference — just use Leipzig data as-is with renames applied
        output = df.copy()
        leipzig_extras = []
        print("    Skipping column ordering (no Berlin reference available)")

    # =========================================================================
    # STEP 6: Save -- overwrite the final files
    # =========================================================================
    print("  Step 6: Saving output...")

    # Save parquet (with embeddings)
    out_parquet = LEIPZIG_DATA_DIR / "leipzig_school_master_table_final_with_embeddings.parquet"
    output.to_parquet(out_parquet, index=False)
    print(f"    Saved: {out_parquet}")

    # Save CSV (without embeddings and temp columns)
    drop_cols = ['embedding', 'embedding_text', 'description_auto']
    csv_output = output.drop(columns=[c for c in drop_cols if c in output.columns], errors='ignore')
    out_csv = LEIPZIG_DATA_DIR / "leipzig_school_master_table_final.csv"
    csv_output.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f"    Saved: {out_csv}")

    # =========================================================================
    # STEP 7: Data quality report
    # =========================================================================
    print("\n  Data quality report:")

    key_cols = [
        'schulnummer', 'schulname', 'school_type', 'schulart', 'traegerschaft',
        'strasse', 'plz', 'ortsteil', 'bezirk',
        'latitude', 'longitude',
        'email', 'telefon', 'website',
        'transit_accessibility_score', 'transit_rail_01_name', 'transit_bus_01_name',
        'transit_stop_count_1000m',
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'crime_total_crimes_2023', 'crime_safety_category',
        'poi_supermarket_count_500m', 'poi_kita_count_500m',
        'description', 'description_de', 'embedding',
        'sprachen', 'besonderheiten',
        'tuition_display', 'metadata_source', 'city',
    ]

    for col in key_cols:
        if col in output.columns:
            if col == 'embedding':
                non_null = output[col].apply(
                    lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0
                ).sum()
            else:
                non_null = output[col].notna().sum()
            pct = non_null / len(output) * 100 if len(output) > 0 else 0
            status = "+" if pct > 50 else "~" if pct > 0 else "-"
            print(f"    {status} {col}: {non_null}/{len(output)} ({pct:.0f}%)")

    print(f"\n{'=' * 70}")
    print(f"  Leipzig: {len(output)} schools, {len(output.columns)} columns "
          f"({len(berlin_columns) if berlin_columns else 0} Berlin + {len(leipzig_extras)} Leipzig extras)")
    print(f"{'=' * 70}")

    return output


def main():
    """Transform Leipzig data to match Berlin schema."""
    print("\n" + "=" * 70)
    print("LEIPZIG -> BERLIN SCHEMA ENFORCEMENT")
    print("=" * 70)

    try:
        result = transform_to_berlin_schema()
        if result is not None:
            print("\n" + "=" * 70)
            print("SCHEMA ENFORCEMENT COMPLETE")
            print(f"  Leipzig: {len(result)} schools -> Berlin schema")
            print("=" * 70)
        else:
            print("\nSCHEMA ENFORCEMENT FAILED: No data to transform")
    except Exception as e:
        import traceback
        print(f"\nSCHEMA ENFORCEMENT FAILED: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
