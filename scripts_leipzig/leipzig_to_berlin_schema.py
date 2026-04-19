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

import re

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


def _format_telefon(code, number):
    """Format phone_code_1 + phone_number_1 as '0{area} {number}'."""
    if pd.isna(code) or pd.isna(number):
        return None
    try:
        code_s = str(int(float(code)))
    except (ValueError, TypeError):
        code_s = str(code).strip()
    num_s = str(number).strip()
    if num_s.endswith('.0'):
        num_s = num_s[:-2]
    if not code_s or not num_s:
        return None
    return f'0{code_s} {num_s}'


def _join_name(first, last):
    if pd.isna(first) and pd.isna(last):
        return None
    parts = []
    if not pd.isna(first):
        parts.append(str(first).strip())
    if not pd.isna(last):
        parts.append(str(last).strip())
    name = ' '.join(p for p in parts if p)
    return name or None


def _classify_school_type(val):
    """Return 'primary' for Grundschule, 'secondary' for everything else.

    Matches the Supabase split (Sonstige + Förderschulen are grouped with
    secondary to keep the 94-school count stable).
    """
    if pd.isna(val):
        return 'secondary'
    s = str(val).strip().lower()
    if 'grund' in s:
        return 'primary'
    return 'secondary'


def _write_split_outputs(parquet_df: pd.DataFrame, csv_df: pd.DataFrame) -> None:
    """Write primary/secondary berlin_schema files consumed by the Supabase uploader."""
    type_col = None
    for c in ('school_type', 'schulart', 'schultyp'):
        if c in csv_df.columns:
            type_col = c
            break
    if type_col is None:
        print("    WARN: no school_type column found — skipping primary/secondary split")
        return

    classified = csv_df[type_col].apply(_classify_school_type)
    for bucket in ('primary', 'secondary'):
        sub_csv = csv_df[classified == bucket]
        sub_pq = parquet_df[classified.values == bucket]
        if sub_csv.empty:
            continue
        csv_path = LEIPZIG_DATA_DIR / f"leipzig_{bucket}_school_master_table_berlin_schema.csv"
        pq_path = LEIPZIG_DATA_DIR / f"leipzig_{bucket}_school_master_table_berlin_schema.parquet"
        sub_csv.to_csv(csv_path, index=False, encoding='utf-8-sig')
        sub_pq.to_parquet(pq_path, index=False)
        print(f"    Saved: {csv_path.name} ({len(sub_csv)} schools)")
        print(f"    Saved: {pq_path.name} ({len(sub_pq)} schools)")


def _fill_saxon_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Populate Berlin-canonical columns from Saxon Schuldatenbank raw columns.

    Fills gaps only — never overwrites existing values. Shared logic with
    the Dresden mapper (same upstream source).
    """
    df = df.copy()

    def _ensure(col):
        if col not in df.columns:
            df[col] = None

    # Email from `mail` (unusual in Leipzig but covers future runs) or school_portal_mail
    _ensure('email')
    for src in ('mail', 'school_portal_mail'):
        if src in df.columns:
            mask = df['email'].isna()
            df.loc[mask, 'email'] = df.loc[mask, src]

    # Telefon from phone_code_1 + phone_number_1
    _ensure('telefon')
    if 'phone_code_1' in df.columns and 'phone_number_1' in df.columns:
        mask = df['telefon'].isna()
        df.loc[mask, 'telefon'] = df.loc[mask].apply(
            lambda r: _format_telefon(r.get('phone_code_1'), r.get('phone_number_1')),
            axis=1,
        )

    # Leitung from headmaster_firstname + headmaster_lastname
    _ensure('leitung')
    if 'headmaster_firstname' in df.columns and 'headmaster_lastname' in df.columns:
        mask = df['leitung'].isna() | (df['leitung'].astype(str).str.strip() == '')
        df.loc[mask, 'leitung'] = df.loc[mask].apply(
            lambda r: _join_name(r.get('headmaster_firstname'), r.get('headmaster_lastname')),
            axis=1,
        )

    # Ortsteil from community_part (Leipzig's raw Ortsteil field)
    _ensure('ortsteil')
    if 'community_part' in df.columns:
        mask = df['ortsteil'].isna() | (df['ortsteil'].astype(str).str.strip() == '')
        df.loc[mask, 'ortsteil'] = df.loc[mask, 'community_part']

    return df


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

    # Fill Berlin-canonical contact/location columns from raw Saxon source
    # (handles cases where the rename condition above was blocked by a
    # previously-added NULL column from an earlier schema pass)
    df = _fill_saxon_gaps(df)

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

    # Split into primary/secondary berlin_schema files (refresh stale artifacts
    # that the Supabase uploader consumes)
    _write_split_outputs(output, csv_output)

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
