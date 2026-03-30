#!/usr/bin/env python3
"""
Enforce Berlin Schema on Frankfurt Data

Transforms Frankfurt final parquet files (with embeddings) to have ALL Berlin
columns in exact order, plus Frankfurt-specific extras appended.

Transformations:
1. Rename Frankfurt columns to Berlin equivalents
2. Map crime data to Berlin crime columns
3. Map ndH count to belastungsstufe proxy
4. Set constant metadata fields
5. Ensure all Berlin columns exist (add as NULL if missing)
6. Order: Berlin columns first, then Frankfurt extras
7. Assert schema match

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
FFM_DATA_DIR = PROJECT_ROOT / "data_frankfurt" / "final"

BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
BERLIN_PRI_REF = PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet"


def get_berlin_schema(school_type):
    ref = BERLIN_SEC_REF if school_type == "secondary" else BERLIN_PRI_REF
    if not ref.exists():
        raise FileNotFoundError(f"Berlin reference not found: {ref}")
    return list(pd.read_parquet(ref).columns)


def transform_to_berlin_schema(school_type):
    print(f"\n{'='*70}")
    print(f"ENFORCE BERLIN SCHEMA ON FRANKFURT {school_type.upper()} DATA")
    print(f"{'='*70}")

    ffm_parquet = FFM_DATA_DIR / f"frankfurt_{school_type}_school_master_table_final_with_embeddings.parquet"
    if not ffm_parquet.exists():
        print(f"  Frankfurt {school_type} parquet not found: {ffm_parquet}")
        return None

    ffm = pd.read_parquet(ffm_parquet)
    berlin_columns = get_berlin_schema(school_type)

    print(f"  Frankfurt input: {len(ffm)} schools, {len(ffm.columns)} columns")
    print(f"  Berlin target:   {len(berlin_columns)} columns")

    df = ffm.copy()

    # Step 1: Renames
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
    print(f"  Renames: {applied}")

    # Step 2: Crime mappings
    crime_map = {
        'crime_straftaten_2023': 'crime_total_crimes_2023',
        'crime_strassenraub_2023': 'crime_street_robbery_2023',
        'crime_koerperverletzung_2023': 'crime_assault_2023',
        'crime_diebstahl_fahrrad_2023': 'crime_bike_theft_2023',
    }
    for src, dst in crime_map.items():
        if src in df.columns:
            df[dst] = df[src]

    if 'crime_bezirk_index' in df.columns:
        def idx_to_cat(idx):
            if pd.isna(idx): return None
            if idx <= 0.7: return 'Sehr sicher'
            if idx <= 0.9: return 'Sicher'
            if idx <= 1.1: return 'Durchschnittlich'
            if idx <= 1.5: return 'Belastet'
            return 'Stark belastet'
        df['crime_safety_category'] = df['crime_bezirk_index'].apply(idx_to_cat)
        df['crime_safety_rank'] = df['crime_bezirk_index'].rank(method='dense', ascending=True)

    # Step 3: Metadata
    # ndH as proxy for belastungsstufe
    if 'ndh_count' in df.columns and 'schueler_gesamt' in df.columns:
        ndh_pct = pd.to_numeric(df['ndh_count'], errors='coerce') / pd.to_numeric(df['schueler_gesamt'], errors='coerce')
        # Map percentage to 1-9 scale (higher = more burdened)
        df['belastungsstufe'] = pd.cut(ndh_pct, bins=[-1, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.1],
                                        labels=[1, 2, 3, 4, 5, 6, 7, 8, 9])

    if 'schulform_name' in df.columns and 'schulart' not in df.columns:
        df['schulart'] = df.get('school_type', df.get('schulform_name'))

    df['metadata_source'] = 'Hessisches Statistisches Landesamt Verzeichnis 6'
    df['leistungsdaten_quelle'] = 'ndH-Anteil (Verzeichnis 6)'

    if 'traegerschaft' in df.columns:
        df['tuition_display'] = df['traegerschaft'].apply(
            lambda x: 'Kostenfrei (öffentliche Schule)' if pd.notna(x) and 'öffentlich' in str(x).lower()
            else 'Privat (Details auf Schulwebsite)' if pd.notna(x) and 'privat' in str(x).lower()
            else None
        )

    # Step 4: Build output
    output = pd.DataFrame(index=range(len(df)))
    populated, added_null = 0, 0
    for col in berlin_columns:
        if col in df.columns:
            output[col] = df[col].values
            populated += 1
        else:
            output[col] = None
            added_null += 1

    ffm_extras = sorted(set(df.columns) - set(berlin_columns))
    for col in ffm_extras:
        output[col] = df[col].values

    print(f"  Berlin cols from data: {populated}/{len(berlin_columns)}")
    print(f"  Berlin cols NULL:      {added_null}/{len(berlin_columns)}")
    print(f"  Frankfurt extras:      {len(ffm_extras)}")

    # Step 5: Verify
    assert list(output.columns)[:len(berlin_columns)] == berlin_columns, "Schema mismatch!"
    print(f"  PASS: {len(berlin_columns)} Berlin cols + {len(ffm_extras)} extras")

    # Step 6: Save
    out_pq = FFM_DATA_DIR / f"frankfurt_{school_type}_school_master_table_final_with_embeddings.parquet"
    output.to_parquet(out_pq, index=False)

    out_csv = FFM_DATA_DIR / f"frankfurt_{school_type}_school_master_table_final.csv"
    output.drop(columns=['embedding'], errors='ignore').to_csv(out_csv, index=False, encoding='utf-8-sig')

    # Berlin-schema files
    bs_pq = FFM_DATA_DIR / f"frankfurt_{school_type}_school_master_table_berlin_schema.parquet"
    output.to_parquet(bs_pq, index=False)
    bs_csv = FFM_DATA_DIR / f"frankfurt_{school_type}_school_master_table_berlin_schema.csv"
    output.drop(columns=['embedding'], errors='ignore').to_csv(bs_csv, index=False, encoding='utf-8-sig')

    print(f"  Saved: {out_pq.name}")

    # Data quality
    print("\n  Data quality:")
    for col in ['schulnummer', 'schulname', 'school_type', 'latitude', 'longitude',
                'transit_accessibility_score', 'crime_total_crimes_2023',
                'description', 'embedding', 'tuition_display']:
        if col in output.columns:
            if col == 'embedding':
                n = output[col].apply(lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0).sum()
            else:
                n = output[col].notna().sum()
            pct = n / len(output) * 100
            s = "+" if pct > 50 else "~" if pct > 0 else "-"
            print(f"    {s} {col}: {n}/{len(output)} ({pct:.0f}%)")

    print(f"\n{'='*70}")
    return output


def main():
    print(f"\n{'='*70}\nFRANKFURT → BERLIN SCHEMA ENFORCEMENT\n{'='*70}")
    results = {}
    for st in ['secondary', 'primary']:
        try:
            r = transform_to_berlin_schema(st)
            if r is not None:
                results[st] = len(r)
        except Exception as e:
            import traceback
            print(f"\n  FAILED for {st}: {e}")
            traceback.print_exc()

    print(f"\n{'='*70}\nSCHEMA ENFORCEMENT COMPLETE\n{'='*70}")
    for st, n in results.items():
        print(f"  {st}: {n} schools → Berlin schema")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
