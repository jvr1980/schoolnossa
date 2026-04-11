#!/usr/bin/env python3
"""
Phase 9: Enforce Berlin Schema on Munich Data

Transforms Munich final parquet files (with embeddings) to have ALL Berlin
columns in exact order, plus Munich-specific extras appended.

Reference: data_berlin/final/school_master_table_final_with_embeddings.parquet

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
MUC_DATA_DIR = PROJECT_ROOT / "data_munich" / "final"

BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"


def get_berlin_schema():
    if not BERLIN_SEC_REF.exists():
        raise FileNotFoundError(f"Berlin reference not found: {BERLIN_SEC_REF}")
    return list(pd.read_parquet(BERLIN_SEC_REF).columns)


def transform_to_berlin_schema(school_type='secondary'):
    print(f"\n{'='*70}")
    print(f"ENFORCE BERLIN SCHEMA ON MUNICH {school_type.upper()} DATA")
    print(f"{'='*70}")

    muc_parquet = MUC_DATA_DIR / f"munich_{school_type}_school_master_table_final_with_embeddings.parquet"
    if not muc_parquet.exists():
        print(f"  Munich parquet not found: {muc_parquet}")
        return None

    muc = pd.read_parquet(muc_parquet)
    berlin_columns = get_berlin_schema()

    print(f"  Munich input: {len(muc)} schools, {len(muc.columns)} columns")
    print(f"  Berlin target: {len(berlin_columns)} columns")

    df = muc.copy()

    # Step 1: Renames
    renames = {
        'ort': 'ortsteil',
        'crime_bezirk': 'bezirk',
    }
    applied = 0
    for old, new in renames.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
            applied += 1
    print(f"  Renames: {applied}")

    # Step 2a: Student/teacher count passthrough (fill gaps only)
    for src_col in ['schueler_gesamt', 'schueler_2024_25_raw']:
        if src_col in df.columns:
            num = pd.to_numeric(df[src_col], errors='coerce')
            if 'schueler_2024_25' not in df.columns:
                df['schueler_2024_25'] = num
            else:
                mask = df['schueler_2024_25'].isna()
                df.loc[mask, 'schueler_2024_25'] = num[mask]
            break

    for src_col in ['lehrer_gesamt', 'lehrer_anzahl', 'lehrer_2024_25_raw']:
        if src_col in df.columns:
            num = pd.to_numeric(df[src_col], errors='coerce')
            if 'lehrer_2024_25' not in df.columns:
                df['lehrer_2024_25'] = num
            else:
                mask = df['lehrer_2024_25'].isna()
                df.loc[mask, 'lehrer_2024_25'] = num[mask]
            break

    # Step 2b: Crime mappings (German → Berlin schema English)
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
            idx = float(idx)
            if idx <= 0.7: return 'Sehr sicher'
            if idx <= 0.9: return 'Sicher'
            if idx <= 1.1: return 'Durchschnittlich'
            if idx <= 1.5: return 'Belastet'
            return 'Stark belastet'
        df['crime_safety_category'] = df['crime_bezirk_index'].apply(idx_to_cat)
        df['crime_safety_rank'] = pd.to_numeric(df['crime_bezirk_index'], errors='coerce').rank(
            method='dense', ascending=True)

    # Step 3: Metadata
    df['metadata_source'] = 'Bayern Kultusministerium Schulsuche'
    df['leistungsdaten_quelle'] = 'Nicht verfügbar (Bayern)'

    if 'traegerschaft' in df.columns:
        df['tuition_display'] = df['traegerschaft'].apply(
            lambda x: 'Kostenfrei (öffentliche Schule)' if pd.notna(x) and 'öffentlich' in str(x).lower()
            else 'Privat (Details auf Schulwebsite)' if pd.notna(x) and 'privat' in str(x).lower()
            else None
        )

    if 'schulart' not in df.columns and 'school_type' in df.columns:
        df['schulart'] = df['school_type']

    # Step 4: Build output — Berlin columns first, then Munich extras
    output = pd.DataFrame(index=range(len(df)))
    populated, added_null = 0, 0
    for col in berlin_columns:
        if col in df.columns:
            output[col] = df[col].values
            populated += 1
        else:
            output[col] = None
            added_null += 1

    muc_extras = sorted(set(df.columns) - set(berlin_columns))
    for col in muc_extras:
        output[col] = df[col].values

    print(f"  Berlin cols from data: {populated}/{len(berlin_columns)}")
    print(f"  Berlin cols NULL:      {added_null}/{len(berlin_columns)}")
    print(f"  Munich extras:         {len(muc_extras)}")

    # Step 5: Verify
    assert list(output.columns)[:len(berlin_columns)] == berlin_columns, "Schema mismatch!"
    print(f"  PASS: {len(berlin_columns)} Berlin cols + {len(muc_extras)} extras")

    # Step 6: Save
    out_pq = MUC_DATA_DIR / f"munich_{school_type}_school_master_table_final_with_embeddings.parquet"
    output.to_parquet(out_pq, index=False)

    out_csv = MUC_DATA_DIR / f"munich_{school_type}_school_master_table_final.csv"
    output.drop(columns=['embedding'], errors='ignore').to_csv(out_csv, index=False, encoding='utf-8-sig')

    bs_pq = MUC_DATA_DIR / f"munich_{school_type}_school_master_table_berlin_schema.parquet"
    output.to_parquet(bs_pq, index=False)
    bs_csv = MUC_DATA_DIR / f"munich_{school_type}_school_master_table_berlin_schema.csv"
    output.drop(columns=['embedding'], errors='ignore').to_csv(bs_csv, index=False, encoding='utf-8-sig')

    print(f"  Saved: {out_pq.name}")

    # Data quality
    print("\n  Data quality:")
    for col in ['schulnummer', 'schulname', 'school_type', 'latitude', 'longitude',
                'transit_accessibility_score', 'crime_total_crimes_2023',
                'description_de', 'embedding', 'tuition_display']:
        if col in output.columns:
            if col == 'embedding':
                n = output[col].apply(lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0).sum()
            else:
                n = output[col].notna().sum()
            pct = n / len(output) * 100
            s = "+" if pct > 50 else "~" if pct > 0 else "-"
            print(f"    {s} {col}: {n}/{len(output)} ({pct:.0f}%)")

    return output


def main(school_type='secondary'):
    print(f"\n{'='*70}\nMUNICH → BERLIN SCHEMA ENFORCEMENT ({school_type.upper()})\n{'='*70}")
    result = transform_to_berlin_schema(school_type)
    if result is not None:
        print(f"\n  {school_type}: {len(result)} schools → Berlin schema")
    print(f"{'='*70}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
