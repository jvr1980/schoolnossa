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

    # Step 1: Renames — Schulwegweiser schema → Berlin schema
    renames = {
        # Old Verzeichnis 6 schema (kept for backward compat if re-run on old data)
        'ort': 'ortsteil',
        'crime_bezirk': 'bezirk',
        # Similar-schools columns
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

    # Leitung: Schulwegweiser provides `schulleitung`; fill gaps in `leitung`
    if 'schulleitung' in df.columns:
        if 'leitung' not in df.columns:
            df['leitung'] = df['schulleitung']
        else:
            mask = df['leitung'].isna() | (df['leitung'].astype(str).str.strip().isin(['', 'None', 'nan']))
            df.loc[mask, 'leitung'] = df.loc[mask, 'schulleitung']

    # Step 2: Derived / mapped columns from Schulwegweiser data

    # Student count: map schueler_gesamt → schueler_2024_25 (fill gaps only)
    if 'schueler_gesamt' in df.columns:
        schueler_num = pd.to_numeric(df['schueler_gesamt'], errors='coerce')
        if 'schueler_2024_25' not in df.columns:
            df['schueler_2024_25'] = schueler_num
        else:
            mask = df['schueler_2024_25'].isna()
            df.loc[mask, 'schueler_2024_25'] = schueler_num[mask]

    # Teacher count: map lehrer_gesamt/lehrer_anzahl → lehrer_2024_25 (fill gaps only)
    for src_col in ['lehrer_gesamt', 'lehrer_anzahl', 'lehrer_2024_25_raw']:
        if src_col in df.columns:
            lehrer_num = pd.to_numeric(df[src_col], errors='coerce')
            if 'lehrer_2024_25' not in df.columns:
                df['lehrer_2024_25'] = lehrer_num
            else:
                mask = df['lehrer_2024_25'].isna()
                df.loc[mask, 'lehrer_2024_25'] = lehrer_num[mask]
            break

    # schulart from school_type (Schulwegweiser already has proper Schulform)
    if 'school_type' in df.columns and 'schulart' not in df.columns:
        df['schulart'] = df['school_type']

    # sprachen: Schulwegweiser provides this directly; also combine sub-fields if needed
    if 'sprachen' not in df.columns or df['sprachen'].isna().all():
        sprachen_parts = []
        for col in ['fruehe_fremdsprache', 'erste_fremdsprache',
                    'zweite_fremdsprache', 'dritte_fremdsprache']:
            if col in df.columns:
                sprachen_parts.append(df[col])
        if sprachen_parts:
            df['sprachen'] = df[sprachen_parts[0].name] if len(sprachen_parts) == 1 else \
                pd.concat(sprachen_parts, axis=1).apply(
                    lambda row: ', '.join(v for v in row if pd.notna(v) and str(v).strip()), axis=1
                ).replace('', None)

    # Crime mappings
    crime_map = {
        'crime_straftaten_2023':       'crime_total_crimes_2023',
        'crime_strassenraub_2023':     'crime_street_robbery_2023',
        'crime_koerperverletzung_2023':'crime_assault_2023',
        'crime_diebstahl_fahrrad_2023':'crime_bike_theft_2023',
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

    # Step 3: Metadata & derived fields

    # belastungsstufe: use ndH ratio if available, else None
    if 'ndh_count' in df.columns and 'schueler_gesamt' in df.columns:
        ndh_pct = pd.to_numeric(df['ndh_count'], errors='coerce') / pd.to_numeric(df['schueler_gesamt'], errors='coerce')
        df['belastungsstufe'] = pd.cut(
            ndh_pct,
            bins=[-1, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.1],
            labels=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        )

    # tuition_display: Schulwegweiser provides Trägerschaft with city/private info
    if 'traegerschaft' in df.columns:
        def traeger_to_tuition(x):
            if pd.isna(x): return None
            xs = str(x).lower()
            if 'stadt frankfurt' in xs or 'öffentlich' in xs or 'staatlich' in xs:
                return 'Kostenfrei (öffentliche Schule)'
            if 'privat' in xs or 'frei' in xs or 'kirchlich' in xs:
                return 'Privat (Details auf Schulwebsite)'
            return None
        df['tuition_display'] = df['traegerschaft'].apply(traeger_to_tuition)

    # Schulwegweiser-specific fields that map to Berlin schema names
    # (besonderheiten → description supplement; profile → leistungsprofil)
    if 'profile' in df.columns and 'leistungsprofil' not in df.columns:
        df['leistungsprofil'] = df['profile']

    if 'ganztagsform' in df.columns and 'betreuungsangebot' not in df.columns:
        df['betreuungsangebot'] = df['ganztagsform']

    # Metadata source — now Schulwegweiser is primary
    df['metadata_source'] = 'Frankfurt Schulwegweiser (frankfurt.de)'
    if 'ndh_count' in df.columns and df['ndh_count'].notna().any():
        df['leistungsdaten_quelle'] = 'ndH-Anteil (Verzeichnis 6 join)'
    else:
        df['leistungsdaten_quelle'] = None

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

    # Guard: school_type must be a specific German type, never 'secondary'/'primary'
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        from scripts_shared.schema.core_schema import validate_school_types
        validate_school_types(output, city="Frankfurt", strict=True)
    except ImportError:
        pass

    # Data quality
    print("\n  Data quality:")
    for col in ['schulnummer', 'schulname', 'school_type', 'ortsteil',
                'latitude', 'longitude', 'website', 'email',
                'schulleitung', 'ganztagsform', 'profile', 'besonderheiten',
                'auszeichnungen', 'sprachen',
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
