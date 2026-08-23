#!/usr/bin/env python3
"""
Stuttgart → Berlin Schema Enforcement
Transforms Stuttgart final data to match Berlin column schema.

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
STG_DATA_DIR = PROJECT_ROOT / "data_stuttgart" / "final"

BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
BERLIN_PRI_REF = PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet"


def get_berlin_schema(school_type):
    ref = BERLIN_SEC_REF if school_type == 'secondary' else BERLIN_PRI_REF
    if not ref.exists():
        raise FileNotFoundError(f"Berlin reference not found: {ref}")
    return list(pd.read_parquet(ref).columns)


def transform_to_berlin_schema(school_type):
    print(f"\n{'='*70}")
    print(f"ENFORCE BERLIN SCHEMA ON STUTTGART {school_type.upper()} DATA")
    print(f"{'='*70}")

    stg_parquet = STG_DATA_DIR / f"stuttgart_{school_type}_school_master_table_final_with_embeddings.parquet"
    if not stg_parquet.exists():
        # Try without embeddings
        stg_parquet = STG_DATA_DIR / f"stuttgart_{school_type}_school_master_table.parquet"
    if not stg_parquet.exists():
        print(f"  Stuttgart {school_type} parquet not found")
        return None

    stg = pd.read_parquet(stg_parquet)
    berlin_columns = get_berlin_schema(school_type)

    print(f"  Stuttgart: {len(stg)} schools, {len(stg.columns)} cols")
    print(f"  Berlin target: {len(berlin_columns)} cols")

    df = stg.copy()

    # Renames
    renames = {
        'ort': 'ortsteil',
        'crime_bezirk': 'bezirk',
        'most_similar_school_01': 'most_similar_school_no_01',
        'most_similar_school_02': 'most_similar_school_no_02',
        'most_similar_school_03': 'most_similar_school_no_03',
    }
    for old, new in renames.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Crime mapping
    crime_map = {
        'crime_straftaten_2025': 'crime_total_crimes_2025',
        'crime_strassenraub_2025': 'crime_street_robbery_2025',
        'crime_koerperverletzung_2025': 'crime_assault_2025',
        'crime_diebstahl_fahrrad_2025': 'crime_bike_theft_2025',
    }
    for src, dst in crime_map.items():
        if src in df.columns:
            df[dst] = df[src]

    # Safety category
    if 'crime_bezirk_index' in df.columns:
        def idx_to_cat(x):
            if pd.isna(x): return None
            if x <= 0.7: return 'Sehr sicher'
            if x <= 0.9: return 'Sicher'
            if x <= 1.1: return 'Durchschnittlich'
            if x <= 1.5: return 'Belastet'
            return 'Stark belastet'
        df['crime_safety_category'] = df['crime_bezirk_index'].apply(idx_to_cat)
        df['crime_safety_rank'] = df['crime_bezirk_index'].rank(method='dense', ascending=True)

    # Student/teacher count passthrough (fill gaps only)
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

    # Metadata
    if 'schulart' not in df.columns and 'schulform_name' in df.columns:
        df['schulart'] = df['schulform_name']
    df['metadata_source'] = 'jedeschule.codefor.de + Stuttgart Schulwegweiser PDF'
    df['leistungsdaten_quelle'] = 'N/A'

    if 'traegerschaft' in df.columns:
        df['tuition_display'] = df['traegerschaft'].apply(
            lambda x: 'Kostenfrei (öffentliche Schule)' if pd.notna(x) and 'öffentlich' in str(x).lower()
            else 'Privat (Details auf Schulwebsite)' if pd.notna(x) and 'privat' in str(x).lower()
            else None
        )

    # Build output
    output = pd.DataFrame(index=range(len(df)))
    populated = added_null = 0
    for col in berlin_columns:
        if col in df.columns:
            output[col] = df[col].values
            populated += 1
        else:
            output[col] = None
            added_null += 1

    extras = sorted(set(df.columns) - set(berlin_columns))
    for col in extras:
        output[col] = df[col].values

    print(f"  Berlin from data: {populated}/{len(berlin_columns)}")
    print(f"  Berlin as NULL: {added_null}/{len(berlin_columns)}")
    print(f"  Stuttgart extras: {len(extras)}")

    # Verify schema column order
    assert list(output.columns)[:len(berlin_columns)] == berlin_columns, "SCHEMA MISMATCH!"
    print(f"  PASS: schema verified")

    # Save — do this BEFORE the optional validate_school_types import so a
    # missing scripts_shared path cannot swallow the output.
    out_pq = STG_DATA_DIR / f"stuttgart_{school_type}_school_master_table_berlin_schema.parquet"
    # Derive stable (year-agnostic) fields + vintage stamps — additive,
    # keeps all year-suffixed columns. See scripts_shared/schema/stable_fields.py.
    try:
        import sys as _sys
        _root = str(Path(__file__).resolve().parent.parent)
        if _root not in _sys.path:
            _sys.path.insert(0, _root)
        from scripts_shared.schema.stable_fields import add_stable_fields
        output = add_stable_fields(output)
    except Exception as _e:
        print(f"  WARN: stable-field derivation skipped: {_e}")
    output.to_parquet(out_pq, index=False)

    out_csv = STG_DATA_DIR / f"stuttgart_{school_type}_school_master_table_berlin_schema.csv"
    csv_out = output.drop(columns=['embedding'], errors='ignore')
    csv_out.to_csv(out_csv, index=False, encoding='utf-8-sig')

    # Also save as final
    final_pq = STG_DATA_DIR / f"stuttgart_{school_type}_school_master_table_final.csv"
    csv_out.to_csv(final_pq, index=False, encoding='utf-8-sig')

    print(f"  Saved: {out_pq.name}")

    # Guard: school_type must be a specific German type, never 'secondary'/'primary'
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        from scripts_shared.schema.core_schema import validate_school_types
        validate_school_types(output, city="Stuttgart", strict=True)
    except ImportError:
        pass
    print(f"{'='*70}")

    return output


def main():
    for st in ['secondary', 'primary']:
        try:
            transform_to_berlin_schema(st)
        except Exception as e:
            import traceback
            print(f"  FAILED for {st}: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
