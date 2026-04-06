#!/usr/bin/env python3
"""
Frankfurt School Data Combiner
Combines all enrichment data into a unified master table.

Source: Phase 1 Schulwegweiser primary scraper (raw CSVs) enriched by
        traffic / transit / crime / POI phases.

Author: Frankfurt School Data Pipeline
Created: 2026-03-30 | Rebuilt 2026-04-06 (Schulwegweiser as primary source)
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_frankfurt"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"


def find_most_enriched_file(school_type):
    """Find the most enriched intermediate file (fallback chain)."""
    candidates = [
        INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_pois.csv",
        INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_crime.csv",
        INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_transit.csv",
        INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_traffic.csv",
        RAW_DIR / f"frankfurt_{school_type}_schools.csv",
    ]
    for fp in candidates:
        if fp.exists():
            df = pd.read_csv(fp)
            logger.info(f"Loaded {len(df)} {school_type} schools from {fp.name} ({len(df.columns)} cols)")
            return df
    raise FileNotFoundError(f"No {school_type} school data found")


def standardize_columns(df):
    """Standardize column order — Schulwegweiser-rich schema."""
    preferred = [
        # Identity
        'schulnummer', 'schulname', 'school_type', 'schulkategorie', 'schulform_raw',
        # Location
        'strasse', 'plz', 'ort', 'ortsteil', 'stadt', 'bundesland',
        'latitude', 'longitude',
        # Contact (from Schulwegweiser)
        'telefon', 'fax', 'email', 'website',
        # Administrative
        'traegerschaft', 'traeger_name', 'form_der_privatschule',
        # Staff
        'schulleitung', 'stv_schulleitung',
        # Statistics
        'schueler_gesamt', 'klassenzahl', 'klassenstufe', 'unterrichtszeit',
        # Legacy ndH (from optional Verz6 join)
        'ndh_count',
        # Academic profile
        'profile', 'sonstige_profile', 'foerderschwerpunkt',
        'unterrichtssprache',
        'fruehe_fremdsprache',
        'erste_fremdsprache', 'zweite_fremdsprache', 'dritte_fremdsprache',
        'sprachen',
        # Day-care / all-day
        'art_des_angebots', 'ganztagsform',
        # Offerings & enrichment
        'besonderheiten', 'besonderheiten_erlaeuterung',
        'auszeichnungen', 'foerderverein', 'schulbibliothek',
        # Vocational fields
        'berufsbereiche', 'ausbildungsberufe', 'schulform_bemerkung',
        # School name history
        'namensgebung',
        # Transit enrichment
        'transit_stops_500m', 'transit_stop_count_1000m',
        'transit_rail_01_name', 'transit_rail_01_distance_m',
        'transit_accessibility_score',
        # Traffic enrichment
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'traffic_accidents_fatal', 'traffic_accidents_serious', 'traffic_accidents_minor',
        'traffic_accidents_bicycle', 'traffic_accidents_pedestrian',
        'traffic_accidents_school_hours', 'traffic_nearest_accident_m',
        # Crime enrichment
        'crime_stadt', 'crime_bezirk',
        'crime_straftaten_2023', 'crime_haeufigkeitszahl_2023',
        'crime_aufklaerungsquote_2023', 'crime_bezirk_index',
        # Portal meta
        'sw_portal_url', 'sw_portal_slug',
        'data_source', 'data_retrieved',
    ]
    ordered = [c for c in preferred if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    return df[ordered + remaining]


def clean_data(df):
    """Clean and validate."""
    df = df.copy()

    if 'schulnummer' in df.columns:
        orig = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < orig:
            logger.info(f"Removed {orig - len(df)} duplicates")

    for col in ['latitude', 'longitude', 'transit_accessibility_score',
                'traffic_accidents_total', 'crime_haeufigkeitszahl_2023',
                'schueler_gesamt', 'klassenzahl']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.zfill(5).replace('nan', None)

    return df


def combine_school_type(school_type):
    df = find_most_enriched_file(school_type)
    df = clean_data(df)
    df = standardize_columns(df)

    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = FINAL_DIR / f"frankfurt_{school_type}_school_master_table.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    parquet_path = FINAL_DIR / f"frankfurt_{school_type}_school_master_table.parquet"
    df.to_parquet(parquet_path, index=False)

    logger.info(f"Saved: {csv_path} and {parquet_path}")

    print(f"\n{'='*70}\nFRANKFURT DATA COMBINER ({school_type.upper()}) - COMPLETE\n{'='*70}")
    print(f"Schools: {len(df)}, Columns: {len(df.columns)}")
    if 'school_type' in df.columns:
        for t, c in df['school_type'].value_counts().items():
            print(f"  {t}: {c}")
    key_cols = {
        'latitude':                  'Coordinates',
        'website':                   'Website',
        'email':                     'Email',
        'ganztagsform':              'Ganztagsform',
        'profile':                   'Profile',
        'besonderheiten':            'Besonderheiten',
        'auszeichnungen':            'Auszeichnungen',
        'transit_accessibility_score': 'Transit',
        'traffic_accidents_total':   'Traffic',
        'crime_haeufigkeitszahl_2023': 'Crime',
    }
    for col, label in key_cols.items():
        if col in df.columns:
            n = df[col].notna().sum()
            print(f"  {label}: {n}/{len(df)} ({100*n/len(df):.0f}%)")
    print(f"{'='*70}")
    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Frankfurt Data Combiner")
    logger.info("=" * 60)
    for st in ['secondary', 'primary']:
        try:
            combine_school_type(st)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
