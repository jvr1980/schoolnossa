#!/usr/bin/env python3
"""
NRW School Additions: Cross-Grade Duplication & Missing Schools

This script handles two issues with the NRW Schulministerium open data:

1. CROSS-GRADE DUPLICATION: Waldorfschulen (Schulform 17) span grades 1-13
   but only appear in the secondary dataset. This script copies them into the
   primary dataset so families searching for primary schools find them too.

2. MISSING SCHOOLS: Several private/international schools are not registered
   in the Schulministerium data at all (foreign-regulated, diplomatic, etc.).
   This script adds them manually with geocoded addresses and website info.

Added to primary:
  - Rudolf-Steiner-Schule Düsseldorf (Waldorf cross-grade copy)
  - Freie Waldorfschule Köln (Waldorf cross-grade copy)
  - Michaeli Schule Köln (Waldorf cross-grade copy)
  - International School of Düsseldorf (new)
  - Lycée français international Simone Veil Düsseldorf (new)
  - Internationale Friedensschule Köln / Cologne International School (new)

Added to secondary:
  - International School of Düsseldorf (new)
  - Lycée français international Simone Veil Düsseldorf (new)
  - Internationale Friedensschule Köln / Cologne International School (new)

Run as Phase 1b in the orchestrator (after scrape, before enrichment).

Author: NRW School Data Pipeline
Created: 2026-02-25
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"

# Waldorf schools in secondary that need cross-grade duplication into primary
WALDORF_CROSS_GRADE = [
    187410,  # Rudolf-Steiner-Schule Düsseldorf
    187604,  # Freie Waldorfschule Köln
    194402,  # Michaeli Schule Freie Waldorfschule Köln
]

# Schools missing entirely from Schulministerium data
# These are international/foreign-regulated schools not registered with NRW ministry
MISSING_SCHOOLS = [
    {
        'schulnummer': '990001',
        'schulname': 'International School of Düsseldorf',
        'schulform_name': 'Internationale Schule',
        'traegerschaft': 'Privat',
        'strasse': 'Niederrheinstraße 336',
        'plz': '40489',
        'ort': 'Düsseldorf',
        'stadt': 'Düsseldorf',
        'latitude': 51.2921056,
        'longitude': 6.7357351,
        'email': 'info@isdedu.de',
        'website': 'https://www.isdedu.de',
        'telefon': '02114440',
        'bundesland': 'Nordrhein-Westfalen',
        'data_source': 'Manual addition - not in NRW Schulministerium data',
        'data_retrieved': datetime.now().strftime('%Y-%m-%d'),
        'sozialindexstufe': None,
        # ISD covers K-12; primary campus at Niederrheinstraße 323, secondary at 336
        # For primary, we use the primary campus address below
        'datasets': ['primary', 'secondary'],
        'primary_override': {
            'strasse': 'Niederrheinstraße 323',
            'latitude': 51.2909773,
            'longitude': 6.7375021,
        },
    },
    {
        'schulnummer': '990002',
        'schulname': 'Lycée français international Simone Veil',
        'schulform_name': 'Internationale Schule',
        'traegerschaft': 'Privat',
        'strasse': 'Graf-Recke-Straße 220',
        'plz': '40237',
        'ort': 'Düsseldorf',
        'stadt': 'Düsseldorf',
        'latitude': 51.2457676,
        'longitude': 6.8199731,
        'email': 'secretariat@lfisv.de',
        'website': 'https://lfisv.de',
        'telefon': '02116107950',
        'bundesland': 'Nordrhein-Westfalen',
        'data_source': 'Manual addition - not in NRW Schulministerium data',
        'data_retrieved': datetime.now().strftime('%Y-%m-%d'),
        'sozialindexstufe': None,
        # Lycée covers Maternelle through Terminale (grades 1-12)
        'datasets': ['primary', 'secondary'],
    },
    {
        'schulnummer': '990003',
        'schulname': 'Internationale Friedensschule Köln (Cologne International School)',
        'schulform_name': 'Internationale Schule',
        'traegerschaft': 'Privat',
        'strasse': 'Neue Sandkaul 29',
        'plz': '50859',
        'ort': 'Köln',
        'stadt': 'Köln',
        'latitude': 50.9629008,
        'longitude': 6.8387925,
        'email': 'info@if-koeln.de',
        'website': 'https://if-koeln.de',
        'telefon': '0221310634219',
        'bundesland': 'Nordrhein-Westfalen',
        'data_source': 'Manual addition - not in NRW Schulministerium data',
        'data_retrieved': datetime.now().strftime('%Y-%m-%d'),
        'sozialindexstufe': None,
        # IB World School covering PYP through DP (grades 1-12)
        'datasets': ['primary', 'secondary'],
    },
]


def add_cross_grade_schools(primary_df: pd.DataFrame, secondary_df: pd.DataFrame) -> pd.DataFrame:
    """Copy Waldorf schools from secondary into primary dataset."""
    logger.info("Adding cross-grade Waldorf schools to primary dataset...")

    added = 0
    existing_snrs = set(primary_df['schulnummer'].astype(str).values)

    for snr in WALDORF_CROSS_GRADE:
        snr_str = str(snr)
        if snr_str in existing_snrs:
            logger.info(f"  Schulnummer {snr} already in primary dataset, skipping")
            continue

        # Find in secondary
        match = secondary_df[secondary_df['schulnummer'].astype(str) == snr_str]
        if match.empty:
            logger.warning(f"  Schulnummer {snr} not found in secondary dataset!")
            continue

        row = match.iloc[0].copy()
        school_name = row.get('schulname', 'Unknown')

        # Mark as primary school while preserving Waldorf identity
        row['school_type'] = 'Grundschule'
        row['is_cross_grade'] = True
        row['cross_grade_note'] = f'Waldorfschule grades 1-13, also in secondary dataset'

        primary_df = pd.concat([primary_df, pd.DataFrame([row])], ignore_index=True)
        added += 1
        logger.info(f"  Added: {school_name} (SNR {snr}) -> primary dataset")

    logger.info(f"Cross-grade duplication complete: {added} schools added to primary")
    return primary_df


def add_missing_schools(primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    """Add schools that are missing from Schulministerium data entirely."""
    logger.info("Adding missing international/private schools...")

    existing_pri_snrs = set(primary_df['schulnummer'].astype(str).values)
    existing_sec_snrs = set(secondary_df['schulnummer'].astype(str).values)

    primary_added = 0
    secondary_added = 0

    for school_def in MISSING_SCHOOLS:
        datasets = school_def.pop('datasets', ['primary', 'secondary'])
        primary_override = school_def.pop('primary_override', {})

        # Build base row with all standard columns set to NaN
        # (enrichment phases will fill in transit, crime, POI, etc.)
        base_row = {}
        for col in primary_df.columns:
            base_row[col] = None

        # Fill in known data
        for key, val in school_def.items():
            base_row[key] = val

        base_row['is_cross_grade'] = True if len(datasets) > 1 else False
        base_row['cross_grade_note'] = 'International school spanning primary and secondary' if len(datasets) > 1 else None

        # Add to secondary
        if 'secondary' in datasets:
            snr_str = str(school_def['schulnummer'])
            if snr_str not in existing_sec_snrs:
                sec_row = base_row.copy()
                sec_row['school_type'] = school_def['schulform_name']
                # Ensure all secondary columns exist
                for col in secondary_df.columns:
                    if col not in sec_row:
                        sec_row[col] = None
                secondary_df = pd.concat([secondary_df, pd.DataFrame([sec_row])], ignore_index=True)
                secondary_added += 1
                logger.info(f"  Added to secondary: {school_def['schulname']} (SNR {snr_str})")
            else:
                logger.info(f"  SNR {snr_str} already in secondary, skipping")

        # Add to primary (with optional address override for separate campus)
        if 'primary' in datasets:
            snr_str = str(school_def['schulnummer'])
            if snr_str not in existing_pri_snrs:
                pri_row = base_row.copy()
                pri_row['school_type'] = 'Grundschule'
                # Apply primary-specific overrides (e.g., different campus address)
                for key, val in primary_override.items():
                    pri_row[key] = val
                # Ensure all primary columns exist
                for col in primary_df.columns:
                    if col not in pri_row:
                        pri_row[col] = None
                primary_df = pd.concat([primary_df, pd.DataFrame([pri_row])], ignore_index=True)
                primary_added += 1
                logger.info(f"  Added to primary: {school_def['schulname']} (SNR {snr_str})")
            else:
                logger.info(f"  SNR {snr_str} already in primary, skipping")

        # Restore popped keys for potential re-runs
        school_def['datasets'] = datasets
        school_def['primary_override'] = primary_override

    logger.info(f"Missing schools added: {primary_added} primary, {secondary_added} secondary")
    return primary_df, secondary_df


def add_schools(school_type: str = 'both'):
    """
    Main entry point. Adds cross-grade and missing schools to raw CSV files.

    Args:
        school_type: 'primary', 'secondary', or 'both' (default)
    """
    logger.info("=" * 60)
    logger.info("NRW SCHOOL ADDITIONS: Cross-Grade & Missing Schools")
    logger.info("=" * 60)

    # Load raw data
    primary_path = RAW_DIR / "nrw_primary_schools.csv"
    secondary_path = RAW_DIR / "nrw_secondary_schools.csv"

    if not primary_path.exists() or not secondary_path.exists():
        logger.error("Raw school data not found. Run Phase 1 (scraper) first.")
        return None

    primary_df = pd.read_csv(primary_path)
    secondary_df = pd.read_csv(secondary_path)

    logger.info(f"Loaded: {len(primary_df)} primary, {len(secondary_df)} secondary schools")

    orig_primary_count = len(primary_df)
    orig_secondary_count = len(secondary_df)

    # Step 1: Cross-grade duplication (Waldorf schools → primary)
    if school_type in ('primary', 'both'):
        primary_df = add_cross_grade_schools(primary_df, secondary_df)

    # Step 2: Add missing schools (international schools not in Schulministerium)
    primary_df, secondary_df = add_missing_schools(primary_df, secondary_df)

    # Save updated raw files
    primary_df.to_csv(primary_path, index=False, encoding='utf-8-sig')
    secondary_df.to_csv(secondary_path, index=False, encoding='utf-8-sig')

    primary_added = len(primary_df) - orig_primary_count
    secondary_added = len(secondary_df) - orig_secondary_count

    # Summary
    print("\n" + "=" * 60)
    print("SCHOOL ADDITIONS COMPLETE")
    print("=" * 60)
    print(f"\nPrimary:   {orig_primary_count} -> {len(primary_df)} (+{primary_added})")
    print(f"Secondary: {orig_secondary_count} -> {len(secondary_df)} (+{secondary_added})")

    if primary_added > 0:
        print(f"\nPrimary additions:")
        new_pri = primary_df.tail(primary_added)
        for _, row in new_pri.iterrows():
            print(f"  - {row['schulname']} ({row['stadt']}) [{row.get('schulform_name', '?')}]")

    if secondary_added > 0:
        print(f"\nSecondary additions:")
        new_sec = secondary_df.tail(secondary_added)
        for _, row in new_sec.iterrows():
            print(f"  - {row['schulname']} ({row['stadt']}) [{row.get('schulform_name', '?')}]")

    print("=" * 60)
    return primary_df, secondary_df


if __name__ == "__main__":
    add_schools('both')
