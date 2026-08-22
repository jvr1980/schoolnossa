#!/usr/bin/env python3
"""
Phase 4: Munich Crime Data Enrichment

Enriches school data with crime statistics using city-level aggregate data
from the BKA PKS Stadt-Falltabellen and PP München Sicherheitsreport.

Munich publishes district-level crime data in PDF format only (Sicherheitsreport).
For consistency and reliability, this script uses city-level aggregates from the BKA
PKS, same approach as the Frankfurt pipeline. If district-level data becomes available
in machine-readable format, this script should be upgraded.

Data Sources:
- PKS 2025 / BKA Stadt-Falltabellen (city aggregate)
- PP München Sicherheitsreport 2024 (for context / validation)

Note: München is Germany's safest major city (50th consecutive year, 2024).

Input: data_munich/intermediate/munich_secondary_schools_with_transit.csv
       (fallback chain: with_traffic, base schools)
Output: data_munich/intermediate/munich_secondary_schools_with_crime.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

# Munich crime data (PKS 2025, BKA Stadt-Falltabellen T01, V1.1 2026-04-21)
# Source: https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/PKS2025/pksTabellen_Interpretationshilfen/StadtFalltabellen/stadtfalltabellen.html
# Category mapping from T01 keys: strassenraub = 216000 Handtaschenraub +
# 217000 sonstige Raubueberfaelle auf Strassen; koerperverletzung = 222000
# gefaehrliche/schwere + 224000 vorsaetzliche einfache KV; fahrrad = ***300.
# Population backed out of the official HZ (Zensus 2022 basis).
MUNICH_CRIME_DATA = {
    'population': 1_505_022,  # implied by HZ (Zensus 22 basis)
    'straftaten_2025': 91_219,
    'haeufigkeitszahl_2025': 6_061,  # per 100k — Germany's safest major city
    'aufklaerungsquote_2025': 65.5,
    'strassenraub_2025': 134,       # 216000 + 217000
    'wohnungseinbruch_2025': 815,   # 435*00
    'koerperverletzung_2025': 12_076,  # 222000 + 224000
    'diebstahl_fahrrad_2025': 5_671,   # ***300
}


def enrich_with_crime(schools_df: pd.DataFrame) -> pd.DataFrame:
    """Assign city-level crime data to all Munich schools."""
    logger.info("Enriching with city-level crime data (München PKS 2025)...")

    df = schools_df.copy()
    pop = MUNICH_CRIME_DATA['population']

    # City-level columns (Berlin schema compatible)
    df['crime_stadt'] = 'München'
    df['crime_bezirk'] = None  # No district-level data in machine-readable format
    df['crime_bezirk_population'] = pop
    df['crime_bezirk_index'] = 1.0  # City average
    df['crime_haeufigkeitszahl_2025'] = MUNICH_CRIME_DATA['haeufigkeitszahl_2025']
    df['crime_aufklaerungsquote_2025'] = MUNICH_CRIME_DATA['aufklaerungsquote_2025']

    # Crime categories (absolute city numbers)
    df['crime_straftaten_2025'] = MUNICH_CRIME_DATA['straftaten_2025']
    df['crime_strassenraub_2025'] = MUNICH_CRIME_DATA['strassenraub_2025']
    df['crime_koerperverletzung_2025'] = MUNICH_CRIME_DATA['koerperverletzung_2025']
    df['crime_diebstahl_fahrrad_2025'] = MUNICH_CRIME_DATA['diebstahl_fahrrad_2025']
    df['crime_wohnungseinbruch_2025'] = MUNICH_CRIME_DATA['wohnungseinbruch_2025']

    # Per-100k rates
    for key in ['straftaten', 'strassenraub', 'koerperverletzung', 'diebstahl_fahrrad', 'wohnungseinbruch']:
        total = MUNICH_CRIME_DATA.get(f'{key}_2025')
        if total:
            df[f'crime_{key}_2025_rate_per_100k'] = round(total / pop * 100_000, 1)

    # Safety category based on Häufigkeitszahl (HZ)
    hz = MUNICH_CRIME_DATA['haeufigkeitszahl_2025']
    if hz < 8000:
        safety_cat = 'Sehr sicher'
    elif hz < 10000:
        safety_cat = 'Sicher'
    elif hz < 12000:
        safety_cat = 'Durchschnittlich'
    else:
        safety_cat = 'Überdurchschnittlich'

    df['crime_safety_category'] = safety_cat
    df['crime_data_source'] = 'city_aggregate'
    df['crime_data_note'] = 'München: sicherste Großstadt Deutschlands (>200k Einwohner) seit 50 Jahren'

    logger.info(f"  All {len(df)} schools assigned city-level crime data")
    logger.info(f"  Häufigkeitszahl: {hz}/100k ('{safety_cat}')")
    logger.info(f"  Note: District-level data available in PDF only (Sicherheitsreport)")

    return df


def find_input_file(school_type='secondary'):
    candidates = [
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_transit.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_traffic.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools.csv",
    ]
    for f in candidates:
        if f.exists():
            return f
    raise FileNotFoundError(f"No {school_type} school data found. Run earlier phases first.")


def enrich_schools(school_type='secondary'):
    logger.info(f"Enriching {school_type} schools with crime data...")

    input_file = find_input_file(school_type)
    logger.info(f"Input: {input_file}")
    schools = pd.read_csv(input_file, dtype=str)
    logger.info(f"Loaded {len(schools)} schools")

    schools = enrich_with_crime(schools)

    output_path = INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_crime.csv"
    schools.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path}")

    print(f"\nCrime enrichment ({school_type}): {len(schools)} schools (city-level data)")
    print(f"Häufigkeitszahl: {MUNICH_CRIME_DATA['haeufigkeitszahl_2025']}/100k")
    print(f"Aufklärungsquote: {MUNICH_CRIME_DATA['aufklaerungsquote_2025']}%")

    return schools


def main(school_type='secondary'):
    logger.info("=" * 60)
    logger.info(f"Phase 4: Munich Crime Enrichment ({school_type})")
    logger.info("=" * 60)
    return enrich_schools(school_type)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
