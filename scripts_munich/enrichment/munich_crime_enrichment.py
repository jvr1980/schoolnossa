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
- PKS 2023 / BKA Stadt-Falltabellen (city aggregate)
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

# Munich crime data (PKS 2023, BKA Stadt-Falltabellen)
# Source: https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/PKS2024/PKSTabellen/StadtFalltabellen/
# Munich is Germany's safest major city (>200k inhabitants) for the 50th time
MUNICH_CRIME_DATA = {
    'population': 1_512_491,  # Einwohner 2023
    'straftaten_2023': 116_195,
    'haeufigkeitszahl_2023': 7_684,  # per 100k — much lower than Frankfurt (14,840)
    'aufklaerungsquote_2023': 62.9,
    'strassenraub_2023': 574,
    'wohnungseinbruch_2023': 952,
    'koerperverletzung_2023': 10_127,
    'diebstahl_fahrrad_2023': 7_456,
}


def enrich_with_crime(schools_df: pd.DataFrame) -> pd.DataFrame:
    """Assign city-level crime data to all Munich schools."""
    logger.info("Enriching with city-level crime data (München PKS 2023)...")

    df = schools_df.copy()
    pop = MUNICH_CRIME_DATA['population']

    # City-level columns (Berlin schema compatible)
    df['crime_stadt'] = 'München'
    df['crime_bezirk'] = None  # No district-level data in machine-readable format
    df['crime_bezirk_population'] = pop
    df['crime_bezirk_index'] = 1.0  # City average
    df['crime_haeufigkeitszahl_2023'] = MUNICH_CRIME_DATA['haeufigkeitszahl_2023']
    df['crime_aufklaerungsquote_2023'] = MUNICH_CRIME_DATA['aufklaerungsquote_2023']

    # Crime categories (absolute city numbers)
    df['crime_straftaten_2023'] = MUNICH_CRIME_DATA['straftaten_2023']
    df['crime_strassenraub_2023'] = MUNICH_CRIME_DATA['strassenraub_2023']
    df['crime_koerperverletzung_2023'] = MUNICH_CRIME_DATA['koerperverletzung_2023']
    df['crime_diebstahl_fahrrad_2023'] = MUNICH_CRIME_DATA['diebstahl_fahrrad_2023']
    df['crime_wohnungseinbruch_2023'] = MUNICH_CRIME_DATA['wohnungseinbruch_2023']

    # Per-100k rates
    for key in ['straftaten', 'strassenraub', 'koerperverletzung', 'diebstahl_fahrrad', 'wohnungseinbruch']:
        total = MUNICH_CRIME_DATA.get(f'{key}_2023')
        if total:
            df[f'crime_{key}_2023_rate_per_100k'] = round(total / pop * 100_000, 1)

    # Safety category based on Häufigkeitszahl (HZ)
    hz = MUNICH_CRIME_DATA['haeufigkeitszahl_2023']
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


def find_input_file():
    candidates = [
        INTERMEDIATE_DIR / "munich_secondary_schools_with_transit.csv",
        INTERMEDIATE_DIR / "munich_secondary_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "munich_secondary_schools.csv",
    ]
    for f in candidates:
        if f.exists():
            return f
    raise FileNotFoundError("No school data found. Run earlier phases first.")


def main():
    logger.info("=" * 60)
    logger.info("Phase 4: Munich Crime Enrichment (City-Level PKS)")
    logger.info("=" * 60)

    input_file = find_input_file()
    logger.info(f"Input: {input_file}")
    schools = pd.read_csv(input_file, dtype=str)
    logger.info(f"Loaded {len(schools)} schools")

    schools = enrich_with_crime(schools)

    output_path = INTERMEDIATE_DIR / "munich_secondary_schools_with_crime.csv"
    schools.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path}")

    print(f"\nCrime enrichment: {len(schools)} schools (city-level data)")
    print(f"Häufigkeitszahl: {MUNICH_CRIME_DATA['haeufigkeitszahl_2023']}/100k")
    print(f"Aufklärungsquote: {MUNICH_CRIME_DATA['aufklaerungsquote_2023']}%")

    return schools


if __name__ == "__main__":
    main()
