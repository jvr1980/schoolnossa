#!/usr/bin/env python3
"""
Frankfurt Crime Data Enrichment
Enriches school data with crime statistics.

Frankfurt does NOT publish neighborhood-level crime data as open data.
Only city-level aggregate data is available from the BKA PKS.

This script assigns uniform city-level crime metrics to all Frankfurt schools,
documenting this as a known limitation vs Berlin/Hamburg.

Data Sources:
- PKS 2023 / BKA Stadt-Falltabellen
- Polizeipräsidium Frankfurt PKS (city aggregate)

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
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

# Frankfurt crime data (PKS 2023, BKA Stadt-Falltabellen)
# Source: https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/PKS2024/PKSTabellen/StadtFalltabellen/
FRANKFURT_CRIME_DATA = {
    'population': 773_068,  # Einwohner 2023
    'straftaten_2023': 114_729,
    'haeufigkeitszahl_2023': 14_840,  # per 100k
    'aufklaerungsquote_2023': 51.2,
    'strassenraub_2023': 1_134,
    'wohnungseinbruch_2023': 1_876,
    'koerperverletzung_2023': 8_421,
    'diebstahl_fahrrad_2023': 5_632,
}


def enrich_with_crime(schools_df: pd.DataFrame) -> pd.DataFrame:
    """Assign city-level crime data to all Frankfurt schools."""
    logger.info("Enriching with city-level crime data (Frankfurt PKS 2023)...")

    df = schools_df.copy()
    pop = FRANKFURT_CRIME_DATA['population']

    # City-level columns
    df['crime_stadt'] = 'Frankfurt am Main'
    df['crime_bezirk'] = None  # No neighborhood-level data available
    df['crime_bezirk_population'] = pop
    df['crime_bezirk_index'] = 1.0  # City average
    df['crime_haeufigkeitszahl_2023'] = FRANKFURT_CRIME_DATA['haeufigkeitszahl_2023']
    df['crime_aufklaerungsquote_2023'] = FRANKFURT_CRIME_DATA['aufklaerungsquote_2023']

    # Crime categories (absolute city numbers)
    df['crime_straftaten_2023'] = FRANKFURT_CRIME_DATA['straftaten_2023']
    df['crime_strassenraub_2023'] = FRANKFURT_CRIME_DATA['strassenraub_2023']
    df['crime_koerperverletzung_2023'] = FRANKFURT_CRIME_DATA['koerperverletzung_2023']
    df['crime_diebstahl_fahrrad_2023'] = FRANKFURT_CRIME_DATA['diebstahl_fahrrad_2023']
    df['crime_wohnungseinbruch_2023'] = FRANKFURT_CRIME_DATA['wohnungseinbruch_2023']

    # Rates per 100k
    for key in ['straftaten', 'strassenraub', 'koerperverletzung', 'diebstahl_fahrrad', 'wohnungseinbruch']:
        total = FRANKFURT_CRIME_DATA.get(f'{key}_2023')
        if total:
            df[f'crime_{key}_2023_rate_per_100k'] = round(total / pop * 100_000, 1)

    df['crime_data_source'] = 'city_aggregate'

    logger.info(f"  All {len(df)} schools assigned city-level crime data")
    logger.info(f"  Häufigkeitszahl: {FRANKFURT_CRIME_DATA['haeufigkeitszahl_2023']}/100k")
    logger.info(f"  Note: No Stadtteil-level crime data available for Frankfurt")

    return df


def enrich_schools(school_type="secondary"):
    """Run crime enrichment."""
    input_file = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_transit.csv"
    if not input_file.exists():
        input_file = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_traffic.csv"
    if not input_file.exists():
        input_file = RAW_DIR / f"frankfurt_{school_type}_schools.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Not found for {school_type}")

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} {school_type} schools from {input_file.name}")

    enriched = enrich_with_crime(df)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_crime.csv"
    enriched.to_csv(out, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out}")

    print(f"\n{'='*70}\nFRANKFURT CRIME ENRICHMENT ({school_type.upper()}) - COMPLETE\n{'='*70}")
    print(f"Schools: {len(enriched)}")
    print(f"Data level: City aggregate (no Stadtteil data available)")
    print(f"Häufigkeitszahl: {FRANKFURT_CRIME_DATA['haeufigkeitszahl_2023']}/100k")
    print(f"{'='*70}")
    return enriched


def main():
    logger.info("=" * 60)
    logger.info("Starting Frankfurt Crime Enrichment (City-level PKS)")
    logger.info("=" * 60)
    for st in ['secondary', 'primary']:
        for d in [INTERMEDIATE_DIR, RAW_DIR]:
            for pat in [f"frankfurt_{st}_schools_with_transit.csv",
                        f"frankfurt_{st}_schools_with_traffic.csv",
                        f"frankfurt_{st}_schools.csv"]:
                if (d / pat).exists():
                    enrich_schools(st)
                    break
            else:
                continue
            break
        else:
            logger.warning(f"No {st} school data found")


if __name__ == "__main__":
    main()
