#!/usr/bin/env python3
"""
Stuttgart Crime Data Enrichment
Enriches school data with crime statistics from PKS Stuttgart 2023.

Stuttgart publishes city-level PKS data. We use Stadtbezirk population data
to estimate district-level crime numbers.

Data Sources:
- PKS Stuttgart 2023 (Polizeiliche Kriminalstatistik)
- Stuttgart population by Stadtbezirk (statistik.stuttgart.de)

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

# Stuttgart city-level crime data (PKS 2023)
# Source: polizei-bw.de / presseportal
STUTTGART_CRIME_DATA = {
    'population': 632_865,
    'straftaten_2023': 68_457,
    'haeufigkeitszahl_2023': 10_817,  # per 100k
    'aufklaerungsquote_2023': 52.3,
    'strassenraub_2023': 489,
    'wohnungseinbruch_2023': 1_210,
    'koerperverletzung_2023': 6_842,
    'diebstahl_fahrrad_2023': 3_520,
}

# Stuttgart Stadtbezirke with estimated crime indices and population
# Crime indices estimated from city reports (relative to city average = 1.0)
STUTTGART_BEZIRKE = {
    'Mitte':          {'crime_index': 2.5, 'einwohner': 24_180},
    'Nord':           {'crime_index': 1.3, 'einwohner': 28_830},
    'Ost':            {'crime_index': 1.1, 'einwohner': 49_220},
    'Süd':            {'crime_index': 1.0, 'einwohner': 44_770},
    'West':           {'crime_index': 1.2, 'einwohner': 53_560},
    'Bad Cannstatt':  {'crime_index': 1.4, 'einwohner': 71_510},
    'Birkach':        {'crime_index': 0.5, 'einwohner':  7_090},
    'Botnang':        {'crime_index': 0.6, 'einwohner': 14_480},
    'Degerloch':      {'crime_index': 0.6, 'einwohner': 17_980},
    'Feuerbach':      {'crime_index': 1.0, 'einwohner': 30_470},
    'Hedelfingen':    {'crime_index': 0.7, 'einwohner': 10_350},
    'Möhringen':      {'crime_index': 0.7, 'einwohner': 32_500},
    'Mühlhausen':     {'crime_index': 0.8, 'einwohner': 25_880},
    'Münster':        {'crime_index': 0.6, 'einwohner':  7_320},
    'Obertürkheim':   {'crime_index': 0.7, 'einwohner': 10_520},
    'Plieningen':     {'crime_index': 0.5, 'einwohner': 14_070},
    'Sillenbuch':     {'crime_index': 0.5, 'einwohner': 25_240},
    'Stammheim':      {'crime_index': 0.8, 'einwohner': 12_620},
    'Untertürkheim':  {'crime_index': 0.9, 'einwohner': 17_030},
    'Vaihingen':      {'crime_index': 0.7, 'einwohner': 44_850},
    'Wangen':         {'crime_index': 0.6, 'einwohner':  9_060},
    'Weilimdorf':     {'crime_index': 0.7, 'einwohner': 33_290},
    'Zuffenhausen':   {'crime_index': 1.1, 'einwohner': 38_180},
}

# PLZ → Bezirk mapping for Stuttgart
STUTTGART_PLZ_BEZIRK = {
    '70173': 'Mitte', '70174': 'Mitte', '70178': 'Süd', '70176': 'West',
    '70180': 'Süd', '70182': 'Ost', '70184': 'Ost', '70186': 'Ost',
    '70188': 'Ost', '70190': 'Nord', '70191': 'Nord', '70192': 'Nord',
    '70193': 'West', '70195': 'Botnang', '70197': 'West', '70199': 'Süd',
    '70327': 'Untertürkheim', '70329': 'Obertürkheim',
    '70372': 'Bad Cannstatt', '70374': 'Bad Cannstatt', '70376': 'Bad Cannstatt',
    '70378': 'Mühlhausen', '70435': 'Zuffenhausen', '70437': 'Zuffenhausen',
    '70439': 'Stammheim', '70469': 'Feuerbach', '70499': 'Weilimdorf',
    '70563': 'Vaihingen', '70565': 'Vaihingen', '70567': 'Möhringen',
    '70569': 'Vaihingen', '70597': 'Degerloch', '70599': 'Plieningen',
    '70619': 'Sillenbuch', '70629': 'Birkach',
}

CRIME_CATEGORIES = {
    'straftaten_2023': 'crime_straftaten_2023',
    'strassenraub_2023': 'crime_strassenraub_2023',
    'koerperverletzung_2023': 'crime_koerperverletzung_2023',
    'diebstahl_fahrrad_2023': 'crime_diebstahl_fahrrad_2023',
    'wohnungseinbruch_2023': 'crime_wohnungseinbruch_2023',
}


def compute_bezirk_estimates(bezirk_data):
    city = STUTTGART_CRIME_DATA
    city_pop = city['population']
    idx = bezirk_data['crime_index']
    pop = bezirk_data['einwohner']

    estimates = {'crime_bezirk_population': pop, 'crime_bezirk_index': idx}
    for city_key, col in CRIME_CATEGORIES.items():
        total = city.get(city_key)
        if total:
            rate = total / city_pop * 100_000 * idx
            estimated = rate * pop / 100_000
            estimates[col] = round(estimated)
            estimates[f'{col}_rate_per_100k'] = round(rate, 1)
        else:
            estimates[col] = None
            estimates[f'{col}_rate_per_100k'] = None
    return estimates


def enrich_with_crime(schools_df):
    df = schools_df.copy()

    # Initialize columns
    for col in ['crime_bezirk', 'crime_stadt', 'crime_bezirk_population',
                'crime_bezirk_index', 'crime_haeufigkeitszahl_2023',
                'crime_aufklaerungsquote_2023', 'crime_data_source']:
        df[col] = None
    for _, col in CRIME_CATEGORIES.items():
        df[col] = None
        df[f'{col}_rate_per_100k'] = None

    matched = 0
    for idx, row in df.iterrows():
        plz = str(row.get('plz', '')).strip()
        bezirk = STUTTGART_PLZ_BEZIRK.get(plz)

        # Try ortsteil if PLZ not mapped
        if not bezirk:
            ortsteil = str(row.get('ortsteil', '')).strip()
            if ortsteil in STUTTGART_BEZIRKE:
                bezirk = ortsteil

        df.at[idx, 'crime_stadt'] = 'Stuttgart'
        df.at[idx, 'crime_haeufigkeitszahl_2023'] = STUTTGART_CRIME_DATA['haeufigkeitszahl_2023']
        df.at[idx, 'crime_aufklaerungsquote_2023'] = STUTTGART_CRIME_DATA['aufklaerungsquote_2023']

        if bezirk and bezirk in STUTTGART_BEZIRKE:
            df.at[idx, 'crime_bezirk'] = bezirk
            estimates = compute_bezirk_estimates(STUTTGART_BEZIRKE[bezirk])
            for col, val in estimates.items():
                df.at[idx, col] = val
            df.at[idx, 'crime_haeufigkeitszahl_2023'] = round(
                STUTTGART_CRIME_DATA['haeufigkeitszahl_2023'] * STUTTGART_BEZIRKE[bezirk]['crime_index'], 1
            )
            df.at[idx, 'crime_data_source'] = 'bezirk_estimated'
            matched += 1
        else:
            # City-level fallback
            city_pop = STUTTGART_CRIME_DATA['population']
            for city_key, col in CRIME_CATEGORIES.items():
                total = STUTTGART_CRIME_DATA.get(city_key)
                if total:
                    df.at[idx, col] = total
                    df.at[idx, f'{col}_rate_per_100k'] = round(total / city_pop * 100_000, 1)
            df.at[idx, 'crime_bezirk_population'] = city_pop
            df.at[idx, 'crime_bezirk_index'] = 1.0
            df.at[idx, 'crime_data_source'] = 'city_aggregate'

    logger.info(f"Bezirk-level: {matched}/{len(df)}, city fallback: {len(df)-matched}/{len(df)}")
    return df


def enrich_schools(school_type='secondary'):
    # Input chain
    for d in [INTERMEDIATE_DIR, RAW_DIR]:
        for p in [f"stuttgart_{school_type}_schools_with_transit.csv",
                  f"stuttgart_{school_type}_schools_with_traffic.csv",
                  f"stuttgart_{school_type}_schools.csv"]:
            f = d / p
            if f.exists():
                schools_df = pd.read_csv(f)
                logger.info(f"Loaded {len(schools_df)} from {f.name}")
                break
        else:
            continue
        break
    else:
        raise FileNotFoundError(f"No {school_type} data found")

    enriched = enrich_with_crime(schools_df)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_crime.csv"
    enriched.to_csv(out, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out}")

    print(f"\n{'='*70}")
    print(f"STUTTGART CRIME ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(enriched)}")
    if 'crime_data_source' in enriched.columns:
        for src, cnt in enriched['crime_data_source'].value_counts().items():
            print(f"  {src}: {cnt}")
    print(f"{'='*70}")

    return enriched


def main():
    for st in ['primary', 'secondary']:
        try:
            enrich_schools(st)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
