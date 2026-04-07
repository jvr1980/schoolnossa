#!/usr/bin/env python3
"""
NRW Crime Data Enrichment
Enriches school data with crime statistics from NRW PKS (Polizeiliche Kriminalstatistik).

Uses Stadtbezirk-level population data (2024) to convert city-wide crime statistics
into estimated Bezirk-level absolute crime numbers, comparable to Berlin's Bezirk data.

Formula:
    city_category_rate_per_100k = city_category_total / city_population * 100,000
    bezirk_rate_per_100k = city_category_rate * bezirk_crime_index
    bezirk_estimated_crimes = bezirk_rate_per_100k * bezirk_population / 100,000

Data Sources:
- PKS NRW / PKS Köln / PKS Düsseldorf (2023 crime statistics)
- Stadt Köln population: citypopulation.de (Melderegister 2024-12-31)
- Stadt Düsseldorf population: citypopulation.de (Melderegister 2024-12-31)

Author: NRW School Data Pipeline
Created: 2026-02-15
Updated: 2026-02-20 — added population-based Bezirk-level crime estimates
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# ===========================================================================
# CITY-LEVEL CRIME DATA (PKS 2023)
# ===========================================================================
# Sources:
# - https://koeln.polizei.nrw/kriminalitaetsstatistik
# - https://duesseldorf.polizei.nrw/kriminalitaetsstatistik
# Häufigkeitszahl = cases per 100,000 residents

CITY_CRIME_DATA = {
    'Köln': {
        'population': 1_090_473,  # Melderegister 2024-12-31
        'straftaten_2023': 135_476,
        'haeufigkeitszahl_2023': 12_434,
        'aufklaerungsquote_2023': 50.8,
        'strassenraub_2023': 1_548,
        'wohnungseinbruch_2023': 2_890,
        'koerperverletzung_2023': 10_723,
        'diebstahl_fahrrad_2023': 8_937,
    },
    'Düsseldorf': {
        'population': 658_245,  # Melderegister 2024-12-31
        'straftaten_2023': 78_826,
        'haeufigkeitszahl_2023': 12_585,
        'aufklaerungsquote_2023': 52.1,
        'strassenraub_2023': 612,
        'wohnungseinbruch_2023': 1_520,
        'koerperverletzung_2023': 5_831,
        'diebstahl_fahrrad_2023': 4_210,
    },
}

# ===========================================================================
# STADTBEZIRK DATA: crime index + population (2024 Melderegister)
# ===========================================================================
# Crime indices are relative to city average (1.0), from PKS Bezirk reports.
# Population from citypopulation.de (Melderegister 2024-12-31)

KOELN_BEZIRKE = {
    'Innenstadt':   {'crime_index': 2.5, 'einwohner': 126_087},
    'Rodenkirchen': {'crime_index': 0.6, 'einwohner': 112_051},
    'Lindenthal':   {'crime_index': 0.7, 'einwohner': 152_644},
    'Ehrenfeld':    {'crime_index': 1.3, 'einwohner': 111_154},
    'Nippes':       {'crime_index': 1.1, 'einwohner': 117_086},
    'Chorweiler':   {'crime_index': 1.2, 'einwohner':  83_848},
    'Porz':         {'crime_index': 1.0, 'einwohner': 115_776},
    'Kalk':         {'crime_index': 1.4, 'einwohner': 121_957},
    'Mülheim':      {'crime_index': 1.2, 'einwohner': 149_870},
}

DUESSELDORF_BEZIRKE = {
    'Bezirk 1':  {'crime_index': 2.0, 'einwohner':  89_669, 'name': 'Altstadt/Carlstadt/Stadtmitte'},
    'Bezirk 2':  {'crime_index': 0.9, 'einwohner':  66_094, 'name': 'Flingern/Düsseltal'},
    'Bezirk 3':  {'crime_index': 1.2, 'einwohner': 123_572, 'name': 'Oberbilk/Friedrichstadt'},
    'Bezirk 4':  {'crime_index': 0.8, 'einwohner':  47_036, 'name': 'Oberkassel/Niederkassel'},
    'Bezirk 5':  {'crime_index': 0.7, 'einwohner':  34_808, 'name': 'Stockum/Lohausen'},
    'Bezirk 6':  {'crime_index': 0.8, 'einwohner':  69_197, 'name': 'Rath/Mörsenbroich'},
    'Bezirk 7':  {'crime_index': 0.7, 'einwohner':  47_652, 'name': 'Gerresheim/Grafenberg'},
    'Bezirk 8':  {'crime_index': 0.6, 'einwohner':  61_142, 'name': 'Eller/Lierenfeld'},
    'Bezirk 9':  {'crime_index': 0.9, 'einwohner':  94_463, 'name': 'Wersten/Holthausen'},
    'Bezirk 10': {'crime_index': 0.5, 'einwohner':  24_612, 'name': 'Garath/Hellerhof'},
}

# ===========================================================================
# PLZ → STADTBEZIRK MAPPINGS
# ===========================================================================

KOELN_PLZ_BEZIRK = {
    '50667': 'Innenstadt', '50668': 'Innenstadt', '50670': 'Innenstadt',
    '50672': 'Innenstadt', '50674': 'Innenstadt', '50676': 'Innenstadt',
    '50677': 'Innenstadt', '50678': 'Innenstadt',
    '50996': 'Rodenkirchen', '50997': 'Rodenkirchen', '50999': 'Rodenkirchen',
    '50968': 'Rodenkirchen', '50969': 'Rodenkirchen',
    '50931': 'Lindenthal', '50933': 'Lindenthal', '50935': 'Lindenthal',
    '50937': 'Lindenthal', '50939': 'Lindenthal',
    '50823': 'Ehrenfeld', '50825': 'Ehrenfeld', '50827': 'Ehrenfeld',
    '50829': 'Ehrenfeld',
    '50733': 'Nippes', '50735': 'Nippes', '50737': 'Nippes',
    '50739': 'Nippes',
    '50765': 'Chorweiler', '50767': 'Chorweiler', '50769': 'Chorweiler',
    '50771': 'Chorweiler',
    '51143': 'Porz', '51145': 'Porz', '51147': 'Porz', '51149': 'Porz',
    '51061': 'Mülheim', '51063': 'Mülheim', '51065': 'Mülheim',
    '51067': 'Mülheim', '51069': 'Mülheim',
    '51103': 'Kalk', '51105': 'Kalk', '51107': 'Kalk', '51109': 'Kalk',
}

DUESSELDORF_PLZ_BEZIRK = {
    '40213': 'Bezirk 1', '40215': 'Bezirk 1', '40210': 'Bezirk 1',
    '40212': 'Bezirk 1', '40217': 'Bezirk 1',
    '40233': 'Bezirk 2', '40235': 'Bezirk 2', '40237': 'Bezirk 2',
    '40219': 'Bezirk 3', '40221': 'Bezirk 3', '40223': 'Bezirk 3',
    '40225': 'Bezirk 3',
    '40545': 'Bezirk 4', '40547': 'Bezirk 4', '40549': 'Bezirk 4',
    '40468': 'Bezirk 5', '40474': 'Bezirk 5', '40476': 'Bezirk 5',
    '40472': 'Bezirk 6', '40470': 'Bezirk 6', '40239': 'Bezirk 6',
    '40625': 'Bezirk 7', '40627': 'Bezirk 7', '40629': 'Bezirk 7',
    '40229': 'Bezirk 8', '40231': 'Bezirk 8',
    '40589': 'Bezirk 9', '40591': 'Bezirk 9', '40593': 'Bezirk 9',
    '40595': 'Bezirk 9', '40597': 'Bezirk 9', '40599': 'Bezirk 9',
    '40595': 'Bezirk 10', '40597': 'Bezirk 10',
}

# Crime categories that we estimate at bezirk level
# Maps: city_data_key → output_column_name
CRIME_CATEGORIES = {
    'straftaten_2023': 'crime_straftaten_2023',
    'strassenraub_2023': 'crime_strassenraub_2023',
    'koerperverletzung_2023': 'crime_koerperverletzung_2023',
    'diebstahl_fahrrad_2023': 'crime_diebstahl_fahrrad_2023',
    'wohnungseinbruch_2023': 'crime_wohnungseinbruch_2023',
}


def match_school_to_bezirk(row: pd.Series) -> Tuple[Optional[str], Optional[str]]:
    """Match a school to its city Bezirk using PLZ."""
    stadt = str(row.get('stadt', row.get('ort', ''))).strip()
    plz = str(row.get('plz', '')).strip()

    if 'Köln' in stadt or 'Koeln' in stadt:
        bezirk = KOELN_PLZ_BEZIRK.get(plz)
        return bezirk, 'Köln'

    elif 'Düsseldorf' in stadt or 'Duesseldorf' in stadt:
        bezirk = DUESSELDORF_PLZ_BEZIRK.get(plz)
        return bezirk, 'Düsseldorf'

    return None, stadt


def compute_bezirk_estimates(city_data: dict, bezirk_data: dict) -> dict:
    """
    Compute estimated absolute crime numbers for a Stadtbezirk.

    Formula:
        city_rate_per_100k = city_category_total / city_population * 100,000
        bezirk_rate_per_100k = city_rate * bezirk_index
        bezirk_estimated = bezirk_rate * bezirk_population / 100,000

    Returns dict of column_name → estimated_value
    """
    city_pop = city_data['population']
    bezirk_index = bezirk_data['crime_index']
    bezirk_pop = bezirk_data['einwohner']

    estimates = {}
    estimates['crime_bezirk_population'] = bezirk_pop
    estimates['crime_bezirk_index'] = bezirk_index

    for city_key, col_name in CRIME_CATEGORIES.items():
        city_total = city_data.get(city_key)
        if city_total is not None:
            # City rate per 100k
            city_rate = city_total / city_pop * 100_000
            # Bezirk rate per 100k, adjusted by index
            bezirk_rate = city_rate * bezirk_index
            # Estimated absolute number for this bezirk
            estimated = bezirk_rate * bezirk_pop / 100_000
            estimates[col_name] = round(estimated)
            # Also store the rate
            estimates[f'{col_name}_rate_per_100k'] = round(bezirk_rate, 1)
        else:
            estimates[col_name] = None
            estimates[f'{col_name}_rate_per_100k'] = None

    return estimates


def enrich_with_crime(schools_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich schools with Bezirk-level crime estimates.

    For schools matched to a Stadtbezirk: uses population-weighted estimates.
    For unmatched schools: falls back to city-wide numbers.
    """
    logger.info("Enriching schools with crime data (Bezirk-level estimates)...")

    df = schools_df.copy()

    # Initialize all crime columns
    output_columns = [
        'crime_bezirk',
        'crime_stadt',
        'crime_bezirk_population',
        'crime_bezirk_index',
        'crime_haeufigkeitszahl_2023',
        'crime_aufklaerungsquote_2023',
        'crime_data_source',
    ]
    for _, col_name in CRIME_CATEGORIES.items():
        output_columns.append(col_name)
        output_columns.append(f'{col_name}_rate_per_100k')

    for col in output_columns:
        df[col] = None

    bezirk_matched = 0
    city_matched = 0

    for idx, row in df.iterrows():
        bezirk, stadt = match_school_to_bezirk(row)
        df.at[idx, 'crime_stadt'] = stadt

        city_data = CITY_CRIME_DATA.get(stadt)
        if not city_data:
            continue

        # City-level fields always available
        df.at[idx, 'crime_haeufigkeitszahl_2023'] = city_data['haeufigkeitszahl_2023']
        df.at[idx, 'crime_aufklaerungsquote_2023'] = city_data['aufklaerungsquote_2023']

        if bezirk:
            df.at[idx, 'crime_bezirk'] = bezirk

            # Look up bezirk data
            bezirk_data = None
            if stadt == 'Köln' and bezirk in KOELN_BEZIRKE:
                bezirk_data = KOELN_BEZIRKE[bezirk]
            elif stadt == 'Düsseldorf' and bezirk in DUESSELDORF_BEZIRKE:
                bezirk_data = DUESSELDORF_BEZIRKE[bezirk]

            if bezirk_data:
                # Compute population-weighted bezirk estimates
                estimates = compute_bezirk_estimates(city_data, bezirk_data)
                for col_name, value in estimates.items():
                    df.at[idx, col_name] = value

                # Bezirk-level haeufigkeitszahl
                df.at[idx, 'crime_haeufigkeitszahl_2023'] = round(
                    city_data['haeufigkeitszahl_2023'] * bezirk_data['crime_index'], 1
                )
                df.at[idx, 'crime_data_source'] = 'bezirk_estimated'
                bezirk_matched += 1
                continue

        # Fallback: city-wide numbers (no bezirk match)
        city_pop = city_data['population']
        for city_key, col_name in CRIME_CATEGORIES.items():
            city_total = city_data.get(city_key)
            if city_total is not None:
                df.at[idx, col_name] = city_total
                df.at[idx, f'{col_name}_rate_per_100k'] = round(
                    city_total / city_pop * 100_000, 1
                )

        df.at[idx, 'crime_bezirk_population'] = city_pop
        df.at[idx, 'crime_bezirk_index'] = 1.0  # city average
        df.at[idx, 'crime_data_source'] = 'city_aggregate'
        city_matched += 1

    total_matched = bezirk_matched + city_matched
    logger.info(f"  Bezirk-level estimates: {bezirk_matched}/{len(df)} schools")
    logger.info(f"  City-level fallback:    {city_matched}/{len(df)} schools")
    logger.info(f"  Total with crime data:  {total_matched}/{len(df)} schools")

    return df


def save_output(df: pd.DataFrame, school_type: str):
    """Save enriched data."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_crime.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame, school_type: str):
    """Print enrichment summary."""
    print(f"\n{'=' * 70}")
    print(f"NRW CRIME ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")

    if 'crime_data_source' in df.columns:
        print("\nData source breakdown:")
        for source, count in df['crime_data_source'].value_counts().items():
            print(f"  - {source}: {count}")

    if 'crime_bezirk' in df.columns:
        bezirk_count = df['crime_bezirk'].notna().sum()
        print(f"\nBezirk-level data: {bezirk_count}/{len(df)} schools")

    # Show estimated crime distribution by bezirk
    if 'crime_straftaten_2023' in df.columns and df['crime_straftaten_2023'].notna().any():
        print("\nEstimated total crimes by Bezirk:")
        by_bezirk = df.groupby(['crime_stadt', 'crime_bezirk']).agg(
            schools=('crime_bezirk', 'size'),
            estimated_crimes=('crime_straftaten_2023', 'first'),
            population=('crime_bezirk_population', 'first'),
            rate_per_100k=('crime_haeufigkeitszahl_2023', 'first'),
        ).reset_index()

        for _, row in by_bezirk.sort_values(['crime_stadt', 'estimated_crimes'], ascending=[True, False]).iterrows():
            bezirk = row['crime_bezirk'] if pd.notna(row['crime_bezirk']) else '(no bezirk)'
            print(f"  {row['crime_stadt']:12s} {bezirk:15s}: "
                  f"{row['estimated_crimes']:>7,.0f} crimes | "
                  f"pop {row['population']:>7,.0f} | "
                  f"rate {row['rate_per_100k']:>8,.1f}/100k | "
                  f"{row['schools']} schools")

    print(f"\n{'=' * 70}")


def enrich_schools(school_type: str = "secondary") -> pd.DataFrame:
    """Run crime enrichment for a school type."""

    # Try to load from most recent enrichment step
    input_file = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_transit.csv"
    if not input_file.exists():
        input_file = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_traffic.csv"
    if not input_file.exists():
        input_file = RAW_DIR / f"nrw_{school_type}_schools.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"School data not found: {input_file}")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} {school_type} schools from {input_file.name}")

    enriched_df = enrich_with_crime(schools_df)

    save_output(enriched_df, school_type)
    print_summary(enriched_df, school_type)

    return enriched_df


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting NRW Crime Data Enrichment (with Bezirk population estimates)")
    logger.info("=" * 60)

    for school_type in ['secondary', 'primary']:
        for parent_dir in [INTERMEDIATE_DIR, RAW_DIR]:
            for pattern in [
                f"nrw_{school_type}_schools_with_transit.csv",
                f"nrw_{school_type}_schools_with_traffic.csv",
                f"nrw_{school_type}_schools.csv",
            ]:
                if (parent_dir / pattern).exists():
                    enrich_schools(school_type)
                    break
            else:
                continue
            break
        else:
            logger.warning(f"No {school_type} school data found")


if __name__ == "__main__":
    main()
