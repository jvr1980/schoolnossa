#!/usr/bin/env python3
"""
Phase 4: Dresden Crime Enrichment

Source: Dresden Open Data Portal — Landeskriminalamt Sachsen
Dataset: "Kriminalität ab Stadtteile 2002ff."
Format: CSV, per-Stadtteil, 2002–2024
License: CC BY 3.0 DE

This is better than NRW's approach (city-wide estimates) because Dresden
provides actual per-Stadtteil crime counts. Similar to Hamburg's granularity.

The script:
1. Downloads crime CSV from Dresden Open Data Portal
2. Maps each school to its Stadtteil (by geocoding/PLZ)
3. Assigns Stadtteil-level crime metrics to each school

Author: Dresden School Data Pipeline
Created: 2026-04-07
"""

import pandas as pd
import numpy as np
import requests
import math
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Dresden Open Data crime CSV endpoint
# The dataset "Kriminalität ab Stadtteile 2002ff." is available at:
CRIME_DATA_URL = "https://opendata.dresden.de/duva2ckan/files/de-sn-dresden-kriminalitaet_ab_stadtteile_2002ff_zahlen_fuer_dd-gesamt_ueber_summenbildung_der_stadtteile_inkl_unbekannt_generieren/content"

# Dresden PLZ → Stadtteil mapping (major PLZ areas)
# Dresden has 63 Stadtteile grouped into 10 Ortsamtsbereiche
# This mapping covers the main PLZ areas
DRESDEN_PLZ_STADTTEIL = {
    '01067': 'Innere Altstadt',
    '01069': 'Südvorstadt',
    '01097': 'Innere Neustadt',
    '01099': 'Äußere Neustadt',
    '01108': 'Weixdorf',
    '01109': 'Klotzsche',
    '01127': 'Pieschen',
    '01129': 'Trachenberge',
    '01139': 'Gorbitz',
    '01156': 'Cossebaude',
    '01157': 'Cotta',
    '01159': 'Löbtau',
    '01169': 'Gorbitz',
    '01187': 'Plauen',
    '01189': 'Kleinpestitz/Mockritz',
    '01217': 'Räcknitz/Zschertnitz',
    '01219': 'Strehlen',
    '01237': 'Prohlis',
    '01239': 'Prohlis',
    '01257': 'Leuben',
    '01259': 'Großzschachwitz',
    '01277': 'Blasewitz',
    '01279': 'Tolkewitz',
    '01307': 'Johannstadt',
    '01309': 'Striesen',
    '01324': 'Weißer Hirsch',
    '01326': 'Loschwitz',
    '01328': 'Schönfeld-Weißig',
    '01445': 'Radebeul',  # nearby, may appear in data
    '01465': 'Langebrück',
}

# Population estimates by Stadtteil for rate calculation (2024 approx.)
# Source: Dresden Stadtteilkatalog
STADTTEIL_POPULATION = {
    'Innere Altstadt': 4500,
    'Pirnaische Vorstadt': 3800,
    'Seevorstadt-Ost': 12500,
    'Wilsdruffer Vorstadt': 4200,
    'Friedrichstadt': 6800,
    'Johannstadt': 16000,
    'Innere Neustadt': 10500,
    'Äußere Neustadt': 17500,
    'Leipziger Vorstadt': 11000,
    'Pieschen': 14500,
    'Mickten': 8500,
    'Trachau': 7500,
    'Trachenberge': 5000,
    'Kaditz': 6000,
    'Übigau': 2500,
    'Cotta': 9000,
    'Löbtau': 14000,
    'Naußlitz': 5500,
    'Gorbitz': 18000,
    'Briesnitz': 7000,
    'Plauen': 15000,
    'Südvorstadt': 16500,
    'Räcknitz/Zschertnitz': 4500,
    'Kleinpestitz/Mockritz': 5000,
    'Coschütz/Gittersee': 4000,
    'Strehlen': 7500,
    'Gruna': 7000,
    'Blasewitz': 11000,
    'Striesen': 17500,
    'Tolkewitz': 6000,
    'Seidnitz/Dobritz': 8000,
    'Leuben': 9500,
    'Laubegast': 7000,
    'Großzschachwitz': 5000,
    'Prohlis': 14500,
    'Reick': 5000,
    'Niedersedlitz': 6000,
    'Lockwitz': 4500,
    'Klotzsche': 8500,
    'Hellerau': 3500,
    'Wilschdorf': 2500,
    'Langebrück': 4000,
    'Weixdorf': 6000,
    'Loschwitz': 6500,
    'Weißer Hirsch': 5500,
    'Hosterwitz/Pillnitz': 3000,
    'Schönfeld-Weißig': 11000,
    'Cossebaude': 5000,
    'Mobschatz': 2000,
    'Altfranken': 1500,
}


def download_crime_data() -> pd.DataFrame:
    """Download crime data from Dresden Open Data Portal."""
    cache_file = CACHE_DIR / "dresden_crime_stadtteile.csv"

    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 30 * 86400:
            logger.info("Loading crime data from cache...")
            # Try various encodings/separators
            for sep in [',', ';', '\t']:
                for enc in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
                    try:
                        df = pd.read_csv(cache_file, sep=sep, encoding=enc)
                        if len(df.columns) > 1:
                            return df
                    except Exception:
                        continue
            logger.warning("Failed to parse cached crime data, re-downloading")

    logger.info("Downloading Dresden crime data from Open Data Portal...")

    try:
        resp = requests.get(
            CRIME_DATA_URL,
            headers={'User-Agent': 'SchoolNossa/1.0 (Dresden crime enrichment)'},
            timeout=60
        )
        resp.raise_for_status()
        content = resp.content

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'wb') as f:
            f.write(content)

        # Parse — try various formats
        for sep in [',', ';', '\t']:
            for enc in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(cache_file, sep=sep, encoding=enc)
                    if len(df.columns) > 1:
                        logger.info(f"Parsed crime data: {len(df)} rows, {len(df.columns)} cols (sep='{sep}', enc={enc})")
                        logger.info(f"Columns: {list(df.columns)}")
                        return df
                except Exception:
                    continue

        logger.warning("Could not parse crime data in any format")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Failed to download crime data: {e}")
        return pd.DataFrame()


def get_latest_crime_by_stadtteil(crime_df: pd.DataFrame) -> Dict[str, Dict]:
    """Extract most recent crime data per Stadtteil."""
    if crime_df.empty:
        return {}

    logger.info(f"Crime data columns: {list(crime_df.columns)}")
    logger.info(f"Crime data shape: {crime_df.shape}")
    logger.info(f"First few rows:\n{crime_df.head()}")

    # The dataset structure may vary — try to identify year and Stadtteil columns
    year_col = None
    stadtteil_col = None
    cases_col = None
    solved_col = None

    for col in crime_df.columns:
        cl = col.lower()
        if 'jahr' in cl or 'year' in cl:
            year_col = col
        elif 'stadtteil' in cl or 'bezirk' in cl or 'ortsteil' in cl or 'gebiet' in cl:
            stadtteil_col = col
        elif 'erfasst' in cl or 'fälle' in cl or 'faelle' in cl or 'cases' in cl:
            cases_col = col
        elif 'aufgekl' in cl or 'solved' in cl:
            solved_col = col

    if not stadtteil_col:
        logger.warning("Could not identify Stadtteil column in crime data")
        # Try first column
        stadtteil_col = crime_df.columns[0]
        logger.info(f"Using first column as Stadtteil: '{stadtteil_col}'")

    if not year_col:
        logger.info("No year column found — using all data")

    # Get most recent year if available
    if year_col:
        crime_df[year_col] = pd.to_numeric(crime_df[year_col], errors='coerce')
        max_year = crime_df[year_col].max()
        logger.info(f"Most recent year: {max_year}")
        recent = crime_df[crime_df[year_col] == max_year].copy()
    else:
        recent = crime_df.copy()

    # Build per-Stadtteil dict
    result = {}
    for _, row in recent.iterrows():
        name = str(row.get(stadtteil_col, '')).strip()
        if not name or name == 'nan':
            continue

        entry = {'stadtteil': name}

        if cases_col:
            entry['crime_cases_total'] = pd.to_numeric(row.get(cases_col), errors='coerce')
        if solved_col:
            entry['crime_cases_solved'] = pd.to_numeric(row.get(solved_col), errors='coerce')

        # Calculate rate if population is available
        pop = STADTTEIL_POPULATION.get(name)
        if pop and cases_col:
            cases = entry.get('crime_cases_total')
            if pd.notna(cases) and cases > 0:
                entry['crime_rate_per_100k'] = round(cases / pop * 100_000, 1)
                entry['crime_population'] = pop

        if year_col:
            entry['crime_year'] = int(max_year) if pd.notna(max_year) else None

        result[name] = entry

    logger.info(f"Crime data for {len(result)} Stadtteile")
    return result


def match_school_to_stadtteil(row: pd.Series) -> Optional[str]:
    """Match school to its Stadtteil using PLZ."""
    plz = str(row.get('plz', '')).strip()
    return DRESDEN_PLZ_STADTTEIL.get(plz)


def enrich_with_crime(schools_df: pd.DataFrame, crime_by_stadtteil: Dict[str, Dict]) -> pd.DataFrame:
    """Assign Stadtteil-level crime data to each school."""
    logger.info("Enriching schools with crime data...")

    df = schools_df.copy()

    crime_cols = ['crime_stadtteil', 'crime_cases_total', 'crime_cases_solved',
                  'crime_rate_per_100k', 'crime_population', 'crime_year', 'crime_data_source']
    for col in crime_cols:
        df[col] = None

    matched = 0
    for idx, row in df.iterrows():
        stadtteil = match_school_to_stadtteil(row)
        if not stadtteil:
            continue

        df.at[idx, 'crime_stadtteil'] = stadtteil

        if stadtteil in crime_by_stadtteil:
            data = crime_by_stadtteil[stadtteil]
            for col in ['crime_cases_total', 'crime_cases_solved', 'crime_rate_per_100k',
                         'crime_population', 'crime_year']:
                if col in data:
                    df.at[idx, col] = data[col]
            df.at[idx, 'crime_data_source'] = 'stadtteil_actual'
            matched += 1
        else:
            df.at[idx, 'crime_data_source'] = 'stadtteil_unmatched'

    logger.info(f"Matched {matched}/{len(df)} schools to Stadtteil crime data")
    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Crime Enrichment (Open Data Portal)")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Load school data (fallback chain)
    for candidate in [
        INTERMEDIATE_DIR / "dresden_schools_with_transit.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_traffic.csv",
        RAW_DIR / "dresden_schools_raw.csv",
    ]:
        if candidate.exists():
            input_file = candidate
            break
    else:
        raise FileNotFoundError("No school data found")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} schools from {input_file.name}")

    # Download and parse crime data
    crime_df = download_crime_data()
    crime_by_stadtteil = get_latest_crime_by_stadtteil(crime_df)

    # Enrich schools
    enriched = enrich_with_crime(schools_df, crime_by_stadtteil)

    # Save
    out_path = INTERMEDIATE_DIR / "dresden_schools_with_crime.csv"
    enriched.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out_path}")

    # Summary
    print(f"\n{'='*70}")
    print("DRESDEN CRIME ENRICHMENT - COMPLETE")
    print(f"{'='*70}")
    print(f"Total schools: {len(enriched)}")
    if 'crime_data_source' in enriched.columns:
        for src, cnt in enriched['crime_data_source'].value_counts().items():
            print(f"  {src}: {cnt}")
    if 'crime_rate_per_100k' in enriched.columns:
        valid = enriched['crime_rate_per_100k'].dropna()
        if not valid.empty:
            print(f"Crime rate range: {valid.min():.0f} - {valid.max():.0f} per 100k")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
