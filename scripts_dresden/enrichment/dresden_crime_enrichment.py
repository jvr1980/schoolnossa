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
CRIME_DATA_URL = "https://opendata.dresden.de/dcat-ap/dataset/de-sn-dresden-kriminalitaet_ab_stadtteile_2002ff_zahlen_fuer_dd-gesamt_ueber_summenbildung_der_stadtteile_inkl_unbekannt_generieren/content.csv"

# Dresden PLZ → Stadtbezirk mapping
# Crime data uses 10 Stadtbezirke (StB 0-9)
# PLZ mapping based on Dresden's official Stadtbezirk boundaries
DRESDEN_PLZ_STADTBEZIRK = {
    '01067': 'StB 0 Altstadt',
    '01069': 'StB 8 Plauen',          # Südvorstadt
    '01097': 'StB 1 Neustadt',
    '01099': 'StB 1 Neustadt',
    '01108': 'StB 3 Klotzsche/nördliche Ortschaften',
    '01109': 'StB 3 Klotzsche/nördliche Ortschaften',
    '01127': 'StB 2 Pieschen',
    '01129': 'StB 2 Pieschen',
    '01139': 'StB 9 Cotta/westliche Ortschaften',  # Gorbitz
    '01156': 'StB 9 Cotta/westliche Ortschaften',  # Cossebaude
    '01157': 'StB 9 Cotta/westliche Ortschaften',  # Cotta
    '01159': 'StB 9 Cotta/westliche Ortschaften',  # Löbtau
    '01169': 'StB 9 Cotta/westliche Ortschaften',  # Gorbitz
    '01187': 'StB 8 Plauen',
    '01189': 'StB 8 Plauen',          # Kleinpestitz/Mockritz
    '01217': 'StB 8 Plauen',          # Räcknitz/Zschertnitz
    '01219': 'StB 7 Prohlis',         # Strehlen
    '01237': 'StB 7 Prohlis',
    '01239': 'StB 7 Prohlis',
    '01257': 'StB 6 Leuben',
    '01259': 'StB 6 Leuben',          # Großzschachwitz
    '01277': 'StB 5 Blasewitz',
    '01279': 'StB 5 Blasewitz',       # Tolkewitz
    '01307': 'StB 0 Altstadt',        # Johannstadt
    '01309': 'StB 5 Blasewitz',       # Striesen
    '01324': 'StB 4 Loschwitz/OS Schönfeld-Weißig',
    '01326': 'StB 4 Loschwitz/OS Schönfeld-Weißig',
    '01328': 'StB 4 Loschwitz/OS Schönfeld-Weißig',
    '01465': 'StB 3 Klotzsche/nördliche Ortschaften',
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


def get_latest_crime_by_stadtbezirk(crime_df: pd.DataFrame) -> Dict[str, Dict]:
    """Extract most recent crime data aggregated per Stadtbezirk."""
    if crime_df.empty:
        return {}

    logger.info(f"Crime data columns: {list(crime_df.columns)}")

    year_col = 'Jahr'
    bezirk_col = 'Stadtbezirk mit zugeordneten Ortschaften'
    cases_col = 'Fälle erfasst (Tatortprinzip)'
    solved_col = 'Fälle aufgeklärt'
    suspects_col = 'Tatverdächtige insgesamt'

    # Get most recent year
    crime_df[year_col] = pd.to_numeric(crime_df[year_col], errors='coerce')
    max_year = crime_df[year_col].max()
    logger.info(f"Most recent year: {max_year}")
    recent = crime_df[crime_df[year_col] == max_year].copy()

    # Convert numeric columns
    for col in [cases_col, solved_col, suspects_col]:
        if col in recent.columns:
            recent[col] = pd.to_numeric(recent[col], errors='coerce')

    # Aggregate by Stadtbezirk
    grouped = recent.groupby(bezirk_col).agg({
        cases_col: 'sum',
        solved_col: 'sum',
        suspects_col: 'sum',
    }).reset_index()

    # Stadtbezirk populations (approx. 2024)
    bezirk_populations = {
        'StB 0 Altstadt': 48000,
        'StB 1 Neustadt': 39000,
        'StB 2 Pieschen': 55000,
        'StB 3 Klotzsche/nördliche Ortschaften': 25000,
        'StB 4 Loschwitz/OS Schönfeld-Weißig': 31000,
        'StB 5 Blasewitz': 90000,
        'StB 6 Leuben': 40000,
        'StB 7 Prohlis': 58000,
        'StB 8 Plauen': 72000,
        'StB 9 Cotta/westliche Ortschaften': 75000,
    }

    result = {}
    for _, row in grouped.iterrows():
        name = str(row[bezirk_col]).strip()
        if not name or name in ('nan', 'unbekannt'):
            continue

        cases = row[cases_col]
        solved = row[solved_col]
        pop = bezirk_populations.get(name)

        entry = {
            'crime_stadtbezirk': name,
            'crime_cases_total': int(cases) if pd.notna(cases) else None,
            'crime_cases_solved': int(solved) if pd.notna(solved) else None,
            'crime_year': int(max_year),
        }

        if pop and pd.notna(cases) and cases > 0:
            entry['crime_rate_per_100k'] = round(cases / pop * 100_000, 1)
            entry['crime_clearance_rate'] = round(solved / cases * 100, 1) if cases > 0 else None
            entry['crime_population'] = pop

        result[name] = entry
        logger.info(f"  {name}: {cases:.0f} cases, rate {entry.get('crime_rate_per_100k', 'N/A')}/100k")

    logger.info(f"Crime data for {len(result)} Stadtbezirke")
    return result


def match_school_to_stadtbezirk(row: pd.Series) -> Optional[str]:
    """Match school to its Stadtbezirk using PLZ."""
    plz = str(row.get('plz', '')).strip().zfill(5)
    return DRESDEN_PLZ_STADTBEZIRK.get(plz)


def enrich_with_crime(schools_df: pd.DataFrame, crime_by_bezirk: Dict[str, Dict]) -> pd.DataFrame:
    """Assign Stadtbezirk-level crime data to each school."""
    logger.info("Enriching schools with crime data...")

    df = schools_df.copy()

    crime_cols = ['crime_stadtbezirk', 'crime_cases_total', 'crime_cases_solved',
                  'crime_rate_per_100k', 'crime_clearance_rate', 'crime_population',
                  'crime_year', 'crime_data_source']
    for col in crime_cols:
        df[col] = None

    matched = 0
    unmatched_plz = set()
    for idx, row in df.iterrows():
        bezirk = match_school_to_stadtbezirk(row)
        if not bezirk:
            plz = str(row.get('plz', '')).strip()
            if plz:
                unmatched_plz.add(plz)
            continue

        df.at[idx, 'crime_stadtbezirk'] = bezirk

        if bezirk in crime_by_bezirk:
            data = crime_by_bezirk[bezirk]
            for col in ['crime_cases_total', 'crime_cases_solved', 'crime_rate_per_100k',
                         'crime_clearance_rate', 'crime_population', 'crime_year']:
                if col in data:
                    df.at[idx, col] = data[col]
            df.at[idx, 'crime_data_source'] = 'stadtbezirk_actual'
            matched += 1
        else:
            df.at[idx, 'crime_data_source'] = 'stadtbezirk_unmatched'

    if unmatched_plz:
        logger.warning(f"Unmatched PLZ codes: {sorted(unmatched_plz)}")
    logger.info(f"Matched {matched}/{len(df)} schools to Stadtbezirk crime data")
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
    crime_by_bezirk = get_latest_crime_by_stadtbezirk(crime_df)

    # Enrich schools
    enriched = enrich_with_crime(schools_df, crime_by_bezirk)

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
