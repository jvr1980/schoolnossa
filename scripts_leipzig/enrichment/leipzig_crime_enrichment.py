#!/usr/bin/env python3
"""
Phase 4: Leipzig Crime Data Enrichment (Ortsteil-Level)
=======================================================

Enriches school data with crime statistics from the Leipzig Open Data API.
Leipzig provides excellent Ortsteil-level (63 districts) crime data -- one of
the best granularities among our pipeline cities.

Data Source: Leipzig statistik.leipzig.de Open Data API
CSV endpoint: https://statistik.leipzig.de/opendata/api/kdvalues?kategorie_nr=12&rubrik_nr=1&periode=y&format=csv
Format: CSV (semicolon-separated)
Granularity: 63 Ortsteile + 10 Stadtbezirke
Years: 2004-2023 (annual)
License: CC BY 4.0 / DL-BY-DE 2.0

Crime categories (Sachmerkmal):
    - Straftaten insgesamt (total crimes)
    - Diebstahl (theft)
    - Koerperverletzung (assault)
    - Vermoegensdelikte (property crimes, from 2010)
    - Straftaten je Einwohner (per-capita rate)

This script:
1. Downloads Ortsteil-level crime data from Leipzig Open Data API
2. Parses the pivot table (rows = Ortsteil x Sachmerkmal, columns = years)
3. Extracts the most recent year of data
4. Maps each school to its Ortsteil (column match, address fallback)
5. Joins crime metrics per Ortsteil
6. Calculates safety scores relative to city average
7. Assigns safety categories (very safe, safe, average, below average, high crime)

Output columns (Berlin schema compatible):
    crime_total, crime_theft, crime_assault, crime_property
    crime_per_1000_residents (from "je Einwohner" * 1000)
    crime_safety_category, crime_safety_rank
    crime_ortsteil, crime_stadtbezirk
    crime_year (most recent year used)

Input: data_leipzig/intermediate/leipzig_schools_with_transit.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_crime.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import requests
import logging
import re
import io
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Leipzig Open Data API endpoint for crime statistics
CRIME_API_URL = (
    "https://statistik.leipzig.de/opendata/api/kdvalues"
    "?kategorie_nr=12&rubrik_nr=1&periode=y&format=csv"
)

# Fallback input chain (most-enriched first)
INPUT_FALLBACKS = [
    INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    INTERMEDIATE_DIR / "leipzig_schools_enriched.csv",
    RAW_DIR / "leipzig_schools_raw.csv",
    RAW_DIR / "leipzig_schools.csv",
]

# Mapping of Sachmerkmal values to our output column names
SACHMERKMAL_MAP = {
    'Straftaten insgesamt': 'crime_total',
    'Diebstahl': 'crime_theft',
    'Körperverletzung': 'crime_assault',
    'Koerperverletzung': 'crime_assault',
    'Vermögensdelikte': 'crime_property',
    'Vermoegensdelikte': 'crime_property',
    'Straftaten je Einwohner': 'crime_per_capita_raw',
}

# Leipzig Stadtbezirke and their Ortsteile
# Source: https://www.leipzig.de/buergerservice-und-verwaltung/unsere-stadt/gebietsgliederung
LEIPZIG_STADTBEZIRKE = {
    'Mitte': [
        'Zentrum', 'Zentrum-Ost', 'Zentrum-Südost', 'Zentrum-Süd',
        'Zentrum-West', 'Zentrum-Nordwest', 'Zentrum-Nord',
    ],
    'Nordost': [
        'Schönefeld-Abtnaundorf', 'Schönefeld-Ost', 'Mockau-Süd',
        'Mockau-Nord', 'Thekla', 'Plaußig-Portitz',
    ],
    'Ost': [
        'Neustadt-Neuschönefeld', 'Volkmarsdorf', 'Anger-Crottendorf',
        'Reudnitz-Thonberg', 'Stötteritz', 'Probstheida',
        'Mölkau', 'Engelsdorf', 'Baalsdorf', 'Althen-Kleinpösna',
    ],
    'Südost': [
        'Connewitz', 'Marienbrunn', 'Lößnig', 'Dölitz-Dösen',
        'Probstheida', 'Meusdorf', 'Liebertwolkwitz', 'Holzhausen',
    ],
    'Süd': [
        'Südvorstadt', 'Connewitz', 'Marienbrunn', 'Lößnig',
        'Dölitz-Dösen',
    ],
    'Südwest': [
        'Schleußig', 'Plagwitz', 'Kleinzschocher', 'Großzschocher',
        'Knautkleeberg-Knauthain', 'Hartmannsdorf-Knautnaundorf',
    ],
    'West': [
        'Schönau', 'Grünau-Ost', 'Grünau-Mitte', 'Grünau-Siedlung',
        'Lausen-Grünau', 'Grünau-Nord', 'Miltitz',
    ],
    'Alt-West': [
        'Lindenau', 'Altlindenau', 'Neulindenau', 'Leutzsch', 'Böhlitz-Ehrenberg',
        'Burghausen-Rückmarsdorf',
    ],
    'Nordwest': [
        'Möckern', 'Wahren', 'Lützschena-Stahmeln', 'Lindenthal',
    ],
    'Nord': [
        'Gohlis-Süd', 'Gohlis-Mitte', 'Gohlis-Nord', 'Eutritzsch',
        'Seehausen', 'Wiederitzsch',
    ],
}

# Build reverse lookup: Ortsteil -> Stadtbezirk
ORTSTEIL_TO_STADTBEZIRK: Dict[str, str] = {}
for bezirk, ortsteile in LEIPZIG_STADTBEZIRKE.items():
    for ot in ortsteile:
        ORTSTEIL_TO_STADTBEZIRK[ot.lower()] = bezirk


def normalize_ortsteil(name: str) -> str:
    """Normalize an Ortsteil name for fuzzy matching."""
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    # Normalize umlauts
    for old, new in [('ä', 'ae'), ('ö', 'oe'), ('ü', 'ue'), ('ß', 'ss')]:
        name = name.replace(old, new)
    # Remove punctuation, collapse whitespace
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


class LeipzigCrimeEnrichment:
    """Enriches Leipzig school data with Ortsteil-level crime statistics."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SchoolNossa/1.0 (Leipzig school data enrichment)'
        })
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _read_crime_csv(path) -> pd.DataFrame:
        """Read crime CSV trying multiple sep/encoding combos."""
        for sep in [',', ';', '\t']:
            for enc in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc)
                    if len(df.columns) > 2 and len(df) > 5:
                        logger.info(f"Parsed crime CSV with sep={repr(sep)}, enc={enc}: {len(df)} rows, {len(df.columns)} cols")
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
        raise ValueError(f"Could not parse {path} with any sep/encoding combo")

    # ------------------------------------------------------------------
    # 1. Load school data
    # ------------------------------------------------------------------

    def load_schools(self) -> pd.DataFrame:
        """Load school data from the fallback chain."""
        for filepath in INPUT_FALLBACKS:
            if filepath.exists():
                df = pd.read_csv(filepath)
                logger.info(f"Loaded {len(df)} schools from {filepath.name}")
                # Normalize coordinate column names
                if 'lat' in df.columns and 'latitude' not in df.columns:
                    df = df.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
                return df

        raise FileNotFoundError(
            f"No school data found. Tried: {[f.name for f in INPUT_FALLBACKS]}"
        )

    # ------------------------------------------------------------------
    # 2. Download and cache crime data
    # ------------------------------------------------------------------

    def download_crime_data(self) -> pd.DataFrame:
        """Download Ortsteil-level crime data from Leipzig Open Data API."""
        cache_path = CACHE_DIR / "leipzig_crime_ortsteil.csv"

        # Use cache if fresh (< 7 days)
        if cache_path.exists():
            age_days = (datetime.now().timestamp() - cache_path.stat().st_mtime) / 86400
            if age_days < 7:
                logger.info(f"Using cached crime data ({age_days:.1f} days old)")
                return self._read_crime_csv(cache_path)

        logger.info(f"Downloading crime data from Leipzig Open Data API...")
        try:
            resp = self.session.get(CRIME_API_URL, timeout=60)
            resp.raise_for_status()

            # Save raw response to cache
            cache_path.write_bytes(resp.content)
            logger.info(f"Cached API response to {cache_path.name} ({len(resp.content) / 1024:.1f} KB)")

            df = self._read_crime_csv(cache_path)
            return df

        except requests.RequestException as e:
            logger.error(f"API download failed: {e}")
            # Try cached version regardless of age
            if cache_path.exists():
                logger.info("Falling back to stale cache")
                return pd.read_csv(cache_path, sep=';', encoding='utf-8')
            raise

    # ------------------------------------------------------------------
    # 3. Parse the pivot table into a usable shape
    # ------------------------------------------------------------------

    def parse_crime_pivot(self, df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """
        Parse the Leipzig crime CSV pivot table.

        The API returns a table with columns like:
            Sachgebiet | Sachmerkmal | Ortsteil | 2004 | 2005 | ... | 2023

        We need to:
        - Identify the year columns
        - Pick the most recent year with data
        - Pivot so rows = Ortsteil, columns = crime category
        """
        logger.info(f"Parsing crime data: {df_raw.shape[0]} rows, {df_raw.shape[1]} columns")
        logger.info(f"Columns: {list(df_raw.columns)}")

        # Identify year columns (numeric column names that look like years)
        year_cols = []
        for col in df_raw.columns:
            col_str = str(col).strip()
            if re.match(r'^(19|20)\d{2}$', col_str):
                year_cols.append(col_str)

        if not year_cols:
            # Sometimes years are in a different column name format
            for col in df_raw.columns:
                col_str = str(col).strip()
                if re.match(r'^\d{4}$', col_str):
                    year_cols.append(col_str)

        year_cols_sorted = sorted(year_cols, reverse=True)
        logger.info(f"Found year columns: {year_cols_sorted[:5]}... (total {len(year_cols_sorted)})")

        # Find the Ortsteil / location column
        location_col = None
        for candidate in ['Ortsteil', 'ortsteil', 'Name', 'name', 'Gebiet', 'Raumbezug']:
            if candidate in df_raw.columns:
                location_col = candidate
                break

        if location_col is None:
            # Heuristic: first string column that isn't Sachgebiet/Sachmerkmal
            for col in df_raw.columns:
                if col not in year_cols and col not in ['Sachgebiet', 'Sachmerkmal', 'sachgebiet', 'sachmerkmal']:
                    if df_raw[col].dtype == object:
                        location_col = col
                        break

        if location_col is None:
            raise ValueError(f"Cannot identify location column. Columns: {list(df_raw.columns)}")

        logger.info(f"Location column: '{location_col}'")

        # Find the Sachmerkmal (crime category) column
        category_col = None
        for candidate in ['Sachmerkmal', 'sachmerkmal', 'Merkmal', 'merkmal']:
            if candidate in df_raw.columns:
                category_col = candidate
                break

        if category_col is None:
            raise ValueError(f"Cannot identify Sachmerkmal column. Columns: {list(df_raw.columns)}")

        logger.info(f"Category column: '{category_col}'")
        logger.info(f"Unique categories: {df_raw[category_col].unique().tolist()}")

        # Find the most recent year with actual data
        most_recent_year = None
        for year in year_cols_sorted:
            col_data = pd.to_numeric(df_raw[year].astype(str).str.replace(',', '.'), errors='coerce')
            non_null = col_data.notna().sum()
            if non_null > 5:  # At least a few Ortsteile with data
                most_recent_year = year
                break

        if most_recent_year is None:
            raise ValueError("No year column with sufficient data found")

        logger.info(f"Most recent year with data: {most_recent_year}")

        # Build per-Ortsteil crime metrics
        records = []
        for ortsteil, group in df_raw.groupby(location_col):
            ortsteil_str = str(ortsteil).strip()
            if not ortsteil_str or ortsteil_str.lower() in ['nan', '', 'stadt leipzig', 'leipzig']:
                continue

            record = {'ortsteil': ortsteil_str}

            for _, row in group.iterrows():
                category = str(row[category_col]).strip()
                value_str = str(row.get(most_recent_year, '')).strip().replace(',', '.')

                try:
                    value = float(value_str)
                except (ValueError, TypeError):
                    value = np.nan

                # Map to our column names
                col_name = SACHMERKMAL_MAP.get(category)
                if col_name:
                    record[col_name] = value

            # Only keep records that have at least one valid metric
            if any(k != 'ortsteil' and not pd.isna(v) for k, v in record.items()):
                records.append(record)

        df_parsed = pd.DataFrame(records)
        logger.info(f"Parsed crime data for {len(df_parsed)} Ortsteile")

        return df_parsed, int(most_recent_year)

    # ------------------------------------------------------------------
    # 4. Map schools to Ortsteile
    # ------------------------------------------------------------------

    def match_school_to_ortsteil(
        self, school_row: pd.Series, known_ortsteile: set
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Match a school to its Ortsteil and Stadtbezirk.

        Strategy:
        1. Direct column match (ortsteil column from Phase 1 data)
        2. Address-based fuzzy matching against known Ortsteil names
        """
        # Strategy 1: direct column
        for col in ['ortsteil', 'Ortsteil', 'stadtteil', 'Stadtteil', 'district']:
            val = school_row.get(col)
            if pd.notna(val) and str(val).strip():
                ot = str(val).strip()
                ot_norm = normalize_ortsteil(ot)

                # Exact normalized match against known Ortsteile
                for known in known_ortsteile:
                    if normalize_ortsteil(known) == ot_norm:
                        bezirk = ORTSTEIL_TO_STADTBEZIRK.get(known.lower(), None)
                        return known, bezirk

                # Partial / substring match
                for known in known_ortsteile:
                    known_norm = normalize_ortsteil(known)
                    if known_norm in ot_norm or ot_norm in known_norm:
                        bezirk = ORTSTEIL_TO_STADTBEZIRK.get(known.lower(), None)
                        return known, bezirk

        # Strategy 2: extract from address
        for col in ['address', 'adresse', 'strasse', 'Strasse', 'full_address']:
            addr = school_row.get(col)
            if pd.notna(addr):
                addr_norm = normalize_ortsteil(str(addr))
                for known in known_ortsteile:
                    known_norm = normalize_ortsteil(known)
                    if len(known_norm) > 3 and known_norm in addr_norm:
                        bezirk = ORTSTEIL_TO_STADTBEZIRK.get(known.lower(), None)
                        return known, bezirk

        # Strategy 3: check stadtbezirk column
        for col in ['stadtbezirk', 'Stadtbezirk', 'bezirk', 'Bezirk']:
            val = school_row.get(col)
            if pd.notna(val) and str(val).strip():
                return None, str(val).strip()

        return None, None

    # ------------------------------------------------------------------
    # 5. Calculate safety scores and categories
    # ------------------------------------------------------------------

    def calculate_safety_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate safety scores relative to city average and assign categories.

        Uses crime_per_1000_residents as the primary metric for ranking.
        Falls back to crime_total if per-capita data is unavailable.
        """
        # Compute crime_per_1000_residents from the "je Einwohner" field
        if 'crime_per_capita_raw' in df.columns:
            # The API provides "Straftaten je Einwohner" as a decimal (e.g. 0.08)
            # Multiply by 1000 to get per-1000-residents
            df['crime_per_1000_residents'] = pd.to_numeric(
                df['crime_per_capita_raw'], errors='coerce'
            ) * 1000
        else:
            df['crime_per_1000_residents'] = np.nan

        # Choose the ranking metric
        if df['crime_per_1000_residents'].notna().sum() >= 5:
            rank_col = 'crime_per_1000_residents'
        elif 'crime_total' in df.columns and df['crime_total'].notna().sum() >= 5:
            rank_col = 'crime_total'
        else:
            logger.warning("Insufficient crime data for safety ranking")
            df['crime_safety_category'] = 'unknown'
            df['crime_safety_rank'] = np.nan
            return df

        # Only rank schools that have crime data
        mask = df[rank_col].notna()
        city_mean = df.loc[mask, rank_col].mean()
        city_std = df.loc[mask, rank_col].std()

        if city_mean == 0 or pd.isna(city_mean):
            df['crime_safety_category'] = 'unknown'
            df['crime_safety_rank'] = np.nan
            return df

        logger.info(f"Safety ranking metric: {rank_col} (mean={city_mean:.2f}, std={city_std:.2f})")

        # Rank: lower crime = better rank (rank 1 = safest)
        df.loc[mask, 'crime_safety_rank'] = df.loc[mask, rank_col].rank(
            method='min', ascending=True
        ).astype('Int64')

        # Categorize using z-score relative to city mean
        def categorize_safety(value):
            if pd.isna(value) or pd.isna(city_mean) or city_std == 0:
                return 'unknown'
            z = (value - city_mean) / city_std
            if z <= -1.0:
                return 'very safe'
            elif z <= -0.3:
                return 'safe'
            elif z <= 0.3:
                return 'average'
            elif z <= 1.0:
                return 'below average'
            else:
                return 'high crime'

        df.loc[mask, 'crime_safety_category'] = df.loc[mask, rank_col].apply(categorize_safety)
        df.loc[~mask, 'crime_safety_category'] = 'unknown'

        return df

    # ------------------------------------------------------------------
    # 6. Main merge logic
    # ------------------------------------------------------------------

    def merge_crime_data(
        self, df_schools: pd.DataFrame, df_crime: pd.DataFrame, crime_year: int
    ) -> pd.DataFrame:
        """Merge crime data into the schools dataframe."""
        logger.info("Merging crime data into school table...")

        df = df_schools.copy()
        known_ortsteile = set(df_crime['ortsteil'].unique())

        # Initialize output columns
        output_cols = [
            'crime_total', 'crime_theft', 'crime_assault', 'crime_property',
            'crime_per_1000_residents', 'crime_per_capita_raw',
            'crime_safety_category', 'crime_safety_rank',
            'crime_ortsteil', 'crime_stadtbezirk',
            'crime_year', 'crime_data_source',
        ]
        for col in output_cols:
            if col not in df.columns:
                df[col] = None

        matched_ortsteil = 0
        matched_bezirk = 0
        unmatched = 0

        # Build a lookup dict for faster matching
        crime_lookup = {}
        for _, row in df_crime.iterrows():
            crime_lookup[normalize_ortsteil(row['ortsteil'])] = row

        # City-wide averages for fallback
        city_averages = {}
        for col in ['crime_total', 'crime_theft', 'crime_assault', 'crime_property', 'crime_per_capita_raw']:
            if col in df_crime.columns:
                city_averages[col] = df_crime[col].mean()

        for idx, school in df.iterrows():
            ortsteil, stadtbezirk = self.match_school_to_ortsteil(school, known_ortsteile)

            df.at[idx, 'crime_year'] = crime_year

            if ortsteil:
                df.at[idx, 'crime_ortsteil'] = ortsteil
                df.at[idx, 'crime_stadtbezirk'] = stadtbezirk or ORTSTEIL_TO_STADTBEZIRK.get(
                    ortsteil.lower(), None
                )

                # Find matching crime row
                ot_norm = normalize_ortsteil(ortsteil)
                crime_row = crime_lookup.get(ot_norm)

                if crime_row is None:
                    # Fuzzy fallback
                    for key, row in crime_lookup.items():
                        if key in ot_norm or ot_norm in key:
                            crime_row = row
                            break

                if crime_row is not None:
                    for col in ['crime_total', 'crime_theft', 'crime_assault',
                                'crime_property', 'crime_per_capita_raw']:
                        if col in crime_row.index and pd.notna(crime_row[col]):
                            df.at[idx, col] = crime_row[col]

                    df.at[idx, 'crime_data_source'] = 'ortsteil'
                    matched_ortsteil += 1
                    continue

            if stadtbezirk:
                df.at[idx, 'crime_stadtbezirk'] = stadtbezirk

                # Average across all Ortsteile in this Stadtbezirk
                bezirk_ortsteile = LEIPZIG_STADTBEZIRKE.get(stadtbezirk, [])
                bezirk_rows = df_crime[
                    df_crime['ortsteil'].apply(normalize_ortsteil).isin(
                        [normalize_ortsteil(ot) for ot in bezirk_ortsteile]
                    )
                ]

                if not bezirk_rows.empty:
                    for col in ['crime_total', 'crime_theft', 'crime_assault',
                                'crime_property', 'crime_per_capita_raw']:
                        if col in bezirk_rows.columns:
                            mean_val = bezirk_rows[col].mean()
                            if pd.notna(mean_val):
                                df.at[idx, col] = round(mean_val, 2)

                    df.at[idx, 'crime_data_source'] = 'stadtbezirk_avg'
                    matched_bezirk += 1
                    continue

            # Fallback: city-wide average
            for col, avg_val in city_averages.items():
                if pd.notna(avg_val):
                    df.at[idx, col] = round(avg_val, 2)

            df.at[idx, 'crime_data_source'] = 'city_average'
            unmatched += 1

        logger.info(f"  Ortsteil-level match:    {matched_ortsteil}/{len(df)} schools")
        logger.info(f"  Stadtbezirk-level avg:   {matched_bezirk}/{len(df)} schools")
        logger.info(f"  City-average fallback:   {unmatched}/{len(df)} schools")

        # Calculate safety metrics
        df = self.calculate_safety_metrics(df)

        # Clean up: compute crime_per_1000_residents and drop raw column
        if 'crime_per_capita_raw' in df.columns:
            # Ensure crime_per_1000_residents is filled from raw where needed
            mask = df['crime_per_1000_residents'].isna() & df['crime_per_capita_raw'].notna()
            df.loc[mask, 'crime_per_1000_residents'] = (
                pd.to_numeric(df.loc[mask, 'crime_per_capita_raw'], errors='coerce') * 1000
            )
            df.drop(columns=['crime_per_capita_raw'], inplace=True)

        return df

    # ------------------------------------------------------------------
    # 7. Save output
    # ------------------------------------------------------------------

    def save_output(self, df: pd.DataFrame):
        """Save enriched data."""
        csv_path = INTERMEDIATE_DIR / "leipzig_schools_with_crime.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {csv_path}")

    # ------------------------------------------------------------------
    # 8. Summary
    # ------------------------------------------------------------------

    def print_summary(self, df: pd.DataFrame):
        """Print enrichment summary."""
        print("\n" + "=" * 70)
        print("LEIPZIG CRIME ENRICHMENT - COMPLETE")
        print("=" * 70)

        print(f"\nTotal schools: {len(df)}")

        # Coverage
        coverage = {
            'crime_total': 'Total crimes',
            'crime_theft': 'Theft',
            'crime_assault': 'Assault',
            'crime_property': 'Property crimes',
            'crime_per_1000_residents': 'Per-1000 rate',
            'crime_ortsteil': 'Matched to Ortsteil',
        }
        print("\nCrime data coverage:")
        for col, label in coverage.items():
            if col in df.columns:
                count = df[col].notna().sum()
                pct = 100 * count / len(df) if len(df) > 0 else 0
                print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

        # Data source breakdown
        if 'crime_data_source' in df.columns:
            print("\nData source breakdown:")
            for source, count in df['crime_data_source'].value_counts().items():
                print(f"  - {source}: {count} schools")

        # Safety category breakdown
        if 'crime_safety_category' in df.columns:
            print("\nSafety category breakdown:")
            for cat, count in df['crime_safety_category'].value_counts().items():
                print(f"  - {cat}: {count} schools")

        # Crime year
        if 'crime_year' in df.columns and df['crime_year'].notna().any():
            year = df['crime_year'].dropna().iloc[0]
            print(f"\nCrime data year: {int(year)}")

        print("\n" + "=" * 70)

    # ------------------------------------------------------------------
    # 9. Run pipeline
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Run the complete crime enrichment pipeline."""
        logger.info("=" * 60)
        logger.info("Starting Leipzig Crime Data Enrichment (Ortsteil-Level)")
        logger.info("=" * 60)

        # Load schools
        df_schools = self.load_schools()

        # Download crime data
        df_raw = self.download_crime_data()

        # Parse the pivot table
        df_crime, crime_year = self.parse_crime_pivot(df_raw)

        # Save parsed crime data for reference
        parsed_cache = CACHE_DIR / "leipzig_crime_parsed.csv"
        df_crime.to_csv(parsed_cache, index=False, encoding='utf-8-sig')
        logger.info(f"Saved parsed crime data to {parsed_cache.name}")

        # Merge into schools
        df_enriched = self.merge_crime_data(df_schools, df_crime, crime_year)

        # Save output
        self.save_output(df_enriched)

        # Summary
        self.print_summary(df_enriched)

        return df_enriched


def main():
    """Main entry point."""
    enricher = LeipzigCrimeEnrichment()
    enricher.run()


if __name__ == "__main__":
    main()
