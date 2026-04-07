#!/usr/bin/env python3
"""
NRW School Data Combiner
Combines all collected and enriched data into a unified master table.

This script:
1. Loads the raw school data (Phase 1)
2. Loads traffic enrichment (Phase 2)
3. Loads transit enrichment (Phase 3)
4. Loads crime enrichment (Phase 4)
5. Loads POI enrichment (Phase 5)
6. Combines into a single master table
7. Standardizes column names and order

Author: NRW School Data Pipeline
Created: 2026-02-15
"""

import pandas as pd
import logging
from pathlib import Path

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
FINAL_DIR = DATA_DIR / "final"


def find_most_enriched_file(school_type: str) -> pd.DataFrame:
    """Find the most enriched intermediate file for a school type.

    The pipeline runs sequentially: raw -> traffic -> transit -> crime -> pois.
    We look for files in reverse order to find the most complete one.
    """
    # Order from most to least enriched
    candidates = [
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_website_metadata.csv",
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_anmeldezahlen.csv",
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_pois.csv",
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_crime.csv",
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_transit.csv",
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_traffic.csv",
        RAW_DIR / f"nrw_{school_type}_schools.csv",
    ]

    for filepath in candidates:
        if filepath.exists():
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} {school_type} schools from {filepath.name} ({len(df.columns)} columns)")
            return df

    raise FileNotFoundError(f"No {school_type} school data found")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and order."""
    logger.info("Standardizing column order...")

    preferred_order = [
        # Core identification
        'schulnummer', 'schulname', 'school_type', 'schulform_name',

        # Location
        'strasse', 'plz', 'ort', 'stadt', 'bundesland',
        'latitude', 'longitude',
        'bezirksregierung_name', 'gemeindeschluessel',

        # Contact
        'telefon', 'email', 'website',

        # Operator
        'traegerschaft',

        # School characteristics
        'sozialindexstufe',

        # Transit (Berlin-compatible format)
        'transit_stops_500m', 'transit_stop_count_1000m',
        'transit_rail_01_name', 'transit_rail_01_distance_m',
        'transit_rail_01_latitude', 'transit_rail_01_longitude', 'transit_rail_01_lines',
        'transit_rail_02_name', 'transit_rail_02_distance_m',
        'transit_rail_02_latitude', 'transit_rail_02_longitude', 'transit_rail_02_lines',
        'transit_rail_03_name', 'transit_rail_03_distance_m',
        'transit_rail_03_latitude', 'transit_rail_03_longitude', 'transit_rail_03_lines',
        'transit_tram_01_name', 'transit_tram_01_distance_m',
        'transit_tram_01_latitude', 'transit_tram_01_longitude', 'transit_tram_01_lines',
        'transit_tram_02_name', 'transit_tram_02_distance_m',
        'transit_tram_02_latitude', 'transit_tram_02_longitude', 'transit_tram_02_lines',
        'transit_tram_03_name', 'transit_tram_03_distance_m',
        'transit_tram_03_latitude', 'transit_tram_03_longitude', 'transit_tram_03_lines',
        'transit_bus_01_name', 'transit_bus_01_distance_m',
        'transit_bus_01_latitude', 'transit_bus_01_longitude', 'transit_bus_01_lines',
        'transit_bus_02_name', 'transit_bus_02_distance_m',
        'transit_bus_02_latitude', 'transit_bus_02_longitude', 'transit_bus_02_lines',
        'transit_bus_03_name', 'transit_bus_03_distance_m',
        'transit_bus_03_latitude', 'transit_bus_03_longitude', 'transit_bus_03_lines',
        'transit_all_lines_1000m',
        'transit_accessibility_score',

        # Traffic accidents
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'traffic_accidents_fatal', 'traffic_accidents_serious', 'traffic_accidents_minor',
        'traffic_accidents_bicycle', 'traffic_accidents_pedestrian',
        'traffic_accidents_school_hours', 'traffic_nearest_accident_m',

        # Crime
        'crime_bezirk', 'crime_stadt',
        'crime_straftaten_2023', 'crime_haeufigkeitszahl_2023',
        'crime_aufklaerungsquote_2023', 'crime_bezirk_index',
        'crime_trend',

        # Metadata
        'data_source', 'data_retrieved',
    ]

    ordered_cols = [c for c in preferred_order if c in df.columns]
    remaining_cols = [c for c in df.columns if c not in ordered_cols]
    ordered_cols.extend(remaining_cols)

    return df[ordered_cols]


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate data."""
    logger.info("Cleaning data...")

    df = df.copy()

    # Remove duplicates
    if 'schulnummer' in df.columns:
        original = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < original:
            logger.info(f"Removed {original - len(df)} duplicate schools")

    # Ensure numeric columns are numeric
    numeric_cols = ['latitude', 'longitude', 'sozialindexstufe',
                    'transit_stops_500m', 'transit_stop_count_1000m', 'transit_accessibility_score',
                    'traffic_accidents_total', 'traffic_accidents_per_year',
                    'crime_haeufigkeitszahl_2023', 'crime_bezirk_index']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Clean PLZ
    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.zfill(5)

    return df


def save_outputs(df: pd.DataFrame, school_type: str):
    """Save combined master table."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = FINAL_DIR / f"nrw_{school_type}_school_master_table.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")

    parquet_path = FINAL_DIR / f"nrw_{school_type}_school_master_table.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")


def print_summary(df: pd.DataFrame, school_type: str):
    """Print summary of combined data."""
    print(f"\n{'=' * 70}")
    print(f"NRW DATA COMBINER ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'school_type' in df.columns:
        print("\nSchools by type:")
        for t, count in df['school_type'].value_counts().items():
            print(f"  - {t}: {count}")

    if 'stadt' in df.columns:
        print("\nSchools by city:")
        for city, count in df['stadt'].value_counts().items():
            print(f"  - {city}: {count}")

    if 'traegerschaft' in df.columns:
        print("\nSchools by operator:")
        for t, count in df['traegerschaft'].value_counts().items():
            print(f"  - {t}: {count}")

    print("\nData coverage:")
    coverage_cols = {
        'schulnummer': 'School ID',
        'schulname': 'School Name',
        'latitude': 'Coordinates',
        'sozialindexstufe': 'Schulsozialindex',
        'transit_accessibility_score': 'Transit Score',
        'traffic_accidents_total': 'Traffic Accidents',
        'crime_haeufigkeitszahl_2023': 'Crime Stats',
        'schueler_2024_25': 'Students (Website)',
        'lehrer_2024_25': 'Teachers (Website)',
        'sprachen': 'Languages',
        'description': 'Description (EN)',
        'description_de': 'Description (DE)',
    }

    # Add POI columns
    poi_cols = [c for c in df.columns if c.startswith('poi_') and c.endswith('_count_500m')]
    if poi_cols:
        coverage_cols[poi_cols[0]] = 'POI Data'

    for col, label in coverage_cols.items():
        if col in df.columns:
            count = df[col].notna().sum()
            pct = 100 * count / len(df)
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'=' * 70}")


def combine_school_type(school_type: str) -> pd.DataFrame:
    """Combine all data for a school type."""
    logger.info(f"Combining {school_type} school data...")

    df = find_most_enriched_file(school_type)
    df = clean_data(df)
    df = standardize_columns(df)
    save_outputs(df, school_type)
    print_summary(df, school_type)

    return df


def main():
    """Main function."""
    logger.info("=" * 60)
    logger.info("Starting NRW School Data Combiner")
    logger.info("=" * 60)

    for school_type in ['secondary', 'primary']:
        try:
            combine_school_type(school_type)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
