#!/usr/bin/env python3
"""
Phase 7: Leipzig Data Combiner
================================

Combines all enriched data sources into a single master table.
Adapted from NRW data combiner (auto-detect most-enriched file pattern).

This script:
1. Finds the most-enriched intermediate file available
2. If that file already has all enrichment columns, uses it directly
3. Otherwise sequentially merges enrichment files on schulnummer
4. Cleans, deduplicates, and standardizes column order
5. Saves combined master table to final/

Input: data_leipzig/intermediate/leipzig_schools_with_*.csv
       data_leipzig/raw/leipzig_schools_raw.csv (fallback)
Output: data_leipzig/final/leipzig_school_master_table_final.csv
        data_leipzig/final/leipzig_school_master_table_final.parquet

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

# Enrichment chain order: most enriched first
ENRICHMENT_CHAIN = [
    INTERMEDIATE_DIR / "leipzig_schools_with_metadata.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_pois.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_crime.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    RAW_DIR / "leipzig_schools_raw.csv",
]

# Column prefixes that indicate enrichment data
ENRICHMENT_PREFIXES = {
    'traffic': 'traffic_',
    'transit': 'transit_',
    'crime': 'crime_',
    'poi': 'poi_',
    'metadata': ['description', 'description_de', 'besonderheiten', 'sprachen',
                 'schueler_', 'lehrer_', 'tuition_', 'ganztagsform'],
}


def find_most_enriched_file() -> tuple:
    """Find the most enriched intermediate file available.

    Returns (DataFrame, Path) for the most complete file found.
    """
    for filepath in ENRICHMENT_CHAIN:
        if filepath.exists():
            df = pd.read_csv(filepath, low_memory=False)
            logger.info(f"Loaded {len(df)} schools from {filepath.name} ({len(df.columns)} columns)")
            return df, filepath

    raise FileNotFoundError(
        "No Leipzig school data found. Expected one of:\n"
        + "\n".join(f"  - {p}" for p in ENRICHMENT_CHAIN)
    )


def check_enrichment_coverage(df: pd.DataFrame) -> dict:
    """Check which enrichment phases are present in the data."""
    coverage = {}

    # Traffic
    traffic_cols = [c for c in df.columns if c.startswith('traffic_')]
    coverage['traffic'] = len(traffic_cols)

    # Transit
    transit_cols = [c for c in df.columns if c.startswith('transit_')]
    coverage['transit'] = len(transit_cols)

    # Crime
    crime_cols = [c for c in df.columns if c.startswith('crime_')]
    coverage['crime'] = len(crime_cols)

    # POI
    poi_cols = [c for c in df.columns if c.startswith('poi_')]
    coverage['poi'] = len(poi_cols)

    # Website metadata / descriptions
    metadata_cols = [c for c in df.columns if c in ['description', 'description_de', 'besonderheiten', 'sprachen']
                     or c.startswith('schueler_') or c.startswith('lehrer_') or c.startswith('tuition_')]
    coverage['metadata'] = len(metadata_cols)

    return coverage


def merge_enrichment_columns(base_df: pd.DataFrame, enrichment_path: Path,
                             enrichment_name: str) -> pd.DataFrame:
    """Merge new columns from an enrichment file into the base DataFrame."""
    if not enrichment_path.exists():
        logger.warning(f"  {enrichment_name} file not found: {enrichment_path.name}")
        return base_df

    enrich_df = pd.read_csv(enrichment_path, low_memory=False)
    logger.info(f"  Loading {enrichment_name}: {len(enrich_df)} rows, {len(enrich_df.columns)} columns")

    if 'schulnummer' not in enrich_df.columns:
        logger.warning(f"  {enrichment_name} has no schulnummer column, skipping")
        return base_df

    # Find columns that are new (not already in base)
    new_cols = [c for c in enrich_df.columns if c not in base_df.columns]
    if not new_cols:
        logger.info(f"  {enrichment_name}: no new columns to merge")
        return base_df

    merge_cols = ['schulnummer'] + new_cols
    merged = base_df.merge(
        enrich_df[merge_cols],
        on='schulnummer',
        how='left'
    )

    logger.info(f"  {enrichment_name}: merged {len(new_cols)} new columns")
    return merged


def sequential_merge(base_df: pd.DataFrame, source_path: Path) -> pd.DataFrame:
    """Sequentially merge enrichment files that have columns missing from base.

    Only merges files that appear EARLIER in the chain than the source
    (i.e., more enriched files that might not exist yet).
    """
    # Build the reverse chain: files that should have been applied but weren't
    enrichment_files = {
        'traffic': INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
        'transit': INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv",
        'crime': INTERMEDIATE_DIR / "leipzig_schools_with_crime.csv",
        'poi': INTERMEDIATE_DIR / "leipzig_schools_with_pois.csv",
        'metadata': INTERMEDIATE_DIR / "leipzig_schools_with_metadata.csv",
    }

    coverage = check_enrichment_coverage(base_df)
    df = base_df.copy()

    for name, filepath in enrichment_files.items():
        if coverage.get(name, 0) == 0 and filepath.exists() and filepath != source_path:
            df = merge_enrichment_columns(df, filepath, name)

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate data."""
    logger.info("Cleaning data...")
    df = df.copy()

    # Remove duplicates on schulnummer
    if 'schulnummer' in df.columns:
        original = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < original:
            logger.info(f"  Removed {original - len(df)} duplicate schools")

    # Ensure numeric columns
    numeric_cols = [
        'latitude', 'longitude',
        'transit_accessibility_score', 'transit_stops_500m', 'transit_stop_count_1000m',
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'crime_total', 'crime_theft', 'crime_assault', 'crime_per_1000_residents',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Map traegerschaft from legal_status_key if still 'unbekannt'
    if 'legal_status_key' in df.columns and 'traegerschaft' in df.columns:
        legal_map = {1: 'oeffentlich', 2: 'frei/privat'}
        mask = df['traegerschaft'].isin(['unbekannt', '', None]) | df['traegerschaft'].isna()
        df.loc[mask, 'traegerschaft'] = df.loc[mask, 'legal_status_key'].map(legal_map)
        mapped = mask.sum()
        if mapped > 0:
            logger.info(f"  Mapped {mapped} traegerschaft values from legal_status_key")

    # Clean PLZ
    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip().str.replace('.0', '', regex=False).str.zfill(5)

    # Clean website URLs
    if 'website' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).lower() in ['nan', 'none', '']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['website'] = df['website'].apply(clean_url)

    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column order."""
    logger.info("Standardizing column order...")

    preferred_order = [
        # Core identification
        'schulnummer', 'schulname', 'school_type', 'schultyp', 'schulart',

        # Location
        'strasse', 'adresse', 'plz', 'ort', 'ortsteil', 'stadtbezirk', 'bundesland',
        'latitude', 'longitude',

        # Contact
        'telefon', 'email', 'website', 'fax',

        # Leadership
        'schulleitung',

        # Operator
        'traeger', 'traegerschaft', 'rechtsform',

        # Student / Teacher data
        'schueler_gesamt', 'schueler_2024_25', 'lehrer_2024_25',

        # Languages & programs
        'sprachen', 'besonderheiten', 'ganztagsform',

        # Descriptions
        'description', 'description_de',

        # Transit
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

        # Traffic
        'traffic_accidents_total', 'traffic_accidents_per_year',
        'traffic_accidents_fatal', 'traffic_accidents_serious', 'traffic_accidents_minor',
        'traffic_accidents_bicycle', 'traffic_accidents_pedestrian',
        'traffic_accidents_school_hours', 'traffic_nearest_accident_m',

        # Crime
        'crime_total', 'crime_theft', 'crime_assault', 'crime_property',
        'crime_per_1000_residents', 'crime_per_capita_raw',
        'crime_ortsteil', 'crime_stadtbezirk',
        'crime_safety_category', 'crime_safety_rank',
        'crime_year', 'crime_data_source',

        # POI (prefix-matched below)

        # Metadata
        'tuition_display', 'tuition_amount', 'tuition_source',
        'data_source', 'data_retrieved',
    ]

    ordered_cols = [c for c in preferred_order if c in df.columns]

    # Add POI columns in order
    poi_cols = sorted([c for c in df.columns if c.startswith('poi_') and c not in ordered_cols])
    ordered_cols.extend(poi_cols)

    # Add remaining columns
    remaining = [c for c in df.columns if c not in ordered_cols]
    ordered_cols.extend(remaining)

    return df[ordered_cols]


def save_outputs(df: pd.DataFrame):
    """Save combined master table."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = FINAL_DIR / "leipzig_school_master_table_final.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")

    parquet_path = FINAL_DIR / "leipzig_school_master_table_final.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")


def print_summary(df: pd.DataFrame):
    """Print summary of combined data."""
    print(f"\n{'=' * 70}")
    print("LEIPZIG DATA COMBINER (Phase 7) - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'school_type' in df.columns:
        print("\nSchools by type:")
        for t, count in df['school_type'].value_counts().items():
            print(f"  - {t}: {count}")
    elif 'schultyp' in df.columns:
        print("\nSchools by type:")
        for t, count in df['schultyp'].value_counts().items():
            print(f"  - {t}: {count}")

    if 'traeger' in df.columns:
        print("\nSchools by operator:")
        for t, count in df['traeger'].value_counts().head(5).items():
            print(f"  - {t}: {count}")

    # Coverage report
    print("\nData coverage:")
    coverage_cols = {
        'schulnummer': 'School ID',
        'schulname': 'School Name',
        'latitude': 'Coordinates',
        'transit_accessibility_score': 'Transit Score',
        'traffic_accidents_total': 'Traffic Accidents',
        'crime_total': 'Crime Stats',
        'crime_safety_category': 'Crime Safety Category',
        'description': 'Description (EN)',
        'description_de': 'Description (DE)',
        'sprachen': 'Languages',
        'besonderheiten': 'Special Features',
        'tuition_display': 'Tuition Info',
    }

    # Add POI coverage
    poi_cols = [c for c in df.columns if c.startswith('poi_') and c.endswith('_count_500m')]
    if poi_cols:
        coverage_cols[poi_cols[0]] = 'POI Data'

    for col, label in coverage_cols.items():
        if col in df.columns:
            count = df[col].notna().sum()
            pct = 100 * count / len(df) if len(df) > 0 else 0
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    # Enrichment column group counts
    enrichment = check_enrichment_coverage(df)
    print("\nEnrichment column counts:")
    for name, count in enrichment.items():
        status = "+" if count > 0 else "-"
        print(f"  {status} {name}: {count} columns")

    print(f"\n{'=' * 70}")


def main():
    """Combine all Leipzig enrichment data into master table."""
    logger.info("=" * 60)
    logger.info("Starting Leipzig Data Combiner (Phase 7)")
    logger.info("=" * 60)

    try:
        # Step 1: Find most enriched file
        df, source_path = find_most_enriched_file()

        # Step 2: Check what enrichments are present
        coverage = check_enrichment_coverage(df)
        logger.info(f"Enrichment coverage in {source_path.name}:")
        for name, count in coverage.items():
            logger.info(f"  {name}: {count} columns")

        # Step 3: If some enrichments are missing, try to merge from other files
        missing = [name for name, count in coverage.items() if count == 0]
        if missing:
            logger.info(f"Missing enrichments: {missing}. Attempting sequential merge...")
            df = sequential_merge(df, source_path)

            # Re-check coverage
            coverage_after = check_enrichment_coverage(df)
            for name in missing:
                if coverage_after.get(name, 0) > 0:
                    logger.info(f"  Recovered {name}: {coverage_after[name]} columns")

        # Step 4: Clean and standardize
        df = clean_data(df)
        df = standardize_columns(df)

        # Step 5: Save
        save_outputs(df)

        # Step 6: Print summary
        print_summary(df)

        logger.info("Leipzig Data Combiner complete!")
        return df

    except Exception as e:
        logger.error(f"Data combination failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
