#!/usr/bin/env python3
"""
Hamburg School Data Combiner
Combines all collected and enriched data into a unified master table.

This script:
1. Loads the raw school data (Phase 1)
2. Merges Abitur statistics (Phase 2)
3. Merges transit data (Phase 6)
4. Creates the combined master table ready for embeddings

Author: Hamburg School Data Pipeline
Created: 2026-02-01
"""

import pandas as pd
import logging
import re
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"


def clean_school_name(name: str) -> str:
    """Clean school name for matching."""
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    # Remove district in parentheses
    name = re.sub(r'\s*\([^)]+\)\s*$', '', name)
    # Normalize spaces
    name = re.sub(r'\s+', ' ', name)
    return name


def extract_core_name(name: str) -> str:
    """Extract core school name for fuzzy matching."""
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()

    # Remove common prefixes/suffixes
    name = re.sub(r'^(das\s+|die\s+)', '', name)

    # Remove district names that are often appended
    # This handles "Albert-Schweitzer-GymnasiumOhlsdorf" -> "albert-schweitzer-gymnasium"
    # Split on capital letter that follows lowercase
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)

    # Remove district in parentheses
    name = re.sub(r'\s*\([^)]+\)\s*', '', name)

    # Keep only the main school name part (before any district)
    # Common Hamburg districts
    districts = ['ohlsdorf', 'stellingen', 'wilstorf', 'altona', 'harburg',
                 'wandsbek', 'eidelstedt', 'lurup', 'ottensen', 'blankenese',
                 'volksdorf', 'sasel', 'poppenbüttel', 'wellingsbüttel',
                 'eppendorf', 'winterhude', 'barmbek', 'uhlenhorst', 'eilbek',
                 'hamm', 'horn', 'billstedt', 'bergedorf', 'lohbrügge',
                 'neugraben', 'harvestehude', 'rotherbaum', 'othmarschen',
                 'nienstedten', 'rissen', 'sülldorf', 'bahrenfeld', 'osdorf',
                 'hochkamp', 'farmsen', 'bramfeld', 'steilshoop', 'langenhorn',
                 'fuhlsbüttel', 'schnelsen', 'niendorf', 'lokstedt', 'hummelsbüttel',
                 'groß flottbek', 'finkenwerder', 'wilhelmsburg', 'veddel',
                 'st. pauli', 'st. georg', 'neustadt', 'altstadt', 'hafencity']

    for district in districts:
        name = re.sub(rf'\s*{district}\s*$', '', name, flags=re.IGNORECASE)

    # Normalize
    name = name.strip().lower()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'[^\w\s-]', '', name)

    return name


def load_base_data() -> pd.DataFrame:
    """Load the base school data from Phase 1."""
    logger.info("Loading base school data...")

    # Try secondary schools first
    schools_path = RAW_DIR / "hamburg_secondary_schools.csv"
    if not schools_path.exists():
        schools_path = RAW_DIR / "hamburg_schools_raw.csv"

    if not schools_path.exists():
        raise FileNotFoundError(f"No school data found at {schools_path}")

    df = pd.read_csv(schools_path)
    logger.info(f"Loaded {len(df)} schools from {schools_path}")

    return df


def load_abitur_data() -> pd.DataFrame:
    """Load Abitur statistics from Phase 2."""
    logger.info("Loading Abitur data...")

    abitur_path = INTERMEDIATE_DIR / "hamburg_abitur_statistics.csv"
    if not abitur_path.exists():
        logger.warning(f"Abitur data not found at {abitur_path}")
        return pd.DataFrame()

    df = pd.read_csv(abitur_path)
    logger.info(f"Loaded Abitur data for {len(df)} schools")

    return df


def load_transit_data() -> pd.DataFrame:
    """Load transit enrichment from Phase 6."""
    logger.info("Loading transit data...")

    transit_path = INTERMEDIATE_DIR / "hamburg_schools_with_transit.csv"
    if not transit_path.exists():
        logger.warning(f"Transit data not found at {transit_path}")
        return pd.DataFrame()

    df = pd.read_csv(transit_path)
    logger.info(f"Loaded transit data for {len(df)} schools")

    # Keep only transit-specific columns
    transit_cols = ['schulnummer', 'hvv_stops_500m', 'hvv_stops_1000m',
                    'nearest_ubahn_name', 'nearest_ubahn_distance_m',
                    'nearest_sbahn_name', 'nearest_sbahn_distance_m',
                    'nearest_bus_name', 'nearest_bus_distance_m',
                    'transit_accessibility_score']

    # Filter to only existing columns
    transit_cols = [c for c in transit_cols if c in df.columns]

    return df[transit_cols]


def load_teacher_data() -> pd.DataFrame:
    """Load teacher data from Phase 3 (website scraping)."""
    logger.info("Loading teacher data...")

    teacher_path = INTERMEDIATE_DIR / "hamburg_schools_with_teachers.csv"
    if not teacher_path.exists():
        logger.warning(f"Teacher data not found at {teacher_path}")
        return pd.DataFrame()

    df = pd.read_csv(teacher_path)
    logger.info(f"Loaded teacher data for {len(df)} schools")

    # Keep only teacher-specific columns
    teacher_cols = ['schulnummer', 'teacher_count', 'teacher_count_source',
                    'kollegium_url', 'scrape_timestamp']

    teacher_cols = [c for c in teacher_cols if c in df.columns]
    return df[teacher_cols] if teacher_cols else pd.DataFrame()


def load_traffic_data() -> pd.DataFrame:
    """Load traffic data from Phase 4."""
    logger.info("Loading traffic data...")

    traffic_path = INTERMEDIATE_DIR / "hamburg_schools_with_traffic.csv"
    if not traffic_path.exists():
        logger.warning(f"Traffic data not found at {traffic_path}")
        return pd.DataFrame()

    df = pd.read_csv(traffic_path)
    logger.info(f"Loaded traffic data for {len(df)} schools")

    # Keep only traffic-specific columns
    traffic_cols = ['schulnummer', 'traffic_dtv_kfz', 'traffic_dtvw_kfz',
                    'traffic_dtv_rad', 'traffic_sv_anteil',
                    'traffic_sensor_distance_m', 'traffic_sensor_count',
                    'traffic_data_source']

    traffic_cols = [c for c in traffic_cols if c in df.columns]
    return df[traffic_cols] if traffic_cols else pd.DataFrame()


def load_crime_data() -> pd.DataFrame:
    """Load crime data from Phase 5."""
    logger.info("Loading crime data...")

    crime_path = INTERMEDIATE_DIR / "hamburg_schools_with_crime.csv"
    if not crime_path.exists():
        logger.warning(f"Crime data not found at {crime_path}")
        return pd.DataFrame()

    df = pd.read_csv(crime_path)
    logger.info(f"Loaded crime data for {len(df)} schools")

    # Keep only crime-specific columns
    crime_cols = ['schulnummer', 'crime_straftaten_2024', 'crime_straftaten_2023',
                  'crime_aufklaerungsquote_2024', 'crime_haeufigkeitszahl_2024',
                  'crime_trend_2024', 'crime_bezirk', 'crime_data_source']

    crime_cols = [c for c in crime_cols if c in df.columns]
    return df[crime_cols] if crime_cols else pd.DataFrame()


def load_poi_data() -> pd.DataFrame:
    """Load POI data from Phase 7."""
    logger.info("Loading POI data...")

    poi_path = INTERMEDIATE_DIR / "hamburg_schools_with_pois.csv"
    if not poi_path.exists():
        logger.warning(f"POI data not found at {poi_path}")
        return pd.DataFrame()

    df = pd.read_csv(poi_path)
    logger.info(f"Loaded POI data for {len(df)} schools")

    # Keep only POI-specific columns (they start with poi_ or specific category names)
    poi_cols = ['schulnummer'] + [c for c in df.columns if c.startswith('poi_') or
                any(c.startswith(cat) for cat in ['supermarket_', 'restaurant_', 'bakery_', 'kita_', 'primary_', 'secondary_'])]

    poi_cols = [c for c in poi_cols if c in df.columns]
    return df[poi_cols] if len(poi_cols) > 1 else pd.DataFrame()


def merge_abitur_data(schools_df: pd.DataFrame, abitur_df: pd.DataFrame) -> pd.DataFrame:
    """Merge Abitur data into school data using fuzzy matching."""
    if abitur_df.empty:
        logger.warning("No Abitur data to merge")
        return schools_df

    logger.info("Merging Abitur data...")

    # Create matching keys using core name extraction
    schools_df = schools_df.copy()
    abitur_df = abitur_df.copy()

    schools_df['_match_name'] = schools_df['schulname'].apply(extract_core_name)
    abitur_df['_match_name'] = abitur_df['schulname_abitur'].apply(extract_core_name)

    # Get Abitur columns to merge
    abitur_cols = ['_match_name'] + [c for c in abitur_df.columns if c.startswith('abitur_')]

    # First try exact match on core name
    merged = schools_df.merge(
        abitur_df[abitur_cols],
        on='_match_name',
        how='left'
    )

    # Count matches
    abitur_match_cols = [c for c in merged.columns if c.startswith('abitur_')]
    if abitur_match_cols:
        match_count = merged[abitur_match_cols[0]].notna().sum()
        logger.info(f"Matched Abitur data for {match_count}/{len(merged)} schools (exact core name match)")

    # If low match rate, try partial matching for remaining
    if abitur_match_cols and match_count < len(schools_df) * 0.3:
        logger.info("Attempting partial name matching for unmatched schools...")

        unmatched_mask = merged[abitur_match_cols[0]].isna()
        unmatched_schools = merged.loc[unmatched_mask, '_match_name'].tolist()

        for idx in merged[unmatched_mask].index:
            school_name = merged.at[idx, '_match_name']
            if not school_name:
                continue

            # Try to find a match in abitur data
            for _, abitur_row in abitur_df.iterrows():
                abitur_name = abitur_row['_match_name']
                if not abitur_name:
                    continue

                # Check if one contains the other
                if school_name in abitur_name or abitur_name in school_name:
                    for col in abitur_match_cols:
                        merged.at[idx, col] = abitur_row.get(col.replace('_match_name', ''), abitur_row.get(col))
                    break

                # Check for significant overlap
                school_words = set(school_name.split())
                abitur_words = set(abitur_name.split())
                if len(school_words & abitur_words) >= min(len(school_words), len(abitur_words)) * 0.7:
                    for col in abitur_match_cols:
                        merged.at[idx, col] = abitur_row.get(col.replace('_match_name', ''), abitur_row.get(col))
                    break

        # Recount
        final_match_count = merged[abitur_match_cols[0]].notna().sum()
        logger.info(f"Final Abitur match: {final_match_count}/{len(merged)} schools after fuzzy matching")

    # Drop temporary column
    merged = merged.drop(columns=['_match_name'], errors='ignore')

    return merged


def merge_transit_data(schools_df: pd.DataFrame, transit_df: pd.DataFrame) -> pd.DataFrame:
    """Merge transit data into school data."""
    if transit_df.empty:
        logger.warning("No transit data to merge")
        return schools_df

    logger.info("Merging transit data...")

    # Merge on schulnummer
    if 'schulnummer' not in transit_df.columns or 'schulnummer' not in schools_df.columns:
        logger.warning("Cannot merge transit data - no common key")
        return schools_df

    # Remove duplicate transit columns if they exist in base data
    transit_only_cols = [c for c in transit_df.columns if c not in schools_df.columns or c == 'schulnummer']

    merged = schools_df.merge(
        transit_df[transit_only_cols],
        on='schulnummer',
        how='left'
    )

    logger.info(f"Merged transit data for {len(merged)} schools")

    return merged


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and order."""
    logger.info("Standardizing column names and order...")

    # Define preferred column order
    preferred_order = [
        # Core identification
        'schulnummer', 'schulname', 'school_type', 'schulart',

        # Location
        'adresse_strasse_hausnr', 'adresse_ort', 'plz', 'stadtteil', 'bezirk',
        'latitude', 'longitude',

        # Contact
        'schul_telefonnr', 'schul_email', 'schul_homepage', 'fax',

        # Leadership
        'name_schulleiter', 'name_stellv_schulleiter',

        # Operator
        'traegerschaft', 'traeger_typ', 'rechtsform',

        # Student/Teacher data
        'schueler_gesamt', 'anzahl_schueler_gesamt', 'schueleranzahl_schuljahr',
        'teacher_count', 'teacher_count_source',

        # Academic performance
        'abitur_durchschnitt_2024', 'abitur_durchschnitt_2023', 'abitur_durchschnitt_2022',

        # Languages
        'fremdsprache', 'fremdsprache_mit_klasse', 'bilingual',

        # Programs
        'ganztagsform', 'oberstufenprofil', 'schulische_ausrichtung',

        # Transit
        'hvv_stops_500m', 'hvv_stops_1000m',
        'nearest_ubahn_name', 'nearest_ubahn_distance_m',
        'nearest_sbahn_name', 'nearest_sbahn_distance_m',
        'nearest_bus_name', 'nearest_bus_distance_m',
        'transit_accessibility_score',

        # Traffic
        'traffic_dtv_kfz', 'traffic_dtvw_kfz', 'traffic_dtv_rad',
        'traffic_sv_anteil', 'traffic_sensor_distance_m', 'traffic_sensor_count',

        # Crime
        'crime_straftaten_2024', 'crime_straftaten_2023',
        'crime_aufklaerungsquote_2024', 'crime_haeufigkeitszahl_2024',
        'crime_bezirk', 'crime_trend_2024',

        # Metadata
        'sozialindex', 'data_source', 'data_retrieved',
    ]

    # Get columns in preferred order
    ordered_cols = []
    for col in preferred_order:
        if col in df.columns:
            ordered_cols.append(col)

    # Add remaining columns
    remaining_cols = [c for c in df.columns if c not in ordered_cols]
    ordered_cols.extend(remaining_cols)

    return df[ordered_cols]


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate data."""
    logger.info("Cleaning data...")

    df = df.copy()

    # Remove duplicate rows based on schulnummer
    if 'schulnummer' in df.columns:
        original_len = len(df)
        df = df.drop_duplicates(subset=['schulnummer'], keep='first')
        if len(df) < original_len:
            logger.info(f"Removed {original_len - len(df)} duplicate schools")

    # Clean phone numbers
    if 'schul_telefonnr' in df.columns:
        df['schul_telefonnr'] = df['schul_telefonnr'].astype(str).str.strip()

    # Clean website URLs
    if 'schul_homepage' in df.columns:
        def clean_url(url):
            if pd.isna(url) or str(url).lower() in ['nan', 'none', '']:
                return None
            url = str(url).strip()
            if not url.startswith('http'):
                url = 'https://' + url
            return url
        df['schul_homepage'] = df['schul_homepage'].apply(clean_url)

    # Ensure numeric columns are numeric
    numeric_cols = ['latitude', 'longitude', 'schueler_gesamt',
                    'hvv_stops_500m', 'hvv_stops_1000m', 'transit_accessibility_score']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def save_outputs(df: pd.DataFrame):
    """Save the combined master table."""
    logger.info("Saving output files...")

    # Ensure final directory exists
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Save CSV
    csv_path = FINAL_DIR / "hamburg_school_master_table.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")

    # Save parquet
    parquet_path = FINAL_DIR / "hamburg_school_master_table.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    # Also save to intermediate for processing
    intermediate_path = INTERMEDIATE_DIR / "hamburg_combined_master.csv"
    df.to_csv(intermediate_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {intermediate_path}")


def print_summary(df: pd.DataFrame):
    """Print summary of combined data."""
    print("\n" + "="*70)
    print("HAMBURG SCHOOL DATA COMBINER - COMPLETE")
    print("="*70)

    print(f"\nTotal schools: {len(df)}")

    if 'school_type' in df.columns:
        print("\nSchools by type:")
        for t, count in df['school_type'].value_counts().items():
            print(f"  - {t}: {count}")

    # Data coverage
    print("\nData coverage:")

    coverage_cols = {
        'schulnummer': 'School ID',
        'schulname': 'School Name',
        'latitude': 'Coordinates',
        'schueler_gesamt': 'Student Count',
        'abitur_durchschnitt_2024': 'Abitur 2024',
        'abitur_durchschnitt_2023': 'Abitur 2023',
        'transit_accessibility_score': 'Transit Score',
        'teacher_count': 'Teacher Count',
        'traffic_dtv_kfz': 'Traffic (Kfz)',
        'crime_haeufigkeitszahl_2024': 'Crime Stats',
    }

    for col, label in coverage_cols.items():
        if col in df.columns:
            count = df[col].notna().sum()
            pct = 100 * count / len(df)
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    # Count POI columns
    poi_cols = [c for c in df.columns if c.startswith('poi_') or 'supermarket' in c.lower()]
    if poi_cols:
        print(f"  - POI columns: {len(poi_cols)}")

    print(f"\nTotal columns: {len(df.columns)}")
    print("\n" + "="*70)


def merge_generic_data(schools_df: pd.DataFrame, data_df: pd.DataFrame, data_name: str) -> pd.DataFrame:
    """Generic merge function for enrichment data on schulnummer."""
    if data_df.empty:
        logger.warning(f"No {data_name} data to merge")
        return schools_df

    logger.info(f"Merging {data_name} data...")

    if 'schulnummer' not in data_df.columns or 'schulnummer' not in schools_df.columns:
        logger.warning(f"Cannot merge {data_name} data - no schulnummer column")
        return schools_df

    # Get columns that don't already exist in schools_df
    new_cols = [c for c in data_df.columns if c not in schools_df.columns or c == 'schulnummer']

    merged = schools_df.merge(
        data_df[new_cols],
        on='schulnummer',
        how='left'
    )

    logger.info(f"Merged {data_name} data for {len(merged)} schools")
    return merged


def main():
    """Main function to combine all data."""
    logger.info("="*60)
    logger.info("Starting Hamburg School Data Combiner")
    logger.info("="*60)

    try:
        # Load all data sources
        schools_df = load_base_data()
        abitur_df = load_abitur_data()
        transit_df = load_transit_data()
        teacher_df = load_teacher_data()
        traffic_df = load_traffic_data()
        crime_df = load_crime_data()
        poi_df = load_poi_data()

        # Merge data (order matters - start with base data)
        combined_df = schools_df
        combined_df = merge_abitur_data(combined_df, abitur_df)
        combined_df = merge_transit_data(combined_df, transit_df)
        combined_df = merge_generic_data(combined_df, teacher_df, "teacher")
        combined_df = merge_generic_data(combined_df, traffic_df, "traffic")
        combined_df = merge_generic_data(combined_df, crime_df, "crime")
        combined_df = merge_generic_data(combined_df, poi_df, "POI")

        # Clean and standardize
        combined_df = clean_data(combined_df)
        combined_df = standardize_columns(combined_df)

        # Save outputs
        save_outputs(combined_df)

        # Print summary
        print_summary(combined_df)

        logger.info("Data combination complete!")
        return combined_df

    except Exception as e:
        logger.error(f"Data combination failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
