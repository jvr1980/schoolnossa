#!/usr/bin/env python3
"""
Hamburg Primary School HVV Transit Enrichment
Phase 6: Calculate transit accessibility for each primary school (Grundschule)

Data Source: HVV Haltestellen GeoJSON from Transparenzportal Hamburg
https://suche.transparenz.hamburg.de/dataset/einzugsbereiche-von-hvv-haltestellen-hamburg4

This script:
1. Downloads HVV transit stop data
2. Calculates distance from each primary school to nearest transit stops
3. Computes transit accessibility scores
4. Enriches the primary school master table with transit data

Author: Hamburg School Data Pipeline
Created: 2026-04-04
"""

import requests
import pandas as pd
import json
import zipfile
import io
import logging
from math import radians, sin, cos, sqrt, atan2
from pathlib import Path
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data source URLs
GEOJSON_URL = "http://archiv.transparenz.hamburg.de/hmbtgarchive/HMDK/hvv_einzugsbereiche_json_202292_snap_6.zip"
CSV_URL = "http://archiv.transparenz.hamburg.de/hmbtgarchive/HMDK/hvv_einzugsbereiche_csv_202291_snap_5.zip"

# Headers for requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg_primary"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

# Distance thresholds (meters)
RADIUS_500M = 500
RADIUS_1000M = 1000


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points on Earth (in meters)."""
    R = 6371000  # Earth's radius in meters

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    return R * c


def download_and_extract_all_geojson(url: str) -> dict:
    """Download ZIP file and extract all relevant GeoJSON transit data."""
    logger.info(f"Downloading transit data from {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content)} bytes")

        all_features = []

        # Extract from ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            file_list = zf.namelist()
            logger.info(f"ZIP contains {len(file_list)} files")

            # Target files - EPSG_4326 for WGS84 coordinates
            # Focus on the "haltestellenbereiche" (stop areas) files
            target_patterns = [
                'haltestellenbereiche_bahn_EPSG_4326.json',
                'haltestellenbereiche_bus_EPSG_4326.json',
                'haltestellenbereiche_faehre_EPSG_4326.json',
                'hvv_bahn_u_EPSG_4326.json',
                'hvv_bahn_s_EPSG_4326.json',
                'hvv_bus_metro_EPSG_4326.json',
            ]

            for filename in file_list:
                # Check if this is a target file
                should_process = any(pattern in filename for pattern in target_patterns)
                if not should_process:
                    continue

                logger.info(f"Extracting: {filename}")
                try:
                    with zf.open(filename) as f:
                        content = f.read()
                        try:
                            data = json.loads(content.decode('utf-8'))
                        except:
                            data = json.loads(content.decode('latin-1'))

                        if 'features' in data:
                            features = data['features']
                            # Tag features with their source type
                            source_type = 'Bus'
                            if 'bahn_u' in filename:
                                source_type = 'U-Bahn'
                            elif 'bahn_s' in filename:
                                source_type = 'S-Bahn'
                            elif 'bahn' in filename:
                                source_type = 'Bahn'
                            elif 'faehre' in filename:
                                source_type = 'Faehre'
                            elif 'metro' in filename:
                                source_type = 'Metrobus'

                            for f in features:
                                f['_source_type'] = source_type

                            all_features.extend(features)
                            logger.info(f"  Found {len(features)} features ({source_type})")
                except Exception as e:
                    logger.warning(f"Failed to parse {filename}: {e}")

        logger.info(f"Total features extracted: {len(all_features)}")
        return {'type': 'FeatureCollection', 'features': all_features}

    except requests.RequestException as e:
        logger.error(f"Failed to download transit data: {e}")
        raise


def download_and_extract_geojson(url: str) -> dict:
    """Wrapper for backward compatibility."""
    return download_and_extract_all_geojson(url)


def extract_transit_stops(geojson_data: dict) -> pd.DataFrame:
    """Extract transit stops from GeoJSON features."""
    logger.info("Extracting transit stops from GeoJSON...")

    stops = []

    for feature in geojson_data.get('features', []):
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})

        # Get source type if available
        source_type = feature.get('_source_type', None)

        # Extract stop information
        stop_name = None
        stop_type = None
        lines = None

        # Try different possible field names for name
        name_fields = ['name', 'haltestelle', 'stop_name', 'bezeichnung', 'haltst_name',
                       'haltestellen_name', 'station', 'stationsname']
        for name_field in name_fields:
            if name_field in properties and properties[name_field]:
                stop_name = properties[name_field]
                break

        # If still no name, try to construct from other fields
        if not stop_name:
            for key, value in properties.items():
                if isinstance(value, str) and len(value) > 3 and len(value) < 100:
                    if any(word in key.lower() for word in ['name', 'bezeich', 'station', 'halt']):
                        stop_name = value
                        break

        for type_field in ['type', 'verkehrsmittel', 'transit_type', 'art', 'haltst_typ']:
            if type_field in properties:
                stop_type = properties[type_field]
                break

        for lines_field in ['lines', 'linien', 'linie', 'route']:
            if lines_field in properties:
                lines = properties[lines_field]
                break

        # Extract coordinates
        lat, lon = None, None
        geom_type = geometry.get('type', '')

        if geom_type == 'Point':
            coords = geometry.get('coordinates', [])
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
        elif geom_type == 'Polygon':
            # Use centroid for polygon
            coords = geometry.get('coordinates', [[]])[0]
            if coords:
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                lon = sum(lons) / len(lons)
                lat = sum(lats) / len(lats)
        elif geom_type == 'MultiPolygon':
            # Use first polygon's centroid
            coords = geometry.get('coordinates', [[[]]]) [0][0]
            if coords:
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                lon = sum(lons) / len(lons)
                lat = sum(lats) / len(lats)

        # Accept stops even without name if we have coordinates
        if lat and lon:
            # Use source type if available, otherwise classify
            if source_type:
                transit_type = source_type
            else:
                transit_type = classify_transit_type(stop_name or "", stop_type, properties)

            # Generate a name if missing
            if not stop_name:
                stop_name = f"{transit_type} Stop ({lat:.4f}, {lon:.4f})"

            stops.append({
                'stop_name': stop_name,
                'stop_type': stop_type,
                'transit_type': transit_type,
                'lines': lines,
                'latitude': lat,
                'longitude': lon,
                'properties': properties
            })

    df = pd.DataFrame(stops)
    logger.info(f"Extracted {len(df)} transit stops")

    # Log breakdown by type
    if not df.empty and 'transit_type' in df.columns:
        for t, count in df['transit_type'].value_counts().items():
            logger.info(f"  - {t}: {count} stops")

    return df


def classify_transit_type(stop_name: str, stop_type: str, properties: dict) -> str:
    """Classify transit stop type (U-Bahn, S-Bahn, Bus, etc.)."""
    name_lower = str(stop_name).lower() if stop_name else ""
    type_lower = str(stop_type).lower() if stop_type else ""
    all_props = str(properties).lower()

    # Check for rail types
    if 'u-bahn' in name_lower or 'u-bahn' in type_lower or 'u-bahn' in all_props:
        return 'U-Bahn'
    elif 's-bahn' in name_lower or 's-bahn' in type_lower or 's-bahn' in all_props:
        return 'S-Bahn'
    elif 's+u' in name_lower or ('s ' in name_lower and 'u ' in name_lower):
        return 'S+U-Bahn'
    elif 'regio' in name_lower or 'regional' in type_lower:
        return 'Regionalbahn'
    elif 'bus' in type_lower or 'metrobus' in name_lower or 'schnellbus' in name_lower:
        return 'Bus'
    elif 'faehr' in name_lower or 'ferry' in type_lower:
        return 'Faehre'

    return 'Bus'  # Default to bus


def enrich_schools_with_transit(schools_df: pd.DataFrame, stops_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate transit metrics for each primary school."""
    logger.info("Calculating transit accessibility for primary schools...")

    if stops_df.empty:
        logger.warning("No transit stops to process")
        return schools_df

    schools_df = schools_df.copy()

    # Initialize new columns
    transit_columns = {
        'hvv_stops_500m': 0,
        'hvv_stops_1000m': 0,
        'nearest_ubahn_name': None,
        'nearest_ubahn_distance_m': None,
        'nearest_sbahn_name': None,
        'nearest_sbahn_distance_m': None,
        'nearest_bus_name': None,
        'nearest_bus_distance_m': None,
        'transit_accessibility_score': 0.0,
    }

    for col, default in transit_columns.items():
        schools_df[col] = default

    # Process each school
    total_schools = len(schools_df)
    for idx, school in schools_df.iterrows():
        if idx % 50 == 0:
            logger.info(f"Processing school {idx + 1}/{total_schools}")

        school_lat = school.get('latitude')
        school_lon = school.get('longitude')

        if pd.isna(school_lat) or pd.isna(school_lon):
            continue

        # Calculate distances to all stops
        stops_500m = 0
        stops_1000m = 0
        nearest_by_type = defaultdict(lambda: {'name': None, 'distance': float('inf')})

        for _, stop in stops_df.iterrows():
            stop_lat = stop.get('latitude')
            stop_lon = stop.get('longitude')

            if pd.isna(stop_lat) or pd.isna(stop_lon):
                continue

            distance = haversine_distance(school_lat, school_lon, stop_lat, stop_lon)

            if distance <= RADIUS_500M:
                stops_500m += 1
            if distance <= RADIUS_1000M:
                stops_1000m += 1

            # Track nearest by type
            transit_type = stop.get('transit_type', 'Bus')
            if distance < nearest_by_type[transit_type]['distance']:
                nearest_by_type[transit_type] = {
                    'name': stop.get('stop_name'),
                    'distance': distance
                }

        # Update school record
        schools_df.at[idx, 'hvv_stops_500m'] = stops_500m
        schools_df.at[idx, 'hvv_stops_1000m'] = stops_1000m

        # Set nearest stops by type
        for transit_type, data in nearest_by_type.items():
            if data['distance'] < float('inf'):
                if transit_type == 'U-Bahn':
                    schools_df.at[idx, 'nearest_ubahn_name'] = data['name']
                    schools_df.at[idx, 'nearest_ubahn_distance_m'] = round(data['distance'])
                elif transit_type == 'S-Bahn':
                    schools_df.at[idx, 'nearest_sbahn_name'] = data['name']
                    schools_df.at[idx, 'nearest_sbahn_distance_m'] = round(data['distance'])
                elif transit_type == 'Bus':
                    schools_df.at[idx, 'nearest_bus_name'] = data['name']
                    schools_df.at[idx, 'nearest_bus_distance_m'] = round(data['distance'])

        # Calculate accessibility score
        # Score based on: stops within 500m, rail access, etc.
        score = 0.0
        score += min(stops_500m * 5, 30)  # Up to 30 points for stops within 500m
        score += min(stops_1000m * 2, 20)  # Up to 20 points for stops within 1000m

        # Bonus for rail access
        ubahn_dist = schools_df.at[idx, 'nearest_ubahn_distance_m']
        sbahn_dist = schools_df.at[idx, 'nearest_sbahn_distance_m']

        if ubahn_dist and ubahn_dist < 500:
            score += 25
        elif ubahn_dist and ubahn_dist < 1000:
            score += 15

        if sbahn_dist and sbahn_dist < 500:
            score += 25
        elif sbahn_dist and sbahn_dist < 1000:
            score += 15

        schools_df.at[idx, 'transit_accessibility_score'] = min(score, 100)

    logger.info("Transit enrichment complete")
    return schools_df


def save_outputs(schools_df: pd.DataFrame, stops_df: pd.DataFrame):
    """Save output files."""
    logger.info("Saving output files...")

    # Save enriched schools
    schools_path = INTERMEDIATE_DIR / "hamburg_primary_schools_with_transit.csv"
    schools_df.to_csv(schools_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {schools_path}")

    # Save parquet
    parquet_path = INTERMEDIATE_DIR / "hamburg_primary_schools_with_transit.parquet"
    schools_df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    # Save stops for reference
    stops_path = RAW_DIR / "hamburg_hvv_stops.csv"
    # Remove complex properties column
    if 'properties' in stops_df.columns:
        stops_df = stops_df.drop(columns=['properties'])
    stops_df.to_csv(stops_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {stops_path}")


def print_summary(schools_df: pd.DataFrame):
    """Print summary statistics."""
    print("\n" + "="*70)
    print("HAMBURG PRIMARY SCHOOL HVV TRANSIT ENRICHMENT - COMPLETE")
    print("="*70)

    print(f"\nTotal primary schools processed: {len(schools_df)}")

    # Transit coverage
    if 'hvv_stops_500m' in schools_df.columns:
        avg_stops_500 = schools_df['hvv_stops_500m'].mean()
        avg_stops_1000 = schools_df['hvv_stops_1000m'].mean()
        print(f"\nAverage HVV stops within 500m: {avg_stops_500:.1f}")
        print(f"Average HVV stops within 1000m: {avg_stops_1000:.1f}")

    # Rail access
    if 'nearest_ubahn_distance_m' in schools_df.columns:
        ubahn_access = schools_df['nearest_ubahn_distance_m'].notna().sum()
        sbahn_access = schools_df['nearest_sbahn_distance_m'].notna().sum()
        print(f"\nSchools with U-Bahn data: {ubahn_access}")
        print(f"Schools with S-Bahn data: {sbahn_access}")

    # Accessibility score distribution
    if 'transit_accessibility_score' in schools_df.columns:
        print(f"\nTransit Accessibility Score:")
        print(f"  Mean: {schools_df['transit_accessibility_score'].mean():.1f}")
        print(f"  Median: {schools_df['transit_accessibility_score'].median():.1f}")
        print(f"  Max: {schools_df['transit_accessibility_score'].max():.1f}")

    print("\n" + "="*70)


def main():
    """Main function to run transit enrichment for primary schools."""
    logger.info("="*60)
    logger.info("Starting Hamburg Primary School HVV Transit Enrichment (Phase 6)")
    logger.info("="*60)

    try:
        # Ensure directories exist
        INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DIR.mkdir(parents=True, exist_ok=True)

        # Load schools data
        schools_path = RAW_DIR / "hamburg_grundschulen.csv"
        if not schools_path.exists():
            schools_path = RAW_DIR / "hamburg_grundschulen_raw.csv"

        if not schools_path.exists():
            logger.error("No primary school data found. Run Phase 1 first.")
            return None

        schools_df = pd.read_csv(schools_path)
        logger.info(f"Loaded {len(schools_df)} primary schools from {schools_path}")

        # Download and process transit data
        geojson_data = download_and_extract_geojson(GEOJSON_URL)
        stops_df = extract_transit_stops(geojson_data)

        # Enrich schools with transit data
        enriched_df = enrich_schools_with_transit(schools_df, stops_df)

        # Save outputs
        save_outputs(enriched_df, stops_df)

        # Print summary
        print_summary(enriched_df)

        logger.info("Phase 6 complete!")
        return enriched_df

    except Exception as e:
        logger.error(f"Transit enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
