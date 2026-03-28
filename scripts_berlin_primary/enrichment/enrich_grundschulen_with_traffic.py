#!/usr/bin/env python3
"""
Enriches Berlin primary school (Grundschule) data with traffic metrics from Berlin traffic sensors.

This script:
1. Geocodes school addresses to get lat/lon coordinates
2. Extracts traffic sensor locations from GeoJSON metadata
3. Matches schools to nearest sensors within a configurable radius
4. Aggregates traffic metrics from sensor data
5. Merges traffic data into the school table

Data sources (shared with secondary school data):
- Telraam: Vehicle traffic (cars, bikes, pedestrians, heavy vehicles, speed)
- Ecocounter: Bicycle and pedestrian counts

Note: Uses the same traffic data as secondary schools but separate output paths.
"""

import pandas as pd
import json
import os
import math
import time
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Install with: pip install tqdm")

# Load environment variables
load_dotenv()

# File paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Primary school data paths
DATA_DIR = PROJECT_ROOT / "data_berlin_primary"
SCHOOLS_FILE = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.csv"
OUTPUT_CSV = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.csv"
OUTPUT_XLSX = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.xlsx"
GEOCODED_CACHE = DATA_DIR / "intermediate" / "geocoded_grundschulen_cache.json"

# Traffic data paths (shared with secondary schools)
TRAFFIC_DATA_DIR = PROJECT_ROOT / "data_berlin" / "raw" / "traffic_data"
TELRAAM_GEOJSON = TRAFFIC_DATA_DIR / "metadata" / "bzm_telraam_segments.geojson"
ECOCOUNTER_GEOJSON = TRAFFIC_DATA_DIR / "metadata" / "bzm_ecocounter_segments.geojson"
TELRAAM_DATA_DIR = TRAFFIC_DATA_DIR / "telraam"
ECOCOUNTER_DATA_DIR = TRAFFIC_DATA_DIR / "ecocounter"

# Configuration
GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
SEARCH_RADIUS_METERS = 500  # Search radius for nearby sensors
GEOCODE_DELAY = 0.1  # Delay between geocoding requests (seconds)


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points on earth (in meters).
    """
    R = 6371000  # Earth's radius in meters

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


def geocode_address_google(address, api_key):
    """
    Geocode an address using Google Geocoding API.
    Returns (latitude, longitude) or (None, None) if failed.
    """
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": api_key,
        "region": "de"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data["status"] == "OK" and data["results"]:
            location = data["results"][0]["geometry"]["location"]
            return location["lat"], location["lng"]
        else:
            return None, None
    except Exception as e:
        print(f"  Geocoding error: {e}")
        return None, None


def geocode_address_nominatim(address):
    """
    Geocode an address using Nominatim (free, no API key required).
    Returns (latitude, longitude) or (None, None) if failed.
    """
    url = "https://nominatim.openstreetmap.org/search"
    headers = {
        "User-Agent": "SchoolNossa/1.0 (primary school data enrichment project)"
    }
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "de"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
        else:
            return None, None
    except Exception as e:
        print(f"  Geocoding error: {e}")
        return None, None


def load_geocode_cache():
    """Load cached geocoding results."""
    if GEOCODED_CACHE.exists():
        with open(GEOCODED_CACHE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_geocode_cache(cache):
    """Save geocoding results to cache."""
    GEOCODED_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(GEOCODED_CACHE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def geocode_schools(df):
    """
    Add latitude and longitude columns to schools dataframe.
    Uses Google Geocoding API with Nominatim as fallback.
    """
    print("\n" + "="*70)
    print("GEOCODING GRUNDSCHULE ADDRESSES")
    print("="*70)

    # Load cache
    cache = load_geocode_cache()
    print(f"Loaded {len(cache)} cached geocoding results")

    # Initialize columns
    if 'latitude' not in df.columns:
        df['latitude'] = None
    if 'longitude' not in df.columns:
        df['longitude'] = None

    # Count schools needing geocoding
    to_geocode = []
    for idx, row in df.iterrows():
        schulnummer = str(row['schulnummer'])
        if schulnummer in cache:
            df.at[idx, 'latitude'] = cache[schulnummer].get('lat')
            df.at[idx, 'longitude'] = cache[schulnummer].get('lon')
        elif pd.notna(row.get('strasse')) and pd.notna(row.get('plz')):
            to_geocode.append((idx, row))

    print(f"Schools to geocode: {len(to_geocode)}")
    print(f"Schools already cached: {len(cache)}")

    if not to_geocode:
        print("All schools already geocoded!")
        return df

    # Check for API key
    use_google = GOOGLE_API_KEY and len(GOOGLE_API_KEY) > 10
    if use_google:
        print(f"Using Google Geocoding API")
    else:
        print("Using Nominatim (free, slower)")

    # Geocode schools
    geocoded_count = 0
    failed_count = 0

    iterator = tqdm(to_geocode, desc="Geocoding") if TQDM_AVAILABLE else to_geocode

    for idx, row in iterator:
        schulnummer = str(row['schulnummer'])
        strasse = row['strasse']
        plz = str(row['plz']).split('.')[0] if pd.notna(row['plz']) else ''

        # Build address
        address = f"{strasse}, {plz} Berlin, Germany"

        # Try geocoding
        if use_google:
            lat, lon = geocode_address_google(address, GOOGLE_API_KEY)
        else:
            lat, lon = geocode_address_nominatim(address)
            time.sleep(1)  # Nominatim rate limit: 1 request/second

        if lat and lon:
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lon
            cache[schulnummer] = {'lat': lat, 'lon': lon, 'address': address}
            geocoded_count += 1
        else:
            failed_count += 1
            if not TQDM_AVAILABLE:
                print(f"  Failed: {row['schulname'][:40]} - {address}")

        time.sleep(GEOCODE_DELAY)

        # Save cache periodically
        if geocoded_count % 20 == 0:
            save_geocode_cache(cache)

    # Final cache save
    save_geocode_cache(cache)

    print(f"\nGeocoding complete:")
    print(f"  - Successfully geocoded: {geocoded_count}")
    print(f"  - Failed: {failed_count}")
    print(f"  - Total with coordinates: {df['latitude'].notna().sum()}")

    return df


def extract_sensor_locations():
    """
    Extract sensor locations from GeoJSON metadata files.
    Returns a DataFrame with sensor information.
    """
    print("\n" + "="*70)
    print("EXTRACTING TRAFFIC SENSOR LOCATIONS")
    print("="*70)

    sensors = []

    # Parse Telraam GeoJSON
    if TELRAAM_GEOJSON.exists():
        with open(TELRAAM_GEOJSON, 'r', encoding='utf-8') as f:
            telraam_data = json.load(f)

        for feature in telraam_data.get('features', []):
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})

            segment_id = props.get('segment_id')

            # Get coordinates from geometry
            coords = geom.get('coordinates')
            if geom.get('type') == 'MultiLineString' and coords:
                # Get midpoint of first line segment
                first_line = coords[0]
                if first_line:
                    mid_idx = len(first_line) // 2
                    lon, lat = first_line[mid_idx]
            elif geom.get('type') == 'Point' and coords:
                lon, lat = coords
            else:
                continue

            # Get OSM address info
            osm = props.get('osm', {})
            address = osm.get('address', {})

            sensors.append({
                'segment_id': segment_id,
                'latitude': lat,
                'longitude': lon,
                'street_name': osm.get('name', 'Unknown'),
                'postcode': address.get('postcode'),
                'borough': address.get('borough'),
                'counter_type': 'telraam'
            })

        print(f"  - Telraam sensors: {len([s for s in sensors if s['counter_type'] == 'telraam'])}")
    else:
        print(f"  - Telraam GeoJSON not found: {TELRAAM_GEOJSON}")

    # Parse Ecocounter GeoJSON
    if ECOCOUNTER_GEOJSON.exists():
        with open(ECOCOUNTER_GEOJSON, 'r', encoding='utf-8') as f:
            ecocounter_data = json.load(f)

        eco_count = 0
        for feature in ecocounter_data.get('features', []):
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})

            segment_id = props.get('segment_id')

            # Get coordinates
            coords = geom.get('coordinates')
            if coords and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
            else:
                continue

            # Get counter info
            counter = props.get('counter', [{}])[0] if props.get('counter') else {}
            eco_counter = props.get('eco-counter', {})

            sensors.append({
                'segment_id': segment_id,
                'latitude': lat,
                'longitude': lon,
                'street_name': eco_counter.get('nom', counter.get('description', 'Unknown')),
                'postcode': None,  # Ecocounter doesn't have postcode in metadata
                'borough': None,
                'counter_type': 'ecocounter'
            })
            eco_count += 1

        print(f"  - Ecocounter sensors: {eco_count}")
    else:
        print(f"  - Ecocounter GeoJSON not found: {ECOCOUNTER_GEOJSON}")

    df_sensors = pd.DataFrame(sensors)
    print(f"  - Total sensors: {len(df_sensors)}")

    return df_sensors


def find_nearest_sensors(df_schools, df_sensors, radius_meters=SEARCH_RADIUS_METERS):
    """
    For each school, find all sensors within the specified radius.
    Returns a dictionary mapping schulnummer to list of nearby sensors with distances.
    """
    print("\n" + "="*70)
    print(f"MATCHING SCHOOLS TO SENSORS (radius: {radius_meters}m)")
    print("="*70)

    school_sensors = {}

    # Only process schools with coordinates
    schools_with_coords = df_schools[df_schools['latitude'].notna()].copy()
    print(f"Schools with coordinates: {len(schools_with_coords)}")

    if len(df_sensors) == 0:
        print("No sensors found - skipping matching")
        for idx, school in df_schools.iterrows():
            school_sensors[str(school['schulnummer'])] = []
        return school_sensors

    iterator = tqdm(schools_with_coords.iterrows(), total=len(schools_with_coords), desc="Matching") if TQDM_AVAILABLE else schools_with_coords.iterrows()

    for idx, school in iterator:
        schulnummer = str(school['schulnummer'])
        school_lat = school['latitude']
        school_lon = school['longitude']

        nearby = []

        for _, sensor in df_sensors.iterrows():
            distance = haversine_distance(
                school_lat, school_lon,
                sensor['latitude'], sensor['longitude']
            )

            if distance <= radius_meters:
                nearby.append({
                    'segment_id': sensor['segment_id'],
                    'distance_m': round(distance, 1),
                    'counter_type': sensor['counter_type'],
                    'street_name': sensor['street_name']
                })

        # Sort by distance
        nearby.sort(key=lambda x: x['distance_m'])
        school_sensors[schulnummer] = nearby

    # Statistics
    schools_with_sensors = sum(1 for v in school_sensors.values() if v)
    total_matches = sum(len(v) for v in school_sensors.values())

    print(f"\nMatching results:")
    if len(schools_with_coords) > 0:
        print(f"  - Schools with nearby sensors: {schools_with_sensors} / {len(schools_with_coords)} ({100*schools_with_sensors/len(schools_with_coords):.1f}%)")
    print(f"  - Total sensor matches: {total_matches}")
    if schools_with_sensors > 0:
        print(f"  - Avg sensors per matched school: {total_matches/schools_with_sensors:.1f}")

    return school_sensors


def load_traffic_data(year=2024):
    """
    Load and aggregate traffic data from CSV files for the specified year.
    Returns a dictionary mapping segment_id to aggregated metrics.
    Note: Files have .gz extension but are actually plain CSV files.
    """
    print("\n" + "="*70)
    print(f"LOADING TRAFFIC DATA (year: {year})")
    print("="*70)

    traffic_metrics = {}

    # Load Telraam data
    telraam_files = []
    if TELRAAM_DATA_DIR.exists():
        for filename in os.listdir(TELRAAM_DATA_DIR):
            if f"_{year}_" in filename:
                telraam_files.append(TELRAAM_DATA_DIR / filename)

    print(f"Telraam files for {year}: {len(telraam_files)}")

    if telraam_files:
        telraam_data = []
        for filepath in telraam_files:
            try:
                # Files have .gz extension but are actually plain CSV
                df = pd.read_csv(filepath, compression=None)
                telraam_data.append(df)
            except Exception as e:
                print(f"  Error reading {filepath}: {e}")

        if telraam_data:
            df_telraam = pd.concat(telraam_data, ignore_index=True)
            print(f"  - Telraam records: {len(df_telraam):,}")

            # Aggregate by segment
            agg = df_telraam.groupby('segment_id').agg({
                'car_total': 'mean',
                'bike_total': 'mean',
                'ped_total': 'mean',
                'heavy_total': 'mean',
                'v85': 'mean'
            }).reset_index()

            for _, row in agg.iterrows():
                segment_id = row['segment_id']
                traffic_metrics[segment_id] = {
                    'avg_cars_per_hour': round(row['car_total'], 1) if pd.notna(row['car_total']) else None,
                    'avg_bikes_per_hour': round(row['bike_total'], 1) if pd.notna(row['bike_total']) else None,
                    'avg_pedestrians_per_hour': round(row['ped_total'], 1) if pd.notna(row['ped_total']) else None,
                    'avg_heavy_per_hour': round(row['heavy_total'], 1) if pd.notna(row['heavy_total']) else None,
                    'v85_speed': round(row['v85'], 1) if pd.notna(row['v85']) else None,
                    'source': 'telraam'
                }

    # Load Ecocounter data (use most recent available year if 2024 not available)
    ecocounter_files = []
    if ECOCOUNTER_DATA_DIR.exists():
        for year_check in [year, 2022, 2021, 2020]:
            for filename in os.listdir(ECOCOUNTER_DATA_DIR):
                if f"_{year_check}_" in filename:
                    ecocounter_files.append(ECOCOUNTER_DATA_DIR / filename)
            if ecocounter_files:
                print(f"Ecocounter files for {year_check}: {len(ecocounter_files)}")
                break

    if ecocounter_files:
        ecocounter_data = []
        for filepath in ecocounter_files:
            try:
                # Files have .gz extension but are actually plain CSV
                df = pd.read_csv(filepath, compression=None)
                ecocounter_data.append(df)
            except Exception as e:
                print(f"  Error reading {filepath}: {e}")

        if ecocounter_data:
            df_eco = pd.concat(ecocounter_data, ignore_index=True)
            print(f"  - Ecocounter records: {len(df_eco):,}")

            # Aggregate by segment
            agg = df_eco.groupby('segment_id').agg({
                'bike_total': 'mean',
                'ped_total': 'mean' if 'ped_total' in df_eco.columns else lambda x: None
            }).reset_index()

            for _, row in agg.iterrows():
                segment_id = row['segment_id']
                if segment_id not in traffic_metrics:
                    traffic_metrics[segment_id] = {
                        'avg_cars_per_hour': None,
                        'avg_bikes_per_hour': round(row['bike_total'], 1) if pd.notna(row['bike_total']) else None,
                        'avg_pedestrians_per_hour': None,
                        'avg_heavy_per_hour': None,
                        'v85_speed': None,
                        'source': 'ecocounter'
                    }
                else:
                    # Ecocounter has better bike data, update if available
                    if pd.notna(row['bike_total']):
                        traffic_metrics[segment_id]['avg_bikes_per_hour'] = round(row['bike_total'], 1)

    print(f"  - Total segments with data: {len(traffic_metrics)}")

    return traffic_metrics


def aggregate_school_traffic(school_sensors, traffic_metrics):
    """
    Aggregate traffic metrics for each school based on nearby sensors.
    Returns a dictionary mapping schulnummer to aggregated traffic data.
    """
    print("\n" + "="*70)
    print("AGGREGATING TRAFFIC METRICS FOR SCHOOLS")
    print("="*70)

    school_traffic = {}

    for schulnummer, sensors in school_sensors.items():
        if not sensors:
            school_traffic[schulnummer] = {
                'traffic_avg_cars_per_hour': None,
                'traffic_avg_bikes_per_hour': None,
                'traffic_avg_pedestrians_per_hour': None,
                'traffic_v85_speed': None,
                'traffic_sensor_distance_m': None,
                'traffic_sensor_count': 0,
                'traffic_data_source': None
            }
            continue

        # Collect metrics from nearby sensors
        cars = []
        bikes = []
        peds = []
        speeds = []
        sources = set()

        for sensor in sensors:
            segment_id = sensor['segment_id']
            if segment_id in traffic_metrics:
                metrics = traffic_metrics[segment_id]
                sources.add(metrics.get('source', 'unknown'))

                if metrics.get('avg_cars_per_hour') is not None:
                    cars.append(metrics['avg_cars_per_hour'])
                if metrics.get('avg_bikes_per_hour') is not None:
                    bikes.append(metrics['avg_bikes_per_hour'])
                if metrics.get('avg_pedestrians_per_hour') is not None:
                    peds.append(metrics['avg_pedestrians_per_hour'])
                if metrics.get('v85_speed') is not None:
                    speeds.append(metrics['v85_speed'])

        # Calculate averages
        school_traffic[schulnummer] = {
            'traffic_avg_cars_per_hour': round(sum(cars)/len(cars), 1) if cars else None,
            'traffic_avg_bikes_per_hour': round(sum(bikes)/len(bikes), 1) if bikes else None,
            'traffic_avg_pedestrians_per_hour': round(sum(peds)/len(peds), 1) if peds else None,
            'traffic_v85_speed': round(sum(speeds)/len(speeds), 1) if speeds else None,
            'traffic_sensor_distance_m': sensors[0]['distance_m'],  # Nearest sensor
            'traffic_sensor_count': len(sensors),
            'traffic_data_source': '/'.join(sorted(sources)) if sources else None
        }

    # Statistics
    with_cars = sum(1 for v in school_traffic.values() if v['traffic_avg_cars_per_hour'] is not None)
    with_bikes = sum(1 for v in school_traffic.values() if v['traffic_avg_bikes_per_hour'] is not None)

    print(f"Schools with car traffic data: {with_cars}")
    print(f"Schools with bike traffic data: {with_bikes}")

    return school_traffic


def merge_traffic_into_schools(df_schools, school_traffic):
    """
    Merge aggregated traffic data into the schools dataframe.
    """
    print("\n" + "="*70)
    print("MERGING TRAFFIC DATA INTO SCHOOL TABLE")
    print("="*70)

    # Add traffic columns
    traffic_columns = [
        'traffic_avg_cars_per_hour',
        'traffic_avg_bikes_per_hour',
        'traffic_avg_pedestrians_per_hour',
        'traffic_v85_speed',
        'traffic_sensor_distance_m',
        'traffic_sensor_count',
        'traffic_data_source'
    ]

    for col in traffic_columns:
        df_schools[col] = None

    # Merge data
    for idx, row in df_schools.iterrows():
        schulnummer = str(row['schulnummer'])
        if schulnummer in school_traffic:
            traffic = school_traffic[schulnummer]
            for col in traffic_columns:
                df_schools.at[idx, col] = traffic.get(col)

    return df_schools


def main():
    """Main function to enrich primary schools with traffic data."""
    print("="*70)
    print("ENRICHING GRUNDSCHULE DATA WITH TRAFFIC METRICS")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check if input file exists
    if not SCHOOLS_FILE.exists():
        print(f"\nError: Input file not found: {SCHOOLS_FILE}")
        print("Please run the combiner script first: combine_grundschulen_and_metadata.py")
        return

    # Load schools
    print("\nLoading school data...")
    df_schools = pd.read_csv(SCHOOLS_FILE)
    print(f"Loaded {len(df_schools)} Grundschulen")

    # Step 1: Geocode schools
    df_schools = geocode_schools(df_schools)

    # Step 2: Extract sensor locations
    df_sensors = extract_sensor_locations()

    # Step 3: Match schools to sensors
    school_sensors = find_nearest_sensors(df_schools, df_sensors)

    # Step 4: Load traffic data
    traffic_metrics = load_traffic_data(year=2024)

    # Step 5: Aggregate traffic for schools
    school_traffic = aggregate_school_traffic(school_sensors, traffic_metrics)

    # Step 6: Merge into school table
    df_schools = merge_traffic_into_schools(df_schools, school_traffic)

    # Ensure output directory exists
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Save results
    print("\n" + "="*70)
    print("SAVING RESULTS")
    print("="*70)

    df_schools.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Saved: {OUTPUT_CSV}")

    df_schools.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"Saved: {OUTPUT_XLSX}")

    # Summary
    print("\n" + "="*70)
    print("ENRICHMENT SUMMARY")
    print("="*70)

    print(f"\nTotal Grundschulen: {len(df_schools)}")
    print(f"\nGeocoding coverage:")
    print(f"  - Schools with coordinates: {df_schools['latitude'].notna().sum()} ({100*df_schools['latitude'].notna().sum()/len(df_schools):.1f}%)")

    print(f"\nTraffic data coverage:")
    print(f"  - Schools with nearby sensors: {(df_schools['traffic_sensor_count'] > 0).sum()}")
    print(f"  - Schools with car traffic data: {df_schools['traffic_avg_cars_per_hour'].notna().sum()}")
    print(f"  - Schools with bike traffic data: {df_schools['traffic_avg_bikes_per_hour'].notna().sum()}")

    if df_schools['traffic_sensor_distance_m'].notna().any():
        print(f"\nSensor distances:")
        print(f"  - Min distance: {df_schools['traffic_sensor_distance_m'].min():.0f}m")
        print(f"  - Median distance: {df_schools['traffic_sensor_distance_m'].median():.0f}m")
        print(f"  - Max distance: {df_schools['traffic_sensor_distance_m'].max():.0f}m")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
