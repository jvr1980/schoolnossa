#!/usr/bin/env python3
"""
Hamburg Traffic Data Enrichment
Enriches school data with traffic metrics from Hamburg's SensorThings API.

This script:
1. Loads school data with coordinates
2. Fetches traffic counting station locations from Hamburg IoT API
3. Downloads traffic volume data (Kfz - motor vehicles, Rad - bicycles)
4. Matches schools to nearest sensors within configurable radius
5. Aggregates traffic metrics and merges into school table

Data sources:
- Hamburg SensorThings API: https://iot.hamburg.de/v1.1/
- Verkehrsdaten Kfz (Infrarotdetektoren): Motor vehicle counts
- Verkehrsdaten Rad (Infrarotdetektoren): Bicycle counts
- WFS Verkehrsstärken: Official DTV (daily traffic volume) values

Author: Hamburg School Data Pipeline
Created: 2026-02-03
"""

import pandas as pd
import numpy as np
import requests
import json
import logging
import math
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

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
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"
CACHE_DIR = DATA_DIR / "cache"

# API endpoints
SENSORTHINGS_BASE = "https://iot.hamburg.de/v1.1"
WFS_VERKEHRSSTAERKEN = "https://geodienste.hamburg.de/HH_WFS_Verkehrsstaerken"
OGC_API_VERKEHRSSTAERKEN = "https://api.hamburg.de/datasets/v1/verkehrsstaerken"

# Configuration
SEARCH_RADIUS_M = 1000  # Search radius for nearby sensors (meters) - increased for better coverage
MAX_WORKERS = 5  # Concurrent API requests
REQUEST_DELAY = 0.2  # Delay between requests (seconds)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points (in meters)."""
    R = 6371000  # Earth's radius in meters

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


class HamburgTrafficEnrichment:
    """Enriches Hamburg school data with traffic metrics."""

    def __init__(self):
        self.sensors_cache: Dict = {}
        self.traffic_data_cache: Dict = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SchoolNossa/1.0 (Hamburg school data enrichment)',
            'Accept': 'application/json'
        })

        # Ensure cache directory exists
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def load_schools(self) -> pd.DataFrame:
        """Load school data with coordinates."""
        logger.info("Loading school data...")

        # Try different possible input files
        possible_files = [
            FINAL_DIR / "hamburg_school_master_table.parquet",
            FINAL_DIR / "hamburg_school_master_table.csv",
            INTERMEDIATE_DIR / "hamburg_schools_with_transit.csv",
            DATA_DIR / "raw" / "hamburg_secondary_schools.csv"
        ]

        for filepath in possible_files:
            if filepath.exists():
                if filepath.suffix == '.parquet':
                    df = pd.read_parquet(filepath)
                else:
                    df = pd.read_csv(filepath)
                logger.info(f"Loaded {len(df)} schools from {filepath.name}")
                return df

        raise FileNotFoundError(f"No school data found in {DATA_DIR}")

    def fetch_sensor_locations_sta(self) -> List[Dict]:
        """Fetch traffic sensor locations from SensorThings API."""
        logger.info("Fetching sensor locations from SensorThings API...")

        sensors = []

        # Fetch Kfz (motor vehicle) counting stations
        kfz_url = f"{SENSORTHINGS_BASE}/Datastreams"
        params = {
            "$filter": "properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung' and properties/layerName eq 'Anzahl_Kfz_Zaehlstelle_1-Tag'",
            "$expand": "Thing/Locations,ObservedProperty",
            "$top": 1000
        }

        try:
            response = self.session.get(kfz_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            for item in data.get('value', []):
                thing = item.get('Thing', {})
                locations = thing.get('Locations', [])

                if locations:
                    loc = locations[0]
                    coords = loc.get('location', {}).get('coordinates', [])

                    if len(coords) >= 2:
                        sensors.append({
                            'sensor_id': item.get('@iot.id'),
                            'datastream_id': item.get('@iot.id'),
                            'name': thing.get('name', 'Unknown'),
                            'description': thing.get('description', ''),
                            'longitude': coords[0],
                            'latitude': coords[1],
                            'sensor_type': 'kfz',
                            'observed_property': item.get('ObservedProperty', {}).get('name', 'Kfz count')
                        })

            logger.info(f"Found {len(sensors)} Kfz counting stations")

        except Exception as e:
            logger.warning(f"Error fetching Kfz sensors: {e}")

        # Fetch Rad (bicycle) counting stations
        rad_url = f"{SENSORTHINGS_BASE}/Datastreams"
        params = {
            "$filter": "properties/serviceName eq 'HH_STA_AutomatisierteVerkehrsmengenerfassung' and properties/layerName eq 'Anzahl_Rad_Zaehlstelle_1-Tag'",
            "$expand": "Thing/Locations,ObservedProperty",
            "$top": 1000
        }

        try:
            response = self.session.get(rad_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            rad_count = 0
            for item in data.get('value', []):
                thing = item.get('Thing', {})
                locations = thing.get('Locations', [])

                if locations:
                    loc = locations[0]
                    coords = loc.get('location', {}).get('coordinates', [])

                    if len(coords) >= 2:
                        sensors.append({
                            'sensor_id': f"rad_{item.get('@iot.id')}",
                            'datastream_id': item.get('@iot.id'),
                            'name': thing.get('name', 'Unknown'),
                            'description': thing.get('description', ''),
                            'longitude': coords[0],
                            'latitude': coords[1],
                            'sensor_type': 'rad',
                            'observed_property': item.get('ObservedProperty', {}).get('name', 'Bicycle count')
                        })
                        rad_count += 1

            logger.info(f"Found {rad_count} bicycle counting stations")

        except Exception as e:
            logger.warning(f"Error fetching bicycle sensors: {e}")

        return sensors

    def fetch_sensor_locations_ogc_api(self) -> List[Dict]:
        """Fetch traffic sensor locations from OGC API Features (official DTV values)."""
        logger.info("Fetching sensor locations from OGC API Features...")

        sensors = []

        # Fetch Kfz DTV/DTVw values - paginate to get all
        kfz_url = f"{OGC_API_VERKEHRSSTAERKEN}/collections/verkehrsstaerken_dtv_dtvw/items"
        offset = 0
        limit = 100

        try:
            while True:
                params = {'f': 'json', 'limit': limit, 'offset': offset}
                response = self.session.get(kfz_url, params=params, timeout=120)
                response.raise_for_status()
                data = response.json()

                features = data.get('features', [])
                if not features:
                    break

                for feature in features:
                    props = feature.get('properties', {})
                    geom = feature.get('geometry', {})
                    coords = geom.get('coordinates', [])

                    if coords and len(coords) >= 2:
                        lon, lat = coords[0], coords[1]

                        # Get the most recent DTV values (2024, fall back to earlier years)
                        dtv = None
                        dtvw = None
                        sv_anteil = None
                        jahr = None

                        for year in [2024, 2023, 2022, 2021, 2020]:
                            if props.get(f'dtv_{year}') is not None:
                                dtv = props.get(f'dtv_{year}')
                                dtvw = props.get(f'dtvw_{year}')
                                sv_anteil = props.get(f'sv_am_dtvw_{year}')
                                jahr = year
                                break

                        if dtv is not None:
                            sensors.append({
                                'sensor_id': f"ogc_{props.get('zaehlstelle', feature.get('id', ''))}",
                                'datastream_id': None,
                                'name': props.get('bezeichnung', 'Unknown'),
                                'description': f"Zählstelle {props.get('zaehlstelle', '')}",
                                'longitude': lon,
                                'latitude': lat,
                                'sensor_type': 'ogc_kfz',
                                'dtv': dtv,
                                'dtvw': dtvw,
                                'sv_anteil': sv_anteil,
                                'jahr': jahr,
                                'observed_property': 'DTV Kfz'
                            })

                offset += limit
                if len(features) < limit:
                    break

            logger.info(f"Found {len(sensors)} OGC API Kfz traffic counting points")

        except Exception as e:
            logger.warning(f"Error fetching OGC API Kfz data: {e}")

        # Fetch bicycle DTV values
        rad_url = f"{OGC_API_VERKEHRSSTAERKEN}/collections/radverkehr_dtv_dtvw/items"
        offset = 0
        rad_count = 0

        try:
            while True:
                params = {'f': 'json', 'limit': limit, 'offset': offset}
                response = self.session.get(rad_url, params=params, timeout=120)
                response.raise_for_status()
                data = response.json()

                features = data.get('features', [])
                if not features:
                    break

                for feature in features:
                    props = feature.get('properties', {})
                    geom = feature.get('geometry', {})
                    coords = geom.get('coordinates', [])

                    if coords and len(coords) >= 2:
                        lon, lat = coords[0], coords[1]

                        # Get the most recent DTV values
                        dtv = None
                        dtvw = None
                        jahr = None

                        for year in [2024, 2023, 2022, 2021, 2020]:
                            if props.get(f'dtv_{year}') is not None:
                                dtv = props.get(f'dtv_{year}')
                                dtvw = props.get(f'dtvw_{year}')
                                jahr = year
                                break

                        if dtv is not None:
                            sensors.append({
                                'sensor_id': f"ogc_rad_{props.get('zaehlstelle', rad_count)}",
                                'datastream_id': None,
                                'name': props.get('bezeichnung', 'Unknown'),
                                'description': f"Rad-Zählstelle {props.get('zaehlstelle', '')}",
                                'longitude': lon,
                                'latitude': lat,
                                'sensor_type': 'ogc_rad',
                                'dtv': dtv,
                                'dtvw': dtvw,
                                'jahr': jahr,
                                'observed_property': 'DTV Rad'
                            })
                            rad_count += 1

                offset += limit
                if len(features) < limit:
                    break

            logger.info(f"Found {rad_count} OGC API bicycle counting points")

        except Exception as e:
            logger.warning(f"Error fetching WFS bicycle data: {e}")

        return sensors

    def fetch_observations(self, datastream_id: int, days_back: int = 365) -> List[Dict]:
        """Fetch observations for a specific datastream from SensorThings API."""
        observations = []

        # Calculate time filter
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        url = f"{SENSORTHINGS_BASE}/Datastreams({datastream_id})/Observations"
        params = {
            "$filter": f"phenomenonTime ge {start_time.isoformat()}Z",
            "$orderby": "phenomenonTime desc",
            "$top": 365  # Max one year of daily data
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            for obs in data.get('value', []):
                result = obs.get('result')
                if result is not None:
                    observations.append({
                        'time': obs.get('phenomenonTime'),
                        'value': result
                    })

        except Exception as e:
            logger.debug(f"Error fetching observations for datastream {datastream_id}: {e}")

        return observations

    def get_all_sensors(self) -> pd.DataFrame:
        """Get all traffic sensors from OGC API Features (primary source)."""
        cache_file = CACHE_DIR / "hamburg_traffic_sensors.json"

        # Check cache (valid for 24 hours)
        if cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                logger.info("Loading sensors from cache...")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    sensors = json.load(f)
                logger.info(f"Loaded {len(sensors)} sensors from cache")
                return pd.DataFrame(sensors)

        # Fetch from OGC API Features (preferred - has JSON support and all data)
        all_sensors = self.fetch_sensor_locations_ogc_api()

        # Only try SensorThings API if OGC API failed
        if not all_sensors:
            logger.info("OGC API returned no data, trying SensorThings API...")
            all_sensors = self.fetch_sensor_locations_sta()

        # Save to cache
        if all_sensors:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(all_sensors, f, ensure_ascii=False, indent=2)

        logger.info(f"Total sensors: {len(all_sensors)}")

        return pd.DataFrame(all_sensors)

    def match_schools_to_sensors(
        self,
        df_schools: pd.DataFrame,
        df_sensors: pd.DataFrame,
        radius_m: int = SEARCH_RADIUS_M
    ) -> Dict[str, List[Dict]]:
        """Match schools to nearby traffic sensors."""
        logger.info(f"Matching schools to sensors within {radius_m}m radius...")

        school_sensors = {}

        # Filter schools with coordinates
        schools_with_coords = df_schools[
            df_schools['latitude'].notna() & df_schools['longitude'].notna()
        ].copy()

        logger.info(f"Schools with coordinates: {len(schools_with_coords)}")

        iterator = schools_with_coords.iterrows()
        if TQDM_AVAILABLE:
            iterator = tqdm(list(iterator), desc="Matching schools to sensors")

        for idx, school in iterator:
            schulnummer = str(school.get('schulnummer', idx))
            school_lat = school['latitude']
            school_lon = school['longitude']

            nearby = []

            for _, sensor in df_sensors.iterrows():
                if pd.isna(sensor['latitude']) or pd.isna(sensor['longitude']):
                    continue

                distance = haversine_distance(
                    school_lat, school_lon,
                    sensor['latitude'], sensor['longitude']
                )

                if distance <= radius_m:
                    nearby.append({
                        'sensor_id': sensor['sensor_id'],
                        'datastream_id': sensor.get('datastream_id'),
                        'distance_m': round(distance, 1),
                        'sensor_type': sensor['sensor_type'],
                        'name': sensor['name'],
                        'dtv': sensor.get('dtv'),
                        'dtvw': sensor.get('dtvw'),
                        'sv_anteil': sensor.get('sv_anteil')
                    })

            # Sort by distance
            nearby.sort(key=lambda x: x['distance_m'])
            school_sensors[schulnummer] = nearby

        # Statistics
        schools_with_sensors = sum(1 for v in school_sensors.values() if v)
        total_matches = sum(len(v) for v in school_sensors.values())

        logger.info(f"Schools with nearby sensors: {schools_with_sensors}/{len(schools_with_coords)}")
        logger.info(f"Total sensor matches: {total_matches}")

        return school_sensors

    def aggregate_traffic_metrics(self, school_sensors: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """Aggregate traffic metrics for each school."""
        logger.info("Aggregating traffic metrics...")

        school_traffic = {}

        for schulnummer, sensors in school_sensors.items():
            if not sensors:
                school_traffic[schulnummer] = {
                    'traffic_dtv_kfz': None,
                    'traffic_dtvw_kfz': None,
                    'traffic_dtv_rad': None,
                    'traffic_sv_anteil': None,
                    'traffic_sensor_distance_m': None,
                    'traffic_sensor_count': 0,
                    'traffic_data_source': None
                }
                continue

            # Separate by sensor type
            kfz_sensors = [s for s in sensors if 'kfz' in s['sensor_type']]
            rad_sensors = [s for s in sensors if 'rad' in s['sensor_type']]

            # Aggregate Kfz metrics (prefer WFS as it has official DTV values)
            kfz_dtv = []
            kfz_dtvw = []
            kfz_sv = []

            for sensor in kfz_sensors:
                if sensor.get('dtv') is not None:
                    kfz_dtv.append(sensor['dtv'])
                if sensor.get('dtvw') is not None:
                    kfz_dtvw.append(sensor['dtvw'])
                if sensor.get('sv_anteil') is not None:
                    kfz_sv.append(sensor['sv_anteil'])

            # Aggregate Rad metrics
            rad_dtv = []
            for sensor in rad_sensors:
                if sensor.get('dtv') is not None:
                    rad_dtv.append(sensor['dtv'])

            # Determine data source
            sources = set()
            for s in sensors:
                if 'ogc' in s['sensor_type']:
                    sources.add('ogc_api')
                elif 'wfs' in s['sensor_type']:
                    sources.add('wfs')
                else:
                    sources.add('sta')

            school_traffic[schulnummer] = {
                'traffic_dtv_kfz': round(np.mean(kfz_dtv)) if kfz_dtv else None,
                'traffic_dtvw_kfz': round(np.mean(kfz_dtvw)) if kfz_dtvw else None,
                'traffic_dtv_rad': round(np.mean(rad_dtv)) if rad_dtv else None,
                'traffic_sv_anteil': round(np.mean(kfz_sv), 1) if kfz_sv else None,
                'traffic_sensor_distance_m': sensors[0]['distance_m'],
                'traffic_sensor_count': len(sensors),
                'traffic_data_source': '/'.join(sorted(sources))
            }

        # Statistics
        with_kfz = sum(1 for v in school_traffic.values() if v['traffic_dtv_kfz'] is not None)
        with_rad = sum(1 for v in school_traffic.values() if v['traffic_dtv_rad'] is not None)

        logger.info(f"Schools with Kfz traffic data: {with_kfz}")
        logger.info(f"Schools with bicycle traffic data: {with_rad}")

        return school_traffic

    def merge_traffic_data(self, df_schools: pd.DataFrame, school_traffic: Dict[str, Dict]) -> pd.DataFrame:
        """Merge traffic data into schools dataframe."""
        logger.info("Merging traffic data into school table...")

        df = df_schools.copy()

        # Add traffic columns
        traffic_columns = [
            'traffic_dtv_kfz',
            'traffic_dtvw_kfz',
            'traffic_dtv_rad',
            'traffic_sv_anteil',
            'traffic_sensor_distance_m',
            'traffic_sensor_count',
            'traffic_data_source'
        ]

        for col in traffic_columns:
            if col not in df.columns:
                df[col] = None

        # Merge
        for idx, row in df.iterrows():
            schulnummer = str(row.get('schulnummer', idx))
            if schulnummer in school_traffic:
                traffic = school_traffic[schulnummer]
                for col in traffic_columns:
                    df.at[idx, col] = traffic.get(col)

        return df

    def save_output(self, df: pd.DataFrame):
        """Save enriched data."""
        logger.info("Saving output files...")

        INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

        # Save CSV
        csv_path = INTERMEDIATE_DIR / "hamburg_schools_with_traffic.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved: {csv_path}")

        # Save parquet
        parquet_path = INTERMEDIATE_DIR / "hamburg_schools_with_traffic.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Saved: {parquet_path}")

    def run(self) -> pd.DataFrame:
        """Run the complete traffic enrichment pipeline."""
        logger.info("="*60)
        logger.info("Starting Hamburg Traffic Data Enrichment")
        logger.info("="*60)

        # Load schools
        df_schools = self.load_schools()

        # Get sensors
        df_sensors = self.get_all_sensors()

        # Match schools to sensors
        school_sensors = self.match_schools_to_sensors(df_schools, df_sensors)

        # Aggregate metrics
        school_traffic = self.aggregate_traffic_metrics(school_sensors)

        # Merge into schools
        df_enriched = self.merge_traffic_data(df_schools, school_traffic)

        # Save output
        self.save_output(df_enriched)

        # Print summary
        self.print_summary(df_enriched)

        return df_enriched

    def print_summary(self, df: pd.DataFrame):
        """Print enrichment summary."""
        print("\n" + "="*70)
        print("HAMBURG TRAFFIC ENRICHMENT - COMPLETE")
        print("="*70)

        print(f"\nTotal schools: {len(df)}")

        print(f"\nTraffic data coverage:")
        coverage = {
            'traffic_dtv_kfz': 'Daily traffic volume (Kfz)',
            'traffic_dtvw_kfz': 'Workday traffic volume (Kfz)',
            'traffic_dtv_rad': 'Daily bicycle traffic',
            'traffic_sv_anteil': 'Heavy vehicle percentage',
            'traffic_sensor_count': 'Nearby sensors'
        }

        for col, label in coverage.items():
            if col in df.columns:
                if col == 'traffic_sensor_count':
                    count = (df[col] > 0).sum()
                else:
                    count = df[col].notna().sum()
                pct = 100 * count / len(df)
                print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

        # Distance stats
        if 'traffic_sensor_distance_m' in df.columns and df['traffic_sensor_distance_m'].notna().any():
            print(f"\nSensor distances:")
            print(f"  - Min: {df['traffic_sensor_distance_m'].min():.0f}m")
            print(f"  - Median: {df['traffic_sensor_distance_m'].median():.0f}m")
            print(f"  - Max: {df['traffic_sensor_distance_m'].max():.0f}m")

        # Traffic stats
        if 'traffic_dtv_kfz' in df.columns and df['traffic_dtv_kfz'].notna().any():
            print(f"\nTraffic volume statistics (Kfz/day):")
            print(f"  - Min: {df['traffic_dtv_kfz'].min():,.0f}")
            print(f"  - Median: {df['traffic_dtv_kfz'].median():,.0f}")
            print(f"  - Max: {df['traffic_dtv_kfz'].max():,.0f}")

        print("\n" + "="*70)


def main():
    """Main entry point."""
    enricher = HamburgTrafficEnrichment()
    enricher.run()


if __name__ == "__main__":
    main()
