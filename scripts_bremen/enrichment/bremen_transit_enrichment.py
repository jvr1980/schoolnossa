#!/usr/bin/env python3
"""
Phase 3: Bremen Transit Enrichment (Overpass API)

Enriches schools with public transit accessibility using OpenStreetMap Overpass API.
Same approach as NRW pipeline. Queries for bus, tram, and rail stops near schools.

Output columns (Berlin schema compatible):
  transit_rail_01..03_name/distance_m/latitude/longitude/lines
  transit_tram_01..03_name/distance_m/latitude/longitude/lines
  transit_bus_01..03_name/distance_m/latitude/longitude/lines
  transit_stop_count_1000m, transit_all_lines_1000m
  transit_accessibility_score

Input (fallback chain):
    1. data_bremen/intermediate/bremen_schools_with_traffic.csv
    2. data_bremen/raw/bremen_school_master.csv

Output:
    - data_bremen/intermediate/bremen_schools_with_transit.csv

Reference: scripts_nrw/enrichment/nrw_transit_enrichment.py
Author: Bremen School Data Pipeline
Created: 2026-04-07
"""

import pandas as pd
import requests
import math
import time
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_bremen"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
RADIUS_500M = 500
RADIUS_1000M = 1000
TOP_N_STOPS = 3
REQUEST_DELAY_S = 1.0


def find_input_file() -> Path:
    """Find the most-enriched input file."""
    fallback_chain = [
        ("bremen_schools_with_traffic.csv", INTERMEDIATE_DIR),
        ("bremen_school_master.csv", RAW_DIR),
    ]
    for filename, directory in fallback_chain:
        path = directory / filename
        if path.exists():
            logger.info(f"Using input: {path.name}")
            return path
    raise FileNotFoundError("No input file found for Bremen schools")


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def classify_osm_transit_type(tags: Dict) -> str:
    railway = tags.get('railway', '')
    station = tags.get('station', '')
    if station == 'subway' or railway == 'subway_entrance':
        return 'U-Bahn'
    elif station == 'light_rail' or railway == 'tram_stop':
        return 'Straßenbahn'
    elif railway in ('station', 'halt'):
        network = tags.get('network', '').lower()
        name = tags.get('name', '').lower()
        if 's-bahn' in network or 's-bahn' in name:
            return 'S-Bahn'
        return 'Bahn'
    if tags.get('bus') == 'yes' or tags.get('highway') == 'bus_stop':
        return 'Bus'
    return 'Bus'


def map_to_berlin_type(osm_type: str) -> str:
    if osm_type in ('U-Bahn', 'S-Bahn', 'Bahn'):
        return 'rail'
    elif osm_type == 'Straßenbahn':
        return 'tram'
    return 'bus'


def query_overpass(min_lat, min_lon, max_lat, max_lon, timeout_s=300):
    query = f"""
    [out:json][timeout:{timeout_s}];
    (
      node["railway"="station"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["railway"="halt"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["railway"="tram_stop"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["railway"="subway_entrance"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["public_transport"="stop_position"]["bus"="yes"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["highway"="bus_stop"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["public_transport"="platform"]["bus"="yes"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["station"="subway"]({min_lat},{min_lon},{max_lat},{max_lon});
      node["station"="light_rail"]({min_lat},{min_lon},{max_lat},{max_lon});
    );
    out body;
    """
    try:
        response = requests.post(
            OVERPASS_URL,
            data={'data': query},
            timeout=timeout_s + 60,
            headers={'User-Agent': 'SchoolNossa/1.0 (Bremen transit enrichment)'}
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Overpass query failed: {e}")
        return None


def parse_overpass_elements(data: dict) -> List[Dict]:
    stops = []
    for element in data.get('elements', []):
        if element.get('type') != 'node':
            continue
        tags = element.get('tags', {})
        lat, lon = element.get('lat'), element.get('lon')
        if lat is None or lon is None:
            continue

        osm_type = classify_osm_transit_type(tags)
        stop_name = tags.get('name') or tags.get('description') or tags.get('ref') or f"{osm_type} ({lat:.4f}, {lon:.4f})"
        lines = tags.get('route_ref', tags.get('line', tags.get('ref', '')))

        stops.append({
            'stop_id': element.get('id'),
            'stop_name': stop_name,
            'osm_type': osm_type,
            'mapped_type': map_to_berlin_type(osm_type),
            'latitude': lat,
            'longitude': lon,
            'network': tags.get('network', ''),
            'operator': tags.get('operator', ''),
            'lines': lines,
        })
    return stops


def fetch_transit_stops_bulk(min_lat, min_lon, max_lat, max_lon) -> List[Dict]:
    """Fetch all transit stops in bounding box, splitting on failure."""
    logger.info(f"Querying Overpass API for transit stops in bbox ({min_lat:.4f},{min_lon:.4f}) to ({max_lat:.4f},{max_lon:.4f})...")

    data = query_overpass(min_lat, min_lon, max_lat, max_lon)
    if data is not None:
        stops = parse_overpass_elements(data)
        logger.info(f"Found {len(stops)} transit stops")

        type_counts = defaultdict(int)
        for stop in stops:
            type_counts[f"{stop['mapped_type']} ({stop['osm_type']})"] += 1
        for t, c in sorted(type_counts.items()):
            logger.info(f"  - {t}: {c}")

        return stops

    # Split into quadrants on failure
    logger.info("Splitting bbox into 4 sub-regions...")
    mid_lat = (min_lat + max_lat) / 2
    mid_lon = (min_lon + max_lon) / 2
    all_stops = []
    seen_ids = set()

    for s_min_lat, s_min_lon, s_max_lat, s_max_lon in [
        (min_lat, min_lon, mid_lat, mid_lon),
        (min_lat, mid_lon, mid_lat, max_lon),
        (mid_lat, min_lon, max_lat, mid_lon),
        (mid_lat, mid_lon, max_lat, max_lon),
    ]:
        time.sleep(2)
        sub_data = query_overpass(s_min_lat, s_min_lon, s_max_lat, s_max_lon)
        if sub_data:
            for stop in parse_overpass_elements(sub_data):
                if stop['stop_id'] not in seen_ids:
                    seen_ids.add(stop['stop_id'])
                    all_stops.append(stop)

    logger.info(f"Found {len(all_stops)} transit stops total after splitting")
    return all_stops


def enrich_schools_with_transit(schools_df: pd.DataFrame, stops: List[Dict]) -> pd.DataFrame:
    """Calculate transit metrics for each school in Berlin-compatible format."""
    logger.info("Calculating transit accessibility for schools...")

    if not stops:
        logger.warning("No transit stops available")
        return schools_df

    stops_df = pd.DataFrame(stops)
    df = schools_df.copy()

    transit_types = ['rail', 'tram', 'bus']
    for ttype in transit_types:
        for rank in range(1, TOP_N_STOPS + 1):
            prefix = f"transit_{ttype}_{rank:02d}"
            for field in ['name', 'distance_m', 'latitude', 'longitude', 'lines']:
                df[f"{prefix}_{field}"] = None

    df['transit_stops_500m'] = 0
    df['transit_stop_count_1000m'] = 0
    df['transit_all_lines_1000m'] = None
    df['transit_accessibility_score'] = 0.0

    stop_lats = stops_df['latitude'].values
    stop_lons = stops_df['longitude'].values
    stop_types = stops_df['mapped_type'].values
    stop_names = stops_df['stop_name'].values
    stop_lines = stops_df['lines'].values

    valid_schools = df[df['latitude'].notna() & df['longitude'].notna()]
    iterator = valid_schools.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Transit enrichment")

    for idx, school in iterator:
        school_lat = float(school['latitude'])
        school_lon = float(school['longitude'])

        stops_500m = 0
        stops_1000m = 0
        all_lines = set()
        stops_by_type = defaultdict(list)

        for i in range(len(stop_lats)):
            dist = haversine_distance(school_lat, school_lon, stop_lats[i], stop_lons[i])
            if dist <= RADIUS_500M:
                stops_500m += 1
            if dist <= RADIUS_1000M:
                stops_1000m += 1
                line_str = stop_lines[i]
                if line_str and str(line_str).strip():
                    for line in str(line_str).replace(';', ',').split(','):
                        if line.strip():
                            all_lines.add(line.strip())

            stops_by_type[stop_types[i]].append({
                'name': stop_names[i], 'distance': dist,
                'latitude': stop_lats[i], 'longitude': stop_lons[i],
                'lines': stop_lines[i] if stop_lines[i] else None,
            })

        df.at[idx, 'transit_stops_500m'] = stops_500m
        df.at[idx, 'transit_stop_count_1000m'] = stops_1000m
        df.at[idx, 'transit_all_lines_1000m'] = ', '.join(sorted(all_lines)) if all_lines else None

        for ttype in transit_types:
            type_stops = sorted(stops_by_type.get(ttype, []), key=lambda x: x['distance'])
            seen = set()
            unique = []
            for s in type_stops:
                key = s['name'].lower() if s['name'] else ''
                if key not in seen:
                    seen.add(key)
                    unique.append(s)

            for rank in range(TOP_N_STOPS):
                prefix = f"transit_{ttype}_{rank + 1:02d}"
                if rank < len(unique):
                    df.at[idx, f"{prefix}_name"] = unique[rank]['name']
                    df.at[idx, f"{prefix}_distance_m"] = round(unique[rank]['distance'])
                    df.at[idx, f"{prefix}_latitude"] = unique[rank]['latitude']
                    df.at[idx, f"{prefix}_longitude"] = unique[rank]['longitude']
                    df.at[idx, f"{prefix}_lines"] = unique[rank]['lines']

        # Accessibility score (0-100)
        score = min(stops_500m * 5, 30) + min(stops_1000m * 2, 20)
        rail = stops_by_type.get('rail', [])
        tram = stops_by_type.get('tram', [])
        if rail and rail[0]['distance'] < 500:
            score += 25
        elif rail and rail[0]['distance'] < 1000:
            score += 15
        elif tram and tram[0]['distance'] < 500:
            score += 20
        elif tram and tram[0]['distance'] < 1000:
            score += 10

        df.at[idx, 'transit_accessibility_score'] = min(score, 100)

    return df


def main() -> str:
    """Main entry point called by orchestrator."""
    logger.info("=" * 60)
    logger.info("Starting Bremen Transit Enrichment (Overpass API)")
    logger.info("=" * 60)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    input_file = find_input_file()
    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} schools from {input_file.name}")

    # Load or fetch transit stops
    cache_file = CACHE_DIR / "bremen_transit_stops.json"
    stops = []

    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 7 * 86400:
            logger.info("Loading transit stops from cache...")
            with open(cache_file, 'r', encoding='utf-8') as f:
                stops = json.load(f)
            logger.info(f"Loaded {len(stops)} cached transit stops")

    if not stops:
        valid = schools_df[schools_df['latitude'].notna() & schools_df['longitude'].notna()]
        if valid.empty:
            logger.warning("No schools with coordinates")
            return str(input_file)

        margin = 0.02
        stops = fetch_transit_stops_bulk(
            valid['latitude'].min() - margin, valid['longitude'].min() - margin,
            valid['latitude'].max() + margin, valid['longitude'].max() + margin,
        )

        if stops:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(stops, f, ensure_ascii=False)

    enriched_df = enrich_schools_with_transit(schools_df, stops)

    output_path = INTERMEDIATE_DIR / "bremen_schools_with_transit.csv"
    enriched_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path} ({len(enriched_df)} schools)")

    # Summary
    if 'transit_stops_500m' in enriched_df.columns:
        logger.info(f"Avg stops within 500m: {enriched_df['transit_stops_500m'].mean():.1f}")
        logger.info(f"Avg stops within 1000m: {enriched_df['transit_stop_count_1000m'].mean():.1f}")

    return str(output_path)


if __name__ == "__main__":
    main()
