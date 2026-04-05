#!/usr/bin/env python3
"""
Phase 3: Munich Transit Accessibility Enrichment

Enriches school data with public transit accessibility using Overpass API.
Munich has U-Bahn, S-Bahn, Tram, and Bus — all well-mapped in OSM.

MVV GTFS is available as a richer source, but Overpass API is used for
consistency with NRW/Frankfurt pipelines and to avoid GTFS parsing complexity.

Output columns (Berlin schema compatible):
  transit_rail_01..03_name/distance_m/latitude/longitude/lines
  transit_tram_01..03_name/distance_m/latitude/longitude/lines
  transit_bus_01..03_name/distance_m/latitude/longitude/lines
  transit_stop_count_1000m, transit_all_lines_1000m, transit_accessibility_score

Input: data_munich/intermediate/munich_secondary_schools_with_traffic.csv
       (fallback: munich_secondary_schools.csv)
Output: data_munich/intermediate/munich_secondary_schools_with_transit.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
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

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
RADIUS_500M = 500
RADIUS_1000M = 1000
TOP_N_STOPS = 3

# Munich bounding box
MUNICH_BBOX = (48.06, 11.36, 48.25, 11.72)


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def classify_osm_transit_type(tags):
    """Classify OSM transit stop into Berlin-compatible types."""
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
        elif 'u-bahn' in network or 'u-bahn' in name:
            return 'U-Bahn'
        return 'Bahn'
    if tags.get('bus') == 'yes' or tags.get('highway') == 'bus_stop':
        return 'Bus'
    return 'Bus'


def map_to_berlin_type(osm_type):
    """Map to Berlin schema transit types: rail, tram, bus."""
    if osm_type in ('U-Bahn', 'S-Bahn', 'Bahn'):
        return 'rail'
    elif osm_type in ('Straßenbahn', 'Tram'):
        return 'tram'
    return 'bus'


def fetch_transit_stops_overpass():
    """Fetch all transit stops in Munich area from Overpass API."""
    cache_file = CACHE_DIR / "munich_transit_stops.json"
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 14 * 86400:
            logger.info("Loading transit stops from cache...")
            with open(cache_file) as f:
                return json.load(f)

    logger.info("Fetching transit stops from Overpass API...")
    bbox = f"{MUNICH_BBOX[0]},{MUNICH_BBOX[1]},{MUNICH_BBOX[2]},{MUNICH_BBOX[3]}"
    query = f"""
    [out:json][timeout:120];
    (
      node["railway"="station"]({bbox});
      node["railway"="halt"]({bbox});
      node["railway"="tram_stop"]({bbox});
      node["railway"="subway_entrance"]({bbox});
      node["highway"="bus_stop"]({bbox});
      node["public_transport"="stop_position"]["bus"="yes"]({bbox});
    );
    out body;
    """

    try:
        response = requests.post(OVERPASS_URL, data={'data': query}, timeout=120)
        response.raise_for_status()
        data = response.json()
        stops = data.get('elements', [])
        logger.info(f"Fetched {len(stops)} transit stops")

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(stops, f)
        return stops
    except Exception as e:
        logger.error(f"Overpass query failed: {e}")
        return []


def process_transit_stops(raw_stops):
    """Process raw Overpass stops into structured list."""
    stops = []
    seen = set()

    for elem in raw_stops:
        if elem.get('type') != 'node':
            continue
        lat = elem.get('lat')
        lon = elem.get('lon')
        if not lat or not lon:
            continue

        tags = elem.get('tags', {})
        name = tags.get('name', 'Unknown')
        osm_type = classify_osm_transit_type(tags)
        berlin_type = map_to_berlin_type(osm_type)

        # Extract lines
        lines = []
        for key in ['line', 'route_ref', 'ref']:
            if key in tags:
                lines.extend(tags[key].replace(',', ';').split(';'))
        lines = [l.strip() for l in lines if l.strip()]

        dedup_key = f"{name}|{berlin_type}|{round(lat,4)}|{round(lon,4)}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        stops.append({
            'name': name, 'lat': lat, 'lon': lon,
            'osm_type': osm_type, 'berlin_type': berlin_type,
            'lines': lines,
        })

    logger.info(f"Processed {len(stops)} unique stops")
    type_counts = defaultdict(int)
    for s in stops:
        type_counts[s['berlin_type']] += 1
    for t, c in sorted(type_counts.items()):
        logger.info(f"  {t}: {c}")

    return stops


def enrich_school_transit(school_lat, school_lon, stops, radius=RADIUS_1000M):
    """Find nearest stops of each type for a single school."""
    by_type = defaultdict(list)

    for stop in stops:
        dist = haversine_distance(school_lat, school_lon, stop['lat'], stop['lon'])
        if dist <= radius:
            by_type[stop['berlin_type']].append({**stop, 'distance': dist})

    # Sort by distance within each type
    for t in by_type:
        by_type[t].sort(key=lambda x: x['distance'])

    result = {}
    all_lines = set()
    stop_count = 0

    for transit_type in ['rail', 'tram', 'bus']:
        nearby = by_type.get(transit_type, [])
        stop_count += len(nearby)

        for i in range(TOP_N_STOPS):
            prefix = f"transit_{transit_type}_{i+1:02d}"
            if i < len(nearby):
                s = nearby[i]
                result[f"{prefix}_name"] = s['name']
                result[f"{prefix}_distance_m"] = round(s['distance'])
                result[f"{prefix}_latitude"] = round(s['lat'], 6)
                result[f"{prefix}_longitude"] = round(s['lon'], 6)
                result[f"{prefix}_lines"] = '; '.join(s['lines']) if s['lines'] else ''
                all_lines.update(s['lines'])
            else:
                for suffix in ['name', 'distance_m', 'latitude', 'longitude', 'lines']:
                    result[f"{prefix}_{suffix}"] = None

    result['transit_stop_count_1000m'] = stop_count
    result['transit_all_lines_1000m'] = '; '.join(sorted(all_lines)) if all_lines else ''

    # Accessibility score (0-100)
    rail_nearby = len(by_type.get('rail', []))
    tram_nearby = len(by_type.get('tram', []))
    bus_nearby = len(by_type.get('bus', []))
    score = min(100, rail_nearby * 20 + tram_nearby * 10 + bus_nearby * 5)
    result['transit_accessibility_score'] = score

    return result


def find_input_file():
    candidates = [
        INTERMEDIATE_DIR / "munich_secondary_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "munich_secondary_schools.csv",
    ]
    for f in candidates:
        if f.exists():
            return f
    raise FileNotFoundError("No school data found. Run earlier phases first.")


def main():
    logger.info("=" * 60)
    logger.info("Phase 3: Munich Transit Enrichment (Overpass API)")
    logger.info("=" * 60)

    input_file = find_input_file()
    schools = pd.read_csv(input_file, dtype=str)
    schools['latitude'] = pd.to_numeric(schools['latitude'], errors='coerce')
    schools['longitude'] = pd.to_numeric(schools['longitude'], errors='coerce')
    logger.info(f"Loaded {len(schools)} schools")

    raw_stops = fetch_transit_stops_overpass()
    stops = process_transit_stops(raw_stops)

    with_coords = schools[schools['latitude'].notna() & schools['longitude'].notna()]
    logger.info(f"Enriching {len(with_coords)} schools with coordinates...")

    transit_data = []
    iterator = list(with_coords.iterrows())
    if TQDM_AVAILABLE:
        iterator = tqdm(iterator, desc="Transit enrichment")

    for idx, row in iterator:
        result = enrich_school_transit(float(row['latitude']), float(row['longitude']), stops)
        result['_idx'] = idx
        transit_data.append(result)

    transit_df = pd.DataFrame(transit_data).set_index('_idx')

    for col in transit_df.columns:
        schools[col] = None
    for idx, row in transit_df.iterrows():
        for col in transit_df.columns:
            schools.at[idx, col] = row[col]

    output_path = INTERMEDIATE_DIR / "munich_secondary_schools_with_transit.csv"
    schools.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path}")

    enriched = schools['transit_stop_count_1000m'].notna().sum()
    print(f"\nTransit enrichment: {enriched}/{len(schools)} schools")
    avg_score = pd.to_numeric(schools['transit_accessibility_score'], errors='coerce').mean()
    print(f"Average accessibility score: {avg_score:.1f}/100")

    return schools


if __name__ == "__main__":
    main()
