#!/usr/bin/env python3
"""
Phase 3: Dresden Transit Enrichment (Overpass API)

Enriches school data with public transit accessibility using OpenStreetMap Overpass API.
Same approach as NRW pipeline.

Dresden bounding box: lat 50.96–51.14, lon 13.57–13.90
Transit types: Bus, Straßenbahn (tram), S-Bahn, regional rail

Output columns (Berlin schema compatible):
  transit_rail_01..03_name/distance_m/latitude/longitude/lines
  transit_tram_01..03_name/distance_m/latitude/longitude/lines
  transit_bus_01..03_name/distance_m/latitude/longitude/lines
  transit_stop_count_1000m, transit_all_lines_1000m, transit_accessibility_score

Author: Dresden School Data Pipeline
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

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
RADIUS_500M = 500
RADIUS_1000M = 1000
TOP_N_STOPS = 3


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def classify_osm_transit_type(tags):
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


def map_to_berlin_type(osm_type):
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
      node["station"="light_rail"]({min_lat},{min_lon},{max_lat},{max_lon});
    );
    out body;
    """
    try:
        resp = requests.post(OVERPASS_URL, data={'data': query}, timeout=timeout_s + 60,
                             headers={'User-Agent': 'SchoolNossa/1.0 (Dresden transit enrichment)'})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Overpass query failed: {e}")
        return None


def parse_overpass_elements(data):
    stops = []
    for el in data.get('elements', []):
        if el.get('type') != 'node':
            continue
        tags = el.get('tags', {})
        lat, lon = el.get('lat'), el.get('lon')
        if lat is None or lon is None:
            continue
        osm_type = classify_osm_transit_type(tags)
        name = tags.get('name') or tags.get('description') or tags.get('ref') or f"{osm_type} ({lat:.4f}, {lon:.4f})"
        lines = tags.get('route_ref', tags.get('line', tags.get('ref', '')))
        stops.append({
            'stop_id': el.get('id'), 'stop_name': name, 'osm_type': osm_type,
            'mapped_type': map_to_berlin_type(osm_type),
            'latitude': lat, 'longitude': lon,
            'network': tags.get('network', ''), 'operator': tags.get('operator', ''),
            'lines': lines,
        })
    return stops


def fetch_transit_stops_bulk(min_lat, min_lon, max_lat, max_lon):
    """Fetch all transit stops in bounding box, splitting on failure."""
    logger.info(f"Querying Overpass API for Dresden transit stops...")

    seen_ids = set()

    def fetch_recursive(mn_lat, mn_lon, mx_lat, mx_lon, depth=0):
        data = query_overpass(mn_lat, mn_lon, mx_lat, mx_lon)
        if data is not None:
            stops = parse_overpass_elements(data)
            unique = [s for s in stops if s['stop_id'] not in seen_ids]
            seen_ids.update(s['stop_id'] for s in unique)
            logger.info(f"{'  '*depth}Found {len(stops)} stops ({len(unique)} new)")
            return unique
        if depth >= 3:
            return []
        logger.info(f"{'  '*depth}Splitting bbox...")
        mid_lat, mid_lon = (mn_lat+mx_lat)/2, (mn_lon+mx_lon)/2
        result = []
        for b in [(mn_lat,mn_lon,mid_lat,mid_lon),(mn_lat,mid_lon,mid_lat,mx_lon),
                   (mid_lat,mn_lon,mx_lat,mid_lon),(mid_lat,mid_lon,mx_lat,mx_lon)]:
            time.sleep(2)
            result.extend(fetch_recursive(*b, depth+1))
        return result

    stops = fetch_recursive(min_lat, min_lon, max_lat, max_lon)
    logger.info(f"Total transit stops: {len(stops)}")

    type_counts = defaultdict(int)
    for s in stops:
        type_counts[f"{s['mapped_type']} ({s['osm_type']})"] += 1
    for t, c in sorted(type_counts.items()):
        logger.info(f"  - {t}: {c}")

    return stops


def enrich_schools_with_transit(schools_df, stops):
    """Calculate transit metrics for each school in Berlin-compatible format."""
    logger.info("Calculating transit accessibility...")

    if not stops:
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

    valid = df[df['latitude'].notna() & df['longitude'].notna()]
    iterator = valid.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Transit enrichment")

    for idx, school in iterator:
        slat, slon = float(school['latitude']), float(school['longitude'])
        stops_500, stops_1000, lines_1000 = 0, 0, set()
        by_type = defaultdict(list)

        for i in range(len(stop_lats)):
            dist = haversine_distance(slat, slon, stop_lats[i], stop_lons[i])
            if dist <= 500: stops_500 += 1
            if dist <= 1000:
                stops_1000 += 1
                if stop_lines[i] and str(stop_lines[i]).strip():
                    for l in str(stop_lines[i]).replace(';', ',').split(','):
                        l = l.strip()
                        if l: lines_1000.add(l)
            by_type[stop_types[i]].append({
                'name': stop_names[i], 'distance': dist,
                'latitude': stop_lats[i], 'longitude': stop_lons[i],
                'lines': stop_lines[i] if stop_lines[i] else None,
            })

        df.at[idx, 'transit_stops_500m'] = stops_500
        df.at[idx, 'transit_stop_count_1000m'] = stops_1000
        df.at[idx, 'transit_all_lines_1000m'] = ', '.join(sorted(lines_1000)) if lines_1000 else None

        for ttype in transit_types:
            sorted_stops = sorted(by_type.get(ttype, []), key=lambda x: x['distance'])
            seen = set()
            unique = []
            for s in sorted_stops:
                nl = s['name'].lower() if s['name'] else ''
                if nl not in seen:
                    seen.add(nl)
                    unique.append(s)
            for rank in range(TOP_N_STOPS):
                prefix = f"transit_{ttype}_{rank+1:02d}"
                if rank < len(unique):
                    s = unique[rank]
                    df.at[idx, f"{prefix}_name"] = s['name']
                    df.at[idx, f"{prefix}_distance_m"] = round(s['distance'])
                    df.at[idx, f"{prefix}_latitude"] = s['latitude']
                    df.at[idx, f"{prefix}_longitude"] = s['longitude']
                    df.at[idx, f"{prefix}_lines"] = s['lines']

        # Accessibility score
        score = min(stops_500 * 5, 30) + min(stops_1000 * 2, 20)
        rail = by_type.get('rail', [])
        tram = by_type.get('tram', [])
        if rail and rail[0]['distance'] < 500: score += 25
        elif rail and rail[0]['distance'] < 1000: score += 15
        elif tram and tram[0]['distance'] < 500: score += 20
        elif tram and tram[0]['distance'] < 1000: score += 10
        df.at[idx, 'transit_accessibility_score'] = min(score, 100)

    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Transit Enrichment (Overpass API)")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Load school data (fallback chain)
    input_file = INTERMEDIATE_DIR / "dresden_schools_with_traffic.csv"
    if not input_file.exists():
        input_file = RAW_DIR / "dresden_schools_raw.csv"
    if not input_file.exists():
        raise FileNotFoundError("No school data found")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} schools from {input_file.name}")

    # Check cache
    cache_file = CACHE_DIR / "dresden_transit_stops.json"
    stops = []

    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 7 * 86400:
            logger.info("Loading transit stops from cache...")
            with open(cache_file, 'r', encoding='utf-8') as f:
                stops = json.load(f)
            # Ensure mapped_type exists
            if stops and 'mapped_type' not in stops[0]:
                for s in stops:
                    s['osm_type'] = s.get('osm_type', 'Bus')
                    s['mapped_type'] = map_to_berlin_type(s['osm_type'])
            logger.info(f"Loaded {len(stops)} cached stops")

    if not stops:
        valid = schools_df[schools_df['latitude'].notna() & schools_df['longitude'].notna()]
        if valid.empty:
            logger.warning("No schools with coordinates")
            return

        margin = 0.02
        stops = fetch_transit_stops_bulk(
            valid['latitude'].min() - margin, valid['longitude'].min() - margin,
            valid['latitude'].max() + margin, valid['longitude'].max() + margin
        )

        if stops:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(stops, f, ensure_ascii=False)

    enriched = enrich_schools_with_transit(schools_df, stops)

    out_path = INTERMEDIATE_DIR / "dresden_schools_with_transit.csv"
    enriched.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out_path}")

    print(f"\n{'='*70}")
    print("DRESDEN TRANSIT ENRICHMENT - COMPLETE")
    print(f"{'='*70}")
    print(f"Total schools: {len(enriched)}")
    if 'transit_stops_500m' in enriched.columns:
        print(f"Avg stops within 500m: {enriched['transit_stops_500m'].mean():.1f}")
        print(f"Avg stops within 1000m: {enriched['transit_stop_count_1000m'].mean():.1f}")
    if 'transit_accessibility_score' in enriched.columns:
        print(f"Avg accessibility score: {enriched['transit_accessibility_score'].mean():.1f}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
