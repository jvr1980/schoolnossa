#!/usr/bin/env python3
"""
Frankfurt Transit Enrichment
Enriches school data with public transit accessibility using OpenStreetMap Overpass API.

Frankfurt has U-Bahn, S-Bahn, Straßenbahn (tram), and bus — all well-mapped in OSM.

Output columns (Berlin schema compatible):
  transit_rail_01..03_name/distance_m/latitude/longitude/lines
  transit_tram_01..03_name/distance_m/latitude/longitude/lines
  transit_bus_01..03_name/distance_m/latitude/longitude/lines
  transit_stop_count_1000m, transit_all_lines_1000m, transit_accessibility_score

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
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
DATA_DIR = PROJECT_ROOT / "data_frankfurt"
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
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
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
        elif 'u-bahn' in network or 'u-bahn' in name:
            return 'U-Bahn'
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


def fetch_transit_stops(min_lat, min_lon, max_lat, max_lon):
    """Fetch all transit stops in bounding box via Overpass API."""
    query = f"""
    [out:json][timeout:300];
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

    logger.info(f"Querying Overpass API for transit stops...")
    try:
        response = requests.post(OVERPASS_URL, data={'data': query}, timeout=360,
                                 headers={'User-Agent': 'SchoolNossa/1.0 (Frankfurt transit enrichment)'})
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Overpass query failed: {e}")
        return []

    stops = []
    for el in data.get('elements', []):
        if el.get('type') != 'node':
            continue
        tags = el.get('tags', {})
        lat, lon = el.get('lat'), el.get('lon')
        if lat is None or lon is None:
            continue

        osm_type = classify_osm_transit_type(tags)
        stops.append({
            'stop_id': el.get('id'),
            'stop_name': tags.get('name') or tags.get('description') or f"{osm_type} ({lat:.4f},{lon:.4f})",
            'osm_type': osm_type,
            'mapped_type': map_to_berlin_type(osm_type),
            'latitude': lat,
            'longitude': lon,
            'lines': tags.get('route_ref', tags.get('line', '')),
        })

    type_counts = defaultdict(int)
    for s in stops:
        type_counts[f"{s['mapped_type']} ({s['osm_type']})"] += 1
    for t, c in sorted(type_counts.items()):
        logger.info(f"  {t}: {c}")

    logger.info(f"Total transit stops: {len(stops)}")
    return stops


def enrich_schools_with_transit(schools_df, stops):
    """Calculate transit metrics for each school."""
    logger.info("Calculating transit accessibility...")

    if not stops:
        return schools_df

    stops_df = pd.DataFrame(stops)
    df = schools_df.copy()

    transit_types = ['rail', 'tram', 'bus']
    for tt in transit_types:
        for r in range(1, TOP_N_STOPS + 1):
            p = f"transit_{tt}_{r:02d}"
            for sfx in ['_name', '_distance_m', '_latitude', '_longitude', '_lines']:
                df[f"{p}{sfx}"] = None

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
        s500, s1000 = 0, 0
        all_lines = set()
        by_type = defaultdict(list)

        for i in range(len(stop_lats)):
            d = haversine_distance(slat, slon, stop_lats[i], stop_lons[i])
            if d <= RADIUS_500M: s500 += 1
            if d <= RADIUS_1000M:
                s1000 += 1
                ls = stop_lines[i]
                if ls and str(ls).strip():
                    for l in str(ls).replace(';', ',').split(','):
                        l = l.strip()
                        if l: all_lines.add(l)

            by_type[stop_types[i]].append({
                'name': stop_names[i], 'distance': d,
                'latitude': stop_lats[i], 'longitude': stop_lons[i],
                'lines': stop_lines[i] if stop_lines[i] else None,
            })

        df.at[idx, 'transit_stops_500m'] = s500
        df.at[idx, 'transit_stop_count_1000m'] = s1000
        df.at[idx, 'transit_all_lines_1000m'] = ', '.join(sorted(all_lines)) if all_lines else None

        for tt in transit_types:
            type_stops = sorted(by_type.get(tt, []), key=lambda x: x['distance'])
            seen = set()
            unique = []
            for s in type_stops:
                nl = s['name'].lower() if s['name'] else ''
                if nl not in seen:
                    seen.add(nl)
                    unique.append(s)

            for r in range(TOP_N_STOPS):
                p = f"transit_{tt}_{r+1:02d}"
                if r < len(unique):
                    s = unique[r]
                    df.at[idx, f"{p}_name"] = s['name']
                    df.at[idx, f"{p}_distance_m"] = round(s['distance'])
                    df.at[idx, f"{p}_latitude"] = s['latitude']
                    df.at[idx, f"{p}_longitude"] = s['longitude']
                    df.at[idx, f"{p}_lines"] = s['lines']

        # Accessibility score (0-100)
        score = min(s500 * 5, 30) + min(s1000 * 2, 20)
        rail = by_type.get('rail', [])
        tram = by_type.get('tram', [])
        if rail and rail[0]['distance'] < 500: score += 25
        elif rail and rail[0]['distance'] < 1000: score += 15
        elif tram and tram[0]['distance'] < 500: score += 20
        elif tram and tram[0]['distance'] < 1000: score += 10
        df.at[idx, 'transit_accessibility_score'] = min(score, 100)

    return df


def enrich_schools(school_type="secondary"):
    """Run transit enrichment."""
    input_file = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_traffic.csv"
    if not input_file.exists():
        input_file = RAW_DIR / f"frankfurt_{school_type}_schools.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Not found: {input_file}")

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} {school_type} schools")

    # Check cache
    cache_file = CACHE_DIR / "frankfurt_transit_stops.json"
    stops = []
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 7 * 86400:
            logger.info("Loading transit stops from cache...")
            with open(cache_file, 'r', encoding='utf-8') as f:
                stops = json.load(f)

    if not stops:
        valid = df[df['latitude'].notna() & df['longitude'].notna()]
        if valid.empty:
            return df
        margin = 0.02
        stops = fetch_transit_stops(
            valid['latitude'].min() - margin, valid['longitude'].min() - margin,
            valid['latitude'].max() + margin, valid['longitude'].max() + margin,
        )
        if stops:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(stops, f, ensure_ascii=False)

    enriched = enrich_schools_with_transit(df, stops)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_transit.csv"
    enriched.to_csv(out, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out}")

    print(f"\n{'='*70}\nFRANKFURT TRANSIT ENRICHMENT ({school_type.upper()}) - COMPLETE\n{'='*70}")
    print(f"Schools: {len(enriched)}")
    if 'transit_stops_500m' in enriched.columns:
        print(f"Avg stops within 500m: {enriched['transit_stops_500m'].mean():.1f}")
        print(f"Avg stops within 1000m: {enriched['transit_stop_count_1000m'].mean():.1f}")
    if 'transit_accessibility_score' in enriched.columns:
        print(f"Avg accessibility score: {enriched['transit_accessibility_score'].mean():.1f}")
    print(f"{'='*70}")
    return enriched


def main():
    logger.info("=" * 60)
    logger.info("Starting Frankfurt Transit Enrichment (Overpass API)")
    logger.info("=" * 60)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for st in ['secondary', 'primary']:
        for d in [INTERMEDIATE_DIR, RAW_DIR]:
            f = d / f"frankfurt_{st}_schools_with_traffic.csv"
            if not f.exists():
                f = d / f"frankfurt_{st}_schools.csv"
            if f.exists():
                enrich_schools(st)
                break
        else:
            logger.warning(f"No {st} school data found")


if __name__ == "__main__":
    main()
