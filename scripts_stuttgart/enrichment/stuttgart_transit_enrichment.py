#!/usr/bin/env python3
"""
Stuttgart Transit Enrichment
Enriches school data with public transit accessibility using OSM Overpass API.

Output columns (Berlin-compatible):
  transit_rail_01..03_name/distance_m/latitude/longitude/lines
  transit_tram_01..03_*
  transit_bus_01..03_*
  transit_stop_count_1000m, transit_all_lines_1000m, transit_accessibility_score

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
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
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
TOP_N_STOPS = 3


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


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


def _query_overpass(min_lat, min_lon, max_lat, max_lon, timeout_s=300):
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
        r = requests.post(OVERPASS_URL, data={'data': query}, timeout=timeout_s+60,
                          headers={'User-Agent': 'SchoolNossa/1.0 (Stuttgart transit enrichment)'})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"Overpass failed: {e}")
        return None


def _fetch_bbox_recursive(min_lat, min_lon, max_lat, max_lon, seen_ids, depth=0, max_depth=3):
    data = _query_overpass(min_lat, min_lon, max_lat, max_lon)
    if data is not None:
        stops = []
        for el in data.get('elements', []):
            if el.get('type') != 'node' or el.get('lat') is None:
                continue
            tags = el.get('tags', {})
            osm_type = classify_osm_transit_type(tags)
            sid = el.get('id')
            if sid in seen_ids:
                continue
            seen_ids.add(sid)
            stops.append({
                'stop_id': sid,
                'stop_name': tags.get('name') or tags.get('description') or f"{osm_type} ({el['lat']:.4f})",
                'osm_type': osm_type,
                'mapped_type': map_to_berlin_type(osm_type),
                'latitude': el['lat'], 'longitude': el['lon'],
                'lines': tags.get('route_ref', tags.get('line', '')),
            })
        return stops

    if depth >= max_depth:
        return []

    mid_lat = (min_lat + max_lat) / 2
    mid_lon = (min_lon + max_lon) / 2
    all_stops = []
    for bbox in [(min_lat,min_lon,mid_lat,mid_lon),(min_lat,mid_lon,mid_lat,max_lon),
                 (mid_lat,min_lon,max_lat,mid_lon),(mid_lat,mid_lon,max_lat,max_lon)]:
        time.sleep(2)
        all_stops.extend(_fetch_bbox_recursive(*bbox, seen_ids, depth+1, max_depth))
    return all_stops


def enrich_schools_with_transit(schools_df, stops):
    if not stops:
        return schools_df

    df = schools_df.copy()
    stops_df = pd.DataFrame(stops)

    transit_types = ['rail', 'tram', 'bus']
    for tt in transit_types:
        for rank in range(1, TOP_N_STOPS+1):
            p = f"transit_{tt}_{rank:02d}"
            for f in ['name', 'distance_m', 'latitude', 'longitude', 'lines']:
                df[f"{p}_{f}"] = None

    df['transit_stops_500m'] = 0
    df['transit_stop_count_1000m'] = 0
    df['transit_all_lines_1000m'] = None
    df['transit_accessibility_score'] = 0.0

    s_lats = stops_df['latitude'].values
    s_lons = stops_df['longitude'].values
    s_types = stops_df['mapped_type'].values
    s_names = stops_df['stop_name'].values
    s_lines = stops_df['lines'].values

    valid = df[df['latitude'].notna() & df['longitude'].notna()]
    iterator = valid.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Transit enrichment")

    for idx, school in iterator:
        lat, lon = float(school['latitude']), float(school['longitude'])
        stops_500 = stops_1000 = 0
        all_lines = set()
        by_type = defaultdict(list)

        for i in range(len(s_lats)):
            dist = haversine_distance(lat, lon, s_lats[i], s_lons[i])
            if dist <= 500: stops_500 += 1
            if dist <= 1000:
                stops_1000 += 1
                if s_lines[i]:
                    for l in str(s_lines[i]).replace(';', ',').split(','):
                        l = l.strip()
                        if l: all_lines.add(l)
            by_type[s_types[i]].append({'name': s_names[i], 'distance': dist,
                                         'latitude': s_lats[i], 'longitude': s_lons[i],
                                         'lines': s_lines[i] if s_lines[i] else None})

        df.at[idx, 'transit_stops_500m'] = stops_500
        df.at[idx, 'transit_stop_count_1000m'] = stops_1000
        df.at[idx, 'transit_all_lines_1000m'] = ', '.join(sorted(all_lines)) if all_lines else None

        for tt in transit_types:
            ts = sorted(by_type.get(tt, []), key=lambda x: x['distance'])
            seen = set()
            unique = []
            for s in ts:
                nl = s['name'].lower() if s['name'] else ''
                if nl not in seen:
                    seen.add(nl)
                    unique.append(s)
            for rank in range(TOP_N_STOPS):
                p = f"transit_{tt}_{rank+1:02d}"
                if rank < len(unique):
                    s = unique[rank]
                    df.at[idx, f"{p}_name"] = s['name']
                    df.at[idx, f"{p}_distance_m"] = round(s['distance'])
                    df.at[idx, f"{p}_latitude"] = s['latitude']
                    df.at[idx, f"{p}_longitude"] = s['longitude']
                    df.at[idx, f"{p}_lines"] = s['lines']

        score = min(stops_500 * 5, 30) + min(stops_1000 * 2, 20)
        rail = by_type.get('rail', [])
        tram = by_type.get('tram', [])
        if rail and rail[0]['distance'] < 500: score += 25
        elif rail and rail[0]['distance'] < 1000: score += 15
        elif tram and tram[0]['distance'] < 500: score += 20
        elif tram and tram[0]['distance'] < 1000: score += 10
        df.at[idx, 'transit_accessibility_score'] = min(score, 100)

    return df


def enrich_schools(school_type='secondary'):
    input_file = INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_traffic.csv"
    if not input_file.exists():
        input_file = RAW_DIR / f"stuttgart_{school_type}_schools.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Not found: {input_file}")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} {school_type} schools")

    cache_file = CACHE_DIR / "stuttgart_transit_stops.json"
    stops = []
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 7 * 86400:
            with open(cache_file, 'r', encoding='utf-8') as f:
                stops = json.load(f)
            logger.info(f"Loaded {len(stops)} cached transit stops")

    if not stops:
        valid = schools_df[schools_df['latitude'].notna()]
        if valid.empty:
            return schools_df
        margin = 0.02
        min_lat = valid['latitude'].min() - margin
        max_lat = valid['latitude'].max() + margin
        min_lon = valid['longitude'].min() - margin
        max_lon = valid['longitude'].max() + margin

        stops = _fetch_bbox_recursive(min_lat, min_lon, max_lat, max_lon, set())
        logger.info(f"Found {len(stops)} transit stops")

        if stops:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(stops, f, ensure_ascii=False)

    enriched = enrich_schools_with_transit(schools_df, stops)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_transit.csv"
    enriched.to_csv(out, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out}")

    print(f"\n{'='*70}")
    print(f"STUTTGART TRANSIT ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(enriched)}")
    print(f"Avg stops 500m: {enriched['transit_stops_500m'].mean():.1f}")
    print(f"Avg stops 1000m: {enriched['transit_stop_count_1000m'].mean():.1f}")
    print(f"Avg accessibility: {enriched['transit_accessibility_score'].mean():.1f}")
    print(f"{'='*70}")

    return enriched


def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for st in ['primary', 'secondary']:
        for d in [INTERMEDIATE_DIR, RAW_DIR]:
            for p in [f"stuttgart_{st}_schools_with_traffic.csv", f"stuttgart_{st}_schools.csv"]:
                if (d / p).exists():
                    enrich_schools(st)
                    break
            else:
                continue
            break


if __name__ == "__main__":
    main()
