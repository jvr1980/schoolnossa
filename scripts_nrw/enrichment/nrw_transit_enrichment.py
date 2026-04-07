#!/usr/bin/env python3
"""
NRW Transit Enrichment
Enriches school data with public transit accessibility using OpenStreetMap Overpass API.

This script:
1. Loads school data with coordinates
2. Queries OSM Overpass API for nearby transit stops (bus, tram, U-Bahn, S-Bahn, Bahn)
3. Finds TOP 3 nearest stops per transport type (rail, tram, bus) with lat/lon/lines
4. Calculates transit accessibility scores
5. Outputs in Berlin-compatible format: transit_{type}_{rank}_{field}

Output columns (Berlin schema compatible):
  transit_rail_01..03_name/distance_m/latitude/longitude/lines
  transit_tram_01..03_name/distance_m/latitude/longitude/lines
  transit_bus_01..03_name/distance_m/latitude/longitude/lines
  transit_stop_count_1000m
  transit_all_lines_1000m
  transit_accessibility_score

Data sources:
- OpenStreetMap Overpass API (free, no API key required)

Author: NRW School Data Pipeline
Created: 2026-02-15
"""

import pandas as pd
import requests
import math
import time
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

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
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Overpass API
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Distance thresholds
RADIUS_500M = 500
RADIUS_1000M = 1000
TOP_N_STOPS = 3

# Request delay to be respectful to Overpass API
REQUEST_DELAY_S = 1.0


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in meters."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _parse_overpass_elements(data: dict) -> List[Dict]:
    """Parse Overpass API response into stop records."""
    stops = []
    for element in data.get('elements', []):
        if element.get('type') != 'node':
            continue

        tags = element.get('tags', {})
        lat = element.get('lat')
        lon = element.get('lon')

        if lat is None or lon is None:
            continue

        # Classify into NRW-specific type first
        osm_type = classify_osm_transit_type(tags)

        # Map to Berlin categories: rail, tram, bus
        mapped_type = map_to_berlin_type(osm_type)

        stop_name = (
            tags.get('name') or
            tags.get('description') or
            tags.get('ref') or
            f"{osm_type} ({lat:.4f}, {lon:.4f})"
        )

        # Extract line info from OSM tags
        lines = tags.get('route_ref', tags.get('line', tags.get('ref', '')))

        stops.append({
            'stop_id': element.get('id'),
            'stop_name': stop_name,
            'osm_type': osm_type,
            'mapped_type': mapped_type,
            'latitude': lat,
            'longitude': lon,
            'network': tags.get('network', ''),
            'operator': tags.get('operator', ''),
            'lines': lines,
        })
    return stops


def _query_overpass(min_lat: float, min_lon: float, max_lat: float, max_lon: float,
                    timeout_s: int = 300) -> Optional[dict]:
    """Send a single Overpass API query. Returns JSON or None."""
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
            headers={'User-Agent': 'SchoolNossa/1.0 (NRW school transit enrichment)'}
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Overpass query failed for bbox ({min_lat:.4f},{min_lon:.4f})-({max_lat:.4f},{max_lon:.4f}): {e}")
        return None


def _fetch_bbox_recursive(min_lat: float, min_lon: float, max_lat: float, max_lon: float,
                           seen_ids: set, depth: int = 0, max_depth: int = 3) -> List[Dict]:
    """Recursively fetch transit stops, splitting bbox on failure."""
    data = _query_overpass(min_lat, min_lon, max_lat, max_lon, timeout_s=300)

    if data is not None:
        stops = _parse_overpass_elements(data)
        unique = []
        for stop in stops:
            if stop['stop_id'] not in seen_ids:
                seen_ids.add(stop['stop_id'])
                unique.append(stop)
        logger.info(f"{'  ' * depth}Found {len(stops)} stops ({len(unique)} new unique)")
        return unique

    if depth >= max_depth:
        logger.warning(f"{'  ' * depth}Max depth reached, skipping bbox ({min_lat:.4f},{min_lon:.4f})-({max_lat:.4f},{max_lon:.4f})")
        return []

    # Split into 2x2 grid
    logger.info(f"{'  ' * depth}Splitting bbox into 4 sub-regions (depth {depth + 1})...")
    mid_lat = (min_lat + max_lat) / 2
    mid_lon = (min_lon + max_lon) / 2

    sub_bboxes = [
        (min_lat, min_lon, mid_lat, mid_lon),
        (min_lat, mid_lon, mid_lat, max_lon),
        (mid_lat, min_lon, max_lat, mid_lon),
        (mid_lat, mid_lon, max_lat, max_lon),
    ]

    all_stops = []
    for i, (s_min_lat, s_min_lon, s_max_lat, s_max_lon) in enumerate(sub_bboxes):
        logger.info(f"{'  ' * depth}  Sub-region {i + 1}/4...")
        time.sleep(2)  # Be polite between requests
        sub_stops = _fetch_bbox_recursive(s_min_lat, s_min_lon, s_max_lat, s_max_lon,
                                           seen_ids, depth + 1, max_depth)
        all_stops.extend(sub_stops)

    return all_stops


def fetch_transit_stops_bulk(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> List[Dict]:
    """Fetch all transit stops in a bounding box using Overpass API.

    If the bbox is large, recursively splits into sub-regions to avoid timeouts.
    """
    logger.info(f"Querying Overpass API for transit stops in bbox ({min_lat:.4f},{min_lon:.4f}) to ({max_lat:.4f},{max_lon:.4f})...")

    seen_ids = set()
    stops = _fetch_bbox_recursive(min_lat, min_lon, max_lat, max_lon, seen_ids)

    logger.info(f"Found {len(stops)} transit stops total")

    # Log breakdown by mapped type
    type_counts = defaultdict(int)
    for stop in stops:
        type_counts[f"{stop['mapped_type']} ({stop['osm_type']})"] += 1
    for t, c in sorted(type_counts.items()):
        logger.info(f"  - {t}: {c}")

    return stops


def classify_osm_transit_type(tags: Dict) -> str:
    """Classify transit type from OSM tags into NRW-specific types."""
    railway = tags.get('railway', '')
    station = tags.get('station', '')

    if station == 'subway' or railway == 'subway_entrance':
        return 'U-Bahn'
    elif station == 'light_rail' or railway == 'tram_stop':
        return 'Straßenbahn'
    elif railway in ('station', 'halt'):
        network = tags.get('network', '').lower()
        name = tags.get('name', '').lower()
        if 's-bahn' in network or 's-bahn' in name or name.startswith('s '):
            return 'S-Bahn'
        elif 'u-bahn' in network or 'u-bahn' in name:
            return 'U-Bahn'
        return 'Bahn'

    if tags.get('bus') == 'yes' or tags.get('highway') == 'bus_stop':
        return 'Bus'

    return 'Bus'


def map_to_berlin_type(osm_type: str) -> str:
    """Map NRW transit types to Berlin's 3 categories."""
    if osm_type in ('U-Bahn', 'S-Bahn', 'Bahn'):
        return 'rail'
    elif osm_type == 'Straßenbahn':
        return 'tram'
    else:
        return 'bus'


def enrich_schools_with_transit(schools_df: pd.DataFrame, stops: List[Dict]) -> pd.DataFrame:
    """Calculate transit metrics for each school in Berlin-compatible format."""
    logger.info("Calculating transit accessibility for schools...")

    if not stops:
        logger.warning("No transit stops available")
        return schools_df

    stops_df = pd.DataFrame(stops)
    schools_df = schools_df.copy()

    # Initialize Berlin-compatible columns
    transit_types = ['rail', 'tram', 'bus']
    for ttype in transit_types:
        for rank in range(1, TOP_N_STOPS + 1):
            prefix = f"transit_{ttype}_{rank:02d}"
            schools_df[f"{prefix}_name"] = None
            schools_df[f"{prefix}_distance_m"] = None
            schools_df[f"{prefix}_latitude"] = None
            schools_df[f"{prefix}_longitude"] = None
            schools_df[f"{prefix}_lines"] = None

    schools_df['transit_stops_500m'] = 0
    schools_df['transit_stop_count_1000m'] = 0
    schools_df['transit_all_lines_1000m'] = None
    schools_df['transit_accessibility_score'] = 0.0

    # Pre-compute stops arrays
    stop_lats = stops_df['latitude'].values
    stop_lons = stops_df['longitude'].values
    stop_mapped_types = stops_df['mapped_type'].values
    stop_names = stops_df['stop_name'].values
    stop_lines = stops_df['lines'].values

    schools_with_coords = schools_df[
        schools_df['latitude'].notna() & schools_df['longitude'].notna()
    ]

    iterator = schools_with_coords.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="Enriching with transit data")

    for idx, school in iterator:
        school_lat = float(school['latitude'])
        school_lon = float(school['longitude'])

        stops_500m = 0
        stops_1000m = 0
        all_lines_1000m = set()

        # Collect all stops by mapped type with distances
        stops_by_type = defaultdict(list)

        for i in range(len(stop_lats)):
            dist = haversine_distance(school_lat, school_lon, stop_lats[i], stop_lons[i])

            if dist <= RADIUS_500M:
                stops_500m += 1
            if dist <= RADIUS_1000M:
                stops_1000m += 1
                # Collect lines within 1000m
                line_str = stop_lines[i]
                if line_str and str(line_str).strip():
                    for line in str(line_str).replace(';', ',').split(','):
                        line = line.strip()
                        if line:
                            all_lines_1000m.add(line)

            mapped_type = stop_mapped_types[i]
            stops_by_type[mapped_type].append({
                'name': stop_names[i],
                'distance': dist,
                'latitude': stop_lats[i],
                'longitude': stop_lons[i],
                'lines': stop_lines[i] if stop_lines[i] else None,
            })

        schools_df.at[idx, 'transit_stops_500m'] = stops_500m
        schools_df.at[idx, 'transit_stop_count_1000m'] = stops_1000m
        schools_df.at[idx, 'transit_all_lines_1000m'] = ', '.join(sorted(all_lines_1000m)) if all_lines_1000m else None

        # For each type, sort by distance and take top 3
        for ttype in transit_types:
            type_stops = stops_by_type.get(ttype, [])
            # Sort by distance
            type_stops.sort(key=lambda x: x['distance'])

            # Deduplicate by name (keep closest)
            seen_names = set()
            unique_stops = []
            for s in type_stops:
                name_lower = s['name'].lower() if s['name'] else ''
                if name_lower not in seen_names:
                    seen_names.add(name_lower)
                    unique_stops.append(s)

            for rank in range(TOP_N_STOPS):
                prefix = f"transit_{ttype}_{rank + 1:02d}"
                if rank < len(unique_stops):
                    stop = unique_stops[rank]
                    schools_df.at[idx, f"{prefix}_name"] = stop['name']
                    schools_df.at[idx, f"{prefix}_distance_m"] = round(stop['distance'])
                    schools_df.at[idx, f"{prefix}_latitude"] = stop['latitude']
                    schools_df.at[idx, f"{prefix}_longitude"] = stop['longitude']
                    schools_df.at[idx, f"{prefix}_lines"] = stop['lines']

        # Calculate accessibility score (0-100)
        score = 0.0
        score += min(stops_500m * 5, 30)
        score += min(stops_1000m * 2, 20)

        # Bonus for rail access
        rail_stops = stops_by_type.get('rail', [])
        tram_stops = stops_by_type.get('tram', [])
        if rail_stops and rail_stops[0]['distance'] < 500:
            score += 25
        elif rail_stops and rail_stops[0]['distance'] < 1000:
            score += 15
        elif tram_stops and tram_stops[0]['distance'] < 500:
            score += 20
        elif tram_stops and tram_stops[0]['distance'] < 1000:
            score += 10

        schools_df.at[idx, 'transit_accessibility_score'] = min(score, 100)

    return schools_df


def save_output(df: pd.DataFrame, school_type: str):
    """Save enriched data."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_transit.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame, school_type: str):
    """Print transit enrichment summary."""
    print(f"\n{'=' * 70}")
    print(f"NRW TRANSIT ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")

    if 'transit_stops_500m' in df.columns:
        print(f"\nAverage transit stops within 500m: {df['transit_stops_500m'].mean():.1f}")
        print(f"Average transit stops within 1000m: {df['transit_stop_count_1000m'].mean():.1f}")

    for ttype, label in [('rail', 'Rail (U-Bahn/S-Bahn/Bahn)'), ('tram', 'Tram/Straßenbahn'), ('bus', 'Bus')]:
        print(f"\n  {label}:")
        for rank in range(1, TOP_N_STOPS + 1):
            dist_col = f'transit_{ttype}_{rank:02d}_distance_m'
            if dist_col in df.columns:
                count = df[dist_col].notna().sum()
                if count > 0:
                    avg_dist = df[dist_col].mean()
                    print(f"    #{rank}: {count}/{len(df)} schools, avg {avg_dist:.0f}m")

    if 'transit_accessibility_score' in df.columns:
        print(f"\nTransit Accessibility Score:")
        print(f"  Mean: {df['transit_accessibility_score'].mean():.1f}")
        print(f"  Median: {df['transit_accessibility_score'].median():.1f}")

    if 'transit_all_lines_1000m' in df.columns:
        count = df['transit_all_lines_1000m'].notna().sum()
        print(f"\nSchools with line data: {count}/{len(df)}")

    print(f"\n{'=' * 70}")


def enrich_schools(school_type: str = "secondary") -> pd.DataFrame:
    """Run transit enrichment for a school type."""

    # Load school data
    input_file = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_traffic.csv"
    if not input_file.exists():
        input_file = RAW_DIR / f"nrw_{school_type}_schools.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"School data not found: {input_file}")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} {school_type} schools")

    # Check for cached transit stops
    cache_file = CACHE_DIR / "nrw_transit_stops.json"
    stops = []

    if cache_file.exists():
        cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if cache_age < 7 * 86400:  # 7 day cache
            logger.info("Loading transit stops from cache...")
            with open(cache_file, 'r', encoding='utf-8') as f:
                stops = json.load(f)

            # Re-map types if cache is from old format (without mapped_type)
            if stops and 'mapped_type' not in stops[0]:
                logger.info("Re-mapping transit types from old cache format...")
                for stop in stops:
                    osm_type = stop.get('transit_type', stop.get('osm_type', 'Bus'))
                    stop['osm_type'] = osm_type
                    stop['mapped_type'] = map_to_berlin_type(osm_type)
                # Delete old cache so new format gets saved
                cache_file.unlink()

            logger.info(f"Loaded {len(stops)} cached transit stops")

    if not stops:
        # Calculate bounding box for all schools
        valid_schools = schools_df[
            schools_df['latitude'].notna() & schools_df['longitude'].notna()
        ]

        if valid_schools.empty:
            logger.warning("No schools with coordinates found")
            return schools_df

        # Add margin (~2km)
        margin = 0.02
        min_lat = valid_schools['latitude'].min() - margin
        max_lat = valid_schools['latitude'].max() + margin
        min_lon = valid_schools['longitude'].min() - margin
        max_lon = valid_schools['longitude'].max() + margin

        stops = fetch_transit_stops_bulk(min_lat, min_lon, max_lat, max_lon)

        # Cache results
        if stops:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(stops, f, ensure_ascii=False)

    # Enrich schools
    enriched_df = enrich_schools_with_transit(schools_df, stops)

    # Save
    save_output(enriched_df, school_type)

    # Summary
    print_summary(enriched_df, school_type)

    return enriched_df


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting NRW Transit Enrichment (Overpass API)")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    for school_type in ['secondary', 'primary']:
        for parent_dir in [INTERMEDIATE_DIR, RAW_DIR]:
            input_file = parent_dir / f"nrw_{school_type}_schools_with_traffic.csv"
            if not input_file.exists():
                input_file = parent_dir / f"nrw_{school_type}_schools.csv"
            if input_file.exists():
                enrich_schools(school_type)
                break
        else:
            logger.warning(f"No {school_type} school data found")


if __name__ == "__main__":
    main()
