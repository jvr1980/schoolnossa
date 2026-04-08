#!/usr/bin/env python3
"""
Phase 3: Leipzig Transit Enrichment (LVB GTFS + Overpass API)
=============================================================

Enriches school data with public transit accessibility using LVB GTFS data
and OpenStreetMap Overpass API as fallback.

HYBRID approach:
1. PRIMARY: Download LVB GTFS ZIP from Leipzig Open Data portal
   - Extract stops.txt (stop_id, stop_name, stop_lat, stop_lon)
   - Extract routes.txt (route_id, route_short_name, route_type) - 0=tram, 3=bus, 2=rail
   - Extract trips.txt (trip_id, route_id)
   - Extract stop_times.txt (trip_id, stop_id)
   - Join: stop -> stop_times -> trips -> routes to map each stop to its route types and line names
2. FALLBACK: Use Overpass API (same as NRW) if GTFS download fails
   Leipzig bbox: 51.24,12.20,51.45,12.55

Output columns (Berlin schema compatible):
    transit_rail_01..03_name/distance_m/latitude/longitude/lines
    transit_tram_01..03_name/distance_m/latitude/longitude/lines
    transit_bus_01..03_name/distance_m/latitude/longitude/lines
    transit_stop_count_1000m
    transit_all_lines_1000m
    transit_accessibility_score

Input: data_leipzig/intermediate/leipzig_schools_with_traffic.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_transit.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import requests
import math
import time
import json
import csv
import io
import zipfile
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
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# GTFS data source
LVB_GTFS_URL = (
    "https://opendata.leipzig.de/dataset/"
    "8803f612-2ce1-4643-82d1-213434889200/resource/"
    "b38955c4-431c-4e8b-a4ef-9964a3a2c95d/download/gtfsmdvlvb.zip"
)

# Overpass API (fallback)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Leipzig bounding box for Overpass fallback
LEIPZIG_BBOX = (51.24, 12.20, 51.45, 12.55)

# Distance thresholds
RADIUS_500M = 500
RADIUS_1000M = 1000
TOP_N_STOPS = 3

# GTFS route_type mapping (https://gtfs.org/schedule/reference/#routestxt)
# 0 = Tram/Streetcar, 1 = Subway/Metro, 2 = Rail, 3 = Bus,
# 4 = Ferry, 5 = Cable tram, 6 = Aerial lift, 7 = Funicular
GTFS_ROUTE_TYPE_MAP = {
    0: 'tram',
    1: 'rail',    # subway/metro -> rail
    2: 'rail',    # rail
    3: 'bus',
    4: 'bus',     # ferry -> bus (rare, fallback)
    5: 'tram',    # cable tram -> tram
    6: 'rail',    # aerial lift -> rail
    7: 'rail',    # funicular -> rail
}

# Fallback input chain
INPUT_FALLBACKS = [
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_crime.csv",
    INTERMEDIATE_DIR / "leipzig_schools.csv",
    RAW_DIR / "leipzig_schools_raw.csv",
    RAW_DIR / "leipzig_schools.csv",
]

# Request headers
HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Leipzig school transit enrichment)',
}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in meters."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (math.sin(delta_phi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# ---------------------------------------------------------------------------
# PRIMARY: GTFS-based transit stop loading
# ---------------------------------------------------------------------------

def download_gtfs_zip(url: str, cache_path: Path) -> Optional[Path]:
    """Download GTFS ZIP to cache. Returns path on success, None on failure."""
    if cache_path.exists():
        cache_age_days = (datetime.now().timestamp() - cache_path.stat().st_mtime) / 86400
        if cache_age_days < 30:
            logger.info(f"Using cached GTFS ZIP ({cache_age_days:.0f} days old): {cache_path}")
            return cache_path
        else:
            logger.info(f"GTFS cache expired ({cache_age_days:.0f} days), re-downloading...")

    logger.info(f"Downloading GTFS ZIP from {url} ...")
    try:
        response = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        response.raise_for_status()

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = cache_path.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded GTFS ZIP ({size_mb:.1f} MB) -> {cache_path}")
        return cache_path
    except Exception as e:
        logger.warning(f"GTFS download failed: {e}")
        return None


def _read_csv_from_zip(zf: zipfile.ZipFile, filename: str) -> Optional[List[Dict]]:
    """Read a CSV file from inside a ZIP archive, returning list of dicts."""
    # GTFS files may be at root or in a subdirectory
    matching = [n for n in zf.namelist() if n.endswith(filename)]
    if not matching:
        logger.warning(f"{filename} not found in GTFS ZIP")
        return None

    target = matching[0]
    logger.info(f"Reading {target} from GTFS ZIP...")
    with zf.open(target) as f:
        text = io.TextIOWrapper(f, encoding='utf-8-sig')
        reader = csv.DictReader(text)
        rows = list(reader)
    logger.info(f"  -> {len(rows)} rows")
    return rows


def parse_gtfs_stops(gtfs_zip_path: Path) -> List[Dict]:
    """Parse GTFS ZIP and return transit stops with route type and line info.

    Join chain: stops.txt -> stop_times.txt -> trips.txt -> routes.txt
    This maps each stop to the set of routes (and their types/names) that serve it.
    """
    logger.info("Parsing GTFS data...")

    with zipfile.ZipFile(gtfs_zip_path, 'r') as zf:
        stops_raw = _read_csv_from_zip(zf, 'stops.txt')
        routes_raw = _read_csv_from_zip(zf, 'routes.txt')
        trips_raw = _read_csv_from_zip(zf, 'trips.txt')
        stop_times_raw = _read_csv_from_zip(zf, 'stop_times.txt')

    if not all([stops_raw, routes_raw, trips_raw, stop_times_raw]):
        logger.error("Missing required GTFS files")
        return []

    # --- Build route lookup: route_id -> {route_type, route_short_name} ---
    route_info: Dict[str, Dict] = {}
    for r in routes_raw:
        rid = r.get('route_id', '')
        try:
            rtype = int(r.get('route_type', 3))
        except (ValueError, TypeError):
            rtype = 3
        route_info[rid] = {
            'route_type': rtype,
            'route_short_name': r.get('route_short_name', '').strip(),
            'route_long_name': r.get('route_long_name', '').strip(),
        }

    logger.info(f"  Routes: {len(route_info)}")

    # --- Build trip -> route_id lookup ---
    trip_to_route: Dict[str, str] = {}
    for t in trips_raw:
        trip_to_route[t.get('trip_id', '')] = t.get('route_id', '')

    logger.info(f"  Trips: {len(trip_to_route)}")

    # --- Build stop -> set of route_ids via stop_times ---
    # This is the heaviest join; stop_times can be millions of rows.
    # We only need the distinct (stop_id, route_id) pairs.
    stop_route_pairs: Dict[str, set] = defaultdict(set)

    logger.info(f"  Processing {len(stop_times_raw)} stop_time records...")
    for st in stop_times_raw:
        sid = st.get('stop_id', '')
        tid = st.get('trip_id', '')
        rid = trip_to_route.get(tid)
        if rid:
            stop_route_pairs[sid].add(rid)

    logger.info(f"  Stops with route data: {len(stop_route_pairs)}")

    # --- Build final stop list ---
    stops: List[Dict] = []
    skipped_no_coords = 0

    for s in stops_raw:
        try:
            lat = float(s.get('stop_lat', ''))
            lon = float(s.get('stop_lon', ''))
        except (ValueError, TypeError):
            skipped_no_coords += 1
            continue

        if lat == 0.0 or lon == 0.0:
            skipped_no_coords += 1
            continue

        stop_id = s.get('stop_id', '')
        stop_name = s.get('stop_name', f"Stop ({lat:.4f}, {lon:.4f})")

        # Determine route types and line names for this stop
        route_ids = stop_route_pairs.get(stop_id, set())

        # Collect mapped types and line names
        mapped_types: set = set()
        line_names: set = set()

        for rid in route_ids:
            ri = route_info.get(rid)
            if ri:
                gtfs_type = ri['route_type']
                mapped = GTFS_ROUTE_TYPE_MAP.get(gtfs_type, 'bus')
                mapped_types.add(mapped)
                name = ri['route_short_name'] or ri['route_long_name']
                if name:
                    line_names.add(name)

        # If a stop has no route data, try to infer type from name
        if not mapped_types:
            name_lower = stop_name.lower()
            if any(kw in name_lower for kw in ('s-bahn', 'hbf', 'bahnhof', 'hauptbahnhof')):
                mapped_types.add('rail')
            elif any(kw in name_lower for kw in ('straßenbahn', 'tram')):
                mapped_types.add('tram')
            else:
                # Skip parent stations (location_type=1) without route data
                location_type = s.get('location_type', '0')
                if location_type == '1':
                    continue
                mapped_types.add('bus')  # Default assumption

        lines_str = ', '.join(sorted(line_names)) if line_names else None

        # A stop may serve multiple types (e.g. tram+bus at same platform).
        # Create one entry per mapped_type so distance calculations work per type.
        for mtype in mapped_types:
            stops.append({
                'stop_id': stop_id,
                'stop_name': stop_name,
                'mapped_type': mtype,
                'latitude': lat,
                'longitude': lon,
                'lines': lines_str,
            })

    if skipped_no_coords:
        logger.info(f"  Skipped {skipped_no_coords} stops without valid coordinates")

    # Deduplicate: same stop_name + mapped_type at same location
    seen: set = set()
    unique_stops: List[Dict] = []
    for stop in stops:
        key = (stop['stop_name'], stop['mapped_type'],
               round(stop['latitude'], 5), round(stop['longitude'], 5))
        if key not in seen:
            seen.add(key)
            unique_stops.append(stop)

    logger.info(f"  Final: {len(unique_stops)} unique stop entries "
                f"(from {len(stops)} before dedup)")

    # Log breakdown by type
    type_counts: Dict[str, int] = defaultdict(int)
    for stop in unique_stops:
        type_counts[stop['mapped_type']] += 1
    for t, c in sorted(type_counts.items()):
        logger.info(f"    {t}: {c}")

    return unique_stops


# ---------------------------------------------------------------------------
# FALLBACK: Overpass API (same approach as NRW)
# ---------------------------------------------------------------------------

def classify_osm_transit_type(tags: Dict) -> str:
    """Classify transit type from OSM tags into Berlin categories."""
    railway = tags.get('railway', '')
    station = tags.get('station', '')

    if station == 'subway' or railway == 'subway_entrance':
        return 'rail'
    elif railway == 'tram_stop' or station == 'light_rail':
        return 'tram'
    elif railway in ('station', 'halt'):
        return 'rail'

    if tags.get('bus') == 'yes' or tags.get('highway') == 'bus_stop':
        return 'bus'

    return 'bus'


def fetch_overpass_stops(bbox: Tuple[float, float, float, float]) -> List[Dict]:
    """Fetch transit stops via Overpass API for the Leipzig bounding box."""
    min_lat, min_lon, max_lat, max_lon = bbox
    logger.info(f"Querying Overpass API for Leipzig bbox "
                f"({min_lat},{min_lon},{max_lat},{max_lon})...")

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
      node["station"="light_rail"]({min_lat},{min_lon},{max_lat},{max_lon});
    );
    out body;
    """

    try:
        response = requests.post(
            OVERPASS_URL,
            data={'data': query},
            timeout=360,
            headers=HEADERS,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Overpass query failed: {e}")
        return []

    stops: List[Dict] = []
    for element in data.get('elements', []):
        if element.get('type') != 'node':
            continue
        tags = element.get('tags', {})
        lat = element.get('lat')
        lon = element.get('lon')
        if lat is None or lon is None:
            continue

        mapped_type = classify_osm_transit_type(tags)

        stop_name = (
            tags.get('name') or
            tags.get('description') or
            tags.get('ref') or
            f"{mapped_type} ({lat:.4f}, {lon:.4f})"
        )

        lines = tags.get('route_ref', tags.get('line', tags.get('ref', '')))

        stops.append({
            'stop_id': str(element.get('id', '')),
            'stop_name': stop_name,
            'mapped_type': mapped_type,
            'latitude': lat,
            'longitude': lon,
            'lines': lines if lines else None,
        })

    logger.info(f"Overpass returned {len(stops)} transit stops")

    type_counts: Dict[str, int] = defaultdict(int)
    for stop in stops:
        type_counts[stop['mapped_type']] += 1
    for t, c in sorted(type_counts.items()):
        logger.info(f"  {t}: {c}")

    return stops


# ---------------------------------------------------------------------------
# Load transit stops (GTFS primary, Overpass fallback)
# ---------------------------------------------------------------------------

def load_transit_stops() -> List[Dict]:
    """Load transit stops using GTFS (primary) or Overpass (fallback).

    Returns list of stop dicts with keys:
      stop_id, stop_name, mapped_type, latitude, longitude, lines
    """
    # Check JSON cache first
    json_cache = CACHE_DIR / "leipzig_transit_stops.json"
    if json_cache.exists():
        cache_age_days = (datetime.now().timestamp() - json_cache.stat().st_mtime) / 86400
        if cache_age_days < 30:
            logger.info(f"Loading transit stops from JSON cache ({cache_age_days:.0f} days old)...")
            with open(json_cache, 'r', encoding='utf-8') as f:
                stops = json.load(f)
            logger.info(f"Loaded {len(stops)} cached transit stops")
            return stops

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # --- PRIMARY: GTFS ---
    gtfs_zip_path = CACHE_DIR / "gtfsmdvlvb.zip"
    gtfs_path = download_gtfs_zip(LVB_GTFS_URL, gtfs_zip_path)

    stops: List[Dict] = []
    if gtfs_path:
        try:
            stops = parse_gtfs_stops(gtfs_path)
        except Exception as e:
            logger.warning(f"GTFS parsing failed: {e}")
            import traceback
            traceback.print_exc()
            stops = []

    # --- FALLBACK: Overpass ---
    if not stops:
        logger.info("GTFS data unavailable, falling back to Overpass API...")
        stops = fetch_overpass_stops(LEIPZIG_BBOX)

    if not stops:
        logger.error("No transit stops obtained from any source")
        return []

    # Cache as JSON for fast reload
    with open(json_cache, 'w', encoding='utf-8') as f:
        json.dump(stops, f, ensure_ascii=False)
    logger.info(f"Cached {len(stops)} transit stops -> {json_cache}")

    return stops


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------

def enrich_schools_with_transit(schools_df: pd.DataFrame,
                                 stops: List[Dict]) -> pd.DataFrame:
    """Calculate transit metrics for each school in Berlin-compatible format.

    For each school:
    - Find nearest 3 stops per type (rail, tram, bus) with name, distance, coords, lines
    - Count total stops within 1000m
    - Collect all unique lines within 1000m
    - Calculate transit accessibility score (0-100)
    """
    logger.info("Calculating transit accessibility for schools...")

    if not stops:
        logger.warning("No transit stops available")
        return schools_df

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

    schools_df['transit_stop_count_1000m'] = 0
    schools_df['transit_all_lines_1000m'] = None
    schools_df['transit_accessibility_score'] = 0.0

    # Pre-compute arrays for fast distance calculation
    stops_df = pd.DataFrame(stops)
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

        stops_1000m = 0
        all_lines_1000m: set = set()
        stops_by_type: Dict[str, List[Dict]] = defaultdict(list)

        for i in range(len(stop_lats)):
            dist = haversine_distance(school_lat, school_lon,
                                      stop_lats[i], stop_lons[i])

            if dist <= RADIUS_1000M:
                stops_1000m += 1
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

        schools_df.at[idx, 'transit_stop_count_1000m'] = stops_1000m
        schools_df.at[idx, 'transit_all_lines_1000m'] = (
            ', '.join(sorted(all_lines_1000m)) if all_lines_1000m else None
        )

        # For each type, sort by distance and take top 3 (deduplicated by name)
        for ttype in transit_types:
            type_stops = stops_by_type.get(ttype, [])
            type_stops.sort(key=lambda x: x['distance'])

            seen_names: set = set()
            unique_stops: List[Dict] = []
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

        # --- Transit accessibility score (0-100) ---
        score = 0.0

        # Points for stop density
        stops_500m = sum(
            1 for s_list in stops_by_type.values()
            for s in s_list if s['distance'] <= RADIUS_500M
        )
        score += min(stops_500m * 5, 30)   # Up to 30 pts for stops within 500m
        score += min(stops_1000m * 2, 20)  # Up to 20 pts for stops within 1000m

        # Bonus for rail/tram access (Leipzig has extensive tram network)
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


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_school_data() -> pd.DataFrame:
    """Load school data with fallback chain."""
    for path in INPUT_FALLBACKS:
        if path.exists():
            df = pd.read_csv(path)
            logger.info(f"Loaded {len(df)} schools from {path}")
            return df

    raise FileNotFoundError(
        "No school data found. Checked:\n" +
        "\n".join(f"  - {p}" for p in INPUT_FALLBACKS)
    )


def save_output(df: pd.DataFrame):
    """Save enriched data to intermediate directory."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame):
    """Print transit enrichment summary."""
    print(f"\n{'=' * 70}")
    print("LEIPZIG TRANSIT ENRICHMENT - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")

    if 'transit_stop_count_1000m' in df.columns:
        avg_1000 = df['transit_stop_count_1000m'].mean()
        print(f"Average transit stops within 1000m: {avg_1000:.1f}")

    for ttype, label in [('rail', 'Rail (S-Bahn/Regional)'),
                          ('tram', 'Tram (Strassenbahn)'),
                          ('bus', 'Bus')]:
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
        print(f"  Mean:   {df['transit_accessibility_score'].mean():.1f}")
        print(f"  Median: {df['transit_accessibility_score'].median():.1f}")
        print(f"  Max:    {df['transit_accessibility_score'].max():.1f}")

    if 'transit_all_lines_1000m' in df.columns:
        count = df['transit_all_lines_1000m'].notna().sum()
        print(f"\nSchools with line data: {count}/{len(df)}")

    print(f"\n{'=' * 70}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Enrich Leipzig schools with transit accessibility data."""
    logger.info("=" * 60)
    logger.info("Starting Leipzig Transit Enrichment (GTFS primary, Overpass fallback)")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load school data
    schools_df = load_school_data()

    # 2. Load transit stops (GTFS or Overpass)
    stops = load_transit_stops()

    if not stops:
        logger.error("No transit stops available. Exiting.")
        return None

    # 3. Enrich schools
    enriched_df = enrich_schools_with_transit(schools_df, stops)

    # 4. Save
    save_output(enriched_df)

    # 5. Summary
    print_summary(enriched_df)

    logger.info("Leipzig transit enrichment complete!")
    return enriched_df


if __name__ == "__main__":
    main()
