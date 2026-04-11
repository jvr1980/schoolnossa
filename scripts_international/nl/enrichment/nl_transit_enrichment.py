#!/usr/bin/env python3
"""
NL Phase 4: Transit Enrichment via OVapi GTFS

Downloads the national Dutch GTFS feed from gtfs.ovapi.nl and computes
nearest transit stops per school, following Berlin's transit column pattern:
- transit_{mode}_{01-03}_{name,distance_m,latitude,longitude,lines}
- transit_stop_count_1000m, transit_all_lines_1000m, transit_accessibility_score

Modes mapped to Berlin schema:
- rail: Trein (NS), Metro
- tram: Tram
- bus: Bus, Nachtbus

Data source: https://gtfs.ovapi.nl/
Format: Standard GTFS (stops.txt, routes.txt, stop_times.txt, trips.txt)

Input:  data_nl/intermediate/nl_schools_with_traffic.csv (or nl_school_master_geocoded.csv)
Output: data_nl/intermediate/nl_schools_with_transit.csv
"""

import io
import logging
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GTFS_URL = "https://gtfs.ovapi.nl/nl/gtfs-nl.zip"
HEADERS = {"User-Agent": "SchoolNossa/1.0"}

# Route type mapping (GTFS route_type → Berlin transit modes)
ROUTE_TYPE_TO_MODE = {
    0: "tram",    # Tram, Streetcar, Light rail
    1: "rail",    # Subway, Metro
    2: "rail",    # Rail (intercity, regional)
    3: "bus",     # Bus
    4: "bus",     # Ferry → treat as bus for accessibility
    5: "tram",    # Cable tram
    6: "rail",    # Aerial lift → treat as rail
    7: "bus",     # Funicular → treat as bus
    11: "bus",    # Trolleybus
    12: "rail",   # Monorail
    100: "rail",  # Railway (extended types)
    200: "bus",   # Coach
    400: "rail",  # Urban railway
    700: "bus",   # Bus service
    900: "tram",  # Tram service
    1000: "bus",  # Water transport
    1100: "bus",  # Air service
    1300: "bus",  # Telecabin
    1400: "rail", # Funicular
}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Haversine distance in meters."""
    R = 6371000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def download_and_parse_gtfs(cache_dir: Path) -> tuple:
    """Download GTFS and extract stops + route-stop mapping."""
    gtfs_zip = cache_dir / "gtfs-nl.zip"
    stops_cache = cache_dir / "gtfs_stops.csv"
    stop_routes_cache = cache_dir / "gtfs_stop_routes.csv"

    # Check cache
    if stops_cache.exists() and stop_routes_cache.exists():
        logger.info("Loading cached GTFS data...")
        stops = pd.read_csv(stops_cache)
        stop_routes = pd.read_csv(stop_routes_cache)
        return stops, stop_routes

    # Download GTFS
    if not gtfs_zip.exists():
        logger.info(f"Downloading GTFS from {GTFS_URL}...")
        resp = requests.get(GTFS_URL, headers=HEADERS, timeout=120, stream=True)
        resp.raise_for_status()
        gtfs_zip.write_bytes(resp.content)
        logger.info(f"  Downloaded: {gtfs_zip.stat().st_size / 1024 / 1024:.1f} MB")
    else:
        logger.info(f"Using cached GTFS zip: {gtfs_zip.name}")

    # Parse GTFS zip
    logger.info("Parsing GTFS feed...")
    with zipfile.ZipFile(gtfs_zip) as zf:
        # stops.txt
        with zf.open("stops.txt") as f:
            stops = pd.read_csv(f, dtype=str)
        logger.info(f"  stops.txt: {len(stops)} stops")

        # routes.txt
        with zf.open("routes.txt") as f:
            routes = pd.read_csv(f, dtype=str)
        logger.info(f"  routes.txt: {len(routes)} routes")

        # trips.txt — link routes to trips
        with zf.open("trips.txt") as f:
            trips = pd.read_csv(f, dtype=str, usecols=["route_id", "trip_id"])
        logger.info(f"  trips.txt: {len(trips)} trips")

        # stop_times.txt — link trips to stops (large file, only need stop_id + trip_id)
        with zf.open("stop_times.txt") as f:
            stop_times = pd.read_csv(f, dtype=str, usecols=["trip_id", "stop_id"])
        logger.info(f"  stop_times.txt: {len(stop_times)} stop_times")

    # Build stop → routes mapping
    logger.info("Building stop-route mapping...")
    trip_routes = trips.merge(routes[["route_id", "route_short_name", "route_type"]], on="route_id")
    stop_trip_routes = stop_times.merge(trip_routes, on="trip_id")

    # Aggregate: for each stop, list unique routes and their types
    stop_routes = (
        stop_trip_routes.groupby("stop_id")
        .agg({
            "route_short_name": lambda x: ",".join(sorted(set(x.dropna()))),
            "route_type": lambda x: list(set(x.dropna())),
        })
        .reset_index()
    )
    stop_routes.columns = ["stop_id", "lines", "route_types"]

    # Convert route_types to transit mode
    def primary_mode(route_types):
        modes = [ROUTE_TYPE_TO_MODE.get(int(rt), "bus") for rt in route_types]
        # Priority: rail > tram > bus
        if "rail" in modes:
            return "rail"
        if "tram" in modes:
            return "tram"
        return "bus"

    stop_routes["mode"] = stop_routes["route_types"].apply(primary_mode)
    stop_routes = stop_routes.drop(columns=["route_types"])

    # Clean stops
    stops = stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]].copy()
    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    stops = stops.dropna(subset=["stop_lat", "stop_lon"])

    # Merge stops with routes
    stops = stops.merge(stop_routes, on="stop_id", how="left")
    stops["mode"] = stops["mode"].fillna("bus")
    stops["lines"] = stops["lines"].fillna("")

    # Cache
    stops.to_csv(stops_cache, index=False)
    stop_routes.to_csv(stop_routes_cache, index=False)
    logger.info(f"  Cached: {len(stops)} stops with route info")

    return stops, stop_routes


def find_nearest_stops(school_lat, school_lon, stops_df, max_distance_m=1500):
    """Find nearest stops by mode for a single school."""
    # Pre-filter by bounding box (~1.5km ≈ 0.015 degrees)
    delta = 0.015
    mask = (
        (stops_df["stop_lat"] > school_lat - delta)
        & (stops_df["stop_lat"] < school_lat + delta)
        & (stops_df["stop_lon"] > school_lon - delta)
        & (stops_df["stop_lon"] < school_lon + delta)
    )
    nearby = stops_df[mask].copy()

    if nearby.empty:
        return {}, 0, ""

    # Compute distances
    nearby["distance_m"] = haversine_distance(
        school_lat, school_lon,
        nearby["stop_lat"].values, nearby["stop_lon"].values,
    )
    nearby = nearby[nearby["distance_m"] <= max_distance_m]

    if nearby.empty:
        return {}, 0, ""

    # Count stops within 1000m
    within_1000 = nearby[nearby["distance_m"] <= 1000]
    stop_count_1000m = len(within_1000)
    all_lines_1000m = ",".join(sorted(set(
        line for lines in within_1000["lines"].dropna()
        for line in lines.split(",") if line
    )))

    # Find top 3 nearest per mode
    result = {}
    for mode in ["rail", "tram", "bus"]:
        mode_stops = nearby[nearby["mode"] == mode].nsmallest(3, "distance_m")
        for idx, (_, row) in enumerate(mode_stops.iterrows()):
            prefix = f"transit_{mode}_{idx + 1:02d}"
            result[f"{prefix}_name"] = row["stop_name"]
            result[f"{prefix}_distance_m"] = round(row["distance_m"], 0)
            result[f"{prefix}_latitude"] = round(row["stop_lat"], 6)
            result[f"{prefix}_longitude"] = round(row["stop_lon"], 6)
            result[f"{prefix}_lines"] = row["lines"]

    return result, stop_count_1000m, all_lines_1000m


def enrich_schools_with_transit(schools: pd.DataFrame, stops: pd.DataFrame) -> pd.DataFrame:
    """Compute transit metrics for each school."""
    logger.info(f"Computing transit metrics for {len(schools)} schools...")

    all_results = []
    for i, (_, school) in enumerate(schools.iterrows()):
        lat = school.get("latitude")
        lon = school.get("longitude")

        if pd.isna(lat) or pd.isna(lon):
            all_results.append({})
            continue

        result, stop_count, all_lines = find_nearest_stops(float(lat), float(lon), stops)
        result["transit_stop_count_1000m"] = stop_count
        result["transit_all_lines_1000m"] = all_lines

        # Accessibility score (0-10): weighted by stop count and mode diversity
        modes_found = sum(1 for mode in ["rail", "tram", "bus"]
                         if f"transit_{mode}_01_name" in result)
        score = min(10, (stop_count / 5) * 3 + modes_found * 2.5)
        result["transit_accessibility_score"] = round(score, 1)

        all_results.append(result)

        if (i + 1) % 200 == 0:
            logger.info(f"  Progress: {i + 1}/{len(schools)}")

    transit_df = pd.DataFrame(all_results)

    # Merge back
    for col in transit_df.columns:
        schools[col] = transit_df[col].values

    return schools


def main():
    """Run transit enrichment."""
    logger.info("=" * 60)
    logger.info("NL Phase 4: Transit Enrichment (OVapi GTFS)")
    logger.info("=" * 60)

    # Find best available input
    input_candidates = [
        INTERMEDIATE_DIR / "nl_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "nl_school_master_geocoded.csv",
    ]
    input_path = next((p for p in input_candidates if p.exists()), None)
    if input_path is None:
        logger.error("No input file found. Run earlier phases first.")
        sys.exit(1)

    schools = pd.read_csv(input_path)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    # Download/parse GTFS
    stops, _ = download_and_parse_gtfs(CACHE_DIR)
    logger.info(f"GTFS stops: {len(stops)}")

    # Enrich
    enriched = enrich_schools_with_transit(schools, stops)

    # Save
    output_path = INTERMEDIATE_DIR / "nl_schools_with_transit.csv"
    enriched.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")

    has_transit = enriched["transit_accessibility_score"].notna().sum() if "transit_accessibility_score" in enriched.columns else 0
    logger.info(f"Schools with transit data: {has_transit}/{len(enriched)}")


if __name__ == "__main__":
    main()
