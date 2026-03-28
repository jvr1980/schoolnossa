#!/usr/bin/env python3
"""
Enriches Berlin primary school (Grundschule) data with public transit stop information from BVG API.

This script:
1. Loads school data with lat/lon coordinates
2. For each school, queries the BVG API for nearby transit stops (no distance limit)
3. Categorizes stops by transport type (rail, tram, bus)
4. Extracts the TOP 3 nearest stops for each type with coordinates and line information
5. Calculates an accessibility score
6. Saves the enriched data back to CSV and XLSX

Data source: https://v6.bvg.transport.rest (free, no API key required)
"""

import pandas as pd
import requests
import time
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Install with: pip install tqdm")

# Configuration
BVG_API_BASE = "https://v6.bvg.transport.rest"
SEARCH_RADIUS_M = 15000  # 15km - effectively unlimited for Berlin
REQUEST_DELAY_S = 0.7  # Stay under 100 req/min rate limit
MAX_RESULTS = 100  # Get more results to find all transport types
TOP_N_STOPS = 3  # Store top 3 nearest stops per type

# File paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_berlin_primary"

SCHOOLS_FILE = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.csv"
OUTPUT_CSV = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.csv"
OUTPUT_XLSX = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.xlsx"


def fetch_nearby_stops(lat: float, lon: float, radius: int = SEARCH_RADIUS_M) -> List[dict]:
    """
    Fetch nearby transit stops from BVG API.

    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        radius: Search radius in meters

    Returns:
        List of stop dictionaries from BVG API
    """
    url = f"{BVG_API_BASE}/locations/nearby"
    params = {
        "latitude": lat,
        "longitude": lon,
        "results": MAX_RESULTS,
        "distance": radius,
        "linesOfStops": "true"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"  API error: {e}")
        return []


def categorize_by_transport_type(stops: List[dict]) -> Dict[str, List[dict]]:
    """
    Group stops by transport type (rail, tram, bus).

    A stop can appear in multiple categories if it serves multiple transport types.

    Args:
        stops: List of stop dictionaries from BVG API

    Returns:
        Dictionary with keys 'rail', 'tram', 'bus' containing lists of stops
    """
    categorized = {"rail": [], "tram": [], "bus": []}

    for stop in stops:
        products = stop.get("products", {})

        # Rail = S-Bahn (suburban) + U-Bahn (subway)
        if products.get("suburban") or products.get("subway"):
            categorized["rail"].append(stop)

        # Tram
        if products.get("tram"):
            categorized["tram"].append(stop)

        # Bus
        if products.get("bus"):
            categorized["bus"].append(stop)

    # Sort each category by distance (nearest first)
    for key in categorized:
        categorized[key].sort(key=lambda x: x.get("distance", 999999))

    return categorized


def extract_lines(stop: dict, transport_type: str) -> str:
    """
    Extract line names for a specific transport type from a stop.

    Args:
        stop: Stop dictionary from BVG API
        transport_type: One of 'rail', 'tram', 'bus'

    Returns:
        Comma-separated string of line names (e.g., "U2, U5, U8")
    """
    lines = stop.get("lines", [])

    # Map our categories to BVG product types
    type_map = {
        "rail": ["suburban", "subway"],
        "tram": ["tram"],
        "bus": ["bus"]
    }

    relevant_products = type_map.get(transport_type, [])
    relevant_lines = [
        line["name"]
        for line in lines
        if line.get("product") in relevant_products
    ]

    # Remove duplicates and sort
    unique_lines = sorted(set(relevant_lines))
    return ", ".join(unique_lines)


def calculate_accessibility_score(categorized: Dict[str, List[dict]], all_stops: List[dict]) -> int:
    """
    Calculate a 0-100 accessibility score based on transit proximity.

    Scoring (optimized for primary school children):
    - Rail within 500m: +40 points (good for parents commuting)
    - Rail within 1000m: +25 points
    - Rail within 2000m: +10 points
    - Tram within 500m: +25 points (good for older children)
    - Tram within 1000m: +15 points
    - Bus within 300m: +20 points (most accessible for young children)
    - Bus within 500m: +10 points
    - Line diversity bonus: up to +10 points

    Args:
        categorized: Dict of stops by transport type
        all_stops: All stops within search radius

    Returns:
        Accessibility score from 0 to 100
    """
    score = 0

    # Rail scoring (most important for commuting)
    if categorized["rail"]:
        dist = categorized["rail"][0].get("distance", 999999)
        if dist <= 500:
            score += 40
        elif dist <= 1000:
            score += 25
        elif dist <= 2000:
            score += 10

    # Tram scoring
    if categorized["tram"]:
        dist = categorized["tram"][0].get("distance", 999999)
        if dist <= 500:
            score += 25
        elif dist <= 1000:
            score += 15

    # Bus scoring (usually available everywhere)
    if categorized["bus"]:
        dist = categorized["bus"][0].get("distance", 999999)
        if dist <= 300:
            score += 20
        elif dist <= 500:
            score += 10

    # Line diversity bonus (count lines within 1000m)
    all_lines = set()
    for stop in all_stops:
        if stop.get("distance", 999999) <= 1000:
            for line in stop.get("lines", []):
                line_name = line.get("name")
                if line_name:
                    all_lines.add(line_name)

    # +1 point for every 3 lines, up to 10 points
    diversity_bonus = min(len(all_lines) // 3, 10)
    score += diversity_bonus

    return min(score, 100)


def get_all_lines_in_radius(stops: List[dict], radius: int = 1000) -> str:
    """
    Extract all unique line names from stops within radius.

    Args:
        stops: List of stop dictionaries
        radius: Radius in meters to consider

    Returns:
        Comma-separated string of all unique line names, sorted
    """
    all_lines = set()
    for stop in stops:
        if stop.get("distance", 999999) <= radius:
            for line in stop.get("lines", []):
                line_name = line.get("name")
                if line_name:
                    all_lines.add(line_name)

    return ", ".join(sorted(all_lines))


def enrich_school(lat: float, lon: float) -> dict:
    """
    Fetch and process transit data for one school.

    Args:
        lat: School latitude
        lon: School longitude

    Returns:
        Dictionary with all transit columns for this school
    """
    # Fetch nearby stops (large radius to find all types)
    stops = fetch_nearby_stops(lat, lon, SEARCH_RADIUS_M)

    # Categorize by transport type
    categorized = categorize_by_transport_type(stops)

    # Build result dictionary
    result = {}

    # Process each transport type - store TOP 3 nearest
    for transport_type in ["rail", "tram", "bus"]:
        type_stops = categorized[transport_type]

        for i in range(TOP_N_STOPS):
            rank = f"{i+1:02d}"  # 01, 02, 03
            prefix = f"transit_{transport_type}_{rank}"

            if i < len(type_stops):
                stop = type_stops[i]

                # Clean up stop name (remove " (Berlin)" suffix)
                name = stop.get("name", "")
                name = name.replace(" (Berlin)", "")

                # Get coordinates
                location = stop.get("location", {})

                result[f"{prefix}_name"] = name
                result[f"{prefix}_distance_m"] = stop.get("distance")
                result[f"{prefix}_latitude"] = location.get("latitude")
                result[f"{prefix}_longitude"] = location.get("longitude")
                result[f"{prefix}_lines"] = extract_lines(stop, transport_type)
            else:
                result[f"{prefix}_name"] = None
                result[f"{prefix}_distance_m"] = None
                result[f"{prefix}_latitude"] = None
                result[f"{prefix}_longitude"] = None
                result[f"{prefix}_lines"] = None

    # Summary fields
    stops_within_1000m = [s for s in stops if s.get("distance", 999999) <= 1000]
    result["transit_stop_count_1000m"] = len(stops_within_1000m)
    result["transit_all_lines_1000m"] = get_all_lines_in_radius(stops, 1000)
    result["transit_accessibility_score"] = calculate_accessibility_score(categorized, stops)

    return result


def main():
    """Main function to enrich all primary schools with transit data."""
    print("="*70)
    print("ENRICHING GRUNDSCHULE DATA WITH PUBLIC TRANSIT INFORMATION")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data source: BVG Transport REST API (free, no authentication)")
    print(f"Search radius: {SEARCH_RADIUS_M}m (no practical limit)")
    print(f"Storing top {TOP_N_STOPS} nearest stops per transport type")

    # Check if input file exists
    if not SCHOOLS_FILE.exists():
        print(f"\nError: Input file not found: {SCHOOLS_FILE}")
        print("Please run the traffic enrichment script first (which includes geocoding)")
        return

    # Load schools
    print("\nLoading school data...")
    df = pd.read_csv(SCHOOLS_FILE)
    print(f"Loaded {len(df)} Grundschulen")

    # Count schools with coordinates
    schools_with_coords = df['latitude'].notna().sum()
    print(f"Schools with coordinates: {schools_with_coords}")

    if schools_with_coords == 0:
        print("No schools have coordinates. Please run geocoding first.")
        return

    # Remove old transit columns if they exist
    old_transit_cols = [c for c in df.columns if c.startswith('transit_')]
    if old_transit_cols:
        df = df.drop(columns=old_transit_cols)
        print(f"Removed {len(old_transit_cols)} old transit columns")

    # Process each school
    print(f"\nFetching transit data for {schools_with_coords} schools...")
    print(f"Estimated time: {schools_with_coords * REQUEST_DELAY_S / 60:.1f} minutes")
    print()

    processed = 0
    errors = 0

    if TQDM_AVAILABLE:
        iterator = tqdm(df.iterrows(), total=len(df), desc="Processing schools")
    else:
        iterator = df.iterrows()

    for idx, row in iterator:
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            try:
                transit_data = enrich_school(row['latitude'], row['longitude'])

                # Update dataframe
                for col, val in transit_data.items():
                    df.at[idx, col] = val

                processed += 1

            except Exception as e:
                errors += 1
                if not TQDM_AVAILABLE:
                    print(f"  Error processing {row['schulname']}: {e}")

            # Rate limiting
            time.sleep(REQUEST_DELAY_S)

    # Save results
    print("\n" + "="*70)
    print("SAVING RESULTS")
    print("="*70)

    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Saved: {OUTPUT_CSV}")

    df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"Saved: {OUTPUT_XLSX}")

    # Summary statistics
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    print(f"\nProcessed: {processed} schools")
    print(f"Errors: {errors}")

    # Count new columns
    transit_cols = [c for c in df.columns if c.startswith('transit_')]
    print(f"\nTransit columns added: {len(transit_cols)}")

    print(f"\nTransit coverage (nearest stop found):")
    print(f"  - Rail (U/S-Bahn): {df['transit_rail_01_name'].notna().sum()} ({100*df['transit_rail_01_name'].notna().sum()/len(df):.1f}%)")
    print(f"  - Tram: {df['transit_tram_01_name'].notna().sum()} ({100*df['transit_tram_01_name'].notna().sum()/len(df):.1f}%)")
    print(f"  - Bus: {df['transit_bus_01_name'].notna().sum()} ({100*df['transit_bus_01_name'].notna().sum()/len(df):.1f}%)")

    if df['transit_rail_01_distance_m'].notna().any():
        print(f"\nDistance to nearest stop (all schools):")
        print(f"  - Rail: min={df['transit_rail_01_distance_m'].min():.0f}m, max={df['transit_rail_01_distance_m'].max():.0f}m, avg={df['transit_rail_01_distance_m'].mean():.0f}m")
        if df['transit_tram_01_distance_m'].notna().any():
            print(f"  - Tram: min={df['transit_tram_01_distance_m'].min():.0f}m, max={df['transit_tram_01_distance_m'].max():.0f}m, avg={df['transit_tram_01_distance_m'].mean():.0f}m")
        print(f"  - Bus: min={df['transit_bus_01_distance_m'].min():.0f}m, max={df['transit_bus_01_distance_m'].max():.0f}m, avg={df['transit_bus_01_distance_m'].mean():.0f}m")

    print(f"\nAccessibility score distribution:")
    print(f"  - Min: {df['transit_accessibility_score'].min()}")
    print(f"  - Median: {df['transit_accessibility_score'].median()}")
    print(f"  - Max: {df['transit_accessibility_score'].max()}")
    print(f"  - Mean: {df['transit_accessibility_score'].mean():.1f}")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
