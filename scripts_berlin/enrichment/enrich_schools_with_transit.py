#!/usr/bin/env python3
"""
Enriches school data with public transit stop information from BVG API.

This script:
1. Loads school data with lat/lon coordinates
2. For each school, queries the BVG API for nearby transit stops (no distance limit)
3. Categorizes stops by transport type (rail, tram, bus)
4. Extracts the TOP 3 nearest stops for each type with coordinates and line information
5. Calculates an accessibility score
6. Saves the enriched data back to CSV and XLSX

Robustness: a school whose BVG lookup fails (API unreachable / no stops
returned) keeps its previous transit values — from the per-school cache
written by this script, the previous final parquet, or the transit columns
already in the input file — instead of being written as a 0/0 summary.
A transit_accessibility_score of 0 with transit_stop_count_1000m of 0 is a
failure marker for a Berlin school, never data. After MAX_CONSECUTIVE_FAILURES
the API is assumed down and the remaining lookups are skipped (fast fail).

Data source: https://v6.bvg.transport.rest (free, no API key required)
"""

import pandas as pd
import requests
import time
import os
from datetime import datetime
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
MAX_RETRIES = 3  # Attempts per school before the lookup counts as failed
RETRY_BACKOFF_S = 2  # Seconds before retry (doubled each attempt)
MAX_CONSECUTIVE_FAILURES = 10  # After this many, assume the API is down and stop querying

# File paths — canonical project layout (previously CWD-relative)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
INTERMEDIATE_DIR = os.path.join(PROJECT_ROOT, "data_berlin", "intermediate")
SCHOOLS_FILE = os.path.join(INTERMEDIATE_DIR, "combined_schools_with_metadata_msa.csv")
OUTPUT_CSV = os.path.join(INTERMEDIATE_DIR, "combined_schools_with_metadata_msa.csv")
OUTPUT_XLSX = os.path.join(INTERMEDIATE_DIR, "combined_schools_with_metadata_msa.xlsx")
# Fallback sources for schools whose lookup fails this run (see module docstring)
TRANSIT_CACHE = os.path.join(PROJECT_ROOT, "data_berlin", "cache", "bvg_transit_cache.csv")
PREVIOUS_FINAL = os.path.join(PROJECT_ROOT, "data_berlin", "final", "school_master_table_final_with_embeddings.parquet")
SUMMARY_COLS = ["transit_stop_count_1000m", "transit_accessibility_score"]


def fetch_nearby_stops(lat: float, lon: float, radius: int = SEARCH_RADIUS_M) -> Optional[List[dict]]:
    """
    Fetch nearby transit stops from BVG API.

    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        radius: Search radius in meters

    Returns:
        List of stop dictionaries from BVG API, or None if the API could not
        be reached / answered with an error after MAX_RETRIES attempts.
    """
    url = f"{BVG_API_BASE}/locations/nearby"
    params = {
        "latitude": lat,
        "longitude": lon,
        "results": MAX_RESULTS,
        "distance": radius,
        "linesOfStops": "true"
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt + 1 < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S * (2 ** attempt))
            else:
                print(f"  API error: {e}")
    return None


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

    Scoring:
    - Rail within 500m: +40 points
    - Rail within 1000m: +25 points
    - Rail within 2000m: +10 points
    - Tram within 500m: +25 points
    - Tram within 1000m: +15 points
    - Bus within 300m: +20 points
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


def enrich_school(lat: float, lon: float) -> Optional[dict]:
    """
    Fetch and process transit data for one school.

    Args:
        lat: School latitude
        lon: School longitude

    Returns:
        Dictionary with all transit columns for this school, or None when the
        API failed / returned no stops at all — the caller then keeps the
        school's previous transit values instead of writing a 0/0 summary.
    """
    # Fetch nearby stops (large radius to find all types)
    stops = fetch_nearby_stops(lat, lon, SEARCH_RADIUS_M)
    if not stops:
        return None

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


def transit_columns() -> List[str]:
    """All transit columns written by enrich_school(), in output order."""
    cols = []
    for transport_type in ["rail", "tram", "bus"]:
        for i in range(TOP_N_STOPS):
            prefix = f"transit_{transport_type}_{i+1:02d}"
            cols += [f"{prefix}_name", f"{prefix}_distance_m", f"{prefix}_latitude",
                     f"{prefix}_longitude", f"{prefix}_lines"]
    return cols + ["transit_stop_count_1000m", "transit_all_lines_1000m", "transit_accessibility_score"]


def load_previous_transit(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Last known transit values per schulnummer, used for schools whose lookup
    fails this run. Priority: transit cache (last good API result) >
    previous final parquet > transit columns already in the input file.
    0/0 summaries are failure markers and are blanked so they never carry.

    Returns:
        DataFrame indexed by schulnummer with the transit_columns(), or None
    """
    sources = []
    for label, path in [("transit cache", TRANSIT_CACHE), ("previous final", PREVIOUS_FINAL)]:
        if os.path.exists(path):
            src = pd.read_parquet(path) if str(path).endswith(".parquet") else pd.read_csv(path, low_memory=False)
            sources.append((label, src))
    sources.append(("input file", df))

    wanted = transit_columns()
    previous = None
    for label, src in sources:
        if 'schulnummer' not in src.columns:
            continue
        part = src.copy()
        part['schulnummer'] = part['schulnummer'].astype(str).str.strip()
        part = part.drop_duplicates('schulnummer').set_index('schulnummer')
        # Supabase-style unranked columns (transit_bus_name, ...) stand in for rank 01
        for transport_type in ["rail", "tram", "bus"]:
            for field in ["name", "distance_m", "lines"]:
                ranked, flat = f"transit_{transport_type}_01_{field}", f"transit_{transport_type}_{field}"
                if flat in part.columns:
                    part[ranked] = part[ranked].fillna(part[flat]) if ranked in part.columns else part[flat]
        part = part[[c for c in wanted if c in part.columns]]
        if part.empty or part.notna().sum().sum() == 0:
            continue
        if all(c in part.columns for c in SUMMARY_COLS):
            failed = (part[SUMMARY_COLS].fillna(0) == 0).all(axis=1)
            part.loc[failed, SUMMARY_COLS] = None
        usable = int(part.drop(columns=SUMMARY_COLS, errors='ignore').notna().any(axis=1).sum())
        print(f"  Previous transit values from {label}: {usable} schools")
        previous = part if previous is None else previous.combine_first(part)
    return previous


def update_transit_cache(fresh_rows: Dict[str, dict]) -> None:
    """Merge this run's successful lookups into the per-school transit cache."""
    if not fresh_rows:
        return
    new = pd.DataFrame.from_dict(fresh_rows, orient='index')
    new.index.name = 'schulnummer'
    new['transit_fetched_at'] = datetime.now().strftime('%Y-%m-%d')
    new['transit_cache_source'] = 'bvg_api'
    if os.path.exists(TRANSIT_CACHE):
        old = pd.read_csv(TRANSIT_CACHE, dtype={'schulnummer': str}, low_memory=False).set_index('schulnummer')
        new = pd.concat([old[~old.index.isin(new.index)], new])
    os.makedirs(os.path.dirname(str(TRANSIT_CACHE)), exist_ok=True)
    new.to_csv(TRANSIT_CACHE, encoding='utf-8-sig')
    print(f"Transit cache updated: {TRANSIT_CACHE} ({len(new)} schools)")


def main():
    """Main function to enrich all schools with transit data."""
    print("="*70)
    print("ENRICHING SCHOOL DATA WITH PUBLIC TRANSIT INFORMATION")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data source: BVG Transport REST API (free, no authentication)")
    print(f"Search radius: {SEARCH_RADIUS_M}m (no practical limit)")
    print(f"Storing top {TOP_N_STOPS} nearest stops per transport type")

    # Load schools
    print("\nLoading school data...")
    df = pd.read_csv(SCHOOLS_FILE)
    print(f"Loaded {len(df)} schools")

    # Count schools with coordinates
    schools_with_coords = df['latitude'].notna().sum()
    print(f"Schools with coordinates: {schools_with_coords}")

    # Previous transit values, kept for schools whose lookup fails this run
    print("\nLoading previous transit values (fallback for failed lookups)...")
    previous = load_previous_transit(df)
    if previous is None:
        print("  None found — failed lookups will stay empty (never 0/0)")

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
    failed_lookups = 0
    kept_previous = 0
    consecutive_failures = 0
    api_down = False
    fresh_rows = {}  # schulnummer -> transit_data, for the cache

    if TQDM_AVAILABLE:
        iterator = tqdm(df.iterrows(), total=len(df), desc="Processing schools")
    else:
        iterator = df.iterrows()

    for idx, row in iterator:
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            schulnummer = str(row['schulnummer']).strip()
            transit_data = None
            if not api_down:
                try:
                    transit_data = enrich_school(row['latitude'], row['longitude'])
                except Exception as e:
                    errors += 1
                    if not TQDM_AVAILABLE:
                        print(f"  Error processing {row['schulname']}: {e}")

            if transit_data is not None:
                # Update dataframe
                for col, val in transit_data.items():
                    df.at[idx, col] = val
                fresh_rows[schulnummer] = transit_data
                processed += 1
                consecutive_failures = 0
            else:
                # Lookup failed: keep the previous values, never write a 0/0 summary
                failed_lookups += 1
                consecutive_failures += 1
                if previous is not None and schulnummer in previous.index:
                    prev_vals = previous.loc[schulnummer].dropna()
                    for col, val in prev_vals.items():
                        df.at[idx, col] = val
                    if len(prev_vals):
                        kept_previous += 1
                if not api_down and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    api_down = True
                    print(f"\nWARNING: {consecutive_failures} consecutive BVG API failures — "
                          f"assuming the API is down, skipping remaining lookups (previous values kept)")

            # Rate limiting
            if not api_down:
                time.sleep(REQUEST_DELAY_S)

    # Make sure every transit column exists even if no lookup succeeded
    for col in transit_columns():
        if col not in df.columns:
            df[col] = None

    # Save results
    print("\n" + "="*70)
    print("SAVING RESULTS")
    print("="*70)

    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Saved: {OUTPUT_CSV}")

    df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"Saved: {OUTPUT_XLSX}")

    update_transit_cache(fresh_rows)

    # Summary statistics
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    print(f"\nProcessed: {processed} schools")
    print(f"Errors: {errors}")
    if failed_lookups:
        print(f"\nWARNING: {failed_lookups} BVG lookups failed"
              f"{' (API assumed down)' if api_down else ''}: "
              f"{kept_previous} schools kept their previous transit values, "
              f"{failed_lookups - kept_previous} have none (left empty, not 0)")
    empty_summary = int(df[SUMMARY_COLS].isna().all(axis=1).sum())
    zero_summary = int((df[SUMMARY_COLS] == 0).all(axis=1).sum())
    if empty_summary or zero_summary:
        print(f"WARNING: transit summary empty for {empty_summary} schools, "
              f"0/0 (failure marker) for {zero_summary} schools")

    # Count new columns
    transit_cols = [c for c in df.columns if c.startswith('transit_')]
    print(f"\nTransit columns added: {len(transit_cols)}")

    print(f"\nTransit coverage (nearest stop found):")
    print(f"  - Rail (U/S-Bahn): {df['transit_rail_01_name'].notna().sum()} ({100*df['transit_rail_01_name'].notna().sum()/len(df):.1f}%)")
    print(f"  - Tram: {df['transit_tram_01_name'].notna().sum()} ({100*df['transit_tram_01_name'].notna().sum()/len(df):.1f}%)")
    print(f"  - Bus: {df['transit_bus_01_name'].notna().sum()} ({100*df['transit_bus_01_name'].notna().sum()/len(df):.1f}%)")

    print(f"\nDistance to nearest stop (all schools):")
    print(f"  - Rail: min={df['transit_rail_01_distance_m'].min():.0f}m, max={df['transit_rail_01_distance_m'].max():.0f}m, avg={df['transit_rail_01_distance_m'].mean():.0f}m")
    print(f"  - Tram: min={df['transit_tram_01_distance_m'].min():.0f}m, max={df['transit_tram_01_distance_m'].max():.0f}m, avg={df['transit_tram_01_distance_m'].mean():.0f}m" if df['transit_tram_01_distance_m'].notna().any() else "  - Tram: No data")
    print(f"  - Bus: min={df['transit_bus_01_distance_m'].min():.0f}m, max={df['transit_bus_01_distance_m'].max():.0f}m, avg={df['transit_bus_01_distance_m'].mean():.0f}m")

    print(f"\nAccessibility score distribution:")
    print(f"  - Min: {df['transit_accessibility_score'].min()}")
    print(f"  - Median: {df['transit_accessibility_score'].median()}")
    print(f"  - Max: {df['transit_accessibility_score'].max()}")
    print(f"  - Mean: {df['transit_accessibility_score'].mean():.1f}")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
