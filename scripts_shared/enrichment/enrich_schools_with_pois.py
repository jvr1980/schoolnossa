#!/usr/bin/env python3
"""
Enriches school data with nearby Points of Interest (POI) using Google Places API (New).

This script:
1. Loads school data with lat/lon coordinates
2. For each school, queries Google Places Nearby Search (New) for various POI types
3. Counts unique POIs within 500m by category
4. Extracts the TOP 3 nearest POIs for selected categories with full details
5. Saves the enriched data back to CSV and XLSX

Features:
- Uses Google Places API (New) - the current API version
- Parallel processing with configurable workers
- Automatic retries with exponential backoff
- Interim saving every N schools
- Resume capability from last checkpoint
- Detailed progress bar

POI Categories:
- Supermarkets
- Restaurants
- Bakeries/Cafes
- Kitas (preschools)
- Primary Schools
- Secondary Schools

Data source: Google Places API (New) - Nearby Search
"""

import pandas as pd
import requests
import time
import os
import math
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Install with: pip install tqdm")

# Load environment variables
load_dotenv()

# Configuration
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
SEARCH_RADIUS_M = 500  # 500 meters
REQUEST_DELAY_S = 0.1  # Delay between API calls (stay under rate limits)
TOP_N_POIS = 3  # Store top 3 nearest POIs per category

# Parallelization settings
MAX_WORKERS = 5  # Number of parallel threads (be careful with API rate limits)
SAVE_INTERVAL = 25  # Save progress every N schools

# Retry settings
MAX_RETRIES = 3
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 30.0

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHOOLS_FILE = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
OUTPUT_XLSX = os.path.join(BASE_DIR, "combined_schools_with_metadata.xlsx")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "poi_enrichment_checkpoint.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "poi_enrichment_progress.csv")

# Google Places API (New) endpoint
NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"

# POI Categories to search using Places API (New) types
# See: https://developers.google.com/maps/documentation/places/web-service/place-types
POI_CATEGORIES = {
    "supermarket": {
        "includedTypes": ["supermarket", "grocery_store"],
        "textQuery": None
    },
    "restaurant": {
        "includedTypes": ["restaurant"],
        "textQuery": None
    },
    "bakery_cafe": {
        "includedTypes": ["bakery", "cafe"],
        "textQuery": None
    },
    "kita": {
        "includedTypes": ["preschool"],
        "textQuery": "Kita Kindertagesstätte Kindergarten"
    },
    "primary_school": {
        "includedTypes": ["primary_school"],
        "textQuery": "Grundschule"
    },
    "secondary_school": {
        "includedTypes": ["secondary_school", "school"],
        "textQuery": "Sekundarschule Gymnasium Oberschule"
    }
}

# Categories that need nearest 3 details
DETAILED_CATEGORIES = ["kita", "primary_school", "supermarket", "restaurant", "bakery_cafe"]

# Thread-safe counter for progress tracking
class ProgressTracker:
    def __init__(self, total: int):
        self.total = total
        self.processed = 0
        self.errors = 0
        self.api_calls = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def increment(self, success: bool = True, api_calls: int = 0):
        with self.lock:
            self.processed += 1
            if not success:
                self.errors += 1
            self.api_calls += api_calls

    def get_stats(self) -> dict:
        with self.lock:
            elapsed = time.time() - self.start_time
            rate = self.processed / elapsed if elapsed > 0 else 0
            remaining = (self.total - self.processed) / rate if rate > 0 else 0
            return {
                "processed": self.processed,
                "total": self.total,
                "errors": self.errors,
                "api_calls": self.api_calls,
                "elapsed_s": elapsed,
                "rate_per_s": rate,
                "eta_s": remaining
            }


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points on Earth in meters."""
    R = 6371000  # Earth's radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


def fetch_nearby_places_new_api(
    lat: float,
    lon: float,
    included_types: List[str],
    text_query: Optional[str] = None,
    radius: int = SEARCH_RADIUS_M
) -> Tuple[List[dict], int]:
    """
    Fetch nearby places using Google Places API (New).

    Returns:
        Tuple of (list of places, number of API calls made)
    """
    if not GOOGLE_PLACES_API_KEY:
        return [], 0

    all_results = []
    api_calls = 0

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types"
    }

    # Build the request body for Nearby Search (New)
    request_body = {
        "locationRestriction": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lon
                },
                "radius": float(radius)
            }
        },
        "maxResultCount": 20
    }

    # Add included types
    if included_types:
        request_body["includedTypes"] = included_types

    # Retry loop with exponential backoff
    backoff = INITIAL_BACKOFF_S
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                NEARBY_SEARCH_URL,
                headers=headers,
                json=request_body,
                timeout=15
            )
            api_calls += 1

            if response.status_code == 200:
                data = response.json()
                places = data.get("places", [])
                all_results.extend(places)
                break  # Success

            elif response.status_code == 429:
                # Rate limited
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_S)
                    continue
                else:
                    print(f"  API rate limit exceeded after {MAX_RETRIES} retries")
                    break

            elif response.status_code == 400:
                error_data = response.json()
                error_msg = error_data.get("error", {}).get("message", "Unknown error")
                print(f"  API bad request: {error_msg}")
                break

            elif response.status_code == 403:
                error_data = response.json()
                error_msg = error_data.get("error", {}).get("message", "Unknown error")
                print(f"  API forbidden: {error_msg}")
                break

            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_S)
                    continue
                else:
                    print(f"  API error after {MAX_RETRIES} retries: HTTP {response.status_code}")
                    break

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
                continue
            else:
                print(f"  Request timeout after {MAX_RETRIES} retries")
                break

        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
                continue
            else:
                print(f"  API error after {MAX_RETRIES} retries: {e}")
                break

    time.sleep(REQUEST_DELAY_S)

    # If we have a text query and need more specific results, do a text search
    if text_query and len(all_results) < 5:
        text_results, text_calls = fetch_text_search_new_api(lat, lon, text_query, radius)
        api_calls += text_calls
        all_results.extend(text_results)

    # Deduplicate by place id
    seen_ids = set()
    unique_results = []
    for place in all_results:
        place_id = place.get("id")
        if place_id and place_id not in seen_ids:
            seen_ids.add(place_id)
            unique_results.append(place)

    return unique_results, api_calls


def fetch_text_search_new_api(
    lat: float,
    lon: float,
    text_query: str,
    radius: int = SEARCH_RADIUS_M
) -> Tuple[List[dict], int]:
    """
    Fetch places using Text Search (New) for keyword-based searches.

    Returns:
        Tuple of (list of places, number of API calls made)
    """
    if not GOOGLE_PLACES_API_KEY or not text_query:
        return [], 0

    TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types"
    }

    request_body = {
        "textQuery": text_query,
        "locationBias": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lon
                },
                "radius": float(radius)
            }
        },
        "maxResultCount": 20
    }

    api_calls = 0
    backoff = INITIAL_BACKOFF_S

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                TEXT_SEARCH_URL,
                headers=headers,
                json=request_body,
                timeout=15
            )
            api_calls += 1

            if response.status_code == 200:
                data = response.json()
                return data.get("places", []), api_calls

            elif response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_S)
                    continue
                break

            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_S)
                    continue
                break

        except requests.RequestException:
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
                continue
            break

    return [], api_calls


def calculate_distance_and_sort(
    places: List[dict],
    school_lat: float,
    school_lon: float
) -> List[dict]:
    """Calculate distance from school to each place and sort by distance."""
    for place in places:
        # Places API (New) uses different structure
        location = place.get("location", {})
        place_lat = location.get("latitude")
        place_lon = location.get("longitude")

        if place_lat and place_lon:
            place["distance_m"] = haversine_distance(
                school_lat, school_lon, place_lat, place_lon
            )
        else:
            place["distance_m"] = 999999

    places.sort(key=lambda x: x.get("distance_m", 999999))
    return places


def enrich_school(idx: int, lat: float, lon: float, school_name: str = "") -> Tuple[int, dict, int]:
    """
    Fetch and process POI data for one school.

    Returns:
        Tuple of (index, poi_data dict, api_calls count)
    """
    result = {}
    total_api_calls = 0

    for category, search_params in POI_CATEGORIES.items():
        places, api_calls = fetch_nearby_places_new_api(
            lat, lon,
            included_types=search_params.get("includedTypes", []),
            text_query=search_params.get("textQuery"),
            radius=SEARCH_RADIUS_M
        )
        total_api_calls += api_calls

        places = calculate_distance_and_sort(places, lat, lon)

        # Filter out the school itself (for school categories)
        if category in ["primary_school", "secondary_school"]:
            places = [
                p for p in places
                if school_name.lower() not in p.get("displayName", {}).get("text", "").lower()
                and p.get("distance_m", 0) > 50
            ]

        # Count within 500m
        count_500m = len([p for p in places if p.get("distance_m", 999999) <= 500])
        result[f"poi_{category}_count_500m"] = count_500m

        # Store top 3 nearest for detailed categories
        if category in DETAILED_CATEGORIES:
            for i in range(TOP_N_POIS):
                rank = f"{i+1:02d}"
                prefix = f"poi_{category}_{rank}"

                if i < len(places):
                    place = places[i]
                    location = place.get("location", {})

                    # Places API (New) uses displayName.text for the name
                    display_name = place.get("displayName", {})
                    name = display_name.get("text") if isinstance(display_name, dict) else display_name

                    result[f"{prefix}_name"] = name
                    result[f"{prefix}_address"] = place.get("formattedAddress")
                    result[f"{prefix}_distance_m"] = round(place.get("distance_m", 0))
                    result[f"{prefix}_latitude"] = location.get("latitude")
                    result[f"{prefix}_longitude"] = location.get("longitude")
                else:
                    result[f"{prefix}_name"] = None
                    result[f"{prefix}_address"] = None
                    result[f"{prefix}_distance_m"] = None
                    result[f"{prefix}_latitude"] = None
                    result[f"{prefix}_longitude"] = None

    return idx, result, total_api_calls


def load_checkpoint() -> set:
    """Load checkpoint file to get already processed indices."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get("processed_indices", []))
        except Exception as e:
            print(f"Warning: Could not load checkpoint: {e}")
    return set()


def save_checkpoint(processed_indices: set):
    """Save checkpoint with processed indices."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                "processed_indices": list(processed_indices),
                "timestamp": datetime.now().isoformat()
            }, f)
    except Exception as e:
        print(f"Warning: Could not save checkpoint: {e}")


def save_interim_results(df: pd.DataFrame, processed_count: int):
    """Save interim results to progress file."""
    try:
        df.to_csv(PROGRESS_FILE, index=False, encoding='utf-8-sig')
        print(f"\n  [Interim save] {processed_count} schools saved to {PROGRESS_FILE}")
    except Exception as e:
        print(f"\n  Warning: Could not save interim results: {e}")


def main():
    """Main function to enrich all schools with POI data."""
    print("="*70)
    print("ENRICHING SCHOOL DATA WITH NEARBY POINTS OF INTEREST")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data source: Google Places API (New) - Nearby Search")
    print(f"Search radius: {SEARCH_RADIUS_M}m")
    print(f"Parallel workers: {MAX_WORKERS}")
    print(f"Save interval: every {SAVE_INTERVAL} schools")
    print(f"Max retries per request: {MAX_RETRIES}")

    if not GOOGLE_PLACES_API_KEY:
        print("\nERROR: GOOGLE_PLACES_API_KEY not set in environment!")
        print("Please set it in your .env file")
        return

    # Load schools
    print("\nLoading school data...")
    df = pd.read_csv(SCHOOLS_FILE)
    print(f"Loaded {len(df)} schools")

    # Check for existing progress file
    if os.path.exists(PROGRESS_FILE):
        print(f"\nFound existing progress file: {PROGRESS_FILE}")
        try:
            df = pd.read_csv(PROGRESS_FILE)
            print(f"Loaded {len(df)} schools from progress file")
        except Exception as e:
            print(f"Could not load progress file, using original: {e}")

    # Load checkpoint
    processed_indices = load_checkpoint()
    if processed_indices:
        print(f"Resuming from checkpoint: {len(processed_indices)} schools already processed")

    # Count schools with coordinates that need processing
    schools_to_process = []
    for idx, row in df.iterrows():
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            if idx not in processed_indices:
                schools_to_process.append((idx, row))

    total_to_process = len(schools_to_process)
    print(f"Schools to process: {total_to_process}")

    if total_to_process == 0:
        print("\nAll schools already processed!")
        return

    # Remove old POI columns if starting fresh
    if not processed_indices:
        old_poi_cols = [c for c in df.columns if c.startswith('poi_')]
        if old_poi_cols:
            df = df.drop(columns=old_poi_cols)
            print(f"Removed {len(old_poi_cols)} old POI columns")

    # Estimate API calls and cost
    # Places API (New) pricing: $0.032 per Nearby Search, $0.035 per Text Search
    estimated_calls = total_to_process * 8
    estimated_cost = estimated_calls * 0.035

    print(f"\nEstimated API calls: ~{estimated_calls}")
    print(f"Estimated cost: ~${estimated_cost:.2f}")
    print()

    # Initialize progress tracker
    tracker = ProgressTracker(total_to_process)

    # Process schools in parallel
    results_buffer = {}
    save_counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        futures = {}
        for idx, row in schools_to_process:
            future = executor.submit(
                enrich_school,
                idx,
                row['latitude'],
                row['longitude'],
                row.get('schulname', '')
            )
            futures[future] = idx

        # Process completed tasks with progress bar
        if TQDM_AVAILABLE:
            pbar = tqdm(
                total=total_to_process,
                desc="Processing schools",
                unit="school",
                dynamic_ncols=True,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] Errors: {postfix}'
            )
            pbar.set_postfix_str("0")

        for future in as_completed(futures):
            try:
                idx, poi_data, api_calls = future.result()

                # Update dataframe
                for col, val in poi_data.items():
                    df.at[idx, col] = val

                # Track progress
                processed_indices.add(idx)
                tracker.increment(success=True, api_calls=api_calls)
                save_counter += 1

                # Update progress bar
                if TQDM_AVAILABLE:
                    stats = tracker.get_stats()
                    pbar.update(1)
                    pbar.set_postfix_str(str(stats["errors"]))

                # Interim save
                if save_counter >= SAVE_INTERVAL:
                    save_interim_results(df, len(processed_indices))
                    save_checkpoint(processed_indices)
                    save_counter = 0

            except Exception as e:
                idx = futures[future]
                tracker.increment(success=False)
                print(f"\nError processing school at index {idx}: {e}")

                if TQDM_AVAILABLE:
                    stats = tracker.get_stats()
                    pbar.set_postfix_str(str(stats["errors"]))

        if TQDM_AVAILABLE:
            pbar.close()

    # Final save
    print("\n" + "="*70)
    print("SAVING FINAL RESULTS")
    print("="*70)

    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Saved: {OUTPUT_CSV}")

    df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"Saved: {OUTPUT_XLSX}")

    # Clean up checkpoint and progress files
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print(f"Removed checkpoint file: {CHECKPOINT_FILE}")
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print(f"Removed progress file: {PROGRESS_FILE}")

    # Summary statistics
    stats = tracker.get_stats()
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    print(f"\nProcessed: {stats['processed']} schools")
    print(f"Errors: {stats['errors']}")
    print(f"Total API calls: {stats['api_calls']}")
    print(f"Time elapsed: {stats['elapsed_s']/60:.1f} minutes")
    print(f"Average rate: {stats['rate_per_s']:.2f} schools/second")

    # Count POI columns
    poi_cols = [c for c in df.columns if c.startswith('poi_')]
    print(f"\nPOI columns added: {len(poi_cols)}")

    print(f"\nPOI counts within 500m (average per school):")
    for category in POI_CATEGORIES.keys():
        col = f"poi_{category}_count_500m"
        if col in df.columns:
            avg = df[col].mean()
            max_val = df[col].max()
            print(f"  - {category}: avg={avg:.1f}, max={int(max_val) if pd.notna(max_val) else 0}")

    print(f"\nNearest POI distances (average):")
    for category in DETAILED_CATEGORIES:
        col = f"poi_{category}_01_distance_m"
        if col in df.columns and df[col].notna().any():
            avg = df[col].mean()
            print(f"  - {category}: {avg:.0f}m")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
