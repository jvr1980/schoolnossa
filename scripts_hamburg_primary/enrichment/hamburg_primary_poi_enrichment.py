#!/usr/bin/env python3
"""
Hamburg Primary School POI Enrichment

Enriches Hamburg primary school (Grundschulen) data with nearby Points of Interest
using Google Places API (New).

This script:
1. Loads Hamburg primary school data with lat/lon coordinates
2. For each school, queries Google Places Nearby Search for various POI types
3. Counts unique POIs within 500m by category
4. Extracts the TOP 3 nearest POIs for selected categories with full details
5. Saves the enriched data back to CSV

POI Categories:
- Supermarkets
- Restaurants
- Bakeries/Cafes
- Kitas (preschools)
- Primary Schools (Grundschulen)
- Secondary Schools

Author: Hamburg School Data Pipeline
Created: 2026-04-04
"""

import pandas as pd
import requests
import time
import os
import math
import json
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg_primary"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

# Configuration
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
SEARCH_RADIUS_M = 500  # 500 meters
REQUEST_DELAY_S = 0.1  # Delay between API calls
TOP_N_POIS = 3  # Store top 3 nearest POIs per category

# Parallelization settings
MAX_WORKERS = 5
SAVE_INTERVAL = 25

# Retry settings
MAX_RETRIES = 3
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 30.0

# Checkpoint file
CHECKPOINT_FILE = INTERMEDIATE_DIR / "primary_poi_enrichment_checkpoint.json"
PROGRESS_FILE = INTERMEDIATE_DIR / "primary_poi_enrichment_progress.csv"

# Google Places API (New) endpoint
NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# POI Categories
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
        "textQuery": "Kita Kindertagesstaette Kindergarten"
    },
    "primary_school": {
        "includedTypes": ["primary_school"],
        "textQuery": "Grundschule"
    },
    "secondary_school": {
        "includedTypes": ["secondary_school", "school"],
        "textQuery": "Sekundarschule Gymnasium Stadtteilschule"
    }
}

DETAILED_CATEGORIES = ["kita", "primary_school", "supermarket", "restaurant", "bakery_cafe"]


class ProgressTracker:
    """Thread-safe progress tracker."""

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
    """Calculate great-circle distance between two points in meters."""
    R = 6371000  # Earth's radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


def fetch_nearby_places(
    lat: float,
    lon: float,
    included_types: List[str],
    text_query: Optional[str] = None,
    radius: int = SEARCH_RADIUS_M
) -> Tuple[List[dict], int]:
    """Fetch nearby places using Google Places API (New)."""
    if not GOOGLE_PLACES_API_KEY:
        return [], 0

    all_results = []
    api_calls = 0

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types"
    }

    request_body = {
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": float(radius)
            }
        },
        "maxResultCount": 20
    }

    if included_types:
        request_body["includedTypes"] = included_types

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
                all_results.extend(data.get("places", []))
                break
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

    time.sleep(REQUEST_DELAY_S)

    # Text search for specific queries
    if text_query and len(all_results) < 5:
        text_results, text_calls = fetch_text_search(lat, lon, text_query, radius)
        api_calls += text_calls
        all_results.extend(text_results)

    # Deduplicate
    seen_ids = set()
    unique_results = []
    for place in all_results:
        place_id = place.get("id")
        if place_id and place_id not in seen_ids:
            seen_ids.add(place_id)
            unique_results.append(place)

    return unique_results, api_calls


def fetch_text_search(
    lat: float,
    lon: float,
    text_query: str,
    radius: int = SEARCH_RADIUS_M
) -> Tuple[List[dict], int]:
    """Fetch places using Text Search for keyword-based searches."""
    if not GOOGLE_PLACES_API_KEY or not text_query:
        return [], 0

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types"
    }

    request_body = {
        "textQuery": text_query,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
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
                return response.json().get("places", []), api_calls
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


def calculate_distance_and_sort(places: List[dict], school_lat: float, school_lon: float) -> List[dict]:
    """Calculate distance from school to each place and sort."""
    for place in places:
        location = place.get("location", {})
        place_lat = location.get("latitude")
        place_lon = location.get("longitude")

        if place_lat and place_lon:
            place["distance_m"] = haversine_distance(school_lat, school_lon, place_lat, place_lon)
        else:
            place["distance_m"] = 999999

    places.sort(key=lambda x: x.get("distance_m", 999999))
    return places


def enrich_school(idx: int, lat: float, lon: float, school_name: str = "") -> Tuple[int, dict, int]:
    """Fetch and process POI data for one primary school."""
    result = {}
    total_api_calls = 0

    for category, search_params in POI_CATEGORIES.items():
        places, api_calls = fetch_nearby_places(
            lat, lon,
            included_types=search_params.get("includedTypes", []),
            text_query=search_params.get("textQuery"),
            radius=SEARCH_RADIUS_M
        )
        total_api_calls += api_calls

        places = calculate_distance_and_sort(places, lat, lon)

        # Filter out the school itself
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
    """Load checkpoint file."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get("processed_indices", []))
        except:
            pass
    return set()


def save_checkpoint(processed_indices: set):
    """Save checkpoint."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                "processed_indices": list(processed_indices),
                "timestamp": datetime.now().isoformat()
            }, f)
    except:
        pass


def save_interim_results(df: pd.DataFrame, processed_count: int):
    """Save interim results."""
    try:
        df.to_csv(PROGRESS_FILE, index=False, encoding='utf-8-sig')
        logger.info(f"Interim save: {processed_count} schools saved")
    except:
        pass


def main():
    """Main function."""
    logger.info("="*70)
    logger.info("HAMBURG PRIMARY SCHOOL POI ENRICHMENT")
    logger.info("="*70)
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Search radius: {SEARCH_RADIUS_M}m")
    logger.info(f"Parallel workers: {MAX_WORKERS}")

    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not set! Please set it in .env file")
        return

    # Load schools
    input_file = FINAL_DIR / "hamburg_primary_school_master_table_final.csv"
    if not input_file.exists():
        input_file = FINAL_DIR / "hamburg_primary_school_master_table.csv"

    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} primary schools")

    # Load checkpoint
    processed_indices = load_checkpoint()
    if processed_indices:
        logger.info(f"Resuming: {len(processed_indices)} already processed")

    # Find schools to process
    schools_to_process = []
    for idx, row in df.iterrows():
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            if idx not in processed_indices:
                schools_to_process.append((idx, row))

    total_to_process = len(schools_to_process)
    logger.info(f"Schools to process: {total_to_process}")

    if total_to_process == 0:
        logger.info("All schools already processed!")
        return

    # Remove old POI columns if starting fresh
    if not processed_indices:
        old_poi_cols = [c for c in df.columns if c.startswith('poi_')]
        if old_poi_cols:
            df = df.drop(columns=old_poi_cols)

    # Estimate cost
    estimated_calls = total_to_process * 8
    estimated_cost = estimated_calls * 0.035
    logger.info(f"Estimated API calls: ~{estimated_calls}")
    logger.info(f"Estimated cost: ~${estimated_cost:.2f}")

    # Initialize tracker
    tracker = ProgressTracker(total_to_process)

    # Process in parallel
    save_counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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

        if TQDM_AVAILABLE:
            pbar = tqdm(total=total_to_process, desc="Processing", unit="school")

        for future in as_completed(futures):
            try:
                idx, poi_data, api_calls = future.result()

                for col, val in poi_data.items():
                    df.at[idx, col] = val

                processed_indices.add(idx)
                tracker.increment(success=True, api_calls=api_calls)
                save_counter += 1

                if TQDM_AVAILABLE:
                    pbar.update(1)

                if save_counter >= SAVE_INTERVAL:
                    save_interim_results(df, len(processed_indices))
                    save_checkpoint(processed_indices)
                    save_counter = 0

            except Exception as e:
                tracker.increment(success=False)
                logger.warning(f"Error: {e}")

        if TQDM_AVAILABLE:
            pbar.close()

    # Save final results
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    output_file = INTERMEDIATE_DIR / "hamburg_primary_schools_with_pois.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_file}")

    # Clean up
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    # Summary
    stats = tracker.get_stats()
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Processed: {stats['processed']} primary schools")
    print(f"Errors: {stats['errors']}")
    print(f"API calls: {stats['api_calls']}")
    print(f"Time: {stats['elapsed_s']/60:.1f} minutes")

    print(f"\nPOI counts (average per school):")
    for category in POI_CATEGORIES.keys():
        col = f"poi_{category}_count_500m"
        if col in df.columns:
            avg = df[col].mean()
            print(f"  - {category}: {avg:.1f}")


if __name__ == "__main__":
    main()
