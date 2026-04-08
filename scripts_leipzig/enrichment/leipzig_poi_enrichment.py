#!/usr/bin/env python3
"""
Phase 5: Leipzig POI Enrichment (Google Places API)
====================================================

Enriches school data with nearby Points of Interest using Google Places API (New).
Shared pattern across all cities — adapted from NRW/Hamburg POI enrichment.

This script:
1. Loads Leipzig school data with lat/lon coordinates
2. For each school, queries Google Places Nearby Search for various POI types
3. Counts unique POIs within 500m by category
4. Extracts TOP 3 nearest POIs for selected categories
5. Saves enriched data

POI Categories:
    - Supermarkets, Restaurants, Bakeries/Cafes
    - Kitas (preschools), Primary Schools, Secondary Schools
    - Parks, Libraries, Sports Facilities

Requires: GOOGLE_PLACES_API_KEY in config.yaml or environment

Input: data_leipzig/intermediate/leipzig_schools_with_crime.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_pois.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import requests
import time
import os
import math
import json
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Configuration
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
SEARCH_RADIUS_M = 500
REQUEST_DELAY_S = 0.1
TOP_N_POIS = 3
MAX_WORKERS = 5
SAVE_INTERVAL = 25
MAX_RETRIES = 3
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 30.0

# Checkpoint
CHECKPOINT_FILE = INTERMEDIATE_DIR / "leipzig_poi_enrichment_checkpoint.json"

# Google Places API (New) endpoint
NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Fallback input chain (most-enriched first)
INPUT_FALLBACKS = [
    INTERMEDIATE_DIR / "leipzig_schools_with_crime.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    RAW_DIR / "leipzig_schools_raw.csv",
]

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
        "textQuery": "Kita Kindertagesstätte Kindergarten"
    },
    "primary_school": {
        "includedTypes": ["primary_school"],
        "textQuery": "Grundschule"
    },
    "secondary_school": {
        "includedTypes": ["secondary_school", "school"],
        "textQuery": "Gesamtschule Gymnasium Realschule Oberschule"
    },
    "park": {
        "includedTypes": ["park"],
        "textQuery": None
    },
    "library": {
        "includedTypes": ["library"],
        "textQuery": None
    },
    "sports": {
        "includedTypes": ["gym", "sports_complex", "swimming_pool", "stadium"],
        "textQuery": None
    },
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
            return {
                "processed": self.processed,
                "total": self.total,
                "errors": self.errors,
                "api_calls": self.api_calls,
                "elapsed_s": elapsed,
                "rate_per_s": rate,
            }


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in meters."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_cached_response(school_id: str, category: str) -> Optional[list]:
    """Load cached API response for a school+category combination."""
    cache_file = CACHE_DIR / "poi" / f"{school_id}_{category}.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return None


def save_cached_response(school_id: str, category: str, data: list):
    """Save API response to cache."""
    cache_subdir = CACHE_DIR / "poi"
    cache_subdir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_subdir / f"{school_id}_{category}.json"
    try:
        with open(cache_file, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def fetch_nearby_places(lat: float, lon: float, included_types: List[str],
                        text_query: Optional[str] = None, radius: int = SEARCH_RADIUS_M,
                        school_id: str = "", category: str = "") -> Tuple[List[dict], int]:
    """Fetch nearby places using Google Places API (New), with caching."""
    if not GOOGLE_PLACES_API_KEY:
        return [], 0

    # Check cache first
    if school_id and category:
        cached = load_cached_response(school_id, category)
        if cached is not None:
            return cached, 0

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
            response = requests.post(NEARBY_SEARCH_URL, headers=headers, json=request_body, timeout=15)
            api_calls += 1
            if response.status_code == 200:
                all_results.extend(response.json().get("places", []))
                break
            elif response.status_code == 429:
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
            else:
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)

    time.sleep(REQUEST_DELAY_S)

    # Text search fallback
    if text_query and len(all_results) < 5:
        text_results, text_calls = _fetch_text_search(lat, lon, text_query, radius)
        api_calls += text_calls
        all_results.extend(text_results)

    # Deduplicate
    seen_ids = set()
    unique = []
    for place in all_results:
        pid = place.get("id")
        if pid and pid not in seen_ids:
            seen_ids.add(pid)
            unique.append(place)

    # Cache the result
    if school_id and category:
        save_cached_response(school_id, category, unique)

    return unique, api_calls


def _fetch_text_search(lat: float, lon: float, text_query: str, radius: int) -> Tuple[List[dict], int]:
    """Fetch places using Text Search."""
    if not GOOGLE_PLACES_API_KEY:
        return [], 0

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types"
    }

    request_body = {
        "textQuery": text_query,
        "locationBias": {
            "circle": {"center": {"latitude": lat, "longitude": lon}, "radius": float(radius)}
        },
        "maxResultCount": 20
    }

    api_calls = 0
    backoff = INITIAL_BACKOFF_S
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(TEXT_SEARCH_URL, headers=headers, json=request_body, timeout=15)
            api_calls += 1
            if response.status_code == 200:
                return response.json().get("places", []), api_calls
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
    return [], api_calls


def enrich_school(idx: int, lat: float, lon: float, school_name: str = "",
                  school_id: str = "") -> Tuple[int, dict, int]:
    """Fetch and process POI data for one school."""
    result = {}
    total_api_calls = 0

    for category, search_params in POI_CATEGORIES.items():
        places, api_calls = fetch_nearby_places(
            lat, lon,
            included_types=search_params.get("includedTypes", []),
            text_query=search_params.get("textQuery"),
            school_id=school_id,
            category=category,
        )
        total_api_calls += api_calls

        # Calculate distances and sort
        for place in places:
            loc = place.get("location", {})
            plat, plon = loc.get("latitude"), loc.get("longitude")
            place["distance_m"] = haversine_distance(lat, lon, plat, plon) if plat and plon else 999999
        places.sort(key=lambda x: x.get("distance_m", 999999))

        # Filter self (avoid counting the school itself)
        if category in ["primary_school", "secondary_school"]:
            places = [p for p in places
                      if school_name.lower() not in p.get("displayName", {}).get("text", "").lower()
                      and p.get("distance_m", 0) > 50]

        # Count within 500m
        result[f"poi_{category}_count_500m"] = len([p for p in places if p.get("distance_m", 999999) <= 500])

        # Top 3 for detailed categories
        if category in DETAILED_CATEGORIES:
            for i in range(TOP_N_POIS):
                rank = f"{i + 1:02d}"
                prefix = f"poi_{category}_{rank}"
                if i < len(places):
                    place = places[i]
                    dn = place.get("displayName", {})
                    name = dn.get("text") if isinstance(dn, dict) else dn
                    location = place.get("location", {})
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
    """Load checkpoint of already-processed school indices."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return set(json.load(f).get("processed_indices", []))
        except Exception:
            pass
    return set()


def save_checkpoint(processed_indices: set):
    """Save checkpoint of processed school indices."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({"processed_indices": list(processed_indices), "timestamp": datetime.now().isoformat()}, f)
    except Exception:
        pass


def find_input_file() -> Optional[Path]:
    """Find the best available input file from the fallback chain."""
    for path in INPUT_FALLBACKS:
        if path.exists():
            return path
    return None


def main():
    """Enrich Leipzig schools with Google Places POI data."""
    logger.info("=" * 60)
    logger.info("Starting Leipzig POI Enrichment (Google Places API)")
    logger.info("=" * 60)

    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not set! Set it in .env file or environment.")
        return

    # Find input file
    input_file = find_input_file()
    if not input_file:
        logger.error("No Leipzig school data found! Checked fallback chain:")
        for fb in INPUT_FALLBACKS:
            logger.error(f"  - {fb}")
        return

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} schools from {input_file.name}")

    # Checkpoint
    processed_indices = load_checkpoint()
    if processed_indices:
        logger.info(f"Resuming: {len(processed_indices)} already processed")

    # Find schools with coordinates that haven't been processed yet
    schools_to_process = [
        (idx, row) for idx, row in df.iterrows()
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude'))
        and idx not in processed_indices
    ]

    total = len(schools_to_process)
    logger.info(f"Schools to process: {total}")

    if total == 0:
        logger.info("All schools already processed!")
        return

    # Remove old POI columns if starting fresh
    if not processed_indices:
        old_cols = [c for c in df.columns if c.startswith('poi_')]
        if old_cols:
            df = df.drop(columns=old_cols)

    estimated_calls = total * len(POI_CATEGORIES)
    logger.info(f"Estimated API calls: ~{estimated_calls}")

    tracker = ProgressTracker(total)
    save_counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for idx, row in schools_to_process:
            # Build a school_id for caching (use index if no unique ID column)
            school_id = str(row.get('schulnummer', row.get('school_id', idx)))
            future = executor.submit(
                enrich_school, idx,
                row['latitude'], row['longitude'],
                row.get('schulname', ''),
                school_id,
            )
            futures[future] = idx

        pbar = tqdm(total=total, desc="POI enrichment") if TQDM_AVAILABLE else None

        for future in as_completed(futures):
            try:
                idx, poi_data, api_calls = future.result()
                for col, val in poi_data.items():
                    df.at[idx, col] = val
                processed_indices.add(idx)
                tracker.increment(success=True, api_calls=api_calls)
                save_counter += 1
                if pbar:
                    pbar.update(1)
                if save_counter >= SAVE_INTERVAL:
                    save_checkpoint(processed_indices)
                    save_counter = 0
            except Exception as e:
                tracker.increment(success=False)
                logger.warning(f"Error processing school: {e}")

        if pbar:
            pbar.close()

    # Save final output
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    output_file = INTERMEDIATE_DIR / "leipzig_schools_with_pois.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_file}")

    # Clean up checkpoint
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    # Print summary
    stats = tracker.get_stats()
    print(f"\n{'=' * 70}")
    print(f"LEIPZIG POI ENRICHMENT - COMPLETE")
    print(f"{'=' * 70}")
    print(f"Processed: {stats['processed']} schools")
    print(f"Errors:    {stats['errors']}")
    print(f"API calls: {stats['api_calls']}")
    print(f"Time:      {stats['elapsed_s'] / 60:.1f} minutes")
    print(f"\nPOI counts (average per school):")
    for cat in POI_CATEGORIES:
        col = f"poi_{cat}_count_500m"
        if col in df.columns:
            avg = df[col].mean()
            print(f"  - {cat}: {avg:.1f}")
    print(f"\nOutput: {output_file}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
