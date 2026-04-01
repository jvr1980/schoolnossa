#!/usr/bin/env python3
"""
Phase 5: Munich POI Enrichment (Google Places API)

Enriches school data with nearby Points of Interest using Google Places API (New).
Same approach as Frankfurt/NRW/Hamburg pipelines.

Requires GOOGLE_PLACES_API_KEY environment variable.

Input: data_munich/intermediate/munich_secondary_schools_with_crime.csv
       (fallback chain: earlier intermediate files)
Output: data_munich/intermediate/munich_secondary_schools_with_pois.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
SEARCH_RADIUS_M = 500
REQUEST_DELAY_S = 0.1
TOP_N_POIS = 3
MAX_WORKERS = 5
SAVE_INTERVAL = 25
MAX_RETRIES = 3

CHECKPOINT_FILE = INTERMEDIATE_DIR / "munich_poi_enrichment_checkpoint.json"
NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

POI_CATEGORIES = {
    "supermarket": {"includedTypes": ["supermarket", "grocery_store"], "textQuery": None},
    "restaurant": {"includedTypes": ["restaurant"], "textQuery": None},
    "bakery_cafe": {"includedTypes": ["bakery", "cafe"], "textQuery": None},
    "kita": {"includedTypes": ["preschool"], "textQuery": "Kita Kindertagesstätte Kindergarten"},
    "primary_school": {"includedTypes": ["primary_school"], "textQuery": "Grundschule"},
    "secondary_school": {"includedTypes": ["secondary_school", "school"],
                          "textQuery": "Gesamtschule Gymnasium Realschule"},
}
DETAILED_CATEGORIES = ["kita", "primary_school", "supermarket", "restaurant", "bakery_cafe"]


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.processed = 0
        self.errors = 0
        self.api_calls = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def increment(self, success=True, api_calls=0):
        with self.lock:
            self.processed += 1
            if not success: self.errors += 1
            self.api_calls += api_calls

    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            return {"processed": self.processed, "total": self.total,
                    "errors": self.errors, "api_calls": self.api_calls, "elapsed_s": elapsed}


def fetch_nearby_places(lat, lon, included_types, text_query=None, radius=SEARCH_RADIUS_M):
    if not GOOGLE_PLACES_API_KEY:
        return [], 0

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types"
    }
    body = {
        "locationRestriction": {"circle": {"center": {"latitude": lat, "longitude": lon}, "radius": float(radius)}},
        "maxResultCount": 20,
    }
    if included_types:
        body["includedTypes"] = included_types

    api_calls = 0
    results = []
    backoff = 1.0
    for _ in range(MAX_RETRIES):
        try:
            resp = requests.post(NEARBY_SEARCH_URL, headers=headers, json=body, timeout=15)
            api_calls += 1
            if resp.status_code == 200:
                results.extend(resp.json().get("places", []))
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    time.sleep(REQUEST_DELAY_S)

    if text_query and len(results) < 5:
        body2 = {"textQuery": text_query,
                 "locationBias": {"circle": {"center": {"latitude": lat, "longitude": lon}, "radius": float(radius)}},
                 "maxResultCount": 20}
        for _ in range(MAX_RETRIES):
            try:
                resp = requests.post(TEXT_SEARCH_URL, headers=headers, json=body2, timeout=15)
                api_calls += 1
                if resp.status_code == 200:
                    results.extend(resp.json().get("places", []))
                    break
            except requests.RequestException:
                pass

    seen = set()
    unique = []
    for p in results:
        pid = p.get("id")
        if pid and pid not in seen:
            seen.add(pid)
            unique.append(p)
    return unique, api_calls


def enrich_school(idx, lat, lon, school_name=""):
    result = {}
    total_api = 0
    for cat, params in POI_CATEGORIES.items():
        places, ac = fetch_nearby_places(lat, lon, params.get("includedTypes", []), params.get("textQuery"))
        total_api += ac
        for p in places:
            loc = p.get("location", {})
            plat, plon = loc.get("latitude"), loc.get("longitude")
            p["distance_m"] = haversine_distance(lat, lon, plat, plon) if plat and plon else 999999
        places.sort(key=lambda x: x.get("distance_m", 999999))
        if cat in ["primary_school", "secondary_school"]:
            places = [p for p in places if school_name.lower() not in p.get("displayName", {}).get("text", "").lower() and p.get("distance_m", 0) > 50]

        result[f"poi_{cat}_count_500m"] = len([p for p in places if p.get("distance_m", 999999) <= 500])
        if cat in DETAILED_CATEGORIES:
            for i in range(TOP_N_POIS):
                pfx = f"poi_{cat}_{i+1:02d}"
                if i < len(places):
                    p = places[i]
                    dn = p.get("displayName", {})
                    name = dn.get("text") if isinstance(dn, dict) else dn
                    loc = p.get("location", {})
                    result[f"{pfx}_name"] = name
                    result[f"{pfx}_address"] = p.get("formattedAddress")
                    result[f"{pfx}_distance_m"] = round(p.get("distance_m", 0))
                    result[f"{pfx}_latitude"] = loc.get("latitude")
                    result[f"{pfx}_longitude"] = loc.get("longitude")
                else:
                    for sfx in ['_name', '_address', '_distance_m', '_latitude', '_longitude']:
                        result[f"{pfx}{sfx}"] = None
    return idx, result, total_api


def find_input_file():
    candidates = [
        INTERMEDIATE_DIR / "munich_secondary_schools_with_crime.csv",
        INTERMEDIATE_DIR / "munich_secondary_schools_with_transit.csv",
        INTERMEDIATE_DIR / "munich_secondary_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "munich_secondary_schools.csv",
    ]
    for f in candidates:
        if f.exists():
            return f
    raise FileNotFoundError("No school data found. Run earlier phases first.")


def main():
    logger.info("=" * 60)
    logger.info("Phase 5: Munich POI Enrichment (Google Places API)")
    logger.info("=" * 60)

    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not set! Set it and rerun.")
        return

    input_file = find_input_file()
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} schools from {input_file.name}")

    processed_indices = set()
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                processed_indices = set(json.load(f).get("processed_indices", []))
        except Exception:
            pass

    to_process = [(idx, row) for idx, row in df.iterrows()
                  if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude'))
                  and idx not in processed_indices]

    if not to_process:
        logger.info("All schools already processed!")
        return df

    if not processed_indices:
        old_cols = [c for c in df.columns if c.startswith('poi_')]
        if old_cols:
            df = df.drop(columns=old_cols)

    tracker = ProgressTracker(len(to_process))
    save_counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(enrich_school, idx, row['latitude'], row['longitude'],
                                    row.get('schulname', '')): idx
                   for idx, row in to_process}
        pbar = tqdm(total=len(to_process), desc="POI enrichment") if TQDM_AVAILABLE else None
        for future in as_completed(futures):
            try:
                idx, poi_data, ac = future.result()
                for col, val in poi_data.items():
                    df.at[idx, col] = val
                processed_indices.add(idx)
                tracker.increment(api_calls=ac)
                save_counter += 1
                if pbar: pbar.update(1)
                if save_counter >= SAVE_INTERVAL:
                    with open(CHECKPOINT_FILE, 'w') as f:
                        json.dump({"processed_indices": list(processed_indices)}, f)
                    save_counter = 0
            except Exception as e:
                tracker.increment(success=False)
                logger.warning(f"Error: {e}")
        if pbar: pbar.close()

    out = INTERMEDIATE_DIR / "munich_secondary_schools_with_pois.csv"
    df.to_csv(out, index=False, encoding='utf-8-sig')

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    stats = tracker.get_stats()
    print(f"\n{'='*70}\nMUNICH POI ENRICHMENT - COMPLETE\n{'='*70}")
    print(f"Processed: {stats['processed']}, API calls: {stats['api_calls']}")
    for cat in POI_CATEGORIES:
        col = f"poi_{cat}_count_500m"
        if col in df.columns:
            print(f"  {cat}: avg {df[col].mean():.1f}/school")
    print(f"{'='*70}")
    return df


if __name__ == "__main__":
    main()
