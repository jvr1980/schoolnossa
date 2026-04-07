#!/usr/bin/env python3
"""
Stuttgart POI Enrichment
Enriches school data with nearby Points of Interest using Google Places API (New).

POI Categories: supermarket, restaurant, bakery_cafe, kita, primary_school, secondary_school

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
ENV_FILE = PROJECT_ROOT / ".env"

# Load API keys from .env and config.yaml
try:
    if ENV_FILE.exists():
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE)
except Exception:
    pass

try:
    import yaml
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            _cfg = yaml.safe_load(f) or {}
        _keys = _cfg.get("api_keys", {})
        if _keys.get("google_places") and "GOOGLE_PLACES_API_KEY" not in os.environ:
            os.environ["GOOGLE_PLACES_API_KEY"] = _keys["google_places"]
except Exception:
    pass

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
SEARCH_RADIUS_M = 500
REQUEST_DELAY_S = 0.1
TOP_N_POIS = 3
MAX_WORKERS = 5
SAVE_INTERVAL = 25
MAX_RETRIES = 3

CHECKPOINT_FILE = INTERMEDIATE_DIR / "stuttgart_poi_enrichment_checkpoint.json"
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
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.processed = self.errors = self.api_calls = 0
        self.lock = threading.Lock()
        self.start = time.time()

    def increment(self, success=True, api_calls=0):
        with self.lock:
            self.processed += 1
            if not success: self.errors += 1
            self.api_calls += api_calls


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
        "maxResultCount": 20
    }
    if included_types:
        body["includedTypes"] = included_types

    api_calls = 0
    results = []
    backoff = 1.0

    for _ in range(MAX_RETRIES):
        try:
            r = requests.post(NEARBY_SEARCH_URL, headers=headers, json=body, timeout=15)
            api_calls += 1
            if r.status_code == 200:
                results.extend(r.json().get("places", []))
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    time.sleep(REQUEST_DELAY_S)

    if text_query and len(results) < 5:
        text_body = {
            "textQuery": text_query,
            "locationBias": {"circle": {"center": {"latitude": lat, "longitude": lon}, "radius": float(radius)}},
            "maxResultCount": 20
        }
        for _ in range(MAX_RETRIES):
            try:
                r = requests.post(TEXT_SEARCH_URL, headers=headers, json=text_body, timeout=15)
                api_calls += 1
                if r.status_code == 200:
                    results.extend(r.json().get("places", []))
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
        places, api = fetch_nearby_places(lat, lon, params.get("includedTypes", []), params.get("textQuery"))
        total_api += api

        for p in places:
            loc = p.get("location", {})
            plat, plon = loc.get("latitude"), loc.get("longitude")
            p["distance_m"] = haversine_distance(lat, lon, plat, plon) if plat and plon else 999999
        places.sort(key=lambda x: x.get("distance_m", 999999))

        if cat in ("primary_school", "secondary_school"):
            places = [p for p in places
                      if school_name.lower() not in p.get("displayName", {}).get("text", "").lower()
                      and p.get("distance_m", 0) > 50]

        result[f"poi_{cat}_count_500m"] = len([p for p in places if p.get("distance_m", 999999) <= 500])

        if cat in DETAILED_CATEGORIES:
            for i in range(TOP_N_POIS):
                prefix = f"poi_{cat}_{i+1:02d}"
                if i < len(places):
                    p = places[i]
                    dn = p.get("displayName", {})
                    name = dn.get("text") if isinstance(dn, dict) else dn
                    loc = p.get("location", {})
                    result[f"{prefix}_name"] = name
                    result[f"{prefix}_address"] = p.get("formattedAddress")
                    result[f"{prefix}_distance_m"] = round(p.get("distance_m", 0))
                    result[f"{prefix}_latitude"] = loc.get("latitude")
                    result[f"{prefix}_longitude"] = loc.get("longitude")
                else:
                    for f in ['name', 'address', 'distance_m', 'latitude', 'longitude']:
                        result[f"{prefix}_{f}"] = None

    return idx, result, total_api


def enrich_schools(school_type='secondary'):
    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not set!")
        return pd.DataFrame()

    for d in [INTERMEDIATE_DIR, RAW_DIR]:
        for p in [f"stuttgart_{school_type}_schools_with_crime.csv",
                  f"stuttgart_{school_type}_schools_with_transit.csv",
                  f"stuttgart_{school_type}_schools_with_traffic.csv",
                  f"stuttgart_{school_type}_schools.csv"]:
            if (d / p).exists():
                df = pd.read_csv(d / p)
                break
        else:
            continue
        break
    else:
        raise FileNotFoundError(f"No {school_type} data found")

    logger.info(f"Loaded {len(df)} schools")

    # Checkpoint
    processed = set()
    if CHECKPOINT_FILE.exists():
        try:
            processed = set(json.load(open(CHECKPOINT_FILE)).get("processed_indices", []))
        except Exception:
            pass

    to_process = [(idx, row) for idx, row in df.iterrows()
                  if pd.notna(row.get('latitude')) and idx not in processed]

    if not to_process:
        logger.info("All schools already processed")
        return df

    tracker = ProgressTracker(len(to_process))
    save_ctr = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(enrich_school, idx, row['latitude'], row['longitude'],
                                    row.get('schulname', '')): idx
                   for idx, row in to_process}

        pbar = tqdm(total=len(to_process), desc="POI enrichment") if TQDM_AVAILABLE else None
        for future in as_completed(futures):
            try:
                idx, poi_data, api = future.result()
                for col, val in poi_data.items():
                    df.at[idx, col] = val
                processed.add(idx)
                tracker.increment(api_calls=api)
                save_ctr += 1
                if pbar: pbar.update(1)
                if save_ctr >= SAVE_INTERVAL:
                    json.dump({"processed_indices": list(processed)}, open(CHECKPOINT_FILE, 'w'))
                    save_ctr = 0
            except Exception as e:
                tracker.increment(success=False)
                logger.warning(f"Error: {e}")
        if pbar: pbar.close()

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    out = INTERMEDIATE_DIR / f"stuttgart_{school_type}_schools_with_pois.csv"
    df.to_csv(out, index=False, encoding='utf-8-sig')

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    print(f"\n{'='*70}")
    print(f"STUTTGART POI ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(df)}, API calls: {tracker.api_calls}")
    for cat in POI_CATEGORIES:
        col = f"poi_{cat}_count_500m"
        if col in df.columns:
            print(f"  {cat}: avg {df[col].mean():.1f}")
    print(f"{'='*70}")

    return df


def main():
    for st in ['primary', 'secondary']:
        try:
            enrich_schools(st)
        except Exception as e:
            logger.warning(f"{st}: {e}")


if __name__ == "__main__":
    main()
