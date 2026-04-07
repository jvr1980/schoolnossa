#!/usr/bin/env python3
"""
Phase 5: Dresden POI Enrichment (Google Places API)

Same shared pattern as NRW/Hamburg — Google Places Nearby Search for each school.
Requires GOOGLE_PLACES_API_KEY in environment or config.yaml.

Reference: scripts_nrw/enrichment/nrw_poi_enrichment.py

Input (fallback chain):
  1. data_dresden/intermediate/dresden_schools_with_crime.csv
  2. data_dresden/intermediate/dresden_schools_with_transit.csv
  3. data_dresden/raw/dresden_schools_raw.csv

Output: data_dresden/intermediate/dresden_schools_with_poi.csv
"""

import pandas as pd
import requests
import os
import math
import json
import time
import logging
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
DATA_DIR = PROJECT_ROOT / "data_dresden"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
SEARCH_RADIUS_M = 500
TOP_N_POIS = 3
REQUEST_DELAY_S = 0.1

POI_CATEGORIES = {
    "supermarket": {"includedTypes": ["supermarket", "grocery_store"]},
    "restaurant": {"includedTypes": ["restaurant"]},
    "bakery_cafe": {"includedTypes": ["bakery", "cafe"]},
    "kita": {"includedTypes": ["preschool"]},
    "primary_school": {"includedTypes": ["primary_school"]},
    "secondary_school": {"includedTypes": ["secondary_school", "school"]},
}


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def search_nearby_places(lat, lon, category, radius=SEARCH_RADIUS_M):
    """Query Google Places API (New) for nearby POIs."""
    if not GOOGLE_PLACES_API_KEY:
        return []

    cat_config = POI_CATEGORIES.get(category, {})
    included_types = cat_config.get("includedTypes", [])

    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_PLACES_API_KEY,
        'X-Goog-FieldMask': 'places.displayName,places.location,places.types,places.rating',
    }

    body = {
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": radius
            }
        },
        "includedTypes": included_types,
        "maxResultCount": 20,
    }

    try:
        resp = requests.post(NEARBY_SEARCH_URL, json=body, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for place in data.get('places', []):
            loc = place.get('location', {})
            name = place.get('displayName', {}).get('text', 'Unknown')
            plat = loc.get('latitude')
            plon = loc.get('longitude')

            if plat and plon:
                dist = haversine_distance(lat, lon, plat, plon)
                results.append({
                    'name': name,
                    'latitude': plat,
                    'longitude': plon,
                    'distance_m': round(dist),
                    'rating': place.get('rating'),
                })

        results.sort(key=lambda x: x['distance_m'])
        return results

    except Exception as e:
        logger.warning(f"Places API error for ({lat},{lon}) {category}: {e}")
        return []


def enrich_school_with_pois(school_row):
    """Get POI counts and nearest POIs for a single school."""
    lat = school_row.get('latitude')
    lon = school_row.get('longitude')

    if pd.isna(lat) or pd.isna(lon):
        return {}

    result = {}
    for category in POI_CATEGORIES:
        time.sleep(REQUEST_DELAY_S)
        places = search_nearby_places(float(lat), float(lon), category)

        result[f'poi_{category}_count_500m'] = len(places)

        for rank in range(TOP_N_POIS):
            prefix = f'poi_{category}_{rank+1:02d}'
            if rank < len(places):
                p = places[rank]
                result[f'{prefix}_name'] = p['name']
                result[f'{prefix}_distance_m'] = p['distance_m']
            else:
                result[f'{prefix}_name'] = None
                result[f'{prefix}_distance_m'] = None

    return result


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden POI Enrichment (Google Places API)")
    logger.info("=" * 60)

    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not set. Set it in environment or .env file.")
        raise RuntimeError("Missing GOOGLE_PLACES_API_KEY")

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Load school data (fallback chain)
    for candidate in [
        INTERMEDIATE_DIR / "dresden_schools_with_crime.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_transit.csv",
        INTERMEDIATE_DIR / "dresden_schools_with_traffic.csv",
        RAW_DIR / "dresden_schools_raw.csv",
    ]:
        if candidate.exists():
            input_file = candidate
            break
    else:
        raise FileNotFoundError("No school data found")

    schools_df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(schools_df)} schools from {input_file.name}")

    # Check for checkpoint
    checkpoint_file = CACHE_DIR / "dresden_poi_checkpoint.json"
    completed = {}
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            completed = json.load(f)
        logger.info(f"Resuming from checkpoint: {len(completed)} schools already done")

    # Enrich each school
    all_poi_data = {}
    iterator = schools_df.iterrows()
    if TQDM_AVAILABLE:
        iterator = tqdm(list(iterator), desc="POI enrichment")

    for idx, row in iterator:
        key = str(row.get('schulnummer', idx))

        if key in completed:
            all_poi_data[key] = completed[key]
            continue

        poi_data = enrich_school_with_pois(row)
        all_poi_data[key] = poi_data
        completed[key] = poi_data

        # Checkpoint periodically
        if len(all_poi_data) % 25 == 0:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(checkpoint_file, 'w') as f:
                json.dump(completed, f)

    # Save final checkpoint
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, 'w') as f:
        json.dump(completed, f)

    # Merge POI data into schools
    all_poi_cols = set()
    for v in all_poi_data.values():
        all_poi_cols.update(v.keys())

    for col in sorted(all_poi_cols):
        schools_df[col] = None

    for idx, row in schools_df.iterrows():
        key = str(row.get('schulnummer', idx))
        if key in all_poi_data:
            for col, val in all_poi_data[key].items():
                schools_df.at[idx, col] = val

    # Save
    out_path = INTERMEDIATE_DIR / "dresden_schools_with_poi.csv"
    schools_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {out_path}")

    print(f"\n{'='*70}")
    print("DRESDEN POI ENRICHMENT - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools enriched: {len(all_poi_data)}")
    for cat in POI_CATEGORIES:
        col = f'poi_{cat}_count_500m'
        if col in schools_df.columns:
            avg = schools_df[col].mean()
            print(f"  {cat}: avg {avg:.1f} within 500m")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
