#!/usr/bin/env python3
"""
NL Phase 6: POI Enrichment

Uses the shared Google Places API enrichment script from scripts_shared/.
This is a thin wrapper that sets up NL-specific paths and calls the shared logic.

Alternatively, for NL we can use CBS Nabijheidsstatistiek (pre-computed
average distances to amenities per buurt) as a free supplement.

Input:  data_nl/intermediate/nl_schools_with_crime.csv (or earlier)
Output: data_nl/intermediate/nl_schools_with_pois.csv
"""

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    """Run POI enrichment using shared Google Places script."""
    logger.info("=" * 60)
    logger.info("NL Phase 6: POI Enrichment (Google Places API)")
    logger.info("=" * 60)

    input_candidates = [
        INTERMEDIATE_DIR / "nl_schools_with_crime.csv",
        INTERMEDIATE_DIR / "nl_schools_with_transit.csv",
        INTERMEDIATE_DIR / "nl_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "nl_school_master_geocoded.csv",
    ]
    input_path = next((p for p in input_candidates if p.exists()), None)
    if input_path is None:
        logger.error("No input file found. Run earlier phases first.")
        sys.exit(1)

    schools = pd.read_csv(input_path)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    # Check for Google Places API key
    import os
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")

    if not api_key:
        logger.warning("GOOGLE_PLACES_API_KEY not set — skipping POI enrichment")
        logger.info("Set the key in .env to enable Google Places POI data")
        # Create empty POI columns so downstream doesn't break
        poi_categories = ["supermarket", "restaurant", "bakery_cafe", "kita", "primary_school"]
        for cat in poi_categories:
            schools[f"poi_{cat}_count_500m"] = None
            for idx in ["01", "02", "03"]:
                for field in ["name", "address", "distance_m", "latitude", "longitude"]:
                    schools[f"poi_{cat}_{idx}_{field}"] = None
        schools["poi_secondary_school_count_500m"] = None

        output_path = INTERMEDIATE_DIR / "nl_schools_with_pois.csv"
        schools.to_csv(output_path, index=False)
        logger.info(f"Saved (empty POI columns): {output_path}")
        return

    # Use shared POI enrichment
    try:
        from scripts_shared.enrichment.enrich_schools_with_pois import enrich_with_pois

        output_path = INTERMEDIATE_DIR / "nl_schools_with_pois.csv"
        enriched = enrich_with_pois(
            df=schools,
            lat_col="latitude",
            lon_col="longitude",
            output_path=str(output_path),
            cache_dir=str(CACHE_DIR / "poi_cache"),
        )
        logger.info(f"Saved: {output_path}")
    except ImportError:
        logger.warning("Shared POI script not importable — running standalone approach")
        # Standalone approach using Google Places API directly
        _run_standalone_poi(schools, api_key)


def _run_standalone_poi(schools: pd.DataFrame, api_key: str):
    """Standalone POI enrichment for NL (if shared script isn't importable)."""
    import requests
    import time
    import json

    logger.info("Running standalone Google Places POI enrichment...")
    SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
    HEADERS = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.location,places.types",
    }

    poi_types = {
        "supermarket": ["supermarket"],
        "restaurant": ["restaurant"],
        "bakery_cafe": ["bakery", "cafe"],
        "kita": ["preschool", "child_care_agency"],
        "primary_school": ["primary_school"],
        "secondary_school": ["secondary_school"],
    }

    poi_cache_dir = CACHE_DIR / "poi_cache"
    poi_cache_dir.mkdir(parents=True, exist_ok=True)

    for i, (_, school) in enumerate(schools.iterrows()):
        lat, lon = school.get("latitude"), school.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            continue

        cache_file = poi_cache_dir / f"{school.get('vestiging_code', i)}.json"
        if cache_file.exists():
            cached = json.loads(cache_file.read_text())
            for key, value in cached.items():
                schools.loc[schools.index[i], key] = value
            continue

        school_pois = {}
        for category, types in poi_types.items():
            body = {
                "includedTypes": types,
                "maxResultCount": 5,
                "locationRestriction": {
                    "circle": {
                        "center": {"latitude": float(lat), "longitude": float(lon)},
                        "radius": 500.0,
                    }
                },
            }
            try:
                resp = requests.post(SEARCH_URL, headers=HEADERS, json=body, timeout=10)
                resp.raise_for_status()
                places = resp.json().get("places", [])

                # Count within 500m
                col_count = f"poi_{category}_count_500m"
                school_pois[col_count] = len(places)

                # Top 3 nearest
                for idx, place in enumerate(places[:3]):
                    prefix = f"poi_{category}_{idx + 1:02d}"
                    loc = place.get("location", {})
                    school_pois[f"{prefix}_name"] = place.get("displayName", {}).get("text", "")
                    school_pois[f"{prefix}_address"] = place.get("formattedAddress", "")
                    school_pois[f"{prefix}_latitude"] = loc.get("latitude")
                    school_pois[f"{prefix}_longitude"] = loc.get("longitude")
                    # Distance computed from coordinates
                    if loc.get("latitude") and loc.get("longitude"):
                        from nl_traffic_enrichment import haversine_distance
                        dist = haversine_distance(float(lat), float(lon),
                                                  loc["latitude"], loc["longitude"])
                        school_pois[f"{prefix}_distance_m"] = round(dist, 0)

                time.sleep(0.1)
            except Exception as e:
                logger.debug(f"  POI error for school {i}: {e}")

        # Cache and apply
        cache_file.write_text(json.dumps(school_pois))
        for key, value in school_pois.items():
            schools.loc[schools.index[i], key] = value

        if (i + 1) % 100 == 0:
            logger.info(f"  POI progress: {i + 1}/{len(schools)}")

    output_path = INTERMEDIATE_DIR / "nl_schools_with_pois.csv"
    schools.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
