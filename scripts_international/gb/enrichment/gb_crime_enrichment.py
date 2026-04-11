#!/usr/bin/env python3
"""
UK Phase 5: Crime Enrichment via police.uk API

Queries the police.uk street-level crime API for each school location.
API is free, no key needed. Rate limit: ~15 req/sec.

Endpoint: GET https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={lng}

Categories: anti-social-behaviour, burglary, criminal-damage-arson, drugs,
other-theft, robbery, shoplifting, vehicle-crime, violent-crime, etc.

Input:  data_gb/intermediate/gb_schools_with_transit.csv
Output: data_gb/intermediate/gb_schools_with_crime.csv
"""

import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_gb"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

POLICE_API = "https://data.police.uk/api/crimes-street/all-crime"
CRIME_CACHE_DIR = CACHE_DIR / "police_uk"
CRIME_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_crimes_for_location(lat: float, lon: float, cache_key: str) -> dict:
    """Fetch crime data for a lat/lon from police.uk API with caching."""
    cache_file = CRIME_CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())

    params = {"lat": round(lat, 6), "lng": round(lon, 6)}
    try:
        resp = requests.get(POLICE_API, params=params, timeout=10)
        if resp.status_code == 503:
            time.sleep(2)
            resp = requests.get(POLICE_API, params=params, timeout=10)
        resp.raise_for_status()
        crimes = resp.json()
    except Exception as e:
        logger.debug(f"  police.uk error for {cache_key}: {e}")
        crimes = []

    # Aggregate by category
    counts = {}
    for crime in crimes:
        cat = crime.get("category", "other-crime")
        counts[cat] = counts.get(cat, 0) + 1

    counts["_total"] = len(crimes)
    cache_file.write_text(json.dumps(counts))
    return counts


def enrich_with_crime(schools: pd.DataFrame) -> pd.DataFrame:
    """Query police.uk for crime near each school."""
    logger.info(f"Fetching crime data for {len(schools)} schools...")

    results = []
    cached = 0
    fetched = 0

    for i, (_, school) in enumerate(schools.iterrows()):
        lat, lon = school.get("latitude"), school.get("longitude")
        urn = str(school.get("urn", i))

        if pd.isna(lat) or pd.isna(lon):
            results.append({})
            continue

        cache_file = CRIME_CACHE_DIR / f"{urn}.json"
        was_cached = cache_file.exists()

        counts = fetch_crimes_for_location(float(lat), float(lon), urn)

        result = {
            "crime_total_nearby": counts.get("_total", 0),
            "crime_violent_nearby": counts.get("violent-crime", 0),
            "crime_property_nearby": (
                counts.get("burglary", 0) + counts.get("other-theft", 0) +
                counts.get("shoplifting", 0) + counts.get("vehicle-crime", 0)
            ),
            "crime_drug_nearby": counts.get("drugs", 0),
            "crime_asb_nearby": counts.get("anti-social-behaviour", 0),
        }
        results.append(result)

        if was_cached:
            cached += 1
        else:
            fetched += 1
            time.sleep(0.07)  # ~15 req/sec

        if (i + 1) % 500 == 0:
            logger.info(f"  Progress: {i + 1}/{len(schools)} (cached: {cached}, fetched: {fetched})")

    crime_df = pd.DataFrame(results)

    # Normalize to per-1000 equivalent (police.uk returns ~1 month of nearby crimes)
    # We use raw counts for ranking since we don't have area population
    if "crime_total_nearby" in crime_df.columns:
        schools["crime_total_per_1000"] = crime_df["crime_total_nearby"].values
        schools["crime_violent_per_1000"] = crime_df["crime_violent_nearby"].values
        schools["crime_property_per_1000"] = crime_df["crime_property_nearby"].values
        schools["crime_drug_per_1000"] = crime_df["crime_drug_nearby"].values

        # Safety rank (1 = safest)
        total = crime_df["crime_total_nearby"]
        schools["crime_safety_rank"] = total.rank(method="min").astype("Int64")
        pct = total.rank(pct=True)
        schools["crime_safety_category"] = pd.cut(
            pct, bins=[0, 0.33, 0.66, 1.0], labels=["safe", "moderate", "high"]
        )

    schools["crime_data_source"] = "police.uk API"
    schools["crime_data_year"] = "2025"

    logger.info(f"Crime enrichment: {cached} cached, {fetched} newly fetched")
    return schools


def main():
    logger.info("=" * 60)
    logger.info("UK Phase 5: Crime Enrichment (police.uk)")
    logger.info("=" * 60)

    candidates = [
        INTERMEDIATE_DIR / "gb_schools_with_transit.csv",
        INTERMEDIATE_DIR / "gb_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "gb_school_master_base.csv",
    ]
    input_path = next((p for p in candidates if p.exists()), None)
    if not input_path:
        logger.error("Run earlier phases first."); sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    enriched = enrich_with_crime(schools)

    output = INTERMEDIATE_DIR / "gb_schools_with_crime.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
