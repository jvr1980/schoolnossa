#!/usr/bin/env python3
"""
Phase 5: Munich POI Enrichment (Google Places API)

Enriches school data with Points of Interest within walking distance.
Uses the shared Google Places API approach (same as all other cities).

Data Source: Google Places API (New)
Access: Requires GOOGLE_PLACES_API_KEY environment variable
Search radius: 500m around each school

This script:
1. Reads school data with coordinates
2. Searches Google Places API for POI categories within 500m
3. Counts and categorizes nearby amenities
4. Adds POI columns to school data

POI categories: supermarkets, parks, playgrounds, pharmacies, libraries,
sports facilities, restaurants, cafes, doctors

Input: data_munich/intermediate/munich_secondary_schools_with_crime.csv
       (fallback chain: with_transit, with_traffic, base schools)
Output: data_munich/intermediate/munich_secondary_schools_with_pois.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

SEARCH_RADIUS_M = 500


def main():
    raise NotImplementedError(
        "Phase 5: Munich POI enrichment not yet implemented.\n"
        "TODO: Adapt from scripts_nrw/enrichment/nrw_poi_enrichment.py\n"
        "  - Same Google Places API approach as all other cities\n"
        "  - Requires GOOGLE_PLACES_API_KEY environment variable"
    )


if __name__ == "__main__":
    main()
