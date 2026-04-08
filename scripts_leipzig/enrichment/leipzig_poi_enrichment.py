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
import json
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Fallback input chain
INPUT_FALLBACKS = [
    INTERMEDIATE_DIR / "leipzig_schools_with_crime.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    DATA_DIR / "raw" / "leipzig_schools_raw.csv",
]


def main():
    """Enrich Leipzig schools with Google Places POI data."""
    raise NotImplementedError(
        "Phase 5: Leipzig POI enrichment not yet implemented.\n"
        "TODO: Adapt scripts_nrw/enrichment/nrw_poi_enrichment.py\n"
        "  - Same Google Places API pattern\n"
        "  - Update file paths for Leipzig\n"
        "  - Cache responses to data_leipzig/cache/"
    )


if __name__ == "__main__":
    main()
