#!/usr/bin/env python3
"""
Phase 3: Leipzig Transit Enrichment (LVB GTFS + Overpass API)
=============================================================

Enriches school data with public transit accessibility using LVB GTFS data
and OpenStreetMap Overpass API as fallback.

Data Sources:
- Primary: LVB GTFS feed from opendata.leipzig.de (tram + bus stops)
  URL: https://opendata.leipzig.de/dataset/lvb-fahrplandaten
- Supplementary: MDV GTFS (S-Bahn Mitteldeutschland)
  URL: https://www.mdv.de/downloads/
- Fallback: OpenStreetMap Overpass API (free, no key)

This script:
1. Downloads LVB GTFS ZIP, extracts stops.txt
2. For each school, finds nearest transit stops by type (tram, bus, rail)
3. Extracts TOP 3 nearest per type with name, distance, coordinates, lines
4. Calculates transit accessibility score

Output columns (Berlin schema compatible):
    transit_rail_01..03_name/distance_m/latitude/longitude/lines
    transit_tram_01..03_name/distance_m/latitude/longitude/lines
    transit_bus_01..03_name/distance_m/latitude/longitude/lines
    transit_stop_count_1000m
    transit_all_lines_1000m
    transit_accessibility_score

Input: data_leipzig/intermediate/leipzig_schools_with_traffic.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_transit.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import requests
import math
import time
import json
import logging
import zipfile
import io
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

LVB_GTFS_URL = "https://opendata.leipzig.de/dataset/8803f612-2ce1-4643-82d1-213434889200/resource/b38955c4-431c-4e8b-a4ef-9964a3a2c95d/download/gtfsmdvlvb.zip"

# Fallback input chain
INPUT_FALLBACKS = [
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    RAW_DIR / "leipzig_schools_raw.csv",
]


def main():
    """Enrich Leipzig schools with transit accessibility data."""
    raise NotImplementedError(
        "Phase 3: Leipzig transit enrichment not yet implemented.\n"
        "TODO:\n"
        "  1. Download LVB GTFS ZIP → extract stops.txt (stop_id, stop_name, stop_lat, stop_lon)\n"
        "  2. Parse route_type from routes.txt: 0=tram, 3=bus, 2=rail\n"
        "  3. Join stops → stop_times → trips → routes to get stop→route_type mapping\n"
        "  4. For each school, find TOP 3 nearest stops per type (tram, bus, rail)\n"
        "  5. Count stops within 1000m, collect unique lines\n"
        "  6. Calculate accessibility score\n"
        "  7. Fallback to Overpass API for any missing coverage"
    )


if __name__ == "__main__":
    main()
