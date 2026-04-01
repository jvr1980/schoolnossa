#!/usr/bin/env python3
"""
Phase 3: Munich Transit Accessibility Enrichment

Enriches school data with transit accessibility metrics using MVV GTFS data.
Falls back to Overpass API if GTFS data unavailable.

Data Source (primary): MVV GTFS from opendata.muenchen.de
    - gesamt_gtfs.zip (~14 MB) — U-Bahn, S-Bahn, Tram, Bus
    - License: CC-BY (MVV GmbH)
Data Source (fallback): OpenStreetMap Overpass API (free, no key)

Coordinate system: WGS84 (EPSG:4326)

This script:
1. Downloads MVV GTFS feed (or queries Overpass API as fallback)
2. Extracts stop locations from stops.txt
3. Classifies stops by type (U-Bahn, S-Bahn, Tram, Bus)
4. Calculates distance from each school to nearest stops by type
5. Counts stops within configurable radius (500m, 1000m)
6. Adds transit columns to school data

Input: data_munich/intermediate/munich_secondary_schools_with_traffic.csv
       (fallback: munich_secondary_schools.csv)
Output: data_munich/intermediate/munich_secondary_schools_with_transit.csv

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

MVV_GTFS_URL = "https://www.mvv-muenchen.de/fileadmin/mediapool/02-Fahrplanauskunft/03-Downloads/openData/gesamt_gtfs.zip"
SEARCH_RADIUS_M = 1000

# Munich bounding box for Overpass fallback
MUNICH_BBOX = (48.06, 11.36, 48.25, 11.72)


def main():
    raise NotImplementedError(
        "Phase 3: Munich transit enrichment not yet implemented.\n"
        "TODO: Use MVV GTFS stops.txt for stop locations, or Overpass API fallback.\n"
        "  - Download gesamt_gtfs.zip from MVV\n"
        "  - Parse stops.txt for stop coordinates and route types\n"
        "  - Calculate nearest U-Bahn, S-Bahn, Tram, Bus for each school"
    )


if __name__ == "__main__":
    main()
