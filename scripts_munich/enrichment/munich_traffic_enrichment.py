#!/usr/bin/env python3
"""
Phase 2: Munich Traffic Data Enrichment (Unfallatlas)

Enriches school data with traffic accident statistics from the national Unfallatlas.
Same approach as NRW pipeline — accident-based, not sensor-based.

Data Source: https://unfallatlas.statistikportal.de/
Format: CSV (semicolon, UTF-8-sig), German comma decimal separator in coordinates
Filter: ULAND=09 (Bayern), then bounding box for München area
Coordinate system: WGS84 (EPSG:4326)

This script:
1. Downloads Unfallatlas CSV for recent years (2022-2024)
2. Filters for Bayern (ULAND=09) and München bounding box
3. Counts accidents within 500m radius of each school
4. Classifies by severity (Leichtverletzte, Schwerverletzte, Getötete)
5. Adds traffic columns to school data

Input: data_munich/intermediate/munich_secondary_schools.csv (or most-enriched available)
Output: data_munich/intermediate/munich_secondary_schools_with_traffic.csv

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
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
BAYERN_ULAND_CODE = '09'

# Munich bounding box (approximate)
MUNICH_BBOX = {
    'lat_min': 48.06, 'lat_max': 48.25,
    'lon_min': 11.36, 'lon_max': 11.72,
}


def main():
    raise NotImplementedError(
        "Phase 2: Munich traffic enrichment not yet implemented.\n"
        "TODO: Adapt from scripts_nrw/enrichment/nrw_traffic_enrichment.py\n"
        "  - Change ULAND filter from '05' (NRW) to '09' (Bayern)\n"
        "  - Use Munich bounding box for geographic filter\n"
        "  - Input/output paths for munich data"
    )


if __name__ == "__main__":
    main()
