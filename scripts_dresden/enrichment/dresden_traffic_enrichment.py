#!/usr/bin/env python3
"""
Phase 2: Dresden Traffic Enrichment

Source: Unfallatlas (Statistisches Bundesamt)
Download: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (Unfallorte{YEAR}_EPSG25832_CSV.zip)
Coordinates: UTM EPSG:25832 → convert to WGS84 with pyproj
Filter: ULAND=14 (Sachsen)

Approach: Same as NRW pipeline — count accidents within radius of each school.
Reference: scripts_nrw/enrichment/nrw_traffic_enrichment.py

Input (fallback chain):
  1. data_dresden/intermediate/dresden_schools_with_transit.csv
  2. data_dresden/raw/dresden_schools_raw.csv

Output: data_dresden/intermediate/dresden_schools_with_traffic.csv
"""

import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data_dresden"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logger = logging.getLogger(__name__)


def main():
    raise NotImplementedError(
        "Dresden traffic enrichment not yet implemented. "
        "Should use Unfallatlas data with ULAND=14 filter (same pattern as NRW)."
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
