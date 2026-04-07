#!/usr/bin/env python3
"""
Phase 3: Dresden Transit Enrichment

Source: Overpass API (OpenStreetMap)
Access: Free, no API key needed
Query: bus_stop, tram_stop, station, halt within Dresden bounding box
        lat 50.96–51.14, lon 13.57–13.90

Approach: Same as NRW pipeline — query Overpass for transit stops near each school.
Reference: scripts_nrw/enrichment/nrw_transit_enrichment.py

Input (fallback chain):
  1. data_dresden/intermediate/dresden_schools_with_traffic.csv
  2. data_dresden/raw/dresden_schools_raw.csv

Output: data_dresden/intermediate/dresden_schools_with_transit.csv
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
        "Dresden transit enrichment not yet implemented. "
        "Should use Overpass API (same pattern as NRW transit enrichment)."
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
