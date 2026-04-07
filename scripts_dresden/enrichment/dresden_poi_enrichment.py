#!/usr/bin/env python3
"""
Phase 5: Dresden POI Enrichment

Source: Google Places API (New)
Access: Requires GOOGLE_PLACES_API_KEY in config.yaml
Method: 500m radius search around each school location

Approach: Same shared pattern as Hamburg/NRW — Google Places nearby search.
Reference: scripts_nrw/enrichment/nrw_poi_enrichment.py

Input (fallback chain):
  1. data_dresden/intermediate/dresden_schools_with_crime.csv
  2. data_dresden/intermediate/dresden_schools_with_transit.csv
  3. data_dresden/raw/dresden_schools_raw.csv

Output: data_dresden/intermediate/dresden_schools_with_poi.csv
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
        "Dresden POI enrichment not yet implemented. "
        "Should use Google Places API (same pattern as NRW POI enrichment)."
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
