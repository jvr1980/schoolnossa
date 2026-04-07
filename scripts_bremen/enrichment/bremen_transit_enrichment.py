#!/usr/bin/env python3
"""
Phase 3: Bremen Transit Enrichment (Overpass API)

Enriches schools with nearest public transit stops using OpenStreetMap Overpass API.
Queries for bus stops, tram stops, and S-Bahn stations within Bremen bounding box.

Input (fallback chain):
    1. data_bremen/intermediate/bremen_schools_with_traffic.csv
    2. data_bremen/raw/bremen_school_master.csv

Output:
    - data_bremen/intermediate/bremen_schools_with_transit.csv

Reference: scripts_nrw/enrichment/nrw_transit_enrichment.py
"""

import logging
import sys
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent.parent.resolve()
DATA_DIR = BASE_DIR / "data_bremen"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logger = logging.getLogger(__name__)


def main():
    """Enrich Bremen schools with transit stop proximity data."""
    raise NotImplementedError(
        "Phase 3: Bremen transit enrichment not yet implemented.\n"
        "Source: Overpass API (free, no key needed)\n"
        "Query: highway=bus_stop, railway=tram_stop, public_transport=stop_position within Bremen bbox\n"
        "Reference: scripts_nrw/enrichment/nrw_transit_enrichment.py"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
