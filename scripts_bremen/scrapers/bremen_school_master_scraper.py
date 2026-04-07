#!/usr/bin/env python3
"""
Phase 1: Bremen School Master Data Scraper

Downloads and merges two data sources:
1. Schulwegweiser Excel from bildung.bremen.de (school details)
2. GeoBremen Shapefile from gdi2.geo.bremen.de (coordinates in EPSG:25832)

Join strategy: Match by school name + address or Schulnummer.
Coordinate conversion: EPSG:25832 (UTM Zone 32N) -> WGS84 (EPSG:4326) via pyproj.

Input:
    - Schulwegweiser Excel: https://www.bildung.bremen.de/schulwegweiser-3714
    - Shapefile ZIP: https://gdi2.geo.bremen.de/inspire/download/Schulstandorte/data/Schulstandorte_HB_BHV.zip

Output:
    - data_bremen/raw/bremen_school_master.csv
"""

import logging
import sys
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent.parent.resolve()
DATA_DIR = BASE_DIR / "data_bremen"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

logger = logging.getLogger(__name__)


def main():
    """Download and merge Bremen school master data."""
    raise NotImplementedError(
        "Phase 1: Bremen school master scraper not yet implemented.\n"
        "Sources:\n"
        "  1. Schulwegweiser Excel: https://www.bildung.bremen.de/schulwegweiser-3714\n"
        "  2. GeoBremen Shapefile: https://gdi2.geo.bremen.de/inspire/download/Schulstandorte/data/Schulstandorte_HB_BHV.zip\n"
        "Approach: Download both, convert SHP coords from EPSG:25832 to WGS84, join on school name/address."
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
