#!/usr/bin/env python3
"""
Phase 4: Dresden Crime Enrichment

Source: Dresden Open Data Portal — Landeskriminalamt Sachsen
URL: https://opendata.dresden.de/ — dataset "Kriminalität ab Stadtteile 2002ff."
Format: CSV
Granularity: Per-Stadtteil (district-level)
Years: 2002–2024
License: CC BY 3.0 DE

Approach: Map each school to its Stadtteil, then assign crime rates.
Similar to Hamburg's Stadtteil-level approach (better than NRW's city-wide estimates).
Reference: scripts_hamburg/enrichment/hamburg_crime_enrichment.py

Input (fallback chain):
  1. data_dresden/intermediate/dresden_schools_with_transit.csv
  2. data_dresden/intermediate/dresden_schools_with_traffic.csv
  3. data_dresden/raw/dresden_schools_raw.csv

Output: data_dresden/intermediate/dresden_schools_with_crime.csv
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
        "Dresden crime enrichment not yet implemented. "
        "Should use Stadtteil-level crime data from Dresden Open Data Portal."
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
