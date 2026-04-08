#!/usr/bin/env python3
"""
Phase 4: Leipzig Crime Data Enrichment (Ortsteil-Level)
=======================================================

Enriches school data with crime statistics from the Leipzig Open Data API.
Leipzig provides excellent Ortsteil-level (63 districts) crime data — one of
the best granularities among our pipeline cities.

Data Source: Leipzig statistik.leipzig.de Open Data API
CSV endpoint: https://statistik.leipzig.de/opendata/api/kdvalues?kategorie_nr=12&rubrik_nr=1&periode=y&format=csv
Format: CSV (semicolon-separated)
Granularity: 63 Ortsteile + 10 Stadtbezirke
Years: 2004–2023 (annual)
License: CC BY 4.0 / DL-BY-DE 2.0

Crime categories (Sachmerkmal):
    - Straftaten insgesamt (total crimes)
    - Diebstahl (theft)
    - Körperverletzung (assault)
    - Vermögensdelikte (property crimes, from 2010)
    - Straftaten je Einwohner (per-capita rate)

This script:
1. Downloads Ortsteil-level crime data from Leipzig Open Data API
2. Geocodes each school to its Ortsteil (using Leipzig Geodaten boundaries or name match)
3. Joins crime metrics from the most recent year
4. Calculates crime safety scores relative to city average

Output columns (Berlin schema compatible):
    crime_total, crime_theft, crime_assault, crime_property
    crime_per_1000_residents
    crime_safety_category, crime_safety_rank
    crime_ortsteil

Input: data_leipzig/intermediate/leipzig_schools_with_transit.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_crime.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import requests
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

CRIME_API_URL = "https://statistik.leipzig.de/opendata/api/kdvalues?kategorie_nr=12&rubrik_nr=1&periode=y&format=csv"

# Fallback input chain
INPUT_FALLBACKS = [
    INTERMEDIATE_DIR / "leipzig_schools_with_transit.csv",
    INTERMEDIATE_DIR / "leipzig_schools_with_traffic.csv",
    RAW_DIR / "leipzig_schools_raw.csv",
]


def main():
    """Enrich Leipzig schools with Ortsteil-level crime statistics."""
    raise NotImplementedError(
        "Phase 4: Leipzig crime enrichment not yet implemented.\n"
        "TODO: Adapt Hamburg crime enrichment pattern (district-level join)\n"
        "  1. Download CSV from Leipzig Open Data API\n"
        "  2. Parse pivot table: rows=Ortsteil, cols=year, values=crime counts\n"
        "  3. Map each school to its Ortsteil (from supplementary Leipzig data or geocode)\n"
        "  4. Join most recent year's crime data per Ortsteil\n"
        "  5. Calculate safety score relative to city average"
    )


if __name__ == "__main__":
    main()
