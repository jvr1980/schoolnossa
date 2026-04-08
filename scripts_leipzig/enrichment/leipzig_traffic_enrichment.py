#!/usr/bin/env python3
"""
Phase 2: Leipzig Traffic Data Enrichment (Unfallatlas)
======================================================

Enriches school data with traffic accident statistics from the federal Unfallatlas.
Adapted from NRW traffic enrichment — same data source, different ULAND filter.

Data Source: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
Format: CSV (semicolon-separated, UTF-8-sig), German decimal separator (comma)
Coordinates: EPSG:25832 (UTM Zone 32N) — requires conversion to WGS84

Filtering: ULAND=14 (Sachsen), then spatial filter for Leipzig area

This script:
1. Downloads Unfallatlas CSV for recent years
2. Filters for Sachsen (ULAND=14) and Leipzig area
3. Counts accidents within 500m radius of each school
4. Classifies by severity (UKATEGORIE) and vehicle type
5. Merges accident metrics into school data

Output columns (Berlin schema compatible):
    traffic_accidents_500m, traffic_accidents_1000m
    traffic_severity_1_count, traffic_severity_2_count, traffic_severity_3_count
    traffic_pedestrian_accidents, traffic_bicycle_accidents
    traffic_safety_score

Input: data_leipzig/raw/leipzig_schools_raw.csv (fallback chain)
Output: data_leipzig/intermediate/leipzig_schools_with_traffic.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import requests
import zipfile
import io
import math
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

UNFALLATLAS_URL_TEMPLATE = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte{year}_EPSG25832_CSV.zip"
SEARCH_RADIUS_M = 500
YEARS_TO_DOWNLOAD = [2024, 2023, 2022]
SACHSEN_ULAND_CODE = '14'

# Fallback input chain (most-enriched first)
INPUT_FALLBACKS = [
    RAW_DIR / "leipzig_schools_raw.csv",
]


def main():
    """Enrich Leipzig schools with Unfallatlas traffic accident data."""
    raise NotImplementedError(
        "Phase 2: Leipzig traffic enrichment not yet implemented.\n"
        "TODO: Adapt scripts_nrw/enrichment/nrw_traffic_enrichment.py\n"
        "  - Change ULAND filter from '05' (NRW) to '14' (Sachsen)\n"
        "  - Filter spatially to Leipzig bounding box\n"
        "  - Same radius counting and severity classification logic"
    )


if __name__ == "__main__":
    main()
