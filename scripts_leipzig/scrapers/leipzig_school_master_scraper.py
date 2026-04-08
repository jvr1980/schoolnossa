#!/usr/bin/env python3
"""
Phase 1: Leipzig School Master Data Scraper
============================================

Downloads school master data from the Sächsische Schuldatenbank REST API.

Data Source: https://schuldatenbank.sachsen.de/api/v1/schools
Format: CSV (comma-separated, UTF-8)
Coordinates: WGS84 (EPSG:4326) — latitude/longitude fields
Auth: None required for basic school data

API Parameters:
    format=csv          — CSV export
    address=Leipzig     — filter by city
    limit=500           — override default 20-row limit
    pre_registered=yes  — only active schools
    only_schools=yes    — exclude non-school institutions
    school_category_key=10 — allgemeinbildende Schulen only

School type keys (general education):
    11 = Grundschule
    12 = Oberschule
    13 = Gymnasium
    14 = Förderschule

Output: data_leipzig/raw/leipzig_schools_raw.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import requests
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

# Sächsische Schuldatenbank API
SCHULDATENBANK_API = "https://schuldatenbank.sachsen.de/api/v1/schools"


def main():
    """Download Leipzig school master data from Schuldatenbank API."""
    raise NotImplementedError(
        "Phase 1: Leipzig school master scraper not yet implemented.\n"
        "TODO:\n"
        "  1. GET from Schuldatenbank API with format=csv&address=Leipzig&limit=500\n"
        "  2. Filter school_category_key=10 for allgemeinbildende Schulen\n"
        "  3. Rename API columns to pipeline standard (latitude→lat, longitude→lon, etc.)\n"
        "  4. Merge Schul_ID + Ortsteil from Leipzig Open Data supplementary CSV\n"
        "  5. Merge student counts from Leipzig Open Data Dataset B\n"
        "  6. Save to data_leipzig/raw/leipzig_schools_raw.csv"
    )


if __name__ == "__main__":
    main()
