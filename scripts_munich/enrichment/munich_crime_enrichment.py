#!/usr/bin/env python3
"""
Phase 4: Munich Crime Data Enrichment

Enriches school data with crime statistics from PP München Sicherheitsreport.

Data Source: https://www.polizei.bayern.de/kriminalitaet/statistik/006991/index.html
Format: PDF (Sicherheitsreport — needs table extraction)
Granularity: Per-Stadtbezirk (25 districts in Munich) or city-wide fallback
Also: https://stadt.muenchen.de/infos/statistik-sicherheit.html

This script:
1. Downloads Sicherheitsreport PDF (or uses pre-extracted data)
2. Extracts crime statistics by Stadtbezirk from PDF tables
3. Maps each school to its Stadtbezirk using coordinates
4. Assigns district-level crime metrics to each school
5. Fallback: uses city-wide München crime rate if PDF parsing fails
6. Adds crime columns to school data

Input: data_munich/intermediate/munich_secondary_schools_with_transit.csv
       (fallback chain: with_traffic, base schools)
Output: data_munich/intermediate/munich_secondary_schools_with_crime.csv

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
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

SICHERHEITSREPORT_URL = "https://www.polizei.bayern.de/mam/kriminalitaet/sicherheitsreport_2024.pdf"

# Munich has 25 Stadtbezirke
MUNICH_STADTBEZIRKE_COUNT = 25


def main():
    raise NotImplementedError(
        "Phase 4: Munich crime enrichment not yet implemented.\n"
        "TODO:\n"
        "  - Download Sicherheitsreport PDF and extract district crime tables\n"
        "  - Or use hardcoded city-wide crime rate as fallback (simpler)\n"
        "  - Map schools to Stadtbezirke using reverse geocoding\n"
        "  - Similar approach to NRW (city-wide estimation) or Hamburg (district-level)"
    )


if __name__ == "__main__":
    main()
