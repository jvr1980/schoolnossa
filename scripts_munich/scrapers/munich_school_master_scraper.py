#!/usr/bin/env python3
"""
Phase 1: Munich School Master Data Scraper

Downloads and processes school data from Bayern Schulsuche CSV export.

Data Source: https://www.km.bayern.de/schulsuche (CSV export)
Format: CSV (semicolon-separated, ISO-8859-15 encoding)
Fields: Schulnummer; Schulart; Name; Straße; PLZ; Ort; Link

This script:
1. Downloads the school master CSV from km.bayern.de Schulsuche
2. Parses with ISO-8859-15 encoding, semicolon separator
3. Filters for München schools (PLZ prefix 80/81 or Ort contains "München")
4. Filters for secondary school types (Gymnasium, Realschule, Mittelschule, etc.)
5. Geocodes addresses to get lat/lon via Google Geocoding API or Nominatim
6. Supplements with opendata.muenchen.de school data if available
7. Outputs to data_munich/intermediate/munich_secondary_schools.csv

Input: Bayern Schulsuche CSV (web download)
Output: data_munich/intermediate/munich_secondary_schools.csv

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
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Bayern Schulsuche CSV export URL
SCHULSUCHE_URL = "https://www.km.bayern.de/schulsuche"

# Secondary school types in Bavaria
SECONDARY_SCHULARTEN = [
    "Gymnasium",
    "Realschule",
    "Mittelschule",
    "Wirtschaftsschule",
    "Freie Waldorfschule",
    "Integrierte Gesamtschule",
    "Förderzentrum",
]

# Munich postal code prefixes
MUNICH_PLZ_PREFIXES = ["80", "81"]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}


def main():
    raise NotImplementedError(
        "Phase 1: Munich school master scraper not yet implemented.\n"
        "TODO:\n"
        "  1. Download CSV from km.bayern.de Schulsuche (semicolon, ISO-8859-15)\n"
        "  2. Filter for München (PLZ 80xxx/81xxx or Ort='München')\n"
        "  3. Filter for secondary Schularten\n"
        "  4. Geocode addresses to get latitude/longitude\n"
        "  5. Scrape school detail pages for website URLs\n"
        "  6. Save to data_munich/intermediate/munich_secondary_schools.csv"
    )


if __name__ == "__main__":
    main()
