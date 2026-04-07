#!/usr/bin/env python3
"""
Phase 4: Bremen Crime Enrichment (Stadtteil-level PKS)

Enriches schools with crime statistics from parliamentary inquiry PDFs.
Data covers 22 Beiratsbereiche with 7 crime categories each.

Sources:
    - 2023-2024: https://www.rathaus.bremen.de/sixcms/media.php/13/20250617_top%2011_Kriminalitaet_in_den_Stadtteilen.pdf
    - 2022-2023: https://www.rathaus.bremen.de/sixcms/media.php/13/20240709_Verteilung_Kriminalitaet_auf_die_Stadtteil.pdf

Crime categories: total, sexual offenses, robbery, assault, burglary, theft, drugs.
Join: school Stadtteil -> Beiratsbereich crime data.

Input (fallback chain):
    1. data_bremen/intermediate/bremen_schools_with_transit.csv
    2. data_bremen/intermediate/bremen_schools_with_traffic.csv
    3. data_bremen/raw/bremen_school_master.csv

Output:
    - data_bremen/intermediate/bremen_schools_with_crime.csv

Reference: scripts_hamburg/enrichment/hamburg_crime_enrichment.py (PDF parsing approach)
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
    """Enrich Bremen schools with Stadtteil-level crime data."""
    raise NotImplementedError(
        "Phase 4: Bremen crime enrichment not yet implemented.\n"
        "Source: Parliamentary inquiry PDFs (22 Beiratsbereiche, 7 crime categories)\n"
        "Approach: Download PDFs, extract tables with tabula-py, join by Stadtteil\n"
        "Reference: scripts_hamburg/enrichment/hamburg_crime_enrichment.py"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
