#!/usr/bin/env python3
"""
United Kingdom (England) School Data Pipeline Orchestrator

Phases:
    1. School Master Data    — GIAS + DfE KS4/KS5 + IMD 2025
    2. (No geocoding needed — GIAS includes Easting/Northing)
    3. Traffic Enrichment    — STATS19 geocoded accident data
    4. Transit Enrichment    — NaPTAN stops
    5. Crime Enrichment      — police.uk API
    6. POI Enrichment        — Google Places API (skipped without API key)
    7. Demographics          — IMD already joined in Phase 1
    8. Data Combination      — Merge all + schema transform + Berlin output

Usage:
    python gb_orchestrator.py                    # Run all phases
    python gb_orchestrator.py --phases 1,3,5     # Run specific phases
    python gb_orchestrator.py --dry-run
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

COUNTRY_CODE = "GB"
COUNTRY_NAME = "United Kingdom (England)"
SCHOOL_TYPE = "secondary"

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_gb"

for subdir in ["raw", "intermediate", "final", "cache", "descriptions"]:
    (DATA_DIR / subdir).mkdir(parents=True, exist_ok=True)

LOG_FILE = DATA_DIR / "orchestrator.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, mode="a")],
)
logger = logging.getLogger(__name__)
sys.path.insert(0, str(PROJECT_ROOT))


def run_phase_1():
    """Phase 1: Download GIAS + DfE + IMD data."""
    from scripts_international.gb.scrapers.gias_school_registry import main
    main()


def run_phase_3():
    """Phase 3: Traffic enrichment (STATS19)."""
    from scripts_international.gb.enrichment.gb_traffic_enrichment import main
    main()


def run_phase_4():
    """Phase 4: Transit enrichment (NaPTAN)."""
    from scripts_international.gb.enrichment.gb_transit_enrichment import main
    main()


def run_phase_5():
    """Phase 5: Crime enrichment (police.uk)."""
    from scripts_international.gb.enrichment.gb_crime_enrichment import main
    main()


def run_phase_6():
    """Phase 6: POI enrichment (Google Places)."""
    # Reuse NL pattern — skip if no API key
    logger.info("POI enrichment — requires GOOGLE_PLACES_API_KEY in .env")
    import os
    from dotenv import load_dotenv
    load_dotenv()
    if not os.getenv("GOOGLE_PLACES_API_KEY"):
        logger.warning("Skipping POI — no API key set")
        return
    raise NotImplementedError("UK POI enrichment not yet implemented")


def run_phase_8():
    """Phase 8: Data combination + schema transform."""
    from scripts_international.gb.processing.gb_to_core_schema import main
    main()


AVAILABLE_PHASES = {
    1: ("GIAS + DfE + IMD Download", run_phase_1),
    3: ("Traffic Enrichment (STATS19)", run_phase_3),
    4: ("Transit Enrichment (NaPTAN)", run_phase_4),
    5: ("Crime Enrichment (police.uk)", run_phase_5),
    6: ("POI Enrichment (Google Places)", run_phase_6),
    8: ("Schema Transform + Berlin Output", run_phase_8),
}


def run_orchestrator(phases=None, dry_run=False):
    if phases is None:
        phases = list(AVAILABLE_PHASES.keys())

    logger.info("=" * 60)
    logger.info(f"{COUNTRY_NAME} School Data Pipeline")
    logger.info(f"Started: {datetime.now().isoformat()}")
    logger.info(f"Phases: {phases}")
    logger.info("=" * 60)

    results = {}
    for num in phases:
        if num not in AVAILABLE_PHASES:
            continue
        name, func = AVAILABLE_PHASES[num]
        logger.info(f"\n{'='*60}\nPHASE {num}: {name}\n{'='*60}")

        if dry_run:
            results[num] = "dry run"
            continue

        start = time.time()
        try:
            func()
            results[num] = f"OK ({time.time()-start:.1f}s)"
        except NotImplementedError as e:
            results[num] = f"NOT IMPL: {e}"
            logger.warning(str(e))
        except Exception as e:
            results[num] = f"FAILED: {e}"
            logger.error(f"Phase {num} failed: {e}", exc_info=True)
            if num == 1:
                break

    logger.info(f"\n{'='*60}\nSUMMARY\n{'='*60}")
    for num, r in results.items():
        logger.info(f"  Phase {num} ({AVAILABLE_PHASES[num][0]}): {r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"{COUNTRY_NAME} Pipeline")
    parser.add_argument("--phases", type=str)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    phases = [int(p) for p in args.phases.split(",")] if args.phases else None
    run_orchestrator(phases=phases, dry_run=args.dry_run)
