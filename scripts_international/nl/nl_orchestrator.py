#!/usr/bin/env python3
"""
Netherlands School Data Pipeline Orchestrator

Phases:
    1. School Master Data    — Download DUO registry + enrollment + exams + staff
    2. Geocoding             — Nominatim geocoding of school addresses
    3. Traffic Enrichment    — BRON accident data near schools
    4. Transit Enrichment    — OVapi GTFS nearest stops
    5. Crime Enrichment      — CBS buurt-level crime via OData
    6. POI Enrichment        — CBS Nabijheidsstatistiek + Google Places
    7. Demographics          — CBS Kerncijfers buurt-level data
    8. Website & Descriptions — LLM-generated school descriptions
    9. Schema Transform      — Produce core + NL extension schema output

Usage:
    python nl_orchestrator.py                    # Run all phases
    python nl_orchestrator.py --phases 1,2       # Run specific phases
    python nl_orchestrator.py --phases 1,2,9     # Download, geocode, transform
    python nl_orchestrator.py --dry-run          # Show phases without running
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

COUNTRY_CODE = "NL"
COUNTRY_NAME = "Netherlands"
SCHOOL_TYPE = "secondary"

PROJECT_ROOT = Path(__file__).parent.parent.parent
SCRIPTS_DIR = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data_nl"

# Ensure data dirs exist
for subdir in ["raw", "intermediate", "final", "cache", "descriptions"]:
    (DATA_DIR / subdir).mkdir(parents=True, exist_ok=True)

# Logging
LOG_FILE = DATA_DIR / "orchestrator.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
logger = logging.getLogger(__name__)

# Add project root for imports
sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# PHASE FUNCTIONS
# =============================================================================

def run_phase_1():
    """Phase 1: Download and parse DUO school registry + enrollment + exams + staff."""
    from scripts_international.nl.scrapers.duo_school_registry import main
    main()


def run_phase_2():
    """Phase 2: Geocode school addresses via Nominatim."""
    from scripts_international.nl.scrapers.geocode_schools import main
    main()


def run_phase_3():
    """Phase 3: Traffic/road safety enrichment (BRON accident data)."""
    from scripts_international.nl.enrichment.nl_traffic_enrichment import main
    main()


def run_phase_4():
    """Phase 4: Transit enrichment via OVapi GTFS."""
    from scripts_international.nl.enrichment.nl_transit_enrichment import main
    main()


def run_phase_5():
    """Phase 5: Crime enrichment via CBS OData API."""
    from scripts_international.nl.enrichment.nl_crime_enrichment import main
    main()


def run_phase_6():
    """Phase 6: POI enrichment (Google Places API)."""
    from scripts_international.nl.enrichment.nl_poi_enrichment import main
    main()


def run_phase_7():
    """Phase 7: Demographics enrichment via CBS Kerncijfers."""
    from scripts_international.nl.enrichment.nl_demographics_enrichment import main
    main()


def run_phase_8():
    """Phase 8: School descriptions + structured data extraction (gpt-5.3-mini with thinking)."""
    from scripts_international.description_pipeline_international import run_description_pipeline
    run_description_pipeline("NL", passes={0, 1, 2})


def run_phase_9():
    """Phase 9: Data combination + schema transform + Berlin output."""
    from scripts_international.nl.processing.nl_combine_and_finalize import main
    main()


# =============================================================================
# ORCHESTRATOR ENGINE
# =============================================================================

AVAILABLE_PHASES = {
    1: ("DUO School Registry", run_phase_1),
    2: ("Geocode Addresses", run_phase_2),
    3: ("Traffic Enrichment", run_phase_3),
    4: ("Transit Enrichment", run_phase_4),
    5: ("Crime Enrichment", run_phase_5),
    6: ("POI Enrichment", run_phase_6),
    7: ("Demographics Enrichment", run_phase_7),
    8: ("Descriptions + Data Extraction (gpt-5.3-mini)", run_phase_8),
    9: ("Schema Transform + Berlin Output", run_phase_9),
}


def run_orchestrator(phases: list[int] = None, dry_run: bool = False):
    """Run the pipeline orchestrator."""
    if phases is None:
        phases = list(AVAILABLE_PHASES.keys())

    logger.info("=" * 60)
    logger.info(f"{COUNTRY_NAME} School Data Pipeline Orchestrator")
    logger.info(f"Country: {COUNTRY_CODE} | School type: {SCHOOL_TYPE}")
    logger.info(f"Started: {datetime.now().isoformat()}")
    logger.info(f"Phases to run: {phases}")
    logger.info("=" * 60)

    results = {}
    for phase_num in phases:
        if phase_num not in AVAILABLE_PHASES:
            logger.warning(f"Unknown phase {phase_num}, skipping.")
            continue

        name, func = AVAILABLE_PHASES[phase_num]
        logger.info(f"\n{'='*60}")
        logger.info(f"PHASE {phase_num}: {name}")
        logger.info(f"{'='*60}")

        if dry_run:
            logger.info(f"  [DRY RUN] Would run: {name}")
            results[phase_num] = "skipped (dry run)"
            continue

        start = time.time()
        try:
            func()
            elapsed = time.time() - start
            results[phase_num] = f"OK ({elapsed:.1f}s)"
            logger.info(f"Phase {phase_num} completed in {elapsed:.1f}s")
        except NotImplementedError as e:
            results[phase_num] = f"NOT IMPLEMENTED: {e}"
            logger.warning(f"Phase {phase_num}: {e}")
        except Exception as e:
            elapsed = time.time() - start
            results[phase_num] = f"FAILED: {e}"
            logger.error(f"Phase {phase_num} failed after {elapsed:.1f}s: {e}", exc_info=True)
            if phase_num == 1:
                logger.error("Phase 1 is critical — stopping pipeline.")
                break

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    for phase_num, result in results.items():
        name = AVAILABLE_PHASES[phase_num][0]
        status = "OK" if result.startswith("OK") else "SKIP" if "NOT IMPL" in result else "FAIL"
        logger.info(f"  Phase {phase_num} ({name}): [{status}] {result}")
    logger.info(f"Finished: {datetime.now().isoformat()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Netherlands School Data Pipeline Orchestrator"
    )
    parser.add_argument(
        "--phases", type=str, default=None,
        help="Comma-separated phase numbers (e.g., '1,2,9')"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show phases without running")
    args = parser.parse_args()

    phases = None
    if args.phases:
        phases = [int(p.strip()) for p in args.phases.split(",")]

    run_orchestrator(phases=phases, dry_run=args.dry_run)
