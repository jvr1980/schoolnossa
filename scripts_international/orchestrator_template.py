#!/usr/bin/env python3
"""
International Pipeline Orchestrator Template

Copy this to scripts_international/{country_code}/orchestrator.py and
customize the phase functions for each country.

Usage:
    python orchestrator.py                    # Run all phases
    python orchestrator.py --phases 1,3,5     # Run specific phases
    python orchestrator.py --skip-embeddings  # Skip embedding generation
    python orchestrator.py --dry-run          # Show phases without executing

Phases:
    1. School Master Data    (scrape/download national school registry)
    2. Traffic Enrichment    (road accident data near schools)
    3. Transit Enrichment    (nearest public transit stops via GTFS)
    4. Crime Enrichment      (area crime statistics)
    5. POI Enrichment        (Google Places / Overpass nearby amenities)
    6. Demographics          (area-level socioeconomic data)
    7. Website Metadata      (school descriptions via LLM pipeline)
    8. Data Combination      (merge all enrichments + generate embeddings)
    9. Schema Transform      (produce Berlin-compatible output)
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# --- CUSTOMIZE THESE ---
COUNTRY_CODE = "XX"  # e.g., "NL", "GB", "FR", "IT", "ES"
COUNTRY_NAME = "Template"
SCHOOL_TYPE = "secondary"
# -----------------------

PROJECT_ROOT = Path(__file__).parent.parent.parent
SCRIPTS_DIR = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / f"data_{COUNTRY_CODE.lower()}"

# Logging setup
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


# =============================================================================
# PHASE FUNCTIONS — Replace these with country-specific implementations
# =============================================================================

def run_phase_1():
    """Phase 1: Download and parse school master data."""
    raise NotImplementedError(
        f"Implement school data scraper for {COUNTRY_NAME}. "
        f"See scripts_international/country_config.py for data source details."
    )


def run_phase_2():
    """Phase 2: Traffic/road safety enrichment."""
    raise NotImplementedError(
        f"Implement traffic enrichment for {COUNTRY_NAME}. "
        "Download geocoded accident data, compute accidents within radius of each school."
    )


def run_phase_3():
    """Phase 3: Transit enrichment via GTFS."""
    raise NotImplementedError(
        f"Implement transit enrichment for {COUNTRY_NAME}. "
        "Download GTFS feed, find nearest stops per transport mode."
    )


def run_phase_4():
    """Phase 4: Crime statistics enrichment."""
    raise NotImplementedError(
        f"Implement crime enrichment for {COUNTRY_NAME}. "
        "Join area crime data to schools based on location."
    )


def run_phase_5():
    """Phase 5: POI enrichment (Google Places / Overpass)."""
    # This phase can often reuse the shared POI enrichment script
    raise NotImplementedError(
        f"Implement POI enrichment for {COUNTRY_NAME}. "
        "Consider reusing scripts_shared/enrichment/enrich_schools_with_pois.py"
    )


def run_phase_6():
    """Phase 6: Demographics/socioeconomic enrichment."""
    raise NotImplementedError(
        f"Implement demographics enrichment for {COUNTRY_NAME}. "
        "Join census/statistics data to schools based on area codes."
    )


def run_phase_7():
    """Phase 7: Website metadata + school descriptions."""
    # Can reuse scripts_shared/generation/school_description_pipeline.py
    raise NotImplementedError(
        f"Implement website/description generation for {COUNTRY_NAME}. "
        "Consider reusing scripts_shared/generation/school_description_pipeline.py"
    )


def run_phase_8(skip_embeddings: bool = False):
    """Phase 8: Combine all enrichments + generate embeddings."""
    raise NotImplementedError(
        f"Implement data combination for {COUNTRY_NAME}. "
        "Merge all enrichment CSVs into final master table, generate embeddings."
    )


def run_phase_9():
    """Phase 9: Transform to Berlin schema for frontend compatibility."""
    from scripts_international.international_to_berlin_schema import transform_file
    transform_file(COUNTRY_CODE)


# =============================================================================
# ORCHESTRATOR ENGINE (no changes needed)
# =============================================================================

AVAILABLE_PHASES = {
    1: ("School Master Data", run_phase_1),
    2: ("Traffic Enrichment", run_phase_2),
    3: ("Transit Enrichment", run_phase_3),
    4: ("Crime Enrichment", run_phase_4),
    5: ("POI Enrichment", run_phase_5),
    6: ("Demographics Enrichment", run_phase_6),
    7: ("Website & Descriptions", run_phase_7),
    8: ("Data Combination & Embeddings", lambda: run_phase_8(skip_embeddings="--skip-embeddings" in sys.argv)),
    9: ("Berlin Schema Transform", run_phase_9),
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
                logger.error("Phase 1 (School Master Data) is critical — stopping pipeline.")
                break

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    for phase_num, result in results.items():
        name = AVAILABLE_PHASES[phase_num][0]
        logger.info(f"  Phase {phase_num} ({name}): {result}")
    logger.info(f"Finished: {datetime.now().isoformat()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"{COUNTRY_NAME} School Data Pipeline Orchestrator"
    )
    parser.add_argument(
        "--phases", type=str, default=None,
        help="Comma-separated phase numbers to run (e.g., '1,3,5')"
    )
    parser.add_argument(
        "--skip-embeddings", action="store_true",
        help="Skip embedding generation in phase 8"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show phases without executing"
    )
    args = parser.parse_args()

    phases = None
    if args.phases:
        phases = [int(p.strip()) for p in args.phases.split(",")]

    run_orchestrator(phases=phases, dry_run=args.dry_run)
