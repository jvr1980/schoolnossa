#!/usr/bin/env python3
"""
Bremen School Data Asset Builder Orchestrator

This orchestrator runs the complete Bremen school data pipeline:

Phase 1: Download school master data (Schulwegweiser Excel + GeoBremen Shapefile)
Phase 2: Traffic enrichment (Unfallatlas accident data, ULAND=04)
Phase 3: Transit enrichment (Overpass API)
Phase 4: Crime enrichment (PKS Stadtteil-level from parliamentary PDFs)
Phase 5: POI enrichment (Google Places API)
Phase 6: Website metadata & descriptions
Phase 7: Data combination (merge all enrichments)
Phase 8: Embeddings & final output
Phase 9: Berlin schema enforcement

Usage:
    python Bremen_school_data_asset_builder_orchestrator.py
    python Bremen_school_data_asset_builder_orchestrator.py --phases 1,2,3
    python Bremen_school_data_asset_builder_orchestrator.py --skip-embeddings

Author: Bremen School Data Pipeline
Created: 2026-04-07
"""

import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Add scripts to path
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR))

# Configure logging
LOG_FILE = SCRIPT_DIR / "orchestrator.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ]
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_bremen"


def run_phase_1():
    """Phase 1: Download and parse school master data."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data (Schulwegweiser + GeoBremen)")
    logger.info("=" * 60)

    from scrapers.bremen_school_master_scraper import main as scrape_master
    return scrape_master()


def run_phase_2():
    """Phase 2: Traffic enrichment from Unfallatlas."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Data (Unfallatlas, ULAND=04)")
    logger.info("=" * 60)

    from enrichment.bremen_traffic_enrichment import main as enrich_traffic
    return enrich_traffic()


def run_phase_3():
    """Phase 3: Transit enrichment from Overpass API."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Enrichment (Overpass API)")
    logger.info("=" * 60)

    from enrichment.bremen_transit_enrichment import main as enrich_transit
    return enrich_transit()


def run_phase_4():
    """Phase 4: Crime enrichment from PKS data."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Statistics (Stadtteil-level PKS)")
    logger.info("=" * 60)

    from enrichment.bremen_crime_enrichment import main as enrich_crime
    return enrich_crime()


def run_phase_5():
    """Phase 5: POI enrichment from Google Places API."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Data (Google Places API)")
    logger.info("=" * 60)

    from enrichment.bremen_poi_enrichment import main as enrich_poi
    return enrich_poi()


def run_phase_6():
    """Phase 6: Website metadata & descriptions."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Website Metadata & Descriptions")
    logger.info("=" * 60)

    from enrichment.bremen_website_metadata_enrichment import main as enrich_website
    return enrich_website()


def run_phase_7():
    """Phase 7: Combine all enrichment data."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Data Combination")
    logger.info("=" * 60)

    from processing.bremen_data_combiner import main as combine_data
    return combine_data()


def run_phase_8(skip_embeddings: bool = False):
    """Phase 8: Generate embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Embeddings & Final Output")
    logger.info("=" * 60)

    if skip_embeddings:
        logger.info("Skipping embedding generation (--skip-embeddings flag)")
        os.environ['SKIP_EMBEDDINGS'] = '1'

    from processing.bremen_embeddings_generator import main as generate_embeddings
    return generate_embeddings()


def run_phase_9():
    """Phase 9: Transform to Berlin schema."""
    logger.info("=" * 60)
    logger.info("PHASE 9: Berlin Schema Enforcement")
    logger.info("=" * 60)

    from bremen_to_berlin_schema import transform_bremen_to_berlin_schema
    return transform_bremen_to_berlin_schema()


def run_full_pipeline(phases: list = None, skip_embeddings: bool = False):
    """Run the full pipeline or specified phases."""
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("BREMEN SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 70)

    results = {}

    # Define available phases
    available_phases = {
        1: ("School Master Data", run_phase_1),
        2: ("Traffic Enrichment (Unfallatlas)", run_phase_2),
        3: ("Transit Enrichment (Overpass)", run_phase_3),
        4: ("Crime Enrichment (PKS)", run_phase_4),
        5: ("POI Enrichment (Google Places)", run_phase_5),
        6: ("Website Metadata & Descriptions", run_phase_6),
        7: ("Data Combination", run_phase_7),
        8: ("Embeddings & Final Output", lambda: run_phase_8(skip_embeddings)),
        9: ("Berlin Schema Enforcement", run_phase_9),
    }

    # Determine which phases to run
    if phases is None:
        phases_to_run = sorted(available_phases.keys())
    else:
        phases_to_run = phases

    # Run specified phases
    for phase_num in phases_to_run:
        if phase_num in available_phases:
            phase_name, phase_func = available_phases[phase_num]
            try:
                logger.info(f"\nRunning Phase {phase_num}: {phase_name}")
                result = phase_func()
                results[phase_num] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Phase {phase_num} failed: {e}")
                results[phase_num] = {"status": "failed", "error": str(e)}

                # Phase 1 is critical — stop if it fails
                if phase_num == 1:
                    logger.critical("Phase 1 (School Master Data) failed — cannot continue.")
                    break
        else:
            logger.warning(f"Phase {phase_num} not recognized")
            results[phase_num] = {"status": "not_implemented"}

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 70)
    print("BREMEN SCHOOL DATA ASSET BUILDER - COMPLETE")
    print("=" * 70)
    print(f"\nDuration: {duration}")
    print("\nPhase Results:")

    for phase_num in sorted(results.keys()):
        result = results[phase_num]
        status = result.get("status", "unknown")
        phase_name = available_phases.get(phase_num, ("Unknown",))[0]
        status_icon = "+" if status == "success" else "X" if status == "failed" else "o"
        print(f"  [{status_icon}] Phase {phase_num}: {phase_name} — {status}")

    # Check final output
    final_path = DATA_DIR / "final" / "bremen_school_master_table_final.csv"
    if final_path.exists():
        import pandas as pd
        df = pd.read_csv(final_path)
        print(f"\nFinal Output: {len(df)} schools")
        print(f"Location: {final_path}")
    else:
        print("\nNote: Final output file not yet generated")

    print("\n" + "=" * 70)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Bremen School Data Asset Builder Orchestrator"
    )
    parser.add_argument(
        "--phases",
        type=str,
        help="Comma-separated list of phases to run (e.g., '1,2,3')"
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip OpenAI embedding generation in Phase 8"
    )

    args = parser.parse_args()

    # Parse phases
    phases = None
    if args.phases:
        phases = [int(p.strip()) for p in args.phases.split(",")]

    # Run pipeline
    results = run_full_pipeline(
        phases=phases,
        skip_embeddings=args.skip_embeddings
    )

    # Exit with appropriate code
    failed = any(r.get("status") == "failed" for r in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
