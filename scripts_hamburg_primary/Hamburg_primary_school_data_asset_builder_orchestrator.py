#!/usr/bin/env python3
"""
Hamburg Primary School Data Asset Builder Orchestrator

This orchestrator runs the complete Hamburg primary school (Grundschulen) data pipeline:

Phase 1: Download Grundschulen master data from Transparenzportal Hamburg
Phase 2: Collect traffic data from Hamburg SensorThings API
Phase 3: Process crime statistics from PKS (Polizeiliche Kriminalstatistik)
Phase 4: Enrich with HVV transit accessibility data
Phase 5: Enrich with POI data (Google Places API)
Phase 6: Combine all data sources into master table
Phase 7: Generate embeddings and create final parquet file
Phase 8: Transform to Berlin schema

Usage:
    python Hamburg_primary_school_data_asset_builder_orchestrator.py
    python Hamburg_primary_school_data_asset_builder_orchestrator.py --phases 1,2,3,4,5
    python Hamburg_primary_school_data_asset_builder_orchestrator.py --skip-embeddings

Author: Hamburg Primary School Data Pipeline
Created: 2026-04-04
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
LOG_DIR = SCRIPT_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "orchestrator_primary.log"),
    ]
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg_primary"


def run_phase_1():
    """Phase 1: Download and parse Transparenzportal Grundschulen data."""
    logger.info("="*60)
    logger.info("PHASE 1: Grundschulen Master Data from Transparenzportal")
    logger.info("="*60)

    from scrapers.hamburg_grundschulen_scraper import main as scrape_grundschulen
    return scrape_grundschulen()


def run_phase_2():
    """Phase 2: Traffic data enrichment."""
    logger.info("="*60)
    logger.info("PHASE 2: Traffic Data from SensorThings API")
    logger.info("="*60)

    from enrichment.hamburg_primary_traffic_enrichment import main as enrich_traffic
    return enrich_traffic()


def run_phase_3():
    """Phase 3: Crime statistics enrichment."""
    logger.info("="*60)
    logger.info("PHASE 3: Crime Statistics from PKS")
    logger.info("="*60)

    from enrichment.hamburg_primary_crime_enrichment import main as enrich_crime
    return enrich_crime()


def run_phase_4():
    """Phase 4: HVV Transit enrichment."""
    logger.info("="*60)
    logger.info("PHASE 4: HVV Transit Accessibility Enrichment")
    logger.info("="*60)

    from enrichment.hamburg_primary_hvv_transit_enrichment import main as enrich_transit
    return enrich_transit()


def run_phase_5():
    """Phase 5: POI enrichment (Google Places)."""
    logger.info("="*60)
    logger.info("PHASE 5: POI Data from Google Places API")
    logger.info("="*60)

    from enrichment.hamburg_primary_poi_enrichment import main as enrich_poi
    return enrich_poi()


def run_data_combiner():
    """Phase 6: Combine all data sources into master table."""
    logger.info("="*60)
    logger.info("PHASE 6: DATA COMBINER - Creating Combined Master Table")
    logger.info("="*60)

    from processing.hamburg_primary_data_combiner import main as combine_data
    return combine_data()


def run_phase_7(skip_embeddings: bool = False):
    """Phase 7: Generate embeddings and final output."""
    logger.info("="*60)
    logger.info("PHASE 7: Embeddings and Final Output")
    logger.info("="*60)

    if skip_embeddings:
        logger.info("Skipping embedding generation (--skip-embeddings flag)")
        os.environ['SKIP_EMBEDDINGS'] = '1'

    from processing.hamburg_primary_embeddings_generator import main as generate_embeddings
    return generate_embeddings()


def run_phase_8():
    """Phase 8: Transform to Berlin schema."""
    logger.info("="*60)
    logger.info("PHASE 8: Berlin Schema Transform")
    logger.info("="*60)

    from processing.hamburg_primary_to_berlin_schema import transform_hamburg_primary_to_berlin_schema
    return transform_hamburg_primary_to_berlin_schema()


def run_full_pipeline(phases: list = None, skip_embeddings: bool = False):
    """Run the full pipeline or specified phases."""
    start_time = datetime.now()

    logger.info("="*70)
    logger.info("HAMBURG PRIMARY SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start time: {start_time}")
    logger.info("="*70)

    results = {}

    # Define available phases
    available_phases = {
        1: ("Grundschulen Master Data", run_phase_1),
        2: ("Traffic Data (SensorThings API)", run_phase_2),
        3: ("Crime Statistics (PKS)", run_phase_3),
        4: ("Transit Enrichment (HVV)", run_phase_4),
        5: ("POI Enrichment (Google Places)", run_phase_5),
        6: ("Data Combiner", run_data_combiner),
        7: ("Final Embeddings", lambda: run_phase_7(skip_embeddings)),
        8: ("Berlin Schema Transform", run_phase_8),
    }

    # Determine which phases to run
    if phases is None:
        phases_to_run = [1, 4]  # Default: scraper + transit for quick test
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
        else:
            logger.warning(f"Phase {phase_num} not implemented yet")
            results[phase_num] = {"status": "not_implemented"}

    # Always run combiner + embeddings + schema transform if we ran any enrichment phases
    enrichment_phases = [1, 2, 3, 4, 5]
    if any(p in phases_to_run for p in enrichment_phases):
        # Run data combiner if not already in the list
        if 6 not in phases_to_run:
            try:
                logger.info("\nRunning Phase 6: Data Combiner...")
                combiner_result = run_data_combiner()
                results[6] = {"status": "success", "result": combiner_result}
            except Exception as e:
                logger.error(f"Phase 6 (Data Combiner) failed: {e}")
                results[6] = {"status": "failed", "error": str(e)}

        # Run embeddings if not already in the list
        if 7 not in phases_to_run:
            try:
                logger.info("\nRunning Phase 7: Embeddings + Final Output...")
                result = run_phase_7(skip_embeddings)
                results[7] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Phase 7 (Embeddings) failed: {e}")
                results[7] = {"status": "failed", "error": str(e)}

        # Run schema transform if not already in the list
        if 8 not in phases_to_run:
            try:
                logger.info("\nRunning Phase 8: Berlin Schema Transform...")
                result = run_phase_8()
                results[8] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Phase 8 (Schema Transform) failed: {e}")
                results[8] = {"status": "failed", "error": str(e)}

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "="*70)
    print("HAMBURG PRIMARY SCHOOL DATA ASSET BUILDER - COMPLETE")
    print("="*70)
    print(f"\nDuration: {duration}")
    print("\nPhase Results:")

    for phase, result in results.items():
        status = result.get("status", "unknown")
        status_icon = "+" if status == "success" else "X" if status == "failed" else "o"
        print(f"  {status_icon} Phase {phase}: {status}")

    # Check final output
    final_path = DATA_DIR / "final" / "hamburg_primary_school_master_table_final.csv"
    if final_path.exists():
        import pandas as pd_check
        df = pd_check.read_csv(final_path)
        print(f"\nFinal Output: {len(df)} primary schools")
        print(f"Location: {final_path}")
    else:
        print("\nNote: Final output file not found")

    print("\n" + "="*70)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Hamburg Primary School Data Asset Builder Orchestrator"
    )
    parser.add_argument(
        "--phases",
        type=str,
        help="Comma-separated list of phases to run (e.g., '1,2,4')"
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip OpenAI embedding generation"
    )
    parser.add_argument(
        "--phase-7-only",
        action="store_true",
        help="Only run Phase 7 (embeddings/final output)"
    )
    parser.add_argument(
        "--phase-8-only",
        action="store_true",
        help="Only run Phase 8 (Berlin schema transform)"
    )

    args = parser.parse_args()

    # Parse phases
    phases = None
    if args.phases:
        phases = [int(p.strip()) for p in args.phases.split(",")]
    elif args.phase_7_only:
        phases = [7]
    elif args.phase_8_only:
        phases = [8]

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
