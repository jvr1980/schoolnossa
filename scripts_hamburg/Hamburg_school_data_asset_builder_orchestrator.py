#!/usr/bin/env python3
"""
Hamburg School Data Asset Builder Orchestrator

This orchestrator runs the complete Hamburg secondary school data pipeline:

Phase 1: Download school master data from Transparenzportal Hamburg
Phase 2: Scrape Abitur statistics from gymnasium-hamburg.net
Phase 3: Scrape teacher counts from school websites (Playwright)
Phase 4: Collect traffic data from Hamburg SensorThings API
Phase 5: Process crime statistics from PKS (Polizeiliche Kriminalstatistik)
Phase 6: Enrich with HVV transit accessibility data
Phase 7: Enrich with POI data (Google Places API)
Phase 8: Generate embeddings and create final parquet file

Usage:
    python Hamburg_school_data_asset_builder_orchestrator.py
    python Hamburg_school_data_asset_builder_orchestrator.py --phases 1,2,3,4,5,6,7
    python Hamburg_school_data_asset_builder_orchestrator.py --skip-embeddings

Author: Hamburg School Data Pipeline
Created: 2026-02-01
Updated: 2026-02-03 - Added phases 3, 4, 5, 7
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg"
LOG_DIR = PROJECT_ROOT / "logs"


def run_phase_1():
    """Phase 1: Download and parse Transparenzportal school data."""
    logger.info("="*60)
    logger.info("PHASE 1: School Master Data from Transparenzportal")
    logger.info("="*60)

    from scrapers.hamburg_school_master_scraper import main as scrape_master
    return scrape_master()


def run_phase_2():
    """Phase 2: Scrape Abitur statistics."""
    logger.info("="*60)
    logger.info("PHASE 2: Abitur Statistics from gymnasium-hamburg.net")
    logger.info("="*60)

    from scrapers.hamburg_abitur_scraper import main as scrape_abitur
    return scrape_abitur()


def run_phase_3():
    """Phase 3: Scrape teacher data from school websites."""
    logger.info("="*60)
    logger.info("PHASE 3: Teacher Data from School Websites")
    logger.info("="*60)

    from scrapers.hamburg_website_scraper import main as scrape_websites
    return scrape_websites()


def run_phase_4():
    """Phase 4: Traffic data enrichment."""
    logger.info("="*60)
    logger.info("PHASE 4: Traffic Data from SensorThings API")
    logger.info("="*60)

    from enrichment.hamburg_traffic_enrichment import main as enrich_traffic
    return enrich_traffic()


def run_phase_5():
    """Phase 5: Crime statistics enrichment."""
    logger.info("="*60)
    logger.info("PHASE 5: Crime Statistics from PKS")
    logger.info("="*60)

    from enrichment.hamburg_crime_enrichment import main as enrich_crime
    return enrich_crime()


def run_phase_6():
    """Phase 6: HVV Transit enrichment."""
    logger.info("="*60)
    logger.info("PHASE 6: HVV Transit Accessibility Enrichment")
    logger.info("="*60)

    from enrichment.hamburg_hvv_transit_enrichment import main as enrich_transit
    return enrich_transit()


def run_phase_7():
    """Phase 7: POI enrichment (Google Places)."""
    logger.info("="*60)
    logger.info("PHASE 7: POI Data from Google Places API")
    logger.info("="*60)

    from enrichment.hamburg_poi_enrichment import main as enrich_poi
    return enrich_poi()


def run_data_combiner():
    """Combine all data sources into master table."""
    logger.info("="*60)
    logger.info("DATA COMBINER: Creating Combined Master Table")
    logger.info("="*60)

    from processing.hamburg_data_combiner import main as combine_data
    return combine_data()


def run_phase_8(skip_embeddings: bool = False):
    """Phase 8: Generate embeddings and final output."""
    logger.info("="*60)
    logger.info("PHASE 8: Embeddings and Final Output")
    logger.info("="*60)

    if skip_embeddings:
        logger.info("Skipping embedding generation (--skip-embeddings flag)")
        os.environ['SKIP_EMBEDDINGS'] = '1'

    from processing.hamburg_embeddings_generator import main as generate_embeddings
    return generate_embeddings()


def run_full_pipeline(phases: list = None, skip_embeddings: bool = False):
    """Run the full pipeline or specified phases."""
    start_time = datetime.now()

    logger.info("="*70)
    logger.info("HAMBURG SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start time: {start_time}")
    logger.info("="*70)

    results = {}

    # Define available phases
    available_phases = {
        1: ("School Master Data", run_phase_1),
        2: ("Abitur Statistics", run_phase_2),
        3: ("Teacher Data (Website Scraping)", run_phase_3),
        4: ("Traffic Data (SensorThings API)", run_phase_4),
        5: ("Crime Statistics (PKS)", run_phase_5),
        6: ("Transit Enrichment (HVV)", run_phase_6),
        7: ("POI Enrichment (Google Places)", run_phase_7),
        8: ("Final Embeddings", lambda: run_phase_8(skip_embeddings)),
    }

    # Determine which phases to run
    if phases is None:
        phases_to_run = [1, 2, 6]  # Default core phases (quick run)
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

    # Always run data combiner and phase 8 if we ran any collection/enrichment phases
    collection_phases = [1, 2, 3, 4, 5, 6, 7]
    if any(p in phases_to_run for p in collection_phases):
        try:
            logger.info("\nRunning Data Combiner...")
            combiner_result = run_data_combiner()
            results['combiner'] = {"status": "success", "result": combiner_result}
        except Exception as e:
            logger.error(f"Data Combiner failed: {e}")
            results['combiner'] = {"status": "failed", "error": str(e)}

        # Run phase 8 if not already in the list
        if 8 not in phases_to_run:
            try:
                logger.info("\nRunning Phase 8: Final Output...")
                result = run_phase_8(skip_embeddings)
                results[8] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Phase 8 failed: {e}")
                results[8] = {"status": "failed", "error": str(e)}

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "="*70)
    print("HAMBURG SCHOOL DATA ASSET BUILDER - COMPLETE")
    print("="*70)
    print(f"\nDuration: {duration}")
    print("\nPhase Results:")

    for phase, result in results.items():
        status = result.get("status", "unknown")
        status_icon = "✓" if status == "success" else "✗" if status == "failed" else "○"
        print(f"  {status_icon} Phase {phase}: {status}")

    # Check final output
    final_path = DATA_DIR / "final" / "hamburg_school_master_table_final.csv"
    if final_path.exists():
        import pandas as pd
        df = pd.read_csv(final_path)
        print(f"\nFinal Output: {len(df)} schools")
        print(f"Location: {final_path}")
    else:
        print("\nNote: Final output file not found")

    print("\n" + "="*70)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Hamburg School Data Asset Builder Orchestrator"
    )
    parser.add_argument(
        "--phases",
        type=str,
        help="Comma-separated list of phases to run (e.g., '1,2,6')"
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip OpenAI embedding generation"
    )
    parser.add_argument(
        "--phase-8-only",
        action="store_true",
        help="Only run Phase 8 (embeddings/final output)"
    )

    args = parser.parse_args()

    # Parse phases
    phases = None
    if args.phases:
        phases = [int(p.strip()) for p in args.phases.split(",")]
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
