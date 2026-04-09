#!/usr/bin/env python3
"""
Dresden School Data Asset Builder Orchestrator

This orchestrator runs the complete Dresden school data pipeline:

Phase 1: Download school master data from Sächsische Schuldatenbank API
Phase 2: Enrich with traffic data from Unfallatlas (accident counts)
Phase 3: Enrich with transit accessibility data (Overpass API)
Phase 4: Enrich with crime statistics (Dresden Open Data, Stadtteil-level)
Phase 5: Enrich with POI data (Google Places API)
Phase 6: Enrich with website metadata & descriptions
Phase 7: Combine all enrichment outputs into master table
Phase 8: Generate embeddings and create final parquet file
Phase 9: Transform to Berlin schema for frontend compatibility

Usage:
    python Dresden_school_data_asset_builder_orchestrator.py
    python Dresden_school_data_asset_builder_orchestrator.py --phases 1,2,3,4,5,6,7
    python Dresden_school_data_asset_builder_orchestrator.py --skip-embeddings

Author: Dresden School Data Pipeline
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
LOG_DIR = PROJECT_ROOT / "logs"


def run_phase_1():
    """Phase 1: Download school master data from Sächsische Schuldatenbank API."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data from Sächsische Schuldatenbank")
    logger.info("=" * 60)

    from scrapers.dresden_school_master_scraper import main as scrape_master
    return scrape_master()


def run_phase_2():
    """Phase 2: Traffic data enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Data from Unfallatlas")
    logger.info("=" * 60)

    from enrichment.dresden_traffic_enrichment import main as enrich_traffic
    return enrich_traffic()


def run_phase_3():
    """Phase 3: Transit accessibility enrichment (Overpass API)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Accessibility (Overpass API)")
    logger.info("=" * 60)

    from enrichment.dresden_transit_enrichment import main as enrich_transit
    return enrich_transit()


def run_phase_4():
    """Phase 4: Crime statistics enrichment (Dresden Open Data)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Statistics (Dresden Open Data Portal)")
    logger.info("=" * 60)

    from enrichment.dresden_crime_enrichment import main as enrich_crime
    return enrich_crime()


def run_phase_5():
    """Phase 5: POI enrichment (Google Places API)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Data from Google Places API")
    logger.info("=" * 60)

    from enrichment.dresden_poi_enrichment import main as enrich_poi
    return enrich_poi()


def run_phase_6():
    """Phase 6: Website metadata & descriptions enrichment."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Website Metadata & Descriptions")
    logger.info("=" * 60)

    from enrichment.dresden_website_metadata_enrichment import main as enrich_web
    return enrich_web()


def run_phase_7():
    """Phase 7: Combine all enrichment outputs into master table."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Data Combination")
    logger.info("=" * 60)

    from processing.dresden_data_combiner import main as combine_data
    return combine_data()


def run_phase_8(skip_embeddings: bool = False):
    """Phase 8: Generate embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Embeddings and Final Output")
    logger.info("=" * 60)

    if skip_embeddings:
        logger.info("Skipping embedding generation (--skip-embeddings flag)")
        os.environ['SKIP_EMBEDDINGS'] = '1'

    from processing.dresden_embeddings_generator import main as generate_embeddings
    return generate_embeddings()


def run_phase_9():
    """Phase 9: Transform to Berlin schema for frontend compatibility."""
    logger.info("=" * 60)
    logger.info("PHASE 9: Berlin Schema Enforcement")
    logger.info("=" * 60)

    # Schema transformer is at scripts_dresden/dresden_to_berlin_schema.py
    parent_dir = SCRIPT_DIR
    sys.path.insert(0, str(parent_dir))
    from dresden_to_berlin_schema import main as transform_schema
    return transform_schema()


def run_full_pipeline(phases: list = None, skip_embeddings: bool = False):
    """Run the full pipeline or specified phases."""
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("DRESDEN SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 70)

    results = {}

    # Define available phases
    available_phases = {
        1: ("School Master Data (Schuldatenbank API)", run_phase_1),
        2: ("Traffic Data (Unfallatlas)", run_phase_2),
        3: ("Transit Enrichment (Overpass API)", run_phase_3),
        4: ("Crime Statistics (Dresden Open Data)", run_phase_4),
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
                    logger.critical("Phase 1 (School Master Data) failed — cannot continue pipeline")
                    break
        else:
            logger.warning(f"Phase {phase_num} not recognized")
            results[phase_num] = {"status": "not_implemented"}

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 70)
    print("DRESDEN SCHOOL DATA ASSET BUILDER - COMPLETE")
    print("=" * 70)
    print(f"\nDuration: {duration}")
    print("\nPhase Results:")

    for phase, result in sorted(results.items()):
        status = result.get("status", "unknown")
        status_icon = "✓" if status == "success" else "✗" if status == "failed" else "○"
        phase_name = available_phases.get(phase, (f"Phase {phase}",))[0]
        print(f"  {status_icon} Phase {phase}: {phase_name} — {status}")

    # Check final output
    final_csv = DATA_DIR / "final" / "dresden_school_master_table_final.csv"
    final_parquet = DATA_DIR / "final" / "dresden_school_master_table_final.parquet"
    if final_csv.exists():
        import pandas as pd
        df = pd.read_csv(final_csv)
        print(f"\nFinal Output: {len(df)} schools")
        print(f"CSV:     {final_csv}")
        if final_parquet.exists():
            print(f"Parquet: {final_parquet}")
    else:
        print("\nNote: Final output file not yet generated")

    print("\n" + "=" * 70)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dresden School Data Asset Builder Orchestrator"
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
