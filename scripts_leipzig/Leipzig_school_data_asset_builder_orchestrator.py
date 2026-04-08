#!/usr/bin/env python3
"""
Leipzig School Data Asset Builder Orchestrator

This orchestrator runs the complete Leipzig school data pipeline:

Phase 1: Download school master data from Sächsische Schuldatenbank API
Phase 2: Enrich with traffic accident data (Unfallatlas, ULAND=14)
Phase 3: Enrich with transit accessibility (LVB GTFS + Overpass API)
Phase 4: Enrich with crime statistics (Leipzig Open Data API, Ortsteil-level)
Phase 5: Enrich with POI data (Google Places API)
Phase 6: Enrich with website metadata & descriptions (Gemini + Search)
Phase 7: Combine all data sources into master table
Phase 8: Generate embeddings and create final parquet
Phase 9: Enforce Berlin schema

Usage:
    python Leipzig_school_data_asset_builder_orchestrator.py
    python Leipzig_school_data_asset_builder_orchestrator.py --phases 1,2,3,4,5,6,7,8,9
    python Leipzig_school_data_asset_builder_orchestrator.py --skip-embeddings
    python Leipzig_school_data_asset_builder_orchestrator.py --with-descriptions
    python Leipzig_school_data_asset_builder_orchestrator.py --with-tuition

Author: Leipzig School Data Pipeline
Created: 2026-04-08
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
DATA_DIR = PROJECT_ROOT / "data_leipzig"
LOG_DIR = PROJECT_ROOT / "logs"


def run_phase_1():
    """Phase 1: Download school master data from Sächsische Schuldatenbank API."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data from Sächsische Schuldatenbank")
    logger.info("=" * 60)

    from scrapers.leipzig_school_master_scraper import main as scrape_master
    return scrape_master()


def run_phase_2():
    """Phase 2: Traffic accident enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Data from Unfallatlas (ULAND=14)")
    logger.info("=" * 60)

    from enrichment.leipzig_traffic_enrichment import main as enrich_traffic
    return enrich_traffic()


def run_phase_3():
    """Phase 3: Transit accessibility enrichment (LVB GTFS + Overpass)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Accessibility (LVB GTFS)")
    logger.info("=" * 60)

    from enrichment.leipzig_transit_enrichment import main as enrich_transit
    return enrich_transit()


def run_phase_4():
    """Phase 4: Crime statistics enrichment (Leipzig Open Data API)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Statistics (Ortsteil-Level)")
    logger.info("=" * 60)

    from enrichment.leipzig_crime_enrichment import main as enrich_crime
    return enrich_crime()


def run_phase_5():
    """Phase 5: POI enrichment (Google Places API)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Data from Google Places API")
    logger.info("=" * 60)

    from enrichment.leipzig_poi_enrichment import main as enrich_poi
    return enrich_poi()


def run_phase_6():
    """Phase 6: Website metadata & description enrichment."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Website Metadata & Descriptions")
    logger.info("=" * 60)

    from enrichment.leipzig_website_metadata_enrichment import main as enrich_metadata
    return enrich_metadata()


def run_phase_7():
    """Phase 7: Combine all enriched data into master table."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Data Combination")
    logger.info("=" * 60)

    from processing.leipzig_data_combiner import main as combine_data
    return combine_data()


def run_phase_8(skip_embeddings: bool = False):
    """Phase 8: Generate embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Embeddings and Final Output")
    logger.info("=" * 60)

    if skip_embeddings:
        logger.info("Skipping embedding generation (--skip-embeddings flag)")
        os.environ['SKIP_EMBEDDINGS'] = '1'

    from processing.leipzig_embeddings_generator import main as generate_embeddings
    return generate_embeddings()


def run_phase_9():
    """Phase 9: Enforce Berlin schema on Leipzig data."""
    logger.info("=" * 60)
    logger.info("PHASE 9: Berlin Schema Enforcement")
    logger.info("=" * 60)

    from leipzig_to_berlin_schema import main as enforce_schema
    return enforce_schema()


def run_description_pipeline():
    """Run the shared description pipeline for Leipzig schools."""
    logger.info("=" * 60)
    logger.info("DESCRIPTION PIPELINE: Web Research + LLM Descriptions")
    logger.info("=" * 60)

    sys.path.insert(0, str(PROJECT_ROOT / "scripts_shared" / "generation"))
    from school_description_pipeline import main as run_descriptions
    return run_descriptions(city="leipzig")


def run_tuition_pipeline(passes: str = "1"):
    """Run the shared tuition pipeline for Leipzig private schools."""
    logger.info("=" * 60)
    logger.info(f"TUITION PIPELINE: Pass {passes}")
    logger.info("=" * 60)

    sys.path.insert(0, str(PROJECT_ROOT / "scripts_shared" / "generation"))
    from tuition_pipeline import main as run_tuition
    return run_tuition(city="leipzig", passes=passes)


def run_full_pipeline(phases: list = None, skip_embeddings: bool = False,
                      with_descriptions: bool = False, with_tuition: bool = False):
    """Run the full pipeline or specified phases."""
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("LEIPZIG SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 70)

    results = {}

    # Define available phases
    available_phases = {
        1: ("School Master Data (Schuldatenbank API)", run_phase_1),
        2: ("Traffic Enrichment (Unfallatlas)", run_phase_2),
        3: ("Transit Enrichment (LVB GTFS)", run_phase_3),
        4: ("Crime Enrichment (Ortsteil-Level)", run_phase_4),
        5: ("POI Enrichment (Google Places)", run_phase_5),
        6: ("Website Metadata & Descriptions", run_phase_6),
        7: ("Data Combination", run_phase_7),
        8: ("Embeddings & Final Output", lambda: run_phase_8(skip_embeddings)),
        9: ("Berlin Schema Enforcement", run_phase_9),
        10: ("Tuition Pass 1", lambda: run_tuition_pipeline("1")),
        11: ("Tuition Pass 2", lambda: run_tuition_pipeline("2")),
        12: ("Tuition Pass 3", lambda: run_tuition_pipeline("3")),
    }

    # Determine which phases to run
    if phases is None:
        phases_to_run = list(range(1, 10))  # All core phases 1-9
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
                logger.error(f"Phase {phase_num} failed: {e}", exc_info=True)
                results[phase_num] = {"status": "failed", "error": str(e)}
                # Phase 1 is critical — stop pipeline if it fails
                if phase_num == 1:
                    logger.error("Phase 1 (School Master Data) failed — pipeline cannot continue.")
                    break
        else:
            logger.warning(f"Phase {phase_num} not defined")
            results[phase_num] = {"status": "not_implemented"}

    # Optional: run description pipeline
    if with_descriptions:
        try:
            logger.info("\nRunning Description Pipeline...")
            result = run_description_pipeline()
            results['descriptions'] = {"status": "success", "result": result}
            # Re-run embeddings + schema after descriptions
            logger.info("\nRe-running Phase 8 (Embeddings) with enriched descriptions...")
            run_phase_8(skip_embeddings)
            logger.info("\nRe-running Phase 9 (Schema) with enriched descriptions...")
            run_phase_9()
        except Exception as e:
            logger.error(f"Description pipeline failed: {e}", exc_info=True)
            results['descriptions'] = {"status": "failed", "error": str(e)}

    # Optional: run tuition pipeline
    if with_tuition:
        for p in [10, 11, 12]:
            if p not in phases_to_run:
                phase_name, phase_func = available_phases[p]
                try:
                    result = phase_func()
                    results[p] = {"status": "success", "result": result}
                except Exception as e:
                    logger.error(f"Phase {p} failed: {e}")
                    results[p] = {"status": "failed", "error": str(e)}

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 70)
    print("LEIPZIG SCHOOL DATA ASSET BUILDER - COMPLETE")
    print("=" * 70)
    print(f"\nDuration: {duration}")
    print("\nPhase Results:")

    for phase, result in results.items():
        status = result.get("status", "unknown")
        status_icon = "+" if status == "success" else "X" if status == "failed" else "o"
        phase_label = available_phases.get(phase, (str(phase), None))[0] if isinstance(phase, int) else phase
        print(f"  [{status_icon}] Phase {phase}: {phase_label} — {status}")

    # Check final output
    final_path = DATA_DIR / "final" / "leipzig_school_master_table_final.csv"
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
        description="Leipzig School Data Asset Builder Orchestrator"
    )
    parser.add_argument(
        "--phases",
        type=str,
        help="Comma-separated list of phases to run (e.g., '1,2,3,4,5,6,7,8,9')"
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip OpenAI embedding generation in Phase 8"
    )
    parser.add_argument(
        "--with-descriptions",
        action="store_true",
        help="Run the description pipeline after core phases"
    )
    parser.add_argument(
        "--with-tuition",
        action="store_true",
        help="Run tuition pipeline passes 1-3 after core phases"
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
        skip_embeddings=args.skip_embeddings,
        with_descriptions=args.with_descriptions,
        with_tuition=args.with_tuition,
    )

    # Exit with appropriate code
    failed = any(r.get("status") == "failed" for r in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
