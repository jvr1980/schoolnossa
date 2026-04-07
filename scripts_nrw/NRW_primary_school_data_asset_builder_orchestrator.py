#!/usr/bin/env python3
"""
NRW Primary School (Grundschule) Data Asset Builder Orchestrator

This orchestrator runs the complete NRW primary school data pipeline
for Cologne and Düsseldorf:

Phase 1: Download school master data from NRW Schulministerium Open Data
          (includes Schulsozialindex merge and coordinate conversion)
          Note: Phase 1 downloads ALL schools; primary/secondary split happens automatically.
Phase 2: Enrich with traffic accident data from Unfallatlas
Phase 3: Enrich with transit accessibility data (Overpass API)
Phase 4: Enrich with crime statistics (PKS NRW)
Phase 5: Enrich with POI data (Google Places API)
Phase 5b (55): Enrich with Anmeldezahlen / application numbers (Düsseldorf only)
Phase 6: Combine all data and generate final master table
Phase 7: Generate embeddings and compute similar schools
Phase 8: Enforce Berlin schema (exact column match for frontend compatibility)

Usage:
    python NRW_primary_school_data_asset_builder_orchestrator.py
    python NRW_primary_school_data_asset_builder_orchestrator.py --phases 2,3,4,5,6,7,8
    python NRW_primary_school_data_asset_builder_orchestrator.py --skip-embeddings

Note: Phase 1 is shared between primary and secondary pipelines.
      If you already ran the secondary pipeline's Phase 1, you can skip it
      here with --phases 2,3,4,5,6,7,8

Author: NRW School Data Pipeline
Created: 2026-02-15
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
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"

SCHOOL_TYPE = "primary"


def run_phase_1():
    """Phase 1: Download and parse NRW school master data (shared with secondary)."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data from NRW Open Data")
    logger.info("=" * 60)

    # Check if primary school data already exists (from secondary pipeline run)
    primary_file = RAW_DIR / "nrw_primary_schools.csv"
    if primary_file.exists():
        import pandas as pd
        df = pd.read_csv(primary_file)
        logger.info(f"Primary school data already exists: {len(df)} schools")
        logger.info("Skipping download (use secondary pipeline Phase 1 to re-download)")
        return df

    from scrapers.nrw_school_master_scraper import main as scrape_master
    return scrape_master()


def run_phase_1b():
    """Phase 1b: Add cross-grade Waldorf schools + missing international schools."""
    logger.info("=" * 60)
    logger.info("PHASE 1b: Cross-Grade & Missing School Additions")
    logger.info("=" * 60)

    from processing.nrw_school_additions import add_schools
    return add_schools('both')


def run_phase_2():
    """Phase 2: Traffic accident enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Accident Data (Unfallatlas)")
    logger.info("=" * 60)

    from enrichment.nrw_traffic_enrichment import enrich_schools
    return enrich_schools(SCHOOL_TYPE)


def run_phase_3():
    """Phase 3: Transit accessibility enrichment."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Accessibility (Overpass API)")
    logger.info("=" * 60)

    from enrichment.nrw_transit_enrichment import enrich_schools
    return enrich_schools(SCHOOL_TYPE)


def run_phase_4():
    """Phase 4: Crime statistics enrichment."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Statistics (PKS NRW)")
    logger.info("=" * 60)

    from enrichment.nrw_crime_enrichment import enrich_schools
    return enrich_schools(SCHOOL_TYPE)


def run_phase_5():
    """Phase 5: POI enrichment (Google Places)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Data (Google Places API)")
    logger.info("=" * 60)

    from enrichment.nrw_poi_enrichment import enrich_schools
    return enrich_schools(SCHOOL_TYPE)


def run_phase_5b():
    """Phase 5b: Anmeldezahlen enrichment (Düsseldorf only)."""
    logger.info("=" * 60)
    logger.info("PHASE 5b: Anmeldezahlen / Application Numbers (Düsseldorf)")
    logger.info("=" * 60)

    from enrichment.nrw_anmeldezahlen_enrichment import enrich_schools
    return enrich_schools(SCHOOL_TYPE)


def run_phase_5c():
    """Phase 5c: Website metadata + description enrichment (Gemini grounding)."""
    logger.info("=" * 60)
    logger.info("PHASE 5c: Website Metadata & Descriptions (Gemini URL Context + Grounding)")
    logger.info("=" * 60)

    from enrichment.nrw_website_metadata_enrichment import enrich_schools
    return enrich_schools(SCHOOL_TYPE)


def run_phase_6():
    """Phase 6: Combine all data."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Data Combination")
    logger.info("=" * 60)

    from processing.nrw_data_combiner import combine_school_type
    return combine_school_type(SCHOOL_TYPE)


def run_phase_7(skip_embeddings: bool = False):
    """Phase 7: Generate embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Embeddings and Final Output")
    logger.info("=" * 60)

    if skip_embeddings:
        logger.info("Skipping embedding generation (--skip-embeddings flag)")
        os.environ['SKIP_EMBEDDINGS'] = '1'

    from processing.nrw_embeddings_generator import process_school_type
    return process_school_type(SCHOOL_TYPE)


def run_phase_8():
    """Phase 8: Enforce Berlin schema on final output."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Enforce Berlin Schema")
    logger.info("=" * 60)

    from nrw_to_berlin_schema import transform_to_berlin_schema
    return transform_to_berlin_schema(SCHOOL_TYPE)


def run_full_pipeline(phases: list = None, skip_embeddings: bool = False):
    """Run the full pipeline or specified phases."""
    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("NRW PRIMARY SCHOOL (GRUNDSCHULE) DATA ASSET BUILDER - STARTING")
    logger.info(f"Target: Cologne & Düsseldorf Grundschulen")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 70)

    results = {}

    available_phases = {
        1: ("School Master Data", run_phase_1),
        15: ("Cross-Grade & Missing Schools", run_phase_1b),
        2: ("Traffic Accidents (Unfallatlas)", run_phase_2),
        3: ("Transit Accessibility", run_phase_3),
        4: ("Crime Statistics (PKS)", run_phase_4),
        5: ("POI Enrichment (Google Places)", run_phase_5),
        55: ("Anmeldezahlen (Düsseldorf)", run_phase_5b),
        56: ("Website Metadata & Descriptions (Gemini)", run_phase_5c),
        6: ("Data Combination", run_phase_6),
        7: ("Embeddings & Final Output", lambda: run_phase_7(skip_embeddings)),
        8: ("Berlin Schema Enforcement", run_phase_8),
    }

    if phases is None:
        phases_to_run = list(available_phases.keys())
    else:
        phases_to_run = phases

    for phase_num in phases_to_run:
        if phase_num in available_phases:
            phase_name, phase_func = available_phases[phase_num]
            try:
                logger.info(f"\nRunning Phase {phase_num}: {phase_name}")
                result = phase_func()
                results[phase_num] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Phase {phase_num} failed: {e}")
                import traceback
                traceback.print_exc()
                results[phase_num] = {"status": "failed", "error": str(e)}

                if phase_num == 1:
                    logger.error("Phase 1 failed - cannot continue pipeline")
                    break

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 70)
    print("NRW PRIMARY SCHOOL DATA ASSET BUILDER - COMPLETE")
    print("=" * 70)
    print(f"\nDuration: {duration}")
    print("\nPhase Results:")

    for phase, result in results.items():
        status = result.get("status", "unknown")
        icon = "✓" if status == "success" else "✗" if status == "failed" else "○"
        phase_name = available_phases.get(phase, ("Unknown",))[0]
        print(f"  {icon} Phase {phase} ({phase_name}): {status}")
        if status == "failed":
            print(f"    Error: {result.get('error', 'unknown')}")

    # Check final output
    final_path = DATA_DIR / "final" / f"nrw_{SCHOOL_TYPE}_school_master_table_final.csv"
    if final_path.exists():
        import pandas as pd
        df = pd.read_csv(final_path)
        print(f"\nFinal Output: {len(df)} Grundschulen, {len(df.columns)} columns")
        print(f"Location: {final_path}")

    print("\n" + "=" * 70)
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="NRW Primary School (Grundschule) Data Asset Builder Orchestrator"
    )
    parser.add_argument(
        "--phases", type=str,
        help="Comma-separated list of phases to run (e.g., '2,3,4,5,6,7')"
    )
    parser.add_argument(
        "--skip-embeddings", action="store_true",
        help="Skip OpenAI embedding generation"
    )

    args = parser.parse_args()

    phases = None
    if args.phases:
        phases = [int(p.strip()) for p in args.phases.split(",")]

    results = run_full_pipeline(phases=phases, skip_embeddings=args.skip_embeddings)

    failed = any(r.get("status") == "failed" for r in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
