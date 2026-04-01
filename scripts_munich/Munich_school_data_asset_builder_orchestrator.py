#!/usr/bin/env python3
"""
Munich School Data Asset Builder Orchestrator

Runs the complete Munich secondary school data pipeline:

Phase 1: Download school master data from Bayern Schulsuche CSV + geocode
Phase 2: Enrich with traffic data (Unfallatlas accidents)
Phase 3: Enrich with transit accessibility (MVV GTFS / Overpass API)
Phase 4: Enrich with crime statistics (PP München Sicherheitsreport)
Phase 5: Enrich with POI data (Google Places API)
Phase 6: Enrich with website metadata & descriptions
Phase 7: Combine all data into master table
Phase 8: Generate embeddings and final output
Phase 9: Enforce Berlin schema

Usage:
    python Munich_school_data_asset_builder_orchestrator.py
    python Munich_school_data_asset_builder_orchestrator.py --phases 1,2,3,4
    python Munich_school_data_asset_builder_orchestrator.py --skip-embeddings
    python Munich_school_data_asset_builder_orchestrator.py --skip-poi

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_munich"


def run_phase_1():
    """Phase 1: School master data from Bayern Schulsuche CSV."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data (Bayern Schulsuche CSV)")
    logger.info("=" * 60)
    from scrapers.munich_school_master_scraper import main as scrape
    return scrape()


def run_phase_2():
    """Phase 2: Traffic enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Data (Unfallatlas)")
    logger.info("=" * 60)
    from enrichment.munich_traffic_enrichment import main as enrich
    return enrich()


def run_phase_3():
    """Phase 3: Transit enrichment (MVV GTFS / Overpass API)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Accessibility (MVV GTFS)")
    logger.info("=" * 60)
    from enrichment.munich_transit_enrichment import main as enrich
    return enrich()


def run_phase_4():
    """Phase 4: Crime enrichment (PP München Sicherheitsreport)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Statistics (Sicherheitsreport)")
    logger.info("=" * 60)
    from enrichment.munich_crime_enrichment import main as enrich
    return enrich()


def run_phase_5():
    """Phase 5: POI enrichment (Google Places)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Data (Google Places API)")
    logger.info("=" * 60)
    from enrichment.munich_poi_enrichment import main as enrich
    return enrich()


def run_phase_6():
    """Phase 6: Website metadata & descriptions."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Website Metadata & Descriptions")
    logger.info("=" * 60)
    from enrichment.munich_website_metadata_enrichment import main as enrich
    return enrich()


def run_phase_7():
    """Phase 7: Data combiner."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Data Combiner")
    logger.info("=" * 60)
    from processing.munich_data_combiner import main as combine
    return combine()


def run_phase_8(skip_embeddings=False):
    """Phase 8: Embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Embeddings and Final Output")
    logger.info("=" * 60)
    import os
    if skip_embeddings:
        os.environ['SKIP_EMBEDDINGS'] = '1'
    from processing.munich_embeddings_generator import main as gen
    return gen()


def run_phase_9():
    """Phase 9: Berlin schema enforcement."""
    logger.info("=" * 60)
    logger.info("PHASE 9: Berlin Schema Enforcement")
    logger.info("=" * 60)
    from munich_to_berlin_schema import main as transform
    return transform()


def run_pipeline(phases=None, skip_embeddings=False, skip_poi=False):
    start = datetime.now()
    logger.info("=" * 70)
    logger.info("MUNICH SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start: {start}")
    logger.info("=" * 70)

    available = {
        1: ("School Master Data (Schulsuche CSV)", run_phase_1),
        2: ("Traffic (Unfallatlas)", run_phase_2),
        3: ("Transit (MVV GTFS)", run_phase_3),
        4: ("Crime (Sicherheitsreport)", run_phase_4),
        5: ("POI (Google Places)", run_phase_5),
        6: ("Website Metadata", run_phase_6),
        7: ("Data Combiner", run_phase_7),
        8: ("Embeddings", lambda: run_phase_8(skip_embeddings)),
        9: ("Berlin Schema", run_phase_9),
    }

    if phases is None:
        phases_to_run = [1, 2, 3, 4]
        if not skip_poi:
            phases_to_run.append(5)
        phases_to_run.extend([6, 7, 8, 9])
    else:
        phases_to_run = phases

    results = {}
    for num in phases_to_run:
        if num in available:
            name, func = available[num]
            try:
                logger.info(f"\nPhase {num}: {name}")
                result = func()
                results[num] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Phase {num} failed: {e}")
                import traceback
                traceback.print_exc()
                results[num] = {"status": "failed", "error": str(e)}
                if num == 1:
                    logger.critical("Phase 1 (School Master Data) failed — cannot continue.")
                    break

    # Summary
    end = datetime.now()
    print(f"\n{'='*70}")
    print("MUNICH SCHOOL DATA ASSET BUILDER - COMPLETE")
    print(f"{'='*70}")
    print(f"Duration: {end - start}")
    print("\nPhase Results:")
    for p, r in results.items():
        icon = "+" if r["status"] == "success" else "x"
        print(f"  {icon} Phase {p}: {r['status']}")

    # Check final output
    fp = DATA_DIR / "final" / "munich_secondary_school_master_table_final.csv"
    if fp.exists():
        import pandas as pd
        df = pd.read_csv(fp)
        print(f"\n  secondary: {len(df)} schools -> {fp.name}")
    print(f"\n{'='*70}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Munich School Data Pipeline")
    parser.add_argument("--phases", type=str, help="Comma-separated phases (e.g., '1,2,3')")
    parser.add_argument("--skip-embeddings", action="store_true")
    parser.add_argument("--skip-poi", action="store_true", help="Skip POI enrichment (requires API key)")
    args = parser.parse_args()

    phases = [int(p.strip()) for p in args.phases.split(",")] if args.phases else None

    results = run_pipeline(phases=phases, skip_embeddings=args.skip_embeddings, skip_poi=args.skip_poi)

    failed = any(r.get("status") == "failed" for r in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
