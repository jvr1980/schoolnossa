#!/usr/bin/env python3
"""
Munich School Data Asset Builder Orchestrator

Runs the complete Munich school data pipeline for primary and/or secondary schools:

Phase 1: Download school master data from jedeschule.codefor.de + geocode
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
    python Munich_school_data_asset_builder_orchestrator.py --school-types primary
    python Munich_school_data_asset_builder_orchestrator.py --school-types primary,secondary
    python Munich_school_data_asset_builder_orchestrator.py --phases 1,2,3,4
    python Munich_school_data_asset_builder_orchestrator.py --skip-embeddings
    python Munich_school_data_asset_builder_orchestrator.py --skip-poi

Author: Munich School Data Pipeline
Created: 2026-04-01
Updated: 2026-04-07 — Added primary school (Grundschule) support
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


def run_phase_1(school_type='secondary'):
    """Phase 1: School master data from jedeschule.codefor.de."""
    logger.info("=" * 60)
    logger.info(f"PHASE 1: School Master Data ({school_type})")
    logger.info("=" * 60)
    from scrapers.munich_school_master_scraper import main as scrape
    return scrape(school_type)


def run_phase_2(school_type='secondary'):
    """Phase 2: Traffic enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info(f"PHASE 2: Traffic Data ({school_type})")
    logger.info("=" * 60)
    from enrichment.munich_traffic_enrichment import main as enrich
    return enrich(school_type)


def run_phase_3(school_type='secondary'):
    """Phase 3: Transit enrichment (MVV GTFS / Overpass API)."""
    logger.info("=" * 60)
    logger.info(f"PHASE 3: Transit Accessibility ({school_type})")
    logger.info("=" * 60)
    from enrichment.munich_transit_enrichment import main as enrich
    return enrich(school_type)


def run_phase_4(school_type='secondary'):
    """Phase 4: Crime enrichment (PP München Sicherheitsreport)."""
    logger.info("=" * 60)
    logger.info(f"PHASE 4: Crime Statistics ({school_type})")
    logger.info("=" * 60)
    from enrichment.munich_crime_enrichment import main as enrich
    return enrich(school_type)


def run_phase_5(school_type='secondary'):
    """Phase 5: POI enrichment (Google Places)."""
    logger.info("=" * 60)
    logger.info(f"PHASE 5: POI Data ({school_type})")
    logger.info("=" * 60)
    from enrichment.munich_poi_enrichment import main as enrich
    return enrich(school_type)


def run_phase_6(school_type='secondary'):
    """Phase 6: Website metadata & descriptions."""
    logger.info("=" * 60)
    logger.info(f"PHASE 6: Website Metadata & Descriptions ({school_type})")
    logger.info("=" * 60)
    from enrichment.munich_website_metadata_enrichment import main as enrich
    return enrich(school_type)


def run_phase_7(school_type='secondary'):
    """Phase 7: Data combiner."""
    logger.info("=" * 60)
    logger.info(f"PHASE 7: Data Combiner ({school_type})")
    logger.info("=" * 60)
    from processing.munich_data_combiner import main as combine
    return combine(school_type)


def run_phase_8(school_type='secondary', skip_embeddings=False):
    """Phase 8: Embeddings and final output."""
    logger.info("=" * 60)
    logger.info(f"PHASE 8: Embeddings and Final Output ({school_type})")
    logger.info("=" * 60)
    import os
    if skip_embeddings:
        os.environ['SKIP_EMBEDDINGS'] = '1'
    from processing.munich_embeddings_generator import main as gen
    return gen(school_type)


def run_phase_9(school_type='secondary'):
    """Phase 9: Berlin schema enforcement."""
    logger.info("=" * 60)
    logger.info(f"PHASE 9: Berlin Schema Enforcement ({school_type})")
    logger.info("=" * 60)
    from munich_to_berlin_schema import main as transform
    return transform(school_type)


def run_pipeline(school_types=None, phases=None, skip_embeddings=False, skip_poi=False):
    start = datetime.now()

    if school_types is None:
        school_types = ['primary', 'secondary']

    logger.info("=" * 70)
    logger.info("MUNICH SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"School types: {', '.join(school_types)}")
    logger.info(f"Start: {start}")
    logger.info("=" * 70)

    all_results = {}

    for school_type in school_types:
        logger.info(f"\n{'#'*70}")
        logger.info(f"# Processing {school_type.upper()} schools")
        logger.info(f"{'#'*70}")

        available = {
            1: ("School Master Data", lambda st=school_type: run_phase_1(st)),
            2: ("Traffic (Unfallatlas)", lambda st=school_type: run_phase_2(st)),
            3: ("Transit (MVV GTFS)", lambda st=school_type: run_phase_3(st)),
            4: ("Crime (Sicherheitsreport)", lambda st=school_type: run_phase_4(st)),
            5: ("POI (Google Places)", lambda st=school_type: run_phase_5(st)),
            6: ("Website Metadata", lambda st=school_type: run_phase_6(st)),
            7: ("Data Combiner", lambda st=school_type: run_phase_7(st)),
            8: ("Embeddings", lambda st=school_type: run_phase_8(st, skip_embeddings)),
            9: ("Berlin Schema", lambda st=school_type: run_phase_9(st)),
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
                    logger.info(f"\nPhase {num}: {name} ({school_type})")
                    result = func()
                    results[num] = {"status": "success", "result": result}
                except Exception as e:
                    logger.error(f"Phase {num} failed for {school_type}: {e}")
                    import traceback
                    traceback.print_exc()
                    results[num] = {"status": "failed", "error": str(e)}
                    if num == 1:
                        logger.critical(f"Phase 1 failed for {school_type} — skipping remaining phases.")
                        break

        all_results[school_type] = results

    # Summary
    end = datetime.now()
    print(f"\n{'='*70}")
    print("MUNICH SCHOOL DATA ASSET BUILDER - COMPLETE")
    print(f"{'='*70}")
    print(f"Duration: {end - start}")

    for school_type, results in all_results.items():
        print(f"\n  {school_type.upper()} Phase Results:")
        for p, r in results.items():
            icon = "+" if r["status"] == "success" else "x"
            print(f"    {icon} Phase {p}: {r['status']}")

        fp = DATA_DIR / "final" / f"munich_{school_type}_school_master_table_final.csv"
        if fp.exists():
            import pandas as pd
            df = pd.read_csv(fp)
            print(f"\n    {school_type}: {len(df)} schools -> {fp.name}")

    print(f"\n{'='*70}")
    return all_results


def main():
    parser = argparse.ArgumentParser(description="Munich School Data Pipeline")
    parser.add_argument("--phases", type=str, help="Comma-separated phases (e.g., '1,2,3')")
    parser.add_argument("--school-types", type=str, default="primary,secondary",
                        help="Comma-separated school types (e.g., 'primary,secondary')")
    parser.add_argument("--skip-embeddings", action="store_true")
    parser.add_argument("--skip-poi", action="store_true", help="Skip POI enrichment (requires API key)")
    args = parser.parse_args()

    phases = [int(p.strip()) for p in args.phases.split(",")] if args.phases else None
    school_types = [s.strip() for s in args.school_types.split(",")]

    results = run_pipeline(school_types=school_types, phases=phases,
                           skip_embeddings=args.skip_embeddings, skip_poi=args.skip_poi)

    failed = any(
        r.get("status") == "failed"
        for st_results in results.values()
        for r in st_results.values()
    )
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
