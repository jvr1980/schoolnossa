#!/usr/bin/env python3
"""
Frankfurt School Data Asset Builder Orchestrator

Runs the complete Frankfurt school data pipeline:

Phase 1: Download school master data from Hessen Statistik Verzeichnis 6
Phase 2: Enrich with traffic data (Unfallatlas)
Phase 3: Enrich with transit accessibility (Overpass API)
Phase 4: Enrich with crime statistics (city-level PKS)
Phase 5: Enrich with POI data (Google Places API)
Phase 6: Combine all data into master table
Phase 7: Generate embeddings and final output
Phase 8: Enforce Berlin schema
Phase 9: Description pipeline — web research + LLM descriptions + structured extraction
         (fills empty columns: lehrer, website, schueler by year, sprachen, besonderheiten, etc.)
         Requires Perplexity + OpenAI API keys. Skipped by default; enable with --with-descriptions.
         After running Phase 9, re-run Phase 7 to regenerate embeddings with new descriptions.

Usage:
    python Frankfurt_school_data_asset_builder_orchestrator.py
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 1,2,3,4
    python Frankfurt_school_data_asset_builder_orchestrator.py --skip-embeddings
    python Frankfurt_school_data_asset_builder_orchestrator.py --skip-poi
    python Frankfurt_school_data_asset_builder_orchestrator.py --with-descriptions
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 9
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 9,7,8  # descriptions → re-embed → schema

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data_frankfurt"


def run_phase_1():
    """Phase 1: School master data from Hessen Verzeichnis 6."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data (Hessen Verzeichnis 6)")
    logger.info("=" * 60)
    from scrapers.frankfurt_school_master_scraper import main as scrape
    return scrape()


def run_phase_2():
    """Phase 2: Traffic enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Data (Unfallatlas)")
    logger.info("=" * 60)
    from enrichment.frankfurt_traffic_enrichment import main as enrich
    return enrich()


def run_phase_3():
    """Phase 3: Transit enrichment (Overpass API)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Accessibility (Overpass API)")
    logger.info("=" * 60)
    from enrichment.frankfurt_transit_enrichment import main as enrich
    return enrich()


def run_phase_4():
    """Phase 4: Crime enrichment (city-level PKS)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Statistics (PKS)")
    logger.info("=" * 60)
    from enrichment.frankfurt_crime_enrichment import main as enrich
    return enrich()


def run_phase_5():
    """Phase 5: POI enrichment (Google Places)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Data (Google Places API)")
    logger.info("=" * 60)
    from enrichment.frankfurt_poi_enrichment import main as enrich
    return enrich()


def run_phase_6():
    """Phase 6: Data combiner."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Data Combiner")
    logger.info("=" * 60)
    from processing.frankfurt_data_combiner import main as combine
    return combine()


def run_phase_7(skip_embeddings=False):
    """Phase 7: Embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Embeddings and Final Output")
    logger.info("=" * 60)
    import os
    if skip_embeddings:
        os.environ['SKIP_EMBEDDINGS'] = '1'
    from processing.frankfurt_embeddings_generator import main as gen
    return gen()


def run_phase_8():
    """Phase 8: Berlin schema enforcement."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Berlin Schema Enforcement")
    logger.info("=" * 60)
    from frankfurt_to_berlin_schema import main as transform
    return transform()


def run_phase_9(passes="0,1,2"):
    """Phase 9: Description pipeline (web research + LLM descriptions + structured extraction).

    Runs school_description_pipeline.py for both primary and secondary schools.
    Fills empty columns: lehrer, website, schueler by year, sprachen, besonderheiten,
    nachfrage, migration, and generates rich DE/EN descriptions via web research.

    After this phase, re-run Phase 7 to regenerate embeddings with the new descriptions.
    """
    logger.info("=" * 60)
    logger.info("PHASE 9: Description Pipeline (Web Research + Structured Extraction)")
    logger.info("=" * 60)

    pipeline_script = PROJECT_ROOT / "scripts_shared" / "generation" / "school_description_pipeline.py"
    if not pipeline_script.exists():
        raise FileNotFoundError(f"Description pipeline script not found: {pipeline_script}")

    results = {}
    for school_type in ["primary", "secondary"]:
        logger.info(f"\nRunning description pipeline for {school_type} schools...")
        cmd = [
            sys.executable, str(pipeline_script),
            "--city", "frankfurt",
            "--school-type", school_type,
            "--passes", passes,
        ]
        result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=False)
        if result.returncode != 0:
            raise RuntimeError(f"Description pipeline failed for {school_type} (exit code {result.returncode})")
        results[school_type] = "success"
        logger.info(f"Phase 9 complete for {school_type} schools")

    logger.info("\nNOTE: Re-run Phase 7 (--phases 7) to regenerate embeddings with the new descriptions.")
    return results


def run_pipeline(phases=None, skip_embeddings=False, skip_poi=False, with_descriptions=False,
                 description_passes="0,1,2"):
    start = datetime.now()
    logger.info("=" * 70)
    logger.info("FRANKFURT SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start: {start}")
    logger.info("=" * 70)

    available = {
        1: ("School Master Data",                   run_phase_1),
        2: ("Traffic (Unfallatlas)",                run_phase_2),
        3: ("Transit (Overpass)",                   run_phase_3),
        4: ("Crime (PKS)",                          run_phase_4),
        5: ("POI (Google Places)",                  run_phase_5),
        6: ("Data Combiner",                        run_phase_6),
        7: ("Embeddings",                           lambda: run_phase_7(skip_embeddings)),
        8: ("Berlin Schema",                        run_phase_8),
        9: ("Description Pipeline (Web + LLM)",     lambda: run_phase_9(description_passes)),
    }

    if phases is None:
        phases_to_run = [1, 2, 3, 4]
        if not skip_poi:
            phases_to_run.append(5)
        phases_to_run.extend([6, 7, 8])
        if with_descriptions:
            # Run descriptions after Phase 8, then re-run embeddings + schema
            phases_to_run.extend([9, 7, 8])
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

    # Summary
    end = datetime.now()
    print(f"\n{'='*70}")
    print("FRANKFURT SCHOOL DATA ASSET BUILDER - COMPLETE")
    print(f"{'='*70}")
    print(f"Duration: {end - start}")
    print("\nPhase Results:")
    for p, r in results.items():
        icon = "+" if r["status"] == "success" else "x"
        print(f"  {icon} Phase {p}: {r['status']}")

    # Check final output
    import pandas as pd
    for st in ['secondary', 'primary']:
        fp = DATA_DIR / "final" / f"frankfurt_{st}_school_master_table_final.csv"
        if fp.exists():
            df = pd.read_csv(fp)
            print(f"\n  {st}: {len(df)} schools → {fp.name}")
    print(f"\n{'='*70}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Frankfurt School Data Pipeline")
    parser.add_argument("--phases",               type=str, help="Comma-separated phases (e.g., '1,2,3,9')")
    parser.add_argument("--skip-embeddings",       action="store_true")
    parser.add_argument("--skip-poi",              action="store_true", help="Skip POI enrichment (requires API key)")
    parser.add_argument("--with-descriptions",     action="store_true",
                        help="Run Phase 9 (description pipeline) after Phase 8 — web research + LLM descriptions + structured extraction. Re-runs Phase 7+8 afterwards.")
    parser.add_argument("--description-passes",    type=str, default="0,1,2",
                        help="Which description pipeline passes to run: 0=research, 1=descriptions, 2=structured (default: 0,1,2)")
    args = parser.parse_args()

    phases = [int(p.strip()) for p in args.phases.split(",")] if args.phases else None

    results = run_pipeline(
        phases=phases,
        skip_embeddings=args.skip_embeddings,
        skip_poi=args.skip_poi,
        with_descriptions=args.with_descriptions,
        description_passes=args.description_passes,
    )

    failed = any(r.get("status") == "failed" for r in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
