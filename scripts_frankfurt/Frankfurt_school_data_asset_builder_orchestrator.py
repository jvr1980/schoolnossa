#!/usr/bin/env python3
"""
Frankfurt School Data Asset Builder Orchestrator

Runs the complete Frankfurt school data pipeline:

Phase 1:  Download school master data from Hessen Statistik Verzeichnis 6
Phase 2:  Schulwegweiser portal scraper — official websites, contacts, profiles, Ganztagsform
          (frankfurt.de/schulwegweiser; uses Playwright; ~15 min first run, cached thereafter)
Phase 3:  Enrich with traffic data (Unfallatlas)
Phase 4:  Enrich with transit accessibility (Overpass API)
Phase 5:  Enrich with crime statistics (city-level PKS)
Phase 6:  Enrich with POI data (Google Places API)
Phase 7:  Combine all data into master table
Phase 8:  Generate embeddings and final output
Phase 9:  Enforce Berlin schema
Phase 10: Description pipeline — web research + LLM descriptions + structured extraction
          (fills empty columns: lehrer, website, schueler by year, sprachen, besonderheiten, etc.)
          Requires Perplexity + OpenAI API keys. Skipped by default; enable with --with-descriptions.
          After running Phase 10, re-run Phase 8 to regenerate embeddings with new descriptions.

Usage:
    python Frankfurt_school_data_asset_builder_orchestrator.py
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 1,2,3,4,5
    python Frankfurt_school_data_asset_builder_orchestrator.py --skip-embeddings
    python Frankfurt_school_data_asset_builder_orchestrator.py --skip-poi
    python Frankfurt_school_data_asset_builder_orchestrator.py --with-descriptions
    python Frankfurt_school_data_asset_builder_orchestrator.py --with-tuition
    python Frankfurt_school_data_asset_builder_orchestrator.py --with-descriptions --with-tuition
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 10
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 10,8,9  # descriptions → re-embed → schema
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 11,12,13  # all tuition passes
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 13        # re-run tuition Pass 3 only
    python Frankfurt_school_data_asset_builder_orchestrator.py --phases 2         # re-run Schulwegweiser only

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


def run_phase_2(force_rescrape=False):
    """Phase 2: Schulwegweiser portal scraper.

    Scrapes frankfurt.de/schulwegweiser for official school websites, contact info,
    Schulprofile, Ganztagsform, and other attributes not in Verzeichnis 6.
    Uses Playwright (headless Chromium). First run ~15 min; cached thereafter.
    Output: data_frankfurt/intermediate/frankfurt_{type}_schools_with_schulwegweiser.csv
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: Schulwegweiser Portal Scraper")
    logger.info("=" * 60)
    from scrapers.frankfurt_schulwegweiser_scraper import main as scrape
    return scrape(force_rescrape=force_rescrape)


def run_phase_3():
    """Phase 3: Traffic enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Traffic Data (Unfallatlas)")
    logger.info("=" * 60)
    from enrichment.frankfurt_traffic_enrichment import main as enrich
    return enrich()


def run_phase_4():
    """Phase 4: Transit enrichment (Overpass API)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Transit Accessibility (Overpass API)")
    logger.info("=" * 60)
    from enrichment.frankfurt_transit_enrichment import main as enrich
    return enrich()


def run_phase_5():
    """Phase 5: Crime enrichment (city-level PKS)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: Crime Statistics (PKS)")
    logger.info("=" * 60)
    from enrichment.frankfurt_crime_enrichment import main as enrich
    return enrich()


def run_phase_6():
    """Phase 6: POI enrichment (Google Places)."""
    logger.info("=" * 60)
    logger.info("PHASE 6: POI Data (Google Places API)")
    logger.info("=" * 60)
    from enrichment.frankfurt_poi_enrichment import main as enrich
    return enrich()


def run_phase_7():
    """Phase 7: Data combiner."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Data Combiner")
    logger.info("=" * 60)
    from processing.frankfurt_data_combiner import main as combine
    return combine()


def run_phase_8(skip_embeddings=False):
    """Phase 8: Embeddings and final output."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Embeddings and Final Output")
    logger.info("=" * 60)
    import os
    if skip_embeddings:
        os.environ['SKIP_EMBEDDINGS'] = '1'
    from processing.frankfurt_embeddings_generator import main as gen
    return gen()


def run_phase_9():
    """Phase 9: Berlin schema enforcement."""
    logger.info("=" * 60)
    logger.info("PHASE 9: Berlin Schema Enforcement")
    logger.info("=" * 60)
    from frankfurt_to_berlin_schema import main as transform
    return transform()


def run_phase_10(passes="0,1,2"):
    """Phase 10: Description pipeline (web research + LLM descriptions + structured extraction).

    Runs school_description_pipeline.py for both primary and secondary schools.
    Fills empty columns: lehrer, website, schueler by year, sprachen, besonderheiten,
    nachfrage, migration, and generates rich DE/EN descriptions via web research.

    After this phase, re-run Phase 8 to regenerate embeddings with the new descriptions.
    """
    logger.info("=" * 60)
    logger.info("PHASE 10: Description Pipeline (Web Research + Structured Extraction)")
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
        logger.info(f"Phase 10 complete for {school_type} schools")

    logger.info("\nNOTE: Re-run Phase 8 (--phases 8) to regenerate embeddings with the new descriptions.")
    return results


def run_phase_11():
    """Phase 11: Tuition fee pipeline — tier classification (Pass 1).

    Classifies private schools into tuition tiers (low/medium/high/premium/ultra)
    and estimates a single monthly fee, using Gemini + Google Search grounding.
    Only processes schools where traegerschaft contains 'privat' or 'frei'.
    """
    logger.info("=" * 60)
    logger.info("PHASE 11: Tuition Tier Classification (Pass 1)")
    logger.info("=" * 60)
    return _run_tuition_pipeline(passes="1")


def run_phase_12():
    """Phase 12: Tuition income matrix generation (Pass 2).

    Generates a 12-bracket income matrix and sibling discount percentages
    for each private school that has a tier from Phase 11.
    Uses Gemini + Google Search grounding.
    """
    logger.info("=" * 60)
    logger.info("PHASE 12: Tuition Income Matrix (Pass 2)")
    logger.info("=" * 60)
    return _run_tuition_pipeline(passes="2")


def run_phase_13():
    """Phase 13: Tuition verification (Pass 3).

    Re-researches private schools whose Pass 2 matrix is flat (all 12 brackets identical)
    using GPT-5.2 via the OpenAI Responses API with web_search + function calling.
    Sets income_based_tuition=False for confirmed flat-fee schools so they are
    excluded from future Pass 3 runs.
    Requires OpenAI API key.
    """
    logger.info("=" * 60)
    logger.info("PHASE 13: Tuition Verification (Pass 3 — GPT-5.2)")
    logger.info("=" * 60)
    return _run_tuition_pipeline(passes="3")


def _run_tuition_pipeline(passes="1,2,3"):
    """Internal helper: runs tuition_pipeline.py for both school types."""
    pipeline_script = PROJECT_ROOT / "scripts_shared" / "generation" / "tuition_pipeline.py"
    if not pipeline_script.exists():
        raise FileNotFoundError(f"Tuition pipeline script not found: {pipeline_script}")

    results = {}
    for school_type in ["primary", "secondary"]:
        logger.info(f"\nRunning tuition pipeline (passes={passes}) for {school_type} schools...")
        cmd = [
            sys.executable, str(pipeline_script),
            "--city", "frankfurt",
            "--school-type", school_type,
            "--passes", passes,
        ]
        result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=False)
        if result.returncode != 0:
            raise RuntimeError(f"Tuition pipeline failed for {school_type} (exit code {result.returncode})")
        results[school_type] = "success"
    return results


def run_pipeline(phases=None, skip_embeddings=False, skip_poi=False, with_descriptions=False,
                 description_passes="0,1,2", with_tuition=False):
    start = datetime.now()
    logger.info("=" * 70)
    logger.info("FRANKFURT SCHOOL DATA ASSET BUILDER - STARTING")
    logger.info(f"Start: {start}")
    logger.info("=" * 70)

    available = {
        1:  ("School Master Data",                    run_phase_1),
        2:  ("Schulwegweiser Portal Scraper",         lambda: run_phase_2()),
        3:  ("Traffic (Unfallatlas)",                 run_phase_3),
        4:  ("Transit (Overpass)",                    run_phase_4),
        5:  ("Crime (PKS)",                           run_phase_5),
        6:  ("POI (Google Places)",                   run_phase_6),
        7:  ("Data Combiner",                         run_phase_7),
        8:  ("Embeddings",                            lambda: run_phase_8(skip_embeddings)),
        9:  ("Berlin Schema",                         run_phase_9),
        10: ("Description Pipeline (Web + LLM)",      lambda: run_phase_10(description_passes)),
        11: ("Tuition Tier Classification (Pass 1)",  run_phase_11),
        12: ("Tuition Income Matrix (Pass 2)",        run_phase_12),
        13: ("Tuition Verification (Pass 3)",         run_phase_13),
    }

    if phases is None:
        phases_to_run = [1, 2, 3, 4, 5]
        if not skip_poi:
            phases_to_run.append(6)
        phases_to_run.extend([7, 8, 9])
        if with_descriptions:
            # Descriptions → re-embed → re-schema
            phases_to_run.extend([10, 8, 9])
        if with_tuition:
            # All 3 tuition passes after Berlin schema
            phases_to_run.extend([11, 12, 13])
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
                        help="Run Phase 10 (description pipeline) after Phase 9. Re-runs Phase 8+9 afterwards.")
    parser.add_argument("--description-passes",    type=str, default="0,1,2",
                        help="Which description pipeline passes to run: 0=research, 1=descriptions, 2=structured (default: 0,1,2)")
    parser.add_argument("--with-tuition",          action="store_true",
                        help="Run Phases 10-12 (tuition pipeline) for private schools — tier classification, income matrix, and verification. Requires Gemini + OpenAI API keys.")
    args = parser.parse_args()

    phases = [int(p.strip()) for p in args.phases.split(",")] if args.phases else None

    results = run_pipeline(
        phases=phases,
        skip_embeddings=args.skip_embeddings,
        skip_poi=args.skip_poi,
        with_descriptions=args.with_descriptions,
        description_passes=args.description_passes,
        with_tuition=args.with_tuition,
    )

    failed = any(r.get("status") == "failed" for r in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
