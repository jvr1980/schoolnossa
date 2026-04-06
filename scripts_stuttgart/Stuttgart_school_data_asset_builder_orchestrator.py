#!/usr/bin/env python3
"""
Stuttgart School Data Pipeline Orchestrator
Coordinates all phases of the Stuttgart school data pipeline.

Phases:
1. School Master Data (jedeschule scraper)
2. Traffic Enrichment (Unfallatlas)
3. Transit Enrichment (Overpass API)
4. Crime Enrichment (PKS Stuttgart)
5. POI Enrichment (Google Places API) — requires GOOGLE_PLACES_API_KEY
6. Data Combiner
7. Embeddings (optional)
8. Berlin Schema Enforcement

Usage:
    python Stuttgart_school_data_asset_builder_orchestrator.py
    python Stuttgart_school_data_asset_builder_orchestrator.py --phases 1,2,3
    python Stuttgart_school_data_asset_builder_orchestrator.py --skip-poi --skip-embeddings

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import sys
import os
import subprocess
import logging
import argparse
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent


def run_phase_1():
    """Phase 1: School Master Data (jedeschule scraper)."""
    logger.info("=" * 60)
    logger.info("PHASE 1: School Master Data (jedeschule)")
    logger.info("=" * 60)
    from scrapers.stuttgart_school_scraper import main as scrape_main
    return scrape_main()


def run_phase_2():
    """Phase 2: Traffic Enrichment (Unfallatlas)."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Traffic Enrichment (Unfallatlas)")
    logger.info("=" * 60)
    from enrichment.stuttgart_traffic_enrichment import main as traffic_main
    traffic_main()


def run_phase_3():
    """Phase 3: Transit Enrichment (Overpass API)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Transit Enrichment (Overpass API)")
    logger.info("=" * 60)
    from enrichment.stuttgart_transit_enrichment import main as transit_main
    transit_main()


def run_phase_4():
    """Phase 4: Crime Enrichment (PKS Stuttgart)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Crime Enrichment (PKS Stuttgart)")
    logger.info("=" * 60)
    from enrichment.stuttgart_crime_enrichment import main as crime_main
    crime_main()


def run_phase_5():
    """Phase 5: POI Enrichment (Google Places API)."""
    logger.info("=" * 60)
    logger.info("PHASE 5: POI Enrichment (Google Places API)")
    logger.info("=" * 60)
    from enrichment.stuttgart_poi_enrichment import main as poi_main
    poi_main()


def run_phase_6():
    """Phase 6: Data Combiner."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Data Combiner")
    logger.info("=" * 60)
    from processing.stuttgart_data_combiner import main as combiner_main
    combiner_main()


def run_phase_7():
    """Phase 7: Embeddings Generator (Gemini)."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Embeddings (Gemini gemini-embedding-001)")
    logger.info("=" * 60)
    from processing.stuttgart_embeddings_generator import main as embeddings_main
    embeddings_main()


def run_phase_8():
    """Phase 8: Berlin Schema Enforcement."""
    logger.info("=" * 60)
    logger.info("PHASE 8: Berlin Schema Enforcement")
    logger.info("=" * 60)
    from stuttgart_to_berlin_schema import main as schema_main
    schema_main()


def run_phase_10(passes="0,1,2"):
    """Phase 10: Description Pipeline (web research + LLM descriptions + structured extraction)."""
    logger.info("=" * 60)
    logger.info("PHASE 10: Description Pipeline (Web Research + Structured Extraction)")
    logger.info("=" * 60)

    pipeline_script = PROJECT_ROOT / "scripts_shared" / "generation" / "school_description_pipeline.py"
    if not pipeline_script.exists():
        raise FileNotFoundError(f"Description pipeline script not found: {pipeline_script}")

    for school_type in ["primary", "secondary"]:
        logger.info(f"\nRunning description pipeline for {school_type} schools...")
        cmd = [
            sys.executable, str(pipeline_script),
            "--city", "stuttgart",
            "--school-type", school_type,
            "--passes", passes,
        ]
        result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=False)
        if result.returncode != 0:
            raise RuntimeError(f"Description pipeline failed for {school_type} (exit code {result.returncode})")
        logger.info(f"Phase 10 complete for {school_type} schools")

    logger.info("\nNOTE: Re-run Phase 7 (embeddings) after this to regenerate with new descriptions.")


def run_phase_11():
    """Phase 11: Tuition tier classification (Pass 1 — Gemini + Google Search)."""
    logger.info("=" * 60)
    logger.info("PHASE 11: Tuition Tier Classification (Pass 1)")
    logger.info("=" * 60)
    return _run_tuition_pipeline(passes="1")


def run_phase_12():
    """Phase 12: Tuition income matrix (Pass 2 — Gemini + Google Search)."""
    logger.info("=" * 60)
    logger.info("PHASE 12: Tuition Income Matrix (Pass 2)")
    logger.info("=" * 60)
    return _run_tuition_pipeline(passes="2")


def run_phase_13():
    """Phase 13: Tuition verification (Pass 3 — GPT-5.2 Responses API)."""
    logger.info("=" * 60)
    logger.info("PHASE 13: Tuition Verification (Pass 3)")
    logger.info("=" * 60)
    return _run_tuition_pipeline(passes="3")


def _run_tuition_pipeline(passes="1,2,3"):
    """Internal helper: runs tuition_pipeline.py for both school types."""
    pipeline_script = PROJECT_ROOT / "scripts_shared" / "generation" / "tuition_pipeline.py"
    if not pipeline_script.exists():
        raise FileNotFoundError(f"Tuition pipeline script not found: {pipeline_script}")

    for school_type in ["primary", "secondary"]:
        logger.info(f"\nRunning tuition pipeline (passes={passes}) for {school_type} schools...")
        cmd = [
            sys.executable, str(pipeline_script),
            "--city", "stuttgart",
            "--school-type", school_type,
            "--passes", passes,
        ]
        result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=False)
        if result.returncode != 0:
            raise RuntimeError(f"Tuition pipeline failed for {school_type} (exit code {result.returncode})")


def main():
    parser = argparse.ArgumentParser(description="Stuttgart School Data Pipeline")
    parser.add_argument('--phases', type=str, default=None,
                        help='Comma-separated phase numbers to run (e.g., 1,2,3)')
    parser.add_argument('--skip-poi', action='store_true',
                        help='Skip Google Places POI enrichment')
    parser.add_argument('--skip-embeddings', action='store_true',
                        help='Skip embedding generation')
    parser.add_argument('--with-descriptions', action='store_true',
                        help='Run Phase 10 (description pipeline)')
    parser.add_argument('--description-passes', type=str, default="0,1,2",
                        help='Which description passes to run (default: 0,1,2)')
    parser.add_argument('--with-tuition', action='store_true',
                        help='Run Phases 11-13 (tuition pipeline)')
    args = parser.parse_args()

    # Ensure we can import from scripts_stuttgart
    sys.path.insert(0, str(SCRIPT_DIR))

    available_phases = {
        1:  ("School Master Data", run_phase_1),
        2:  ("Traffic (Unfallatlas)", run_phase_2),
        3:  ("Transit (Overpass)", run_phase_3),
        4:  ("Crime (PKS)", run_phase_4),
        5:  ("POI (Google Places)", run_phase_5),
        6:  ("Data Combiner", run_phase_6),
        7:  ("Embeddings (Gemini)", run_phase_7),
        8:  ("Berlin Schema", run_phase_8),
        10: ("Descriptions (Web+LLM)", lambda: run_phase_10(args.description_passes)),
        11: ("Tuition Tier (Pass 1)", run_phase_11),
        12: ("Tuition Matrix (Pass 2)", run_phase_12),
        13: ("Tuition Verify (Pass 3)", run_phase_13),
    }

    if args.phases:
        phases_to_run = [int(p.strip()) for p in args.phases.split(',')]
    else:
        phases_to_run = [1, 2, 3, 4, 5, 6, 7, 8]
        if args.skip_poi:
            phases_to_run.remove(5)
        if args.skip_embeddings:
            phases_to_run.remove(7)
        if args.with_descriptions:
            phases_to_run.extend([10, 7, 8])  # descriptions → re-embed → re-schema
        if args.with_tuition:
            phases_to_run.extend([11, 12, 13])

    print("\n" + "=" * 70)
    print("STUTTGART SCHOOL DATA PIPELINE")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Phases: {phases_to_run}")
    print("=" * 70)

    results = {}

    for phase_num in phases_to_run:
        if phase_num not in available_phases:
            logger.warning(f"Unknown phase {phase_num}")
            continue

        phase_name, phase_fn = available_phases[phase_num]
        logger.info(f"\n>>> Starting Phase {phase_num}: {phase_name}")

        try:
            phase_fn()
            results[phase_num] = {"name": phase_name, "status": "success"}
            logger.info(f"<<< Phase {phase_num} complete")
        except Exception as e:
            results[phase_num] = {"name": phase_name, "status": "failed", "error": str(e)}
            logger.error(f"<<< Phase {phase_num} FAILED: {e}")

            # Phase 1 is critical — stop if it fails
            if phase_num == 1:
                logger.error("Phase 1 failed — cannot continue")
                break

    # Summary
    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    for num, info in sorted(results.items()):
        status = "✓" if info["status"] == "success" else "✗"
        msg = f"  {status} Phase {num}: {info['name']}"
        if info["status"] == "failed":
            msg += f" — {info.get('error', '')}"
        print(msg)

    skipped = set(available_phases.keys()) - set(phases_to_run)
    for num in sorted(skipped):
        print(f"  ○ Phase {num}: {available_phases[num][0]} (skipped)")

    print("=" * 70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    main()
