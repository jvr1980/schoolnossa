#!/usr/bin/env python
"""Unified Berlin data collection orchestrator.

Runs all data collection and integration steps for Berlin in sequence:
  1. School data from Berlin Open Data (WFS)
  2. Crime data from Kriminalitätsatlas

Output schema matches the UK orchestrator (country, city, district,
school_type, metrics, crime_stats).

Usage:
    # Collect everything (latest crime year)
    python scripts/orchestrate_berlin.py

    # Specific crime year
    python scripts/orchestrate_berlin.py --crime-year 2023

    # Schools only
    python scripts/orchestrate_berlin.py --skip-crime

    # Crime only (schools must already be in DB)
    python scripts/orchestrate_berlin.py --skip-schools --crime-year 2023
"""

import sys
import os
import argparse
import time

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.models.database import SessionLocal
from src.services.school_service import SchoolService
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_schools(service: SchoolService) -> dict:
    """Phase 1: Collect school data from Berlin Open Data (WFS).

    Fetches school directory from Berlin WFS endpoint and stores
    them in the database with country='DE', city='Berlin'.

    Args:
        service: SchoolService instance (with active DB session)

    Returns:
        Import statistics dict
    """
    logger.info("  [Phase 1] Collecting school data from Berlin Open Data")
    result = service.import_schools_from_berlin_open_data()
    logger.info(
        f"  [Phase 1] Berlin: {result['created']} created, "
        f"{result['updated']} updated, {result['errors']} errors "
        f"({result['total_processed']} total)"
    )
    return result


def collect_crime(service: SchoolService, year: int | None) -> dict:
    """Phase 2: Collect crime data from Kriminalitätsatlas.

    Downloads CSV from Berlin Open Data and maps crime statistics to
    schools by Bezirksregion.

    Output schema matches UK crime data:
    - total_crimes, violent_crimes, theft, burglary, robbery,
      vehicle_crime, drugs, antisocial_behaviour, criminal_damage
    - raw_data (JSONB), data_source, area_name

    Args:
        service: SchoolService instance (with active DB session)
        year: Target year (None = latest available)

    Returns:
        Import statistics dict
    """
    logger.info(f"  [Phase 2] Collecting crime data from Kriminalitätsatlas (year={year or 'latest'})")
    result = service.import_crime_data_berlin(year)
    logger.info(
        f"  [Phase 2] Berlin: {result['created']} created, "
        f"{result['updated']} updated, {result['errors']} errors "
        f"({result['total_processed']} total)"
    )
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Berlin data collection orchestrator — schools + crime in one pipeline"
    )
    parser.add_argument(
        "--crime-year",
        type=int,
        help="Year for crime data (default: latest available)",
    )
    parser.add_argument(
        "--skip-schools",
        action="store_true",
        help="Skip school data collection (only collect crime)",
    )
    parser.add_argument(
        "--skip-crime",
        action="store_true",
        help="Skip crime data collection (only collect schools)",
    )

    args = parser.parse_args()

    if args.skip_schools and args.skip_crime:
        parser.error("Cannot skip both schools and crime — nothing to collect")

    logger.info("=" * 72)
    logger.info("BERLIN DATA COLLECTION ORCHESTRATOR")
    logger.info("=" * 72)
    logger.info(f"  Crime year:   {args.crime_year or '(latest available)'}")
    logger.info(f"  Skip schools: {args.skip_schools}")
    logger.info(f"  Skip crime:   {args.skip_crime}")
    logger.info("=" * 72)

    start_time = time.time()
    db = SessionLocal()

    try:
        service = SchoolService(db)
        schools_result = None
        crime_result = None
        status = "success"

        # Phase 1: Schools
        if not args.skip_schools:
            try:
                schools_result = collect_schools(service)
            except Exception as e:
                logger.error(f"  School collection failed: {e}", exc_info=True)
                schools_result = {"status": "failed", "error": str(e)}
                status = "partial"

        # Phase 2: Crime
        if not args.skip_crime:
            try:
                crime_result = collect_crime(service, args.crime_year)
            except Exception as e:
                logger.error(f"  Crime collection failed: {e}", exc_info=True)
                crime_result = {"status": "failed", "error": str(e)}
                status = "partial"

        # Summary
        elapsed = time.time() - start_time

        logger.info("")
        logger.info("=" * 72)
        logger.info("BERLIN DATA COLLECTION SUMMARY")
        logger.info("=" * 72)

        if schools_result:
            if schools_result.get("total_processed"):
                s = schools_result
                logger.info(
                    f"  Schools: {s['total_processed']} processed "
                    f"({s['created']} created, {s['updated']} updated, {s['errors']} errors)"
                )
            else:
                logger.info(f"  Schools: FAILED — {schools_result.get('error', 'unknown')}")
        else:
            logger.info("  Schools: skipped")

        if crime_result:
            if crime_result.get("total_processed"):
                c = crime_result
                logger.info(
                    f"  Crime:   {c['total_processed']} processed "
                    f"({c['created']} created, {c['updated']} updated, {c['errors']} errors)"
                )
            else:
                logger.info(f"  Crime:   FAILED — {crime_result.get('error', 'unknown')}")
        else:
            logger.info("  Crime:   skipped")

        logger.info(f"  Status:  {status}")
        logger.info(f"  Time:    {elapsed:.1f}s")
        logger.info("=" * 72)

        return 0 if status == "success" else 1

    except Exception as e:
        logger.error(f"Orchestration failed: {e}", exc_info=True)
        return 1

    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
