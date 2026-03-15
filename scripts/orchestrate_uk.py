#!/usr/bin/env python
"""Unified UK data collection orchestrator.

Runs all data collection and integration steps for UK cities in sequence:
  1. School data from DfE (GIAS + GCSE + A-Level performance)
  2. Crime data from data.police.uk

Mirrors the Berlin orchestration pattern so that output schemas are identical
(country, city, district, school_type, metrics, crime_stats).

Usage:
    # Collect everything for all UK cities
    python scripts/orchestrate_uk.py --all

    # Single city
    python scripts/orchestrate_uk.py --city London

    # Custom academic year and crime month
    python scripts/orchestrate_uk.py --all --year 2023-24 --crime-date 2024-06

    # Schools only (skip crime)
    python scripts/orchestrate_uk.py --city Manchester --skip-crime

    # Crime only (schools must already be in DB)
    python scripts/orchestrate_uk.py --city Manchester --skip-schools --crime-date 2024-06
"""

import sys
import os
import argparse
import time

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.models.database import SessionLocal
from src.services.school_service import SchoolService
from src.collectors.united_kingdom.cities import TARGET_CITIES
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_schools_for_city(service: SchoolService, city: str, academic_year: str) -> dict:
    """Phase 1: Collect school data from DfE for a single city.

    Fetches GIAS school directory + GCSE/A-Level performance data and
    stores them in the database using the same schema as Berlin schools
    (country, city, district, school_type, contact_info, metrics).

    Args:
        service: SchoolService instance (with active DB session)
        city: Target city name
        academic_year: Academic year for performance data (e.g., '2023-24')

    Returns:
        Import statistics dict
    """
    logger.info(f"  [Phase 1] Collecting school data for {city} ({academic_year})")
    result = service.import_schools_from_uk_dfe(city, academic_year)
    logger.info(
        f"  [Phase 1] {city}: {result['created']} created, "
        f"{result['updated']} updated, {result['errors']} errors "
        f"({result['total_processed']} total)"
    )
    return result


def collect_crime_for_city(service: SchoolService, city: str, crime_date: str) -> dict:
    """Phase 2: Collect crime data from data.police.uk for a single city.

    Fetches street-level crime within 1-mile radius of every school in
    the city that already exists in the database with coordinates.

    Output schema matches Berlin crime data:
    - total_crimes, violent_crimes, theft, burglary, robbery,
      vehicle_crime, drugs, antisocial_behaviour, criminal_damage
    - raw_data (JSONB), data_source, area_name, radius_meters

    Args:
        service: SchoolService instance (with active DB session)
        city: Target city name
        crime_date: Month in YYYY-MM format

    Returns:
        Import statistics dict
    """
    logger.info(f"  [Phase 2] Collecting crime data for {city} ({crime_date})")
    result = service.import_crime_data_uk(city, crime_date)
    logger.info(
        f"  [Phase 2] {city}: {result['created']} created, "
        f"{result['updated']} updated, {result['errors']} errors "
        f"({result['total_processed']} total)"
    )
    return result


def orchestrate_city(
    city: str,
    academic_year: str,
    crime_date: str | None,
    skip_schools: bool = False,
    skip_crime: bool = False,
) -> dict:
    """Run the full collection pipeline for a single UK city.

    Args:
        city: Target city
        academic_year: Academic year for DfE data
        crime_date: Month (YYYY-MM) for crime data, or None to skip
        skip_schools: If True, skip school collection
        skip_crime: If True, skip crime collection

    Returns:
        Combined results dict for the city
    """
    db = SessionLocal()
    try:
        service = SchoolService(db)
        city_result = {"city": city, "status": "success", "schools": None, "crime": None}

        # Phase 1: Schools
        if not skip_schools:
            try:
                city_result["schools"] = collect_schools_for_city(service, city, academic_year)
            except Exception as e:
                logger.error(f"  School collection failed for {city}: {e}", exc_info=True)
                city_result["schools"] = {"status": "failed", "error": str(e)}
                city_result["status"] = "partial"

        # Phase 2: Crime
        if not skip_crime:
            if not crime_date:
                logger.warning(f"  Skipping crime collection for {city}: no --crime-date provided")
            else:
                try:
                    city_result["crime"] = collect_crime_for_city(service, city, crime_date)
                except Exception as e:
                    logger.error(f"  Crime collection failed for {city}: {e}", exc_info=True)
                    city_result["crime"] = {"status": "failed", "error": str(e)}
                    city_result["status"] = "partial"

        return city_result

    except Exception as e:
        logger.error(f"Orchestration failed for {city}: {e}", exc_info=True)
        return {"city": city, "status": "failed", "error": str(e)}

    finally:
        db.close()


def print_summary(results: list[dict]) -> None:
    """Print a summary table of all city results."""
    logger.info("")
    logger.info("=" * 72)
    logger.info("UK DATA COLLECTION SUMMARY")
    logger.info("=" * 72)

    total_schools_created = 0
    total_schools_updated = 0
    total_crime_created = 0
    total_crime_updated = 0
    failed_cities = []

    for r in results:
        city = r["city"]
        status = r["status"]

        schools_info = "-"
        if r.get("schools") and r["schools"].get("total_processed"):
            s = r["schools"]
            schools_info = f"{s['total_processed']} schools ({s['created']} new, {s['updated']} updated)"
            total_schools_created += s.get("created", 0)
            total_schools_updated += s.get("updated", 0)

        crime_info = "-"
        if r.get("crime") and r["crime"].get("total_processed"):
            c = r["crime"]
            crime_info = f"{c['total_processed']} crime records ({c['created']} new, {c['updated']} updated)"
            total_crime_created += c.get("created", 0)
            total_crime_updated += c.get("updated", 0)

        if status == "failed":
            failed_cities.append(city)

        logger.info(f"  {city:15s} [{status:7s}]  Schools: {schools_info}")
        logger.info(f"  {' ':15s}              Crime:   {crime_info}")

    logger.info("-" * 72)
    logger.info(f"  Schools total: {total_schools_created} created, {total_schools_updated} updated")
    logger.info(f"  Crime total:   {total_crime_created} created, {total_crime_updated} updated")

    if failed_cities:
        logger.info(f"  FAILED cities: {', '.join(failed_cities)}")

    logger.info("=" * 72)


def main():
    parser = argparse.ArgumentParser(
        description="UK data collection orchestrator — schools + crime in one pipeline"
    )
    parser.add_argument(
        "--city",
        type=str,
        choices=list(TARGET_CITIES.keys()),
        help="City to collect data for",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Collect data for all target cities",
    )
    parser.add_argument(
        "--year",
        type=str,
        default="2023-24",
        help="Academic year for school performance data (default: 2023-24)",
    )
    parser.add_argument(
        "--crime-date",
        type=str,
        help="Month for crime data in YYYY-MM format (e.g., 2024-06). "
             "If omitted, crime collection is skipped.",
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

    if not args.city and not args.all:
        parser.error("Either --city or --all is required")

    if args.skip_schools and args.skip_crime:
        parser.error("Cannot skip both schools and crime — nothing to collect")

    cities = list(TARGET_CITIES.keys()) if args.all else [args.city]

    logger.info("=" * 72)
    logger.info("UK DATA COLLECTION ORCHESTRATOR")
    logger.info("=" * 72)
    logger.info(f"  Cities:       {', '.join(cities)}")
    logger.info(f"  Academic year: {args.year}")
    logger.info(f"  Crime date:   {args.crime_date or '(skipped)'}")
    logger.info(f"  Skip schools: {args.skip_schools}")
    logger.info(f"  Skip crime:   {args.skip_crime}")
    logger.info("=" * 72)

    start_time = time.time()
    results = []

    for city in cities:
        logger.info(f"\n--- {city} ---")
        result = orchestrate_city(
            city=city,
            academic_year=args.year,
            crime_date=args.crime_date,
            skip_schools=args.skip_schools,
            skip_crime=args.skip_crime,
        )
        results.append(result)

    elapsed = time.time() - start_time
    print_summary(results)
    logger.info(f"  Total time: {elapsed:.1f}s")

    failed = [r for r in results if r["status"] == "failed"]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
