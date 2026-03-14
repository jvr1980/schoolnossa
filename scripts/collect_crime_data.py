#!/usr/bin/env python
"""Collect crime data for schools.

Usage:
    # UK — collect crime for London schools for a specific month
    python scripts/collect_crime_data.py --country UK --city London --date 2024-06

    # UK — collect for all UK cities
    python scripts/collect_crime_data.py --country UK --all --date 2024-06

    # Berlin — collect annual crime data
    python scripts/collect_crime_data.py --country DE --year 2024

    # Berlin — collect latest available
    python scripts/collect_crime_data.py --country DE
"""

import sys
import os
import argparse

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


def collect_uk_crime(city: str, date: str) -> dict:
    """Collect UK crime data for a city."""
    db = SessionLocal()
    try:
        logger.info("=" * 60)
        logger.info(f"Collecting UK crime data for {city}, date={date}")
        logger.info("=" * 60)

        service = SchoolService(db)
        result = service.import_crime_data_uk(city, date)

        logger.info(f"Crime collection for {city}: {result}")
        return result

    except Exception as e:
        logger.error(f"Crime collection failed for {city}: {e}", exc_info=True)
        return {"status": "failed", "city": city, "error": str(e)}
    finally:
        db.close()


def collect_berlin_crime(year: int = None) -> dict:
    """Collect Berlin crime data."""
    db = SessionLocal()
    try:
        logger.info("=" * 60)
        logger.info(f"Collecting Berlin crime data (year={year or 'latest'})")
        logger.info("=" * 60)

        service = SchoolService(db)
        result = service.import_crime_data_berlin(year)

        logger.info(f"Berlin crime collection: {result}")
        return result

    except Exception as e:
        logger.error(f"Berlin crime collection failed: {e}", exc_info=True)
        return {"status": "failed", "error": str(e)}
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Collect crime data for schools"
    )
    parser.add_argument(
        "--country",
        type=str,
        choices=["UK", "DE"],
        required=True,
        help="Country to collect crime data for",
    )
    parser.add_argument(
        "--city",
        type=str,
        choices=list(TARGET_CITIES.keys()),
        help="UK city to collect for (required for UK unless --all)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Collect for all UK cities",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Month in YYYY-MM format (required for UK)",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Year for Berlin data (default: latest)",
    )

    args = parser.parse_args()

    if args.country == "UK":
        if not args.date:
            parser.error("--date is required for UK crime data (YYYY-MM format)")
        if not args.city and not args.all:
            parser.error("Either --city or --all is required for UK")

        cities = list(TARGET_CITIES.keys()) if args.all else [args.city]
        results = []

        for city in cities:
            result = collect_uk_crime(city, args.date)
            results.append(result)

        if len(results) > 1:
            logger.info("\n" + "=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            for r in results:
                logger.info(f"  {r.get('city', 'unknown')}: {r.get('status')} "
                           f"({r.get('total_processed', 0)} schools)")

        failed = [r for r in results if r.get("status") == "failed"]
        return 1 if failed else 0

    elif args.country == "DE":
        result = collect_berlin_crime(args.year)
        return 1 if result.get("status") == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
