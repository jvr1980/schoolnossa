#!/usr/bin/env python
"""Collect school data from UK Department for Education.

Usage:
    python scripts/collect_uk_data.py --city London
    python scripts/collect_uk_data.py --city Manchester --year 2023-24
    python scripts/collect_uk_data.py --all
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


def collect_city(city: str, academic_year: str) -> dict:
    """Collect school data for a single UK city."""
    db = SessionLocal()

    try:
        logger.info("=" * 60)
        logger.info(f"Starting UK school data collection for {city}")
        logger.info(f"Academic year: {academic_year}")
        logger.info("=" * 60)

        service = SchoolService(db)
        result = service.import_schools_from_uk_dfe(city, academic_year)

        logger.info("=" * 60)
        logger.info(f"Collection completed for {city}!")
        logger.info(f"  Created: {result['created']}")
        logger.info(f"  Updated: {result['updated']}")
        logger.info(f"  Errors: {result['errors']}")
        logger.info(f"  Total processed: {result['total_processed']}")
        logger.info("=" * 60)

        return result

    except Exception as e:
        logger.error(f"Collection failed for {city}: {str(e)}", exc_info=True)
        return {"status": "failed", "city": city, "error": str(e)}

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Collect UK school data from Department for Education"
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
        help="Academic year (default: 2023-24)",
    )

    args = parser.parse_args()

    if not args.city and not args.all:
        parser.error("Either --city or --all is required")

    cities = list(TARGET_CITIES.keys()) if args.all else [args.city]
    results = []

    for city in cities:
        result = collect_city(city, args.year)
        results.append(result)

    # Summary
    if len(results) > 1:
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        total_created = sum(r.get("created", 0) for r in results)
        total_updated = sum(r.get("updated", 0) for r in results)
        total_errors = sum(r.get("errors", 0) for r in results)
        for r in results:
            city = r.get("city", "unknown")
            status = r.get("status", "unknown")
            logger.info(f"  {city}: {status} ({r.get('total_processed', 0)} schools)")
        logger.info(f"\n  Total created: {total_created}")
        logger.info(f"  Total updated: {total_updated}")
        logger.info(f"  Total errors: {total_errors}")

    failed = [r for r in results if r.get("status") == "failed"]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
