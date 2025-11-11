#!/usr/bin/env python
"""Collect school data from Berlin Open Data Portal"""

import sys
import os

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


def main():
    """Main collection script"""
    db = SessionLocal()

    try:
        logger.info("=" * 60)
        logger.info("Starting Berlin school data collection")
        logger.info("=" * 60)

        service = SchoolService(db)
        result = service.import_schools_from_berlin_open_data()

        logger.info("=" * 60)
        logger.info("Collection completed!")
        logger.info(f"  Created: {result['created']}")
        logger.info(f"  Updated: {result['updated']}")
        logger.info(f"  Errors: {result['errors']}")
        logger.info(f"  Total processed: {result['total_processed']}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Collection failed: {str(e)}", exc_info=True)
        return 1

    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
