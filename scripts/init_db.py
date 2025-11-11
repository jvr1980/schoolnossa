#!/usr/bin/env python
"""Initialize database tables"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.models.database import init_db, engine
from src.models.school import School, SchoolMetricsAnnual, EnrollmentArea, CollectionLog
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Initialize database tables"""
    try:
        logger.info("Initializing database tables...")
        init_db()
        logger.info("✓ Database tables created successfully!")

        # Verify tables were created
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        logger.info(f"Created tables: {', '.join(tables)}")

        if not tables:
            logger.error("No tables were created!")
            sys.exit(1)

        return 0

    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
