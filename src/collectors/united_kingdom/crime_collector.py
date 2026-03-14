"""UK crime data collector using the data.police.uk API.

Fetches street-level crime data within a 1-mile radius of each school's
coordinates. No API key required.

API documentation: https://data.police.uk/docs/
Rate limit: 15 requests/second sustained, 30 burst.
Data availability: December 2013 to present, updated monthly.

Licence: Open Government Licence v3.0.
"""

import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)

API_BASE = "https://data.police.uk/api"

# Map data.police.uk crime categories to our unified schema columns
CATEGORY_MAP = {
    # → violent_crimes
    "violent-crime": "violent_crimes",
    "violence-and-sexual-offences": "violent_crimes",
    # → theft
    "theft-from-the-person": "theft",
    "other-theft": "theft",
    "shoplifting": "theft",
    "bicycle-theft": "theft",
    # → burglary
    "burglary": "burglary",
    # → robbery
    "robbery": "robbery",
    # → vehicle_crime
    "vehicle-crime": "vehicle_crime",
    # → drugs
    "drugs": "drugs",
    # → antisocial_behaviour
    "anti-social-behaviour": "antisocial_behaviour",
    # → criminal_damage
    "criminal-damage-arson": "criminal_damage",
    # Categories that go into total but not a specific column
    "public-order": None,
    "possession-of-weapons": None,
    "other-crime": None,
}


class UKCrimeCollector:
    """Collects crime data from data.police.uk for school locations."""

    def __init__(self):
        self.timeout = 30.0
        # Semaphore to respect 15 req/sec rate limit
        self._semaphore = asyncio.Semaphore(10)

    async def fetch_crimes_near_school(
        self,
        latitude: float,
        longitude: float,
        date: str,
    ) -> List[Dict[str, Any]]:
        """Fetch street-level crimes within 1 mile of a location.

        Args:
            latitude: School latitude (WGS84)
            longitude: School longitude (WGS84)
            date: Month in YYYY-MM format (e.g., '2024-01')

        Returns:
            List of crime records from the API
        """
        url = f"{API_BASE}/crimes-street/all-crime"
        params = {
            "lat": str(latitude),
            "lng": str(longitude),
            "date": date,
        }

        async with self._semaphore:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)

                if response.status_code == 503:
                    logger.warning(
                        f"Too many crimes in area around ({latitude}, {longitude}) "
                        f"for {date} — API returned 503"
                    )
                    return []

                response.raise_for_status()
                return response.json()

    def aggregate_crimes(self, crimes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate raw crime records into summary counts by category.

        Args:
            crimes: Raw crime records from data.police.uk API

        Returns:
            Dict with aggregated counts matching CrimeStats columns
        """
        counts = Counter()
        category_breakdown = Counter()

        for crime in crimes:
            category = crime.get("category", "")
            category_breakdown[category] += 1

            mapped = CATEGORY_MAP.get(category)
            if mapped:
                counts[mapped] += 1

        total = len(crimes)

        return {
            "total_crimes": total,
            "violent_crimes": counts.get("violent_crimes", 0),
            "theft": counts.get("theft", 0),
            "burglary": counts.get("burglary", 0),
            "robbery": counts.get("robbery", 0),
            "vehicle_crime": counts.get("vehicle_crime", 0),
            "drugs": counts.get("drugs", 0),
            "antisocial_behaviour": counts.get("antisocial_behaviour", 0),
            "criminal_damage": counts.get("criminal_damage", 0),
            "raw_data": dict(category_breakdown),
        }

    async def collect_crime_for_school(
        self,
        school_id: str,
        latitude: float,
        longitude: float,
        date: str,
    ) -> Optional[Dict[str, Any]]:
        """Collect and aggregate crime data for a single school.

        Args:
            school_id: School identifier (URN)
            latitude: School latitude
            longitude: School longitude
            date: Month in YYYY-MM format

        Returns:
            Dict ready for CrimeStats model, or None on failure
        """
        try:
            crimes = await self.fetch_crimes_near_school(latitude, longitude, date)
            aggregated = self.aggregate_crimes(crimes)

            year, month = date.split("-")

            return {
                "school_id": school_id,
                "year": int(year),
                "month": int(month),
                "radius_meters": 1609,  # 1 mile
                "area_name": "1-mile radius",
                "data_source": "data_police_uk",
                **aggregated,
            }

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error fetching crime for school {school_id}: "
                f"{e.response.status_code}"
            )
            return None
        except Exception as e:
            logger.error(f"Error fetching crime for school {school_id}: {e}")
            return None

    async def collect_crimes_for_schools(
        self,
        schools: List[Dict[str, Any]],
        date: str,
    ) -> List[Dict[str, Any]]:
        """Collect crime data for multiple schools concurrently.

        Respects data.police.uk rate limits (15 req/sec) via semaphore.

        Args:
            schools: List of dicts with school_id, latitude, longitude
            date: Month in YYYY-MM format

        Returns:
            List of crime stats dicts ready for database import
        """
        logger.info(
            f"Collecting crime data for {len(schools)} schools, date={date}"
        )

        tasks = [
            self.collect_crime_for_school(
                school["school_id"],
                float(school["latitude"]),
                float(school["longitude"]),
                date,
            )
            for school in schools
            if school.get("latitude") and school.get("longitude")
        ]

        results = await asyncio.gather(*tasks)

        successful = [r for r in results if r is not None]
        logger.info(
            f"Collected crime data for {len(successful)}/{len(tasks)} schools"
        )
        return successful

    async def get_available_dates(self) -> List[str]:
        """Get list of available crime data dates from the API.

        Returns:
            List of date strings in YYYY-MM format, most recent first
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{API_BASE}/crimes-street-dates")
            response.raise_for_status()
            dates = response.json()
            return [d["date"] for d in dates]


class UKCrimeCollectorSync:
    """Synchronous wrapper for UKCrimeCollector."""

    def __init__(self):
        self.collector = UKCrimeCollector()

    def collect_crimes_for_schools(
        self,
        schools: List[Dict[str, Any]],
        date: str,
    ) -> List[Dict[str, Any]]:
        """Collect crime data for schools synchronously."""
        return asyncio.run(
            self.collector.collect_crimes_for_schools(schools, date)
        )

    def get_available_dates(self) -> List[str]:
        """Get available dates synchronously."""
        return asyncio.run(self.collector.get_available_dates())
