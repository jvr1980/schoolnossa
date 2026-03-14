"""Berlin crime data collector using the Kriminalitätsatlas open data.

Downloads the Kriminalitätsatlas CSV from Berlin Open Data portal, which
contains crime counts per Bezirksregion (district region) for 17 crime
categories. Schools are mapped to Bezirksregionen via their district.

Data source: https://daten.berlin.de/datensaetze/kriminalitatsatlas-berlin
Format: CSV with crime counts per Bezirksregion and year
Licence: CC BY 3.0 DE
Geographic granularity: Bezirksregion (143 areas, ~25,000 residents each)
Historical data: 2013–2024 (10+ years)
"""

import httpx
import csv
import io
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

# Berlin Open Data Kriminalitätsatlas dataset URL
# The dataset provides crime counts by Bezirksregion
KRIMINALITAETSATLAS_URL = (
    "https://daten.berlin.de/api/3/action/package_show"
)
KRIMINALITAETSATLAS_PACKAGE_ID = "kriminalitatsatlas-berlin"

# Mapping of Kriminalitätsatlas crime categories (German) to our unified schema
CATEGORY_MAP = {
    "Straftaten insgesamt": "total_crimes",
    "Gewaltkriminalität": "violent_crimes",
    "Diebstahl insgesamt": "theft",
    "Wohnungseinbruch": "burglary",
    "Raub": "robbery",
    "Kfz-Diebstahl": "vehicle_crime",
    "Rauschgiftdelikte": "drugs",
    "Sachbeschädigung": "criminal_damage",
    # Additional categories that contribute to total but don't have
    # a dedicated column — stored in raw_data
    "Fahrraddiebstahl": None,  # Subset of theft
    "Taschendiebstahl": None,  # Subset of theft
    "Betrug": None,
    "Branddelikte": None,
    "Körperverletzung": None,  # Subset of violent
    "Sexualdelikte": None,  # Subset of violent
    "Bedrohung": None,
}


class BerlinCrimeCollector:
    """Collector for Berlin Kriminalitätsatlas crime data.

    Downloads the CSV dataset and maps crime statistics to schools
    based on their Bezirksregion (district region).
    """

    def __init__(self):
        self.timeout = 60.0

    async def fetch_dataset_url(self) -> Optional[str]:
        """Fetch the CSV download URL from the CKAN API.

        Returns:
            URL of the CSV resource, or None if not found
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                KRIMINALITAETSATLAS_URL,
                params={"id": KRIMINALITAETSATLAS_PACKAGE_ID},
            )
            response.raise_for_status()
            data = response.json()

            resources = data.get("result", {}).get("resources", [])
            for resource in resources:
                fmt = resource.get("format", "").upper()
                url = resource.get("url", "")
                if fmt == "CSV" and url:
                    return url

            # Fallback: look for any downloadable resource
            for resource in resources:
                url = resource.get("url", "")
                if url and url.endswith(".csv"):
                    return url

            logger.warning("No CSV resource found in Kriminalitätsatlas dataset")
            return None

    async def fetch_crime_data(self) -> List[Dict[str, Any]]:
        """Download and parse the Kriminalitätsatlas CSV.

        Returns:
            List of raw crime data rows as dicts
        """
        dataset_url = await self.fetch_dataset_url()
        if not dataset_url:
            logger.error("Could not determine Kriminalitätsatlas CSV URL")
            return []

        async with httpx.AsyncClient(
            timeout=self.timeout, follow_redirects=True
        ) as client:
            logger.info(f"Downloading Kriminalitätsatlas from {dataset_url}")
            response = await client.get(dataset_url, timeout=120.0)
            response.raise_for_status()

            # Try to detect encoding — Berlin data is often ISO-8859-1
            content = response.content
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                text = content.decode("iso-8859-1")

            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = list(reader)
            logger.info(f"Parsed {len(rows)} rows from Kriminalitätsatlas")
            return rows

    def aggregate_by_bezirksregion(
        self, rows: List[Dict[str, Any]], year: Optional[int] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Aggregate crime data by Bezirksregion for a given year.

        The CSV typically has columns like:
        - Bezirk (district name)
        - Bezirksregion (district region name)
        - Straftat (crime type)
        - Anzahl (count) or year-specific columns

        The exact format may vary; this method handles common variants.

        Args:
            rows: Parsed CSV rows
            year: Target year (if None, uses latest available)

        Returns:
            Dict mapping Bezirksregion name to aggregated crime counts
        """
        result = defaultdict(lambda: {
            "total_crimes": 0,
            "violent_crimes": 0,
            "theft": 0,
            "burglary": 0,
            "robbery": 0,
            "vehicle_crime": 0,
            "drugs": 0,
            "criminal_damage": 0,
            "antisocial_behaviour": None,  # Not available for Berlin
            "raw_data": {},
            "district": None,
        })

        for row in rows:
            # Try common column name patterns
            region = (
                row.get("Bezirksregion")
                or row.get("bezirksregion")
                or row.get("BZR")
                or row.get("Bezeichnung")
                or ""
            ).strip()

            if not region:
                continue

            district = (
                row.get("Bezirk")
                or row.get("bezirk")
                or ""
            ).strip()

            crime_type = (
                row.get("Straftat")
                or row.get("straftat")
                or row.get("Delikt")
                or ""
            ).strip()

            # Get count — try year-specific column first, then generic
            count_str = None
            if year:
                count_str = row.get(str(year))
            if not count_str:
                count_str = (
                    row.get("Anzahl")
                    or row.get("anzahl")
                    or row.get("Fallzahl")
                    or row.get("Häufigkeitszahl")
                    or "0"
                )

            try:
                count = int(count_str.replace(".", "").replace(",", "").strip())
            except (ValueError, AttributeError):
                count = 0

            entry = result[region]
            entry["district"] = district or entry["district"]
            entry["raw_data"][crime_type] = count

            # Map to unified columns
            mapped = CATEGORY_MAP.get(crime_type)
            if mapped and mapped in entry:
                entry[mapped] = count

        return dict(result)

    async def collect_crime_for_schools(
        self,
        schools: List[Dict[str, Any]],
        year: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Collect crime data for Berlin schools.

        Maps each school to its Bezirksregion using the district field
        and assigns the area-level crime statistics.

        Args:
            schools: List of school dicts with school_id, district
            year: Target year (default: latest available)

        Returns:
            List of crime stats dicts ready for database import
        """
        rows = await self.fetch_crime_data()
        if not rows:
            return []

        crime_by_region = self.aggregate_by_bezirksregion(rows, year)
        if not crime_by_region:
            logger.warning("No crime data could be aggregated")
            return []

        # Determine year from data if not specified
        if not year:
            # Try to find the latest year from column names
            if rows:
                for col in sorted(rows[0].keys(), reverse=True):
                    if col.isdigit() and 2010 <= int(col) <= 2030:
                        year = int(col)
                        break
            if not year:
                year = 2024  # fallback

        results = []
        matched = 0

        for school in schools:
            school_id = school.get("school_id")
            district = school.get("district", "")

            if not school_id or not district:
                continue

            # Try to match school district to a Bezirksregion
            # Schools have Bezirk-level district; crime data has Bezirksregion.
            # We match by finding Bezirksregionen within the school's Bezirk.
            matching_regions = [
                (region_name, data)
                for region_name, data in crime_by_region.items()
                if data.get("district", "").lower() == district.lower()
            ]

            if not matching_regions:
                # Fallback: try partial match on region name
                matching_regions = [
                    (region_name, data)
                    for region_name, data in crime_by_region.items()
                    if district.lower() in region_name.lower()
                    or region_name.lower() in district.lower()
                ]

            if matching_regions:
                # Average across matching regions if multiple
                avg_data = self._average_regions(matching_regions)
                results.append({
                    "school_id": school_id,
                    "year": year,
                    "month": None,  # Annual data
                    "radius_meters": None,  # Area-based, not radius
                    "area_name": matching_regions[0][0] if len(matching_regions) == 1 else f"{district} (avg {len(matching_regions)} regions)",
                    "data_source": "berlin_kriminalitaetsatlas",
                    **avg_data,
                })
                matched += 1

        logger.info(
            f"Matched crime data for {matched}/{len(schools)} Berlin schools"
        )
        return results

    def _average_regions(
        self, regions: List[tuple]
    ) -> Dict[str, Any]:
        """Average crime counts across multiple Bezirksregionen."""
        n = len(regions)
        if n == 0:
            return {}

        fields = [
            "total_crimes", "violent_crimes", "theft", "burglary",
            "robbery", "vehicle_crime", "drugs", "criminal_damage",
        ]

        averaged = {}
        raw = {}
        for field in fields:
            total = sum(data.get(field, 0) or 0 for _, data in regions)
            averaged[field] = round(total / n)

        averaged["antisocial_behaviour"] = None  # Not available for Berlin

        # Merge raw data
        for _, data in regions:
            for k, v in data.get("raw_data", {}).items():
                raw[k] = raw.get(k, 0) + (v or 0)
        # Average raw data too
        averaged["raw_data"] = {k: round(v / n) for k, v in raw.items()}

        return averaged


class BerlinCrimeCollectorSync:
    """Synchronous wrapper for BerlinCrimeCollector."""

    def __init__(self):
        self.collector = BerlinCrimeCollector()

    def collect_crime_for_schools(
        self,
        schools: List[Dict[str, Any]],
        year: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Collect crime data for Berlin schools synchronously."""
        import asyncio
        return asyncio.run(
            self.collector.collect_crime_for_schools(schools, year)
        )
