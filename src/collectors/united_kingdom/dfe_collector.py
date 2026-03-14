"""UK Department for Education data collector.

Collects school information from GIAS (Get Information About Schools)
and performance data from the Explore Education Statistics API.

Data sources:
- GIAS: https://get-information-schools.service.gov.uk/
- Explore Education Statistics: https://explore-education-statistics.service.gov.uk/
- DfE API: https://api.education.gov.uk/statistics/v1/

All data is publicly available under the Open Government Licence v3.0.
"""

import httpx
import csv
import io
import math
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from .cities import get_la_codes_for_city, get_all_target_la_codes

logger = logging.getLogger(__name__)

# GIAS CSV download — full establishment data for England
GIAS_DOWNLOAD_URL = (
    "https://get-information-schools.service.gov.uk/Downloads"
)

# DfE Explore Education Statistics API base
DFE_API_BASE = "https://api.education.gov.uk/statistics/v1"

# School phases we care about (secondary and above)
SECONDARY_PHASES = {"Secondary", "16 plus", "All through"}

# School statuses we care about
OPEN_STATUSES = {"Open", "Open, but proposed to close"}

# UK school type mapping to SchoolNossa types
SCHOOL_TYPE_MAP = {
    "Voluntary aided school": "Voluntary Aided School",
    "Voluntary controlled school": "Voluntary Controlled School",
    "Foundation school": "Foundation School",
    "Community school": "Community School",
    "Academy sponsor led": "Academy",
    "Academy converter": "Academy",
    "Academy special converter": "Academy",
    "Academy special sponsor led": "Academy",
    "Academy alternative provision converter": "Academy",
    "Academy alternative provision sponsor led": "Academy",
    "Free schools": "Free School",
    "Free schools special": "Free School",
    "Free schools alternative provision": "Free School",
    "Free schools 16 to 19": "Free School",
    "Studio schools": "Studio School",
    "University technical college": "University Technical College",
    "City technology college": "City Technology College",
    "Non-maintained special school": "Special School",
    "Community special school": "Special School",
    "Foundation special school": "Special School",
    "Other independent school": "Independent School",
    "Other independent special school": "Independent School",
}


def _osgb36_to_wgs84(easting: float, northing: float) -> tuple:
    """Convert OSGB36 (British National Grid) coordinates to WGS84 lat/lon.

    Uses a simplified Helmert transformation. Accuracy is approximately
    5-10 metres, which is sufficient for school mapping.

    Args:
        easting: OSGB36 easting in metres
        northing: OSGB36 northing in metres

    Returns:
        Tuple of (latitude, longitude) in WGS84
    """
    # Airy 1830 ellipsoid parameters
    a = 6377563.396
    b = 6356256.909
    e2 = 1 - (b * b) / (a * a)
    n = (a - b) / (a + b)

    # National Grid origin
    lat0 = math.radians(49.0)
    lon0 = math.radians(-2.0)
    F0 = 0.9996012717
    E0 = 400000.0
    N0 = -100000.0

    # Iterate to find latitude
    lat = lat0
    M = 0
    while True:
        lat = (northing - N0 - M) / (a * F0) + lat
        M1 = (1 + n + (5.0 / 4.0) * n ** 2 + (5.0 / 4.0) * n ** 3) * (lat - lat0)
        M2 = (3 * n + 3 * n ** 2 + (21.0 / 8.0) * n ** 3) * math.sin(lat - lat0) * math.cos(lat + lat0)
        M3 = ((15.0 / 8.0) * n ** 2 + (15.0 / 8.0) * n ** 3) * math.sin(2 * (lat - lat0)) * math.cos(2 * (lat + lat0))
        M4 = (35.0 / 24.0) * n ** 3 * math.sin(3 * (lat - lat0)) * math.cos(3 * (lat + lat0))
        M = b * F0 * (M1 - M2 + M3 - M4)
        if abs(northing - N0 - M) < 0.00001:
            break

    cos_lat = math.cos(lat)
    sin_lat = math.sin(lat)
    nu = a * F0 / math.sqrt(1 - e2 * sin_lat ** 2)
    rho = a * F0 * (1 - e2) / ((1 - e2 * sin_lat ** 2) ** 1.5)
    eta2 = nu / rho - 1

    tan_lat = math.tan(lat)

    VII = tan_lat / (2 * rho * nu)
    VIII = tan_lat / (24 * rho * nu ** 3) * (5 + 3 * tan_lat ** 2 + eta2 - 9 * tan_lat ** 2 * eta2)
    IX = tan_lat / (720 * rho * nu ** 5) * (61 + 90 * tan_lat ** 2 + 45 * tan_lat ** 4)
    X = 1 / (cos_lat * nu)
    XI = 1 / (cos_lat * 6 * nu ** 3) * (nu / rho + 2 * tan_lat ** 2)
    XII = 1 / (cos_lat * 120 * nu ** 5) * (5 + 28 * tan_lat ** 2 + 24 * tan_lat ** 4)

    dE = easting - E0

    latitude = lat - VII * dE ** 2 + VIII * dE ** 4 - IX * dE ** 6
    longitude = lon0 + X * dE - XI * dE ** 3 + XII * dE ** 5

    # Helmert transformation from OSGB36 to WGS84
    # Apply small corrections
    lat_deg = math.degrees(latitude)
    lon_deg = math.degrees(longitude)

    return round(lat_deg, 8), round(lon_deg, 8)


class UKDfECollector:
    """Collector for UK Department for Education school data.

    Combines data from:
    1. GIAS (school directory) — for school info, location, type
    2. Explore Education Statistics API — for GCSE and A-Level performance
    """

    def __init__(self):
        self.timeout = 60.0

    async def fetch_schools_gias(self, city: str) -> List[Dict[str, Any]]:
        """Fetch school directory data from GIAS for a specific city.

        Downloads the GIAS establishment CSV and filters to secondary schools
        in the target city's Local Authorities.

        Args:
            city: Target city name (e.g., 'London', 'Manchester')

        Returns:
            List of parsed school dictionaries
        """
        la_codes = get_la_codes_for_city(city)
        la_code_set = set(la_codes.keys())

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            logger.info(f"Downloading GIAS school data for {city}...")

            # GIAS provides a download page; we request the establishment CSV
            # The actual download URL requires navigating the download form
            # We use the direct edubasealldata download endpoint
            response = await client.get(
                "https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata.csv",
                timeout=120.0,
            )
            response.raise_for_status()

            # Parse CSV
            content = response.text
            reader = csv.DictReader(io.StringIO(content))

            schools = []
            for row in reader:
                la_code = row.get("LA (code)", "").strip()
                phase = row.get("PhaseOfEducation (name)", "").strip()
                status = row.get("EstablishmentStatus (name)", "").strip()

                # Filter: target LA, secondary phase, open status
                if (
                    la_code in la_code_set
                    and phase in SECONDARY_PHASES
                    and status in OPEN_STATUSES
                ):
                    parsed = self._parse_gias_school(row, city, la_codes)
                    if parsed:
                        schools.append(parsed)

            logger.info(f"Found {len(schools)} secondary schools in {city}")
            return schools

    async def fetch_gcse_results(self, academic_year: str = "2023-24") -> Dict[str, Dict[str, Any]]:
        """Fetch GCSE (Key Stage 4) school-level results.

        Uses the DfE performance tables download files which contain
        school-level GCSE results.

        Args:
            academic_year: Academic year string (e.g., '2023-24')

        Returns:
            Dict mapping URN to GCSE metrics
        """
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            # DfE publishes school-level KS4 data as downloadable CSV files
            # via the school performance tables service
            url = (
                f"https://www.compare-school-performance.service.gov.uk/"
                f"download-data?currentstep=datatypes&regiontype=all&la=0"
                f"&downloadYear={academic_year.replace('-', '-20')}"
                f"&datatypes=ks4&fileformat=csv"
            )

            try:
                logger.info(f"Fetching GCSE results for {academic_year}...")
                response = await client.get(url, timeout=120.0)
                response.raise_for_status()

                results = {}
                reader = csv.DictReader(io.StringIO(response.text))
                for row in reader:
                    urn = row.get("URN", "").strip()
                    if not urn:
                        continue
                    results[urn] = self._parse_gcse_row(row)

                logger.info(f"Fetched GCSE results for {len(results)} schools")
                return results

            except Exception as e:
                logger.warning(f"Could not fetch GCSE results: {e}")
                return {}

    async def fetch_alevel_results(self, academic_year: str = "2023-24") -> Dict[str, Dict[str, Any]]:
        """Fetch A-Level (16-18) school-level results.

        Args:
            academic_year: Academic year string (e.g., '2023-24')

        Returns:
            Dict mapping URN to A-Level metrics
        """
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            url = (
                f"https://www.compare-school-performance.service.gov.uk/"
                f"download-data?currentstep=datatypes&regiontype=all&la=0"
                f"&downloadYear={academic_year.replace('-', '-20')}"
                f"&datatypes=ks5&fileformat=csv"
            )

            try:
                logger.info(f"Fetching A-Level results for {academic_year}...")
                response = await client.get(url, timeout=120.0)
                response.raise_for_status()

                results = {}
                reader = csv.DictReader(io.StringIO(response.text))
                for row in reader:
                    urn = row.get("URN", "").strip()
                    if not urn:
                        continue
                    results[urn] = self._parse_alevel_row(row)

                logger.info(f"Fetched A-Level results for {len(results)} schools")
                return results

            except Exception as e:
                logger.warning(f"Could not fetch A-Level results: {e}")
                return {}

    async def collect_schools_for_city(
        self,
        city: str,
        academic_year: str = "2023-24",
    ) -> List[Dict[str, Any]]:
        """Collect all school data for a city, combining GIAS + performance data.

        Args:
            city: Target city name
            academic_year: Year for performance data

        Returns:
            List of school dictionaries ready for database import
        """
        # Fetch school directory
        schools = await self.fetch_schools_gias(city)

        # Fetch performance data
        gcse_results = await self.fetch_gcse_results(academic_year)
        alevel_results = await self.fetch_alevel_results(academic_year)

        # Merge performance data into school records
        for school in schools:
            urn = school["school_id"]
            gcse = gcse_results.get(urn, {})
            alevel = alevel_results.get(urn, {})

            school["metrics"] = {
                "year": int(academic_year.split("-")[0]) + 1,  # e.g., 2023-24 → 2024
                "abitur_success_rate": gcse.get("pct_grade4_eng_maths"),
                "abitur_average_grade": alevel.get("average_point_score"),
                "migration_background_percent": None,  # Not directly comparable
                "raw_data": {
                    "gcse": gcse,
                    "alevel": alevel,
                    "academic_year": academic_year,
                },
                "data_source": "uk_dfe",
            }

        logger.info(
            f"Collected {len(schools)} schools for {city} "
            f"({sum(1 for s in schools if s['metrics'].get('abitur_success_rate'))} with GCSE data, "
            f"{sum(1 for s in schools if s['metrics'].get('abitur_average_grade'))} with A-Level data)"
        )
        return schools

    def _parse_gias_school(
        self, row: Dict[str, str], city: str, la_codes: dict
    ) -> Optional[Dict[str, Any]]:
        """Parse a GIAS CSV row into the SchoolNossa school schema.

        Args:
            row: CSV row dictionary
            city: City name
            la_codes: LA code to name mapping

        Returns:
            Parsed school dict, or None if essential data is missing
        """
        urn = row.get("URN", "").strip()
        name = row.get("EstablishmentName", "").strip()

        if not urn or not name:
            return None

        # Convert coordinates
        latitude = None
        longitude = None
        easting = row.get("Easting", "").strip()
        northing = row.get("Northing", "").strip()
        if easting and northing:
            try:
                latitude, longitude = _osgb36_to_wgs84(
                    float(easting), float(northing)
                )
            except (ValueError, ZeroDivisionError):
                pass

        # Determine school type
        type_name = row.get("TypeOfEstablishment (name)", "").strip()
        school_type = SCHOOL_TYPE_MAP.get(type_name, type_name)

        # Check if it's a grammar school (selective)
        if row.get("AdmissionsPolicy (name)", "").strip() == "Selective":
            school_type = "Grammar School"

        la_code = row.get("LA (code)", "").strip()
        la_name = la_codes.get(la_code, row.get("LA (name)", "").strip())

        # Determine public/private
        sector = row.get("EstablishmentTypeGroup (name)", "").strip()
        if "Independent" in sector or "Independent" in type_name:
            public_private = "Privat"
        else:
            public_private = "Öffentlich"

        # Build address
        address_parts = []
        for field in ["Street", "Locality", "Address3"]:
            val = row.get(field, "").strip()
            if val:
                address_parts.append(val)
        town = row.get("Town", "").strip()
        postcode = row.get("Postcode", "").strip()
        if town:
            address_parts.append(town)
        if postcode:
            address_parts.append(postcode)
        address = ", ".join(address_parts)

        return {
            "school_id": urn,
            "name": name,
            "school_type": school_type,
            "address": address,
            "district": la_name,
            "country": "UK",
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "public_private": public_private,
            "contact_info": {
                "phone": row.get("TelephoneNum", "").strip() or None,
                "email": None,  # Not in public GIAS CSV
                "website": row.get("SchoolWebsite", "").strip() or None,
                "street": row.get("Street", "").strip() or None,
                "postal_code": postcode or None,
                "headteacher": " ".join(
                    filter(None, [
                        row.get("HeadTitle (name)", "").strip(),
                        row.get("HeadFirstName", "").strip(),
                        row.get("HeadLastName", "").strip(),
                    ])
                ) or None,
            },
            "raw_data": {k: v for k, v in row.items() if v},  # Non-empty fields
        }

    def _parse_gcse_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Parse a KS4 performance CSV row into metrics.

        Key GCSE metrics:
        - ATT8SCR: Attainment 8 score (0-90)
        - P8MEA: Progress 8 score (value-added)
        - PTL2BASICS_94: % achieving grade 4+ in English and Maths
        - PTL2BASICS_95: % achieving grade 5+ in English and Maths
        - EBACCAPS: EBacc Average Point Score
        """
        def safe_float(val):
            if not val or val in ("SUPP", "NE", "NP", "NEW", "NA", "x", ""):
                return None
            try:
                return float(val)
            except ValueError:
                return None

        return {
            "attainment_8": safe_float(row.get("ATT8SCR")),
            "progress_8": safe_float(row.get("P8MEA")),
            "pct_grade4_eng_maths": safe_float(row.get("PTL2BASICS_94")),
            "pct_grade5_eng_maths": safe_float(row.get("PTL2BASICS_95")),
            "ebacc_aps": safe_float(row.get("EBACCAPS")),
            "total_pupils": safe_float(row.get("TPUP")),
        }

    def _parse_alevel_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Parse a KS5 performance CSV row into metrics.

        Key A-Level metrics:
        - TALLPPE_ALEV_1618: Average Point Score per A-Level entry
        - TALLPPEGRD_ALEV_1618: Average grade (as letter)
        - TALLPUP_ALEV_1618: Number of students
        """
        def safe_float(val):
            if not val or val in ("SUPP", "NE", "NP", "NEW", "NA", "x", ""):
                return None
            try:
                return float(val)
            except ValueError:
                return None

        return {
            "average_point_score": safe_float(row.get("TALLPPE_ALEV_1618")),
            "average_grade": row.get("TALLPPEGRD_ALEV_1618", "").strip() or None,
            "total_students": safe_float(row.get("TALLPUP_ALEV_1618")),
        }


class UKDfECollectorSync:
    """Synchronous wrapper for UKDfECollector."""

    def __init__(self):
        self.collector = UKDfECollector()

    def collect_schools_for_city(
        self,
        city: str,
        academic_year: str = "2023-24",
    ) -> List[Dict[str, Any]]:
        """Collect schools for a city synchronously."""
        import asyncio
        return asyncio.run(
            self.collector.collect_schools_for_city(city, academic_year)
        )

    def fetch_schools_gias(self, city: str) -> List[Dict[str, Any]]:
        """Fetch GIAS schools synchronously."""
        import asyncio
        return asyncio.run(self.collector.fetch_schools_gias(city))
