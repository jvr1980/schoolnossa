"""Catchment profiling engine.

Builds a CatchmentProfile for each school by querying multiple data sources
within its catchment radius. Directly adapted from GreenspaceFinder's
step5_profile.py pattern.
"""

import asyncio
import logging
from typing import Dict, List, Optional

from ..services.crime import CrimeService
from ..services.population import PopulationService
from ..services.google_places import GooglePlacesClient
from ..utils.geo import haversine
from .dimensions import DIMENSIONS, DimensionSource
from .types import CatchmentProfile

logger = logging.getLogger(__name__)


class SchoolProfiler:
    """Builds catchment profiles for schools.

    For each school location, queries all configured data sources within
    the catchment radius and populates dimension values.
    """

    def __init__(
        self,
        radius_m: float = 1000.0,
        google_client: Optional[GooglePlacesClient] = None,
    ) -> None:
        self.radius_m = radius_m
        self._crime = CrimeService()
        self._population = PopulationService()
        self._google = google_client or GooglePlacesClient()

    async def profile_school(
        self,
        school_id: str,
        lat: float,
        lng: float,
        address: str = "",
        school_type: str = "",
        all_schools: Optional[List[dict]] = None,
    ) -> CatchmentProfile:
        """Build a complete catchment profile for one school.

        Args:
            school_id: Unique school identifier.
            lat: School latitude.
            lng: School longitude.
            address: Full address string (for postcode extraction).
            school_type: Type of school (for competition dimension).
            all_schools: List of all schools (for competition calculation).
                Each entry needs 'school_id', 'latitude', 'longitude', 'school_type'.
        """
        profile = CatchmentProfile(
            school_id=school_id,
            latitude=lat,
            longitude=lng,
            radius_m=self.radius_m,
        )

        # --- Local data dimensions (fast, no API calls) ---
        self._profile_crime(profile, lat, lng, address)
        self._profile_population(profile, lat, lng, address)

        if all_schools:
            self._profile_competition(profile, lat, lng, school_type, all_schools)

        # --- Google Nearby Search dimensions (async, rate-limited) ---
        await self._profile_google_pois(profile, lat, lng)

        return profile

    def _profile_crime(
        self, profile: CatchmentProfile, lat: float, lng: float, address: str
    ) -> None:
        profile.set("crime_index", self._crime.get_crime_index(lat, lng, address))

    def _profile_population(
        self, profile: CatchmentProfile, lat: float, lng: float, address: str
    ) -> None:
        pop, density = self._population.estimate_catchment_population(
            lat, lng, self.radius_m, address
        )
        profile.set("catchment_population", pop)
        profile.set("population_density", density)

    def _profile_competition(
        self,
        profile: CatchmentProfile,
        lat: float,
        lng: float,
        school_type: str,
        all_schools: List[dict],
    ) -> None:
        """Count same-type schools within radius and find nearest."""
        count = 0
        nearest = float("inf")

        for other in all_schools:
            if other["school_id"] == profile.school_id:
                continue
            if school_type and other.get("school_type") != school_type:
                continue

            olat = float(other["latitude"])
            olng = float(other["longitude"])
            dist = haversine(lat, lng, olat, olng)

            if dist <= self.radius_m:
                count += 1
            if dist < nearest:
                nearest = dist

        profile.set("other_schools_count", float(count))
        if nearest < float("inf"):
            profile.set("nearest_school_m", nearest)

    async def _profile_google_pois(
        self, profile: CatchmentProfile, lat: float, lng: float
    ) -> None:
        """Query Google Nearby Search for all configured POI dimensions."""
        google_dims = [
            d for d in DIMENSIONS.values()
            if d.source == DimensionSource.GOOGLE_NEARBY and d.google_place_type
        ]

        # Group by place_type to avoid duplicate API calls
        type_to_dims: Dict[str, list] = {}
        for d in google_dims:
            type_to_dims.setdefault(d.google_place_type, []).append(d)

        tasks = []
        for place_type in type_to_dims:
            tasks.append(self._query_and_assign(profile, lat, lng, place_type, type_to_dims[place_type]))

        await asyncio.gather(*tasks)

    async def _query_and_assign(
        self,
        profile: CatchmentProfile,
        lat: float,
        lng: float,
        place_type: str,
        dims: list,
    ) -> None:
        count, nearest = await self._google.count_nearby(
            lat, lng, self.radius_m, place_type
        )

        for d in dims:
            if d.key.endswith("_count"):
                profile.set(d.key, float(count))
            elif d.key.endswith("_nearest_m") or d.key.endswith("_m"):
                profile.set(d.key, nearest if nearest is not None else self.radius_m * 2)

    async def profile_schools(
        self,
        schools: List[dict],
        progress_callback=None,
    ) -> List[CatchmentProfile]:
        """Profile a batch of schools.

        Args:
            schools: List of dicts with school_id, latitude, longitude, address, school_type.
            progress_callback: Optional callable(done, total) for progress updates.

        Returns:
            List of CatchmentProfile objects.
        """
        profiles = []
        total = len(schools)

        for i, school in enumerate(schools):
            try:
                p = await self.profile_school(
                    school_id=school["school_id"],
                    lat=float(school["latitude"]),
                    lng=float(school["longitude"]),
                    address=school.get("address", ""),
                    school_type=school.get("school_type", ""),
                    all_schools=schools,
                )
                profiles.append(p)
            except Exception as e:
                logger.error(f"Error profiling school {school['school_id']}: {e}")
                continue

            if progress_callback:
                progress_callback(i + 1, total)

        return profiles
