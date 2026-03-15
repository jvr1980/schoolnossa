"""Population data service.

Estimates catchment population and density for a given location using
postcode-level data. Modeled after GreenspaceFinder's population.py.
"""

import json
import math
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

from ..utils.geo import haversine

_DATA_PATH = Path(__file__).parent / "data" / "berlin_population.json"


class PopulationService:
    """Berlin population data lookup and catchment estimation."""

    def __init__(self) -> None:
        with open(_DATA_PATH, encoding="utf-8") as f:
            data = json.load(f)
        self._postcodes: Dict[str, dict] = data["postcodes"]

        # Precompute centroid approximations per postcode for nearest-match
        # (In a real system these would come from PLZ boundary centroids)
        self._city_avg_density = sum(
            p["density"] for p in self._postcodes.values()
        ) / max(len(self._postcodes), 1)

    def _extract_postcode(self, address: str) -> Optional[str]:
        match = re.search(r"\b(1\d{4})\b", address)
        return match.group(1) if match else None

    def get_postcode_data(self, address: str) -> Optional[dict]:
        """Get population data for the postcode extracted from an address."""
        pc = self._extract_postcode(address)
        if pc:
            return self._postcodes.get(pc)
        return None

    def get_density(self, address: str) -> float:
        """Return population density (people/km²) for the postcode in address."""
        data = self.get_postcode_data(address)
        if data:
            return float(data["density"])
        return self._city_avg_density

    def estimate_catchment_population(
        self,
        lat: float,
        lng: float,
        radius_m: float,
        address: str = "",
    ) -> Tuple[float, float]:
        """Estimate population within a circular catchment.

        Returns:
            (estimated_population, density_per_km2)
        """
        data = self.get_postcode_data(address) if address else None

        if data:
            population = data["population"]
            area_km2 = data["area_km2"]
            density = data["density"]
        else:
            density = self._city_avg_density
            area_km2 = 1.0
            population = density

        # Circle area in km²
        circle_area_km2 = math.pi * (radius_m / 1000) ** 2
        # Scale postcode population by overlap ratio
        overlap_ratio = min(circle_area_km2 / area_km2, 1.0)
        estimated_pop = population * overlap_ratio

        return estimated_pop, density
