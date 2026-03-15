"""Crime data service.

Looks up district-level crime index for a given location (address or coordinates).
Modeled after GreenspaceFinder's crime.py — address → postcode → Bezirk → crime_index.
"""

import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, Optional

from ..utils.geo import haversine

_DATA_PATH = Path(__file__).parent / "data" / "berlin_crime.json"

# District centroids for fallback nearest-match
_DISTRICT_CENTROIDS = {
    "Mitte": (52.5200, 13.4050),
    "Friedrichshain-Kreuzberg": (52.5013, 13.4206),
    "Pankow": (52.5726, 13.4021),
    "Charlottenburg-Wilmersdorf": (52.5065, 13.2846),
    "Spandau": (52.5346, 13.1954),
    "Steglitz-Zehlendorf": (52.4344, 13.2416),
    "Tempelhof-Schöneberg": (52.4663, 13.3814),
    "Neukölln": (52.4393, 13.4436),
    "Treptow-Köpenick": (52.4432, 13.5691),
    "Marzahn-Hellersdorf": (52.5363, 13.5777),
    "Lichtenberg": (52.5225, 13.5013),
    "Reinickendorf": (52.5767, 13.3333),
}


class CrimeService:
    """Berlin crime data lookup."""

    def __init__(self) -> None:
        with open(_DATA_PATH, encoding="utf-8") as f:
            data = json.load(f)
        self._districts: Dict[str, dict] = data["districts"]
        self._postcode_map: Dict[str, str] = data["postcode_to_district"]

    def _normalize(self, text: str) -> str:
        """Normalize unicode for comparison (ö → oe etc.)."""
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

    def _district_from_address(self, address: str) -> Optional[str]:
        """Try to extract Bezirk name directly from an address string."""
        addr_norm = self._normalize(address)
        for district in self._districts:
            if self._normalize(district) in addr_norm:
                return district
        return None

    def _district_from_postcode(self, address: str) -> Optional[str]:
        """Extract 5-digit Berlin postcode and map to district."""
        match = re.search(r"\b(1\d{4})\b", address)
        if match:
            return self._postcode_map.get(match.group(1))
        return None

    def _nearest_district(self, lat: float, lng: float) -> str:
        """Fallback: find nearest district centroid by haversine."""
        best, best_dist = "Mitte", float("inf")
        for district, (clat, clng) in _DISTRICT_CENTROIDS.items():
            d = haversine(lat, lng, clat, clng)
            if d < best_dist:
                best, best_dist = district, d
        return best

    def get_crime_index(
        self,
        lat: float,
        lng: float,
        address: str = "",
    ) -> float:
        """Return the crime index for a location.

        Lookup chain: address string → postcode → nearest centroid.
        """
        district = None
        if address:
            district = self._district_from_address(address)
            if not district:
                district = self._district_from_postcode(address)
        if not district:
            district = self._nearest_district(lat, lng)

        info = self._districts.get(district, {})
        return float(info.get("crime_index", 100))
