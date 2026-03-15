"""Google Places API client for POI dimension queries.

Rate-limited async client modeled after GreenspaceFinder's google_places.py.
Uses semaphore-based concurrency control, retry with backoff, and result caching.
"""

import asyncio
import os
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from ..utils.geo import haversine

logger = logging.getLogger(__name__)

_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
_NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"
_MAX_CONCURRENT = 5
_MIN_DELAY = 0.1
_MAX_RETRIES = 3


class GooglePlacesClient:
    """Async Google Places client with rate limiting and caching."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._api_key = api_key or _API_KEY
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
        self._cache: Dict[str, Any] = {}

    def _cache_key(self, lat: float, lng: float, place_type: str, radius: float) -> str:
        return f"{lat:.5f},{lng:.5f},{place_type},{radius:.0f}"

    async def nearby_search(
        self,
        lat: float,
        lng: float,
        radius_m: float,
        place_type: str,
        max_results: int = 20,
    ) -> List[Dict[str, Any]]:
        """Search for places of a given type near a location.

        Returns list of place dicts with at least 'lat', 'lng', 'name'.
        Falls back to empty list if API key is not configured.
        """
        if not self._api_key:
            logger.debug("Google Places API key not configured, returning empty results")
            return []

        ck = self._cache_key(lat, lng, place_type, radius_m)
        if ck in self._cache:
            return self._cache[ck]

        body = {
            "includedTypes": [place_type],
            "maxResultCount": min(max_results, 20),
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": radius_m,
                }
            },
        }
        headers = {
            "X-Goog-Api-Key": self._api_key,
            "X-Goog-FieldMask": "places.displayName,places.location,places.types",
        }

        for attempt in range(1, _MAX_RETRIES + 1):
            async with self._semaphore:
                await asyncio.sleep(_MIN_DELAY)
                try:
                    async with httpx.AsyncClient(timeout=15.0) as client:
                        resp = await client.post(_NEARBY_URL, json=body, headers=headers)
                        if resp.status_code in (429, 500, 503):
                            wait = 0.5 * (2 ** (attempt - 1))
                            logger.warning(f"Google API {resp.status_code}, retrying in {wait}s")
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        data = resp.json()
                except httpx.HTTPError as e:
                    logger.error(f"Google Places request failed: {e}")
                    if attempt == _MAX_RETRIES:
                        return []
                    continue

            places = []
            for p in data.get("places", []):
                loc = p.get("location", {})
                places.append({
                    "lat": loc.get("latitude"),
                    "lng": loc.get("longitude"),
                    "name": p.get("displayName", {}).get("text", ""),
                    "types": p.get("types", []),
                })
            self._cache[ck] = places
            return places

        return []

    async def count_nearby(
        self,
        lat: float,
        lng: float,
        radius_m: float,
        place_type: str,
    ) -> Tuple[int, Optional[float]]:
        """Count places of a type and return (count, nearest_distance_m).

        Returns (0, None) if no results.
        """
        places = await self.nearby_search(lat, lng, radius_m, place_type)
        if not places:
            return 0, None

        nearest = min(
            haversine(lat, lng, p["lat"], p["lng"])
            for p in places
            if p.get("lat") is not None
        )
        return len(places), nearest

    def count_nearby_sync(
        self,
        lat: float,
        lng: float,
        radius_m: float,
        place_type: str,
    ) -> Tuple[int, Optional[float]]:
        """Synchronous wrapper for count_nearby."""
        return asyncio.run(self.count_nearby(lat, lng, radius_m, place_type))
