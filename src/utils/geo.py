"""Geographic utility functions.

Ported from GreenspaceFinder's geo.py — haversine distance and bounding box helpers.
"""

import math
from typing import Tuple


EARTH_RADIUS_M = 6_371_000


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return distance in meters between two lat/lng points."""
    lat1, lng1, lat2, lng2 = (math.radians(v) for v in (lat1, lng1, lat2, lng2))
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def bounding_box(lat: float, lng: float, radius_m: float) -> Tuple[float, float, float, float]:
    """Return (south, west, north, east) bounding box for a circle.

    Useful for pre-filtering before exact haversine checks.
    """
    lat_delta = math.degrees(radius_m / EARTH_RADIUS_M)
    lng_delta = math.degrees(radius_m / (EARTH_RADIUS_M * math.cos(math.radians(lat))))
    return (lat - lat_delta, lng - lng_delta, lat + lat_delta, lng + lng_delta)
