#!/usr/bin/env python3
"""
NL POI enrichment via Overpass (OpenStreetMap) — the free alternative to
Google Places.

Produces the same 81 Berlin-schema POI columns as the Places-based enricher:
6 categories x (count within 500m + the 3 nearest with name/address/distance/
lat/lon). Drop-in replacement, so the two are interchangeable per country.

Why this shape rather than per-school queries: 6,060 primary schools would mean
6,060 Overpass calls, which is both slow and abusive of a donated service. The
Netherlands is small enough to pull each POI category once nationwide (~10
queries total), cache it, and do the distance work locally against a 1km grid
index — the same approach as the BRON accident enrichment.

School categories deliberately do NOT come from OSM: we already hold the
authoritative DUO registry of every funded primary and secondary school with
coordinates, which is more complete and better named than OSM's amenity=school,
and OSM rarely distinguishes primary from secondary in NL.

Costs nothing. Google Places would be ~$0.154/school (~$932 for primary alone).

Usage:
    NL_DATA_DIR=data_nl_po python3 .../nl_poi_overpass_enrichment.py
"""

import json
import logging
import math
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

NL_DATA_DIR = os.environ.get("NL_DATA_DIR", "data_nl")
DATA_DIR = PROJECT_ROOT / NL_DATA_DIR
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
# POI extracts are national, so both school levels share one download.
SHARED_CACHE_DIR = PROJECT_ROOT / "data_nl" / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
USER_AGENT = "SchoolNossa/1.0 (school comparison platform; contact@schoolnossa.com)"

# Netherlands bounding box (S, W, N, E) — includes the Wadden islands.
NL_BBOX = (50.70, 3.20, 53.60, 7.25)

# Berlin POI category -> the OSM selectors that populate it.
OSM_CATEGORIES = {
    "supermarket": ['node["shop"="supermarket"]', 'way["shop"="supermarket"]'],
    "restaurant": ['node["amenity"="restaurant"]', 'way["amenity"="restaurant"]'],
    "bakery_cafe": ['node["shop"="bakery"]', 'way["shop"="bakery"]',
                    'node["amenity"="cafe"]', 'way["amenity"="cafe"]'],
    "kita": ['node["amenity"="kindergarten"]', 'way["amenity"="kindergarten"]'],
}
# primary_school / secondary_school come from our own DUO registries instead.
SCHOOL_CATEGORIES = {
    "primary_school": "data_nl_po/intermediate/nl_school_master_geocoded.csv",
    "secondary_school": "data_nl/intermediate/nl_school_master_geocoded.csv",
}

RADIUS_M = 500
NEAREST_N = 3
EARTH_R = 6371000.0


def _overpass_query(selectors: list[str], timeout: int = 600) -> list[dict]:
    s, w, n, e = NL_BBOX
    body = "".join(f"{sel}({s},{w},{n},{e});" for sel in selectors)
    query = f"[out:json][timeout:{timeout}];({body});out center tags;"

    last_error = None
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            logger.info(f"    querying {endpoint.split('/')[2]}...")
            resp = requests.post(endpoint, data={"data": query},
                                 headers={"User-Agent": USER_AGENT}, timeout=timeout + 60)
            resp.raise_for_status()
            return resp.json().get("elements", [])
        except Exception as exc:  # try the mirror before giving up
            last_error = exc
            logger.warning(f"    {endpoint.split('/')[2]} failed: {exc}")
            time.sleep(5)
    raise RuntimeError(f"All Overpass endpoints failed: {last_error}")


def _address_from_tags(tags: dict) -> str:
    street = tags.get("addr:street", "")
    number = tags.get("addr:housenumber", "")
    city = tags.get("addr:city", "")
    postcode = tags.get("addr:postcode", "")
    line = " ".join(p for p in (street, number) if p).strip()
    tail = " ".join(p for p in (postcode, city) if p).strip()
    return ", ".join(p for p in (line, tail) if p)


def fetch_category(name: str, selectors: list[str]) -> pd.DataFrame:
    """One national extract per category, cached to disk."""
    cache = SHARED_CACHE_DIR / f"overpass_nl_{name}.json"
    if cache.exists() and cache.stat().st_size > 100:
        elements = json.loads(cache.read_text())
        logger.info(f"  {name}: {len(elements)} from cache")
    else:
        logger.info(f"  {name}: downloading from Overpass...")
        elements = _overpass_query(selectors)
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(elements))
        logger.info(f"  {name}: {len(elements)} elements")
        time.sleep(3)  # be a good citizen between category queries

    rows = []
    for el in elements:
        # Nodes carry lat/lon; ways carry a computed 'center' from `out center`.
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags") or {}
        rows.append({
            "name": tags.get("name") or tags.get("operator") or "",
            "address": _address_from_tags(tags),
            "latitude": float(lat),
            "longitude": float(lon),
        })
    df = pd.DataFrame(rows)
    named = int((df["name"].astype(str).str.strip() != "").sum()) if len(df) else 0
    logger.info(f"  {name}: {len(df)} geocoded ({named} named)")
    return df


def load_school_category(name: str, rel_path: str) -> pd.DataFrame:
    """Schools as POIs, from our own registry rather than OSM."""
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        logger.warning(f"  {name}: {rel_path} not found — category left empty")
        return pd.DataFrame(columns=["name", "address", "latitude", "longitude"])
    df = pd.read_csv(path, low_memory=False)
    out = pd.DataFrame({
        "name": df.get("school_name", pd.Series(dtype=str)),
        "address": (df.get("street_address", pd.Series(dtype=str)).fillna("").astype(str)
                    + ", " + df.get("city", pd.Series(dtype=str)).fillna("").astype(str)).str.strip(", "),
        "latitude": pd.to_numeric(df.get("latitude"), errors="coerce"),
        "longitude": pd.to_numeric(df.get("longitude"), errors="coerce"),
    }).dropna(subset=["latitude", "longitude"])
    logger.info(f"  {name}: {len(out)} from DUO registry")
    return out


def _grid_index(lats: np.ndarray, lons: np.ndarray, cell_deg: float):
    grid: dict[tuple[int, int], list[int]] = {}
    for idx, (la, lo) in enumerate(zip(lats, lons)):
        grid.setdefault((int(la // cell_deg), int(lo // cell_deg)), []).append(idx)
    return grid


def enrich(schools: pd.DataFrame, catalogs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    lat0 = math.radians(float(pd.to_numeric(schools["latitude"], errors="coerce").median()))
    m_per_deg_lat = 111132.0
    m_per_deg_lon = 111320.0 * math.cos(lat0)
    # ~1.1km cells, so a 500m radius is always inside the 3x3 neighbourhood.
    cell_deg = 0.01

    school_lat = pd.to_numeric(schools["latitude"], errors="coerce").to_numpy()
    school_lon = pd.to_numeric(schools["longitude"], errors="coerce").to_numpy()

    for cat, cat_df in catalogs.items():
        counts = np.full(len(schools), np.nan)
        nearest = {i: {"name": [None] * len(schools), "address": [None] * len(schools),
                       "distance_m": [None] * len(schools),
                       "latitude": [None] * len(schools), "longitude": [None] * len(schools)}
                   for i in range(1, NEAREST_N + 1)}

        if cat_df.empty:
            logger.warning(f"  {cat}: catalog empty — skipping")
        else:
            plat = cat_df["latitude"].to_numpy()
            plon = cat_df["longitude"].to_numpy()
            pname = cat_df["name"].fillna("").to_numpy()
            paddr = cat_df["address"].fillna("").to_numpy()
            grid = _grid_index(plat, plon, cell_deg)

            for i, (sla, slo) in enumerate(zip(school_lat, school_lon)):
                if not (np.isfinite(sla) and np.isfinite(slo)):
                    continue
                gx, gy = int(sla // cell_deg), int(slo // cell_deg)
                near: list[int] = []
                for dx in (-1, 0, 1):
                    for dy in (-1, 0, 1):
                        near.extend(grid.get((gx + dx, gy + dy), ()))
                if not near:
                    counts[i] = 0
                    continue
                idx = np.asarray(near)
                dy_m = (plat[idx] - sla) * m_per_deg_lat
                dx_m = (plon[idx] - slo) * m_per_deg_lon
                dist = np.hypot(dx_m, dy_m)
                counts[i] = int(np.sum(dist <= RADIUS_M))
                for rank, j in enumerate(np.argsort(dist)[:NEAREST_N], start=1):
                    slot = nearest[rank]
                    slot["name"][i] = str(pname[idx[j]]) or None
                    slot["address"][i] = str(paddr[idx[j]]) or None
                    slot["distance_m"][i] = round(float(dist[j]), 1)
                    slot["latitude"][i] = float(plat[idx[j]])
                    slot["longitude"][i] = float(plon[idx[j]])

        schools[f"poi_{cat}_count_500m"] = counts
        # secondary_school carries only a count in the Berlin schema.
        if cat != "secondary_school":
            for rank in range(1, NEAREST_N + 1):
                for field, values in nearest[rank].items():
                    schools[f"poi_{cat}_{rank:02d}_{field}"] = values

        have = int(np.isfinite(counts).sum())
        med = np.nanmedian(counts) if have else float("nan")
        logger.info(f"  {cat}: {have}/{len(schools)} schools, median {med:.0f} within {RADIUS_M}m")

    return schools


def main():
    logger.info("=" * 60)
    logger.info(f"NL POI enrichment via Overpass/OSM ({NL_DATA_DIR})")
    logger.info("=" * 60)

    for candidate in ("nl_schools_with_quality.csv", "nl_schools_with_demographics.csv",
                      "nl_schools_with_crime.csv", "nl_school_master_geocoded.csv"):
        input_path = INTERMEDIATE_DIR / candidate
        if input_path.exists():
            break
    else:
        logger.error("No input intermediate found")
        sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    SHARED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    catalogs = {}
    for name, selectors in OSM_CATEGORIES.items():
        catalogs[name] = fetch_category(name, selectors)
    for name, rel_path in SCHOOL_CATEGORIES.items():
        catalogs[name] = load_school_category(name, rel_path)

    total_pois = sum(len(c) for c in catalogs.values())
    if total_pois == 0:
        raise RuntimeError("No POIs fetched — refusing to write empty columns")
    logger.info(f"\nCatalogs: {total_pois:,} POIs across {len(catalogs)} categories")

    enriched = enrich(schools, catalogs)
    enriched["poi_data_source"] = "OpenStreetMap via Overpass (ODbL) + DUO registry"

    output = INTERMEDIATE_DIR / "nl_schools_with_pois.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"\nSaved: {output}")


if __name__ == "__main__":
    main()
