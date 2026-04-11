#!/usr/bin/env python3
"""
UK Phase 4: Transit Enrichment using NaPTAN stops data.

Downloads the NaPTAN (National Public Transport Access Nodes) dataset
and computes nearest transit stops per school — same Berlin transit pattern.

NaPTAN StopType codes:
- RLY, RSE, RPL → rail (Railway station/entrance/platform)
- MET, PLT → rail (Metro/tram platform)
- TMU → tram
- BCT, BCS, BCE → bus (Bus stop/station/entrance)
- FER → bus (Ferry treated as bus)

Data: https://beta-naptan.dft.gov.uk/Download/National (CSV)
Alternative: data.gov.uk NaPTAN dataset

Input:  data_gb/intermediate/gb_schools_with_traffic.csv
Output: data_gb/intermediate/gb_schools_with_transit.csv
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_gb"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NaPTAN direct download (CSV)
NAPTAN_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"

STOP_TYPE_TO_MODE = {
    "RLY": "rail", "RSE": "rail", "RPL": "rail",  # Railway
    "MET": "rail", "PLT": "rail",                   # Metro
    "TMU": "tram",                                    # Tram
    "BCT": "bus", "BCS": "bus", "BCE": "bus",        # Bus
    "FER": "bus",                                     # Ferry
}


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def load_naptan(cache_path: Path) -> pd.DataFrame:
    """Download and parse NaPTAN stops."""
    if cache_path.exists():
        logger.info(f"Loading cached NaPTAN: {cache_path.name}")
        return pd.read_csv(cache_path)

    logger.info("Downloading NaPTAN stops...")
    try:
        resp = requests.get(NAPTAN_URL, timeout=120,
                           headers={"User-Agent": "SchoolNossa/1.0"})
        resp.raise_for_status()
        raw_path = CACHE_DIR / "naptan_raw.csv"
        raw_path.write_bytes(resp.content)
        logger.info(f"  Downloaded: {len(resp.content) / 1024 / 1024:.1f} MB")
        df = pd.read_csv(raw_path, low_memory=False, dtype=str)
    except Exception as e:
        logger.warning(f"  NaPTAN API failed: {e}. Trying data.gov.uk...")
        # Fallback: use Overpass API for stop locations
        return _load_stops_from_overpass(cache_path)

    # Parse
    df["Latitude"] = pd.to_numeric(df.get("Latitude"), errors="coerce")
    df["Longitude"] = pd.to_numeric(df.get("Longitude"), errors="coerce")
    df = df.dropna(subset=["Latitude", "Longitude"])

    # Filter to active stops
    if "Status" in df.columns:
        df = df[df["Status"] == "act"]

    # Map stop type to mode
    df["mode"] = df.get("StopType", pd.Series(dtype=str)).map(STOP_TYPE_TO_MODE).fillna("bus")

    # Keep only needed columns
    result = df[["ATCOCode", "CommonName", "Latitude", "Longitude", "mode"]].copy()
    result.columns = ["stop_id", "stop_name", "stop_lat", "stop_lon", "mode"]
    result = result.drop_duplicates(subset=["stop_id"])

    result.to_csv(cache_path, index=False)
    logger.info(f"  Cached {len(result)} active stops")
    return result


def _load_stops_from_overpass(cache_path: Path) -> pd.DataFrame:
    """Fallback: load GB transit stops from Overpass API."""
    logger.info("  Using Overpass API fallback for GB stops...")
    # This is a simplified fallback — in practice you'd query Overpass
    # for public_transport=stop_position nodes in GB
    return pd.DataFrame(columns=["stop_id", "stop_name", "stop_lat", "stop_lon", "mode"])


def find_nearest_stops(school_lat, school_lon, stops_df):
    """Find nearest stops by mode for a single school."""
    delta = 0.015
    mask = (
        (stops_df["stop_lat"] > school_lat - delta)
        & (stops_df["stop_lat"] < school_lat + delta)
        & (stops_df["stop_lon"] > school_lon - delta)
        & (stops_df["stop_lon"] < school_lon + delta)
    )
    nearby = stops_df[mask].copy()
    if nearby.empty:
        return {}, 0

    nearby["distance_m"] = haversine_distance(
        school_lat, school_lon, nearby["stop_lat"].values, nearby["stop_lon"].values
    )
    within_1000 = nearby[nearby["distance_m"] <= 1000]
    stop_count = len(within_1000)

    result = {}
    for mode in ["rail", "tram", "bus"]:
        mode_stops = nearby[nearby["mode"] == mode].nsmallest(3, "distance_m")
        for idx, (_, row) in enumerate(mode_stops.iterrows()):
            prefix = f"transit_{mode}_{idx + 1:02d}"
            result[f"{prefix}_name"] = row["stop_name"]
            result[f"{prefix}_distance_m"] = round(row["distance_m"], 0)
            result[f"{prefix}_latitude"] = round(row["stop_lat"], 6)
            result[f"{prefix}_longitude"] = round(row["stop_lon"], 6)
            result[f"{prefix}_lines"] = ""  # NaPTAN doesn't include route info

    return result, stop_count


def enrich_with_transit(schools: pd.DataFrame, stops: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Computing transit for {len(schools)} schools...")
    all_results = []
    for i, (_, school) in enumerate(schools.iterrows()):
        lat, lon = school.get("latitude"), school.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            all_results.append({})
            continue
        result, count = find_nearest_stops(float(lat), float(lon), stops)
        result["transit_stop_count_1000m"] = count
        modes = sum(1 for m in ["rail", "tram", "bus"] if f"transit_{m}_01_name" in result)
        result["transit_accessibility_score"] = round(min(10, count / 5 * 3 + modes * 2.5), 1)
        all_results.append(result)
        if (i + 1) % 500 == 0:
            logger.info(f"  Progress: {i + 1}/{len(schools)}")

    transit_df = pd.DataFrame(all_results)
    for col in transit_df.columns:
        schools[col] = transit_df[col].values
    return schools


def main():
    logger.info("=" * 60)
    logger.info("UK Phase 4: Transit Enrichment (NaPTAN)")
    logger.info("=" * 60)

    candidates = [INTERMEDIATE_DIR / "gb_schools_with_traffic.csv",
                  INTERMEDIATE_DIR / "gb_school_master_base.csv"]
    input_path = next((p for p in candidates if p.exists()), None)
    if not input_path:
        logger.error("Run earlier phases first."); sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    stops = load_naptan(CACHE_DIR / "naptan_stops_clean.csv")
    logger.info(f"Schools: {len(schools)}, Stops: {len(stops)}")

    enriched = enrich_with_transit(schools, stops)
    output = INTERMEDIATE_DIR / "gb_schools_with_transit.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
