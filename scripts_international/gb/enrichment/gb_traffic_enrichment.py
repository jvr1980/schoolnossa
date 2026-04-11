#!/usr/bin/env python3
"""
UK Phase 3: Traffic/Road Safety Enrichment using STATS19 data.

Downloads geocoded road accident data from DfT and computes accident
density near each school — same pattern as NL traffic enrichment.

Data source: https://data.dft.gov.uk/road-accidents-safety-data/
Format: CSV with latitude, longitude, accident_severity

Input:  data_gb/intermediate/gb_school_master_base.csv
Output: data_gb/intermediate/gb_schools_with_traffic.csv
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
RAW_DIR = DATA_DIR / "raw"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STATS19_URL = "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-collision-last-5-years.csv"


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def load_accidents(cache_path: Path) -> pd.DataFrame:
    """Load STATS19 accident data."""
    if cache_path.exists():
        logger.info(f"Loading cached accidents: {cache_path.name}")
        df = pd.read_csv(cache_path, usecols=["latitude", "longitude", "accident_severity"],
                         low_memory=False)
        return df.dropna(subset=["latitude", "longitude"])

    # Check if already downloaded in raw
    raw_path = RAW_DIR / "gb_accidents.csv"
    if raw_path.exists():
        logger.info(f"Loading from raw: {raw_path.name}")
        df = pd.read_csv(raw_path, low_memory=False)
    else:
        logger.info(f"Downloading STATS19: {STATS19_URL[:60]}...")
        resp = requests.get(STATS19_URL, timeout=120)
        resp.raise_for_status()
        raw_path.write_bytes(resp.content)
        logger.info(f"  Saved: {raw_path.name} ({len(resp.content) / 1024 / 1024:.1f} MB)")
        df = pd.read_csv(raw_path, low_memory=False)

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    # Cache slim version
    df[["latitude", "longitude", "accident_severity"]].to_csv(cache_path, index=False)
    logger.info(f"  Cached {len(df)} geocoded accidents")
    return df


def enrich_with_traffic(schools: pd.DataFrame, accidents: pd.DataFrame) -> pd.DataFrame:
    """Compute accident density for each school."""
    logger.info(f"Computing traffic metrics for {len(schools)} schools...")

    acc_lats = accidents["latitude"].values
    acc_lons = accidents["longitude"].values
    acc_fatal = (accidents["accident_severity"] == 1).values  # 1=Fatal

    results = []
    for i, (_, school) in enumerate(schools.iterrows()):
        lat, lon = school.get("latitude"), school.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            results.append({})
            continue

        distances = haversine_distance(float(lat), float(lon), acc_lats, acc_lons)
        results.append({
            "traffic_accidents_500m": int(np.sum(distances <= 500)),
            "traffic_accidents_1000m": int(np.sum(distances <= 1000)),
            "traffic_accidents_fatal_1000m": int(np.sum((distances <= 1000) & acc_fatal)),
        })

        if (i + 1) % 500 == 0:
            logger.info(f"  Progress: {i + 1}/{len(schools)}")

    traffic_df = pd.DataFrame(results)
    max_acc = traffic_df["traffic_accidents_1000m"].max()
    if max_acc and max_acc > 0:
        traffic_df["traffic_volume_index"] = (traffic_df["traffic_accidents_1000m"] / max_acc * 10).round(1)

    for col in traffic_df.columns:
        schools[col] = traffic_df[col].values

    schools["traffic_data_source"] = "STATS19 (DfT)"
    return schools


def main():
    logger.info("=" * 60)
    logger.info("UK Phase 3: Traffic Enrichment (STATS19)")
    logger.info("=" * 60)

    input_path = INTERMEDIATE_DIR / "gb_school_master_base.csv"
    if not input_path.exists():
        logger.error("Run Phase 1 first."); sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    accidents = load_accidents(CACHE_DIR / "stats19_slim.csv")
    logger.info(f"Schools: {len(schools)}, Accidents: {len(accidents)}")

    enriched = enrich_with_traffic(schools, accidents)
    output = INTERMEDIATE_DIR / "gb_schools_with_traffic.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
