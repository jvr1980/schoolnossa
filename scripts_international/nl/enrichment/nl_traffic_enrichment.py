#!/usr/bin/env python3
"""
NL Phase 3: Traffic/Road Safety Enrichment

Uses BRON (Bestand geRegistreerde Ongevallen in Nederland) accident data
to compute road accident density near each school.

Data source: Rijkswaterstaat via data.overheid.nl
Format: CSV with lat/lon per accident

Computes:
- traffic_accidents_500m: accident count within 500m of school
- traffic_accidents_1000m: accident count within 1000m
- traffic_accidents_fatal_1000m: fatal accidents within 1000m
- traffic_volume_index: normalized 0-10 score based on accident density

Input:  data_nl/intermediate/nl_school_master_geocoded.csv
        data_nl/cache/bron_accidents.csv (downloaded or cached)
Output: data_nl/intermediate/nl_schools_with_traffic.csv
"""

import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# BRON data URLs — Rijkswaterstaat publishes annual accident data
# The WFS endpoint provides geocoded accident records
BRON_WFS_URL = "https://geodata.rijkswaterstaat.nl/services/ogc/gdr/v1_0/collections/registratie_ongevallen/items"
HEADERS = {"User-Agent": "SchoolNossa/1.0"}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Compute haversine distance in meters between two points."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def download_bron_accidents(cache_path: Path, force: bool = False) -> pd.DataFrame:
    """
    Download BRON accident data via WFS API.
    Falls back to a simplified approach using paginated requests.
    """
    if cache_path.exists() and not force:
        logger.info(f"Loading cached accident data: {cache_path.name}")
        return pd.read_csv(cache_path)

    logger.info("Downloading BRON accident data from Rijkswaterstaat WFS...")
    all_records = []
    offset = 0
    limit = 1000
    max_records = 50000  # Safety limit

    while offset < max_records:
        params = {
            "f": "json",
            "limit": limit,
            "offset": offset,
        }
        try:
            resp = requests.get(BRON_WFS_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                break

            for f in features:
                props = f.get("properties", {})
                geom = f.get("geometry", {})
                coords = geom.get("coordinates", [None, None]) if geom else [None, None]
                all_records.append({
                    "accident_id": props.get("registratienummer"),
                    "year": props.get("jaar"),
                    "month": props.get("maand"),
                    "severity": props.get("ernst"),  # UMS = fatal, LET = injury
                    "longitude": coords[0] if coords else None,
                    "latitude": coords[1] if coords else None,
                })

            offset += limit
            logger.info(f"  Downloaded {len(all_records)} accident records...")
            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"  WFS request failed at offset {offset}: {e}")
            break

    if not all_records:
        logger.warning("  No accident records downloaded. WFS may be unavailable.")
        logger.info("  Creating empty accident dataframe.")
        return pd.DataFrame(columns=["accident_id", "year", "month", "severity", "longitude", "latitude"])

    df = pd.DataFrame(all_records)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])

    df.to_csv(cache_path, index=False)
    logger.info(f"  Cached {len(df)} geocoded accidents to {cache_path.name}")
    return df


def enrich_schools_with_traffic(schools: pd.DataFrame, accidents: pd.DataFrame) -> pd.DataFrame:
    """Compute accident density metrics for each school."""
    logger.info(f"Computing traffic metrics for {len(schools)} schools...")

    results = []
    school_lats = schools["latitude"].values.astype(float)
    school_lons = schools["longitude"].values.astype(float)

    if accidents.empty or len(accidents) == 0:
        logger.warning("No accident data available — filling traffic columns with NaN")
        schools["traffic_accidents_500m"] = np.nan
        schools["traffic_accidents_1000m"] = np.nan
        schools["traffic_accidents_fatal_1000m"] = np.nan
        schools["traffic_volume_index"] = np.nan
        schools["traffic_data_source"] = "BRON (no data available)"
        return schools

    acc_lats = accidents["latitude"].values.astype(float)
    acc_lons = accidents["longitude"].values.astype(float)
    acc_fatal = (accidents["severity"].str.upper() == "UMS").values if "severity" in accidents.columns else np.zeros(len(accidents), dtype=bool)

    for i in range(len(schools)):
        if np.isnan(school_lats[i]) or np.isnan(school_lons[i]):
            results.append({
                "traffic_accidents_500m": np.nan,
                "traffic_accidents_1000m": np.nan,
                "traffic_accidents_fatal_1000m": np.nan,
            })
            continue

        distances = haversine_distance(school_lats[i], school_lons[i], acc_lats, acc_lons)

        results.append({
            "traffic_accidents_500m": int(np.sum(distances <= 500)),
            "traffic_accidents_1000m": int(np.sum(distances <= 1000)),
            "traffic_accidents_fatal_1000m": int(np.sum((distances <= 1000) & acc_fatal)),
        })

        if (i + 1) % 200 == 0:
            logger.info(f"  Progress: {i + 1}/{len(schools)}")

    traffic_df = pd.DataFrame(results)

    # Normalize to 0-10 traffic volume index
    max_acc = traffic_df["traffic_accidents_1000m"].max()
    if max_acc and max_acc > 0:
        traffic_df["traffic_volume_index"] = (
            traffic_df["traffic_accidents_1000m"] / max_acc * 10
        ).round(1)
    else:
        traffic_df["traffic_volume_index"] = 0.0

    for col in traffic_df.columns:
        schools[col] = traffic_df[col].values

    schools["traffic_data_source"] = "BRON (Rijkswaterstaat)"
    schools["traffic_accidents_year"] = str(accidents["year"].max()) if "year" in accidents.columns and not accidents.empty else "unknown"

    return schools


def main():
    """Run traffic enrichment."""
    logger.info("=" * 60)
    logger.info("NL Phase 3: Traffic/Road Safety Enrichment")
    logger.info("=" * 60)

    # Load schools
    input_path = INTERMEDIATE_DIR / "nl_school_master_geocoded.csv"
    if not input_path.exists():
        logger.error(f"Input not found: {input_path}. Run phases 1-2 first.")
        sys.exit(1)

    schools = pd.read_csv(input_path)
    logger.info(f"Loaded {len(schools)} schools")

    # Download/load accidents
    accidents = download_bron_accidents(CACHE_DIR / "bron_accidents.csv")
    logger.info(f"Accident records: {len(accidents)}")

    # Enrich
    enriched = enrich_schools_with_traffic(schools, accidents)

    # Save
    output_path = INTERMEDIATE_DIR / "nl_schools_with_traffic.csv"
    enriched.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")

    # Stats
    has_data = enriched["traffic_accidents_1000m"].notna().sum()
    logger.info(f"Schools with traffic data: {has_data}/{len(enriched)}")


if __name__ == "__main__":
    main()
