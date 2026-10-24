#!/usr/bin/env python3
"""
NL Primary Phase 2: geocode basisonderwijs addresses.

Reuses the VO geocoder (PDOK Locatieserver, Nominatim fallback) and its shared
cache, so schools that share a postcode with a secondary school resolve for
free. At 6k schools the PDOK path matters: Nominatim's 1 req/s cap would make
this a ~1.7 hour job.

Input:  data_nl_po/intermediate/nl_po_school_master_base.csv
Output: data_nl_po/intermediate/nl_po_school_master_geocoded.csv
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_international.nl.scrapers.geocode_schools import geocode_address  # noqa: E402

DATA_DIR = PROJECT_ROOT / "data_nl_po"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
# Shared with the VO run — same address space, so hits carry across.
CACHE_FILE = PROJECT_ROOT / "data_nl" / "cache" / "geocode_cache.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main(force: bool = False):
    logger.info("=" * 60)
    logger.info("NL Primary Phase 2: Geocoding")
    logger.info("=" * 60)

    input_path = INTERMEDIATE_DIR / "nl_school_master_base.csv"
    if not input_path.exists():
        logger.error(f"Input not found: {input_path}")
        sys.exit(1)

    df = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(df)} primary schools")

    cache = {}
    if CACHE_FILE.exists() and not force:
        cache = json.loads(CACHE_FILE.read_text())
        logger.info(f"Cache: {len(cache)} existing entries")

    lats, lons = [], []
    cached = geocoded = failed = 0
    for i, row in enumerate(df.itertuples(), 1):
        key = f"{row.postal_code}|{row.street_address}|{row.city}"
        was_cached = key in cache
        lat, lon = geocode_address(
            str(getattr(row, "street_address", "") or ""),
            str(getattr(row, "postal_code", "") or ""),
            str(getattr(row, "city", "") or ""),
            cache,
        )
        lats.append(lat)
        lons.append(lon)
        if lat is None:
            failed += 1
        elif was_cached:
            cached += 1
        else:
            geocoded += 1
        if i % 500 == 0:
            logger.info(f"  Progress: {i}/{len(df)} "
                        f"(cached: {cached}, geocoded: {geocoded}, failed: {failed})")
            CACHE_FILE.write_text(json.dumps(cache))

    df["latitude"] = lats
    df["longitude"] = lons
    CACHE_FILE.write_text(json.dumps(cache))

    output = INTERMEDIATE_DIR / "nl_school_master_geocoded.csv"
    df.to_csv(output, index=False)

    have = df["latitude"].notna().sum()
    logger.info(f"\n  With coordinates: {have}/{len(df)} ({100 * have / len(df):.1f}%)")
    logger.info(f"  From cache: {cached} | newly geocoded: {geocoded} | failed: {failed}")
    logger.info(f"  Saved: {output}")
    if have == 0:
        raise RuntimeError("Geocoding produced no coordinates — refusing to report success")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    main(force=args.force)
