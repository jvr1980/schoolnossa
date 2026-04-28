#!/usr/bin/env python3
"""
NL Phase 6: POI Enrichment via Google Places API (New).

For each school, fetches nearby POIs (supermarket, restaurant, bakery/cafe,
kita/preschool, primary school, secondary school) within 500m and stores
counts + top-3 nearest with distance/coords.

Delegates per-school work to scripts_shared.enrichment.enrich_schools_with_pois.enrich_school
to keep parity with Berlin (and future cities). Per-school JSON cache so
re-runs are free.

Input:  data_nl/intermediate/nl_schools_with_crime.csv (fallback chain below)
Output: data_nl/intermediate/nl_schools_with_pois.csv
"""

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache" / "poi_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAX_WORKERS = 5


def _find_input() -> Path:
    """Prefer the most-enriched available intermediate so POI output carries
    all prior enrichments forward."""
    for name in [
        "nl_schools_with_demographics.csv",
        "nl_schools_with_crime.csv",
        "nl_schools_with_transit.csv",
        "nl_schools_with_traffic.csv",
        "nl_school_master_geocoded.csv",
    ]:
        p = INTERMEDIATE_DIR / name
        if p.exists():
            return p
    raise FileNotFoundError("No NL intermediate found. Run earlier phases first.")


def _cache_key(row: pd.Series, idx: int) -> str:
    """Stable per-school cache key. Prefer vestiging_code, then brin_code, then idx."""
    for col in ("vestiging_code", "brin_code"):
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            return str(val).strip().replace("/", "_")
    return f"idx_{idx}"


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 6: POI Enrichment (Google Places API)")
    logger.info("=" * 60)

    load_dotenv()
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        logger.warning("GOOGLE_PLACES_API_KEY not set — skipping POI enrichment")
        return

    input_path = _find_input()
    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    # Drop existing POI columns so we start fresh (cache still used)
    poi_cols = [c for c in schools.columns if c.startswith("poi_")]
    if poi_cols:
        schools = schools.drop(columns=poi_cols)

    # Import shared enrichment function
    from scripts_shared.enrichment.enrich_schools_with_pois import enrich_school

    # Queue of (idx, cache_key, lat, lon, name) tuples to process
    work = []
    cached_results: dict[int, dict] = {}
    for idx, row in schools.iterrows():
        lat, lon = row.get("latitude"), row.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            continue
        key = _cache_key(row, idx)
        cache_file = CACHE_DIR / f"{key}.json"
        if cache_file.exists():
            try:
                cached_results[idx] = json.loads(cache_file.read_text())
                continue
            except Exception:
                pass  # Corrupt cache — re-fetch
        work.append((idx, key, float(lat), float(lon), str(row.get("school_name", ""))))

    logger.info(f"  Cache hits: {len(cached_results)}  To fetch: {len(work)}")

    if not work:
        logger.info("  All schools cached — writing output")
    else:
        # Estimated: 6 API calls/school × $0.032 ≈ $0.19/school
        est_cost = len(work) * 6 * 0.032
        logger.info(f"  Estimated API cost: ~${est_cost:.2f} ({len(work) * 6} calls)")

        fetched = 0
        start = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future_to_key = {
                pool.submit(enrich_school, idx, lat, lon, name): (idx, key)
                for idx, key, lat, lon, name in work
            }
            for fut in as_completed(future_to_key):
                idx, key = future_to_key[fut]
                try:
                    _, poi_data, _ = fut.result()
                except Exception as e:
                    logger.warning(f"  POI fetch failed for {key}: {e}")
                    continue
                # Write per-school cache
                (CACHE_DIR / f"{key}.json").write_text(json.dumps(poi_data))
                cached_results[idx] = poi_data
                fetched += 1
                if fetched % 100 == 0:
                    rate = fetched / (time.time() - start)
                    eta = (len(work) - fetched) / rate if rate else 0
                    logger.info(f"  Progress: {fetched}/{len(work)}  rate={rate:.1f}/s  eta={eta/60:.1f}m")

        logger.info(f"  Fetched {fetched} schools in {(time.time()-start)/60:.1f} min")

    # Apply results to dataframe
    all_keys: set[str] = set()
    for poi_data in cached_results.values():
        all_keys.update(poi_data.keys())
    for col in all_keys:
        schools[col] = schools.index.map(lambda i: cached_results.get(i, {}).get(col))

    output_path = INTERMEDIATE_DIR / "nl_schools_with_pois.csv"
    schools.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path} ({len(schools)} schools, {len(schools.columns)} cols)")


if __name__ == "__main__":
    main()
