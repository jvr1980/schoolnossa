#!/usr/bin/env python3
"""
UK Phase 5: Crime Enrichment via police.uk API

Queries the police.uk street-level crime API for each school location.
API is free, no key needed.

Endpoint: GET https://data.police.uk/api/crimes-street/all-crime
          ?lat={lat}&lng={lng}&date={YYYY-MM}

The endpoint returns every recorded crime within a 1 mile radius of the point
for a single calendar month.

-------------------------------------------------------------------------------
April 2026 silent failure — why this file was rewritten
-------------------------------------------------------------------------------
The original version produced 0.0 for all 5,303 schools and reported success.
Three separate defects combined:

  1. police.uk rate-limits hard. Sustained ~9 req/s returns HTTP 429 for roughly
     two thirds of requests (measured). The old loop fired ~14 req/s.
  2. `except Exception: crimes = []` swallowed the 429 raised by
     raise_for_status(), and then **wrote the empty result to the on-disk
     cache**. Every one of the 5,144 cached files was `{"_total": 0}` — a cached
     failure, indistinguishable from a real answer, and permanent: re-running
     the phase could never repair it.
  3. `counts.get("_total", 0)` filled 0 rather than leaving NaN, and
     `crime_data_source` / `crime_data_year` were stamped unconditionally at the
     end. So null-rate coverage checks reported "100% populated" while every
     value was zero, and rank-tertiling a constant produced a single
     `crime_safety_category` for the entire country.

This is the same failure mode as the NL traffic no-op (2026-09-10) and the GB
NaPTAN / STATS19 column mismatches (2026-04-20): a silent fallback that keeps
the pipeline green. The rules that follow from it, applied here:

  * A failed request is never cached. Only a genuine HTTP 200 body is.
  * A failed request yields NaN, never 0.
  * The metadata stamp is only written where a real measurement landed.
  * `_require_variation()` raises RuntimeError rather than letting a constant
     or an all-zero column reach the final table.

Input:  data_gb/intermediate/gb_schools_with_transit.csv
Output: data_gb/intermediate/gb_schools_with_crime.csv
"""

import argparse
import html
import json
import logging
import math
import random
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
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

POLICE_API = "https://data.police.uk/api/crimes-street/all-crime"
LOCATE_API = "https://data.police.uk/api/locate-neighbourhood"
FORCE_NEIGHBOURHOODS_API = "https://data.police.uk/api/{force}/neighbourhoods"
CRIME_CACHE_DIR = CACHE_DIR / "police_uk"

# --- Sampling -----------------------------------------------------------------
# One request covers one school-month, and the API tolerates ~6 req/s sustained.
# A full 12-month pull would be ~62k requests (>3h), so we sample three months
# spread across the year and scale up. Seasonal spread matters: acquisitive
# crime peaks in winter, ASB in summer, so a single month is a biased estimate
# of the annual figure.
CRIME_YEAR = "2025"
SAMPLE_MONTHS = ["2025-02", "2025-06", "2025-10"]
MONTHS_PER_YEAR = 12

# --- Rate limiting ------------------------------------------------------------
# Measured against the live API: 4 workers at 6 req/s -> 95% HTTP 200;
# 6 workers at 8 req/s -> 85%; 8 workers unthrottled (~9 req/s) -> 32%.
# Workers must comfortably exceed rate x latency or the pool, not the bucket,
# becomes the constraint: at ~1.5s per response, 5 workers cap throughput at
# ~3.3 req/s regardless of the limit. The bucket is what keeps us under the
# API's tolerance, so oversizing the pool is safe.
REQUESTS_PER_SECOND = 6.5
MAX_WORKERS = 14
MAX_ATTEMPTS = 6

# --- Rate denominator ---------------------------------------------------------
# police.uk returns crimes inside a fixed 1 mile radius, so the catchment area is
# identical for every school; converting the count to a per-1,000-residents rate
# needs a resident population for that circle. GB has no per-school population:
# gb_lsoa_code is 0% populated, and an LSOA holds ~1,500 people by design, so
# LSOA population carries no density signal even where it is known.
#
# We therefore use one documented national constant rather than inventing a
# per-school denominator. From ONS "Built-up areas in England and Wales:
# Census 2021": built-up areas hold ~83% of the population on ~8.7% of the land.
# England: 0.83 x 56.5M residents / (0.087 x 130,279 km2) ~= 4,100 people/km2.
# Schools sit inside built-up areas, so this is the right density band — the
# England-wide average (~434/km2) would describe empty countryside.
#
# Consequence, stated plainly: this constant sets the SCALE of the rate, not its
# spread. Relative differences between schools come entirely from the measured
# crime counts. A per-school denominator (postcode-level population within 1
# mile) remains an open item.
CATCHMENT_RADIUS_KM = 1.60934  # 1 mile
CATCHMENT_AREA_KM2 = math.pi * CATCHMENT_RADIUS_KM ** 2  # ~8.14 km2
BUILT_UP_POP_DENSITY_PER_KM2 = 4100.0
CATCHMENT_POPULATION = CATCHMENT_AREA_KM2 * BUILT_UP_POP_DENSITY_PER_KM2  # ~33,400

DATA_SOURCE = (
    f"police.uk street-level crime ({'/'.join(SAMPLE_MONTHS)}, annualised), "
    f"per 1,000 residents of the 1-mile catchment"
)

PROPERTY_CATEGORIES = ("burglary", "other-theft", "shoplifting", "vehicle-crime",
                       "bicycle-theft", "theft-from-the-person", "robbery")

_session = requests.Session()
_session.mount("https://", requests.adapters.HTTPAdapter(pool_maxsize=MAX_WORKERS * 2))
_session.headers.update({"User-Agent": "schoolnossa-gb-pipeline/1.0"})


class RateLimiter:
    """Token bucket shared by every worker thread."""

    def __init__(self, per_second: float):
        self._interval = 1.0 / per_second
        self._lock = threading.Lock()
        self._next = time.monotonic()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = max(now, self._next) + self._interval
        if wait > 0:
            time.sleep(wait)


_limiter = RateLimiter(REQUESTS_PER_SECOND)


def _get_json(url: str, params: dict | None = None):
    """GET with backoff. Returns the parsed body, or None if it never succeeded.

    None means "we do not know", and must never be conflated with an empty
    result. That conflation is the bug this module exists to not repeat.
    """
    for attempt in range(MAX_ATTEMPTS):
        _limiter.acquire()
        try:
            resp = _session.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            logger.debug(f"  transport error ({attempt + 1}/{MAX_ATTEMPTS}): {exc}")
            time.sleep(min(2 ** attempt, 30) + random.random())
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                logger.debug("  HTTP 200 with unparseable body")
                return None
        if resp.status_code == 404:
            # Genuine "no such area" — a real answer, not a failure.
            return []
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() \
                else min(2 ** attempt, 30)
            time.sleep(delay + random.random())
            continue
        logger.debug(f"  unexpected HTTP {resp.status_code}")
        return None
    return None


def _aggregate(crimes: list) -> dict:
    counts: dict[str, int] = {}
    for crime in crimes:
        if not isinstance(crime, dict):
            continue
        counts[crime.get("category", "other-crime")] = \
            counts.get(crime.get("category", "other-crime"), 0) + 1
    counts["_total"] = len(crimes)
    return counts


def fetch_month(lat: float, lon: float, month: str, cache_key: str) -> dict | None:
    """Crime counts by category for one point-month. None = request failed.

    Only a successful response is cached. Caching a failure is what made the
    April run unrepairable.
    """
    cache_file = CRIME_CACHE_DIR / month / f"{cache_key}.json"
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text())
        except (ValueError, OSError):
            cache_file.unlink(missing_ok=True)

    body = _get_json(POLICE_API, {"lat": round(lat, 6), "lng": round(lon, 6),
                                  "date": month})
    if body is None or not isinstance(body, list):
        return None

    counts = _aggregate(body)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(counts))
    return counts


def fetch_neighbourhood(lat: float, lon: float, cache_key: str) -> dict | None:
    """Police force + neighbourhood id for a point. None = request failed."""
    cache_file = CRIME_CACHE_DIR / "neighbourhood" / f"{cache_key}.json"
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text())
        except (ValueError, OSError):
            cache_file.unlink(missing_ok=True)

    body = _get_json(LOCATE_API, {"q": f"{round(lat, 6)},{round(lon, 6)}"})
    if body is None:
        return None
    if not isinstance(body, dict):
        body = {}  # 404 -> [] -> point outside any force area; a real answer.

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(body))
    return body


def purge_poisoned_cache() -> int:
    """Delete the April cache: flat {urn}.json files that are all `_total: 0`.

    They are cached HTTP 429s. Left in place they would be re-read forever and
    silently reproduce the all-zero column.
    """
    removed = 0
    for path in CRIME_CACHE_DIR.glob("*.json"):
        try:
            payload = json.loads(path.read_text())
        except (ValueError, OSError):
            payload = None
        if not isinstance(payload, dict) or payload.get("_total", 0) == 0:
            path.unlink(missing_ok=True)
            removed += 1
    if removed:
        logger.warning(f"Purged {removed} poisoned cache entries from the April run "
                       f"(cached HTTP 429s stored as zero-crime results)")
    return removed


def _require_variation(series: pd.Series, name: str, min_coverage: float = 0.5) -> None:
    """Refuse to ship a column that is empty, constant, or all-zero.

    The April crime run and the April NL traffic run both passed a null-rate
    check while carrying no information. Coverage is necessary but not
    sufficient — distinct values are the test that catches this class of bug.
    """
    values = series.dropna()
    coverage = len(values) / len(series) if len(series) else 0.0
    if coverage < min_coverage:
        raise RuntimeError(
            f"{name}: only {coverage:.1%} of {len(series)} schools have a value "
            f"(need >= {min_coverage:.0%}). Refusing to report success — see the "
            f"April 2026 all-zero crime run."
        )
    distinct = values.nunique()
    if distinct <= 1:
        only = values.iloc[0] if len(values) else "<empty>"
        raise RuntimeError(
            f"{name}: {distinct} distinct value(s) across {len(values)} rows "
            f"(constant {only!r}). This is what an all-zero enrichment looks "
            f"like to a null-rate check. Refusing to write."
        )
    if pd.api.types.is_numeric_dtype(values) and float(values.abs().max()) == 0.0:
        raise RuntimeError(f"{name}: every value is zero. Refusing to write.")


def _load_force_names() -> dict:
    """force id -> {neighbourhood id: name}, one request per force (~44).

    Cached to disk: the Met alone returns ~600 neighbourhoods and the whole
    sweep costs ~100s, which is pure waste on a re-run.
    """
    cache_file = CRIME_CACHE_DIR / "force_neighbourhoods.json"
    if cache_file.exists():
        try:
            lookup = json.loads(cache_file.read_text())
            logger.info(f"  Neighbourhood names for {len(lookup)} forces (cached)")
            return lookup
        except (ValueError, OSError):
            cache_file.unlink(missing_ok=True)

    forces = _get_json("https://data.police.uk/api/forces") or []
    lookup: dict[str, dict[str, str]] = {}
    for force in forces:
        fid = force.get("id")
        if not fid:
            continue
        hoods = _get_json(FORCE_NEIGHBOURHOODS_API.format(force=fid))
        if not hoods:
            continue
        # Names arrive HTML-escaped ("Park Barn &amp; Westborough").
        lookup[fid] = {h.get("id"): html.unescape(h.get("name") or "")
                       for h in hoods if h.get("id")}
    if lookup:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(json.dumps(lookup))
    logger.info(f"  Loaded neighbourhood names for {len(lookup)} police forces")
    return lookup


def enrich_with_crime(schools: pd.DataFrame, months: list[str]) -> pd.DataFrame:
    lat = pd.to_numeric(schools.get("latitude"), errors="coerce")
    lon = pd.to_numeric(schools.get("longitude"), errors="coerce")
    have_coords = lat.notna() & lon.notna()
    logger.info(f"{int(have_coords.sum())}/{len(schools)} schools have coordinates")

    # Several URNs repeat and some schools share a site; one catchment per
    # distinct point keeps the request count honest.
    points: dict[str, tuple[float, float]] = {}
    keys = pd.Series([None] * len(schools), index=schools.index, dtype=object)
    for idx in schools.index[have_coords]:
        key = f"{lat[idx]:.5f}_{lon[idx]:.5f}"
        keys[idx] = key
        points.setdefault(key, (float(lat[idx]), float(lon[idx])))

    total_requests = len(points) * len(months) + len(points)
    logger.info(f"{len(points)} distinct catchments x {len(months)} months "
                f"+ neighbourhood lookup = ~{total_requests} requests "
                f"(~{total_requests / REQUESTS_PER_SECOND / 60:.0f} min at "
                f"{REQUESTS_PER_SECOND} req/s)")

    # --- crime counts ---------------------------------------------------------
    jobs = [(key, month) for key in points for month in months]
    results: dict[tuple[str, str], dict | None] = {}
    done = Counter()

    def run(job):
        key, month = job
        plat, plon = points[key]
        out = fetch_month(plat, plon, month, key)
        results[job] = out
        done["ok" if out is not None else "fail"] += 1
        n = done["ok"] + done["fail"]
        if n % 1000 == 0:
            logger.info(f"  crime: {n}/{len(jobs)} "
                        f"(ok {done['ok']}, failed {done['fail']})")
        return None

    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        list(pool.map(run, jobs))
    logger.info(f"  crime fetch complete: {done['ok']} ok, {done['fail']} failed")

    # --- neighbourhood names --------------------------------------------------
    force_names = _load_force_names()
    hoods: dict[str, str | None] = {}
    hood_done = Counter()

    def run_hood(key):
        plat, plon = points[key]
        body = fetch_neighbourhood(plat, plon, key)
        if body is None:
            hoods[key] = None
            hood_done["fail"] += 1
        else:
            name = force_names.get(body.get("force"), {}).get(body.get("neighbourhood"))
            hoods[key] = name
            hood_done["ok"] += 1
        n = hood_done["ok"] + hood_done["fail"]
        if n % 1000 == 0:
            logger.info(f"  neighbourhood: {n}/{len(points)}")
        return None

    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        list(pool.map(run_hood, list(points)))
    logger.info(f"  neighbourhood lookup complete: {hood_done['ok']} ok, "
                f"{hood_done['fail']} failed")

    # --- assemble -------------------------------------------------------------
    scale = MONTHS_PER_YEAR / len(months)
    per_1000 = CATCHMENT_POPULATION / 1000.0

    n = len(schools)
    total = np.full(n, np.nan)
    violent = np.full(n, np.nan)
    prop = np.full(n, np.nan)
    drug = np.full(n, np.nan)
    area = pd.Series([None] * n, index=schools.index, dtype=object)

    for pos, idx in enumerate(schools.index):
        key = keys[idx]
        if key is None:
            continue
        months_for_school = [results.get((key, m)) for m in months]
        # A partial month set would understate the annual figure, so a school is
        # only scored when every sampled month came back.
        if any(m is None for m in months_for_school):
            continue
        summed = Counter()
        for month_counts in months_for_school:
            summed.update(month_counts)
        total[pos] = summed["_total"] * scale
        violent[pos] = summed["violent-crime"] * scale
        prop[pos] = sum(summed[c] for c in PROPERTY_CATEGORIES) * scale
        drug[pos] = summed["drugs"] * scale
        area[idx] = hoods.get(key)

    schools["crime_total_per_1000"] = total / per_1000
    schools["crime_violent_per_1000"] = violent / per_1000
    schools["crime_property_per_1000"] = prop / per_1000
    schools["crime_drug_per_1000"] = drug / per_1000
    schools["crime_area_name"] = area

    scored = pd.Series(total, index=schools.index)
    # Rank and tertile on the measured annual count. Schools we could not
    # measure stay NaN in every derived column rather than joining a bucket.
    schools["crime_safety_rank"] = scored.rank(method="min").astype("Int64")
    pct = scored.rank(pct=True)
    schools["crime_safety_category"] = pd.cut(
        pct, bins=[0, 1 / 3, 2 / 3, 1.0],
        # Berlin's tertile vocabulary (rebuild_final_table). "high" would be
        # dropped by the UI filters, which match the literal.
        labels=["safe", "moderate", "elevated"],
    ).astype(object)

    measured = scored.notna()
    schools["crime_data_source"] = np.where(measured, DATA_SOURCE, None)
    schools["crime_data_year"] = np.where(measured, CRIME_YEAR, None)

    # --- guards ---------------------------------------------------------------
    for col in ("crime_total_per_1000", "crime_violent_per_1000",
                "crime_property_per_1000", "crime_drug_per_1000",
                "crime_safety_category", "crime_area_name"):
        _require_variation(schools[col], col)

    vocab = set(schools["crime_safety_category"].dropna().unique())
    if not vocab <= {"safe", "moderate", "elevated"}:
        raise RuntimeError(f"crime_safety_category vocabulary drifted: {sorted(vocab)}")

    logger.info("")
    logger.info("Distinct-value check (the test a null-rate check fails):")
    for col in ("crime_total_per_1000", "crime_violent_per_1000",
                "crime_property_per_1000", "crime_drug_per_1000", "crime_area_name"):
        vals = schools[col].dropna()
        if pd.api.types.is_numeric_dtype(vals):
            logger.info(f"  {col:26s} n={len(vals):5d} distinct={vals.nunique():5d} "
                        f"min={vals.min():8.1f} median={vals.median():8.1f} "
                        f"max={vals.max():9.1f}")
        else:
            logger.info(f"  {col:26s} n={len(vals):5d} distinct={vals.nunique():5d}")
    logger.info(f"  crime_safety_category      "
                f"{schools['crime_safety_category'].value_counts().to_dict()}")
    return schools


def main():
    parser = argparse.ArgumentParser(description="GB crime enrichment (police.uk)")
    parser.add_argument("--months", default=",".join(SAMPLE_MONTHS),
                        help="Comma-separated YYYY-MM months to sample")
    args = parser.parse_args()
    months = [m.strip() for m in args.months.split(",") if m.strip()]

    logger.info("=" * 60)
    logger.info("UK Phase 5: Crime Enrichment (police.uk)")
    logger.info("=" * 60)

    candidates = [
        INTERMEDIATE_DIR / "gb_schools_with_transit.csv",
        INTERMEDIATE_DIR / "gb_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "gb_school_master_base.csv",
    ]
    input_path = next((p for p in candidates if p.exists()), None)
    if not input_path:
        logger.error("Run earlier phases first.")
        sys.exit(1)

    CRIME_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    purge_poisoned_cache()

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")
    enriched = enrich_with_crime(schools, months)

    output = INTERMEDIATE_DIR / "gb_schools_with_crime.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
