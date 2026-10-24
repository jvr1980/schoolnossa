#!/usr/bin/env python3
"""
NL Phase 5: Crime Enrichment via CBS OData API (table 83648NED)

Fetches gemeente-level crime rates directly via OData, filtered to:
- Most recent year (2024 or latest available)
- Municipality level (RegioS starting with 'GM')
- Key crime categories mapped to core schema

CBS crime categories used:
- T001161: Total crimes → crime_total_per_1000
- CRI3000: Violent + sexual crimes → crime_violent_per_1000
- CRI1000: Property crimes → crime_property_per_1000
- CRI6000: Drug crimes → crime_drug_per_1000

Input:  data_nl/intermediate/nl_school_master_geocoded.csv (or later)
Output: data_nl/intermediate/nl_schools_with_crime.csv
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
# Data dir is overridable so the primary (basisonderwijs) pipeline can reuse
# these enrichers unchanged: NL_DATA_DIR=data_nl_po. Caches keyed by location
# rather than by school (accidents, GTFS, CBS tables) still live under data_nl,
# so both levels share one download.
import os
DATA_DIR = PROJECT_ROOT / os.environ.get("NL_DATA_DIR", "data_nl")
SHARED_CACHE_DIR = PROJECT_ROOT / "data_nl" / "cache"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = SHARED_CACHE_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CBS_TABLE = "83648NED"
CBS_BASE = f"https://opendata.cbs.nl/ODataApi/odata/{CBS_TABLE}/TypedDataSet"

# Crime category codes → our schema columns
CRIME_CATEGORIES = {
    "T001161": "crime_total_per_1000",     # Misdrijven, totaal
    "CRI3000": "crime_violent_per_1000",   # Gewelds- en seksuele misdrijven
    "CRI1000": "crime_property_per_1000",  # Vermogensmisdrijven
    "CRI6000": "crime_drug_per_1000",      # Drugsmisdrijven
}


def fetch_cbs_crime(cache_path: Path) -> pd.DataFrame:
    """Fetch gemeente-level crime per 1000 from CBS OData, pivoted to one row per gemeente."""
    if cache_path.exists():
        logger.info(f"Loading cached crime data: {cache_path.name}")
        return pd.read_csv(cache_path)

    logger.info(f"Fetching CBS crime data (table {CBS_TABLE})...")

    # Find latest period
    periods_url = f"https://opendata.cbs.nl/ODataApi/odata/{CBS_TABLE}/Perioden"
    periods = requests.get(periods_url, timeout=15).json().get("value", [])
    latest_period = periods[-1]["Key"]  # e.g. "2024JJ00"
    logger.info(f"  Latest period: {latest_period}")

    # Fetch all gemeente-level rows for the latest period and our crime categories
    crime_codes = list(CRIME_CATEGORIES.keys())
    all_rows = []

    for code in crime_codes:
        url = CBS_BASE
        params = {
            "$filter": f"startswith(RegioS,'GM') and SoortMisdrijf eq '{code}' and Perioden eq '{latest_period}'",
            "$select": "RegioS,SoortMisdrijf,GeregistreerdeMisdrijvenPer1000Inw_3",
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            rows = resp.json().get("value", [])
            all_rows.extend(rows)
            logger.info(f"  {code} ({CRIME_CATEGORIES[code]}): {len(rows)} gemeenten")
        except Exception as e:
            logger.warning(f"  Failed for {code}: {e}")

    if not all_rows:
        logger.warning("No crime data fetched")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["gemeente_code"] = df["RegioS"].str.strip().str.replace("GM", "")
    df["column_name"] = df["SoortMisdrijf"].map(CRIME_CATEGORIES)
    df["rate"] = pd.to_numeric(df["GeregistreerdeMisdrijvenPer1000Inw_3"], errors="coerce")

    # Pivot: one row per gemeente, one column per crime type
    pivoted = df.pivot_table(
        index="gemeente_code",
        columns="column_name",
        values="rate",
        aggfunc="first",
    ).reset_index()

    pivoted["crime_data_year"] = latest_period[:4]
    pivoted.to_csv(cache_path, index=False)
    logger.info(f"  Cached: {len(pivoted)} gemeenten with crime rates")
    return pivoted


def enrich_schools(schools: pd.DataFrame, crime: pd.DataFrame) -> pd.DataFrame:
    """Join crime data to schools on gemeente_code."""
    if crime.empty:
        logger.warning("No crime data — filling with NaN")
        for col in CRIME_CATEGORIES.values():
            schools[col] = np.nan
        schools["crime_data_source"] = "CBS (no data)"
        return schools

    # Normalize join key
    schools["_gem"] = schools["gemeente_code"].astype(str).str.strip().str.zfill(4)
    crime["_gem"] = crime["gemeente_code"].astype(str).str.strip().str.zfill(4)

    # Rank and band the *areas*, then attach to schools. Berlin ranks its 12
    # Bezirke (1..12); ranking school rows instead gave NL values up to 5,858,
    # weighted every gemeente by how many schools it has, and gave the same
    # gemeente a different rank in the primary and secondary tables. Computing
    # it on the CBS table makes rank 1 = safest gemeente, identical everywhere.
    if "crime_total_per_1000" in crime.columns:
        rate = pd.to_numeric(crime["crime_total_per_1000"], errors="coerce")
        crime["crime_safety_rank"] = rate.rank(method="min").astype("Int64")
        # Vocabulary must match the German tertiles (safe/moderate/elevated) —
        # the UI filters on these literals across all countries.
        crime["crime_safety_category"] = pd.cut(
            rate.rank(pct=True), bins=[0, 1 / 3, 2 / 3, 1.0],
            labels=["safe", "moderate", "elevated"], include_lowest=True,
        ).astype("object")

    merged = schools.merge(crime, on="_gem", how="left", suffixes=("", "_crime"))

    merged["crime_data_source"] = "CBS StatLine 83648NED"
    merged = merged.drop(columns=["_gem", "gemeente_code_crime"], errors="ignore")

    filled = merged["crime_total_per_1000"].notna().sum()
    logger.info(f"Schools with crime data: {filled}/{len(merged)}")
    return merged


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 5: Crime Enrichment (CBS OData)")
    logger.info("=" * 60)

    candidates = [
        INTERMEDIATE_DIR / "nl_schools_with_transit.csv",
        INTERMEDIATE_DIR / "nl_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "nl_school_master_geocoded.csv",
    ]
    input_path = next((p for p in candidates if p.exists()), None)
    if not input_path:
        logger.error("No input found."); sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    crime = fetch_cbs_crime(CACHE_DIR / "cbs_crime_gemeente.csv")
    enriched = enrich_schools(schools, crime)

    output = INTERMEDIATE_DIR / "nl_schools_with_crime.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
