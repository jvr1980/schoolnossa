#!/usr/bin/env python3
"""
NL Phase 7: Demographics Enrichment via CBS Kerncijfers Wijken en Buurten

Uses CBS OData API to fetch gemeente-level socioeconomic indicators.
The table 85984NED has hundreds of variables — we extract:
- Population density (Bevolkingsdichtheid)
- Average household income (GemiddeldInkomenPerInwoner)
- % non-Western migration background
- Unemployment/welfare recipients
- Average house value (WOZ)

Input:  data_nl/intermediate/nl_schools_with_crime.csv (or earlier)
Output: data_nl/intermediate/nl_schools_with_demographics.csv
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# CBS Kerncijfers wijken en buurten 2024
CBS_TABLE = "85984NED"
CBS_BASE = f"https://opendata.cbs.nl/ODataApi/odata/{CBS_TABLE}"


def fetch_cbs_demographics(cache_path: Path) -> pd.DataFrame:
    """Fetch gemeente-level demographics from CBS Kerncijfers via OData."""
    if cache_path.exists():
        logger.info(f"Loading cached demographics: {cache_path.name}")
        return pd.read_csv(cache_path)

    logger.info(f"Fetching CBS Kerncijfers (table {CBS_TABLE})...")

    # First, get the column metadata to find the right variable names
    props_url = f"{CBS_BASE}/DataProperties"
    try:
        props_resp = requests.get(props_url, timeout=15)
        props_resp.raise_for_status()
        props = props_resp.json().get("value", [])
        logger.info(f"  Table has {len(props)} variables")

        # Log interesting ones for debugging
        for p in props:
            key = p.get("Key", "")
            title = p.get("Title", "")
            if any(kw in title.lower() for kw in ["bevolking", "inkomen", "woz", "dichtheid",
                                                     "migratie", "westers", "uitkering", "werkloos"]):
                logger.info(f"  Found: {key} = {title}")
    except Exception as e:
        logger.warning(f"  Could not fetch properties: {e}")
        props = []

    # Fetch gemeente-level data — select key columns
    # Column names in CBS tables use IDs like BevolkingsdichtheidInwonersPerKm2_3
    # We need to discover the right column IDs
    url = f"{CBS_BASE}/TypedDataSet"
    params = {
        "$filter": "startswith(WijkenEnBuurten,'GM')",
        "$top": "2",  # Just get 2 rows to see column names
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        sample = resp.json().get("value", [])
        if sample:
            all_cols = list(sample[0].keys())
            logger.info(f"  Sample columns ({len(all_cols)} total): {all_cols[:10]}...")

            # Find columns by keyword matching
            col_map = {}
            for col in all_cols:
                cl = col.lower()
                if "bevolkingsdichtheid" in cl:
                    col_map["area_population_density"] = col
                elif "gemiddeldinkomen" in cl and "inwoner" in cl:
                    col_map["area_median_income"] = col
                elif "niet-westers" in cl or "nietwesters" in cl:
                    col_map["migration_background_pct"] = col
                elif "woz" in cl and "gemiddeld" in cl:
                    col_map["woz_value"] = col
                elif ("uitkering" in cl or "bijstand" in cl) and "totaal" in cl:
                    col_map["area_unemployment_rate"] = col

            logger.info(f"  Mapped columns: {col_map}")
        else:
            logger.warning("  No sample data returned")
            return pd.DataFrame()
    except Exception as e:
        logger.warning(f"  Sample query failed: {e}")
        return pd.DataFrame()

    # Fetch all gemeente rows (no $skip — use odata.nextLink for pagination)
    select_cols = ["WijkenEnBuurten"] + list(col_map.values())
    all_data = []

    fetch_url = url
    fetch_params = {
        "$filter": "startswith(WijkenEnBuurten,'GM')",
        "$select": ",".join(select_cols),
    }
    try:
        resp = requests.get(fetch_url, params=fetch_params, timeout=60)
        resp.raise_for_status()
        page = resp.json()
        all_data.extend(page.get("value", []))

        # Follow odata.nextLink if paginated
        next_link = page.get("odata.nextLink")
        while next_link:
            resp = requests.get(next_link, timeout=60)
            resp.raise_for_status()
            page = resp.json()
            all_data.extend(page.get("value", []))
            next_link = page.get("odata.nextLink")
            logger.info(f"  Paginated to {len(all_data)} rows...")

        logger.info(f"  Fetched {len(all_data)} gemeente rows")
    except Exception as e:
        logger.warning(f"  Fetch error: {e}")

    if not all_data:
        logger.warning("  No demographics data fetched")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df["gemeente_code"] = df["WijkenEnBuurten"].str.strip().str.replace("GM", "")

    # Rename to our schema
    rename = {v: k for k, v in col_map.items()}
    df = df.rename(columns=rename)

    # Convert to numeric
    for col in col_map.keys():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop(columns=["WijkenEnBuurten"], errors="ignore")
    df.to_csv(cache_path, index=False)
    logger.info(f"  Cached: {len(df)} gemeenten with demographics")
    return df


def enrich_schools(schools: pd.DataFrame, demographics: pd.DataFrame) -> pd.DataFrame:
    """Join demographics to schools on gemeente_code."""
    if demographics.empty:
        logger.warning("No demographics data — filling with NaN")
        return schools

    schools["_gem"] = schools["gemeente_code"].astype(str).str.strip().str.zfill(4)
    demographics["_gem"] = demographics["gemeente_code"].astype(str).str.strip().str.zfill(4)

    merged = schools.merge(demographics, on="_gem", how="left", suffixes=("", "_demo"))

    # Compute deprivation index (0-10 scale, lower income = higher deprivation)
    if "area_median_income" in merged.columns:
        income_pct = merged["area_median_income"].rank(pct=True)
        merged["deprivation_index"] = ((1 - income_pct) * 10).round(1)

    merged = merged.drop(columns=["_gem", "gemeente_code_demo"], errors="ignore")
    filled = merged["area_median_income"].notna().sum() if "area_median_income" in merged.columns else 0
    logger.info(f"Schools with demographics: {filled}/{len(merged)}")
    return merged


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 7: Demographics Enrichment (CBS Kerncijfers)")
    logger.info("=" * 60)

    candidates = [
        INTERMEDIATE_DIR / "nl_schools_with_pois.csv",
        INTERMEDIATE_DIR / "nl_schools_with_crime.csv",
        INTERMEDIATE_DIR / "nl_schools_with_transit.csv",
        INTERMEDIATE_DIR / "nl_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "nl_school_master_geocoded.csv",
    ]
    input_path = next((p for p in candidates if p.exists()), None)
    if not input_path:
        logger.error("No input found."); sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    demographics = fetch_cbs_demographics(CACHE_DIR / "cbs_kerncijfers_gemeente.csv")
    enriched = enrich_schools(schools, demographics)

    output = INTERMEDIATE_DIR / "nl_schools_with_demographics.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
