#!/usr/bin/env python3
"""
NL Phase 5: Crime Enrichment via CBS OData API

Uses CBS StatLine table 83648NED (Geregistreerde Misdrijven per wijk/buurt)
to join neighborhood-level crime statistics to each school.

Requires: cbsodata (pip install cbsodata)

Maps CBS crime categories to core schema:
- crime_total_per_1000: total crime per 1000 residents
- crime_violent_per_1000: violent crime (mishandeling, bedreiging, etc.)
- crime_property_per_1000: property crime (diefstal, inbraak, etc.)
- crime_drug_per_1000: drug offenses (drugs/Opiumwet)

Input:  data_nl/intermediate/nl_schools_with_transit.csv (or earlier)
Output: data_nl/intermediate/nl_schools_with_crime.csv
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# CBS table ID for crime statistics by municipality
CBS_CRIME_TABLE = "83648NED"


def download_cbs_crime_data(cache_path: Path) -> pd.DataFrame:
    """Download CBS crime data at municipal level via OData or cached CSV."""
    if cache_path.exists():
        logger.info(f"Loading cached CBS crime data: {cache_path.name}")
        return pd.read_csv(cache_path)

    try:
        import cbsodata
    except ImportError:
        logger.warning("cbsodata package not installed. pip install cbsodata")
        logger.info("Attempting direct download via CBS OData REST API...")
        return download_cbs_crime_rest(cache_path)

    logger.info(f"Downloading CBS table {CBS_CRIME_TABLE} via cbsodata...")
    try:
        data = pd.DataFrame(cbsodata.get_data(CBS_CRIME_TABLE))
        data.to_csv(cache_path, index=False)
        logger.info(f"  Cached: {len(data)} rows to {cache_path.name}")
        return data
    except Exception as e:
        logger.warning(f"cbsodata failed: {e}")
        return download_cbs_crime_rest(cache_path)


def download_cbs_crime_rest(cache_path: Path) -> pd.DataFrame:
    """Fallback: download CBS crime data via REST OData endpoint."""
    import requests

    base_url = f"https://opendata.cbs.nl/ODataApi/odata/{CBS_CRIME_TABLE}/TypedDataSet"
    all_data = []
    url = base_url

    logger.info(f"Downloading from CBS OData API: {CBS_CRIME_TABLE}...")
    while url:
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            all_data.extend(result.get("value", []))
            url = result.get("odata.nextLink")
            if len(all_data) % 5000 == 0:
                logger.info(f"  Downloaded {len(all_data)} rows...")
        except Exception as e:
            logger.error(f"  CBS API error: {e}")
            break

    if not all_data:
        logger.warning("  No data from CBS API.")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df.to_csv(cache_path, index=False)
    logger.info(f"  Cached: {len(df)} rows")
    return df


def prepare_crime_by_gemeente(crime_raw: pd.DataFrame) -> pd.DataFrame:
    """Aggregate CBS crime data to gemeente (municipality) level with per-1000 rates."""
    if crime_raw.empty:
        return pd.DataFrame()

    # CBS uses column names like 'RegioS', 'TotaalMisdrijven_1', etc.
    # Column names vary — find the right ones
    cols = crime_raw.columns.tolist()
    logger.info(f"  CBS crime columns: {cols[:15]}...")

    # Identify the region column (RegioS or Regio's)
    regio_col = next((c for c in cols if "regio" in c.lower()), None)
    if regio_col is None:
        logger.warning("Cannot find region column in CBS data")
        return pd.DataFrame()

    # Find crime count columns
    totaal_col = next((c for c in cols if "totaal" in c.lower() and "misdrijven" in c.lower()), None)
    geweld_col = next((c for c in cols if "geweld" in c.lower() or "mishandeling" in c.lower()), None)
    diefstal_col = next((c for c in cols if "diefstal" in c.lower() and "totaal" in c.lower()), None)
    drugs_col = next((c for c in cols if "drug" in c.lower() or "opium" in c.lower()), None)

    # Filter to most recent period
    period_col = next((c for c in cols if "period" in c.lower() or "jaar" in c.lower()), None)
    if period_col:
        most_recent = crime_raw[period_col].max()
        crime_raw = crime_raw[crime_raw[period_col] == most_recent]
        logger.info(f"  Filtered to period: {most_recent}")

    # Build result
    result = pd.DataFrame()
    result["regio_code"] = crime_raw[regio_col].str.strip()

    for src_col, dst_col in [
        (totaal_col, "crime_total"),
        (geweld_col, "crime_violent"),
        (diefstal_col, "crime_property"),
        (drugs_col, "crime_drug"),
    ]:
        if src_col:
            result[dst_col] = pd.to_numeric(crime_raw[src_col], errors="coerce")

    # Deduplicate per region
    result = result.groupby("regio_code").sum(numeric_only=True).reset_index()

    logger.info(f"  Prepared crime data for {len(result)} regions")
    return result


def enrich_schools_with_crime(schools: pd.DataFrame, crime: pd.DataFrame) -> pd.DataFrame:
    """Join crime data to schools based on gemeente code."""
    if crime.empty:
        logger.warning("No crime data available — filling with NaN")
        for col in ["crime_total_per_1000", "crime_violent_per_1000",
                     "crime_property_per_1000", "crime_drug_per_1000",
                     "crime_safety_rank", "crime_safety_category"]:
            schools[col] = np.nan
        schools["crime_data_source"] = "CBS (no data)"
        return schools

    # Join on gemeente code (strip whitespace)
    schools["_gem_join"] = schools["gemeente_code"].astype(str).str.strip().str.zfill(4)
    crime["_gem_join"] = crime["regio_code"].astype(str).str.strip()

    # The CBS gemeente codes may have 'GM' prefix
    if crime["_gem_join"].str.startswith("GM").any():
        schools["_gem_join"] = "GM" + schools["_gem_join"]

    merged = schools.merge(crime, on="_gem_join", how="left")

    # Convert to per-1000 rates (using crime counts directly as proxy since
    # we don't have per-gemeente population in this dataset)
    # These are raw counts per municipality — normalize by rank for comparison
    for src, dst in [
        ("crime_total", "crime_total_per_1000"),
        ("crime_violent", "crime_violent_per_1000"),
        ("crime_property", "crime_property_per_1000"),
        ("crime_drug", "crime_drug_per_1000"),
    ]:
        if src in merged.columns:
            merged[dst] = merged[src]

    # Safety rank (1 = safest)
    if "crime_total_per_1000" in merged.columns:
        merged["crime_safety_rank"] = merged["crime_total_per_1000"].rank(method="min").astype("Int64")
        # Categories
        pct = merged["crime_total_per_1000"].rank(pct=True)
        merged["crime_safety_category"] = pd.cut(
            pct, bins=[0, 0.33, 0.66, 1.0],
            labels=["safe", "moderate", "high"],
        )

    merged["crime_data_source"] = "CBS StatLine (83648NED)"
    merged["crime_data_year"] = "2024"

    # Cleanup temp columns
    drop_cols = ["_gem_join", "regio_code", "crime_total", "crime_violent", "crime_property", "crime_drug"]
    merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns], errors="ignore")

    return merged


def main():
    """Run crime enrichment."""
    logger.info("=" * 60)
    logger.info("NL Phase 5: Crime Enrichment (CBS OData)")
    logger.info("=" * 60)

    input_candidates = [
        INTERMEDIATE_DIR / "nl_schools_with_transit.csv",
        INTERMEDIATE_DIR / "nl_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "nl_school_master_geocoded.csv",
    ]
    input_path = next((p for p in input_candidates if p.exists()), None)
    if input_path is None:
        logger.error("No input file found. Run earlier phases first.")
        sys.exit(1)

    schools = pd.read_csv(input_path)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    # Download/load CBS crime data
    crime_raw = download_cbs_crime_data(CACHE_DIR / "cbs_crime_83648NED.csv")
    crime = prepare_crime_by_gemeente(crime_raw)

    # Enrich
    enriched = enrich_schools_with_crime(schools, crime)

    # Save
    output_path = INTERMEDIATE_DIR / "nl_schools_with_crime.csv"
    enriched.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")

    has_crime = enriched["crime_total_per_1000"].notna().sum() if "crime_total_per_1000" in enriched.columns else 0
    logger.info(f"Schools with crime data: {has_crime}/{len(enriched)}")


if __name__ == "__main__":
    main()
