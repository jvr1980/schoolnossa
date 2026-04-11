#!/usr/bin/env python3
"""
NL Phase 7: Demographics Enrichment via CBS Kerncijfers Wijken en Buurten

Uses CBS StatLine table 85984NED (Kerncijfers wijken en buurten 2024)
to join neighborhood-level socioeconomic data to each school.

Key variables:
- Population density
- Income (gemiddeld inkomen per inwoner)
- Migration background percentage
- Housing value (WOZ waarde)
- Unemployment / welfare recipients

Requires: cbsodata (pip install cbsodata) or falls back to REST API

Input:  data_nl/intermediate/nl_schools_with_pois.csv (or earlier)
Output: data_nl/intermediate/nl_schools_with_demographics.csv
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

# CBS table for Kerncijfers wijken en buurten
CBS_KERNCIJFERS_TABLE = "85984NED"  # 2024 edition


def download_cbs_kerncijfers(cache_path: Path) -> pd.DataFrame:
    """Download CBS Kerncijfers via cbsodata or REST fallback."""
    if cache_path.exists():
        logger.info(f"Loading cached CBS Kerncijfers: {cache_path.name}")
        return pd.read_csv(cache_path, low_memory=False)

    try:
        import cbsodata
        logger.info(f"Downloading CBS table {CBS_KERNCIJFERS_TABLE} via cbsodata...")
        data = pd.DataFrame(cbsodata.get_data(CBS_KERNCIJFERS_TABLE))
        data.to_csv(cache_path, index=False)
        logger.info(f"  Cached: {len(data)} rows")
        return data
    except ImportError:
        logger.warning("cbsodata not installed. Using REST API fallback...")
    except Exception as e:
        logger.warning(f"cbsodata failed: {e}. Using REST API fallback...")

    # REST fallback
    import requests
    base_url = f"https://opendata.cbs.nl/ODataApi/odata/{CBS_KERNCIJFERS_TABLE}/TypedDataSet"
    all_data = []
    url = base_url

    while url:
        try:
            resp = requests.get(url, timeout=60)
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
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df.to_csv(cache_path, index=False)
    logger.info(f"  Cached: {len(df)} rows")
    return df


def prepare_demographics_by_gemeente(raw: pd.DataFrame) -> pd.DataFrame:
    """Extract key demographic variables at gemeente level from CBS Kerncijfers."""
    if raw.empty:
        return pd.DataFrame()

    cols = raw.columns.tolist()
    logger.info(f"  CBS Kerncijfers has {len(cols)} columns, {len(raw)} rows")

    # Find region column
    regio_col = next((c for c in cols if "regio" in c.lower()), None)
    if regio_col is None:
        logger.warning("Cannot find region column")
        return pd.DataFrame()

    # Filter to gemeente level (GM prefix, exclude wijk BU/WK)
    raw["_regio_clean"] = raw[regio_col].astype(str).str.strip()
    gem = raw[raw["_regio_clean"].str.startswith("GM")].copy()
    logger.info(f"  Filtered to {len(gem)} gemeente records")

    if gem.empty:
        return pd.DataFrame()

    result = pd.DataFrame()
    result["gemeente_code"] = gem["_regio_clean"].str.replace("GM", "").str.strip()

    # Map common CBS column name patterns to our fields
    column_patterns = {
        "area_population_density": ["bevolkingsdichtheid", "bev.dichtheid", "population_density"],
        "area_median_income": ["gemiddeld inkomen", "gem. inkomen", "average_income"],
        "migration_background_pct": ["migratieachtergrond", "migration_background"],
        "area_unemployment_rate": ["werkloosheidsuitkering", "unemployment"],
        "woz_value": ["woz", "gem.woningwaarde", "average_woz"],
    }

    for target, patterns in column_patterns.items():
        matched_col = None
        for pattern in patterns:
            matched_col = next((c for c in cols if pattern.lower() in c.lower()), None)
            if matched_col:
                break
        if matched_col:
            result[target] = pd.to_numeric(gem[matched_col], errors="coerce").values
            logger.info(f"  Mapped: {matched_col} -> {target}")
        else:
            logger.debug(f"  No match for {target}")

    result = result.drop_duplicates(subset=["gemeente_code"])
    logger.info(f"  Demographics prepared for {len(result)} gemeenten")
    return result


def enrich_schools_with_demographics(schools: pd.DataFrame, demographics: pd.DataFrame) -> pd.DataFrame:
    """Join demographic data to schools based on gemeente code."""
    if demographics.empty:
        logger.warning("No demographics data — filling with NaN")
        for col in ["area_population_density", "area_median_income",
                     "migration_background_pct", "area_unemployment_rate",
                     "deprivation_index"]:
            schools[col] = np.nan
        return schools

    # Normalize join keys
    schools["_gem_join"] = schools["gemeente_code"].astype(str).str.strip().str.zfill(4)
    demographics["_gem_join"] = demographics["gemeente_code"].astype(str).str.strip().str.zfill(4)

    merged = schools.merge(demographics, on="_gem_join", how="left", suffixes=("", "_demo"))

    # Compute a deprivation index (0-10 scale, higher = more deprived)
    # Based on income rank (lower income = higher deprivation)
    if "area_median_income" in merged.columns:
        income_pct = merged["area_median_income"].rank(pct=True)
        merged["deprivation_index"] = ((1 - income_pct) * 10).round(1)

    # Cleanup
    merged = merged.drop(columns=["_gem_join", "gemeente_code_demo"], errors="ignore")

    has_data = merged["area_median_income"].notna().sum() if "area_median_income" in merged.columns else 0
    logger.info(f"  Schools with demographics: {has_data}/{len(merged)}")

    return merged


def main():
    """Run demographics enrichment."""
    logger.info("=" * 60)
    logger.info("NL Phase 7: Demographics Enrichment (CBS Kerncijfers)")
    logger.info("=" * 60)

    input_candidates = [
        INTERMEDIATE_DIR / "nl_schools_with_pois.csv",
        INTERMEDIATE_DIR / "nl_schools_with_crime.csv",
        INTERMEDIATE_DIR / "nl_schools_with_transit.csv",
        INTERMEDIATE_DIR / "nl_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "nl_school_master_geocoded.csv",
    ]
    input_path = next((p for p in input_candidates if p.exists()), None)
    if input_path is None:
        logger.error("No input file found. Run earlier phases first.")
        sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    # Download/load CBS data
    raw = download_cbs_kerncijfers(CACHE_DIR / f"cbs_kerncijfers_{CBS_KERNCIJFERS_TABLE}.csv")
    demographics = prepare_demographics_by_gemeente(raw)

    # Enrich
    enriched = enrich_schools_with_demographics(schools, demographics)

    # Save
    output_path = INTERMEDIATE_DIR / "nl_schools_with_demographics.csv"
    enriched.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
