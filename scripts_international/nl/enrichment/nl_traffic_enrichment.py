#!/usr/bin/env python3
"""
NL Phase 3: Traffic/Road Safety Enrichment

Downloads BRON accident data from Rijkswaterstaat file server (not WFS),
then computes accident density near each school.

The BRON CSV links accidents to NWB road segments (hectometer+letter combos).
For geo-matching we use a simplified approach: download the accident data and,
since direct lat/lon isn't in the CSV, we use a municipality-level aggregation
(accidents per gemeente) as an area-level traffic safety indicator.

For per-school radius-based analysis, we'd need the NWB geometry file to
geocode each accident — this is a future enhancement.

Data source: https://downloads.rijkswaterstaatdata.nl/bron/
Format: ZIP containing CSV

Input:  data_nl/intermediate/nl_school_master_geocoded.csv
Output: data_nl/intermediate/nl_schools_with_traffic.csv
"""

import io
import logging
import sys
import zipfile
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

# Use single-year file — 2023 is most recent complete year (~300MB)
BRON_URL = "https://downloads.rijkswaterstaatdata.nl/bron/01-01-2023_31-12-2023.zip"
BRON_ZIP_PATH = None  # Set by download function


def download_and_parse_bron(cache_path: Path) -> pd.DataFrame:
    """Download BRON accident data and aggregate per gemeente."""
    if cache_path.exists():
        logger.info(f"Loading cached BRON data: {cache_path.name}")
        return pd.read_csv(cache_path)

    # Download to disk — file is ~300MB, use curl for reliable resume
    zip_path = CACHE_DIR / "bron_2023.zip"
    expected_min_size = 250 * 1024 * 1024  # At least 250MB for a valid file

    if not zip_path.exists() or zip_path.stat().st_size < expected_min_size:
        logger.info(f"Downloading BRON accident data with curl (supports resume)...")
        import subprocess
        result = subprocess.run(
            ["curl", "-L", "-C", "-", "--retry", "5", "--retry-delay", "5",
             "-o", str(zip_path), BRON_URL],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            logger.warning(f"  curl failed: {result.stderr[:200]}")
            if zip_path.exists() and zip_path.stat().st_size < expected_min_size:
                logger.warning(f"  Incomplete download ({zip_path.stat().st_size / 1024 / 1024:.0f} MB). "
                               f"Run manually: curl -L -C - --retry 5 -o {zip_path} {BRON_URL}")
                return pd.DataFrame()
        logger.info(f"  Downloaded: {zip_path.stat().st_size / 1024 / 1024:.0f} MB")
    else:
        logger.info(f"  Using cached ZIP: {zip_path.name} ({zip_path.stat().st_size / 1024 / 1024:.0f} MB)")

    # Extract CSV from ZIP
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        logger.info(f"  ZIP contains: {csv_names}")
        if not csv_names:
            logger.warning("  No CSV in BRON ZIP!")
            return pd.DataFrame()

        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f, sep=";", low_memory=False, dtype=str, encoding="latin1")

    logger.info(f"  Raw BRON: {len(df)} accidents, {len(df.columns)} columns")
    logger.info(f"  Columns: {list(df.columns)[:15]}")

    # Find relevant columns (names vary by year)
    cols_lower = {c.lower(): c for c in df.columns}
    gemeente_col = next((cols_lower[k] for k in cols_lower if "gemeente" in k and "naam" in k), None)
    gemeente_code_col = next((cols_lower[k] for k in cols_lower if "gemeente" in k and ("code" in k or "nr" in k)), None)
    severity_col = next((cols_lower[k] for k in cols_lower if "ernst" in k or "afloop" in k or "severity" in k), None)

    logger.info(f"  Gemeente name col: {gemeente_col}")
    logger.info(f"  Gemeente code col: {gemeente_code_col}")
    logger.info(f"  Severity col: {severity_col}")

    # Aggregate per gemeente
    if gemeente_code_col:
        group_col = gemeente_code_col
    elif gemeente_col:
        group_col = gemeente_col
    else:
        # Try to find any gemeente-like column
        for c in df.columns:
            if "gme" in c.lower() or "gem" in c.lower():
                group_col = c
                break
        else:
            logger.warning("  Cannot find gemeente column in BRON data")
            logger.info(f"  All columns: {list(df.columns)}")
            return pd.DataFrame()

    agg = df.groupby(group_col).size().reset_index(name="accidents_total")
    agg.columns = ["gemeente_ref", "accidents_total"]

    # Count fatal/severe if severity column exists
    if severity_col:
        fatal_mask = df[severity_col].str.upper().isin(["DOD", "UMS", "DOOD", "DODELIJK"])
        agg_fatal = df[fatal_mask].groupby(group_col).size().reset_index(name="accidents_fatal")
        agg_fatal.columns = ["gemeente_ref", "accidents_fatal"]
        agg = agg.merge(agg_fatal, on="gemeente_ref", how="left")
        agg["accidents_fatal"] = agg["accidents_fatal"].fillna(0).astype(int)

    agg.to_csv(cache_path, index=False)
    logger.info(f"  Aggregated: {len(agg)} gemeenten with accident counts")
    return agg


def enrich_schools(schools: pd.DataFrame, accidents: pd.DataFrame) -> pd.DataFrame:
    """Join accident data to schools on gemeente."""
    if accidents.empty:
        logger.warning("No accident data — filling with NaN")
        schools["traffic_accidents_gemeente"] = np.nan
        schools["traffic_volume_index"] = np.nan
        schools["traffic_data_source"] = "BRON (no data)"
        return schools

    # Try joining on gemeente code first, then name
    schools["_gem_name"] = schools["gemeente_name"].astype(str).str.strip().str.upper()
    accidents["_gem_ref"] = accidents["gemeente_ref"].astype(str).str.strip().str.upper()

    # Also try numeric gemeente code
    schools["_gem_code"] = schools["gemeente_code"].astype(str).str.strip().str.zfill(4)
    accidents["_gem_code"] = accidents["gemeente_ref"].astype(str).str.strip().str.zfill(4)

    # Try code join first
    merged = schools.merge(
        accidents[["_gem_code", "accidents_total"]].rename(columns={"accidents_total": "traffic_accidents_gemeente"}),
        on="_gem_code", how="left"
    )

    code_filled = merged["traffic_accidents_gemeente"].notna().sum()

    # If code join got less than 50%, try name join
    if code_filled < len(schools) * 0.5:
        logger.info(f"  Code join got {code_filled}/{len(schools)}, trying name join...")
        merged2 = schools.merge(
            accidents[["_gem_ref", "accidents_total"]].rename(
                columns={"_gem_ref": "_gem_name", "accidents_total": "traffic_accidents_gemeente"}
            ),
            on="_gem_name", how="left"
        )
        name_filled = merged2["traffic_accidents_gemeente"].notna().sum()
        if name_filled > code_filled:
            merged = merged2
            logger.info(f"  Name join: {name_filled}/{len(schools)}")

    # Normalize to 0-10 index
    max_acc = merged["traffic_accidents_gemeente"].max()
    if max_acc and max_acc > 0:
        merged["traffic_volume_index"] = (merged["traffic_accidents_gemeente"] / max_acc * 10).round(1)
    else:
        merged["traffic_volume_index"] = np.nan

    merged["traffic_data_source"] = "BRON 2023 (Rijkswaterstaat)"
    merged["traffic_accidents_year"] = "2023"

    merged = merged.drop(columns=["_gem_name", "_gem_code"], errors="ignore")
    filled = merged["traffic_accidents_gemeente"].notna().sum()
    logger.info(f"Schools with traffic data: {filled}/{len(merged)}")
    return merged


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 3: Traffic Enrichment (BRON)")
    logger.info("=" * 60)

    input_path = INTERMEDIATE_DIR / "nl_school_master_geocoded.csv"
    if not input_path.exists():
        logger.error(f"Input not found: {input_path}"); sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools")

    accidents = download_and_parse_bron(CACHE_DIR / "bron_gemeente_2023.csv")
    enriched = enrich_schools(schools, accidents)

    output = INTERMEDIATE_DIR / "nl_schools_with_traffic.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
