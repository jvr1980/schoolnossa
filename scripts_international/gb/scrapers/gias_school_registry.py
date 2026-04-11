#!/usr/bin/env python3
"""
UK Phase 1: Download and parse school data from GIAS + DfE Performance Tables.

Downloads:
1. GIAS bulk extract — all establishments with URN, address, Ofsted, coordinates
2. KS4 (GCSE) performance data — Attainment 8, Progress 8, % Grade 5+ Eng & Maths
3. KS5 (A-Level) performance data — value added, average point scores
4. IMD 2025 — Index of Multiple Deprivation by LSOA

Outputs:
    data_gb/raw/gias_edubasealldata.csv
    data_gb/raw/dfe_ks4_performance.csv
    data_gb/raw/dfe_ks5_value_added.csv
    data_gb/raw/imd_2025_file7.csv
    data_gb/intermediate/gb_school_master_base.csv (merged)

Usage:
    python gias_school_registry.py
    python gias_school_registry.py --skip-download
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_gb"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

for d in [RAW_DIR, INTERMEDIATE_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "SchoolNossa/1.0 (school comparison platform)"}

# URLs
today = date.today().strftime("%Y%m%d")
URLS = {
    "gias": f"https://ea-edubase-api-prod.azurewebsites.net/edubase/edubasealldata{today}.csv",
    "ks4": "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/5b3d308c-da72-467f-b2ef-ab77d576a455/csv",
    "ks5_va": "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/ea4d23d7-b46b-4b94-bdc8-93e2d866a2f3/csv",
    "imd": "https://assets.publishing.service.gov.uk/media/691ded56d140bbbaa59a2a7d/File_7_-_All_IoD2025_Scores__Ranks__Deciles_and_Population_Denominators.csv",
    "accidents": "https://data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-collision-last-5-years.csv",
}


def download_file(url: str, output_path: Path, force: bool = False) -> Path:
    """Download a file from URL with caching."""
    if output_path.exists() and not force:
        size_kb = output_path.stat().st_size / 1024
        logger.info(f"  Cached: {output_path.name} ({size_kb:.0f} KB)")
        return output_path

    logger.info(f"  Downloading: {url[:80]}...")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        resp.raise_for_status()
        output_path.write_bytes(resp.content)
        logger.info(f"  Saved: {output_path.name} ({len(resp.content) / 1024:.0f} KB)")
        return output_path
    except requests.HTTPError as e:
        # GIAS URL with today's date may not exist yet — try yesterday
        if "gias" in str(output_path):
            yesterday = (date.today().replace(day=date.today().day - 1)).strftime("%Y%m%d")
            alt_url = f"https://ea-edubase-api-prod.azurewebsites.net/edubase/edubasealldata{yesterday}.csv"
            logger.info(f"  Retrying with yesterday's date: {yesterday}")
            resp = requests.get(alt_url, headers=HEADERS, timeout=120)
            resp.raise_for_status()
            output_path.write_bytes(resp.content)
            logger.info(f"  Saved: {output_path.name} ({len(resp.content) / 1024:.0f} KB)")
            return output_path
        raise


def download_all(force: bool = False) -> dict:
    """Download all datasets."""
    logger.info("Downloading UK school datasets...")
    files = {}
    for key, url in URLS.items():
        path = RAW_DIR / f"gb_{key}.csv"
        try:
            files[key] = download_file(url, path, force=force)
        except Exception as e:
            logger.warning(f"  Failed to download {key}: {e}")
            files[key] = None
    return files


def osgb_to_wgs84(easting, northing):
    """Convert OSGB36 (Easting/Northing) to WGS84 (lat/lon) using pyproj."""
    try:
        from pyproj import Transformer
        transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
        lon, lat = transformer.transform(easting, northing)
        return lat, lon
    except ImportError:
        # Fallback: approximate conversion (less accurate)
        # This is a rough polynomial approximation for GB
        lat = 49.766 + (northing - 100000) / 111320
        lon = -7.557 + (easting - 400000) / (111320 * np.cos(np.radians(lat)))
        return lat, lon


def load_gias(files: dict) -> pd.DataFrame:
    """Parse GIAS bulk extract into clean format."""
    logger.info("\nParsing GIAS data...")
    path = files.get("gias")
    if path is None or not path.exists():
        raise FileNotFoundError("GIAS file not found")

    # GIAS uses cp1252 encoding
    df = pd.read_csv(path, encoding="cp1252", low_memory=False, dtype=str)
    logger.info(f"  Raw GIAS: {len(df)} establishments, {len(df.columns)} columns")

    # Filter to open secondary schools in England
    # PhaseOfEducation: 4=Secondary, 5=16+, 7=All-through
    # EstablishmentStatus: 1=Open
    df_filtered = df[
        (df["PhaseOfEducation (code)"].isin(["4", "5", "7"]))
        & (df["EstablishmentStatus (code)"] == "1")
    ].copy()
    logger.info(f"  Filtered to open secondary/16+/all-through: {len(df_filtered)} schools")

    # Rename columns
    rename = {
        "URN": "urn",
        "EstablishmentName": "school_name",
        "TypeOfEstablishment (name)": "establishment_type",
        "TypeOfEstablishment (code)": "establishment_type_code",
        "PhaseOfEducation (name)": "phase",
        "Street": "street",
        "Town": "town",
        "Postcode": "postcode",
        "LA (name)": "local_authority",
        "LA (code)": "la_code",
        "LSOA (code)": "lsoa_code",
        "LSOA (name)": "lsoa_name",
        "SchoolWebsite": "website",
        "TelephoneNum": "phone",
        "HeadFirstName": "head_first_name",
        "HeadLastName": "head_last_name",
        "OfstedRating (name)": "ofsted_rating",
        "OfstedLastInsp": "ofsted_date",
        "NumberOfPupils": "number_of_pupils",
        "SchoolCapacity": "school_capacity",
        "Easting": "easting",
        "Northing": "northing",
        "ReligiousCharacter (name)": "religious_character",
        "Gender (name)": "gender_type",
        "AdmissionsPolicy (name)": "admissions_policy",
        "Trusts (name)": "trust_name",
        "PercentageFSM": "fsm_pct",
        "OpenDate": "open_date",
    }
    df_filtered = df_filtered.rename(columns=rename)

    # Convert coordinates
    logger.info("  Converting OSGB to WGS84 coordinates...")
    df_filtered["easting"] = pd.to_numeric(df_filtered["easting"], errors="coerce")
    df_filtered["northing"] = pd.to_numeric(df_filtered["northing"], errors="coerce")

    has_coords = df_filtered["easting"].notna() & df_filtered["northing"].notna()
    lats, lons = [], []
    for _, row in df_filtered.iterrows():
        if pd.notna(row["easting"]) and pd.notna(row["northing"]):
            lat, lon = osgb_to_wgs84(row["easting"], row["northing"])
            lats.append(lat)
            lons.append(lon)
        else:
            lats.append(None)
            lons.append(None)
    df_filtered["latitude"] = lats
    df_filtered["longitude"] = lons
    logger.info(f"  Coordinates: {sum(1 for l in lats if l is not None)}/{len(df_filtered)}")

    # Build principal name
    df_filtered["principal"] = (
        df_filtered["head_first_name"].fillna("") + " " + df_filtered["head_last_name"].fillna("")
    ).str.strip()

    # Numeric conversions
    df_filtered["number_of_pupils"] = pd.to_numeric(df_filtered["number_of_pupils"], errors="coerce")
    df_filtered["school_capacity"] = pd.to_numeric(df_filtered["school_capacity"], errors="coerce")
    df_filtered["fsm_pct"] = pd.to_numeric(df_filtered["fsm_pct"], errors="coerce")

    # Keep relevant columns
    keep_cols = [
        "urn", "school_name", "establishment_type", "phase", "street", "town",
        "postcode", "local_authority", "la_code", "lsoa_code", "website", "phone",
        "principal", "ofsted_rating", "ofsted_date", "number_of_pupils",
        "school_capacity", "latitude", "longitude", "religious_character",
        "gender_type", "admissions_policy", "trust_name", "fsm_pct",
    ]
    df_filtered = df_filtered[[c for c in keep_cols if c in df_filtered.columns]]

    logger.info(f"  Parsed: {len(df_filtered)} schools")
    return df_filtered


def load_ks4(files: dict) -> pd.DataFrame:
    """Parse KS4 (GCSE) performance data."""
    logger.info("\nParsing KS4 performance data...")
    path = files.get("ks4")
    if path is None or not path.exists():
        logger.warning("  KS4 file not found")
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False, dtype=str)
    logger.info(f"  Raw KS4: {len(df)} rows, {len(df.columns)} columns")

    # Filter to most recent year, school-level, all pupils
    if "time_period" in df.columns:
        most_recent = df["time_period"].max()
        df = df[df["time_period"] == most_recent]
        logger.info(f"  Filtered to period: {most_recent}")

    if "geographic_level" in df.columns:
        df = df[df["geographic_level"] == "School"]

    # Filter to overall (not by gender/disadvantage subgroups)
    for filter_col in ["sex", "disadvantage_status"]:
        if filter_col in df.columns:
            total_val = df[filter_col].mode().iloc[0] if len(df[filter_col].mode()) > 0 else "Total"
            df = df[df[filter_col] == total_val]

    # Extract key metrics
    result = pd.DataFrame()
    result["urn"] = df["school_urn"] if "school_urn" in df.columns else df.get("urn")

    for src, dst in [
        ("attainment8_average", "ks4_attainment8"),
        ("progress8_average", "ks4_progress8"),
    ]:
        if src in df.columns:
            result[dst] = pd.to_numeric(df[src], errors="coerce")

    result = result.drop_duplicates(subset=["urn"])
    logger.info(f"  KS4: {len(result)} schools with performance data")
    return result


def load_imd(files: dict) -> pd.DataFrame:
    """Parse IMD 2025 data at LSOA level."""
    logger.info("\nParsing IMD data...")
    path = files.get("imd")
    if path is None or not path.exists():
        logger.warning("  IMD file not found")
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False, dtype=str)
    logger.info(f"  Raw IMD: {len(df)} LSOAs, {len(df.columns)} columns")

    # Find the right column names (may vary between 2019 and 2025)
    cols = df.columns.tolist()
    lsoa_col = next((c for c in cols if "lsoa" in c.lower() and "code" in c.lower()), None)
    decile_col = next((c for c in cols if "decile" in c.lower() and "multiple" in c.lower()), None)
    rank_col = next((c for c in cols if "rank" in c.lower() and "multiple" in c.lower()), None)
    income_col = next((c for c in cols if "income" in c.lower() and "score" in c.lower()), None)
    crime_col = next((c for c in cols if "crime" in c.lower() and "score" in c.lower()), None)

    result = pd.DataFrame()
    if lsoa_col:
        result["lsoa_code"] = df[lsoa_col].str.strip()
    if decile_col:
        result["imd_decile"] = pd.to_numeric(df[decile_col], errors="coerce")
    if rank_col:
        result["imd_rank"] = pd.to_numeric(df[rank_col], errors="coerce")
    if income_col:
        result["imd_income_score"] = pd.to_numeric(df[income_col], errors="coerce")
    if crime_col:
        result["imd_crime_score"] = pd.to_numeric(df[crime_col], errors="coerce")

    result = result.drop_duplicates(subset=["lsoa_code"])
    logger.info(f"  IMD: {len(result)} LSOAs with deprivation data")
    return result


def merge_all(gias: pd.DataFrame, ks4: pd.DataFrame, imd: pd.DataFrame) -> pd.DataFrame:
    """Merge all datasets into school master table."""
    logger.info("\nMerging datasets...")
    master = gias.copy()

    # Merge KS4
    if not ks4.empty and "urn" in ks4.columns:
        master["urn"] = master["urn"].astype(str)
        ks4["urn"] = ks4["urn"].astype(str)
        master = master.merge(ks4, on="urn", how="left")
        filled = master["ks4_attainment8"].notna().sum() if "ks4_attainment8" in master.columns else 0
        logger.info(f"  + KS4: {filled}/{len(master)} schools with GCSE data")

    # Merge IMD via LSOA
    if not imd.empty and "lsoa_code" in master.columns:
        master["lsoa_code"] = master["lsoa_code"].astype(str).str.strip()
        master = master.merge(imd, on="lsoa_code", how="left")
        filled = master["imd_decile"].notna().sum() if "imd_decile" in master.columns else 0
        logger.info(f"  + IMD: {filled}/{len(master)} schools with deprivation data")

    logger.info(f"\n  Final: {len(master)} schools, {len(master.columns)} columns")
    return master


def main(skip_download: bool = False, force_download: bool = False):
    """Run the full UK Phase 1 pipeline."""
    logger.info("=" * 60)
    logger.info("UK Phase 1: GIAS + DfE + IMD School Data")
    logger.info("=" * 60)

    if skip_download:
        files = {key: RAW_DIR / f"gb_{key}.csv" for key in URLS}
    else:
        files = download_all(force=force_download)

    gias = load_gias(files)
    ks4 = load_ks4(files)
    imd = load_imd(files)

    master = merge_all(gias, ks4, imd)

    output_path = INTERMEDIATE_DIR / "gb_school_master_base.csv"
    master.to_csv(output_path, index=False)
    logger.info(f"\nSaved: {output_path}")

    # Quality report
    logger.info("\nDATA QUALITY:")
    for col in master.columns:
        n = master[col].notna().sum()
        pct = n / len(master) * 100
        if pct > 0:
            logger.info(f"  {'+' if pct > 50 else '~'} {col}: {n}/{len(master)} ({pct:.0f}%)")

    return master


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UK Phase 1: GIAS School Registry")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()
    main(skip_download=args.skip_download, force_download=args.force_download)
