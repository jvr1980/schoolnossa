#!/usr/bin/env python3
"""
Phase 1: Leipzig School Master Data Scraper
============================================

Downloads school master data from the Saechsische Schuldatenbank REST API
and enriches it with supplementary Leipzig Open Data.

Data Sources:
    1. Saechsische Schuldatenbank REST API
       URL: https://schuldatenbank.sachsen.de/api/v1/schools
       Format: CSV (comma-separated, UTF-8)
       Coordinates: WGS84 (EPSG:4326)
       Auth: None required

    2. Leipzig Open Data — School directory with Ortsteil mapping
       URL: https://opendata.leipzig.de/.../allgemeinbildende_schulen_leipzig_sj-2024_25_standorte_mit_adresse.csv

    3. Leipzig Open Data — Student numbers per school
       URL: https://opendata.leipzig.de/.../schuelerzahlen_allgemeinbildende-schulen_leipzig_seitsj2021_22.csv

API Parameters:
    format=csv              — CSV export
    address=Leipzig         — filter by city
    limit=500               — override default 20-row limit
    pre_registered=yes      — only active schools
    only_schools=yes        — exclude non-school institutions
    school_category_key=10  — allgemeinbildende Schulen only

School type keys (general education):
    11 = Grundschule
    12 = Oberschule
    13 = Gymnasium
    14 = Foerderschule

Output: data_leipzig/raw/leipzig_schools_raw.csv

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import io
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------
SCHULDATENBANK_API = "https://schuldatenbank.sachsen.de/api/v1/schools"

LEIPZIG_OPENDATA_DIRECTORY_URL = (
    "https://opendata.leipzig.de/dataset/"
    "b01f056d-d416-4a31-b351-a7ca58d1f9d9/resource/"
    "98565885-c0e9-4b33-813c-efdfc1073611/download/"
    "allgemeinbildende_schulen_leipzig_sj-2024_25_standorte_mit_adresse.csv"
)

LEIPZIG_OPENDATA_STUDENTS_URL = (
    "https://opendata.leipzig.de/dataset/"
    "1bb1c349-2259-450f-a5cb-d769a94a0e75/resource/"
    "5a76b7bb-d8a6-4b01-a1ba-3271a6ec72bf/download/"
    "schuelerzahlen_allgemeinbildende-schulen_leipzig_seitsj2021_22.csv"
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv, text/html, application/json, */*;q=0.8",
}

SCHOOL_TYPE_MAP = {
    "11": "Grundschule",
    "12": "Oberschule",
    "13": "Gymnasium",
    "14": "Foerderschule",
}

LEGAL_STATUS_MAP = {
    "1": "oeffentlich",
    "2": "frei/privat",
}

# Primary vs secondary classification
PRIMARY_TYPE_KEYS = ["11"]
SECONDARY_TYPE_KEYS = ["12", "13"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def ensure_directories():
    """Create necessary directories."""
    for d in [RAW_DIR, CACHE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def download_url(url: str, description: str, timeout: int = 120) -> bytes:
    """Download content from *url* and return raw bytes."""
    logger.info("Downloading %s from %s", description, url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        logger.info(
            "Downloaded %s (%s bytes)", description, f"{len(resp.content):,}"
        )
        return resp.content
    except requests.RequestException as exc:
        logger.error("Failed to download %s: %s", description, exc)
        raise


def cache_and_load(content: bytes, cache_path: Path, description: str) -> bytes:
    """Write *content* to *cache_path* and log it."""
    with open(cache_path, "wb") as fh:
        fh.write(content)
    logger.info("Cached %s -> %s", description, cache_path)
    return content


# ---------------------------------------------------------------------------
# 1. Fetch Schuldatenbank API
# ---------------------------------------------------------------------------
def fetch_schuldatenbank(use_cache: bool = True) -> pd.DataFrame:
    """GET from the Saechsische Schuldatenbank REST API and return a DataFrame."""
    cache_path = CACHE_DIR / "schuldatenbank_leipzig_raw.csv"

    if use_cache and cache_path.exists():
        logger.info("Using cached Schuldatenbank response: %s", cache_path)
        raw_bytes = cache_path.read_bytes()
    else:
        params = {
            "format": "csv",
            "address": "Leipzig",
            "limit": "500",
            "pre_registered": "yes",
            "only_schools": "yes",
            "school_category_key": "10",
        }
        raw_bytes = download_url(
            SCHULDATENBANK_API,
            "Schuldatenbank API (Leipzig, allgemeinbildend)",
            timeout=120,
        )
        # The API returns CSV when format=csv; request with params
        # Re-do with params in case the first call was a bare URL
        resp = requests.get(
            SCHULDATENBANK_API,
            params=params,
            headers=HEADERS,
            timeout=120,
        )
        resp.raise_for_status()
        raw_bytes = resp.content
        cache_and_load(raw_bytes, cache_path, "Schuldatenbank API response")

    # Parse CSV — comma-separated, UTF-8
    df = pd.read_csv(
        io.BytesIO(raw_bytes),
        sep=",",
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
    )
    logger.info(
        "Parsed Schuldatenbank CSV: %d rows, %d columns", len(df), len(df.columns)
    )
    logger.info("Columns: %s", list(df.columns))
    return df


# ---------------------------------------------------------------------------
# 2. Fetch Leipzig Open Data supplementary CSVs
# ---------------------------------------------------------------------------
def fetch_leipzig_directory(use_cache: bool = True) -> pd.DataFrame:
    """Download the Leipzig school directory CSV (Ortsteil mapping)."""
    cache_path = CACHE_DIR / "leipzig_opendata_directory.csv"

    if use_cache and cache_path.exists():
        logger.info("Using cached Leipzig directory: %s", cache_path)
        raw_bytes = cache_path.read_bytes()
    else:
        try:
            raw_bytes = download_url(
                LEIPZIG_OPENDATA_DIRECTORY_URL,
                "Leipzig Open Data school directory",
            )
            cache_and_load(raw_bytes, cache_path, "Leipzig directory CSV")
        except Exception as exc:
            logger.warning("Could not download Leipzig directory CSV: %s", exc)
            return pd.DataFrame()

    # Try multiple encodings and separators (German municipal CSVs vary)
    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(raw_bytes), sep=sep, dtype=str, encoding=enc,
                    on_bad_lines="skip",
                )
                if len(df.columns) >= 4:
                    logger.info(
                        "Parsed Leipzig directory CSV (%s/%s): %d rows, cols=%s",
                        enc, sep, len(df), list(df.columns)[:8],
                    )
                    return df
            except Exception:
                continue

    logger.warning("Could not parse Leipzig directory CSV with any encoding")
    return pd.DataFrame()


def fetch_leipzig_students(use_cache: bool = True) -> pd.DataFrame:
    """Download the Leipzig student-numbers CSV."""
    cache_path = CACHE_DIR / "leipzig_opendata_students.csv"

    if use_cache and cache_path.exists():
        logger.info("Using cached Leipzig students CSV: %s", cache_path)
        raw_bytes = cache_path.read_bytes()
    else:
        try:
            raw_bytes = download_url(
                LEIPZIG_OPENDATA_STUDENTS_URL,
                "Leipzig Open Data student numbers",
            )
            cache_and_load(raw_bytes, cache_path, "Leipzig students CSV")
        except Exception as exc:
            logger.warning("Could not download Leipzig students CSV: %s", exc)
            return pd.DataFrame()

    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(raw_bytes), sep=sep, dtype=str, encoding=enc,
                    on_bad_lines="skip",
                )
                if len(df.columns) >= 3:
                    logger.info(
                        "Parsed Leipzig students CSV (%s/%s): %d rows, cols=%s",
                        enc, sep, len(df), list(df.columns)[:8],
                    )
                    return df
            except Exception:
                continue

    logger.warning("Could not parse Leipzig students CSV with any encoding")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 3. Column mapping — Schuldatenbank API -> pipeline standard
# ---------------------------------------------------------------------------
def map_api_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Schuldatenbank API columns to the pipeline standard."""
    logger.info("Mapping API columns to pipeline standard...")
    df = df.copy()

    # Direct column renames  (API name -> pipeline name)
    rename_map = {
        "institution_number": "schulnummer",
        "name": "schulname",
        "street": "strasse",
        "postcode": "plz",
        "community": "ort",
        "mail": "email",
        "homepage": "website",
        "latitude": "lat",
        "longitude": "lon",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Build full address
    if "strasse" in df.columns and "plz" in df.columns and "ort" in df.columns:
        df["adresse"] = (
            df["strasse"].fillna("")
            + ", "
            + df["plz"].fillna("")
            + " "
            + df["ort"].fillna("")
        ).str.strip(", ")

    # School type
    if "school_type_keys" in df.columns:
        # The API may return a list-like string e.g. "11" or "11,12"
        # Take the first key for primary classification
        df["schultyp_key"] = df["school_type_keys"].str.split(",").str[0].str.strip()
        df["schultyp"] = df["schultyp_key"].map(SCHOOL_TYPE_MAP).fillna("Sonstige")
    elif "school_type_key" in df.columns:
        df["schultyp_key"] = df["school_type_key"].str.strip()
        df["schultyp"] = df["schultyp_key"].map(SCHOOL_TYPE_MAP).fillna("Sonstige")

    # Legal status / Traegerschaft
    if "legal_status_key" in df.columns:
        df["traegerschaft"] = (
            df["legal_status_key"].str.strip().map(LEGAL_STATUS_MAP).fillna("unbekannt")
        )
    elif "legal_status" in df.columns:
        df["traegerschaft"] = df["legal_status"]

    # Phone number
    phone_code = None
    phone_num = None
    for col in df.columns:
        if "phone_code" in col.lower() and "1" in col:
            phone_code = col
        if "phone_number" in col.lower() and "1" in col:
            phone_num = col
    if phone_code and phone_num:
        df["telefon"] = (
            df[phone_code].fillna("").str.strip()
            + df[phone_num].fillna("").str.strip()
        )
        df.loc[df["telefon"].str.strip() == "", "telefon"] = None

    # Schulleitung
    head_first = None
    head_last = None
    for col in df.columns:
        if "headmaster_firstname" in col.lower():
            head_first = col
        if "headmaster_lastname" in col.lower():
            head_last = col
    if head_first and head_last:
        df["schulleitung"] = (
            df[head_first].fillna("").str.strip()
            + " "
            + df[head_last].fillna("").str.strip()
        ).str.strip()
        df.loc[df["schulleitung"] == "", "schulleitung"] = None

    # Ensure lat/lon are numeric
    for col in ["lat", "lon"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean PLZ — 5 digits
    if "plz" in df.columns:
        df["plz"] = df["plz"].astype(str).str.strip().str.zfill(5)
        df.loc[df["plz"].str.len() != 5, "plz"] = None

    # Clean website
    if "website" in df.columns:
        def _clean_url(url):
            if pd.isna(url) or str(url).strip() in ("", "nan", "None"):
                return None
            url = str(url).strip()
            if not url.startswith("http"):
                url = "https://" + url
            return url

        df["website"] = df["website"].apply(_clean_url)

    # Classify primary/secondary
    if "schultyp_key" in df.columns:
        df["school_category"] = df["schultyp_key"].apply(
            lambda k: "primary"
            if k in PRIMARY_TYPE_KEYS
            else ("secondary" if k in SECONDARY_TYPE_KEYS else "other")
        )

    # Metadata
    df["data_source"] = "Saechsische Schuldatenbank API"
    df["data_retrieved"] = datetime.now().strftime("%Y-%m-%d")
    df["bundesland"] = "Sachsen"
    df["stadt"] = "Leipzig"

    logger.info("Column mapping complete. Columns: %s", list(df.columns))
    return df


# ---------------------------------------------------------------------------
# 4. Merge supplementary Leipzig Open Data
# ---------------------------------------------------------------------------
def _find_column(df, candidates):
    """Return the first column name from *candidates* that exists (case-insensitive)."""
    col_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in col_lower:
            return col_lower[cand.lower()]
    return None


def merge_directory(schools_df: pd.DataFrame, dir_df: pd.DataFrame) -> pd.DataFrame:
    """Merge Stadtbezirk and Ortsteil from the Leipzig directory CSV."""
    if dir_df.empty:
        logger.warning("No Leipzig directory data to merge")
        return schools_df

    logger.info("Merging Leipzig directory (Ortsteil/Stadtbezirk)...")

    # Find join key in directory data
    dir_id_col = _find_column(dir_df, [
        "Schul_ID", "schul_id", "Schulnummer", "schulnummer",
        "Schul-ID", "ID",
    ])
    dir_name_col = _find_column(dir_df, [
        "Schulname", "schulname", "Name", "name", "Schule",
    ])

    # Find Ortsteil / Stadtbezirk columns
    ortsteil_col = _find_column(dir_df, [
        "Ortsteil", "ortsteil", "OT",
    ])
    bezirk_col = _find_column(dir_df, [
        "Stadtbezirk", "stadtbezirk", "Bezirk", "bezirk",
    ])

    cols_to_merge = {}
    if ortsteil_col:
        cols_to_merge[ortsteil_col] = "ortsteil"
    if bezirk_col:
        cols_to_merge[bezirk_col] = "stadtbezirk"

    if not cols_to_merge:
        logger.warning("No Ortsteil/Stadtbezirk columns found in directory CSV")
        return schools_df

    # Try merging on ID first, then on name
    merged = schools_df
    match_count = 0

    if dir_id_col and "schulnummer" in schools_df.columns:
        dir_merge = dir_df[[dir_id_col] + list(cols_to_merge.keys())].copy()
        dir_merge = dir_merge.rename(columns={dir_id_col: "schulnummer", **cols_to_merge})
        dir_merge["schulnummer"] = dir_merge["schulnummer"].astype(str).str.strip()
        schools_df["schulnummer"] = schools_df["schulnummer"].astype(str).str.strip()

        merged = schools_df.merge(dir_merge, on="schulnummer", how="left")
        match_count = merged["ortsteil"].notna().sum() if "ortsteil" in merged.columns else 0

    # Fallback: fuzzy name matching if ID merge yielded few results
    if match_count < len(schools_df) * 0.5 and dir_name_col and "schulname" in schools_df.columns:
        logger.info("ID merge yielded %d matches; trying name-based merge...", match_count)
        dir_merge = dir_df[[dir_name_col] + list(cols_to_merge.keys())].copy()
        dir_merge = dir_merge.rename(columns={dir_name_col: "_dir_name", **cols_to_merge})
        dir_merge["_dir_name_clean"] = dir_merge["_dir_name"].str.lower().str.strip()

        merged["_school_name_clean"] = merged["schulname"].str.lower().str.strip()

        name_lookup = dir_merge.set_index("_dir_name_clean")
        for idx, row in merged.iterrows():
            if pd.notna(row.get("ortsteil")):
                continue
            name = row.get("_school_name_clean", "")
            if name in name_lookup.index:
                match = name_lookup.loc[name]
                if isinstance(match, pd.DataFrame):
                    match = match.iloc[0]
                if "ortsteil" in cols_to_merge.values():
                    merged.at[idx, "ortsteil"] = match.get("ortsteil")
                if "stadtbezirk" in cols_to_merge.values():
                    merged.at[idx, "stadtbezirk"] = match.get("stadtbezirk")

        merged = merged.drop(columns=["_school_name_clean"], errors="ignore")

    final_count = (
        merged["ortsteil"].notna().sum() if "ortsteil" in merged.columns else 0
    )
    logger.info("Directory merge: matched Ortsteil for %d/%d schools", final_count, len(merged))
    return merged


def merge_student_numbers(schools_df: pd.DataFrame, stu_df: pd.DataFrame) -> pd.DataFrame:
    """Merge student counts from the Leipzig Open Data students CSV."""
    if stu_df.empty:
        logger.warning("No Leipzig student numbers data to merge")
        return schools_df

    logger.info("Merging Leipzig student numbers...")

    # Find join key
    stu_id_col = _find_column(stu_df, [
        "Schul_ID", "schul_id", "Schulnummer", "schulnummer",
        "Schul-ID", "ID",
    ])
    stu_name_col = _find_column(stu_df, [
        "Schulname", "schulname", "Name", "name", "Schule",
    ])

    # Find the most recent student count column
    # Columns may look like "SJ2024/25" or "Schuelerzahl_2024_25" etc.
    count_cols = [
        c for c in stu_df.columns
        if any(kw in c.lower() for kw in ["2024", "2023", "schueler", "anzahl", "gesamt"])
    ]
    if not count_cols:
        # Fallback: take the last numeric-looking column
        count_cols = [c for c in stu_df.columns if c not in [stu_id_col, stu_name_col]]

    if not count_cols:
        logger.warning("No student count columns found")
        return schools_df

    # Use the last column as "most recent"
    count_col = count_cols[-1]
    logger.info("Using student count column: %s", count_col)

    # Merge on ID
    if stu_id_col and "schulnummer" in schools_df.columns:
        stu_merge = stu_df[[stu_id_col, count_col]].copy()
        stu_merge = stu_merge.rename(columns={stu_id_col: "schulnummer", count_col: "schueler_gesamt"})
        stu_merge["schulnummer"] = stu_merge["schulnummer"].astype(str).str.strip()
        stu_merge["schueler_gesamt"] = pd.to_numeric(stu_merge["schueler_gesamt"], errors="coerce")

        # De-duplicate — keep latest / largest
        stu_merge = stu_merge.dropna(subset=["schueler_gesamt"])
        stu_merge = stu_merge.sort_values("schueler_gesamt", ascending=False).drop_duplicates("schulnummer")

        schools_df["schulnummer"] = schools_df["schulnummer"].astype(str).str.strip()
        merged = schools_df.merge(stu_merge, on="schulnummer", how="left")

        match_count = merged["schueler_gesamt"].notna().sum()
        logger.info("Student numbers merge: matched %d/%d schools", match_count, len(merged))
        return merged

    # Fallback: try on name
    if stu_name_col and "schulname" in schools_df.columns:
        stu_merge = stu_df[[stu_name_col, count_col]].copy()
        stu_merge.columns = ["_stu_name", "schueler_gesamt"]
        stu_merge["_stu_name_clean"] = stu_merge["_stu_name"].str.lower().str.strip()
        stu_merge["schueler_gesamt"] = pd.to_numeric(stu_merge["schueler_gesamt"], errors="coerce")
        stu_merge = stu_merge.dropna(subset=["schueler_gesamt"])
        stu_merge = stu_merge.sort_values("schueler_gesamt", ascending=False).drop_duplicates("_stu_name_clean")

        name_map = stu_merge.set_index("_stu_name_clean")["schueler_gesamt"]
        schools_df["schueler_gesamt"] = (
            schools_df["schulname"].str.lower().str.strip().map(name_map)
        )
        match_count = schools_df["schueler_gesamt"].notna().sum()
        logger.info("Student numbers merge (name): matched %d/%d schools", match_count, len(schools_df))

    return schools_df


# ---------------------------------------------------------------------------
# 5. Split + save
# ---------------------------------------------------------------------------
def split_by_school_type(df: pd.DataFrame):
    """Split into primary and secondary DataFrames."""
    logger.info("Splitting by school type...")

    if "schultyp_key" not in df.columns:
        logger.warning("No schultyp_key column — returning all as secondary")
        return pd.DataFrame(), df

    primary = df[df["schultyp_key"].isin(PRIMARY_TYPE_KEYS)].copy()
    secondary = df[df["schultyp_key"].isin(SECONDARY_TYPE_KEYS)].copy()
    other = df[~df["schultyp_key"].isin(PRIMARY_TYPE_KEYS + SECONDARY_TYPE_KEYS)].copy()

    logger.info(
        "Primary: %d | Secondary: %d | Other (Foerderschule etc.): %d",
        len(primary), len(secondary), len(other),
    )
    return primary, secondary


def save_outputs(all_df: pd.DataFrame, primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    """Save processed outputs."""
    logger.info("Saving output files...")

    # All schools
    all_path = RAW_DIR / "leipzig_schools_raw.csv"
    all_df.to_csv(all_path, index=False, encoding="utf-8-sig")
    logger.info("Saved: %s (%d schools)", all_path, len(all_df))

    # Primary
    if not primary_df.empty:
        pri_path = RAW_DIR / "leipzig_primary_schools.csv"
        primary_df.to_csv(pri_path, index=False, encoding="utf-8-sig")
        logger.info("Saved: %s (%d schools)", pri_path, len(primary_df))

    # Secondary
    if not secondary_df.empty:
        sec_path = RAW_DIR / "leipzig_secondary_schools.csv"
        secondary_df.to_csv(sec_path, index=False, encoding="utf-8-sig")
        logger.info("Saved: %s (%d schools)", sec_path, len(secondary_df))


# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
def print_summary(all_df: pd.DataFrame, primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    """Print summary statistics."""
    print("\n" + "=" * 70)
    print("LEIPZIG SCHOOL MASTER DATA SCRAPER - PHASE 1 COMPLETE")
    print("=" * 70)

    print(f"\nTotal allgemeinbildende Schulen: {len(all_df)}")
    print(f"  Primary (Grundschule):  {len(primary_df)}")
    print(f"  Secondary (Oberschule + Gymnasium): {len(secondary_df)}")
    print(f"  Other (Foerderschule etc.): {len(all_df) - len(primary_df) - len(secondary_df)}")

    if "schultyp" in all_df.columns:
        print("\nBy school type:")
        for st, count in all_df["schultyp"].value_counts().items():
            print(f"  - {st}: {count}")

    if "traegerschaft" in all_df.columns:
        print("\nBy operator (Traegerschaft):")
        for t, count in all_df["traegerschaft"].value_counts().items():
            print(f"  - {t}: {count}")

    if "lat" in all_df.columns:
        coord_count = all_df["lat"].notna().sum()
        pct = 100 * coord_count / len(all_df) if len(all_df) else 0
        print(f"\nCoordinate coverage: {coord_count}/{len(all_df)} ({pct:.0f}%)")

    if "ortsteil" in all_df.columns:
        ot_count = all_df["ortsteil"].notna().sum()
        pct = 100 * ot_count / len(all_df) if len(all_df) else 0
        print(f"Ortsteil coverage:   {ot_count}/{len(all_df)} ({pct:.0f}%)")

    if "schueler_gesamt" in all_df.columns:
        stu_count = all_df["schueler_gesamt"].notna().sum()
        pct = 100 * stu_count / len(all_df) if len(all_df) else 0
        print(f"Student numbers:     {stu_count}/{len(all_df)} ({pct:.0f}%)")

    if "website" in all_df.columns:
        web_count = all_df["website"].notna().sum()
        pct = 100 * web_count / len(all_df) if len(all_df) else 0
        print(f"Website coverage:    {web_count}/{len(all_df)} ({pct:.0f}%)")

    if "email" in all_df.columns:
        email_count = all_df["email"].notna().sum()
        pct = 100 * email_count / len(all_df) if len(all_df) else 0
        print(f"Email coverage:      {email_count}/{len(all_df)} ({pct:.0f}%)")

    print(f"\nColumns ({len(all_df.columns)}): {list(all_df.columns)}")
    print("=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    """Download and process Leipzig school master data."""
    logger.info("=" * 60)
    logger.info("Starting Leipzig School Master Data Scraper (Phase 1)")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # 1. Fetch main data from Schuldatenbank API
        schools_df = fetch_schuldatenbank(use_cache=True)

        if schools_df.empty:
            logger.error("No data returned from Schuldatenbank API")
            sys.exit(1)

        # 2. Map API columns to pipeline standard
        schools_df = map_api_columns(schools_df)

        # 3. Fetch supplementary Leipzig Open Data
        dir_df = fetch_leipzig_directory(use_cache=True)
        stu_df = fetch_leipzig_students(use_cache=True)

        # 4. Merge supplementary data
        schools_df = merge_directory(schools_df, dir_df)
        schools_df = merge_student_numbers(schools_df, stu_df)

        # 5. Split by school type and save
        primary_df, secondary_df = split_by_school_type(schools_df)
        save_outputs(schools_df, primary_df, secondary_df)

        # 6. Summary
        print_summary(schools_df, primary_df, secondary_df)

        logger.info("Phase 1 complete!")
        return schools_df

    except Exception as exc:
        logger.error("Scraper failed: %s", exc)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
