#!/usr/bin/env python3
"""
Frankfurt Verzeichnis 6 Enrichment (Phase 2 — optional)

Joins Hessen Verzeichnis 6 data into the Schulwegweiser-based raw CSVs to add:
  - schulnummer   : official 4-digit HKM school ID (primary key for Berlin schema)
  - ndh_count     : non-German native language student count (belastungsstufe proxy)

Matching: fuzzy name match (SequenceMatcher ≥ 0.75) + PLZ cross-check.
Schools without a Verzeichnis 6 match get a generated ID: "SW-{slug}".

Input:
  data_frankfurt/raw/frankfurt_primary_schools.csv    (from Phase 1)
  data_frankfurt/raw/frankfurt_secondary_schools.csv
  data_frankfurt/raw/frankfurt_vocational_schools.csv (optional)

Output: writes schulnummer + ndh_count back into the same raw CSVs.

Author: Frankfurt School Data Pipeline
Created: 2026-04-06
"""

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR   = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR     = PROJECT_ROOT / "data_frankfurt"
RAW_DIR      = DATA_DIR / "raw"
CACHE_DIR    = DATA_DIR / "cache"

HESSEN_VERZ6_URL = (
    "https://statistik.hessen.de/sites/statistik.hessen.de/files/2025-09/verz-6_25_0.xlsx"
)
VERZ6_CACHE = CACHE_DIR / "verz6_excel.xlsx"

# Prior year — Verz6 2024 = school year 2023/24
HESSEN_VERZ6_PRIOR_URLS = [
    "https://statistik.hessen.de/sites/statistik.hessen.de/files/2024-09/verz-6_24_0.xlsx",
    "https://statistik.hessen.de/sites/statistik.hessen.de/files/2024-08/verz-6_24_0.xlsx",
    "https://statistik.hessen.de/sites/statistik.hessen.de/files/2024-10/verz-6_24_0.xlsx",
    "https://statistik.hessen.de/sites/statistik.hessen.de/files/2024-11/verz-6_24_0.xlsx",
    "https://statistik.hessen.de/sites/statistik.hessen.de/files/2025-03/verz-6_24_0.xlsx",
]
VERZ6_PRIOR_CACHE = CACHE_DIR / "verz6_excel_2024.xlsx"

# Frankfurt Landkreis code in Verzeichnis 6
FFM_LANDKREIS = 412


# ── Download + parse Verzeichnis 6 ───────────────────────────────────────────

def download_verz6() -> Path:
    """Download Verzeichnis 6 Excel to cache. Returns local path."""
    if VERZ6_CACHE.exists():
        logger.info(f"  Using cached Verzeichnis 6: {VERZ6_CACHE}")
        return VERZ6_CACHE

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"  Downloading Verzeichnis 6 from {HESSEN_VERZ6_URL}...")
    r = requests.get(HESSEN_VERZ6_URL, timeout=60, stream=True)
    r.raise_for_status()
    with open(VERZ6_CACHE, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
    logger.info(f"  Saved: {VERZ6_CACHE} ({VERZ6_CACHE.stat().st_size:,} bytes)")
    return VERZ6_CACHE


def download_verz6_prior():
    """Try to download previous year's Verz6 (2023/24) from known candidate URLs."""
    if VERZ6_PRIOR_CACHE.exists():
        logger.info(f"  Using cached prior-year Verzeichnis 6: {VERZ6_PRIOR_CACHE}")
        return VERZ6_PRIOR_CACHE

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for url in HESSEN_VERZ6_PRIOR_URLS:
        try:
            logger.info(f"  Trying prior-year Verz6: {url}")
            r = requests.get(url, timeout=30, stream=True)
            if r.status_code == 200:
                with open(VERZ6_PRIOR_CACHE, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
                logger.info(f"  Saved prior-year Verz6: {VERZ6_PRIOR_CACHE}")
                return VERZ6_PRIOR_CACHE
        except Exception as e:
            logger.debug(f"  Failed {url}: {e}")

    logger.warning("  Prior-year Verz6 not found at any known URL — schueler_2023_24 will remain empty")
    return None


def load_verz6(path_override=None) -> pd.DataFrame:
    """Load and parse Verzeichnis 6 Excel for Frankfurt schools.

    The Excel file has multiple sheets; school data is in 'Schulverzeichnis'.
    Row 0 is the header row with German column names.
    Pass path_override to load a different year's file (e.g. prior year).
    """
    path = path_override or download_verz6()

    # Data is in the 'Schulverzeichnis' sheet, header in row 0
    df = pd.read_excel(path, sheet_name="Schulverzeichnis", header=0, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    # Rename Hessen-specific column names to internal names
    renames = {
        "Schul-nummer":    "schulnummer",
        "Landkreis":       "landkreis",
        "Name der Schule": "schulname",
        "PLZ":             "plz_verz6",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})

    # Filter to Frankfurt (Landkreis 412)
    if "landkreis" in df.columns:
        df = df[df["landkreis"] == FFM_LANDKREIS].copy()
    else:
        logger.warning("  No 'landkreis' column found — not filtering by city")

    # ndH column — "Nichtdeutscher Herkunfts-\nsprache" or similar
    ndh_cols = [c for c in df.columns
                if "nichtdeutsch" in c.lower() or "herkunft" in c.lower()
                or "ndh" in c.lower()]
    if ndh_cols:
        df = df.rename(columns={ndh_cols[0]: "ndh_count"})

    # Student total — "Schülerinnen und Schüler insgesamt ohne Vorklassen"
    # Verz6 Ausgabe 2025 = school year 2024/25
    schueler_col = next(
        (c for c in df.columns if "schüler" in c.lower() and "insgesamt" in c.lower()
         and "ohne" in c.lower()),
        None
    )
    if schueler_col:
        df = df.rename(columns={schueler_col: "schueler_verz6"})
        logger.info(f"  Found student count column: '{schueler_col}'")

    keep = ["schulnummer", "schulname", "plz_verz6"]
    if "ndh_count" in df.columns:
        keep.append("ndh_count")
    if "schueler_verz6" in df.columns:
        keep.append("schueler_verz6")

    available = [c for c in keep if c in df.columns]
    df = df[available].dropna(subset=["schulnummer", "schulname"])
    df["schulnummer"] = df["schulnummer"].apply(
        lambda x: str(int(float(x))).strip() if pd.notna(x) else None
    )
    df["schulname"] = df["schulname"].astype(str).str.strip()

    logger.info(f"  Loaded {len(df)} Frankfurt schools from Verzeichnis 6")
    return df


# ── Fuzzy matching ────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    name = str(name).lower().strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"\s*(frankfurt\s*(am\s*main)?|ffm)$", "", name)
    return name


def best_match(sw_name, sw_plz, verz6_df):
    """Return (schulnummer, score) of best Verzeichnis 6 match, or (None, 0)."""
    best_score = 0.0
    best_nr    = None
    sw_norm    = normalize(sw_name)

    for _, row in verz6_df.iterrows():
        score = SequenceMatcher(None, sw_norm, normalize(str(row["schulname"]))).ratio()
        # Boost if PLZ matches
        if sw_plz and "plz_verz6" in row and str(row["plz_verz6"]).strip() == str(sw_plz).strip():
            score = min(1.0, score + 0.05)
        if score > best_score:
            best_score = score
            best_nr    = str(row["schulnummer"])

    return (best_nr, best_score)


# ── Enrich one CSV file ───────────────────────────────────────────────────────

def enrich_file(csv_path: Path, verz6_df: pd.DataFrame) -> pd.DataFrame:
    if not csv_path.exists():
        logger.warning(f"  Not found: {csv_path}, skipping")
        return None

    df = pd.read_csv(csv_path)
    logger.info(f"  Enriching {csv_path.name} ({len(df)} schools)...")

    # Ensure columns exist
    for col in ["schulnummer", "ndh_count"]:
        if col not in df.columns:
            df[col] = None

    has_schueler = "schueler_verz6" in verz6_df.columns
    schueler_filled = 0
    matched = 0
    generated = 0

    for idx, row in df.iterrows():
        sw_name = str(row.get("schulname", ""))
        sw_plz  = str(row.get("plz", ""))

        # Skip schulnummer matching if already has a real one; still try to fill stats
        existing = str(row.get("schulnummer", "") or "")
        needs_nr = not existing or existing.startswith("SW-") or existing in {"nan", "None", ""}

        if needs_nr:
            nr, score = best_match(sw_name, sw_plz, verz6_df)
        else:
            # Already has schulnummer — find its Verz6 row directly
            nr = existing
            score = 1.0

        if score >= 0.75 and nr:
            match_row = verz6_df[verz6_df["schulnummer"] == nr]
            if not match_row.empty:
                if needs_nr:
                    df.at[idx, "schulnummer"] = nr
                    matched += 1
                # Fill ndh_count if missing
                if "ndh_count" in verz6_df.columns:
                    current_ndh = df.at[idx, "ndh_count"] if "ndh_count" in df.columns else None
                    if pd.isna(current_ndh) or current_ndh in {None, ""}:
                        df.at[idx, "ndh_count"] = match_row.iloc[0]["ndh_count"]
                # Fill schueler_2024_25 from Verz6 student count (official, preferred over web)
                if has_schueler:
                    verz_val = match_row.iloc[0]["schueler_verz6"]
                    if pd.notna(verz_val):
                        if "schueler_2024_25" not in df.columns:
                            df["schueler_2024_25"] = None
                        # Verz6 is authoritative — overwrite even if web value exists
                        df.at[idx, "schueler_2024_25"] = int(verz_val)
                        schueler_filled += 1
            elif needs_nr:
                slug = str(row.get("sw_portal_slug", "")) or re.sub(r"[^a-z0-9-]", "-", sw_name.lower())
                df.at[idx, "schulnummer"] = f"SW-{slug}"
                generated += 1
        elif needs_nr:
            slug = str(row.get("sw_portal_slug", "")) or re.sub(r"[^a-z0-9-]", "-", sw_name.lower())
            df.at[idx, "schulnummer"] = f"SW-{slug}"
            generated += 1
            logger.debug(f"    No match for {sw_name!r} (best={score:.2f}) → generated ID")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"  schulnummer: {matched} from Verz6 + {generated} generated")
    if has_schueler:
        logger.info(f"  schueler_2024_25: {schueler_filled} filled from Verz6 (official)")
    return df


def backfill_prior_year(csv_path: Path, prior_df: pd.DataFrame):
    """Backfill schueler_2023_24 from prior-year Verz6 where missing."""
    if not csv_path.exists():
        return
    df = pd.read_csv(csv_path)
    if "schulnummer" not in df.columns:
        return

    filled = 0
    if "schueler_2023_24" not in df.columns:
        df["schueler_2023_24"] = None

    for idx, row in df.iterrows():
        nr = str(row.get("schulnummer", "") or "")
        if not nr or nr.startswith("SW-") or nr in {"nan", "None", ""}:
            continue
        current = df.at[idx, "schueler_2023_24"]
        if pd.notna(current):
            continue
        match = prior_df[prior_df["schulnummer"] == nr]
        if not match.empty and pd.notna(match.iloc[0].get("schueler_verz6")):
            df.at[idx, "schueler_2023_24"] = int(match.iloc[0]["schueler_verz6"])
            filled += 1

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"  schueler_2023_24: {filled} filled from prior-year Verz6 → {csv_path.name}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Verzeichnis 6 Enrichment (schulnummer + ndH + schueler)")
    logger.info("=" * 60)

    verz6 = load_verz6()

    fnames = ["frankfurt_primary_schools.csv",
              "frankfurt_secondary_schools.csv",
              "frankfurt_vocational_schools.csv"]

    for fname in fnames:
        logger.info(f"\n── {fname} ──")
        enrich_file(RAW_DIR / fname, verz6)

    # Prior-year Verz6 for schueler_2023_24
    logger.info("\n── Prior-year Verz6 (2023/24) ──")
    prior_path = download_verz6_prior()
    if prior_path:
        prior_df = load_verz6(path_override=prior_path)
        for fname in fnames:
            backfill_prior_year(RAW_DIR / fname, prior_df)

    logger.info("\nVerzeichnis 6 enrichment complete.")


if __name__ == "__main__":
    main()
