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


def load_verz6() -> pd.DataFrame:
    """Load and parse Verzeichnis 6 Excel for Frankfurt schools."""
    path = download_verz6()
    raw = pd.read_excel(path, header=None, engine="openpyxl")

    # Find the header row (contains 'schulname' or 'Schulname' or 'schulnummer')
    header_row = None
    for i, row in raw.iterrows():
        vals = [str(v).lower() for v in row.values if pd.notna(v)]
        if any("schulname" in v or "schul-nummer" in v or "schulnummer" in v for v in vals):
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find header row in Verzeichnis 6 Excel")

    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    # Rename Hessen-specific column names
    renames = {
        "Schul-nummer": "schulnummer",
        "Landkreis":    "landkreis",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})

    # Filter Frankfurt
    if "landkreis" in df.columns:
        df = df[df["landkreis"] == FFM_LANDKREIS].copy()
    else:
        logger.warning("  No 'landkreis' column found — not filtering by city")

    # Keep only useful columns
    keep = ["schulnummer", "schulname"]
    # ndH column — typically titled with "ndH" or similar
    ndh_cols = [c for c in df.columns if "ndh" in c.lower() or "nichtdeutsch" in c.lower() or "ndH" in str(c)]
    if ndh_cols:
        keep += ndh_cols[:1]
        df = df.rename(columns={ndh_cols[0]: "ndh_count"})
    # PLZ
    plz_cols = [c for c in df.columns if "plz" in c.lower() or "postleitzahl" in c.lower()]
    if plz_cols:
        keep.append(plz_cols[0])
        df = df.rename(columns={plz_cols[0]: "plz_verz6"})

    available = [c for c in keep if c in df.columns]
    df = df[available].dropna(subset=["schulnummer", "schulname"])
    df["schulnummer"] = df["schulnummer"].astype(str).str.strip()
    df["schulname"]   = df["schulname"].astype(str).str.strip()

    logger.info(f"  Loaded {len(df)} Frankfurt schools from Verzeichnis 6")
    return df


# ── Fuzzy matching ────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    name = str(name).lower().strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"\s*(frankfurt\s*(am\s*main)?|ffm)$", "", name)
    return name


def best_match(sw_name: str, sw_plz: str, verz6_df: pd.DataFrame) -> tuple[str | None, float]:
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

    matched = 0
    generated = 0

    for idx, row in df.iterrows():
        # Skip if already has a real schulnummer
        existing = str(row.get("schulnummer", "") or "")
        if existing and not existing.startswith("SW-") and existing not in {"nan", "None", ""}:
            continue

        sw_name = str(row.get("schulname", ""))
        sw_plz  = str(row.get("plz", ""))
        nr, score = best_match(sw_name, sw_plz, verz6_df)

        if score >= 0.75 and nr:
            df.at[idx, "schulnummer"] = nr
            # Also pull ndh_count if available
            if "ndh_count" in verz6_df.columns:
                match_row = verz6_df[verz6_df["schulnummer"] == nr]
                if not match_row.empty:
                    df.at[idx, "ndh_count"] = match_row.iloc[0]["ndh_count"]
            matched += 1
            logger.debug(f"    Matched {sw_name!r} → {nr} (score={score:.2f})")
        else:
            # Generate stable ID from portal slug
            slug = str(row.get("sw_portal_slug", "")) or re.sub(r"[^a-z0-9-]", "-", sw_name.lower())
            df.at[idx, "schulnummer"] = f"SW-{slug}"
            generated += 1
            logger.debug(f"    No match for {sw_name!r} (best={score:.2f}) → generated ID")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"  schulnummer: {matched} from Verz6 + {generated} generated → saved")
    return df


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Verzeichnis 6 Enrichment (schulnummer + ndH)")
    logger.info("=" * 60)

    verz6 = load_verz6()

    for fname in ["frankfurt_primary_schools.csv",
                  "frankfurt_secondary_schools.csv",
                  "frankfurt_vocational_schools.csv"]:
        logger.info(f"\n── {fname} ──")
        enrich_file(RAW_DIR / fname, verz6)

    logger.info("\nVerzeichnis 6 enrichment complete.")


if __name__ == "__main__":
    main()
