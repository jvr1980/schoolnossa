#!/usr/bin/env python3
"""
Phase 4 — ingest the 42 private-school candidates into the live Munich
master tables (raw + final + berlin_schema, primary AND/OR secondary
depending on the candidate's schulart).

Input:
  data_munich/intermediate/private_research/munich_private_schools_to_ingest.csv

What this does (local only — does NOT touch Supabase):
  1. Geocode rows that lack lat/lng via Places Text Search (reuses
     GOOGLE_PLACES_API_KEY).
  2. Generate stable schulnummern: `MUCPRIV_{prim|sec}_{slug}` where
     slug is a 12-char hash of the source-name+source_url. Same school
     in both primary and secondary tables shares a base slug.
  3. Map candidate fields onto the existing Munich raw/final schemas.
  4. APPEND to the existing CSVs and parquets in place. Pre-existing
     rows untouched. Rows that already match by schulname+plz+ort are
     skipped (idempotent).
  5. Re-write the parquet siblings of the CSVs.

Outputs (modified in place):
  data_munich/raw/munich_{primary,secondary}_schools_raw.csv
  data_munich/final/munich_{primary,secondary}_school_master_table.csv
  data_munich/final/munich_{primary,secondary}_school_master_table.parquet
  data_munich/final/munich_{primary,secondary}_school_master_table_final.csv
  data_munich/final/munich_{primary,secondary}_school_master_table_final_with_embeddings.parquet
  data_munich/final/munich_{primary,secondary}_school_master_table_berlin_schema.csv
  data_munich/final/munich_{primary,secondary}_school_master_table_berlin_schema.parquet

After running, the new schools are present but their enrichment columns
(POI, transit, crime, etc.) are empty — Step 3b will run the enrichment
chain.

Usage:
    python3 scripts_munich/research_private_schools/phase4_ingest_to_pipeline.py --dry-run
    python3 scripts_munich/research_private_schools/phase4_ingest_to_pipeline.py
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

INPUT = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research" / "munich_private_schools_to_ingest.csv"

RAW_DIR = PROJECT_ROOT / "data_munich" / "raw"
FINAL_DIR = PROJECT_ROOT / "data_munich" / "final"
GEOCODE_CACHE = PROJECT_ROOT / "data_munich" / "cache" / "private_research" / "geocode.json"
GEOCODE_CACHE.parent.mkdir(parents=True, exist_ok=True)

API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")
PLACES_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_FIELDS = "places.id,places.location,places.formattedAddress,places.types"

CENTER = {"latitude": 48.1374, "longitude": 11.5755}

DATA_SOURCE_LABEL = "private-schools-research-2026-04 (OSM + km.bayern + Places + Gemini)"

# Map candidate -> primary/secondary table school_type label.
# Munich's public rows (Bavarian open data) use the plural labels
# 'Gymnasien' / 'Realschulen' / 'Mittelschulen' / 'Grundschulen' and the
# Lovable filter UI keys on exactly those strings, so private schools must use
# the same vocabulary instead of the generic 'Sekundarschule' placeholder.
# Pedagogical concepts without a Bavarian bucket get the cross-city canonical
# names (Waldorfschule, Internationale Schule, Montessorischule).
_SECONDARY_KEYWORDS = [
    # (keywords in name/schulart_detail, label) — first match wins, ordered
    # from most specific concept to the state-school hierarchy.
    (("waldorf", "rudolf-steiner", "steiner-schule"), "Waldorfschule"),
    (("europäische schule", "international school", "internationale schule",
      "lycée", "lycee", "lyzeum", "deutsch-französische"), "Internationale Schule"),
    (("montessori",), "Montessorischule"),
    (("gymnasium", "gymnasien"), "Gymnasien"),
    (("realschule", "realschulen"), "Realschulen"),
    (("mittelschule", "hauptschule", "volksschule", "werkrealschule"), "Mittelschulen"),
]


def school_type_label(candidate: dict, table: str) -> str:
    """Pick a 'school_type' string consistent with the existing Munich rows."""
    if table == "primary":
        return "Grundschulen"
    text = " ".join(str(candidate.get(k, "") or "") for k in
                    ("schulname", "schulart_detail", "gemini_category")).lower()
    # Names that carry no type word but are well known
    if "nymphenburger schulen" in text:      # private Gymnasium + Realschule
        return "Gymnasien"
    for keywords, label in _SECONDARY_KEYWORDS:
        if any(k in text for k in keywords):
            return label
    return "Sekundarschule"  # unknown concept; logged by the caller


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def _load_geocode_cache() -> dict:
    if GEOCODE_CACHE.exists():
        try:
            return json.loads(GEOCODE_CACHE.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def _save_geocode_cache(cache: dict) -> None:
    tmp = GEOCODE_CACHE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cache, indent=2, ensure_ascii=False))
    tmp.replace(GEOCODE_CACHE)


def geocode(name: str, address: str, cache: dict) -> tuple[float | None, float | None, str]:
    """Use Places searchText to resolve coords. Returns (lat, lon, full_address)."""
    if not API_KEY:
        return None, None, ""
    key = f"{name}|{address}".lower().strip()
    if key in cache:
        c = cache[key]
        return c.get("lat"), c.get("lon"), c.get("address", "")
    body = {
        "textQuery": f"{name} {address}".strip(),
        "locationBias": {"circle": {"center": CENTER, "radius": 25000.0}},
        "maxResultCount": 1,
        "languageCode": "de",
        "includedType": "school",
    }
    try:
        r = requests.post(
            PLACES_URL, json=body,
            headers={"Content-Type": "application/json",
                     "X-Goog-Api-Key": API_KEY,
                     "X-Goog-FieldMask": PLACES_FIELDS},
            timeout=20,
        )
        r.raise_for_status()
        places = r.json().get("places", [])
        if not places:
            cache[key] = {"lat": None, "lon": None, "address": ""}
            return None, None, ""
        p = places[0]
        loc = p.get("location") or {}
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        addr = p.get("formattedAddress") or ""
        cache[key] = {"lat": lat, "lon": lon, "address": addr}
        time.sleep(0.3)
        return lat, lon, addr
    except Exception as e:
        print(f"  geocode error for {name[:40]}: {e}")
        return None, None, ""


# ---------------------------------------------------------------------------
# Schulnummer generation
# ---------------------------------------------------------------------------

def make_schulnummer(table_prefix: str, schulname: str, plz: str) -> str:
    """Stable, idempotent schulnummer for private-school candidates.

    Pattern: MUCPRIV_{primary|sec}_{12-char-hash}
    """
    base = f"{schulname}|{plz}".strip().lower()
    h = hashlib.sha1(base.encode()).hexdigest()[:12]
    return f"MUCPRIV_{table_prefix}_{h}"


# ---------------------------------------------------------------------------
# Row construction
# ---------------------------------------------------------------------------

def build_raw_row(candidate: dict, table: str, lat: float | None, lon: float | None,
                   geocoded_address: str) -> dict:
    schulname = candidate["schulname"].strip()
    strasse = candidate.get("address_line1", "").strip()
    plz = candidate.get("plz", "").strip()
    ort = candidate.get("ort", "").strip() or "München"
    if not strasse and geocoded_address:
        # extract street portion from "Strasse 12, 80331 München, Deutschland"
        first = geocoded_address.split(",")[0].strip()
        if first and first != ort:
            strasse = first
    if not plz and geocoded_address:
        m = re.search(r"\b(\d{5})\s+", geocoded_address)
        if m:
            plz = m.group(1)

    table_prefix = "pri" if table == "primary" else "sec"
    schulnummer = make_schulnummer(table_prefix, schulname, plz)

    adresse = ", ".join([s for s in (strasse, f"{plz} {ort}".strip()) if s])

    row = {
        "schulnummer": schulnummer,
        "schulname": schulname,
        "strasse": strasse,
        "plz": plz,
        "ort": ort,
        "website": candidate.get("website", "") or "",
        "email": "",
        "school_type": school_type_label(candidate, table),
        "traegerschaft": candidate.get("traegerschaft_hint", "privat") or "privat",
        "traeger": "",
        "fax": "",
        "phone": "",
        "director": "",
        "latitude": lat if lat is not None else "",
        "longitude": lon if lon is not None else "",
        "adresse": adresse,
        "data_source": DATA_SOURCE_LABEL,
        "data_retrieved": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "bundesland": "Bayern",
        "stadt": ort,
        "telefon": "",
        "schueler_2024_25": "",
        "lehrer_2024_25": "",
        "sprachen": "",
        "gruendungsjahr": "",
        "leitung": "",
        "besonderheiten": "",
        "tuition_monthly_eur": "",
        "scholarship_available": "",
        "admission_criteria_bullets": "",
        "admission_application_window": "",
        "admission_notes_de": "",
        "open_days": "",
        "last_open_day_seen": "",
        "admission_fetched_at": "",
    }
    return row


# ---------------------------------------------------------------------------
# File mutation
# ---------------------------------------------------------------------------

_NA_PLACEHOLDERS = ("", "nan", "None", "NaN", "<NA>")


def _looks_bool_column(ser: pd.Series) -> bool:
    """Object pandas columns that are actually bool (after dropna)."""
    nonnull = ser.dropna()
    if nonnull.empty:
        return False
    return all(isinstance(v, (bool,)) for v in nonnull)


def _normalize_na(v):
    if v is None:
        return pd.NA
    if isinstance(v, float) and pd.isna(v):
        return pd.NA
    if isinstance(v, str) and v.strip() in _NA_PLACEHOLDERS:
        return pd.NA
    return v


def _coerce_to_existing_dtypes(new_df: pd.DataFrame, df_existing: pd.DataFrame) -> pd.DataFrame:
    """Best-effort: coerce each column in `new_df` so it can land in the
    same parquet/pandas schema as `df_existing`. Replaces empty / 'nan' /
    'None' / 'NaN' / '<NA>' strings with pd.NA across ALL columns (so
    pyarrow doesn't try to coerce '' to bool/int) and additionally maps
    bool-like object columns to actual bool/NA."""
    for col in new_df.columns:
        new_df[col] = new_df[col].apply(_normalize_na)
        existing = df_existing[col]
        existing_dtype = existing.dtype

        if pd.api.types.is_integer_dtype(existing_dtype) or pd.api.types.is_float_dtype(existing_dtype):
            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
        elif pd.api.types.is_bool_dtype(existing_dtype) or _looks_bool_column(existing):
            mapping = {True: True, False: False, "True": True, "False": False,
                       "true": True, "false": False, 1: True, 0: False, "1": True, "0": False}
            new_df[col] = new_df[col].map(
                lambda v: pd.NA if pd.isna(v) else mapping.get(v, pd.NA)
            )
    return new_df


def append_or_skip(df_existing: pd.DataFrame, new_rows: list[dict]) -> tuple[pd.DataFrame, int, int]:
    """Append rows whose schulnummer is not already present. Returns (df, added, skipped)."""
    existing_snrs = set(df_existing["schulnummer"].astype(str).tolist())
    added = skipped = 0
    rows_to_add = []
    for row in new_rows:
        if row["schulnummer"] in existing_snrs:
            skipped += 1
            continue
        rows_to_add.append(row)
        added += 1
    if not rows_to_add:
        return df_existing, added, skipped
    new_df = pd.DataFrame(rows_to_add)
    # Keep existing column order — add missing cols as NA, drop extras
    for c in df_existing.columns:
        if c not in new_df.columns:
            new_df[c] = pd.NA
    new_df = new_df[df_existing.columns]
    new_df = _coerce_to_existing_dtypes(new_df, df_existing)
    combined = pd.concat([df_existing, new_df], ignore_index=True)
    return combined, added, skipped


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Plan only — do not write any output files")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)

    if not INPUT.exists():
        print(f"missing {INPUT}; run earlier phases first")
        return 1

    df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)
    if args.limit:
        df = df.head(args.limit)
    print(f"Ingesting {len(df)} private-school candidates")

    cache = _load_geocode_cache()

    primary_rows: list[dict] = []
    secondary_rows: list[dict] = []
    geocoded = 0
    no_coords = 0
    for _, c in df.iterrows():
        cand = c.to_dict()
        lat = cand.get("latitude") or ""
        lon = cand.get("longitude") or ""
        try:
            lat = float(lat) if lat else None
            lon = float(lon) if lon else None
        except (TypeError, ValueError):
            lat, lon = None, None

        geocoded_addr = ""
        if lat is None or lon is None:
            address_for_geocode = " ".join(
                p for p in (cand.get("address_line1", ""), cand.get("plz", ""),
                            cand.get("ort", "") or "München")
                if p
            )
            print(f"  geocoding: {cand['schulname'][:55]}")
            if not args.dry_run:
                lat, lon, geocoded_addr = geocode(cand["schulname"], address_for_geocode, cache)
                if lat is not None:
                    geocoded += 1
                else:
                    no_coords += 1

        level = (cand.get("schulart") or "").strip().lower()
        if level in ("primary", "both"):
            primary_rows.append(build_raw_row(cand, "primary", lat, lon, geocoded_addr))
        if level in ("secondary", "both"):
            secondary_rows.append(build_raw_row(cand, "secondary", lat, lon, geocoded_addr))

    if not args.dry_run:
        _save_geocode_cache(cache)

    print(f"\n  primary rows to add:   {len(primary_rows)}")
    print(f"  secondary rows to add: {len(secondary_rows)}")
    print(f"  geocoded fresh:        {geocoded}")
    print(f"  still without coords:  {no_coords}")

    if args.dry_run:
        print("\n  DRY RUN — no files modified.")
        return 0

    # Write into all the Munich data files
    targets = {
        "primary": [
            (RAW_DIR / "munich_primary_schools_raw.csv", "csv"),
            (FINAL_DIR / "munich_primary_school_master_table.csv", "csv"),
            (FINAL_DIR / "munich_primary_school_master_table.parquet", "parquet"),
            (FINAL_DIR / "munich_primary_school_master_table_final.csv", "csv"),
            (FINAL_DIR / "munich_primary_school_master_table_final_with_embeddings.parquet", "parquet"),
            (FINAL_DIR / "munich_primary_school_master_table_berlin_schema.csv", "csv"),
            (FINAL_DIR / "munich_primary_school_master_table_berlin_schema.parquet", "parquet"),
        ],
        "secondary": [
            (RAW_DIR / "munich_secondary_schools_raw.csv", "csv"),
            (FINAL_DIR / "munich_secondary_school_master_table.csv", "csv"),
            (FINAL_DIR / "munich_secondary_school_master_table.parquet", "parquet"),
            (FINAL_DIR / "munich_secondary_school_master_table_final.csv", "csv"),
            (FINAL_DIR / "munich_secondary_school_master_table_final_with_embeddings.parquet", "parquet"),
            (FINAL_DIR / "munich_secondary_school_master_table_berlin_schema.csv", "csv"),
            (FINAL_DIR / "munich_secondary_school_master_table_berlin_schema.parquet", "parquet"),
        ],
    }

    summary = {"primary": [], "secondary": []}
    for table, files in targets.items():
        rows = primary_rows if table == "primary" else secondary_rows
        for path, fmt in files:
            if not path.exists():
                print(f"  SKIP {path.name}: missing")
                continue
            df_existing = pd.read_csv(path, low_memory=False) if fmt == "csv" else pd.read_parquet(path)
            merged, added, skipped = append_or_skip(df_existing, rows)
            if added:
                if fmt == "csv":
                    merged.to_csv(path, index=False, encoding="utf-8-sig")
                else:
                    merged.to_parquet(path, index=False)
            summary[table].append({"file": path.name, "added": added, "skipped": skipped, "total_now": len(merged)})

    print("\n=== Ingestion summary ===")
    for table, items in summary.items():
        print(f"\n  {table}:")
        for it in items:
            print(f"    {it['file']:<60} added={it['added']:<3} skipped={it['skipped']:<3} total={it['total_now']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
