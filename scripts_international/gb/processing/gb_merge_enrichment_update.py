#!/usr/bin/env python3
"""
Merge freshly-enriched GB columns into the existing final table.

Surgical alternative to a full pipeline re-run: Phase 6 (Google Places POI) and
Phase 8 (LLM descriptions + embeddings) cost real money, so when only a free
layer changes — here the police.uk crime enrichment, which shipped all-zero in
April — we update those columns in place on school_id and leave everything else
byte-identical.

Mirrors scripts_international/nl/processing/nl_merge_enrichment_update.py, which
does the same for NL, and scripts_shared/processing/refresh_traffic_columns.py
for the German cities.

Unlike those, this one runs a distinct-value guard on every merged column before
writing. A null-rate check is what let the all-zero crime column ship: every row
had a value, the value was 0.0, and coverage read as 100%.

Usage:
    python3 scripts_international/gb/processing/gb_merge_enrichment_update.py
    python3 ... --source gb_schools_with_crime.csv --dry-run
"""

import argparse
import logging
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data_gb"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Columns this update owns. Anything not listed is left untouched, so the paid
# layers (POI, descriptions, embeddings) can never be clobbered.
MERGE_COLUMNS = [
    "crime_total_per_1000",
    "crime_violent_per_1000",
    "crime_property_per_1000",
    "crime_drug_per_1000",
    "crime_safety_rank",
    "crime_safety_category",
    "crime_area_name",
    "crime_data_source",
    "crime_data_year",
]

# Columns that must vary once merged. Constants here are the signature of a
# silent enrichment failure, not a valid result.
MUST_VARY = {
    "crime_total_per_1000",
    "crime_violent_per_1000",
    "crime_property_per_1000",
    "crime_drug_per_1000",
    "crime_safety_category",
    "crime_area_name",
}

# Vocabulary the German tertiles use; the UI filters on the literal.
SAFETY_VOCAB = {"safe", "moderate", "elevated"}

ID_LEFT = "school_id"
ID_RIGHT = "urn"

# 159 URNs repeat in both the source and the final table (split sites sharing a
# URN), so school_id alone is not unique — merging on it would give 159 rows a
# neighbouring site's crime figure. The crime catchment is defined by the
# school's coordinates, and school_id + rounded lat/lon is unique across all
# 5,303 rows on both sides, so that is the join key.
COORD_DP = 5  # ~1 m


def _row_key(df: pd.DataFrame, id_col: str) -> pd.Series:
    return (
        df[id_col].astype(str).str.strip().str.upper()
        + "|" + pd.to_numeric(df["latitude"], errors="coerce").round(COORD_DP).astype(str)
        + "|" + pd.to_numeric(df["longitude"], errors="coerce").round(COORD_DP).astype(str)
    )


def _check_distinct(series: pd.Series, col: str) -> str:
    """Return a one-line distinct-value summary; raise if a MUST_VARY col is flat."""
    values = series.dropna()
    distinct = values.nunique()
    if col in MUST_VARY:
        if distinct <= 1:
            only = values.iloc[0] if len(values) else "<all null>"
            raise RuntimeError(
                f"{col}: {distinct} distinct value(s) after merge (constant {only!r}). "
                f"That is what the April all-zero crime column looked like to a "
                f"null-rate check. Refusing to write."
            )
        if pd.api.types.is_numeric_dtype(values) and float(values.abs().max()) == 0.0:
            raise RuntimeError(f"{col}: every value is zero after merge. Refusing to write.")
    if pd.api.types.is_numeric_dtype(values) and len(values):
        return (f"distinct={distinct:5d}  min={values.min():9.2f}  "
                f"median={values.median():9.2f}  max={values.max():10.2f}")
    return f"distinct={distinct:5d}"


def merge_update(source_name: str, dry_run: bool = False) -> pd.DataFrame:
    src_path = INTERMEDIATE_DIR / source_name
    final_path = FINAL_DIR / "gb_school_master_table_final.parquet"
    if not src_path.exists():
        logger.error(f"Source not found: {src_path}")
        sys.exit(1)
    if not final_path.exists():
        logger.error(f"Final table not found: {final_path}")
        sys.exit(1)

    final = pd.read_parquet(final_path)
    src = pd.read_csv(src_path, low_memory=False)
    logger.info(f"Final: {len(final)} rows x {len(final.columns)} cols")
    logger.info(f"Source: {len(src)} rows ({src_path.name})")

    id_right = ID_RIGHT if ID_RIGHT in src.columns else ID_LEFT
    wanted = [c for c in MERGE_COLUMNS if c in src.columns]
    missing = [c for c in MERGE_COLUMNS if c not in src.columns]
    if missing:
        logger.warning(f"Not present in source, skipped: {missing}")
    if not wanted:
        logger.error("Source carries none of the merge columns — nothing to do")
        sys.exit(1)

    src["_row_key"] = _row_key(src, id_right)
    final["_row_key"] = _row_key(final, ID_LEFT)

    slim = src[["_row_key"] + wanted].drop_duplicates("_row_key")
    matched = final["_row_key"].isin(set(slim["_row_key"])).sum()
    logger.info(f"Row-key match (school_id + lat/lon): {matched}/{len(final)} rows")
    if matched == 0:
        logger.error("No key overlap — refusing to write")
        sys.exit(1)
    if matched < len(final):
        logger.warning(f"{len(final) - matched} rows will be set to NaN "
                       f"(no measurement for that site)")

    lookup = slim.set_index("_row_key")
    updated = final.copy()

    logger.info("")
    logger.info("Column                          coverage before -> after")
    logger.info("-" * 62)
    for col in wanted:
        before = updated[col].notna().mean() * 100 if col in updated.columns else 0.0
        updated[col] = updated["_row_key"].map(lookup[col])
        after = updated[col].notna().mean() * 100
        note = "  <-- new" if before == 0 else ""
        logger.info(f"  {col:30s} {before:5.1f}% -> {after:5.1f}%{note}")

    logger.info("")
    logger.info("Distinct-value check (null rates hid the April bug; this is the test)")
    logger.info("-" * 78)
    for col in wanted:
        logger.info(f"  {col:30s} {_check_distinct(updated[col], col)}")

    cats = set(updated["crime_safety_category"].dropna().unique()) \
        if "crime_safety_category" in updated.columns else set()
    if cats and not cats <= SAFETY_VOCAB:
        raise RuntimeError(
            f"crime_safety_category vocabulary drifted: {sorted(cats)} "
            f"(expected a subset of {sorted(SAFETY_VOCAB)})")
    if cats:
        logger.info(f"  crime_safety_category vocabulary: {sorted(cats)} OK")

    updated = updated.drop(columns=["_row_key"])
    if list(updated.columns) != list(final.drop(columns=["_row_key"]).columns):
        raise RuntimeError("Column set changed — a merge update must be columns-in-place")

    if dry_run:
        logger.info("\n--dry-run: nothing written")
        return updated

    backup = FINAL_DIR / f"backup_{date.today().isoformat()}"
    backup.mkdir(parents=True, exist_ok=True)
    for existing in FINAL_DIR.glob("gb_school_master_table_*"):
        if existing.is_file():
            shutil.copy2(existing, backup / existing.name)
    logger.info(f"\nBacked up previous finals to {backup}")

    updated.to_parquet(final_path, index=False)
    updated.to_csv(final_path.with_suffix(".csv"), index=False)
    logger.info(f"Wrote {final_path.name} ({len(updated)} x {len(updated.columns)})")
    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Merge GB enrichment updates into the final table without "
                    "re-running the paid phases")
    parser.add_argument("--source", default="gb_schools_with_crime.csv",
                        help="Intermediate CSV carrying the refreshed columns")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("GB enrichment merge-update")
    logger.info("=" * 60)
    merge_update(args.source, args.dry_run)


if __name__ == "__main__":
    main()
