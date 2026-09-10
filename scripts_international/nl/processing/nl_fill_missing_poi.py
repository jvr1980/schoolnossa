#!/usr/bin/env python3
"""
Fill POI columns only for schools that have none.

The NL secondary table already carries Google Places POI for the schools that
were enriched in April (~$250 of calls). A registry refresh added a handful of
new schools, which have no POI at all. Re-running Places for them is possible
but re-running it for everyone would be wasteful, and letting the free Overpass
output overwrite paid Places data everywhere would trade quality for uniformity.

So: keep Places where it exists, fill the gaps from Overpass, and stamp
poi_data_source per row so the mixed provenance is visible rather than silent.

Usage:
    python3 scripts_international/nl/processing/nl_fill_missing_poi.py [--dry-run]
"""

import argparse
import logging
import os
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

NL_DATA_DIR = os.environ.get("NL_DATA_DIR", "data_nl")
DATA_DIR = PROJECT_ROOT / NL_DATA_DIR
TABLE_PREFIX = "nl_po" if NL_DATA_DIR.endswith("_po") else "nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PRESENCE_COL = "poi_supermarket_count_500m"  # written for every enriched row
PLACES_LABEL = "Google Places"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="nl_schools_with_pois.csv")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    final_path = FINAL_DIR / f"{TABLE_PREFIX}_school_master_table_final.parquet"
    src_path = INTERMEDIATE_DIR / args.source
    for p in (final_path, src_path):
        if not p.exists():
            logger.error(f"Not found: {p}")
            sys.exit(1)

    final = pd.read_parquet(final_path)
    src = pd.read_csv(src_path, low_memory=False)
    id_col = "school_id" if "school_id" in src.columns else "vestiging_code"

    poi_cols = [c for c in src.columns if c.startswith("poi_") and c != "poi_data_source"]
    if not poi_cols:
        logger.error("No poi_ columns in source — nothing to fill")
        sys.exit(1)

    final["school_id"] = final["school_id"].astype(str).str.strip().str.upper()
    src[id_col] = src[id_col].astype(str).str.strip().str.upper()
    lookup = src.drop_duplicates(id_col).set_index(id_col)

    if PRESENCE_COL not in final.columns:
        final[PRESENCE_COL] = pd.NA
    gaps = final[PRESENCE_COL].isna()
    fillable = gaps & final["school_id"].isin(lookup.index)
    logger.info(f"Rows without POI: {int(gaps.sum())}/{len(final)} "
                f"({int(fillable.sum())} of them present in {src_path.name})")
    if not fillable.any():
        logger.info("Nothing to fill.")
        return

    if "poi_data_source" not in final.columns:
        # Existing rows predate the stamp; label them by what actually produced
        # them rather than leaving provenance blank.
        final["poi_data_source"] = None
        final.loc[~gaps, "poi_data_source"] = PLACES_LABEL

    filled_cols = 0
    for col in poi_cols:
        if col not in final.columns:
            final[col] = None
        mapped = final.loc[fillable, "school_id"].map(lookup[col])
        if mapped.notna().any():
            final.loc[fillable, col] = mapped.values
            filled_cols += 1

    src_label = (lookup["poi_data_source"].dropna().iloc[0]
                 if "poi_data_source" in lookup.columns and lookup["poi_data_source"].notna().any()
                 else "OpenStreetMap via Overpass (ODbL)")
    final.loc[fillable, "poi_data_source"] = src_label

    logger.info(f"Filled {filled_cols} POI columns for {int(fillable.sum())} schools "
                f"from: {src_label}")
    logger.info("Provenance after fill: "
                f"{final['poi_data_source'].value_counts(dropna=False).to_dict()}")

    if args.dry_run:
        logger.info("--dry-run: nothing written")
        return

    backup = FINAL_DIR / f"backup_{date.today().isoformat()}"
    backup.mkdir(parents=True, exist_ok=True)
    for existing in FINAL_DIR.glob(f"{TABLE_PREFIX}_school_master_table_*"):
        if existing.is_file():
            shutil.copy2(existing, backup / existing.name)

    final.to_parquet(final_path, index=False)
    final.to_csv(final_path.with_suffix(".csv"), index=False)
    logger.info(f"Wrote {final_path.name} ({len(final)} x {len(final.columns)})")


if __name__ == "__main__":
    main()
