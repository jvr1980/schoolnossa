#!/usr/bin/env python3
"""
Merge freshly-enriched NL columns into the existing final table.

Surgical alternative to a full pipeline re-run: POI enrichment costs ~$250 in
Google Places calls and descriptions/embeddings are equally expensive, so when
only the free layers change (traffic, inspectorate quality, CBS SES) we update
those columns in place on school_id and leave everything else byte-identical.

Mirrors scripts_shared/processing/refresh_traffic_columns.py, which does the
same for the German cities.

Usage:
    python3 scripts_international/nl/processing/nl_merge_enrichment_update.py
    python3 ... --source nl_schools_with_quality.csv --dry-run
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

# Overridable so the primary (basisonderwijs) pipeline reuses this unchanged:
# NL_DATA_DIR=data_nl_po. Output filenames are derived from it too, so the two
# levels never write over each other.
import os
NL_DATA_DIR = os.environ.get("NL_DATA_DIR", "data_nl")
DATA_DIR = PROJECT_ROOT / NL_DATA_DIR
TABLE_PREFIX = "nl_po" if NL_DATA_DIR.endswith("_po") else "nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Columns this update owns. Anything not listed is left untouched, so paid
# layers (POI, descriptions, embeddings, tuition) can never be clobbered.
MERGE_COLUMNS = [
    # Traffic — was a silent no-op until the BRON WFS rewrite
    "traffic_accidents_500m",
    "traffic_accidents_1000m",
    "traffic_accidents_fatal_1000m",
    "traffic_accidents_injury_1000m",
    "traffic_accidents_year",
    "traffic_volume_index",
    "traffic_data_source",
    # Onderwijsinspectie
    "school_quality_rating",
    "school_quality_rating_national",
    "school_quality_assessed_date",
    "school_quality_source",
    # CBS achterstandsscore
    "deprivation_index",
    "deprivation_index_national",
    "deprivation_data_year",
    "deprivation_data_source",
    # Verified profile labels + Amsterdam contact backfill
    "nl_tto_tracks",
    "nl_bilingual_tto",
    "nl_verified_profiles",
    "email",
    "phone",
    "website",
]

# Core-schema columns filled from source columns under a different name.
RENAME_INTO = {
    "school_board_id": "nl_school_board",
}

# Columns where the incoming value is better but the existing one is a valid
# fallback, so we fill gaps instead of overwriting. deprivation_index is the
# case that matters: the new CBS achterstandsscore is school-level (the right
# analogue of Berlin's belastungsstufe) but covers 89% of schools, while the
# previous CBS buurt-level area deprivation covered 99.8%. Overwriting would
# trade 10 points of coverage for the better definition; coalescing keeps both.
COALESCE_COLUMNS = {"deprivation_index", "email", "phone", "website"}

ID_LEFT = "school_id"
ID_RIGHT = "vestiging_code"


def merge_update(source_name: str, dry_run: bool = False) -> pd.DataFrame:
    src_path = INTERMEDIATE_DIR / source_name
    final_path = FINAL_DIR / f"{TABLE_PREFIX}_school_master_table_final.parquet"
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
    src = src.rename(columns=RENAME_INTO)
    wanted = [c for c in MERGE_COLUMNS + list(RENAME_INTO.values()) if c in src.columns]
    missing = [c for c in MERGE_COLUMNS if c not in src.columns]
    if missing:
        logger.warning(f"Not present in source, skipped: {missing}")

    slim = src[[id_right] + wanted].drop_duplicates(id_right)
    slim[id_right] = slim[id_right].astype(str).str.strip().str.upper()
    final[ID_LEFT] = final[ID_LEFT].astype(str).str.strip().str.upper()

    matched = final[ID_LEFT].isin(set(slim[id_right])).sum()
    logger.info(f"ID match: {matched}/{len(final)} rows")
    if matched == 0:
        logger.error("No id overlap — refusing to write")
        sys.exit(1)

    lookup = slim.set_index(id_right)
    logger.info("\nColumn                              before ->  after")
    logger.info("-" * 58)
    updated = final.copy()
    for col in wanted:
        before = updated[col].notna().mean() * 100 if col in updated.columns else 0.0
        mapped = updated[ID_LEFT].map(lookup[col])
        if col in COALESCE_COLUMNS and col in updated.columns:
            updated[col] = mapped.where(mapped.notna(), updated[col])
            note = "  (coalesced)"
        else:
            updated[col] = mapped
            note = "  <-- new" if before == 0 else ""
        after = updated[col].notna().mean() * 100
        logger.info(f"  {col:34s} {before:5.1f}% -> {after:5.1f}%{note}")

    if dry_run:
        logger.info("\n--dry-run: nothing written")
        return updated

    backup = FINAL_DIR / f"backup_{date.today().isoformat()}"
    backup.mkdir(parents=True, exist_ok=True)
    for existing in FINAL_DIR.glob(f"{TABLE_PREFIX}_school_master_table_*"):
        if existing.is_file():
            shutil.copy2(existing, backup / existing.name)
    logger.info(f"\nBacked up previous finals to {backup}")

    updated.to_parquet(final_path, index=False)
    updated.to_csv(final_path.with_suffix(".csv"), index=False)
    logger.info(f"Wrote {final_path.name} ({len(updated)} x {len(updated.columns)})")
    return updated


def main():
    parser = argparse.ArgumentParser(description="Merge NL enrichment updates into the final table")
    parser.add_argument("--source", default="nl_schools_with_quality.csv",
                        help="Intermediate CSV carrying the refreshed columns")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("NL enrichment merge-update")
    logger.info("=" * 60)
    merge_update(args.source, args.dry_run)


if __name__ == "__main__":
    main()
