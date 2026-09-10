#!/usr/bin/env python3
"""
NL Phase 8: Data Combination & Finalization

Merges all enrichment phases into a single master table, transforms to
core + NL extension schema, and optionally generates embeddings.

This is the final processing step before the Berlin schema transform.

Input:  The most-enriched intermediate CSV available
Output: data_nl/final/nl_school_master_table_final.parquet
        data_nl/final/nl_school_master_table_final.csv
"""

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Tail of the free enrichment chain, newest link first. POI is deliberately
# absent: it is paid, so it is only re-run for schools that lack it and is
# therefore routinely older and shorter than the free chain. Selecting inputs by
# column count used to pick it anyway (81 POI columns beat everything), which
# silently reverted the pipeline to the previous run's rows and stale values.
# POI and the other paid layers are merged back in by column instead.
FREE_CHAIN = [
    "nl_schools_with_profiles.csv",
    "nl_schools_with_quality.csv",
    "nl_schools_with_demographics.csv",
    "nl_schools_with_crime.csv",
    "nl_schools_with_transit.csv",
    "nl_schools_with_traffic.csv",
    "nl_school_master_geocoded.csv",
    "nl_school_master_base.csv",
]

# Paid / expensive columns carried forward from the previous final, in core
# schema names (the shared German carry list speaks Berlin names).
NL_CARRY_PREFIXES = [
    "poi_",              # Google Places, ~$250 a full run
    "description",       # description, description_local, description_source
    "summary_",
    "embedding",
    "most_similar",
    "similar_schools",
    "tuition",
    "admission_",
    "open_days",
    "env_",
]
NL_CARRY_EXACT = [
    "languages_offered",  # LLM-derived
    "special_features",
    "founding_year",
    "principal",
    "phone",
    "website",
    "email",
]


def find_best_input() -> Path:
    """Newest available link in the free enrichment chain."""
    for name in FREE_CHAIN:
        path = INTERMEDIATE_DIR / name
        if path.exists():
            return path
    raise FileNotFoundError("No intermediate data found. Run earlier phases first.")


def carry_paid_columns(fresh: pd.DataFrame) -> pd.DataFrame:
    """Gap-fill paid columns from the previous final table on school_id."""
    previous_path = FINAL_DIR / "nl_school_master_table_final.parquet"
    if not previous_path.exists():
        logger.info("No previous final table — nothing to carry forward")
        return fresh

    from scripts_shared.processing.merge_enriched_columns import merge_enriched_columns
    previous = pd.read_parquet(previous_path)
    logger.info(f"Carrying paid columns from previous final "
                f"({len(previous)} rows, {len(previous.columns)} cols)")

    merged = merge_enriched_columns(
        fresh, previous,
        join_key="school_id",
        carry_prefixes=NL_CARRY_PREFIXES,
        carry_exact=NL_CARRY_EXACT,
    )
    new_ids = set(fresh["school_id"]) - set(previous["school_id"])
    if new_ids:
        logger.info(f"  {len(new_ids)} school(s) new since the last run — they carry "
                    f"no POI/description yet: {sorted(new_ids)}")
    return merged


def main(skip_embeddings: bool = False):
    """Combine all enrichments and produce final output."""
    logger.info("=" * 60)
    logger.info("NL Phase 8: Data Combination & Finalization")
    logger.info("=" * 60)

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Load best available data
    input_path = find_best_input()
    logger.info(f"Loading from: {input_path.name}")
    df = pd.read_csv(input_path, low_memory=False)
    logger.info(f"  {len(df)} schools, {len(df.columns)} columns")

    # Run the core schema transform
    from scripts_international.nl.processing.nl_to_core_schema import transform
    output = transform(input_path)

    # Carry POI / descriptions / embeddings / tuition forward before overwriting
    output = carry_paid_columns(output)

    # Save final outputs
    parquet_path = FINAL_DIR / "nl_school_master_table_final.parquet"
    csv_path = FINAL_DIR / "nl_school_master_table_final.csv"
    if parquet_path.exists():
        import shutil
        from datetime import date
        backup = FINAL_DIR / f"backup_{date.today().isoformat()}"
        backup.mkdir(parents=True, exist_ok=True)
        for existing in FINAL_DIR.glob("nl_school_master_table_*"):
            if existing.is_file():
                shutil.copy2(existing, backup / existing.name)
        logger.info(f"Backed up previous finals to {backup}")

    output.to_parquet(parquet_path, index=False)
    output.to_csv(csv_path, index=False)

    logger.info(f"\nFinal output: {len(output)} schools, {len(output.columns)} columns")
    logger.info(f"  Parquet: {parquet_path}")
    logger.info(f"  CSV: {csv_path}")

    # Coverage summary
    total = len(output)
    key_cols = [
        "school_name", "latitude", "longitude", "students_current",
        "teachers_current", "academic_performance_score",
        "transit_accessibility_score", "crime_safety_category",
        "area_median_income", "description",
    ]
    logger.info("\nKey column coverage:")
    for col in key_cols:
        if col in output.columns:
            n = output[col].notna().sum()
            pct = n / total * 100
            status = "+" if pct > 50 else "~" if pct > 0 else "-"
            logger.info(f"  {status} {col}: {n}/{total} ({pct:.0f}%)")

    # Also produce Berlin-compatible output
    logger.info("\nGenerating Berlin-compatible schema...")
    try:
        from scripts_international.international_to_berlin_schema import transform_to_berlin
        berlin_df = transform_to_berlin(output, "NL")
        berlin_parquet = FINAL_DIR / "nl_school_master_table_berlin_schema.parquet"
        berlin_csv = FINAL_DIR / "nl_school_master_table_berlin_schema.csv"
        berlin_df.to_parquet(berlin_parquet, index=False)
        berlin_df.to_csv(berlin_csv, index=False)

        populated = sum(1 for col in berlin_df.columns if berlin_df[col].notna().any())
        logger.info(f"  Berlin schema: {len(berlin_df.columns)} cols ({populated} with data)")
        logger.info(f"  Saved: {berlin_parquet}")
    except Exception as e:
        logger.warning(f"Berlin schema transform failed: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-embeddings", action="store_true")
    args = parser.parse_args()
    main(skip_embeddings=args.skip_embeddings)
