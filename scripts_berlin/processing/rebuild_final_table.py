#!/usr/bin/env python3
"""
Rebuild the Berlin final tables from a freshly refreshed free chain.

Takes the fully free-enriched intermediate (combined_schools_with_metadata_msa
for secondary, combined_grundschulen_with_metadata for primary), merges back
the paid-enrichment columns (POIs, descriptions, embeddings, LLM metadata)
from the previous final parquet on schulnummer, derives the stable
(year-agnostic) fields, and writes the new final parquet + CSV.

This replaces the paid phases 6-8 of the orchestrators for a free refresh:
new schools simply carry NaN in paid columns until a future paid run.

Usage:
    python3 scripts_berlin/processing/rebuild_final_table.py --which secondary
    python3 scripts_berlin/processing/rebuild_final_table.py --which primary
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.processing.merge_enriched_columns import merge_enriched_columns  # noqa: E402
from scripts_shared.schema.stable_fields import add_stable_fields, stable_coverage_report  # noqa: E402

CONFIG = {
    'secondary': {
        'fresh': PROJECT_ROOT / "data_berlin" / "intermediate" / "combined_schools_with_metadata_msa.csv",
        'previous': PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet",
        'out_parquet': PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet",
        'out_csv': PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final.csv",
    },
    'primary': {
        'fresh': PROJECT_ROOT / "data_berlin_primary" / "intermediate" / "combined_grundschulen_with_metadata.csv",
        'previous': PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet",
        'out_parquet': PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet",
        'out_csv': PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final.csv",
    },
}


def rebuild(which: str) -> pd.DataFrame:
    cfg = CONFIG[which]
    if not cfg['fresh'].exists():
        raise FileNotFoundError(f"Fresh chain output missing: {cfg['fresh']} — run the free chain first")
    fresh = pd.read_csv(cfg['fresh'], low_memory=False)
    logger.info(f"Fresh {which}: {len(fresh)} rows, {len(fresh.columns)} cols")

    if cfg['previous'].exists():
        prev = pd.read_parquet(cfg['previous'])
        merged = merge_enriched_columns(fresh, prev)
    else:
        logger.warning("No previous final parquet — proceeding without merge-back")
        merged = fresh

    merged = add_stable_fields(merged)
    logger.info(f"Stable-field coverage: {stable_coverage_report(merged)}")

    cfg['out_parquet'].parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(cfg['out_parquet'], index=False)
    merged.drop(columns=['embedding'], errors='ignore').to_csv(
        cfg['out_csv'], index=False, encoding='utf-8-sig')
    logger.info(f"Wrote {cfg['out_parquet'].name} + {cfg['out_csv'].name} "
                f"({len(merged)} rows, {len(merged.columns)} cols)")
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--which', choices=['secondary', 'primary', 'both'], default='both')
    args = ap.parse_args()
    targets = ['secondary', 'primary'] if args.which == 'both' else [args.which]
    for t in targets:
        rebuild(t)


if __name__ == '__main__':
    sys.exit(main())
