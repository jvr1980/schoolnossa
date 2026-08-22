#!/usr/bin/env python3
"""
Merge-back utility: carry paid-enrichment columns forward across a base-data refresh.

Problem this solves
-------------------
The Design-A city pipelines (Munich, Stuttgart, Dresden — and both Berlin
pipelines) assemble their final table by taking the *most enriched
intermediate file wholesale* (see e.g.
scripts_munich/processing/munich_data_combiner.py::find_most_enriched_file).
When the base scraper is re-run, the freshly rebuilt chain
(base -> _with_traffic -> _with_transit -> _with_crime) no longer contains
the paid-API columns (Google Places POIs, LLM descriptions/metadata,
embeddings). Either the combiner silently keeps shipping the stale
_with_metadata file, or — if the stale intermediates are deleted — the paid
columns are lost and would cost real money to regenerate.

This script joins the previous final parquet onto the fresh table on
`schulnummer` and carries forward the configured paid column groups, so a
free refresh preserves everything that was paid for. New schools simply get
NaN in those columns until a future paid run fills them.

Usage
-----
    python scripts_shared/processing/merge_enriched_columns.py \
        --fresh data_munich/intermediate/munich_secondary_schools_with_crime.csv \
        --previous data_munich/final/munich_secondary_school_master_table_final_with_embeddings.parquet \
        --output data_munich/intermediate/munich_secondary_schools_with_metadata.csv

    # or programmatically:
    from scripts_shared.processing.merge_enriched_columns import merge_enriched_columns

Column-group semantics
----------------------
A column from the previous final table is carried forward if it matches one
of the configured prefixes/names AND is absent from the fresh table. If the
fresh table already has the column (e.g. besonderheiten re-scraped for
free), the fresh value wins and the previous value only fills NaN gaps
(fill-gaps semantics, mirroring upload_to_supabase.py).
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paid / expensive column groups to preserve across base refreshes.
# Prefixes end with '_' where they are namespaces; exact names otherwise.
DEFAULT_CARRY_PREFIXES = [
    'poi_',                 # Google Places enrichment (~81-87 cols)
    'transit_',             # transit enrichment (free but slow; carry as gap-fill)
    'description',          # description, description_de, description_en, description_source...
    'summary_',             # Bremen summary_en variant
    'embedding',            # OpenAI/Gemini vectors + embedding_text
    'similar_schools',      # derived from embeddings
    'most_similar',         # most_similar_school_01..03
    'tuition',              # Stuttgart Gemini/GPT tuition pipeline
    'admission_',           # Gemini admission/open-days enrichment
    'open_days',
    'last_open_day_seen',
]
DEFAULT_CARRY_EXACT = [
    'besonderheiten',       # LLM website metadata
    'sprachen',
    'gruendungsjahr',
    'leitung',
    'profil',
    'website_scrape_status',
]
# Year-suffixed statistics: for cities whose base source doesn't publish them
# (e.g. Munich/Stuttgart, where they were LLM-scraped from school websites),
# a base refresh loses them entirely. Gap-fill semantics still lets a fresh
# scrape win wherever it provides values.
DEFAULT_CARRY_PREFIXES += [
    'schueler_', 'lehrer_', 'migration_', 'abitur_', 'nachfrage_',
    'msa_', 'notendurchschnitt_',
]
JOIN_KEY = 'schulnummer'


def _matches(col: str, prefixes, exact) -> bool:
    return col in exact or any(col.startswith(p) for p in prefixes)


def merge_enriched_columns(fresh_df: pd.DataFrame,
                           previous_df: pd.DataFrame,
                           join_key: str = JOIN_KEY,
                           carry_prefixes=None,
                           carry_exact=None) -> pd.DataFrame:
    """Return fresh_df with paid-enrichment columns carried forward from previous_df."""
    carry_prefixes = DEFAULT_CARRY_PREFIXES if carry_prefixes is None else carry_prefixes
    carry_exact = DEFAULT_CARRY_EXACT if carry_exact is None else carry_exact

    for name, df in (('fresh', fresh_df), ('previous', previous_df)):
        if join_key not in df.columns:
            raise KeyError(f"{name} table has no join key column '{join_key}'")

    fresh = fresh_df.copy()
    prev = previous_df.copy()
    # Normalize join key dtype (csv reload can flip int <-> str)
    fresh[join_key] = fresh[join_key].astype(str).str.strip()
    prev[join_key] = prev[join_key].astype(str).str.strip()
    if prev[join_key].duplicated().any():
        dups = prev[join_key][prev[join_key].duplicated()].tolist()
        raise ValueError(f"previous table has duplicate {join_key} values: {dups[:5]}")

    carry_cols = [c for c in prev.columns
                  if c != join_key and _matches(c, carry_prefixes, carry_exact)]
    new_cols = [c for c in carry_cols if c not in fresh.columns]
    gap_cols = [c for c in carry_cols if c in fresh.columns]

    prev_indexed = prev.set_index(join_key)
    matched = fresh[join_key].isin(prev_indexed.index)
    n_new_schools = int((~matched).sum())

    # 1) columns missing entirely from fresh: bring them over via join
    if new_cols:
        fresh = fresh.merge(prev[[join_key] + new_cols], on=join_key, how='left')

    # 2) columns present in fresh: fill NaN gaps only (fresh value wins)
    filled_counts = {}
    for col in gap_cols:
        before = fresh[col].isna().sum()
        fresh[col] = fresh[col].where(
            fresh[col].notna(),
            fresh[join_key].map(prev_indexed[col])
        )
        filled = before - fresh[col].isna().sum()
        if filled:
            filled_counts[col] = int(filled)

    # Normalize mixed-dtype object columns produced by gap-filling (e.g. a
    # fresh string '91;N3' filled into a column whose carried values were
    # numeric) — pyarrow refuses to write such columns. Vector/list columns
    # (embedding*) are left untouched.
    for col in set(new_cols + gap_cols):
        if col.startswith('embedding') or col not in fresh.columns:
            continue
        if fresh[col].dtype == object:
            sample_types = {type(v).__name__ for v in fresh[col].dropna().head(100)}
            if len(sample_types) > 1 and 'str' in sample_types:
                fresh[col] = fresh[col].map(lambda v: v if pd.isna(v) else str(v))

    # Reporting
    logger.info("=" * 60)
    logger.info("MERGE-BACK REPORT")
    logger.info(f"  fresh rows: {len(fresh)} | previous rows: {len(prev)}")
    logger.info(f"  matched on {join_key}: {int(matched.sum())} | new schools (NaN paid cols): {n_new_schools}")
    logger.info(f"  columns carried over (absent from fresh): {len(new_cols)}")
    for group_label, pref in [('poi_*', 'poi_'), ('transit_*', 'transit_'), ('embedding*', 'embedding')]:
        cols = [c for c in new_cols if c.startswith(pref)]
        if cols:
            nn = int(fresh[cols[0]].notna().sum())
            logger.info(f"    {group_label}: {len(cols)} cols, {nn}/{len(fresh)} rows populated")
    if filled_counts:
        logger.info(f"  gap-fills into existing columns: {filled_counts}")
    dropped = [c for c in prev.columns
               if c != join_key and c not in fresh.columns]
    if dropped:
        logger.info(f"  NOT carried (outside carry list), sample: {sorted(dropped)[:8]} (+{max(0,len(dropped)-8)} more)")
    logger.info("=" * 60)
    return fresh


def _load(path: Path) -> pd.DataFrame:
    if path.suffix == '.parquet':
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('--fresh', required=True, type=Path,
                    help='freshly rebuilt table (csv/parquet) from the free chain')
    ap.add_argument('--previous', required=True, type=Path,
                    help='previous final table (parquet) holding paid columns')
    ap.add_argument('--output', required=True, type=Path,
                    help='where to write the merged table (csv and/or parquet by extension)')
    ap.add_argument('--join-key', default=JOIN_KEY)
    args = ap.parse_args()

    fresh = _load(args.fresh)
    prev = _load(args.previous)
    merged = merge_enriched_columns(fresh, prev, join_key=args.join_key)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.suffix == '.parquet':
        merged.to_parquet(args.output, index=False)
    else:
        merged.to_csv(args.output, index=False, encoding='utf-8-sig')
    logger.info(f"Wrote {args.output} ({len(merged)} rows, {len(merged.columns)} cols)")


if __name__ == '__main__':
    sys.exit(main())
