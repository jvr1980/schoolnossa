#!/usr/bin/env python3
"""
Refresh traffic_* columns in final tables from a freshly-run traffic enrichment,
without re-running the whole pipeline.

Use case: the Unfallatlas ships a new year (each July) but the city's base
data is unchanged. In the Design-A pipelines (Frankfurt, NRW), the combiner
takes the most-enriched intermediate wholesale, so a fresh `_with_traffic`
file never reaches the final unless every downstream intermediate is
rebuilt. This script takes the traffic_* columns from the fresh enrichment
output and overwrites them in the final tables on schulnummer, then
re-derives the stable fields.

Overwrite (not fill-gaps) is intentional here: traffic data is refreshed
annually and the new vintage should replace the old one.

Usage:
    python3 scripts_shared/processing/refresh_traffic_columns.py --city frankfurt
    python3 scripts_shared/processing/refresh_traffic_columns.py --city leipzig
    python3 scripts_shared/processing/refresh_traffic_columns.py --city nrw
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

from scripts_shared.schema.stable_fields import add_stable_fields  # noqa: E402

# city -> list of (fresh _with_traffic file, [final files to update])
CITY_CONFIG = {
    'frankfurt': [
        ("data_frankfurt/intermediate/frankfurt_primary_schools_with_traffic.csv",
         ["data_frankfurt/final/frankfurt_primary_school_master_table_final.csv",
          "data_frankfurt/final/frankfurt_primary_school_master_table_final_with_embeddings.parquet",
          "data_frankfurt/final/frankfurt_primary_school_master_table_berlin_schema.csv",
          "data_frankfurt/final/frankfurt_primary_school_master_table_berlin_schema.parquet"]),
        ("data_frankfurt/intermediate/frankfurt_secondary_schools_with_traffic.csv",
         ["data_frankfurt/final/frankfurt_secondary_school_master_table_final.csv",
          "data_frankfurt/final/frankfurt_secondary_school_master_table_final_with_embeddings.parquet",
          "data_frankfurt/final/frankfurt_secondary_school_master_table_berlin_schema.csv",
          "data_frankfurt/final/frankfurt_secondary_school_master_table_berlin_schema.parquet"]),
    ],
    'leipzig': [
        ("data_leipzig/intermediate/leipzig_schools_with_traffic.csv",
         ["data_leipzig/final/leipzig_school_master_table_final.csv",
          "data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet",
          "data_leipzig/final/leipzig_primary_school_master_table_berlin_schema.csv",
          "data_leipzig/final/leipzig_primary_school_master_table_berlin_schema.parquet",
          "data_leipzig/final/leipzig_secondary_school_master_table_berlin_schema.csv",
          "data_leipzig/final/leipzig_secondary_school_master_table_berlin_schema.parquet"]),
    ],
    'nrw': [
        ("data_nrw/intermediate/nrw_primary_schools_with_traffic.csv",
         ["data_nrw/final/nrw_primary_school_master_table_final_with_embeddings.parquet",
          "data_nrw/final/duesseldorf_primary_school_master_table_final.csv",
          "data_nrw/final/duesseldorf_primary_school_master_table_final_with_embeddings.parquet",
          "data_nrw/final/koeln_primary_school_master_table_final.csv",
          "data_nrw/final/koeln_primary_school_master_table_final_with_embeddings.parquet"]),
        ("data_nrw/intermediate/nrw_secondary_schools_with_traffic.csv",
         ["data_nrw/final/nrw_secondary_school_master_table_final_with_embeddings.parquet",
          "data_nrw/final/duesseldorf_secondary_school_master_table_final.csv",
          "data_nrw/final/duesseldorf_secondary_school_master_table_final_with_embeddings.parquet",
          "data_nrw/final/koeln_secondary_school_master_table_final.csv",
          "data_nrw/final/koeln_secondary_school_master_table_final_with_embeddings.parquet"]),
    ],
}


def _load(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix == '.parquet' else pd.read_csv(path, low_memory=False)


def _save(df: pd.DataFrame, path: Path):
    if path.suffix == '.parquet':
        df.to_parquet(path, index=False)
    else:
        df.drop(columns=['embedding'], errors='ignore').to_csv(path, index=False, encoding='utf-8-sig')


def refresh_city(city: str):
    for fresh_rel, final_rels in CITY_CONFIG[city]:
        fresh_path = PROJECT_ROOT / fresh_rel
        if not fresh_path.exists():
            logger.warning(f"Fresh traffic file missing, skipping: {fresh_rel}")
            continue
        fresh = _load(fresh_path)
        traffic_cols = [c for c in fresh.columns if c.startswith('traffic_')]
        if not traffic_cols or 'schulnummer' not in fresh.columns:
            logger.warning(f"No traffic columns / schulnummer in {fresh_rel}")
            continue
        fresh['schulnummer'] = fresh['schulnummer'].astype(str).str.strip()
        fresh = fresh[fresh['schulnummer'].notna() & (fresh['schulnummer'] != 'nan')]
        fresh = fresh.drop_duplicates(subset=['schulnummer'], keep='first')
        src = fresh.set_index('schulnummer')[traffic_cols]
        logger.info(f"{fresh_rel}: {len(traffic_cols)} traffic cols, {len(src)} schools")

        for final_rel in final_rels:
            final_path = PROJECT_ROOT / final_rel
            if not final_path.exists():
                logger.warning(f"  final missing, skipping: {final_rel}")
                continue
            df = _load(final_path)
            if 'schulnummer' not in df.columns:
                logger.warning(f"  no schulnummer in {final_rel}")
                continue
            keys = df['schulnummer'].astype(str).str.strip()
            matched = keys.isin(src.index)
            for col in traffic_cols:
                df[col] = keys.map(src[col]).where(matched, df[col] if col in df.columns else pd.NA)
            df = add_stable_fields(df)
            _save(df, final_path)
            logger.info(f"  updated {final_rel} ({int(matched.sum())}/{len(df)} rows matched)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--city', choices=list(CITY_CONFIG) + ['all'], required=True)
    args = ap.parse_args()
    cities = list(CITY_CONFIG) if args.city == 'all' else [args.city]
    for c in cities:
        refresh_city(c)


if __name__ == '__main__':
    sys.exit(main())
