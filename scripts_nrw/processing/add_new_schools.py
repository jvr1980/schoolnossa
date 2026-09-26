#!/usr/bin/env python3
"""
Add NRW schools that opened since the last build to the Düsseldorf/Köln finals,
without rebuilding the other ~400 schools.

The real pipeline phases run unchanged on a one-school input inside a sandbox
(data_nrw/cache/new_schools_sandbox/<date>/): master data → traffic → transit →
crime → POI (Google Places) → website metadata + descriptions (Gemini) →
combiner → descriptions → Berlin schema. Each module's directory constants
are pointed at the sandbox; shared read-only caches (Unfallatlas, website
caches) stay in data_nrw/cache. The transit stop cache is sandboxed too, since
a one-school bbox would otherwise overwrite the region-wide cache.

Anmeldezahlen is skipped (a school opened this year has no application
figures). Local embeddings stay empty: the finals hold 3072-dim OpenAI vectors
and no OpenAI key is configured; the app uses the 768-dim Supabase embedding,
which scripts_shared/enrichment/replicate_lovable_description_jobs.py creates.

The resulting row is aligned to the existing finals' columns and appended to
the city CSV/parquet and the combined NRW parquet. Existing rows are asserted
unchanged. Then insert it into Supabase with
scripts_shared/insert_new_schools_to_supabase.py --ids <schulnummer> --emit-sql.

Usage:
    venv/bin/python scripts_nrw/processing/add_new_schools.py --schulnummer 100255
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
for p in (PROJECT_ROOT, PROJECT_ROOT / "scripts_nrw", PROJECT_ROOT / "scripts_nrw" / "scrapers",
          PROJECT_ROOT / "scripts_nrw" / "enrichment", PROJECT_ROOT / "scripts_nrw" / "processing"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import nrw_school_master_scraper as scraper  # noqa: E402
from scripts_shared.processing.refresh_traffic_columns import _load, _save  # noqa: E402
from scripts_shared.schema.stable_fields import add_stable_fields  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CACHE_DIR = PROJECT_ROOT / "data_nrw" / "cache"
FINAL_DIR = PROJECT_ROOT / "data_nrw" / "final"
RELATIVE_BY_BEZIRK = ['crime_safety_rank', 'crime_safety_category']


def sandbox_dirs(root: Path):
    dirs = {k: root / k for k in ('raw', 'intermediate', 'final', 'cache')}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def master_rows(schulnummern: set):
    """The new schools as the scraper would emit them, split by school type."""
    current = max(CACHE_DIR.glob('nrw_schuldaten_20??-??-??.csv'))
    df = scraper.parse_schuldaten_csv(current.read_bytes())
    df = scraper.filter_active_schools(df)
    df = scraper.filter_target_cities(df)
    df = scraper.convert_utm_to_wgs84(df)
    df = scraper.normalize_columns(df)
    ssi_path = max(CACHE_DIR.glob('nrw_schulsozialindex_sj_20??_??.csv'))
    df = scraper.merge_schulsozialindex(df, scraper.parse_schulsozialindex_csv(ssi_path.read_bytes()))
    df = df[df['schulnummer'].astype(str).isin(schulnummern)]
    missing = schulnummern - set(df['schulnummer'].astype(str))
    if missing:
        sys.exit(f"Not active in Düsseldorf/Köln per {current.name}: {sorted(missing)}")
    logger.info(f"Master data from {current.name} + {ssi_path.name}")
    primary, secondary = scraper.split_by_school_type(df)
    return {'primary': primary, 'secondary': secondary}


def run_phases(school_type: str, box: dict, skip_poi: bool, skip_website: bool = False):
    import nrw_traffic_enrichment as traffic
    import nrw_transit_enrichment as transit
    import nrw_crime_enrichment as crime
    import nrw_poi_enrichment as poi
    import nrw_website_metadata_enrichment as website
    import nrw_data_combiner as combiner
    import nrw_embeddings_generator as embeddings
    import nrw_to_berlin_schema as schema

    for mod in (traffic, transit, crime, poi, website, combiner):
        mod.RAW_DIR, mod.INTERMEDIATE_DIR = box['raw'], box['intermediate']
    transit.CACHE_DIR = box['cache']
    poi.CHECKPOINT_FILE = box['intermediate'] / 'nrw_poi_enrichment_checkpoint.json'
    combiner.FINAL_DIR = embeddings.FINAL_DIR = box['final']
    schema.NRW_DATA_DIR = box['final']

    steps = [('traffic', traffic.enrich_schools), ('transit', transit.enrich_schools),
             ('crime', crime.enrich_schools)]
    if not skip_poi:
        steps.append(('POI', poi.enrich_schools))
    if not skip_website:  # the combiner falls back to the POI output
        steps.append(('website metadata', website.enrich_schools))
    steps.append(('combine', combiner.combine_school_type))
    for name, fn in steps:
        logger.info(f"── {school_type}: {name}")
        fn(school_type)
    logger.info(f"── {school_type}: descriptions (embeddings skipped)")
    os.environ['SKIP_EMBEDDINGS'] = '1'
    embeddings.process_school_type(school_type)
    logger.info(f"── {school_type}: Berlin schema")
    schema.transform_to_berlin_schema(school_type)


def append_to_finals(school_type: str, box: dict, schulnummern: set):
    for city_parquet in sorted(box['final'].glob(f'*_{school_type}_school_master_table_final_with_embeddings.parquet')):
        slug = city_parquet.name.split('_')[0]
        if slug == 'nrw' or city_parquet.name.startswith('.'):  # ._* = macOS metadata on the exFAT drive
            continue
        new = pd.read_parquet(city_parquet)
        new = new[new['schulnummer'].astype(str).isin(schulnummern)]
        if new.empty:
            continue
        targets = [FINAL_DIR / f"{slug}_{school_type}_school_master_table_final.csv",
                   FINAL_DIR / f"{slug}_{school_type}_school_master_table_final_with_embeddings.parquet",
                   FINAL_DIR / f"nrw_{school_type}_school_master_table_final_with_embeddings.parquet"]
        for path in targets:
            df = _load(path)
            present = set(df['schulnummer'].astype(str)) & schulnummern
            if present:
                logger.info(f"  {path.name}: already has {sorted(present)}, skipped")
                continue
            dropped = [c for c in new.columns if c not in df.columns and new[c].notna().any()]
            if dropped:
                logger.warning(f"  {path.name}: populated columns not in the finals, not carried: {dropped}")
            row = new.reindex(columns=df.columns)
            if 'embedding' in df.columns:
                row['embedding'] = None
            # Ranks computed in the sandbox are "1 of 1"; crime is district-level, so
            # take them from the existing schools in the same Bezirk
            for c in RELATIVE_BY_BEZIRK:
                if c in df.columns and 'bezirk' in df.columns:
                    peers = df.loc[df['bezirk'] == row['bezirk'].iloc[0], c].dropna()
                    row[c] = peers.mode().iloc[0] if len(peers) else None
            for c in df.columns:  # keep the finals' dtypes where the value allows it
                if pd.api.types.is_integer_dtype(df[c]) and row[c].notna().all():
                    row[c] = row[c].astype(df[c].dtype)
                elif df[c].dtype == object and row[c].notna().all() \
                        and df[c].dropna().map(lambda v: isinstance(v, str)).all():
                    row[c] = row[c].astype(str)  # e.g. transit_*_lines: text in the finals, int here
            out = add_stable_fields(pd.concat([df, row], ignore_index=True))
            out = out[df.columns]
            before = df.reset_index(drop=True)
            assert out.iloc[:len(before)].astype(str).equals(
                add_stable_fields(before)[df.columns].astype(str)), f"{path.name}: existing rows changed"
            _save(out, path)
            filled = int(row.iloc[0].notna().sum())
            logger.info(f"  {path.name}: {len(df)} → {len(out)} rows (new row fills {filled}/{len(df.columns)} columns)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--schulnummer', required=True, help='Comma-separated schulnummern')
    ap.add_argument('--skip-poi', action='store_true', help='No Google Places calls')
    ap.add_argument('--append-only', action='store_true',
                    help="Reuse today's sandbox output; only append to the finals")
    args = ap.parse_args()
    wanted = {s.strip() for s in args.schulnummer.split(',') if s.strip()}

    box = sandbox_dirs(CACHE_DIR / 'new_schools_sandbox' / f"{datetime.now():%Y-%m-%d}")
    logger.info(f"Sandbox: {box['raw'].parent.relative_to(PROJECT_ROOT)}")
    rows = master_rows(wanted)
    for school_type, df in rows.items():
        df.to_csv(box['raw'] / f"nrw_{school_type}_schools.csv", index=False, encoding='utf-8-sig')
    for school_type, df in rows.items():
        if df.empty:
            continue
        logger.info(f"{school_type}: {sorted(df['schulnummer'].astype(str))}")
        if not args.append_only:
            run_phases(school_type, box, args.skip_poi)
        append_to_finals(school_type, box, wanted)


if __name__ == '__main__':
    sys.exit(main())
