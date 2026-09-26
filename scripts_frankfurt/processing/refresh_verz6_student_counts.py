#!/usr/bin/env python3
"""
Refresh official Verzeichnis 6 student counts in the Frankfurt final tables,
without re-running the pipeline.

Use case: Hessen publishes a new Verz6 edition each late summer (survey of
1 November, i.e. the running school year) while Frankfurt's base — the
Schulwegweiser school list — is unchanged. This writes schueler_{school_year}
for the newest edition and the one before it into the finals on schulnummer,
then re-derives the stable fields so schueler_current / data_school_year
advance to the new school year.

Only the schueler_* columns and the stable fields change. Rows without a
Verz6 match (SW-* ids) keep their values; ndh_count is left alone.

Usage:
    venv/bin/python scripts_frankfurt/processing/refresh_verz6_student_counts.py --dry-run
    venv/bin/python scripts_frankfurt/processing/refresh_verz6_student_counts.py
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
for p in (PROJECT_ROOT, PROJECT_ROOT / "scripts_frankfurt" / "scrapers"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from frankfurt_verz6_enrichment import apply_verz6_counts, get_verz6, load_verz6  # noqa: E402
from scripts_shared.processing.refresh_traffic_columns import CITY_CONFIG, _load, _save  # noqa: E402
from scripts_shared.schema.stable_fields import add_stable_fields  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Same eight final files the traffic refresh maintains (primary + secondary)
FINAL_FILES = [rel for _, finals in CITY_CONFIG['frankfurt'] for rel in finals]


def refresh_file(path: Path, editions, dry_run: bool):
    df = _load(path)
    before = df.copy()
    for verz6_df in editions:
        df, written = apply_verz6_counts(df, verz6_df)
        logger.info(f"    schueler_{verz6_df.attrs['school_year']}: {written}/{len(df)} rows from Verz6")
    df = add_stable_fields(df)

    assert len(df) == len(before), "row count changed"

    def as_text(s):  # None / NaN / <NA> all mean "missing" for the change check
        return s.astype(str).replace({'None': 'nan', '<NA>': 'nan'})

    changed = [c for c in df.columns
               if c not in before.columns or not as_text(df[c]).equals(as_text(before[c]))]
    unexpected = [c for c in changed if not c.startswith('schueler_')
                  and c not in ('data_school_year', 'lehrer_data_year', 'migration_data_year')]
    assert not unexpected, f"unexpected columns changed: {unexpected}"

    old_cur = pd.to_numeric(before.get('schueler_current'), errors='coerce')
    moved = (df['schueler_current'] != old_cur) & df['schueler_current'].notna()
    logger.info(f"    changed columns: {changed}")
    logger.info(f"    schueler_current changed on {int(moved.sum())} rows; "
                f"data_school_year: {df['data_school_year'].value_counts(dropna=False).to_dict()}")
    if not dry_run:
        _save(df, path)
    return df, before


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Report changes without writing')
    args = ap.parse_args()

    edition, latest_path = get_verz6()
    editions = []
    try:
        _, prior_path = get_verz6(edition - 1)
        editions.append(load_verz6(prior_path))  # older first so the newest is written last
    except FileNotFoundError as e:
        logger.warning(f"{e} — only the newest edition is applied")
    editions.append(load_verz6(latest_path))

    for rel in FINAL_FILES:
        path = PROJECT_ROOT / rel
        if not path.exists():
            logger.warning(f"  final missing, skipping: {rel}")
            continue
        logger.info(f"  {rel}")
        refresh_file(path, editions, args.dry_run)

    if args.dry_run:
        logger.info("DRY RUN — nothing written.")


if __name__ == '__main__':
    sys.exit(main())
