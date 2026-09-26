#!/usr/bin/env python3
"""
Write the newest Berlin per-school student and teacher counts into the Berlin
final tables, without re-running the pipeline.

Reads the newest data_berlin/raw/bildungsstatistik_{school_year}.csv (since
2025/26 produced by scrapers/scrape_schulportrait_statistics.py), writes
schueler_{school_year} / lehrer_{school_year} on schulnummer == BSN, and
re-derives the stable fields so schueler_current / lehrer_current /
data_school_year advance. Older year columns are untouched.

Only schueler_* / lehrer_* and the stable fields change (asserted); re-running
changes nothing. Schools without a new figure keep their previous one, so a
private school without 2025/26 teacher data keeps its 2024/25 lehrer_current.

Usage:
    venv/bin/python scripts_berlin/processing/refresh_bildungsstatistik_counts.py --dry-run
    venv/bin/python scripts_berlin/processing/refresh_bildungsstatistik_counts.py
Then: venv/bin/python scripts_shared/emit_school_year_advance_sql.py --city berlin --out DIR
"""

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.processing.refresh_traffic_columns import _load, _save  # noqa: E402
from scripts_shared.schema.stable_fields import add_stable_fields  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DIR = PROJECT_ROOT / "data_berlin" / "raw"
FINAL_FILES = [
    "data_berlin/final/school_master_table_final.csv",
    "data_berlin/final/school_master_table_final_with_embeddings.parquet",
    "data_berlin_primary/final/grundschule_master_table_final.csv",
    "data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet",
]
SOURCE_COLUMNS = {'Schüler (m/w/d)': 'schueler', 'Lehrkräfte (m,w,d)': 'lehrer'}
STABLE = {'schueler_current', 'lehrer_current', 'data_school_year'}
TEACHER_JUMP = 0.4  # flag |Δ| > 40 % year on year: the portrait's "Lehrkräfte" may be defined differently


def newest_stats():
    files = sorted(p for p in RAW_DIR.glob('bildungsstatistik_20??_??.csv'))
    if not files:
        sys.exit(f"No bildungsstatistik_*.csv in {RAW_DIR}")
    path = files[-1]
    school_year = re.search(r'(20\d\d_\d\d)', path.name).group(1)
    df = pd.read_csv(path, sep=';', dtype={'BSN': str})
    df['BSN'] = df['BSN'].str.strip()
    stats = {prefix: pd.to_numeric(df.set_index('BSN')[col], errors='coerce').dropna()
             for col, prefix in SOURCE_COLUMNS.items()}
    logger.info(f"{path.name}: {len(df)} schools, {len(stats['schueler'])} with students, "
                f"{len(stats['lehrer'])} with teachers")
    return school_year, stats


def refresh_file(path: Path, school_year: str, stats: dict, dry_run: bool):
    df = _load(path)
    before = df.copy()
    key = df['schulnummer'].astype(str).str.strip()
    for prefix, values in stats.items():
        col = f"{prefix}_{school_year}"
        new = key.map(values)
        df[col] = new.combine_first(df[col]) if col in df.columns else new
        logger.info(f"    {col}: {int(new.notna().sum())}/{len(df)} rows")
    df = add_stable_fields(df)

    assert len(df) == len(before), "row count changed"

    def as_text(s):
        """Value-level text for the change check: None/NaN/<NA> are all missing, and a
        CSV's 2025.0 equals the '2025' add_stable_fields writes for vintage stamps."""
        def one(v):
            if v is None or v is pd.NA or (isinstance(v, float) and pd.isna(v)):
                return 'nan'
            if isinstance(v, float) and v.is_integer():
                return str(int(v))
            return str(v)
        return s.map(one)

    changed = [c for c in df.columns
               if c not in before.columns or not as_text(df[c]).equals(as_text(before[c]))]
    unexpected = [c for c in changed if not c.startswith(('schueler_', 'lehrer_')) and c not in STABLE]
    assert not unexpected, f"unexpected columns changed: {unexpected}"

    prev_year = f"{int(school_year[:4]) - 1}_{school_year[2:4]}"
    prev_col, new_col = f"lehrer_{prev_year}", f"lehrer_{school_year}"
    if prev_col in df.columns and new_col in df.columns:
        ratio = df[new_col] / df[prev_col]
        jumps = df[(ratio - 1).abs() > TEACHER_JUMP]
        for _, r in jumps.iterrows():
            logger.warning(f"    teacher jump {r['schulnummer']} {str(r.get('schulname'))[:40]}: "
                           f"{r[prev_col]:.0f} → {r[new_col]:.0f}")
    logger.info(f"    changed columns: {changed}; data_school_year: "
                f"{df['data_school_year'].value_counts(dropna=False).to_dict()}")
    if not dry_run:
        _save(df, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Report changes without writing')
    args = ap.parse_args()

    school_year, stats = newest_stats()
    for rel in FINAL_FILES:
        path = PROJECT_ROOT / rel
        if not path.exists():
            logger.warning(f"  final missing, skipping: {rel}")
            continue
        logger.info(f"  {rel}")
        refresh_file(path, school_year, stats, args.dry_run)

    if args.dry_run:
        logger.info("DRY RUN — nothing written.")


if __name__ == '__main__':
    sys.exit(main())
