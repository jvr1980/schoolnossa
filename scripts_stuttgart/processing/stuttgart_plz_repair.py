#!/usr/bin/env python3
"""
Stuttgart PLZ repair (2026-08-23)

Background
----------
Until 2026-08-23 scrapers/stuttgart_school_scraper.py extracted the PLZ with
``re.search(r'(\\d{5})\\s*(Stuttgart)?', html)`` over the whole detail page.
The first 5-digit number on a stuttgart.de directory page is the directory
entry id (the same number that builds ``schulnummer = 'STG-<id>'``), so every
Stuttgart row shipped with ``plz == schulnummer digits`` (e.g. STG-17965 ->
plz 17965). The wrong PLZ was also baked into the templated ``description``
("Adresse: Willy-Brandt-Straße 4, 19965 Stuttgart") and a few LLM
``description_de`` texts, and uploaded to Supabase.

The scraper is fixed (schema.org JSON-LD postalCode / Anschrift box) and the
free chain (scraper -> traffic -> transit -> crime -> combiner) has been
re-run, so the combiner output ``stuttgart_<type>_school_master_table.parquet``
already carries the correct ``plz``. The paid text columns are carried
forward from the previous finals by merge_enriched_columns and therefore
still contain the old PLZ. This script:

1. builds the old->new PLZ map per schulnummer (old = previous finals, new =
   combiner output),
2. applies an exact per-row string replacement ``'<old> Stuttgart' ->
   '<new> Stuttgart'`` to description / description_de / description_en /
   summary_de / summary_en (descriptions are NOT regenerated, embeddings are
   NOT recomputed — no paid API calls),
3. writes master_table.{csv,parquet}, *_final_with_embeddings.parquet and
   *_final.csv (the schema step scripts_stuttgart/stuttgart_to_berlin_schema.py
   is run afterwards by the caller),
4. emits one idempotent UPDATE per corrected row for the Lovable MCP SQL tool
   (guarded on the old plz) into data_shared/supabase_sql/.

Usage
-----
    python3 scripts_stuttgart/processing/stuttgart_plz_repair.py \
        [--previous-dir data_stuttgart/final/backup_2026-08-23] \
        [--sql-out data_shared/supabase_sql/stuttgart_plz_fix_2026-08-23.sql] \
        [--verify-supabase]        # read-only check that the WHERE guard matches
        [--dry-run]
"""

import argparse
import logging
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
FINAL_DIR = PROJECT_ROOT / "data_stuttgart" / "final"
DEFAULT_PREVIOUS_DIR = FINAL_DIR / "backup_2026-08-23"
DEFAULT_SQL_OUT = PROJECT_ROOT / "data_shared" / "supabase_sql" / f"stuttgart_plz_fix_{date.today().isoformat()}.sql"

PLZ_RE = re.compile(r'^7[01]\d{3}$')
TEXT_COLS = ['description', 'description_de', 'description_en', 'summary_de', 'summary_en']
# Supabase text columns that can carry the baked-in PLZ (both tables have them)
SQL_TEXT_COLS = ['description', 'description_de', 'description_en']
TABLES = {'secondary': 'schools', 'primary': 'primary_schools'}


def _norm_plz(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ''
    s = str(v).strip()
    if s.endswith('.0'):
        s = s[:-2]
    return s


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def repair_school_type(school_type: str, previous_dir: Path, dry_run: bool):
    """Return (df_fixed, rows) where rows = [(schulnummer, old, new, n_text_replacements)]."""
    table_pq = FINAL_DIR / f"stuttgart_{school_type}_school_master_table.parquet"
    prev_csv = previous_dir / f"stuttgart_{school_type}_school_master_table_final.csv"
    if not table_pq.exists():
        raise FileNotFoundError(f"combiner output missing: {table_pq} — run stuttgart_data_combiner.py first")
    if not prev_csv.exists():
        raise FileNotFoundError(f"previous final missing: {prev_csv}")

    df = pd.read_parquet(table_pq)
    prev = pd.read_csv(prev_csv, low_memory=False, dtype={'plz': str, 'schulnummer': str})
    prev_plz = dict(zip(prev['schulnummer'].astype(str).str.strip(), prev['plz'].map(_norm_plz)))

    df['schulnummer'] = df['schulnummer'].astype(str).str.strip()
    df['plz'] = df['plz'].map(_norm_plz)

    rows = []
    unresolved = []
    text_cols = [c for c in TEXT_COLS if c in df.columns]
    for idx, r in df.iterrows():
        snr, new = r['schulnummer'], r['plz']
        old = prev_plz.get(snr)
        if not PLZ_RE.match(new):
            unresolved.append((snr, old, new))
            continue
        if old is None or old == new:
            continue
        n_rep = 0
        needle, repl = f"{old} Stuttgart", f"{new} Stuttgart"
        for c in text_cols:
            v = r[c]
            if isinstance(v, str) and needle in v:
                df.at[idx, c] = v.replace(needle, repl)
                n_rep += v.count(needle)
        rows.append((snr, old, new, n_rep))

    logger.info(f"[{school_type}] rows={len(df)} plz corrected={len(rows)} "
                f"text replacements={sum(n for *_, n in rows)} unresolved={len(unresolved)}")
    for snr, old, new in unresolved:
        logger.warning(f"[{school_type}] UNRESOLVED plz for {snr}: old={old!r} new={new!r}")

    # Post-condition: no text column still carries "<old> Stuttgart" for its row
    leftovers = 0
    for idx, r in df.iterrows():
        old = prev_plz.get(r['schulnummer'])
        if not old or old == r['plz']:
            continue
        for c in text_cols:
            v = r[c]
            if isinstance(v, str) and f"{old} Stuttgart" in v:
                leftovers += 1
    if leftovers:
        raise RuntimeError(f"[{school_type}] {leftovers} text cells still contain the old PLZ")

    if not dry_run:
        df.to_parquet(table_pq, index=False)
        df.to_csv(table_pq.with_suffix('.csv'), index=False, encoding='utf-8-sig')
        fe_pq = FINAL_DIR / f"stuttgart_{school_type}_school_master_table_final_with_embeddings.parquet"
        df.to_parquet(fe_pq, index=False)
        df.drop(columns=['embedding'], errors='ignore').to_csv(
            FINAL_DIR / f"stuttgart_{school_type}_school_master_table_final.csv",
            index=False, encoding='utf-8-sig')
        logger.info(f"[{school_type}] wrote {table_pq.name}, .csv, {fe_pq.name}, *_final.csv")
    return df, rows, unresolved


def emit_sql(all_rows: dict, out_path: Path, dry_run: bool) -> int:
    lines = [
        f"-- Stuttgart PLZ fix generated {date.today().isoformat()} by scripts_stuttgart/processing/stuttgart_plz_repair.py",
        "-- Root cause: the stuttgart.de scraper stored the directory entry id as plz (plz == schulnummer digits).",
        "-- One UPDATE per corrected row; idempotent (guarded on the old plz). replace() on NULL text is NULL (no-op).",
        "-- Run via the Lovable MCP SQL tool (anon key is read-only).",
        "",
    ]
    n = 0
    for school_type, rows in all_rows.items():
        table = TABLES[school_type]
        lines.append(f"-- {table} / stuttgart: {len(rows)} row(s)")
        for snr, old, new, _ in rows:
            needle, repl = _sql_str(f"{old} Stuttgart"), _sql_str(f"{new} Stuttgart")
            sets = [f"plz = {_sql_str(new)}"] + [
                f"{c} = replace({c}, {needle}, {repl})" for c in SQL_TEXT_COLS]
            lines.append(
                f"UPDATE {table} SET " + ", ".join(sets) +
                f" WHERE city = 'stuttgart' AND schulnummer = {_sql_str(snr)} AND plz = {_sql_str(old)};")
            n += 1
        lines.append("")
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(lines), encoding='utf-8')
        logger.info(f"wrote {out_path} ({n} statements)")
    return n


def verify_supabase(all_rows: dict):
    """Read-only: confirm the Supabase plz equals the old value the SQL guards on."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts_shared.upload_to_supabase import fetch_supabase_schools
    for school_type, rows in all_rows.items():
        table = TABLES[school_type]
        sb, _, _ = fetch_supabase_schools(table, 'stuttgart', ['plz'])
        sb_plz = {r['schulnummer']: _norm_plz(r.get('plz')) for r in sb}
        match = sum(1 for snr, old, *_ in rows if sb_plz.get(snr) == old)
        missing = [snr for snr, *_ in rows if snr not in sb_plz]
        mismatch = [(snr, old, sb_plz.get(snr)) for snr, old, *_ in rows
                    if snr in sb_plz and sb_plz.get(snr) != old]
        logger.info(f"[{table}] Supabase rows={len(sb)} guard matches={match}/{len(rows)} "
                    f"missing={len(missing)} mismatch={len(mismatch)}")
        for m in mismatch[:10]:
            logger.warning(f"[{table}] guard mismatch {m}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--previous-dir', type=Path, default=DEFAULT_PREVIOUS_DIR,
                    help='directory holding the previous *_final.csv files (old plz values)')
    ap.add_argument('--sql-out', type=Path, default=DEFAULT_SQL_OUT)
    ap.add_argument('--verify-supabase', action='store_true',
                    help='read Supabase (anon, read-only) and confirm the WHERE guards match')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    all_rows = {}
    for st in ['secondary', 'primary']:
        df, rows, unresolved = repair_school_type(st, args.previous_dir, args.dry_run)
        all_rows[st] = rows
        dist = df['plz'].str[:2].value_counts().to_dict()
        logger.info(f"[{st}] plz prefix distribution after fix: {dist}")

    n = emit_sql(all_rows, args.sql_out, args.dry_run)
    if args.verify_supabase:
        verify_supabase(all_rows)
    print(f"\nSQL statements: {n} -> {args.sql_out}")


if __name__ == '__main__':
    main()
