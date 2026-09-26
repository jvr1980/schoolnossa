#!/usr/bin/env python3
"""
Recompute the location-derived data (traffic, transit, crime, POIs, Bezirk) of
NRW schools that moved, without touching anything else.

refresh_nrw_master_delta.py already moved their address and coordinates; the
enrichment values still describe the old site. This runs the real pipeline
phases for just these schools in a sandbox (add_new_schools.run_phases, website
phase skipped) and overwrites only location columns in the finals:
- groups (e.g. `poi_kita`, `transit_bus`, `crime`) are replaced as a whole when
  the sandbox produced any value for them, so stale slots cannot survive;
- `bezirk` follows the new site; crime_safety_rank/category come from the
  existing schools of the new Bezirk (the sandbox would rank 1 of 1).
Anything else is asserted unchanged. --emit-sql DIR writes the matching
Supabase UPDATEs plus a rollback snapshot of the live values.

Usage:
    venv/bin/python scripts_nrw/processing/refresh_school_locations.py --schulnummer 164501,100212 --dry-run
    venv/bin/python scripts_nrw/processing/refresh_school_locations.py --schulnummer 164501,100212 \
        --emit-sql data_shared/supabase_sql/nrw_relocations_2026-09
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts_nrw" / "processing"))

import add_new_schools as ans  # noqa: E402  (sets up sys.path for the NRW modules)
from scripts_shared.processing.refresh_traffic_columns import CITY_CONFIG, _load, _save  # noqa: E402
from scripts_shared.upload_to_supabase import (  # noqa: E402
    COL_ALIASES, _coerce, _infer_pg_types, _sql_literal, fetch_supabase_schools, load_local_df,
)

logger = logging.getLogger(__name__)

LOCATION_PREFIXES = ('traffic_', 'transit_', 'poi_', 'crime_')
EXTRA_COLUMNS = ['bezirk']
RELATIVE = ['crime_safety_rank', 'crime_safety_category']
FINAL_FILES = [rel for _, finals in CITY_CONFIG['nrw'] for rel in finals]


def group_of(col: str) -> str:
    """poi_kita_03_name → poi_kita, transit_bus_02_lines → transit_bus, crime_* → crime."""
    if col.startswith('crime_'):
        return 'crime'
    m = re.match(r'^((?:poi|transit)_[a-z_]+?)_(?:\d\d_|count_|distance|lines|name)', col)
    return m.group(1) if m else col


def sandbox_rows(wanted: set, skip_poi: bool, reuse: bool = False) -> pd.DataFrame:
    box = ans.sandbox_dirs(ans.CACHE_DIR / 'relocations_sandbox' / f"{datetime.now():%Y-%m-%d}")
    rows = ans.master_rows(wanted)
    out = []
    for school_type, df in rows.items():
        df.to_csv(box['raw'] / f"nrw_{school_type}_schools.csv", index=False, encoding='utf-8-sig')
        if df.empty:
            continue
        if not reuse:  # --reuse: take today's sandbox output (e.g. after a --dry-run) without new API calls
            ans.run_phases(school_type, box, skip_poi, skip_website=True)
        for p in box['final'].glob(f'*_{school_type}_school_master_table_final_with_embeddings.parquet'):
            if not p.name.startswith(('nrw_', '.')):
                out.append(pd.read_parquet(p))
    new = pd.concat(out, ignore_index=True)
    new['schulnummer'] = new['schulnummer'].astype(str)
    return new[new['schulnummer'].isin(wanted)]


def refresh_file(path: Path, new: pd.DataFrame, dry_run: bool):
    df = _load(path)
    snr = df['schulnummer'].astype(str)
    targets = [c for c in df.columns if c.startswith(LOCATION_PREFIXES) or c in EXTRA_COLUMNS]
    before = df.copy()
    edits = {}
    for s, fresh in new.set_index('schulnummer').iterrows():
        idx = df.index[snr == s]
        if len(idx) == 0:
            continue
        i = idx[0]
        groups = {group_of(c) for c in targets if c in fresh.index and pd.notna(fresh[c])}
        cols = [c for c in targets if group_of(c) in groups or c in EXTRA_COLUMNS]
        for c in cols:
            v = fresh[c] if c in fresh.index else None
            if isinstance(v, float) and pd.isna(v):
                v = None
            if pd.api.types.is_integer_dtype(df[c]) and v is not None:
                v = int(v)
            elif df[c].dtype == object and v is not None and not isinstance(v, str) \
                    and df[c].dropna().map(lambda x: isinstance(x, str)).all():
                v = str(v)
            if isinstance(v, str) and df[c].dtype != object:  # e.g. a line "V2" in a numeric lines column
                df[c] = df[c].astype(object)
            df.at[i, c] = v
        for c in RELATIVE:  # district-level rank from the peers of the new Bezirk
            if c in df.columns:
                peers = df.loc[(df['bezirk'] == df.at[i, 'bezirk']) & (df.index != i), c].dropna()
                df.at[i, c] = peers.mode().iloc[0] if len(peers) else None
        changed = [c for c in df.columns if str(before.at[i, c]) != str(df.at[i, c])]
        edits[s] = changed
        for c in changed:
            logger.debug(f"      {c}: {before.at[i, c]!r} → {df.at[i, c]!r}")
        logger.info(f"    {s}: {len(changed)} columns changed "
                    f"(bezirk {before.at[i, 'bezirk'] if 'bezirk' in df else '-'} → {df.at[i, 'bezirk'] if 'bezirk' in df else '-'})")

    others = df.index[~snr.isin(new['schulnummer'])]
    assert df.loc[others].astype(str).equals(before.loc[others].astype(str)), "other rows changed"
    off_target = {c for cs in edits.values() for c in cs if not (c.startswith(LOCATION_PREFIXES) or c in EXTRA_COLUMNS)}
    assert not off_target, f"non-location columns changed: {off_target}"
    if edits and not dry_run:
        _save(df, path)
    return edits


def emit_sql(out_dir: Path, wanted: set):
    out_dir.mkdir(parents=True, exist_ok=True)
    apply, rollback = [], []
    for city in ('duesseldorf', 'koeln'):
        for table, kind in (('schools', 'secondary'), ('primary_schools', 'primary')):
            local = load_local_df(PROJECT_ROOT / f"data_nrw/final/{city}_{kind}_school_master_table_final.csv")
            local['schulnummer'] = local['schulnummer'].astype(str)
            local = local[local['schulnummer'].isin(wanted)]
            if local.empty:
                continue
            fields = sorted({c for c in local.columns
                             if c.startswith(LOCATION_PREFIXES) or c in EXTRA_COLUMNS or c in COL_ALIASES.values()}
                            - {'schueler_2024_25', 'sprachen'})
            sb_rows, active, _ = fetch_supabase_schools(table, city, fields)
            types = _infer_pg_types(sb_rows, active)
            sb = {str(r['schulnummer']): r for r in sb_rows}
            for _, row in local.iterrows():
                s = row['schulnummer']
                if s not in sb:
                    continue
                # Bare NULL: all-NULL columns get a guessed type (text) that would not
                # cast into integer/numeric columns
                lit = lambda v, f: 'NULL' if v is None else _sql_literal(v, types[f])
                sets = [f"{f} = {lit(_coerce(f, row.get(f)), f)}" for f in active]
                olds = [f"{f} = {lit(sb[s].get(f), f)}" for f in active]
                where = f"WHERE city = '{city}' AND schulnummer = '{s}';"
                apply.append(f"UPDATE {table} SET {', '.join(sets)}\n  {where}  -- {row.get('schulname')}")
                rollback.append(f"UPDATE {table} SET {', '.join(olds)}\n  {where}")
    (out_dir / 'relocations_apply.sql').write_text('\n'.join(apply) + '\n', encoding='utf-8')
    (out_dir / 'relocations_rollback_snapshot.sql').write_text(
        f"-- Live values before the relocation refresh ({datetime.now():%Y-%m-%d %H:%M})\n" + '\n'.join(rollback) + '\n',
        encoding='utf-8')
    logger.info(f"Wrote {len(apply)} UPDATEs + rollback to {out_dir}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--schulnummer', required=True)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--skip-poi', action='store_true')
    ap.add_argument('--emit-sql', type=Path, metavar='DIR')
    ap.add_argument('--reuse', action='store_true', help="Reuse today's sandbox output instead of re-running the phases")
    args = ap.parse_args()
    wanted = {s.strip() for s in args.schulnummer.split(',') if s.strip()}

    new = sandbox_rows(wanted, args.skip_poi, args.reuse)
    logger.info(f"Sandbox produced {len(new)} rows: {sorted(new['schulnummer'])}")
    for rel in FINAL_FILES:
        path = PROJECT_ROOT / rel
        if path.exists():
            logger.info(f"  {rel}")
            refresh_file(path, new, args.dry_run)
    if args.emit_sql and not args.dry_run:
        emit_sql(args.emit_sql, wanted)
    if args.dry_run:
        logger.info("DRY RUN — nothing written.")


if __name__ == '__main__':
    sys.exit(main())
