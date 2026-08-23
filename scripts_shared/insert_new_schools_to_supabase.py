#!/usr/bin/env python3
"""
Insert schools that exist locally but not yet in Supabase (e.g. new schools
found by a data refresh).

Complements upload_to_supabase.py, which is strictly PATCH (fill gaps on rows
that already exist) and can never add a row. Rows are matched on
(city, schulnummer); a local row whose schulnummer is absent from the Supabase
table for that city is a candidate.

Modes (default is a dry-run listing — nothing is written):
    --emit-sql FILE   write idempotent INSERT ... WHERE NOT EXISTS statements
                      for the Lovable MCP SQL tool (Lovable Cloud owns the DB;
                      the anon key is read-only, so this is the usual route)
    --apply           POST rows via PostgREST (needs SUPABASE_SERVICE_ROLE_KEY
                      or a Lovable-installed INSERT policy)

Usage:
    # List every local-only row per city/table (pre-existing drift included)
    python3 scripts_shared/insert_new_schools_to_supabase.py

    # Only the schools a refresh found, as SQL for the Lovable SQL tool
    python3 scripts_shared/insert_new_schools_to_supabase.py \\
        --ids 04A44,5605-0,7747-0,5485-0,STG-19965,138,449 \\
        --emit-sql data_shared/supabase_sql/insert_new_schools_2026-08-23.sql

    # Same set, live via REST
    python3 scripts_shared/insert_new_schools_to_supabase.py --ids 04A44,... --apply

Column handling: every Supabase column of the target table that the local row
populates is inserted (id/created_at/updated_at are left to their defaults;
the pgvector `embedding` column is skipped). Values are typed from the live
table (types inferred from existing rows) so the generated SQL is cast-safe.
"""

import argparse
import json
import logging
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import scripts_shared.upload_to_supabase as up  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Per (city, table) source override. upload_to_supabase.CITY_FILES is the
# default; Hamburg's `_final.csv` keeps Hamburg-native column names (no plz,
# strasse, email ...), so inserts read the Berlin-canonical output of
# scripts_hamburg/hamburg_to_berlin_schema.py instead.
INSERT_SOURCES = {
    ('hamburg', 'schools'):
        'data_hamburg/final/hamburg_school_master_table_berlin_schema.csv',
    ('hamburg', 'primary_schools'):
        'data_hamburg_primary/final/hamburg_primary_school_master_table_berlin_schema.csv',
}

# Never sent: server defaults / not insertable from flat local data.
EXCLUDE_COLS = {'id', 'created_at', 'updated_at', 'embedding'}

# Columns that MUST be present for a row to be inserted.
REQUIRED = ['schulnummer', 'schulname']


# ----------------------------------------------------------------------------
# Supabase side
# ----------------------------------------------------------------------------

_TYPE_CACHE = {}


def table_pg_types(table):
    """Infer a Postgres type per column from existing rows (embedding skipped).

    Pages through the table without the vector column; all-NULL columns fall
    back to the uploader's INTEGER/FLOAT sets, else text.
    """
    if table in _TYPE_CACHE:
        return _TYPE_CACHE[table]
    cols = sorted(c for c in up.get_table_columns(table) if c != 'embedding')
    rows, offset = [], 0
    select = ','.join(cols)
    while True:
        data = up.supabase_request(
            'GET', f'{table}?select={select}&offset={offset}&limit=1000')
        rows.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    types = up._infer_pg_types(rows, cols)
    _TYPE_CACHE[table] = types
    return types


def existing_schulnummern(table, city):
    rows, _, _ = up.fetch_supabase_schools(table, city, [])
    return {str(r['schulnummer']) for r in rows if r.get('schulnummer')}


# ----------------------------------------------------------------------------
# Local side
# ----------------------------------------------------------------------------

def iter_sources(only_city=None, only_table=None):
    for city, sec, pri in up.CITY_FILES:
        if only_city and city != only_city:
            continue
        for table, default_rel in (('schools', sec), ('primary_schools', pri)):
            if only_table and table != only_table:
                continue
            rel = INSERT_SOURCES.get((city, table), default_rel)
            path = up.PROJECT_ROOT / rel
            if not path.exists():
                logger.warning(f'  Skipping {city}/{table}: {rel} not found')
                continue
            yield city, table, rel, path


def coerce_for(pg_type, value):
    """Python value for REST / literal rendering, typed by the target column."""
    if up._is_empty(value):
        return None
    if pg_type == 'integer':
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
    if pg_type == 'numeric':
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if pg_type == 'boolean':
        if isinstance(value, str):
            return value.strip().lower() in ('true', '1', 'yes', 'ja', 't')
        return bool(value)
    if pg_type == 'jsonb':
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value
    # text: tidy float-ish ints ('13627.0' -> '13627'), numpy scalars -> str
    s = str(value).strip()
    if re.match(r'^-?\d+\.0$', s):
        s = s[:-2]
    return s


def build_payload(local_row, table_cols, pg_types, city):
    payload = {}
    for col in table_cols:
        if col in EXCLUDE_COLS or col == 'city':
            continue
        if col not in local_row.index:
            continue
        val = coerce_for(pg_types.get(col, 'text'), local_row[col])
        if val is None:
            continue
        payload[col] = val
    payload['city'] = city
    # A 0/0 transit summary is the transit enricher's failure marker (API
    # unreachable), not data — no German city school has zero stops within
    # 1 km. Leave both NULL so a later fill-gaps run can populate them
    # (a stored 0 would block it).
    if (payload.get('transit_stop_count_1000m') == 0
            and payload.get('transit_accessibility_score') == 0):
        payload.pop('transit_stop_count_1000m')
        payload.pop('transit_accessibility_score')
    return payload


def sql_literal(value, pg_type):
    """Literal for INSERT ... SELECT (target-column typed; no casts needed
    for unknown string literals, bare numbers for numeric/integer)."""
    if value is None:
        return 'NULL'
    if pg_type in ('integer', 'numeric'):
        return repr(value) if isinstance(value, float) else str(value)
    if pg_type == 'boolean':
        return 'TRUE' if value else 'FALSE'
    if pg_type == 'jsonb':
        txt = json.dumps(value, ensure_ascii=False)
    else:
        txt = str(value)
    return "'" + txt.replace("'", "''") + "'"


def insert_sql(table, payload, pg_types):
    cols = sorted(payload)
    lits = [sql_literal(payload[c], pg_types.get(c, 'text')) for c in cols]
    city = payload['city'].replace("'", "''")
    snr = str(payload['schulnummer']).replace("'", "''")
    return (
        f"-- {table} / {payload['city']} / {payload['schulnummer']}: "
        f"{payload.get('schulname', '')!s}\n"
        f"INSERT INTO {table} ({', '.join(cols)})\n"
        f"SELECT {', '.join(lits)}\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {table} "
        f"WHERE city = '{city}' AND schulnummer = '{snr}');\n"
    )


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--city', default=None, help='Limit to one city (Supabase city value)')
    p.add_argument('--table', default=None, choices=['schools', 'primary_schools'])
    p.add_argument('--ids', default=None,
                   help='Comma-separated schulnummern to insert (default: list all '
                        'local-only rows; with --apply/--emit-sql, all of them)')
    p.add_argument('--emit-sql', metavar='FILE', default=None,
                   help='Write idempotent INSERT statements to FILE (no REST writes)')
    p.add_argument('--apply', action='store_true',
                   help='POST rows via PostgREST (service-role key or INSERT policy required)')
    p.add_argument('--list-only', action='store_true',
                   help='Only list local-only rows (default behaviour without --apply/--emit-sql)')
    p.add_argument('--assume-stable-cols', action='store_true',
                   help='Treat the stable-field columns from '
                        'scripts_shared/schema/supabase_stable_fields.sql as existing '
                        '(for SQL generated before that DDL has run, to be executed '
                        'right after it)')
    return p.parse_args()


def main():
    args = parse_args()
    wanted = None
    if args.ids:
        wanted = {s.strip() for s in args.ids.split(',') if s.strip()}

    mode = ('EMIT SQL' if args.emit_sql else 'APPLY (REST POST)' if args.apply
            else 'LIST (dry run)')
    print('=' * 72)
    print('SUPABASE INSERT NEW SCHOOLS')
    print('=' * 72)
    print(f'Mode:    {mode}')
    if wanted:
        print(f'IDs:     {", ".join(sorted(wanted))}')
    print()

    planned = []   # (table, payload)
    seen_wanted = set()
    for city, table, rel, path in iter_sources(args.city, args.table):
        df = up.load_local_df(path)
        if 'schulnummer' not in df.columns:
            logger.warning(f'  {city}/{table}: no schulnummer column in {rel}')
            continue
        present = existing_schulnummern(table, city)
        local_ids = df['schulnummer'].astype(str)
        cand = df[~local_ids.isin(present) & local_ids.notna()
                  & ~local_ids.isin(['', 'nan', 'None'])]
        if wanted is not None:
            cand = cand[cand['schulnummer'].astype(str).isin(wanted)]
        if cand.empty:
            continue
        print(f'{city} / {table} ({rel}): {len(cand)} local-only row(s), '
              f'Supabase has {len(present)}')
        if args.emit_sql or args.apply:
            pg_types = dict(table_pg_types(table))
        else:
            pg_types = {}
        table_cols = set(up.get_table_columns(table))
        if args.assume_stable_cols:
            for f in up.FIELD_GROUPS['stable']:
                if f in table_cols:
                    continue
                # primary_schools has no abitur/nachfrage/migration stable cols
                if table == 'primary_schools' and f in (
                        'migration_current', 'nachfrage_prozent_current',
                        'abitur_durchschnitt_current', 'abitur_year'):
                    continue
                table_cols.add(f)
                pg_types[f] = ('integer' if f in up.INTEGER_FIELDS
                               else 'numeric' if f in up.FLOAT_FIELDS else 'text')
        for _, row in cand.iterrows():
            snr = str(row['schulnummer'])
            name = row.get('schulname')
            missing_req = [c for c in REQUIRED if up._is_empty(row.get(c))]
            if missing_req:
                print(f'   SKIP {snr}: missing {missing_req}')
                continue
            if args.emit_sql or args.apply:
                payload = build_payload(row, table_cols, pg_types, city)
                planned.append((table, payload))
                seen_wanted.add(snr)
                keyinfo = {k: payload.get(k) for k in
                           ('school_type', 'schulart', 'plz', 'ortsteil', 'bezirk')}
                print(f'   + {snr}  {name}  [{len(payload)} cols]  {keyinfo}')
            else:
                seen_wanted.add(snr)
                print(f'   - {snr}  {name}  '
                      f'({row.get("school_type")}/{row.get("schulart")}, '
                      f'{row.get("ortsteil")}, plz {row.get("plz")})')
        print()

    if wanted:
        not_found = sorted(wanted - seen_wanted)
        if not_found:
            print(f'WARNING: requested ids not found locally or already in '
                  f'Supabase: {", ".join(not_found)}\n')

    if not (args.emit_sql or args.apply):
        print('LIST ONLY — nothing written. Use --ids ... --emit-sql FILE or --apply.')
        return

    if not planned:
        print('Nothing to insert.')
        return

    if args.emit_sql:
        out = up.PROJECT_ROOT / args.emit_sql
        out.parent.mkdir(parents=True, exist_ok=True)
        header = (f"-- New-school INSERTs generated {date.today().isoformat()} by "
                  f"scripts_shared/insert_new_schools_to_supabase.py\n"
                  f"-- {len(planned)} row(s); each statement is idempotent "
                  f"(WHERE NOT EXISTS on city+schulnummer).\n"
                  f"-- Run via the Lovable MCP SQL tool. Literals are typed by the "
                  f"target columns (INSERT ... SELECT).\n\n")
        body = '\n'.join(insert_sql(t, p, table_pg_types(t)) for t, p in planned)
        out.write_text(header + body, encoding='utf-8')
        print(f'Wrote {len(planned)} INSERT statement(s) -> {out} '
              f'({out.stat().st_size / 1024:.1f} KB)')
        return

    # --apply: REST POST, one row at a time, re-checking existence first.
    ok = fail = skipped = 0
    for table, payload in planned:
        city, snr = payload['city'], str(payload['schulnummer'])
        exists = up.supabase_request(
            'GET', f'{table}?select=id&city=eq.{city}&schulnummer=eq.{snr}&limit=1')
        if exists:
            skipped += 1
            print(f'   skip {table}/{city}/{snr}: already present')
            continue
        try:
            up.supabase_request('POST', table, payload, for_writes=True)
            ok += 1
            print(f'   inserted {table}/{city}/{snr}')
        except Exception as e:  # logged by supabase_request
            fail += 1
            print(f'   FAILED {table}/{city}/{snr}: {e}')
    print(f'\nInserted {ok}, failed {fail}, skipped {skipped}.')


if __name__ == '__main__':
    main()
