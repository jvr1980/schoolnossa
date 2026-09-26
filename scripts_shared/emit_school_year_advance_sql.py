#!/usr/bin/env python3
"""
Emit SQL that advances schueler_current (+ lehrer_current) + data_school_year in Supabase when a
newer school-year vintage has landed locally (Wave B: Frankfurt Verz6, NRW
Schulliste, Berlin Bildungsstatistik).

upload_to_supabase.py is fill-gaps only by design, so it cannot move a value
that is already populated. This emits UPDATEs that overwrite the pair only
where the Supabase row's data_school_year is NULL or older than the local one:
idempotent, and a row never moves backwards. Reads Supabase with the anon key;
writes nothing — run the files via the Lovable MCP SQL tool.

Usage:
    venv/bin/python scripts_shared/emit_school_year_advance_sql.py --city frankfurt --dry-run
    venv/bin/python scripts_shared/emit_school_year_advance_sql.py --city frankfurt \
        --out data_shared/supabase_sql/frankfurt_school_year_2025_26
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.upload_to_supabase import (  # noqa: E402
    CITY_FILES, _coerce, _infer_pg_types, _sql_literal, fetch_supabase_schools, load_local_df,
)

FIELDS = ['schueler_current', 'lehrer_current', 'lehrer_data_year', 'data_school_year']


def plan_table(table, city, local_df):
    """[(supabase_id, schulnummer, name, old_value, old_year, new_value, new_year, new_lehrer, lehrer_year)], pg_types."""
    sb_rows, _, missing = fetch_supabase_schools(table, city, FIELDS)
    if missing:
        sys.exit(f"{table} lacks {missing} — run scripts_shared/schema/supabase_stable_fields.sql first")
    sb_by_snr = {str(r['schulnummer']): r for r in sb_rows if r.get('schulnummer')}
    plan = []
    for _, row in local_df.iterrows():
        sb = sb_by_snr.get(str(row.get('schulnummer', '')))
        new_value = _coerce('schueler_current', row.get('schueler_current'))
        new_year = _coerce('data_school_year', row.get('data_school_year'))
        if sb is None or new_value is None or new_year is None:
            continue
        old_year = sb.get('data_school_year')
        if old_year is not None and str(old_year) >= new_year:
            continue
        # lehrer_current rides along; COALESCE in the SQL keeps the live value where we have none
        plan.append((sb['id'], str(row['schulnummer']), row.get('schulname'),
                     sb.get('schueler_current'), old_year, new_value, new_year,
                     _coerce('lehrer_current', row.get('lehrer_current')),
                     _coerce('lehrer_data_year', row.get('lehrer_data_year'))))
    return plan, _infer_pg_types(sb_rows, FIELDS)


def write_sql(out_dir, table, city, plan, pg_types):
    new_years = sorted({p[6] for p in plan})
    # Separator comma before the trailing "-- schulnummer name" comment, never inside it
    values = [f"  ('{sb_id}'::uuid, {_sql_literal(value, pg_types['schueler_current'])}, "
              f"{_sql_literal(lehrer, pg_types['lehrer_current'])}, "
              f"{_sql_literal(lehrer_year, 'text')}, "
              f"{_sql_literal(year, 'text')}){',' if i < len(plan) - 1 else ''}"
              f"  -- {snr} {' '.join(str(name).split())}"
              for i, (sb_id, snr, name, _, _, value, year, lehrer, lehrer_year) in enumerate(plan)]
    lines = [
        f"-- {table} / {city}: advance schueler_current + lehrer_current (+ its vintage) + data_school_year ({len(plan)} rows)",
        "-- Overwrites only rows whose data_school_year is NULL or older than the new one;",
        "-- re-running is a no-op.",
        f"UPDATE {table} AS s SET",
        "  schueler_current = v.schueler_current,",
        "  lehrer_current = COALESCE(v.lehrer_current, s.lehrer_current),",
        "  lehrer_data_year = CASE WHEN v.lehrer_current IS NULL THEN s.lehrer_data_year ELSE v.lehrer_data_year END,",
        "  data_school_year = v.data_school_year",
        "FROM (VALUES",
        *values,
        ") AS v(id, schueler_current, lehrer_current, lehrer_data_year, data_school_year)",
        "WHERE s.id = v.id",
        "  AND (s.data_school_year IS NULL OR s.data_school_year < v.data_school_year);",
        "",
        f"-- Check: SELECT data_school_year, count(*) FROM {table} WHERE city = '{city}' GROUP BY 1;",
        f"-- expected after the update: {', '.join(new_years)} on these {len(plan)} rows",
    ]
    path = Path(out_dir) / f"{table}__{city}.sql"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--city', required=True, choices=[c for c, _, _ in CITY_FILES])
    ap.add_argument('--out', help='Directory for the SQL files (omit with --dry-run)')
    ap.add_argument('--dry-run', action='store_true', help='Print the plan, write nothing')
    args = ap.parse_args()
    if not args.dry_run and not args.out:
        ap.error('--out is required unless --dry-run')

    city, sec_file, pri_file = next(c for c in CITY_FILES if c[0] == args.city)
    for table, rel in (('schools', sec_file), ('primary_schools', pri_file)):
        local_df = load_local_df(PROJECT_ROOT / rel)
        plan, pg_types = plan_table(table, city, local_df)
        print(f"\n{table} / {city}: {len(plan)} rows to advance (local {len(local_df)})")
        for _, snr, name, old_value, old_year, new_value, new_year, _, _ in plan:
            print(f"  {snr:>6} {str(name)[:40]:40} {old_value!s:>6} ({old_year}) -> {new_value:>6} ({new_year})")
        if plan and not args.dry_run:
            print(f"  SQL: {write_sql(PROJECT_ROOT / args.out, table, city, plan, pg_types)}")


if __name__ == '__main__':
    main()
