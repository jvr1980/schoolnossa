#!/usr/bin/env python3
"""
Upload enriched school data to Lovable Supabase (fill gaps only).

Rules:
- FILL GAPS ONLY: UPDATE SET field = local_value WHERE field IS NULL
- NEVER overwrite non-null Supabase values
- Only touch columns listed in SELECTED_FIELDS for the chosen groups
- Match schools by schulnummer

Usage:
    # Dry-run everything (default groups, all cities)
    python3 scripts_shared/upload_to_supabase.py --dry-run

    # Actually upload
    python3 scripts_shared/upload_to_supabase.py

    # Limit to specific groups / cities
    python3 scripts_shared/upload_to_supabase.py --dry-run \
            --groups contact,location,descriptions --city dresden
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SUPABASE_URL = 'https://whzvzoumldeqgyrqlilt.supabase.co/rest/v1'

# Write operations require the service role key (RLS blocks UPDATE for anon).
# Export it in your shell or .env — never commit it:
#   export SUPABASE_SERVICE_ROLE_KEY="eyJ..."
# Reads fall back to the anon key (publicly documented, safe to embed).
SUPABASE_ANON_KEY = (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
    'eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoenZ6b3VtbGRlcWd5cnFsaWx0Iiwicm9sZSI6ImFub24i'
    'LCJpYXQiOjE3Njg3OTQ0MzEsImV4cCI6MjA4NDM3MDQzMX0.'
    'ex4S1up25OAcGD8hQoOSfzf3NVAG5qCmNriixYfAAKs'
)


def _resolve_key(for_writes: bool):
    if for_writes:
        key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
        if not key:
            raise SystemExit(
                'Set SUPABASE_SERVICE_ROLE_KEY in your environment before running '
                'without --dry-run. RLS blocks anon UPDATE on schools/primary_schools.'
            )
        return key
    return os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or SUPABASE_ANON_KEY


def _headers(for_writes: bool):
    key = _resolve_key(for_writes)
    return {
        'apikey': key,
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }

# ============================================================================
# FIELD GROUPS — only the groups you pass via --groups will be updated.
# Each field is uploaded only when Supabase has NULL for that row.
# ============================================================================
FIELD_GROUPS = {
    'contact': ['email', 'telefon', 'website', 'leitung'],
    'location': ['ortsteil', 'bezirk'],
    'school_attr': [
        'schueler_2024_25', 'lehrer_2024_25', 'sprachen',
        'gruendungsjahr', 'besonderheiten', 'traegerschaft', 'schulart',
    ],
    'descriptions': ['description_de', 'description_en'],
    'crime': ['crime_safety_category', 'crime_safety_rank',
              'crime_total_crimes_2023'],
    'transit_summary': ['transit_accessibility_score',
                        'transit_stop_count_1000m',
                        'transit_all_lines_1000m'],
    # Nearest stop (Supabase uses unprefixed names for #1, _02/_03 for the rest)
    'transit_nearest': ['transit_bus_name', 'transit_bus_distance_m',
                        'transit_bus_lines',
                        'transit_rail_name', 'transit_rail_distance_m',
                        'transit_rail_lines',
                        'transit_tram_name', 'transit_tram_distance_m',
                        'transit_tram_lines'],
}
DEFAULT_GROUPS = ['contact', 'location', 'school_attr', 'descriptions',
                  'crime', 'transit_summary']

# Local → Supabase column renames. These apply when scanning the local
# dataframe for the Supabase target column.
COL_ALIASES = {
    # Legacy aggregate column names
    'schueler_gesamt': 'schueler_2024_25',
    'anzahl_schueler_gesamt': 'schueler_2024_25',
    'fremdsprache': 'sprachen',
    # Local uses _01 for nearest stop; Supabase uses the unprefixed name
    'transit_bus_01_name': 'transit_bus_name',
    'transit_bus_01_distance_m': 'transit_bus_distance_m',
    'transit_bus_01_lines': 'transit_bus_lines',
    'transit_rail_01_name': 'transit_rail_name',
    'transit_rail_01_distance_m': 'transit_rail_distance_m',
    'transit_rail_01_lines': 'transit_rail_lines',
    'transit_tram_01_name': 'transit_tram_name',
    'transit_tram_01_distance_m': 'transit_tram_distance_m',
    'transit_tram_01_lines': 'transit_tram_lines',
}

INTEGER_FIELDS = {
    'schueler_2024_25', 'lehrer_2024_25', 'gruendungsjahr',
    'transit_stop_count_1000m', 'crime_safety_rank',
}
FLOAT_FIELDS = {
    'transit_accessibility_score', 'transit_bus_distance_m',
    'transit_rail_distance_m', 'transit_tram_distance_m',
    'crime_total_crimes_2023',
}

# ============================================================================
# City / file configuration
# ============================================================================
CITY_FILES = [
    # (city, schools table file, primary_schools table file)
    # Berlin: source of truth in Supabase already — skip by default
    # ('berlin',
    #  'data_berlin/final/school_master_table_final_with_embeddings.parquet',
    #  'data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet'),
    ('muenchen',
     'data_munich/final/munich_secondary_school_master_table_final.csv',
     'data_munich/final/munich_primary_school_master_table_final.csv'),
    ('stuttgart',
     'data_stuttgart/final/stuttgart_secondary_school_master_table_final.csv',
     'data_stuttgart/final/stuttgart_primary_school_master_table_final.csv'),
    ('dresden',
     'data_dresden/final/dresden_secondary_school_master_table_final.csv',
     'data_dresden/final/dresden_primary_school_master_table_final.csv'),
    ('frankfurt',
     'data_frankfurt/final/frankfurt_secondary_school_master_table_final.csv',
     'data_frankfurt/final/frankfurt_primary_school_master_table_final.csv'),
    ('duesseldorf',
     'data_nrw/final/duesseldorf_secondary_school_master_table_final.csv',
     'data_nrw/final/duesseldorf_primary_school_master_table_final.csv'),
    ('koeln',
     'data_nrw/final/koeln_secondary_school_master_table_final.csv',
     'data_nrw/final/koeln_primary_school_master_table_final.csv'),
    ('hamburg',
     'data_hamburg/final/hamburg_school_master_table_final.csv',
     'data_hamburg_primary/final/hamburg_primary_school_master_table_final.csv'),
    ('bremen',
     'data_bremen/final/bremen_secondary_school_master_table_berlin_schema.csv',
     'data_bremen/final/bremen_primary_school_master_table_berlin_schema.csv'),
    ('leipzig',
     'data_leipzig/final/leipzig_secondary_school_master_table_berlin_schema.csv',
     'data_leipzig/final/leipzig_primary_school_master_table_berlin_schema.csv'),
]

# ============================================================================
# Supabase I/O
# ============================================================================

def supabase_request(method, path, data=None, for_writes=False):
    """Make a Supabase REST API request. Use for_writes=True for PATCH/POST/DELETE."""
    url = f'{SUPABASE_URL}/{path}'
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=_headers(for_writes), method=method)
    try:
        resp = urllib.request.urlopen(req)
        content = resp.read().decode()
        return json.loads(content) if content.strip() else None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        logger.error(f'  HTTP {e.code}: {body[:300]}')
        raise


_TABLE_COLUMNS_CACHE = {}


def get_table_columns(table):
    """Return the set of column names the Supabase table exposes."""
    if table not in _TABLE_COLUMNS_CACHE:
        sample = supabase_request('GET', f'{table}?limit=1')
        if not sample:
            _TABLE_COLUMNS_CACHE[table] = set()
        else:
            _TABLE_COLUMNS_CACHE[table] = set(sample[0].keys())
    return _TABLE_COLUMNS_CACHE[table]


def fetch_supabase_schools(table, city, fields):
    """Fetch schulnummer + given fields for a city from Supabase.

    Skips fields that don't exist in the target table (returned in
    extra result so callers know what was skipped).
    """
    table_cols = get_table_columns(table)
    safe_fields = [f for f in fields if f in table_cols]
    skipped_missing_in_table = [f for f in fields if f not in table_cols]

    select = ','.join(['id', 'schulnummer', 'city'] + safe_fields)
    all_data = []
    offset = 0
    while True:
        path = f'{table}?select={select}&city=eq.{city}&offset={offset}&limit=1000'
        data = supabase_request('GET', path)
        all_data.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    return all_data, safe_fields, skipped_missing_in_table


# ============================================================================
# Local data
# ============================================================================

def load_local_df(filepath):
    """Load local CSV/parquet and make Supabase-target columns reachable via aliases."""
    if filepath.suffix == '.parquet':
        df = pd.read_parquet(filepath)
    else:
        df = pd.read_csv(filepath, low_memory=False)

    # Mirror alias columns so `df[supabase_col]` works
    for local_col, supabase_col in COL_ALIASES.items():
        if local_col in df.columns and supabase_col not in df.columns:
            df[supabase_col] = df[local_col]

    if 'schulnummer' in df.columns:
        df['schulnummer'] = df['schulnummer'].astype(str)
    return df


def _is_empty(val):
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() in ('', 'None', 'nan', 'NaN'):
        return True
    return False


def _coerce(field, value):
    """Coerce local value to the type Supabase expects. Returns None if unusable."""
    if _is_empty(value):
        return None
    if field in INTEGER_FIELDS:
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    if field in FLOAT_FIELDS:
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    s = str(value).strip()
    if not s:
        return None
    # Auto-detect numeric string formatting quirks for untyped fields
    if re.match(r'^-?\d+\.0$', s):
        try:
            return int(float(s))
        except (ValueError, TypeError):
            return s
    return s


# ============================================================================
# Update flow
# ============================================================================

def update_city(table, city, local_df, fields, dry_run=False):
    """Fill NULL Supabase values from local data. Returns summary dict."""
    sb_rows, active_fields, skipped_in_table = fetch_supabase_schools(table, city, fields)
    summary = {
        'city': city, 'table': table,
        'sb_rows': len(sb_rows), 'local_rows': len(local_df),
        'matched': 0, 'planned_updates': 0,
        'per_field': {f: 0 for f in active_fields},
        'skipped_no_local_col': [],
        'skipped_not_in_table': skipped_in_table,
    }

    if not sb_rows:
        logger.warning(f'  No Supabase rows for {city} in {table}')
        return summary

    sb_by_snr = {str(r['schulnummer']): r for r in sb_rows if r.get('schulnummer')}
    logger.info(f'  Supabase: {len(sb_by_snr)} rows, Local: {len(local_df)} rows')

    local_cols_present = [f for f in active_fields if f in local_df.columns]
    summary['skipped_no_local_col'] = [f for f in active_fields if f not in local_df.columns]

    for _, local_row in local_df.iterrows():
        snr = str(local_row.get('schulnummer', ''))
        if snr not in sb_by_snr:
            continue
        summary['matched'] += 1
        sb_row = sb_by_snr[snr]

        payload = {}
        for field in local_cols_present:
            if not _is_empty(sb_row.get(field)):
                continue  # Supabase already has data — skip (fill gaps only)
            coerced = _coerce(field, local_row.get(field))
            if coerced is None:
                continue
            payload[field] = coerced
            summary['per_field'][field] += 1

        if not payload:
            continue
        summary['planned_updates'] += 1

        if dry_run:
            continue

        sb_id = sb_row['id']
        # Per-field PATCH with `<field>=is.null` filter so the server-side
        # guard matches the Python-side check. If something modifies the
        # cell between our SELECT and PATCH, the row count will be 0 and
        # no overwrite happens. Belt-and-suspenders for service-role writes.
        for field, new_val in payload.items():
            try:
                supabase_request(
                    'PATCH',
                    f'{table}?id=eq.{sb_id}&{field}=is.null',
                    {field: new_val},
                    for_writes=True,
                )
            except Exception as e:
                logger.error(f'  Failed update {snr}.{field}: {e}')

    return summary


def print_summary(summary):
    """Print a readable per-field fill count for a (city, table) pair."""
    city, table = summary['city'], summary['table']
    print(f"\n  {city} / {table} — matched {summary['matched']}/{summary['sb_rows']} "
          f"({summary['planned_updates']} rows need ≥1 fill)")
    filled = {f: n for f, n in summary['per_field'].items() if n > 0}
    if filled:
        for f, n in sorted(filled.items(), key=lambda x: -x[1]):
            print(f"    fill {f}: +{n}")
    else:
        print(f"    (nothing to fill — all selected fields already populated)")
    if summary['skipped_no_local_col']:
        print(f"    skipped (no local column): "
              f"{', '.join(summary['skipped_no_local_col'])}")
    if summary.get('skipped_not_in_table'):
        print(f"    skipped (not in Supabase table): "
              f"{', '.join(summary['skipped_not_in_table'])}")


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--dry-run', action='store_true',
                   help='Plan changes only; do not write to Supabase')
    p.add_argument('--groups', default=','.join(DEFAULT_GROUPS),
                   help=f'Comma-separated field groups. '
                        f'Available: {", ".join(FIELD_GROUPS)}. '
                        f'Default: {",".join(DEFAULT_GROUPS)}')
    p.add_argument('--city', default=None,
                   help='Limit to a single city (e.g. dresden, leipzig)')
    p.add_argument('--list-fields', action='store_true',
                   help='Print the selected field set and exit')
    return p.parse_args()


def main():
    args = parse_args()

    groups = [g.strip() for g in args.groups.split(',') if g.strip()]
    unknown = [g for g in groups if g not in FIELD_GROUPS]
    if unknown:
        logger.error(f"Unknown groups: {unknown}. Available: {list(FIELD_GROUPS)}")
        sys.exit(1)

    fields = []
    for g in groups:
        for f in FIELD_GROUPS[g]:
            if f not in fields:
                fields.append(f)

    print("=" * 72)
    print("SUPABASE FILL-GAPS UPLOAD")
    print("=" * 72)
    print(f"Mode:    {'DRY RUN (no writes)' if args.dry_run else 'LIVE WRITE'}")
    print(f"Groups:  {', '.join(groups)}")
    print(f"Fields:  {', '.join(fields)}")
    if args.city:
        print(f"City:    {args.city}")
    print()

    if args.list_fields:
        return

    # Fail fast: live writes require service role key (RLS blocks anon UPDATE)
    if not args.dry_run:
        _resolve_key(for_writes=True)

    os.chdir(PROJECT_ROOT)

    summaries = []
    for city, sec_file, pri_file in CITY_FILES:
        if args.city and city != args.city:
            continue
        for table, rel_path in (('schools', sec_file), ('primary_schools', pri_file)):
            filepath = PROJECT_ROOT / rel_path
            if not filepath.exists():
                logger.warning(f'  Skipping {city} / {table}: {rel_path} not found')
                continue
            logger.info(f'\nProcessing {city} / {table} ({rel_path})')
            local_df = load_local_df(filepath)
            s = update_city(table, city, local_df, fields, dry_run=args.dry_run)
            summaries.append(s)
            print_summary(s)

    # Aggregate
    print("\n" + "=" * 72)
    print("TOTALS")
    print("=" * 72)
    agg = {f: 0 for f in fields}
    total_planned = 0
    for s in summaries:
        total_planned += s['planned_updates']
        for f, n in s['per_field'].items():
            agg[f] = agg.get(f, 0) + n
    print(f"  Rows with ≥1 planned fill: {total_planned}")
    for f in fields:
        if agg.get(f, 0) > 0:
            print(f"    {f}: +{agg[f]}")
    print()
    if args.dry_run:
        print("  DRY RUN — nothing was written. Re-run without --dry-run to apply.")


if __name__ == '__main__':
    main()
