#!/usr/bin/env python3
"""
Upload validated description edits (scrub_description_claims.py output) to the
Supabase staging table `_desc_scrub_staging` via PostgREST.

There is no service-role key and the MCP SQL tool cannot carry megabytes of text,
so rows go through a staging table whose temporary anon INSERT policy only
accepts a one-time `batch` token. Promotion happens afterwards in SQL: back up
the live rows, then UPDATE only where md5(current text) = old_md5, so any text
that changed since it was read is left alone. Then drop the policy and the table.

Usage:
    venv/bin/python scripts_shared/enrichment/upload_description_scrub.py \
        --results data_shared/description_scrub_2026-09-27 --token-file <path> [--dry-run]
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from scripts_shared.upload_to_supabase import SUPABASE_URL, _headers  # noqa: E402


def staged_rows(results_dir: Path, token: str):
    rows, seen = [], set()
    # corrections first: they win if a school appears in both files
    for name in ('results_corrections.jsonl', 'results.jsonl'):
        path = results_dir / name
        if not path.exists():
            continue
        for line in path.read_text(encoding='utf-8').splitlines():
            r = json.loads(line)
            for field, new in (r.get('new') or {}).items():
                key = (r['id'], field)
                if key in seen:
                    continue
                seen.add(key)
                old = r['old'][field]
                rows.append({'id': r['id'], 'tbl': r['tbl'], 'field': field,
                             'old_md5': hashlib.md5(old.encode('utf-8')).hexdigest(),
                             'new_text': new, 'batch': token})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', type=Path, required=True)
    ap.add_argument('--token-file', type=Path, required=True)
    ap.add_argument('--batch-size', type=int, default=100)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    token = args.token_file.read_text().strip()
    rows = staged_rows(args.results, token)
    by_field = {}
    for r in rows:
        by_field[(r['tbl'], r['field'])] = by_field.get((r['tbl'], r['field']), 0) + 1
    print(f"{len(rows)} field edits for {len({r['id'] for r in rows})} schools: {by_field}")
    if args.dry_run:
        return
    headers = dict(_headers(False))
    headers.update({'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
    sent = 0
    for i in range(0, len(rows), args.batch_size):
        batch = rows[i:i + args.batch_size]
        resp = requests.post(f"{SUPABASE_URL}/_desc_scrub_staging", headers=headers, json=batch, timeout=120)
        if resp.status_code >= 300:
            sys.exit(f"batch at {i}: HTTP {resp.status_code} {resp.text[:300]}")
        sent += len(batch)
    print(f"uploaded {sent} rows")


if __name__ == '__main__':
    main()
