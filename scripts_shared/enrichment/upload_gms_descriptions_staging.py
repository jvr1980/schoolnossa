#!/usr/bin/env python3
"""
Stage the description/embedding results of run_admin_jobs_from_snapshot.py in
public._gms_desc_staging (row_id, token, payload) through the temporary,
token-guarded, insert-only anon policy. Promotion and checks are SQL.

Usage:
    venv/bin/python scripts_shared/enrichment/upload_gms_descriptions_staging.py \
        --rows data_shared/cache/gms_admin_jobs/staging_rows.json [--dry-run]
"""
import argparse
import json
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from scripts_shared.upload_to_supabase import SUPABASE_URL, _headers  # noqa: E402

KEYS = ('description', 'description_de', 'description_en', 'description_researched_at',
        'description_grounding', 'embedding')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rows', type=Path, required=True)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    src = json.loads(args.rows.read_text(encoding='utf-8'))
    rows = [{'row_id': r['id'], 'token': r['token'], 'payload': {k: r['payload'][k] for k in KEYS}} for r in src]
    print(f"{len(rows)} rows, {len(json.dumps(rows)) / 1e6:.2f} MB")
    if args.dry_run:
        return
    headers = dict(_headers(False))
    headers.update({'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
    for i in range(0, len(rows), 10):
        resp = requests.post(f"{SUPABASE_URL}/_gms_desc_staging", headers=headers, json=rows[i:i + 10], timeout=180)
        if resp.status_code >= 300:
            sys.exit(f"batch at {i}: HTTP {resp.status_code} {resp.text[:300]}")
    print(f"uploaded {len(rows)} rows to _gms_desc_staging")


if __name__ == '__main__':
    main()
