#!/usr/bin/env python3
"""
Upload refresh_admission_open_days.py results to Supabase (schools and primary_schools).

Rows go to the staging table _admission_staging through a one-time-token anon INSERT
policy (same pattern as upload_verify_trim.py); the promotion is SQL:

  - open_days(_en): replaced by the newly found upcoming events (an empty list is
    written too — the April dates have all passed);
  - criteria, application window, notes: replaced when the new run found them,
    otherwise the existing value stays (COALESCE);
  - last_open_day_seen: the later of the existing and the new value;
  - admission_fetched_at: now(), for every school whose site was read.
Schools whose site could not be read (or have no website) are not staged.

Usage:
    venv/bin/python scripts_shared/enrichment/upload_admission_refresh.py --results <dir> --token-file <path> [--dry-run]
"""
import argparse
import json
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from scripts_shared.upload_to_supabase import SUPABASE_URL, _headers  # noqa: E402


def events(evs, lang):
    out = []
    for e in evs:
        out.append({'date': e['date'], 'start_time': e.get('start_time'), 'end_time': e.get('end_time'),
                    'event_type': e.get(f'event_type_{lang}') or e.get('event_type_de'),
                    'audience': e.get(f'audience_{lang}') or e.get('audience_de'),
                    'notes': e.get(f'notes_{lang}') or e.get('notes_de')})
    return out


def staged(r, token):
    w = r.get('window')
    crit = r.get('criteria') or []
    return {'id': r['id'], 'tbl': r['tbl'], 'batch': token,
            'criteria_de': [c['de'] for c in crit] or None,
            'criteria_en': [c.get('en') or c['de'] for c in crit] or None,
            'window_de': {'opens': w.get('opens'), 'closes': w.get('closes'), 'notes': w.get('notes_de')} if w else None,
            'window_en': {'opens': w.get('opens'), 'closes': w.get('closes'), 'notes': w.get('notes_en') or w.get('notes_de')} if w else None,
            'notes_de': r.get('notes_de'), 'notes_en': r.get('notes_en'),
            'open_days_de': events(r.get('open_days') or [], 'de'),
            'open_days_en': events(r.get('open_days') or [], 'en'),
            'last_seen': r.get('last_open_day_seen')}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', type=Path, required=True)
    ap.add_argument('--token-file', type=Path, required=True)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    token = args.token_file.read_text().strip()
    latest = {}
    for line in (args.results / 'admission_results.jsonl').read_text().splitlines():
        r = json.loads(line)
        latest[r['id']] = r
    rows = [staged(r, token) for r in latest.values() if r['status'] in ('ok', 'nothing on the site')]
    stats = {}
    for r in latest.values():
        s = r['status'].split(':')[0]
        stats[s] = stats.get(s, 0) + 1
    print(f"{len(latest)} schools: {stats}; staging {len(rows)} "
          f"({sum(bool(r['open_days_de']) for r in rows)} with upcoming open days, "
          f"{sum(bool(r['window_de']) for r in rows)} with a window, {sum(bool(r['criteria_de']) for r in rows)} with criteria)")
    if args.dry_run:
        return
    headers = dict(_headers(False))
    headers.update({'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
    for i in range(0, len(rows), 200):
        resp = requests.post(f"{SUPABASE_URL}/_admission_staging", headers=headers, json=rows[i:i + 200], timeout=180)
        if resp.status_code >= 300:
            sys.exit(f"batch at {i}: HTTP {resp.status_code} {resp.text[:300]}")
    print(f"uploaded {len(rows)} rows to _admission_staging")


if __name__ == '__main__':
    main()
