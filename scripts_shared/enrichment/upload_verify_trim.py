#!/usr/bin/env python3
"""
Upload a verify_trim_descriptions.py run (policy B) to Supabase.

Two kinds of rows, both sent through PostgREST with the anon key while a
temporary INSERT policy accepts only a one-time token (see
upload_description_scrub.py for the pattern):

  descriptions   → _desc_scrub_staging (id, tbl, field, old_md5, new_text, batch).
                   Promotion happens afterwards in SQL: back up the live rows, then
                   UPDATE only where md5(current text) = old_md5.
  verifications  → description_verifications: per school the claims, verdicts,
                   evidence URLs/quotes and what was deleted, so every edit can be
                   traced to a source.

Held back (not uploaded as text edits, listed for review):
  - a field whose trim failed validation,
  - a school whose trim removes more than --max-shrink of either text. In the full
    run such cases were thin placeholder texts ("details are not provided") or the
    trim over-deleting in one language (English -82%, German -5%); they need a
    rewrite, not a trim.

Usage:
    venv/bin/python scripts_shared/enrichment/upload_verify_trim.py --results <dir> \
        --token-file <path> --what descriptions|verifications [--dry-run]
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

POLICY = 'B'


def load(results_dir: Path, model: str):
    runs = {}
    for line in (results_dir / f'results_{model}.jsonl').read_text(encoding='utf-8').splitlines():
        r = json.loads(line)
        if r.get('status') == 'ok':
            runs[r['id']] = r  # the last ok line per school wins
    return runs


def description_rows(runs, token, max_shrink):
    rows, held = [], []
    for r in runs.values():
        trim = r[f'trim_{POLICY}']
        old = r['old']
        changed = {f: t for f, t in trim['new'].items() if f in old and t != old[f]}
        if not changed:
            continue
        problems = {f: p for f, p in trim['problems'].items() if f != '_call'}
        if problems:  # keep both languages in step: no half-trimmed schools
            held += [(r['id'], r['schulname'], f'{f}: {p}') for f, p in problems.items()]
            continue
        shrink = {f: 1 - len(changed.get(f, old[f])) / max(1, len(old[f])) for f in old}
        if max(shrink.values()) > max_shrink:
            held.append((r['id'], r['schulname'], ', '.join(f"{f[12:]} -{s:.0%}" for f, s in shrink.items())))
            continue
        for f, t in changed.items():
            rows.append({'id': r['id'], 'tbl': r['tbl'], 'field': f,
                         'old_md5': hashlib.md5(old[f].encode('utf-8')).hexdigest(), 'new_text': t, 'batch': token})
    return rows, held


def verification_rows(runs, token, model):
    rows = []
    for r in runs.values():
        v = r['verify']
        rows.append({'school_id': r['id'], 'tbl': r['tbl'], 'model': model, 'policy': POLICY,
                     'claims': [{k: c.get(k) for k in ('claim', 'category', 'kind', 'verdict', 'evidence_url',
                                                       'evidence', 'evidence_found', 'source_says')}
                                for c in v['claims']],
                     'deleted': r[f'trim_{POLICY}']['deleted'],
                     'search_queries': v.get('queries') or [],
                     'site_pages': v.get('site_pages'), 'batch': token})
    return rows


def post(table, rows, batch_size):
    headers = dict(_headers(False))
    headers.update({'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
    for i in range(0, len(rows), batch_size):
        resp = requests.post(f"{SUPABASE_URL}/{table}", headers=headers, json=rows[i:i + batch_size], timeout=180)
        if resp.status_code >= 300:
            sys.exit(f"{table} batch at {i}: HTTP {resp.status_code} {resp.text[:300]}")
    print(f"uploaded {len(rows)} rows to {table}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', type=Path, required=True)
    ap.add_argument('--model', default='pro')
    ap.add_argument('--token-file', type=Path, required=True)
    ap.add_argument('--what', choices=('descriptions', 'verifications'), required=True)
    ap.add_argument('--max-shrink', type=float, default=0.25)
    ap.add_argument('--batch-size', type=int, default=100)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    token = args.token_file.read_text().strip()
    runs = load(args.results, args.model)
    model = next(iter(runs.values()))['model']
    # A run can be uploaded in parts (e.g. across a quota reset): skip schools already sent
    sent_file = args.results / f'uploaded_{args.what}.txt'
    sent = set(sent_file.read_text().split()) if sent_file.exists() else set()
    runs = {k: v for k, v in runs.items() if k not in sent}
    if args.what == 'descriptions':
        rows, held = description_rows(runs, token, args.max_shrink)
        print(f"{len(runs)} schools checked; {len(rows)} field edits for {len({r['id'] for r in rows})} schools; "
              f"{len(held)} held back")
        (args.results / 'held_back.json').write_text(json.dumps(held, ensure_ascii=False, indent=1), encoding='utf-8')
        table = '_desc_scrub_staging'
    else:
        rows = verification_rows(runs, token, model)
        print(f"{len(rows)} verification records")
        table = 'description_verifications'
    if not args.dry_run:
        post(table, rows, args.batch_size)
        with open(sent_file, 'a') as fh:
            fh.write(''.join(f'{k}\n' for k in runs))


if __name__ == '__main__':
    main()
