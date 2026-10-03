#!/usr/bin/env python3
"""
Rewrite the descriptions that verify-and-trim could not fix by deletion
(held back because a trim would remove >25% of a text, or no clean trim was
found even with Gemini Pro). See docs/DEVJOURNAL.md 2026-09-28.

Per school:
  1. research   — new raw description with the strict, sources-required prompt
                  (replicate_lovable_description_jobs.research: Gemini 3.1 Pro + Google Search)
  2. polish     — German and English texts (generate_de_en)
  3. verify     — the same claim check as the full run (verify_trim_descriptions.verify)
  4. trim       — policy B deletions, deletion-only validator and grammar check
  5. accept     — only if every check passes and neither text loses >25%; otherwise
                  the old text stays (status says why)
  6. embed      — 768-dim embedding of the new raw description (as the app's job does)

Output: <out>/rewrite_results.jsonl. Upload with --upload: rows go to the staging
table _desc_rewrite_staging through a one-time-token anon INSERT policy; promotion
(backup + md5-guarded UPDATE) and the verification records are done in SQL.

Usage:
    venv/bin/python scripts_shared/enrichment/rewrite_held_descriptions.py --ids ids.txt --out <dir> [--workers 8]
    venv/bin/python scripts_shared/enrichment/rewrite_held_descriptions.py --out <dir> --upload --token-file <path>
"""
import argparse
import concurrent.futures as cf
import hashlib
import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import replicate_lovable_description_jobs as job  # noqa: E402
import verify_trim_descriptions as vt  # noqa: E402
from scrub_description_claims import COUNT_DE, COUNT_EN  # noqa: E402
from scripts_shared.upload_to_supabase import SUPABASE_URL, _headers  # noqa: E402

PRO = vt.MODELS['pro']
MAX_SHRINK = 0.25
COLS = job.COLS + ',strasse,plz'


def load_rows(ids):
    rows = []
    for tbl in ('schools', 'primary_schools'):
        for i in range(0, len(ids), 80):
            chunk = ','.join(ids[i:i + 80])
            rows += [dict(r, tbl=tbl) for r in job.fetch(tbl, f'id=in.({chunk})', COLS)]
    return rows


def rewrite(s):
    out = {'id': s['id'], 'tbl': s['tbl'], 'city': s['city'], 'schulname': s['schulname'],
           'old': {f: s.get(f) for f in vt.FIELDS}}
    raw, grounding = job.research(s)
    if not raw:
        return {**out, 'status': 'research failed (no sourced text)'}
    de_en = job.generate_de_en(dict(s, description=raw), s['tbl'] == 'schools')
    if not de_en:
        return {**out, 'status': 'polish failed'}
    row = dict(s, **de_en)
    v = vt.verify(row, PRO)
    if 'error' in v:
        return {**out, 'status': f"verify failed: {v['error'][:80]}"}
    texts = {f: de_en[f] for f in vt.FIELDS}
    dels = vt.to_delete(v['claims'], 'B', vt.site_hosts_of(row))
    t = vt.trim(texts, dels)
    out.update({'raw': raw, 'grounding': grounding, 'generated': texts, 'verify': v,
                'deleted': [c.get('claim') for c in dels], 'trim': t})
    problems = {k: p for k, p in t['problems'].items() if k != '_call'}
    if problems:
        return {**out, 'status': f"new text failed the trim checks: {next(iter(problems.values()))[:80]}"}
    shrink = max(1 - len(t['new'][f]) / max(1, len(texts[f])) for f in texts)
    if shrink > MAX_SHRINK:
        return {**out, 'status': f'new text would also lose {shrink:.0%}'}
    if COUNT_DE.search(t['new']['description_de']) or COUNT_EN.search(t['new']['description_en']):
        return {**out, 'status': 'new text states student/teacher/class counts'}
    vec = job.embed(raw)
    if not vec:
        return {**out, 'status': 'embedding failed'}
    return {**out, 'new': t['new'], 'embedding': [round(x, 6) for x in vec], 'status': 'ok'}


def upload(out_dir, token):
    res = [json.loads(l) for l in (out_dir / 'rewrite_results.jsonl').read_text().splitlines()]
    rows = [{'id': r['id'], 'tbl': r['tbl'],
             'old_md5_de': hashlib.md5(r['old']['description_de'].encode()).hexdigest(),
             'old_md5_en': hashlib.md5(r['old']['description_en'].encode()).hexdigest(),
             'description': r['raw'], 'description_grounding': json.dumps(r['grounding'], ensure_ascii=False),
             'description_de': r['new']['description_de'], 'description_en': r['new']['description_en'],
             'embedding': '[' + ','.join(repr(x) for x in r['embedding']) + ']',
             'claims': [{k: c.get(k) for k in ('claim', 'category', 'kind', 'verdict', 'evidence_url', 'evidence',
                                               'evidence_found', 'source_says')} for c in r['verify']['claims']],
             'deleted': r['deleted'], 'search_queries': r['verify'].get('queries') or [],
             'site_pages': r['verify'].get('site_pages'), 'batch': token}
            for r in res if r['status'] == 'ok']
    headers = dict(_headers(False))
    headers.update({'Content-Type': 'application/json', 'Prefer': 'return=minimal'})
    for i in range(0, len(rows), 25):
        resp = requests.post(f"{SUPABASE_URL}/_desc_rewrite_staging", headers=headers, json=rows[i:i + 25], timeout=180)
        if resp.status_code >= 300:
            sys.exit(f"batch at {i}: HTTP {resp.status_code} {resp.text[:300]}")
    print(f"uploaded {len(rows)} rewrites to _desc_rewrite_staging")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--ids', type=Path)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--upload', action='store_true')
    ap.add_argument('--token-file', type=Path)
    args = ap.parse_args()
    if args.upload:
        return upload(args.out, args.token_file.read_text().strip())
    out_path = args.out / 'rewrite_results.jsonl'
    done = {json.loads(l)['id'] for l in out_path.read_text().splitlines()} if out_path.exists() else set()
    ids = [i for i in args.ids.read_text().split() if i not in done]
    rows = load_rows(ids)
    print(f"{len(rows)} schools to rewrite", flush=True)

    def safe(s):
        try:
            return rewrite(s)
        except Exception as e:  # noqa: BLE001
            return {'id': s['id'], 'tbl': s['tbl'], 'schulname': s['schulname'], 'status': f'error: {e}'[:120]}

    with open(out_path, 'a', encoding='utf-8') as fh, cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, fut in enumerate(cf.as_completed([ex.submit(safe, s) for s in rows]), 1):
            fh.write(json.dumps(fut.result(), ensure_ascii=False) + '\n'); fh.flush()
            if i % 10 == 0:
                print(f"  {i}/{len(rows)}", flush=True)
    stats = {}
    for l in out_path.read_text().splitlines():
        st = json.loads(l)['status'].split(':')[0]
        stats[st] = stats.get(st, 0) + 1
    print(f"done: {stats}", flush=True)


if __name__ == '__main__':
    main()
