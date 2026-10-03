#!/usr/bin/env python3
"""
Find the official website for German schools whose `website` is empty, so that the
admission and description pipelines can read the school's own pages.

Same method as the Stuttgart repair in repair_register_fields.py: Gemini 3 Flash with
Google Search proposes a URL, and it is accepted only if the page loads and names the
school (a distinctive word of its name) or its street. Otherwise the field stays empty.
Answers are cached (data_shared/cache/website_search/); failed calls are not cached.

Output in data_shared/website_fill_<date>/: deltas.csv, apply.sql (guarded: only where
website IS NULL or ''), rollback.sql.

Usage:
    venv/bin/python scripts_shared/processing/fill_missing_websites.py [--workers 6]
"""
import argparse
import concurrent.futures as cf
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts_shared' / 'enrichment'))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import replicate_lovable_description_jobs as job  # noqa: E402
from repair_register_fields import _site_ok, sql_literal  # noqa: E402

OUT_DIR = PROJECT_ROOT / 'data_shared' / f"website_fill_{datetime.now():%Y-%m-%d}"
CACHE = PROJECT_ROOT / 'data_shared' / 'cache' / 'website_search' / 'answers.json'
CITY = {'berlin': 'Berlin', 'hamburg': 'Hamburg', 'muenchen': 'München', 'frankfurt': 'Frankfurt am Main',
        'koeln': 'Köln', 'duesseldorf': 'Düsseldorf', 'stuttgart': 'Stuttgart', 'dresden': 'Dresden',
        'leipzig': 'Leipzig', 'bremen': 'Bremen'}


def ask(row, cache):
    if row['id'] in cache:
        return cache[row['id']]
    prompt = (f"What is the official website of the school \"{row['schulname']}\", {row.get('strasse') or ''}, "
              f"{row.get('plz') or ''} {CITY[row['city']]}, Germany? Use Google Search. Answer with only the URL of "
              f"the school's own website, or NONE if it has no website of its own (a city directory, school finder, "
              f"the operator's general site or a parent school does not count).")
    data = job.gemini('gemini-3-flash-preview', {'contents': [{'parts': [{'text': prompt}]}],
                                                 'tools': [{'googleSearch': {}}]}, timeout=120)
    return job.gen_text(data).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=6)
    args = ap.parse_args()
    requests.packages.urllib3.disable_warnings()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    rows = [dict(r, tbl=tbl) for tbl in ('schools', 'primary_schools') for city in CITY
            for r in job.fetch(tbl, f'city=eq.{city}&or=(website.is.null,website.eq.)',
                               'id,city,schulnummer,schulname,strasse,plz,website')]
    print(f"{len(rows)} schools without a website", flush=True)

    def one(row):
        try:
            answer = ask(row, cache)
        except Exception as e:  # noqa: BLE001  (quota/timeouts are not cached)
            return row, None, f'search failed: {str(e)[:60]}'
        cache[row['id']] = answer
        m = re.search(r'https?://[^\s)\]>"]+|www\.[^\s)\]>"]+', answer)
        if not m:
            return row, None, 'no own website found'
        site = _site_ok(m.group(0).rstrip('.,'), row)
        return row, site, 'Google Search, page names the school' if site else f'rejected: {m.group(0)[:60]}'

    with cf.ThreadPoolExecutor(args.workers) as ex:
        results = list(ex.map(one, rows))
    CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
    records, apply, rollback = [], [], []
    for row, site, reason in results:
        records.append({'tbl': row['tbl'], 'id': row['id'], 'city': row['city'], 'schulname': row['schulname'],
                        'website': site, 'reason': reason})
        if site:
            apply.append(f"UPDATE public.{row['tbl']} SET website = {sql_literal(site)} WHERE id = '{row['id']}' "
                         f"AND (website IS NULL OR website = '');")
            rollback.append(f"UPDATE public.{row['tbl']} SET website = NULL WHERE id = '{row['id']}' "
                            f"AND website = {sql_literal(site)};")
    pd.DataFrame(records).to_csv(OUT_DIR / 'deltas.csv', index=False, encoding='utf-8-sig')
    (OUT_DIR / 'apply.sql').write_text('\n'.join(apply) + '\n')
    (OUT_DIR / 'rollback.sql').write_text('\n'.join(rollback) + '\n')
    print(pd.Series([r[2].split(':')[0] for r in results]).value_counts().to_dict(), flush=True)
    print(f"{len(apply)} websites found → {OUT_DIR.relative_to(PROJECT_ROOT)}/apply.sql", flush=True)


if __name__ == '__main__':
    main()
