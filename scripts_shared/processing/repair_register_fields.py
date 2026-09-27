#!/usr/bin/env python3
"""
Repair register fields that the September 2026 description audits found wrong
(docs/audits/DESCRIPTION_VERIFY_TRIM_TEST_2026-09.md). Deltas only: each change
is (table, id, column, old → new, reason); nothing else is touched.

- Stuttgart website: 48 schools carried https://www.stuttgart-inklusiv.de/ (the
  city's accessibility guide, linked on every stuttgart.de page). New value: the
  official LOBW Schulverzeichnis URL, else a Google-searched URL that must load
  and name the school or its street, else empty.
- Stuttgart traegerschaft: from LOBW instead of a name heuristic. The named
  operator decides first (Schulverwaltungsamt / Stadt / Land → Öffentlich; gGmbH,
  e.V., foundation, church → Privat), because the type 'Baden-Württemberg' also
  covers private schools (element-i: Konzept-e für Schulen gGmbH). Without a named
  operator the type decides ('Gemeinde' → Öffentlich; church, association,
  foundation, other legal person, unassigned → Privat). Primary rows split off a
  combined school ('-GS') take their parent's value (was hard-coded Öffentlich).
- Bremen traegerschaft: from the official Schulform ("Private Grundschule", ...)
  instead of name keywords ('frei' matched "Freiligrathstraße").
- München website: two rows held the placeholder https://test-canary.example/ in
  Supabase only; restored from the final tables.

Output in data_shared/register_repair_<date>/: deltas.csv, apply.sql (guarded:
UPDATE ... WHERE id = … AND col IS NOT DISTINCT FROM old), rollback.sql.
--patch-finals applies the same deltas to the city final tables.

Usage:
    venv/bin/python scripts_shared/processing/repair_register_fields.py [--patch-finals]
"""
import argparse
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

from scripts_shared.upload_to_supabase import SUPABASE_URL, _headers  # noqa: E402

OUT_DIR = PROJECT_ROOT / 'data_shared' / f"register_repair_{datetime.now():%Y-%m-%d}"
SEARCH_CACHE = PROJECT_ROOT / 'data_stuttgart' / 'cache' / 'website_search_2026-09.json'
BAD_STUTTGART_SITE = 'stuttgart-inklusiv.de'
PLACEHOLDER = 'test-canary.example'
USER_AGENT = 'Mozilla/5.0 (compatible; SchoolNossa/1.0; +https://schoolnossa.de)'
PUBLIC_TRAEGER = {'Gemeinde', 'Baden-Württemberg'}
PRIVATE_TRAEGER = {'Verband/Verein', 'Sonstige juristische Person', 'Kirche/Glaubensgem.', 'Stiftung', 'Ohne Zuordnung'}
PUBLIC_OPERATOR = re.compile(r'schulverwaltungsamt|landeshauptstadt|\bstadt\b|land baden-württemberg', re.I)
PRIVATE_OPERATOR = re.compile(r'gmbh|e\.\s?v\.|stiftung|kirche|diözese|verein|gemeinnützig|<verband|<sonstige', re.I)


def lobw_traeger(match):
    """(Öffentlich|Privat, reason) from a LOBW entry, or (None, None) if it does not say."""
    wl = (match.get('WL_KURZ_BEZEICHNUNG') or '').strip()
    op = (match.get('STR_KURZ_BEZEICHNUNG') or '').strip()
    if PUBLIC_OPERATOR.search(op):
        return 'Öffentlich', f"LOBW operator '{op}'"
    if PRIVATE_OPERATOR.search(op):
        return 'Privat', f"LOBW operator '{op}'"
    if wl in PUBLIC_TRAEGER:
        return 'Öffentlich', f"LOBW Schulträger type '{wl}'"
    if wl in PRIVATE_TRAEGER:
        return 'Privat', f"LOBW Schulträger type '{wl}'"
    return None, None


GENERIC = {'schule', 'grundschule', 'gymnasium', 'realschule', 'werkrealschule', 'gemeinschaftsschule', 'stuttgart',
           'aussenstelle', 'außenstelle', 'und', 'der', 'die', 'das', 'mit', 'grund', 'standort', 'evang', 'evangelische',
           'freie', 'private', 'priv', 'st', 'schulen', 'gwrs'}

FINALS = {
    'stuttgart': {pt: [f'data_stuttgart/final/stuttgart_{pt}_school_master_table{s}'
                       for s in ('.csv', '.parquet', '_final.csv', '_final_with_embeddings.parquet',
                                 '_berlin_schema.csv', '_berlin_schema.parquet')]
                  for pt in ('primary', 'secondary')},
    'bremen': {'all': [f'data_bremen/final/bremen_{p}school_master_table{s}'
                       for p in ('', 'primary_', 'secondary_')
                       for s in ('_final.csv', '_final.parquet', '_final_with_embeddings.parquet',
                                 '_berlin_schema.csv', '_berlin_schema.parquet')]},
}


def db_rows(city):
    rows = []
    for tbl in ('schools', 'primary_schools'):
        r = requests.get(f"{SUPABASE_URL}/{tbl}", headers=_headers(False), timeout=60, params={
            'select': 'id,schulnummer,schulname,strasse,plz,website,traegerschaft', 'city': f'eq.{city}'})
        r.raise_for_status()
        rows += [dict(x, tbl=tbl) for x in r.json()]
    return rows


def _street(s):
    s = (s or '').lower().replace('straße', 'str').replace('strasse', 'str').replace('str.', 'str')
    return re.sub(r'[^a-z0-9äöü]', '', s)


def _tokens(name):
    return {t for t in re.findall(r'[a-zäöüß]+', (name or '').lower().replace('-', ' ')) if len(t) > 2} - GENERIC


def lobw_match(row, lobw):
    """LOBW entry at the same address whose name shares a distinctive word with ours (None if unsure)."""
    same_address = [l for l in lobw if _street(l['DISTR']) == _street(row['strasse'])
                    and (l.get('PLZSTR') or '').strip() == str(row['plz'])]
    named = [l for l in same_address if _tokens(row['schulname']) & _tokens(l['NAME'])]
    return named[0] if len(named) == 1 else None


def _site_ok(url, row):
    """The URL loads and its page names the school (a distinctive word) or its street."""
    try:
        r = requests.get(url if url.startswith('http') else f'http://{url}', timeout=15,
                         headers={'User-Agent': USER_AGENT}, verify=False)
        if not r.ok:
            return None
        text = r.text.lower()
        street = re.sub(r'\s*\d.*$', '', (row['strasse'] or '').lower()).replace('straße', 'str')
        if any(t in text for t in _tokens(row['schulname'])) or (street and street[:8] in text.replace('straße', 'str')):
            return r.url
    except requests.RequestException:
        pass
    return None


def search_site(row):
    """Official website via Gemini + Google Search (cached), accepted only if it passes _site_ok."""
    import replicate_lovable_description_jobs as job
    cache = json.loads(SEARCH_CACHE.read_text()) if SEARCH_CACHE.exists() else {}
    if row['id'] not in cache:
        prompt = (f"What is the official website of the school \"{row['schulname']}\", {row['strasse']}, "
                  f"{row['plz']} Stuttgart, Germany? Use Google Search. Answer with only the URL of the school's own "
                  f"website, or NONE if the school has no website of its own (a city directory, a parent school or "
                  f"a school finder does not count).")
        try:
            data = job.gemini('gemini-3-flash-preview', {'contents': [{'parts': [{'text': prompt}]}],
                                                         'tools': [{'googleSearch': {}}]}, timeout=120)
            answer = job.gen_text(data)
        except Exception as e:  # noqa: BLE001
            answer = f'ERROR {e}'
        cache[row['id']] = answer.strip()
        SEARCH_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
    m = re.search(r'https?://[^\s)\]>"]+|www\.[^\s)\]>"]+', cache[row['id']])
    return _site_ok(m.group(0).rstrip('.,'), row) if m else None


def stuttgart_deltas():
    lobw = json.loads((PROJECT_ROOT / 'data_stuttgart' / 'cache' / 'lobw_stuttgart.json').read_text())
    rows = db_rows('stuttgart')
    deltas, traeger = [], {}
    for row in rows:
        match = lobw_match(row, lobw)
        new, reason = lobw_traeger(match) if match else (None, None)
        traeger[row['schulnummer']] = (new or row['traegerschaft'], reason)
        if BAD_STUTTGART_SITE in (row['website'] or ''):
            site = _site_ok(match['INTERNET'].strip(), row) if match and match.get('INTERNET') else None
            reason = 'LOBW Schulverzeichnis URL'
            if not site:
                site, reason = search_site(row), 'Google Search, page names the school'
            deltas.append((row, 'website', row['website'], site, reason if site else 'no own website found'))
    # A branch or the primary part ("X (Außenstelle)", "X (Grundschule)") shares the site of "X"
    base = lambda name: re.sub(r'\s*\([^)]*\)\s*', ' ', name).strip().lower()
    found = {base(r['schulname']): (d[3], r['schulname']) for d in deltas for r in [d[0]] if d[3]}
    for i, (row, col, old, new, reason) in enumerate(deltas):
        if not new and base(row['schulname']) in found:
            site, name = found[base(row['schulname'])]
            deltas[i] = (row, col, old, site, f"same school as '{name}'")
    for row in rows:
        new, reason = traeger[row['schulnummer']]
        parent = row['schulnummer'][:-3] if row['schulnummer'].endswith('-GS') else None
        if parent in traeger:  # primary part of a combined school follows its school (was hard-coded Öffentlich)
            new, reason = traeger[parent][0], f"same operator as {parent} ({traeger[parent][1] or 'its current value'})"
        if reason and new != row['traegerschaft']:
            deltas.append((row, 'traegerschaft', row['traegerschaft'], new, reason))
    return deltas


def bremen_deltas():
    raw = pd.concat([pd.read_csv(PROJECT_ROOT / 'data_bremen' / 'raw' / f, dtype=str)
                     for f in ('bremen_school_master.csv', 'bremen_other_schools.csv')]).drop_duplicates('schulnummer')
    official = {}
    for _, r in raw.iterrows():
        form, name = str(r.get('schulform_raw') or '').lower(), str(r.get('Name1') or '').lower()
        official[str(r['schulnummer'])] = (('Privat' if 'privat' in form or 'privat' in name else 'Öffentlich'),
                                           f"official Schulform '{r.get('schulform_raw')}'")
    deltas = []
    for row in db_rows('bremen'):
        if str(row['schulnummer']) in official:
            new, reason = official[str(row['schulnummer'])]
            if new != row['traegerschaft']:
                deltas.append((row, 'traegerschaft', row['traegerschaft'], new, reason))
    return deltas


def munich_deltas():
    finals = pd.concat([pd.read_csv(PROJECT_ROOT / 'data_munich' / 'final' / f'munich_{pt}_school_master_table_final.csv',
                                    dtype=str) for pt in ('primary', 'secondary')])
    site = dict(zip(finals['schulnummer'], finals['website']))
    return [(row, 'website', row['website'], site.get(row['schulnummer']), 'value in the final tables (Supabase-only placeholder)')
            for row in db_rows('muenchen') if PLACEHOLDER in (row['website'] or '')]


def sql_literal(v):
    return 'NULL' if v in (None, '') else "'" + str(v).replace("'", "''") + "'"


def patch_finals(city, deltas):
    changes = {}
    for row, col, old, new, _ in deltas:
        changes.setdefault(str(row['schulnummer']), {})[col] = (old, new or '')
    for group in FINALS.get(city, {}).values():
        for rel in group:
            path = PROJECT_ROOT / rel
            if not path.exists():
                continue
            if path.suffix == '.csv':
                df = pd.read_csv(path, dtype=str, keep_default_na=False)
            else:
                df = pd.read_parquet(path)
            key = df['schulnummer'].astype(str)
            n = 0
            for snr, cols in changes.items():
                idx = key[key == snr].index
                for col, (old, new) in cols.items():
                    if col in df.columns and len(idx):
                        df.loc[idx, col] = new if new else ('' if path.suffix == '.csv' else None)
                        n += len(idx)
            if n:
                if path.suffix == '.csv':
                    df.to_csv(path, index=False, encoding='utf-8-sig')
                else:
                    df.to_parquet(path, index=False)
            print(f"  {rel}: {n} cells")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--patch-finals', action='store_true')
    args = ap.parse_args()
    requests.packages.urllib3.disable_warnings()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    per_city = {'stuttgart': stuttgart_deltas(), 'bremen': bremen_deltas(), 'muenchen': munich_deltas()}
    records, apply, rollback = [], [], []
    for city, deltas in per_city.items():
        for row, col, old, new, reason in deltas:
            records.append({'city': city, 'tbl': row['tbl'], 'id': row['id'], 'schulnummer': row['schulnummer'],
                            'schulname': row['schulname'], 'column': col, 'old': old, 'new': new, 'reason': reason})
            apply.append(f"UPDATE public.{row['tbl']} SET {col} = {sql_literal(new)} WHERE id = '{row['id']}' "
                         f"AND {col} IS NOT DISTINCT FROM {sql_literal(old)};")
            rollback.append(f"UPDATE public.{row['tbl']} SET {col} = {sql_literal(old)} WHERE id = '{row['id']}' "
                            f"AND {col} IS NOT DISTINCT FROM {sql_literal(new)};")
        print(f"{city}: {len(deltas)} changes", pd.Series([d[1] for d in deltas]).value_counts().to_dict())
    pd.DataFrame(records).to_csv(OUT_DIR / 'deltas.csv', index=False, encoding='utf-8-sig')
    (OUT_DIR / 'apply.sql').write_text('\n'.join(apply) + '\n')
    (OUT_DIR / 'rollback.sql').write_text('\n'.join(rollback) + '\n')
    print(f"→ {OUT_DIR.relative_to(PROJECT_ROOT)}/ (deltas.csv, apply.sql, rollback.sql)")
    if args.patch_finals:
        for city in ('stuttgart', 'bremen'):
            patch_finals(city, per_city[city])


if __name__ == '__main__':
    main()
