#!/usr/bin/env python3
"""
Scrape per-school student and teacher counts from the bildung.berlin.de
school portraits (Schulverzeichnis) into the bildungsstatistik CSV format.

Why: the old source, bildungsstatistik.berlin.de ListGen/SVZ_Fakt5.aspx, was
taken offline in September 2026 (its successor /next/ is login-only) and never
published 2025/26. The portraits are the only public per-school source; they
show the current school year only (students as of the October census, staff
as of 1 November) and are overwritten when the next year goes live, so the
raw HTML is cached as the archive.

Per BSN, in one session (the server keeps the selected school in the cookie):
  Schulliste.aspx?Suchbegriff={BSN} → redirects to Schulportrait.aspx, or a list
  page whose first IDSchulzweig is opened; then schuelerschaft.aspx and
  schulpersonal.aspx.

Output: data_berlin/raw/bildungsstatistik_{school_year}.csv, same columns as the
SVZ_Fakt5 export, so enrich_berlin_schools_with_statistics.py can read it.
Teacher w/m are left empty: the portraits give only percentages. Private
schools often have no teacher figures.

Usage:
    venv/bin/python scripts_berlin/scrapers/scrape_schulportrait_statistics.py --limit 5
    venv/bin/python scripts_berlin/scrapers/scrape_schulportrait_statistics.py
"""

import argparse
import csv
import html
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data_berlin" / "raw"
CACHE_DIR = PROJECT_ROOT / "data_berlin" / "cache" / f"schulportrait_{datetime.now():%Y-%m}"
BSN_SOURCES = [
    RAW_DIR / "bildungsstatistik_2024_25.csv",
    PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final.csv",
    PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final.csv",
]
BASE = "https://www.bildung.berlin.de/Schulverzeichnis/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/128.0 Safari/537.36',
    'Accept-Language': 'de-DE,de;q=0.9',
}
DELAY_S = 1.0
NO_DATA = 'keine Daten verfügbar'
OUT_COLUMNS = ['Schuljahr', 'BSN', 'NAME', 'Schüler (m/w/d)', 'Schüler (w)', 'Schüler (m)',
               'Lehrkräfte (m,w,d)', 'Lehrkräfte (w)', 'Lehrkräfte (m)']


def table_rows(page: str):
    out = []
    for table in re.findall(r'<table.*?</table>', page, re.S):
        for tr in re.findall(r'<tr.*?</tr>', table, re.S):
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.S)
            out.append([re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', '', c))).strip() for c in cells])
    return [r for r in out if r]


def _int(text):
    try:
        return int(str(text).replace('.', '').strip())
    except ValueError:
        return None


def _plain(page: str) -> str:
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', page)))


def _is_tab(page: str) -> bool:
    """A real tab page has a data table or the explicit no-data notice."""
    return NO_DATA in page or '<table' in page


def fetch(bsn: str):
    """Return (schuelerschaft_html, schulpersonal_html), or None if the BSN is not found."""
    s = requests.Session()
    s.headers.update(HEADERS)
    r = s.get(BASE + 'Schulliste.aspx', params={'Suchbegriff': bsn}, timeout=30)
    time.sleep(DELAY_S)
    portrait_id = None
    if 'Schulportrait' not in r.url:
        # A list page: the search also matches other schools that mention the BSN
        # (e.g. "Carl-Legien-Schule (Schulbetrieb an 01B04) - 08B05"), so open
        # each hit until the portrait heading ends with this BSN.
        for candidate in dict.fromkeys(re.findall(r'IDSchulzweig=\s*(\d+)', r.text)):
            p = s.get(BASE + 'Schulportrait.aspx', params={'IDSchulzweig': candidate}, timeout=30)
            time.sleep(DELAY_S)
            heading = re.findall(r'lblUebSchule[^>]*>([^<]*)', p.text)
            if heading and html.unescape(heading[0]).strip().endswith(bsn):
                portrait_id = candidate
                break
        if portrait_id is None:
            return None
    pages = []
    for tab in ('schuelerschaft.aspx', 'schulpersonal.aspx'):
        for attempt in range(2):
            page = s.get(BASE + tab, timeout=30).text
            time.sleep(DELAY_S)
            if _is_tab(page) or attempt:
                break
            # Session lost the school (seen once in testing): reopen the portrait and retry
            if portrait_id:
                s.get(BASE + 'Schulportrait.aspx', params={'IDSchulzweig': portrait_id}, timeout=30)
            else:
                s.get(BASE + 'Schulliste.aspx', params={'Suchbegriff': bsn}, timeout=30)
            time.sleep(DELAY_S)
        pages.append(page)
    return tuple(pages)


def parse(bsn: str, ss: str, sp: str) -> dict:
    heading = re.findall(r'lblUebSchule[^>]*>([^<]*)', ss) or re.findall(r'lblUebSchule[^>]*>([^<]*)', sp)
    heading = html.unescape(heading[0]).strip() if heading else ''
    rec = {'BSN': bsn, 'NAME': re.sub(rf'\s*-\s*{re.escape(bsn)}$', '', heading),
           'bsn_verified': heading.endswith(bsn)}
    year = re.findall(r'Jahrgangsstufen\s*(20\d\d/\d\d)', _plain(ss)) or re.findall(r'(20\d\d/\d\d)', _plain(ss))
    rec['Schuljahr'] = year[0] if year else None
    rec['stand_schueler'] = (re.findall(r'Daten-Stand:\s*([\d.]+)', ss) or [None])[0]
    rec['stand_personal'] = (re.findall(r'Daten-Stand:\s*([\d.]+)', sp) or [None])[0]

    rows = table_rows(ss)
    total = w = m = None
    if rows and rows[0][0] == 'Jahrgangsstufe':
        grades = [r for r in rows[1:] if r[0] != 'Insgesamt' and len(r) >= 4]
        total = next((_int(r[-1]) for r in rows if r[0] == 'Insgesamt'), None)
        w = sum(_int(r[1]) or 0 for r in grades)
        m = sum(_int(r[2]) or 0 for r in grades)
        if total is not None and w + m != total:
            logger.warning(f"  {bsn}: w+m={w + m} ≠ Insgesamt {total}; keeping the total only")
            w = m = None
    elif rows and rows[0][0] == 'Schulzweig':  # OSZ layout: one row per Bildungsgang
        total = sum(_int(r[-1]) or 0 for r in rows[1:] if r[0] != 'Insgesamt')
    rec.update({'Schüler (m/w/d)': total, 'Schüler (w)': w, 'Schüler (m)': m})
    teachers = next((_int(r[-1]) for r in table_rows(sp) if r[0] == 'Lehrkräfte'), None)
    rec.update({'Lehrkräfte (m,w,d)': teachers, 'Lehrkräfte (w)': None, 'Lehrkräfte (m)': None})
    return rec


def load_bsns():
    bsns = set()
    for path in BSN_SOURCES:
        if not path.exists():
            continue
        if path.name.startswith('bildungsstatistik_'):
            df = pd.read_csv(path, sep=';', dtype=str)
            bsns |= set(df['BSN'].dropna().str.strip())
        else:
            df = pd.read_csv(path, dtype=str, usecols=['schulnummer'])
            bsns |= set(df['schulnummer'].dropna().str.strip())
    return sorted(b for b in bsns if re.fullmatch(r'\d\d[A-Z]\d\d', b))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, help='Only the first N BSNs (smoke test)')
    args = ap.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    bsns = load_bsns()[:args.limit] if args.limit else load_bsns()
    logger.info(f"{len(bsns)} BSNs; HTML cache {CACHE_DIR.relative_to(PROJECT_ROOT)}")

    records, not_found = [], []
    for i, bsn in enumerate(bsns, 1):
        ss_path, sp_path = CACHE_DIR / f"{bsn}_schuelerschaft.html", CACHE_DIR / f"{bsn}_schulpersonal.html"
        miss_path = CACHE_DIR / f"{bsn}.notfound"
        if miss_path.exists():
            not_found.append(bsn)
            continue
        if not (ss_path.exists() and sp_path.exists()):
            try:
                pages = fetch(bsn)
            except requests.RequestException as e:
                logger.warning(f"  {bsn}: {e} — skipped, re-run to retry")
                continue
            if pages is None:
                miss_path.touch()
                not_found.append(bsn)
                continue
            ss_path.write_text(pages[0], encoding='utf-8')
            sp_path.write_text(pages[1], encoding='utf-8')
        records.append(parse(bsn, ss_path.read_text(encoding='utf-8'), sp_path.read_text(encoding='utf-8')))
        if i % 50 == 0:
            logger.info(f"  {i}/{len(bsns)}")

    df = pd.DataFrame(records)
    bad = df[~df['bsn_verified']]
    if len(bad):
        logger.warning(f"{len(bad)} pages whose heading does not carry the BSN (excluded): {list(bad['BSN'])}")
    df = df[df['bsn_verified']]
    years = df['Schuljahr'].value_counts().to_dict()
    logger.info(f"Schuljahr: {years}; Daten-Stand students {df['stand_schueler'].value_counts().head(3).to_dict()}, "
                f"staff {df['stand_personal'].value_counts().head(3).to_dict()}")
    school_year = max(years, key=years.get)
    df = df[df['Schuljahr'] == school_year]

    out = RAW_DIR / f"bildungsstatistik_{school_year.replace('/', '_')}.csv"
    with open(out, 'w', encoding='utf-8', newline='') as f:  # same shape as the SVZ_Fakt5 export, trailing ';'
        f.write(';'.join(OUT_COLUMNS) + ';\n')
        w = csv.writer(f, delimiter=';', lineterminator=';\n')
        for rec in df.sort_values('BSN').to_dict('records'):
            w.writerow(['' if pd.isna(rec[c]) else (int(rec[c]) if isinstance(rec[c], float) else rec[c])
                        for c in OUT_COLUMNS])
    meta = {'source': BASE + 'Schulportrait.aspx', 'scraped': f"{datetime.now():%Y-%m-%d}",
            'school_year': school_year, 'schools': len(df),
            'with_students': int(df['Schüler (m/w/d)'].notna().sum()),
            'with_teachers': int(df['Lehrkräfte (m,w,d)'].notna().sum()),
            'stand_schueler': df['stand_schueler'].mode().tolist(), 'stand_personal': df['stand_personal'].mode().tolist(),
            'not_found': not_found}
    out.with_suffix('.meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f"Wrote {out.relative_to(PROJECT_ROOT)}: {meta['schools']} schools, "
                f"{meta['with_students']} with students, {meta['with_teachers']} with teachers; "
                f"{len(not_found)} BSNs not found")


if __name__ == '__main__':
    main()
