#!/usr/bin/env python3
"""
Refresh admission criteria, application windows and open days for German schools
(schools and primary_schools), from the school's own website.

Replaces the April 2026 run (enrich_german_schools_with_admission_and_open_days.py),
which used Gemini with Google Search grounding. Grounded calls are capped per day,
and the descriptions pipeline needs them, so this version crawls the site itself
and gives Gemini the page text without tools:

  1. crawl   — homepage + up to 30 same-site pages, registration/dates pages first
               (cached in data_shared/cache/school_site_pages_admission/)
  2. extract — Gemini 3 Flash returns German and English fields in one call; every
               criterion, window and open day carries a verbatim quote from the pages
  3. check   — items whose quote is not on a crawled page are dropped; open days
               before today are dropped (the latest past one becomes last_open_day_seen),
               and so are open days whose date contradicts the quote's year or weekday
               and first-day events for already admitted pupils (implausible())

Output: <out>/admission_results.jsonl (one line per school). Nothing is written to
Supabase here; upload with upload_admission_refresh.py.

Usage:
    venv/bin/python scripts_shared/enrichment/refresh_admission_open_days.py --out <dir> [--tables schools,primary_schools] [--limit N]
"""
import argparse
import concurrent.futures as cf
import json
import re
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import replicate_lovable_description_jobs as job  # noqa: E402  (Gemini client, Supabase fetch)
import verify_trim_descriptions as vt  # noqa: E402  (site crawler, quote matching)

MODEL = 'gemini-3-flash-preview'
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = PROJECT_ROOT / 'data_shared' / 'cache' / 'school_site_pages_admission'
MAX_PAGES = 30
PRIORITY = re.compile(r'anmeld|aufnahme|bewerb|übergang|uebergang|einschul|schulanfang|schulstart|kennenlern|'
                      r'tag.der.offenen|offene.t|offenen.t|infoabend|info-abend|informationsabend|schnupper|'
                      r'termin|kalender|aktuell|news|neuigkeit|klasse.?5|5\.?.klass|fünft|fuenft|klasse.?7|'
                      r'oberstufe|eltern|kontakt.*anmeld', re.I)
GERMAN_CITIES = vt.GERMAN_CITIES
COLS = 'id,city,schulnummer,schulname,school_type,strasse,plz,website'

PROMPT = """You extract admission information for parents from the pages of a German school's own website.

School: {schulname} ({school_type}), {strasse}, {plz} {city}
Today: {today}

Website pages (fetched today):
{pages}

Extract ONLY what these pages state. Never guess, never use outside knowledge, never fill gaps.
- admission criteria: how children get a place at THIS school (catchment/Einzugsbereich, Losverfahren, Notendurchschnitt/Förderprognose, Geschwisterkind, profile tests, required documents, Anmeldung in person, …). Short bullets.
- application window: the registration/application period for the next intake, with ISO dates if stated.
- open days: Tag der offenen Tür, Infoabend, Schnuppertag, Kennenlernnachmittag, Anmeldetag, etc., with ISO date and times if stated. Include past ones too (we filter them).
- For EVERY bullet, window and open day give "evidence": a quote copied character for character from the pages above (max 30 words) that states it, and "source_url": the page it is on.
- Give each text in German (de) and English (en). The English must say the same as the German.

Answer with ONLY a JSON object:
{{"criteria": [{{"de": "...", "en": "...", "evidence": "...", "source_url": "..."}}],
  "application_window": {{"opens": "YYYY-MM-DD"|null, "closes": "YYYY-MM-DD"|null, "notes_de": "...", "notes_en": "...", "evidence": "...", "source_url": "..."}} | null,
  "notes_de": "one short paragraph with other admission facts from the pages, or empty", "notes_en": "...", "notes_evidence": "..."|null,
  "open_days": [{{"date": "YYYY-MM-DD", "start_time": "HH:MM"|null, "end_time": "HH:MM"|null, "event_type_de": "Tag der offenen Tür"|"Infoabend"|"Schnuppertag"|"Anmeldetag"|"Sonstiges", "event_type_en": "...", "audience_de": "...", "audience_en": "...", "notes_de": "...", "notes_en": "...", "evidence": "...", "source_url": "..."}}]}}"""


def _norm(t):
    return vt._norm(t)


def corpus_of(pages):
    return _norm(' '.join(p.get('text', '') for p in pages))


def found(evidence, corpus):
    return vt.evidence_found(evidence, corpus)


def _iso(d):
    try:
        return date.fromisoformat(str(d)[:10])
    except (TypeError, ValueError):
        return None


def extract(row, pages):
    texts, total = [], 0
    for p in pages:
        if p.get('text') and total < vt.TOTAL_CHARS:
            texts.append(f"--- {p['url']}\n{p['text'][:vt.TOTAL_CHARS - total]}")
            total += len(texts[-1])
    prompt = PROMPT.format(today=date.today().isoformat(), pages='\n\n'.join(texts),
                           **{k: row.get(k) or '' for k in ('schulname', 'school_type', 'strasse', 'plz', 'city')})
    body = {'contents': [{'parts': [{'text': prompt}]}],
            'generationConfig': {'responseMimeType': 'application/json', 'maxOutputTokens': 16384,
                                 'thinkingConfig': {'thinkingLevel': 'low'}}}
    for attempt in range(3):
        try:
            data = job.gemini(MODEL, body, timeout=240)
            return json.loads(job.gen_text(data), strict=False), vt.usage_of(data)
        except Exception as e:  # noqa: BLE001
            err = str(e)[:150]
            time.sleep(10 * (attempt + 1))
    return {'error': err}, None


WEEKDAYS = ('montag', 'dienstag', 'mittwoch', 'donnerstag', 'freitag', 'samstag', 'sonntag')
MONTHS = {'januar': 1, 'februar': 2, 'märz': 3, 'april': 4, 'mai': 5, 'juni': 6, 'juli': 7, 'august': 8,
          'september': 9, 'oktober': 10, 'november': 11, 'dezember': 12}
# a 2-digit year must follow the date directly ("17.11.25"), so "03.03. 14:00" is not read as 2014
NUM_DATE = re.compile(r'(?<!\d)(\d{1,2})\.\s?(\d{1,2})\.(?:\s?(\d{4})(?![\d:])|(\d{2})(?!\d))?')
WORD_DATE = re.compile(r'(?<!\d)(\d{1,2})\.?\s+(' + '|'.join(MONTHS) + r')(?:\s+(\d{4}))?', re.I)
NEW_CLASS = re.compile(r'einschul|erstklässler|neuen?\s+(1\.|ersten?)\s*klasse|klasse\s+eins', re.I)
FOR_APPLICANTS = re.compile(r'info|anmeld|offene|schnupper|beratung|interessiert|kennenlernen|aufnahme|test|prüfung', re.I)


def implausible(ev, d):
    """Why an open day should be dropped although its quote is on the site, or None.

    The quote is verbatim, but the model fills in a missing year itself and sometimes
    moves a past date into next year; and first-day ceremonies are not open days."""
    text = ev.get('evidence') or ''
    quoted = [(int(a), int(b), y4 or y2) for a, b, y4, y2 in NUM_DATE.findall(text)]
    quoted += [(int(a), MONTHS[b.lower()], c) for a, b, c in WORD_DATE.findall(text)]
    same_day = [(dd, mm, yy) for dd, mm, yy in quoted if (dd, mm) == (d.day, d.month)]
    years = {int(yy) + (2000 if len(yy) == 2 else 0) for _, _, yy in same_day if yy}
    if years and d.year not in years:
        return 'year differs from the quote'
    named = {w for w in WEEKDAYS if re.search(rf'\b{w}\b', text, re.I)}
    if len(same_day) == 1 and len(quoted) == 1 and len(named) == 1 and WEEKDAYS[d.weekday()] not in named:
        return 'weekday differs from the quote'
    kind = ' '.join(str(ev.get(k) or '') for k in ('event_type_de', 'notes_de')) + ' ' + text
    if NEW_CLASS.search(kind) and not FOR_APPLICANTS.search(kind):
        return 'first-day event for admitted pupils'
    return None


def check(out, corpus):
    """Keep only items whose quote is on a crawled page; split open days into upcoming / past."""
    today = date.today()
    dropped = 0
    criteria = []
    for c in out.get('criteria') or []:
        if isinstance(c, dict) and c.get('de') and found(c.get('evidence'), corpus):
            criteria.append(c)
        else:
            dropped += 1
    window = out.get('application_window') if isinstance(out.get('application_window'), dict) else None
    if window and not (found(window.get('evidence'), corpus) and (window.get('opens') or window.get('closes') or window.get('notes_de'))):
        window, dropped = None, dropped + 1
    if window:
        for k in ('opens', 'closes'):
            window[k] = _iso(window.get(k)).isoformat() if _iso(window.get(k)) else None
        end = _iso(window.get('closes') or window.get('opens'))
        if end and (today - end).days > 400:  # an old page's window (e.g. Oct 2024) says nothing about the next round
            window, dropped = None, dropped + 1
    notes_ok = bool(out.get('notes_de')) and found(out.get('notes_evidence'), corpus)
    upcoming, past = [], []
    for ev in out.get('open_days') or []:
        d = _iso(ev.get('date')) if isinstance(ev, dict) else None
        if not d or not found(ev.get('evidence'), corpus) or implausible(ev, d):
            dropped += 1
            continue
        ev['date'] = d.isoformat()
        (upcoming if d >= today else past).append(ev)
    upcoming.sort(key=lambda e: e['date'])
    return {'criteria': criteria, 'window': window,
            'notes_de': out.get('notes_de') if notes_ok else None, 'notes_en': out.get('notes_en') if notes_ok else None,
            'open_days': upcoming, 'last_open_day_seen': max((e['date'] for e in past), default=None),
            'dropped_unverified': dropped}


def process(row):
    res = {'id': row['id'], 'tbl': row['tbl'], 'city': row['city'], 'schulname': row['schulname'],
           'website': row.get('website')}
    if not row.get('website'):
        return {**res, 'status': 'no website'}
    pages = vt.crawl_site(row, priority=PRIORITY, cache_dir=CACHE_DIR, max_pages=MAX_PAGES)
    good = [p for p in pages if p.get('text')]
    if sum(len(p['text']) for p in good) < 1500:
        return {**res, 'status': 'site not readable', 'site_error': next((p.get('error') for p in pages if 'error' in p), None)}
    out, usage = extract(row, good)
    if 'error' in out:
        return {**res, 'status': f"extract failed: {out['error']}"}
    checked = check(out, corpus_of(good))
    has = checked['criteria'] or checked['window'] or checked['open_days'] or checked['notes_de']
    return {**res, **checked, 'pages': len(good), 'usage': usage,
            'status': 'ok' if has else 'nothing on the site'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--tables', default='schools,primary_schools')
    ap.add_argument('--ids', type=Path, help='only these school ids')
    ap.add_argument('--limit', type=int)
    ap.add_argument('--workers', type=int, default=12)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    rows = [dict(r, tbl=tbl) for tbl in args.tables.split(',') for city in GERMAN_CITIES
            for r in job.fetch(tbl, f'city=eq.{city}', COLS)]
    if args.ids:
        wanted = set(args.ids.read_text().split())
        rows = [r for r in rows if r['id'] in wanted]
    out_path = args.out / 'admission_results.jsonl'
    done = {json.loads(l)['id'] for l in out_path.read_text().splitlines()} if out_path.exists() else set()
    rows = [r for r in rows if r['id'] not in done][:args.limit]
    print(f"{len(rows)} schools to refresh", flush=True)

    def safe(r):
        try:
            return process(r)
        except Exception as e:  # noqa: BLE001
            return {'id': r['id'], 'tbl': r['tbl'], 'schulname': r['schulname'], 'status': f'error: {e}'[:150]}

    with open(out_path, 'a', encoding='utf-8') as fh, cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, fut in enumerate(cf.as_completed([ex.submit(safe, r) for r in rows]), 1):
            fh.write(json.dumps(fut.result(), ensure_ascii=False) + '\n'); fh.flush()
            if i % 50 == 0:
                print(f"  {i}/{len(rows)}", flush=True)
    stats = {}
    for l in out_path.read_text().splitlines():
        s = json.loads(l)['status'].split(':')[0]
        stats[s] = stats.get(s, 0) + 1
    print(f"done: {stats}", flush=True)


if __name__ == '__main__':
    main()
