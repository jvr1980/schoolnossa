#!/usr/bin/env python3
"""
Minimal-edit clean-up of the school descriptions users see (description_de /
description_en), following the September 2026 description audit
(docs/audits/DESCRIPTION_AUDIT_2026-09.md):

1. remove every statement of student / teacher / staff / class counts — the
   app shows official counts separately, with their year;
2. apply targeted corrections for schools whose audited claims were wrong
   (CORRECTIONS below; those rows also get their raw `description` edited).

Each text goes through Gemini (no web search, temperature 0) with a strict
"change nothing else, add nothing" instruction, and every result is validated:
no count phrase left, no number that was not in the input, ≥ 95 % of the
output's words taken from the input, and length ≥ 60 % of the input. Rows that
fail are skipped and listed for review. Nothing is written to Supabase here:
results go to data_shared/description_scrub_<date>/results.jsonl for upload.

Usage:
    venv/bin/python scripts_shared/enrichment/scrub_description_claims.py --only-corrections
    venv/bin/python scripts_shared/enrichment/scrub_description_claims.py --limit 20
    venv/bin/python scripts_shared/enrichment/scrub_description_claims.py
"""
import argparse
import concurrent.futures as cf
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import replicate_lovable_description_jobs as job  # noqa: E402  (Gemini client + Supabase read helper)

OUT_DIR = PROJECT_ROOT / 'data_shared' / f"description_scrub_{datetime.now():%Y-%m-%d}"
MODEL = 'gemini-3-flash-preview'
GERMAN_CITIES = ('berlin', 'hamburg', 'muenchen', 'frankfurt', 'koeln', 'duesseldorf',
                 'stuttgart', 'dresden', 'leipzig', 'bremen')

COUNT_DE = re.compile(r'\d[\d.]*\s*(?:Schülerinnen|Schüler|Kinder|Lernende|Jugendliche|Lehrkräfte|Lehrerinnen|Lehrer|'
                      r'Pädagog|Klassen|Mitarbeitende)', re.I)
COUNT_EN = re.compile(r'\d[\d,]*\s*(?:students|pupils|children|learners|teachers|educators|staff members|classes)', re.I)

# Audited claims that the school's own site or an official source contradicts (2026-09-27)
CORRECTIONS = {
    '3df95bc1-2114-44c2-9109-8667fc400446': "Delete every mention of the school keeping goats or sheep (only the chickens are real; goats/sheep are merely planned).",
    'ed6a5dd6-a2e8-4834-ab13-ebc50d1811c9': "Delete every mention of TAFF (it was a talent pilot from 2015/16 that has ended).",
    '67e746e5-da3b-4428-a68f-c363b0665f3c': "Delete every mention of (annual or traditional) trips to Great Britain/England; the school currently runs none.",
    '7a6ea806-b210-4431-8081-2124c2e69ad4': "The Generationenwerkstatt is set up like an artist's studio (Künstleratelier) led by a professional artist, oriented on the methods of the Kunstakademie Düsseldorf. Replace any description of it as traditional craftsmanship/handicraft accordingly.",
    'bf815319-8b05-49a8-bf1c-7a60c27b0975': "The school is in Stuttgart-Stammheim (Park-Realschule Stammheim), not in Zuffenhausen. Replace Zuffenhausen with Stammheim wherever it is given as the school's location.",
    '663b5abf-dbeb-439c-b789-bd58aa412bbc': "(a) The new building was handed over in May 2024 and the school moved in after the 2024 summer holidays: correct any claim that it moved in May 2024. (b) Its Stadtteil is Vogelsang (in the Stadtbezirk Ehrenfeld): where Ehrenfeld is given as its Stadtteil/neighbourhood, say Vogelsang. (c) The DGNB Platinum certification is only targeted ('angestrebt'), not awarded: say so.",
    '2cbada05-bb06-454e-8ce8-e4697de3f2eb': "Delete every mention of an exchange or partnership with a school in Great Britain/England (the English partner school is currently being replaced).",
    # Counts only (handled by the general rule): Kant-Gymnasium, Helmuth Hübener, Georg-Droste-Schule
    '232f2a30-362a-4cbf-bfcb-05a3f1b477f5': None,
    'b4d32765-7728-465b-89ac-136fedfc91a9': None,
    '55419130-cd51-45de-8de4-965d5ba7fca6': None,
}

RULES = """You edit existing school descriptions. Make ONLY these changes:
1. Remove every NUMBER of students, pupils, children, learners, teachers, educators, staff members or classes (e.g. "rund 450 Schülerinnen und Schüler", "approximately 60 teachers", "12 Klassen").
   - If the count is the subject or object of its sentence, replace only the number phrase with a neutral phrase so the sentence stays grammatical (e.g. "lernen hier rund 1.065 Schülerinnen und Schüler" -> "lernen hier die Schülerinnen und Schüler"; "serves approximately 300 students" -> "serves its students").
   - If the count is only an introductory phrase, delete the phrase and let the sentence start with the rest (e.g. "Mit rund 1.000 Schülerinnen und Schülern bietet die Schule ..." -> "Die Schule bietet ..."; "Serving approximately 996 students, the school provides ..." -> "The school provides ...").
   - If a sentence only states the count, delete the sentence.
   - Never leave an empty phrase behind (e.g. "with its students and its teachers", "mit den Schülerinnen und Schülern"): drop such a phrase entirely.
   - NOT counts, keep them unchanged: grade levels ("Klassen 5 bis 10", "grades 1-4"), ages, years, addresses, stream counts ("dreizügig"), and descriptions of staff roles without a number ("ein Team aus Lehrkräften, Sonderpädagogen und Schulsozialarbeitern").
{extra}
Change nothing else: keep every other word, spelling, sentence order, formatting and line break exactly as it is. Do not fix, reword or shorten anything else. Do not add any information.
Return JSON with exactly the keys you were given, each holding the edited text."""


def words(t):
    return re.findall(r'\w+', (t or '').lower())


def numbers(t):
    return set(re.findall(r'\d+', t or ''))


FUNCTION_WORDS = {'die', 'der', 'das', 'den', 'dem', 'des', 'ihre', 'ihren', 'ihrer', 'seine', 'seinen', 'sie', 'es', 'schule',
                  'the', 'its', 'their', 'a', 'an', 'it', 'school'}


def sentences(t):
    return [x for x in re.split(r'(?<=[.!?])\s+|\n+', t or '') if x.strip()]


def only_deletions(old, new):
    """Every new sentence must use only words of its closest old sentence (plus articles)."""
    cased = lambda t: re.findall(r'\w+', t or '')
    olds = [set(cased(o)) for o in sentences(old)]
    for ns in sentences(new):
        toks = cased(ns)
        if not toks:
            continue
        nw = set(toks[1:]) | {toks[0], toks[0].lower(), toks[0].capitalize()}  # first word may change case
        best = max(olds, key=lambda ow: len(set(toks) & ow) / len(set(toks) | ow)) if olds else set()
        extra = {w for w in set(toks[1:]) - best if w.lower() not in FUNCTION_WORDS}
        if not (nw & best) and toks[0].lower() not in FUNCTION_WORDS and toks[0].capitalize() not in best and toks[0].lower() not in {w.lower() for w in best}:
            extra.add(toks[0])
        if extra:
            return f"reworded: {sorted(extra)[:5]}"
    return None


def validate(old, new, is_correction):
    if not isinstance(new, str) or not new.strip():
        return 'empty output'
    if COUNT_DE.search(new) or COUNT_EN.search(new):
        return 'count still present'
    added = numbers(new) - numbers(old)
    if added and not is_correction:
        return f'new numbers {sorted(added)}'
    ow, nw = set(words(old)), words(new)
    recall = sum(1 for w in nw if w in ow) / max(1, len(nw))
    if recall < (0.85 if is_correction else 0.95):
        return f'words not from input: {recall:.0%}'
    if len(new) < 0.6 * len(old):
        return f'too short ({len(new)}/{len(old)})'
    if not is_correction:
        return only_deletions(old, new)
    return None


def edit(row):
    rid = row['id']
    correction = CORRECTIONS.get(rid)
    is_correction = rid in CORRECTIONS
    fields = ['description_de', 'description_en'] + (['description'] if is_correction else [])
    texts = {f: row[f] for f in fields if row.get(f) and not str(row[f]).startswith('[RESEARCH_FAILED')}
    extra = f"2. {correction}" if correction else ""
    body = {'contents': [{'parts': [{'text': json.dumps(texts, ensure_ascii=False)}]}],
            'systemInstruction': {'parts': [{'text': RULES.format(extra=extra)}]},
            'generationConfig': {'temperature': 0, 'responseMimeType': 'application/json', 'maxOutputTokens': 32768,
                                 # A pure edit needs little reasoning; unbounded thinking ate the whole budget (MAX_TOKENS)
                                 'thinkingConfig': {'thinkingLevel': 'low'}}}
    result = {'id': rid, 'tbl': row['tbl'], 'city': row['city'], 'schulnummer': row['schulnummer'],
              'schulname': row['schulname'], 'correction': bool(correction), 'old': texts, 'new': {}, 'problems': {}}
    pending, err = dict(texts), None
    for attempt in range(3):  # retries cover malformed JSON and outputs that fail validation
        if attempt:  # a deterministic retry would repeat the same output
            body['generationConfig']['temperature'] = 0.3
            body['contents'][0]['parts'][0]['text'] = json.dumps(pending, ensure_ascii=False)
        try:
            out = json.loads(job.gen_text(job.gemini(MODEL, body, timeout=180)), strict=False)
        except Exception as e:  # noqa: BLE001
            err = str(e)[:120]
            time.sleep(3 * (attempt + 1))
            continue
        for f in list(pending):
            new = out.get(f)
            problem = validate(texts[f], new, is_correction)
            if problem:
                result['problems'][f] = problem
            else:
                result['problems'].pop(f, None)
                if new != texts[f]:
                    result['new'][f] = new
                pending.pop(f)
        if not pending:
            break
    if pending and not result['new'] and err and not result['problems']:
        return {'id': rid, 'tbl': row['tbl'], 'status': 'failed', 'error': err}
    result['status'] = 'ok' if not result['problems'] else ('partial' if result['new'] else 'rejected')
    return result


def load_rows(only_corrections):
    cols = 'id,city,schulnummer,schulname,description,description_de,description_en'
    rows = []
    for tbl in ('schools', 'primary_schools'):
        for city in GERMAN_CITIES:
            for r in job.fetch(tbl, f'city=eq.{city}', cols):
                r['tbl'] = tbl
                has_count = bool(COUNT_DE.search(r.get('description_de') or '') or COUNT_EN.search(r.get('description_en') or ''))
                if only_corrections and r['id'] in CORRECTIONS:
                    rows.append(r)
                elif not only_corrections and has_count and r['id'] not in CORRECTIONS:  # corrections run separately
                    rows.append(r)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--only-corrections', action='store_true')
    ap.add_argument('--limit', type=int)
    ap.add_argument('--workers', type=int, default=8)
    args = ap.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_rows(args.only_corrections)
    rows.sort(key=lambda r: (r['id'] not in CORRECTIONS, r['id']))
    if args.limit:
        rows = rows[:args.limit]
    print(f"{len(rows)} rows to edit ({sum(r['id'] in CORRECTIONS for r in rows)} with corrections)", flush=True)
    out_path = OUT_DIR / ('results_corrections.jsonl' if args.only_corrections else 'results.jsonl')
    done = {json.loads(l)['id'] for l in out_path.read_text().splitlines()} if out_path.exists() else set()
    todo = [r for r in rows if r['id'] not in done]
    with open(out_path, 'a', encoding='utf-8') as fh, cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, res in enumerate(ex.map(edit, todo), 1):
            fh.write(json.dumps(res, ensure_ascii=False) + '\n'); fh.flush()
            if i % 50 == 0:
                print(f"  {i}/{len(todo)}", flush=True)
    stats = {}
    for l in out_path.read_text().splitlines():
        s = json.loads(l)['status']; stats[s] = stats.get(s, 0) + 1
    print(f"done: {stats} → {out_path}", flush=True)


if __name__ == '__main__':
    main()
