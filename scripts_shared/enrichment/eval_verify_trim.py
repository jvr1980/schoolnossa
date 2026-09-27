#!/usr/bin/env python3
"""
Score verify_trim_descriptions.py against an independent claim-level audit.

Ground truth: audit claims (verified / not_found / contradicted, with severity)
made by separate agents that never saw the checker's output. For every school
a judge (Gemini Flash) marks which audit claims each text version still
states: the original, and the trimmed text per model and policy. The original
column is a sanity check on the judge (every claim should be present).

Reported per model × policy:
  - share of contradicted (and material) claims removed   → what the trim fixes
  - share of verified claims removed                      → collateral damage
  - share of not_found claims removed
  - schools with ≥1 material error left
  - characters removed, and cost per school

Usage:
    venv/bin/python scripts_shared/enrichment/eval_verify_trim.py --dir data_shared/description_verify_2026-09-27 \
        --truth truth.json
"""
import argparse
import concurrent.futures as cf
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import replicate_lovable_description_jobs as job  # noqa: E402

JUDGE_MODEL = 'gemini-3-flash-preview'
# $ per 1M tokens (ai.google.dev pricing, ≤200k-token prompts); thinking is billed as output
PRICE = {'gemini-3.1-pro-preview': (2.0, 12.0), 'gemini-3-flash-preview': (0.5, 3.0)}
SEARCH_PRICE = 14 / 1000  # per grounded search query beyond the 5,000 free per month
COUNT = re.compile(r'\d[\d.,]*\s*(students|pupils|children|teachers|staff|educators|Schüler|Kinder|Lehrkr|Lehrer)', re.I)

JUDGE = """Below are numbered factual claims about a school, and several versions of its description (each in German and English).
For each version, list the numbers of the claims that the version still states, fully or partly, in either language. A claim counts as stated if a reader of that version would still get that piece of information.

Claims:
{claims}

Versions:
{versions}

Answer with ONLY JSON: {{"<version name>": [claim numbers still stated], ...}} with one key per version."""


def cost(usage, model):
    if not usage:
        return 0.0
    pin, pout = PRICE[model]
    return ((usage['promptTokenCount'] + usage['toolUsePromptTokenCount']) * pin
            + (usage['candidatesTokenCount'] + usage['thoughtsTokenCount']) * pout) / 1e6


def judge(claims, versions):
    body = {'contents': [{'parts': [{'text': JUDGE.format(
        claims='\n'.join(f"{i}. {c['claim']}" for i, c in enumerate(claims, 1)),
        versions='\n\n'.join(f"=== {name}\n{t.get('description_de', '')}\n---\n{t.get('description_en', '')}"
                             for name, t in versions.items()))}]}],
        'generationConfig': {'responseMimeType': 'application/json', 'maxOutputTokens': 16384}}
    for _ in range(3):
        try:
            out = json.loads(job.gen_text(job.gemini(JUDGE_MODEL, body, timeout=300)), strict=False)
            return {k: {int(x) for x in v} for k, v in out.items()}
        except Exception:  # noqa: BLE001
            continue
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', type=Path, required=True)
    ap.add_argument('--truth', type=Path, required=True, help='{school id: [claims with status/severity]}')
    ap.add_argument('--models', default='pro,flash')
    args = ap.parse_args()
    truth = json.loads(args.truth.read_text(encoding='utf-8'))
    runs = {}
    for m in args.models.split(','):
        for line in (args.dir / f'results_{m}.jsonl').read_text().splitlines():
            r = json.loads(line)
            if r.get('status') == 'ok':
                runs.setdefault(r['id'], {})[m] = r  # the last ok line per school wins
    ids = [i for i in truth if i in runs and len(runs[i]) == len(args.models.split(','))]
    print(f"{len(ids)} schools with audit + all model runs")

    def score(sid):
        rs = runs[sid]
        any_r = next(iter(rs.values()))
        versions = {'original': any_r['old']}
        for m, r in rs.items():
            for p in 'AB':
                t = dict(any_r['old'])
                t.update(r[f'trim_{p}']['new'])  # a field whose trim failed validation stays original
                versions[f'{m}_{p}'] = t
        return sid, judge(truth[sid], versions)

    judged = {}
    cache = args.dir / 'judged.json'
    if cache.exists():
        judged = json.loads(cache.read_text())
    todo = [i for i in ids if i not in judged]
    with cf.ThreadPoolExecutor(8) as ex:
        for sid, res in ex.map(score, todo):
            if res:
                judged[sid] = {k: sorted(v) for k, v in res.items()}
    cache.write_text(json.dumps(judged))

    rows = []
    for sid in ids:
        if sid not in judged:
            continue
        for n, c in enumerate(truth[sid], 1):
            rows.append({'sid': sid, 'n': n, 'status': c['status'], 'severity': c.get('severity'),
                         'count': bool(COUNT.search(c['claim'])), 'set': c.get('set'),
                         **{v: n in set(judged[sid][v]) for v in judged[sid]}})
    versions = [v for v in judged[next(iter(judged))] if v != 'original']
    orig_missing = [r for r in rows if not r['original']]
    print(f"judge sanity: {len(orig_missing)} of {len(rows)} claims judged absent from the ORIGINAL text "
          f"({len(orig_missing) / len(rows):.1%}); those are excluded below")
    rows = [r for r in rows if r['original']]

    def share(sel, v):
        sel = list(sel)
        return f"{sum(not r[v] for r in sel) / len(sel):.0%} of {len(sel)}" if sel else 'n/a'

    for label, keep in (('all claims', lambda r: True), ('excluding student/teacher counts', lambda r: not r['count'])):
        sub = [r for r in rows if keep(r)]
        print(f"\n== {label}: removed by the trim")
        print(f"{'version':10} {'material':>14} {'contradicted':>14} {'not_found':>14} {'verified':>14}")
        for v in versions:
            print(f"{v:10} {share((r for r in sub if r['severity'] == 'material'), v):>14} "
                  f"{share((r for r in sub if r['status'] == 'contradicted'), v):>14} "
                  f"{share((r for r in sub if r['status'] == 'not_found'), v):>14} "
                  f"{share((r for r in sub if r['status'] == 'verified'), v):>14}")

    print("\n== schools with ≥1 material error (excluding counts)")
    sids = sorted({r['sid'] for r in rows})
    for v in ['original'] + versions:
        n = sum(any(r['sid'] == s and r['severity'] == 'material' and not r['count'] and r[v] for r in rows) for s in sids)
        print(f"{v:10} {n} of {len(sids)} ({n / len(sids):.0%})")

    print("\n== text removed, cost per school")
    for m in args.models.split(','):
        rs = [runs[s][m] for s in sids]
        model = rs[0]['model']
        vc = sum(cost(r['verify']['usage'], model) for r in rs) / len(rs)
        tc = sum(cost(r[f'trim_{p}'].get('usage'), 'gemini-3-flash-preview') for r in rs for p in 'B') / len(rs)
        q = sum(len(r['verify'].get('queries') or []) for r in rs) / len(rs)
        for p in 'AB':
            old = sum(len(r['old'].get(f, '')) for r in rs for f in r['old'])
            new = sum(len({**r['old'], **r[f'trim_{p}']['new']}.get(f, '')) for r in rs for f in r['old'])
            fails = sum(any(f not in r[f'trim_{p}']['new'] for f in r['old']) for r in rs)
            print(f"{m}_{p}: {1 - new / old:.0%} of characters removed; trim validation failed for {fails} schools")
        print(f"{m}: verify ${vc:.3f} + trim ${tc:.4f} per school; {q:.1f} search queries per school "
              f"(${q * SEARCH_PRICE:.3f} if over the free tier)")


if __name__ == '__main__':
    main()
