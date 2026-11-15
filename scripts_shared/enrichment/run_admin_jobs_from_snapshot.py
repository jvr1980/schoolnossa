#!/usr/bin/env python3
"""Run the Lovable admin "Fehlende" jobs locally for a fixed set of rows, without the admin wizard.

Input is a JSON snapshot of the rows, taken with the Lovable SQL tool (the anon key cannot read
schools/primary_schools since the 2026-10-25 RLS change). Output is fill-gaps SQL: every column is
written as COALESCE(column, new value), so a value that already exists is never overwritten.

Jobs (same prompts, models and rules as the edge functions, read 2026-11-15):
  1. research-school-descriptions  -> description, description_grounding   (replicate_lovable_description_jobs)
  2. generate-school-descriptions  -> description_de, description_en
  3. regenerate-embeddings         -> embedding (gemini-embedding-001, 768 dims)
  4. fetch-environment-data        -> env_aqi_* (Air Quality API, UAQI), env_pollen_* (Pollen API, 1 day)
  5. generate-tuition-tiers        -> tuition_tier, tuition_monthly_eur, tuition_tier_reasoning (private only)
  6. generate-tuition-granular     -> tuition_income_matrix, tuition_sibling_discounts, ... (private only)

One difference: when a tuition AI call fails, the edge functions write a fallback guess
("medium", 200 EUR). This script writes nothing for that school and reports it instead.

Usage:
  python scripts_shared/enrichment/run_admin_jobs_from_snapshot.py \
      --snapshot data_shared/cache/gms_admin_jobs/snapshot_2026-11-15.json \
      --workdir data_shared/cache/gms_admin_jobs --table schools --city Berlin
"""
import argparse
import concurrent.futures as cf
import importlib.util
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_spec = importlib.util.spec_from_file_location(
    'rlj', Path(__file__).parent / 'replicate_lovable_description_jobs.py')
rlj = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rlj)  # loads .env, GEMINI_API_KEY, prompts

PLACES_KEY = os.environ['GOOGLE_PLACES_API_KEY']
TIER_MODEL = 'gemini-3.1-pro-preview'

INCOME_RANGES = [
    ('under20', '< €20.000', 15000), ('21-30', '€21.000 - €30.000', 25500),
    ('31-40', '€31.000 - €40.000', 35500), ('41-50', '€41.000 - €50.000', 45500),
    ('51-75', '€51.000 - €75.000', 63000), ('76-100', '€76.000 - €100.000', 88000),
    ('101-125', '€101.000 - €125.000', 113000), ('126-150', '€126.000 - €150.000', 138000),
    ('151-175', '€151.000 - €175.000', 163000), ('176-200', '€176.000 - €200.000', 188000),
    ('201-250', '€201.000 - €250.000', 225500), ('over250', '> €250.000', 300000),
]


def now_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def is_private(s):
    return any(k in (s.get('traegerschaft') or '').lower() for k in ('privat', 'frei'))


def http_json(url, body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method='POST' if body is not None else 'GET',
                                 headers={'Content-Type': 'application/json'})
    return json.load(urllib.request.urlopen(req, timeout=timeout))


# ---- 4. fetch-environment-data ---------------------------------------------------------------
def fetch_env(s):
    out = {}
    lat, lon = s['latitude'], s['longitude']
    if not s.get('env_aqi_fetched_at'):
        try:
            d = http_json(f'https://airquality.googleapis.com/v1/currentConditions:lookup?key={PLACES_KEY}',
                          {'location': {'latitude': lat, 'longitude': lon}})
            uaqi = next((i for i in d.get('indexes') or [] if i.get('code') == 'uaqi'), None)
            if uaqi:
                out.update(env_aqi_annual_avg=uaqi['aqi'], env_aqi_category=uaqi.get('category'),
                           env_aqi_dominant_pollutant=uaqi.get('dominantPollutant'))
            out['env_aqi_fetched_at'] = now_iso()  # edge function marks empty answers as fetched too
        except Exception as e:
            print(f"  aqi {s['schulnummer']}: {e}", flush=True)
    if not s.get('env_pollen_fetched_at'):
        try:
            d = http_json(f'https://pollen.googleapis.com/v1/forecast:lookup?key={PLACES_KEY}'
                          f'&location.latitude={lat}&location.longitude={lon}&days=1')
            info = (d.get('dailyInfo') or [{}])[0].get('pollenTypeInfo') or []
            val = {p['code']: (p.get('indexInfo') or {}).get('value') for p in info}
            tree, grass, weed = val.get('TREE'), val.get('GRASS'), val.get('WEED')
            if info:
                out.update(env_pollen_tree_avg=tree, env_pollen_grass_avg=grass, env_pollen_weed_avg=weed)
                vals = [v for v in (tree, grass, weed) if v is not None]
                if vals:
                    out['env_pollen_overall_avg'] = sum(vals) / len(vals)
            out['env_pollen_fetched_at'] = now_iso()
        except Exception as e:
            print(f"  pollen {s['schulnummer']}: {e}", flush=True)
    return out


# ---- 5./6. tuition ---------------------------------------------------------------------------
def gemini_json(prompt, schema, temperature=None, timeout=300):
    gen = {'responseMimeType': 'application/json', 'responseSchema': schema}
    if temperature is not None:
        gen['temperature'] = temperature
    body = {'contents': [{'parts': [{'text': prompt}]}], 'generationConfig': gen, 'tools': [{'googleSearch': {}}]}
    for attempt in range(3):
        try:
            text = rlj.gen_text(rlj.gemini(TIER_MODEL, body, timeout=timeout))
            if text:
                return json.loads(text)
        except urllib.error.HTTPError as e:
            print(f'  gemini HTTP {e.code}: {e.read().decode()[:160]}', flush=True)
            if e.code == 429:
                time.sleep(15)
        except Exception as e:
            print(f'  gemini: {e}', flush=True)
        time.sleep(2 * (attempt + 1))
    return None


def tuition_tier(s, city):
    # Deterministic path needs known monthly/annual tuition; none of the snapshot rows have it,
    # but keep the rule for parity.
    m = s.get('tuition_monthly_eur') or ((s.get('tuition_annual_eur') or 0) / 12) or None
    if m:
        tier = ('low' if m <= 100 else 'medium' if m <= 300 else 'high' if m <= 500
                else 'premium' if m <= 750 else 'ultra')
        why = (f'Based on maximum monthly tuition of €{m:.0f} (income-based, using highest tier).'
               if s.get('income_based_tuition') else f'Based on monthly tuition of €{m:.0f}.')
        return {'cost_tier': tier, 'approximate_monthly_tuition_eur': round(m), 'reasoning': why}
    def unk(v):
        return 'Unknown' if v is None else v
    prompt = f'''Search the web for "{s['schulname']}" tuition fees {city}.

School info:
- Name: {s['schulname']}
- Type: {s.get('school_type') or "Unknown"}
- Category: secondary
- Website: {s.get('website') or "Not available"}
- Trägerschaft: {s.get('traegerschaft') or "Unknown"}
- Known monthly tuition: {unk(s.get('tuition_monthly_eur'))}
- Known annual tuition: {unk(s.get('tuition_annual_eur'))}
- Income-based: {unk(s.get('income_based_tuition'))}
- Notes: {s.get('tuition_notes') or "None"}

Categorize into cost tier based on MONTHLY tuition (not including meals/aftercare):
- low: €0-100/month (Waldorf with sliding scale, symbolic fees)
- medium: €101-300/month (German private schools, church schools)
- high: €301-500/month (bilingual, IB schools)
- premium: €501-750/month (international schools)
- ultra: €750+/month (elite international schools)

Rules:
- If income-based: use average German household income €45,000/year
- If grade-based: use secondary school (grades 7-12) fees
- Convert annual to monthly (÷12), exclude registration fees'''
    schema = {'type': 'object', 'properties': {
        'cost_tier': {'type': 'string', 'enum': ['low', 'medium', 'high', 'premium', 'ultra']},
        'approximate_monthly_tuition_eur': {'type': 'number'},
        'reasoning': {'type': 'string'}},
        'required': ['cost_tier', 'approximate_monthly_tuition_eur', 'reasoning']}
    r = gemini_json(prompt, schema)
    if r and r.get('cost_tier') not in ('low', 'medium', 'high', 'premium', 'ultra'):
        r['cost_tier'] = 'medium'
    return r


def tuition_granular(s, tier, city):
    # The edge function's select omits tuition_tier_reasoning, so the prompt never contains it.
    known = [f'Cost tier: {tier["cost_tier"]}',
             f'Known monthly tuition: €{tier["approximate_monthly_tuition_eur"]}']
    if s.get('tuition_annual_eur'):
        known.append(f'Known annual tuition: €{s["tuition_annual_eur"]}')
    if s.get('income_based_tuition'):
        known.append('Uses income-based tuition: Yes')
    if s.get('tuition_notes'):
        known.append(f'Notes: {s["tuition_notes"]}')
    ranges = '\n'.join(f'- {v}: {lab} (midpoint €{mid:,})' for v, lab, mid in INCOME_RANGES)
    prompt = f'''Search the web for detailed tuition fee structure for "{s['schulname']}" (private secondary school (grades 7-12) in {city}).
Use Google Search grounding. Prefer official sources (school website / fee schedule PDFs) where available.

SCHOOL INFORMATION:
- Name: {s['schulname']}
- Type: {s.get('school_type') or "Unknown"}
- Website: {s.get('website') or "Not available"}
- Trägerschaft (Operator): {s.get('traegerschaft') or "Unknown"}

KNOWN DATA:
{chr(10).join(known)}

TASK: Estimate the monthly tuition fee (in EUR) for each household income bracket below:

INCOME BRACKETS:
{ranges}

ESTIMATION GUIDELINES:
1. For income-based schools (Waldorf, Montessori, community schools):
   - Typically 1-3% of gross annual income, divided by 12
   - Usually have minimum (~€100-150) and maximum (~€500-800) caps
   - Create a realistic gradient across income levels

2. For flat-fee schools (IB, International, some private Gymnasien):
   - Same fee regardless of income
   - Use the known monthly/annual fee if available
   - If unknown, estimate based on tier: low=€150-250, medium=€300-450, high=€500-800, very_high=€900+

3. For sibling discounts:
   - Common pattern: 2nd child 10-20%, 3rd child 20-30%, 4th+ 25-50%
   - If no info available, use conservative defaults: 10%, 15%, 20%

Provide realistic estimates based on the school type and tier. All values must be positive numbers.'''
    schema = {'type': 'object', 'properties': {
        'income_matrix': {'type': 'object', 'properties': {
            v: {'type': 'number', 'description': f'Monthly tuition in EUR for {lab} annual household income'}
            for v, lab, _ in INCOME_RANGES}, 'required': [v for v, _, _ in INCOME_RANGES]},
        'sibling_discounts': {'type': 'object', 'properties': {
            k: {'type': 'number'} for k in ('child_2_pct', 'child_3_pct', 'child_4_plus_pct')},
            'required': ['child_2_pct', 'child_3_pct', 'child_4_plus_pct']},
        'reasoning': {'type': 'string'}},
        'required': ['income_matrix', 'sibling_discounts', 'reasoning']}
    r = gemini_json(prompt, schema, temperature=0.7, timeout=180)
    if not r:
        return None
    mx = {v: (r.get('income_matrix') or {}).get(v) for v, _, _ in INCOME_RANGES}
    mx = {k: (x if isinstance(x, (int, float)) and x > 0 else 200) for k, x in mx.items()}
    sd = r.get('sibling_discounts') or {}
    return {'income_matrix': mx,
            'sibling_discounts': {'child_2_pct': sd.get('child_2_pct', 10), 'child_3_pct': sd.get('child_3_pct', 15),
                                  'child_4_plus_pct': sd.get('child_4_plus_pct', 20)},
            'reasoning': r.get('reasoning') or 'AI estimation completed'}


# ---- per school ------------------------------------------------------------------------------
def process(s, table, city, results_dir):
    path = results_dir / f"{s['id']}.json"
    res = json.loads(path.read_text()) if path.exists() else {}

    def save():
        path.write_text(json.dumps(res, ensure_ascii=False))

    if s.get('description') is None and 'description' not in res:
        d, g = rlj.research(s)
        if d:
            res['description'] = d
            res['description_researched_at'] = now_iso()
        if g:
            res['description_grounding'] = g
        save()
    raw = s.get('description') or res.get('description')
    if raw and (s.get('description_de') is None or s.get('description_en') is None) and 'description_de' not in res:
        de_en = rlj.generate_de_en(dict(s, description=raw), table == 'schools')
        if de_en:
            res.update(de_en)
        save()
    if raw and s.get('emb_null', True) and 'embedding' not in res:
        v = rlj.embed(raw)
        if v:
            res['embedding'] = [round(x, 6) for x in v]
        save()
    if s.get('latitude') is not None and 'env' not in res and (
            not s.get('env_aqi_fetched_at') or not s.get('env_pollen_fetched_at')):
        res['env'] = fetch_env(s)
        save()
    if is_private(s) and s.get('tuition_tier') is None:
        if 'tier' not in res:
            t = tuition_tier(s, city)
            if t:
                res['tier'] = t
            save()
        if res.get('tier') and s.get('tuition_income_matrix') is None and 'granular' not in res:
            g = tuition_granular(s, res['tier'], city)
            if g:
                res['granular'] = g
                res['granular_at'] = now_iso()
            save()
    print(f"done {s['schulnummer']}: " + ','.join(k for k in res if k != 'description_grounding'), flush=True)
    return s, res


# ---- SQL -------------------------------------------------------------------------------------
def lit(v, cast=None):
    if v is None:
        return 'null'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, (dict, list)):
        v, cast = json.dumps(v, ensure_ascii=False), cast or 'jsonb'
    out = rlj.dollar(str(v))
    return f'{out}::{cast}' if cast else out


def statements(table, results):
    stmts = []
    for s, res in results:
        sets = {}
        for k in ('description', 'description_de', 'description_en'):
            if res.get(k):
                sets[k] = lit(res[k])
        if res.get('description'):
            sets['description_researched_at'] = lit(res['description_researched_at'], 'timestamptz')
        if res.get('description_grounding'):
            sets['description_grounding'] = lit(res['description_grounding'])
        if res.get('embedding'):
            sets['embedding'] = "'[" + ','.join(repr(x) for x in res['embedding']) + "]'::vector"
        for k, v in (res.get('env') or {}).items():
            if v is not None:
                sets[k] = lit(v, 'timestamptz' if k.endswith('_fetched_at') else None)
        if res.get('tier'):
            t = res['tier']
            sets['tuition_tier'] = lit(t['cost_tier'])
            sets['tuition_monthly_eur'] = lit(t['approximate_monthly_tuition_eur'])
            sets['tuition_tier_reasoning'] = lit(t['reasoning'])
        if res.get('granular'):
            g = res['granular']
            sets['tuition_income_matrix'] = lit(g['income_matrix'])
            sets['tuition_sibling_discounts'] = lit(g['sibling_discounts'])
            sets['tuition_granular_reasoning'] = lit(g['reasoning'])
            sets['tuition_granular_generated_at'] = lit(res['granular_at'], 'timestamptz')
        if sets:
            body = ', '.join(f'{k} = COALESCE({k}, {v})' for k, v in sets.items())
            stmts.append(f"UPDATE public.{table} SET {body}, updated_at = now() WHERE id = '{s['id']}';")
    return stmts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', required=True)
    ap.add_argument('--workdir', required=True)
    ap.add_argument('--table', default='schools')
    ap.add_argument('--city', default='Berlin')
    ap.add_argument('--workers', type=int, default=4)
    a = ap.parse_args()
    work = PROJECT_ROOT / a.workdir
    results_dir = work / 'results'
    sql_dir = work / 'sql'
    results_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(exist_ok=True)
    rows = json.loads((PROJECT_ROOT / a.snapshot).read_text())
    print(f'{len(rows)} rows', flush=True)
    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        results = list(ex.map(lambda s: process(s, a.table, a.city, results_dir), rows))

    stmts = statements(a.table, results)
    for f in sql_dir.glob('*.sql'):
        f.unlink()
    chunk, size, idx = [], 0, 1
    for st in stmts:
        if chunk and size + len(st) > 45000:
            (sql_dir / f'update_{idx:02d}.sql').write_text('\n'.join(chunk), encoding='utf-8')
            chunk, size, idx = [], 0, idx + 1
        chunk.append(st)
        size += len(st)
    if chunk:
        (sql_dir / f'update_{idx:02d}.sql').write_text('\n'.join(chunk), encoding='utf-8')

    def count(f):
        return sum(1 for _, r in results if f(r))
    print(f"SQL: {idx} file(s) in {sql_dir.relative_to(PROJECT_ROOT)}; {len(stmts)} row updates")
    print(f"description {count(lambda r: r.get('description'))}, de/en {count(lambda r: r.get('description_de'))}, "
          f"embedding {count(lambda r: r.get('embedding'))}, aqi {count(lambda r: (r.get('env') or {}).get('env_aqi_annual_avg') is not None)}, "
          f"pollen {count(lambda r: (r.get('env') or {}).get('env_pollen_overall_avg') is not None)}, "
          f"tier {count(lambda r: r.get('tier'))}, granular {count(lambda r: r.get('granular'))}")
    miss = [s['schulnummer'] for s, r in results if s.get('description') is None and not r.get('embedding')]
    miss_t = [s['schulnummer'] for s, r in results if is_private(s) and not r.get('granular')]
    if miss or miss_t:
        print('INCOMPLETE descriptions:', miss, '| tuition:', miss_t)


if __name__ == '__main__':
    main()
