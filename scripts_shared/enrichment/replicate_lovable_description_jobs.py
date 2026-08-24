#!/usr/bin/env python3
"""Local replica of the Lovable admin enrichment jobs (research-school-descriptions,
generate-school-descriptions, regenerate-embeddings) in their 'missing only' mode.
Same prompts/models/params as the edge functions (studied 2026-08-23); results are
emitted as guarded fill-gaps SQL for the Lovable MCP SQL tool. Cached per school."""
import concurrent.futures as cf
import json, os, re, sys, time, urllib.request, urllib.error
from pathlib import Path

# Cache/output live under data_shared (gitignored); the template ships with the repo.
SCRATCH = Path('/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa/data_shared/cache/lovable_description_jobs')
SCRATCH.mkdir(parents=True, exist_ok=True)
CACHE = SCRATCH / "results"
CACHE.mkdir(exist_ok=True)
SQLDIR = SCRATCH / "sql"
SQLDIR.mkdir(exist_ok=True)
sys.path.insert(0, '/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa')
from dotenv import load_dotenv
load_dotenv('/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa/.env')
API_KEY = os.environ['GEMINI_API_KEY']
import scripts_shared.upload_to_supabase as up

CITY_NAME = {'muenchen': 'Munich', 'koeln': 'Cologne', 'duesseldorf': 'Düsseldorf',
             'berlin': 'Berlin', 'hamburg': 'Hamburg', 'stuttgart': 'Stuttgart',
             'bremen': 'Bremen', 'dresden': 'Dresden', 'frankfurt': 'Frankfurt', 'leipzig': 'Leipzig'}

TEMPLATE = open(Path(__file__).parent / 'lovable_description_template.txt', encoding='utf-8').read()

def gemini(model, body, timeout=300):
    req = urllib.request.Request(
        f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent',
        data=json.dumps(body).encode(), method='POST',
        headers={'Content-Type': 'application/json', 'x-goog-api-key': API_KEY})
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.load(resp)

def gen_text(data):
    parts = data.get('candidates', [{}])[0].get('content', {}).get('parts', [])
    return ''.join(p.get('text', '') for p in parts).strip()

def research(s):
    city = CITY_NAME.get(s.get('city') or '', s.get('city') or 'Berlin')
    ttype = 'private' if any(k in (s.get('traegerschaft') or '').lower() for k in ('privat', 'frei')) else 'public'
    prompt = f'''Research the school "{s['schulname']}" in {city}, Germany using Google Search.
{f"The school's website is: {s['website']}" if s.get('website') else "The school does not have a known website."}

Known data about this school:
- Name: {s['schulname']}
- Type: {s.get('school_type') or 'Unknown'}
- Ownership: {s.get('traegerschaft') or 'Unknown'} ({ttype})
- District: {s.get('bezirk') or 'Unknown'}
- Neighborhood: {s.get('ortsteil') or 'Unknown'}
- Founded: {s.get('gruendungsjahr') or 'Unknown'}
- Special features: {s.get('besonderheiten') or 'None listed'}
- Languages offered: {s.get('sprachen') or 'Not specified'}
- Student count (2024/25): {s.get('schueler_2024_25') or 'Unknown'}
- Teacher count (2024/25): {s.get('lehrer_2024_25') or 'Unknown'}

INSTRUCTIONS:
1. Search the web for information about this school, especially from {f"their website ({s['website']})" if s.get('website') else 'any official sources'}.
2. Write a comprehensive, detailed, and up-to-date description of this school in ENGLISH following the template structure below.
3. Only include sections and details you can verify or reasonably infer. Omit sections where you have no information rather than guessing.
4. If the school has limited online presence, use the known data above to write as complete a description as possible.
5. Write in a professional, factual, parent-friendly tone.
6. The description should be rich and informative — aim for 400-800 words.

TEMPLATE (use this structure, adapt sections based on available information):

{TEMPLATE}

Write the description now. Output ONLY the description text, no headers like "Description:" or markdown formatting.'''
    body = {'contents': [{'parts': [{'text': prompt}]}],
            'generationConfig': {'temperature': 0.7, 'maxOutputTokens': 8192},
            'tools': [{'googleSearch': {}}]}
    for model in ('gemini-3.1-pro-preview', 'gemini-3-flash-preview'):
        for attempt in range(3):
            try:
                text = gen_text(gemini(model, body))
                if len(text) > 200:
                    return text
            except urllib.error.HTTPError as e:
                msg = e.read().decode()[:200]
                if e.code == 429:
                    time.sleep(15); continue
                if e.code == 404:
                    print(f'  research model {model} unavailable: {msg[:120]}', flush=True)
                    break
                print(f'  research {s["schulname"][:30]}: HTTP {e.code} {msg}', flush=True)
            except Exception as e:
                print(f'  research {s["schulname"][:30]}: {e}', flush=True)
            time.sleep(2 * (attempt + 1))
    return None

def generate_de_en(s, is_secondary):
    city = CITY_NAME.get(s.get('city') or '', 'Berlin')
    label = 'secondary schools' if is_secondary else 'primary schools'
    raw = s.get('description')
    has_raw = raw and raw.strip() and not raw.startswith('[RESEARCH_FAILED')
    if has_raw:
        system = f'''You are an expert educational content writer creating clean, informative school descriptions for parents searching for {label} in {city}.

Your task: Clean up the raw description data and generate TWO polished descriptions (German and English).

The raw data may contain:
- JSON schemas or technical formatting - REMOVE these completely
- Source references, URLs, or citation markers - REMOVE these
- Excessive formatting, brackets, or special characters - CLEAN these up
- Incomplete sentences or fragments - COMPLETE them naturally

Guidelines:
- Each description should be 5-10 sentences (150-300 words)
- Extract and highlight key strengths, programs, and unique features
- Cover educational philosophy, curriculum highlights, extracurricular offerings, and campus environment
- Mention notable achievements, special programs, language offerings, and community aspects
- Make the text natural, detailed, and parent-friendly
- German description should be native-quality German, not a translation
- English description should be fluent English
- DO NOT include any JSON, technical notation, or source references
- Focus on what makes this school special for families and provide a comprehensive overview

Respond ONLY with valid JSON in this exact format:
{{"description_de": "German description here...", "description_en": "English description here..."}}'''
        user = f'''Clean up and translate this raw school description for "{s['schulname']}" ({s.get('school_type') or ('Secondary School' if is_secondary else 'Primary School')} in {s.get('bezirk') or city}):

Raw description:
{raw}

Additional context:
- District: {s.get('bezirk') or 'Unknown'}
- Neighborhood: {s.get('ortsteil') or 'Unknown'}
- Languages: {s.get('sprachen') or 'Not specified'}'''
    else:
        system = f'''You are an expert educational content writer creating concise, informative school descriptions for parents searching for {label} in {city}.

Your task: Generate TWO descriptions (German and English) for each school.

Guidelines:
- Each description should be 5-10 sentences (150-300 words)
- Highlight key strengths, unique features, and educational approach
- Include relevant location/accessibility info and neighborhood context
- Mention language offerings, extracurricular activities, and special programs
- Cover the school community, facilities, and overall atmosphere
- Be factual but engaging and comprehensive
- Use natural, parent-friendly language
- German description should be native-quality, not a translation

Respond ONLY with valid JSON in this exact format:
{{"description_de": "German description here...", "description_en": "English description here..."}}'''
        user = f"Generate descriptions for this school:\n\nSchool Name: {s['schulname']}\nType: {s.get('school_type') or 'Unknown'}\nDistrict: {s.get('bezirk') or 'Unknown'}\nNeighborhood: {s.get('ortsteil') or 'Unknown'}\nOwnership: {s.get('traegerschaft') or 'Unknown'}"
    body = {'contents': [{'parts': [{'text': user}]}],
            'systemInstruction': {'parts': [{'text': system}]},
            'generationConfig': {'temperature': 0.7, 'responseMimeType': 'application/json'}}
    for model in ('gemini-3-flash-preview', 'gemini-2.5-flash'):
        for attempt in range(3):
            try:
                out = json.loads(gen_text(gemini(model, body, timeout=120)))
                if out.get('description_de') and out.get('description_en'):
                    return out
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(10); continue
                if e.code == 404:
                    break
                print(f'  generate {s["schulname"][:30]}: HTTP {e.code}', flush=True)
            except Exception as e:
                print(f'  generate {s["schulname"][:30]}: {e}', flush=True)
            time.sleep(2 * (attempt + 1))
    return None

def embed(text):
    body = {'model': 'models/gemini-embedding-001', 'taskType': 'RETRIEVAL_DOCUMENT',
            'content': {'parts': [{'text': text}]}, 'output_dimensionality': 768}
    req = urllib.request.Request(
        'https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent',
        data=json.dumps(body).encode(), method='POST',
        headers={'Content-Type': 'application/json', 'x-goog-api-key': API_KEY})
    for attempt in range(3):
        try:
            return json.load(urllib.request.urlopen(req, timeout=60))['embedding']['values']
        except Exception as e:
            print(f'  embed: {e}', flush=True); time.sleep(3 * (attempt + 1))
    return None

def fetch(table, filt, cols):
    rows, off = [], 0
    while True:
        d = up.supabase_request('GET', f'{table}?select={cols}&{filt}&offset={off}&limit=1000')
        rows += d
        if len(d) < 1000: break
        off += 1000
    return rows

COLS = 'id,city,schulnummer,schulname,school_type,bezirk,ortsteil,traegerschaft,website,gruendungsjahr,besonderheiten,sprachen,schueler_2024_25,lehrer_2024_25,description,description_de,description_en'

def process(job):
    table, s = job
    cpath = CACHE / f"{s['id']}.json"
    res = json.loads(cpath.read_text()) if cpath.exists() else {}
    if s.get('description') is None and 'description' not in res:
        d = research(s)
        if d: res['description'] = d
    raw = s.get('description') or res.get('description')
    if raw and (s.get('description_de') is None or s.get('description_en') is None) and 'description_de' not in res:
        de_en = generate_de_en(dict(s, description=raw), table == 'schools')
        if de_en: res.update(de_en)
    text_for_embedding = raw
    if text_for_embedding and 'embedding' not in res:
        v = embed(text_for_embedding)
        if v: res['embedding'] = [round(x, 6) for x in v]
    cpath.write_text(json.dumps(res, ensure_ascii=False))
    print(f"done {table[:3]} {s['city']}/{s['schulnummer']}: " +
          ','.join(k for k in ('description','description_de','embedding') if k in res), flush=True)
    return (table, s, res)

def dollar(text):
    for tag in ('$d1$', '$d2$', '$d3$'):
        if tag not in text: return f'{tag}{text}{tag}'
    return "'" + text.replace("'", "''") + "'"

def main():
    jobs = []
    for table in ('schools', 'primary_schools'):
        need = fetch(table, 'or=(description.is.null,description_de.is.null,description_en.is.null)', COLS)
        emb_missing = {r['id'] for r in fetch(table, 'embedding=is.null&description=not.is.null', 'id')}
        seen = set()
        for r in need:
            jobs.append((table, r)); seen.add(r['id'])
        for rid in emb_missing - seen:
            row = fetch(table, f'id=eq.{rid}', COLS)
            if row: jobs.append((table, row[0]))
    print(f'{len(jobs)} schools to process', flush=True)
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        results = list(ex.map(process, jobs))
    # Emit SQL
    stmts = []
    for table, s, res in results:
        sets, guards = [], []
        if 'description' in res and s.get('description') is None:
            sets.append(f"description = COALESCE(description, {dollar(res['description'])})")
            sets.append("description_researched_at = COALESCE(description_researched_at, now())")
        if 'description_de' in res:
            sets.append(f"description_de = COALESCE(description_de, {dollar(res['description_de'])})")
            sets.append(f"description_en = COALESCE(description_en, {dollar(res['description_en'])})")
        if 'embedding' in res:
            vec = '[' + ','.join(repr(x) for x in res['embedding']) + ']'
            sets.append(f"embedding = COALESCE(embedding, '{vec}'::vector)")
        if sets:
            stmts.append(f"UPDATE {table} SET {', '.join(sets)} WHERE id = '{s['id']}';")
    for f in SQLDIR.glob('*.sql'): f.unlink()
    chunk, size, idx = [], 0, 1
    for st in stmts:
        chunk.append(st); size += len(st)
        if size > 45000:
            (SQLDIR / f'enrich_{idx:02d}.sql').write_text('\n'.join(chunk), encoding='utf-8'); chunk, size = [], 0; idx += 1
    if chunk:
        (SQLDIR / f'enrich_{idx:02d}.sql').write_text('\n'.join(chunk), encoding='utf-8')
    ok = sum(1 for _, s, r in results if 'embedding' in r)
    print(f'SQL: {idx} file(s) in {SQLDIR}; {len(stmts)} row updates; {ok} embeddings', flush=True)
    missing = [(t, s['city'], s['schulnummer'], sorted(set(['description','description_de','embedding']) - set(r))) for t, s, r in results if len(r) < 3 and s.get('description') is None]
    if missing:
        print('INCOMPLETE:', missing, flush=True)

if __name__ == '__main__':
    main()
