#!/usr/bin/env python3
"""
School Description Pipeline
============================
Three-pass pipeline to generate rich descriptions and extract structured data
for schools, using web research + LLM generation.

Pass 0 (--passes 0): Web research via Perplexity Sonar → raw research text
Pass 1 (--passes 1): LLM description generation → description_de + description_en
Pass 2 (--passes 2): Structured data extraction → fills empty columns (lehrer, website, etc.)

Usage:
    python school_description_pipeline.py --city frankfurt --school-type primary
    python school_description_pipeline.py --city frankfurt --school-type primary --passes 0,1
    python school_description_pipeline.py --city frankfurt --school-type primary --passes 2 --limit 5
    python school_description_pipeline.py --city frankfurt --school-type primary --force-rerun

Output:
    - Updates the school master table CSV/parquet in data_{city}/final/
    - Caches all API responses in data_{city}/cache/description_pipeline_{school_type}.json
    - Does NOT regenerate embeddings — run the embeddings generator separately after this

Author: SchoolNossa Pipeline
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Columns that Pass 2 can populate — mapped from JSON key → CSV column name
PASS2_COLUMN_MAP = {
    "website":                   "website",
    "gruendungsjahr":            "gruendungsjahr",
    "lehrer_2024_25":            "lehrer_2024_25",
    "lehrer_2023_24":            "lehrer_2023_24",
    "sprachen":                  "sprachen",
    "besonderheiten":            "besonderheiten",
    "ganztagsform":              "ganztagsform",
    "schueler_2024_25":          "schueler_2024_25",      # student count for 2024/25 from website
    "schueler_2023_24":          "schueler_2023_24",      # student count for 2023/24 from website
    "schueler_gesamt_web":       "schueler_gesamt_web",   # year-unknown count from website (fallback when year unclear)
    "nachfrage_plaetze_2025_26": "nachfrage_plaetze_2025_26",
    "nachfrage_wuensche_2025_26":"nachfrage_wuensche_2025_26",
    "nachfrage_plaetze_2024_25": "nachfrage_plaetze_2024_25",
    "nachfrage_wuensche_2024_25":"nachfrage_wuensche_2024_25",
    "migration_2024_25":         "migration_2024_25",
    "migration_2023_24":         "migration_2023_24",
}

# Nothing is cross-check-only anymore — all extracted values are written to their own columns
CROSS_CHECK_ONLY = set()


def load_config():
    config_path = SCRIPT_DIR / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


def get_api_keys(config):
    return {
        "openai":      config.get("api_keys", {}).get("openai")      or os.environ.get("OPENAI_API_KEY"),
        "perplexity":  config.get("api_keys", {}).get("perplexity")   or os.environ.get("PERPLEXITY_API_KEY"),
        "gemini":      config.get("api_keys", {}).get("gemini")        or os.environ.get("GEMINI_API_KEY"),
    }


def load_prompt(prompt_file):
    prompt_path = PROJECT_ROOT / "prompts" / prompt_file
    with open(prompt_path) as f:
        return f.read()


def find_input_csv(city, school_type):
    """Find the most complete input CSV for a city/school_type."""
    data_dir = PROJECT_ROOT / f"data_{city}" / "final"
    candidates = [
        data_dir / f"{city}_{school_type}_school_master_table_final.csv",
        data_dir / f"{city}_{school_type}_school_master_table.csv",
        data_dir / f"{city}_school_master_table_final.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"No input CSV found for {city}/{school_type} in {data_dir}")


def load_cache(cache_path):
    if cache_path.exists():
        with open(cache_path) as f:
            return json.load(f)
    return {}


def save_cache(cache, cache_path):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Pass 0 — Web Research (Perplexity Sonar)
# ---------------------------------------------------------------------------

def build_pass0_prompt(row, city):
    """Fill the Pass 0 prompt template with school data."""
    website_line = (
        f"The school's website is: {row.get('website', '')}"
        if pd.notna(row.get("website")) and row.get("website")
        else "The school does not have a known website."
    )

    def fmt(v):
        return str(v) if pd.notna(v) and v != "" else "Unknown"

    return f"""Research the school "{row['schulname']}" in {city}, Germany using Google Search.
{website_line}

Known data about this school:
- Name: {row['schulname']}
- Type: {fmt(row.get('school_type'))}
- Ownership: {fmt(row.get('traegerschaft'))} (public/private)
- District: {fmt(row.get('bezirk'))}
- Neighborhood: {fmt(row.get('ortsteil'))}
- Founded: {fmt(row.get('gruendungsjahr'))}
- Special features: {fmt(row.get('besonderheiten'))}
- Languages offered: {fmt(row.get('sprachen'))}
- Student count (2024/25): {fmt(row.get('schueler_gesamt'))}
- Teacher count (2024/25): {fmt(row.get('lehrer_2024_25'))}

INSTRUCTIONS:
1. Search the web for information about this school, especially from their website or any official sources.
2. Write a comprehensive, detailed, and up-to-date description of this school in ENGLISH.
3. Only include sections and details you can verify or reasonably infer. Omit sections where you have no information.
4. If the school has limited online presence, use the known data above to write as complete a description as possible.
5. Write in a professional, factual, parent-friendly tone. Aim for 400-800 words.
6. Output ONLY the description text, no headers or markdown formatting."""


def run_pass0_perplexity(prompt, api_key, model="sonar", delay=2.0):
    """Call Perplexity API for web-grounded research.

    Returns (content_text, citations_list) where citations_list is a list of
    source URLs Perplexity used. One of these is often the school's official website.
    """
    import urllib.request

    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.perplexity.ai/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    time.sleep(delay)
    content = result["choices"][0]["message"]["content"]
    citations = result.get("citations", [])
    return content, citations


def clean_url(url):
    """Strip citation markers like [1], [2] and trailing punctuation from URLs."""
    import re
    url = url.strip()
    # Remove citation markers like [1], [2][3], etc.
    url = re.sub(r"\[\d+\]+$", "", url)
    url = re.sub(r"(\[\d+\])+", "", url)
    # Strip trailing punctuation
    url = url.rstrip(".,;:)")
    return url


def find_school_website_from_citations(citations, school_name, city="Frankfurt"):
    """Identify the school's official website from Perplexity citation URLs.

    Filters out generic sites (Wikipedia, directories, social media, etc.)
    and returns the most likely official school domain.
    Only returns a URL if it looks like the school's OWN website (not a third-party listing).
    """
    if not citations:
        return None

    # Domains that are NOT official school websites (third-party listing/info sites)
    GENERIC_DOMAINS = {
        # General
        "wikipedia.org", "wikimedia.org", "wikidata.org",
        "google.com", "google.de", "maps.google.com",
        "facebook.com", "instagram.com", "twitter.com", "youtube.com",
        "linkedin.com", "xing.com",
        "openstreetmap.org",
        # German directories / info portals
        "dasoertliche.de", "gelbeseiten.de", "11880.com", "cylex.de",
        "meinestadt.de", "stadtbranchenbuch.com", "branchenbuch.de",
        "schul-info.de", "bildungsserver.de", "bildungsportal.de",
        "kek-online.de", "jedeschule.de", "schulen.de",
        # FOIA / transparency platforms
        "fragdenstaat.de", "informationsfreiheit.de",
        # State ministry / statistics portals (not school's own site)
        "schulministerium.nrw.de", "kultusministerium.hessen.de",
        "statistik.hessen.de", "schul-bw.de",
        "bildung.hessen.de",
        # City portals (generic - the SCHOOL subdomain schulportal.hessen.de is fine)
        "frankfurt.de", "berlin.de", "hamburg.de", "koeln.de", "muenchen.de",
        "duesseldorf.de", "stuttgart.de",
        # News / press
        "faz.net", "fr.de", "hessenschau.de", "spiegel.de", "zeit.de",
        "sueddeutsche.de", "welt.de",
    }

    import re
    from urllib.parse import urlparse

    scored = []
    for raw_url in citations:
        url = clean_url(raw_url)
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]

            # Skip if matches any generic domain
            if any(domain == g or domain.endswith("." + g) for g in GENERIC_DOMAINS):
                continue

            # Score: higher = more likely to be official school site
            score = 0
            if domain.endswith(".de"):
                score += 2

            # Frankfurt/Hessen official school portal subdomains (e.g., xyz.frankfurt.schule.hessen.de)
            if "schule.hessen.de" in domain or "schulportal.hessen.de" in domain:
                score += 5

            # School-type keywords in domain
            if any(k in domain for k in ["schule", "gymnasium", "grundschule", "gesamtschule",
                                          "realschule", "hauptschule", "waldorf", "montessori"]):
                score += 3

            # School name words in domain (strong signal)
            name_normalized = re.sub(r"[^a-z0-9]", "", school_name.lower())
            dom_normalized = re.sub(r"[^a-z0-9]", "", domain.lower())
            name_prefix = name_normalized[:8]
            if name_prefix and (name_prefix in dom_normalized):
                score += 4

            scored.append((score, url))
        except Exception:
            continue

    if not scored:
        return None

    scored.sort(key=lambda x: -x[0])
    best_score, best_url = scored[0]
    # Only return URL if it has a meaningful school-specific score (not just a random .de site)
    return best_url if best_score >= 3 else None


def run_website_search_perplexity(school_name, city, api_key, model="sonar", delay=2.0):
    """Targeted search: find only the official website URL for a school.

    Used as a fallback when Pass 0 + Pass 2 could not determine the website.
    Returns (website_url_or_None, citations_list).
    """
    import urllib.request

    prompt = (
        f'What is the official website URL of "{school_name}" in {city}, Germany? '
        f"Return only the URL — nothing else. If you cannot find it, return 'not found'."
    )
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}]
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.perplexity.ai/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    time.sleep(delay)
    content = result["choices"][0]["message"]["content"].strip()
    citations = result.get("citations", [])

    # Check if the model returned a URL directly (clean up citation markers first)
    import re
    url_match = re.search(r"https?://\S+", content)
    if url_match:
        url = clean_url(url_match.group(0))
        # Validate: should look like a real domain, not a generic one
        if url and "not found" not in content.lower():
            # Cross-check: run it through citation filter logic to avoid directories
            candidate = find_school_website_from_citations([url], school_name, city)
            if candidate:
                return candidate, citations

    # Fallback: check citations for the official site
    url_from_citations = find_school_website_from_citations(citations, school_name, city)
    return url_from_citations, citations


# ---------------------------------------------------------------------------
# Pass 1 — Description Generation (OpenAI)
# ---------------------------------------------------------------------------

def build_pass1_messages(row, city, raw_research):
    """Build the system + user messages for Pass 1."""
    school_type_label = "primary schools" if "grundschule" in str(row.get("school_type", "")).lower() else "secondary schools"

    system = f"""You are an expert educational content writer creating clean, informative school descriptions for parents searching for {school_type_label} in {city}.

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
- Focus on what makes this school special for families

Respond ONLY with valid JSON in this exact format:
{{"description_de": "German description here...", "description_en": "English description here..."}}"""

    def fmt(v):
        return str(v) if pd.notna(v) and str(v).strip() not in ("", "nan", "Unknown") else "not available"

    user = f"""Here is the raw research data for the school:

---
{raw_research}
---

Additional known facts:
- School name: {row['schulname']}
- School type: {fmt(row.get('school_type'))}
- City: {city}
- Ownership: {fmt(row.get('traegerschaft'))}
- Student count: {fmt(row.get('schueler_gesamt'))}
- Neighborhood: {fmt(row.get('ortsteil'))}

Generate the two descriptions now."""

    return system, user


def build_pass1_fallback_messages(row, city):
    """Fallback Pass 1 when no raw research is available."""
    system = """You are an expert educational content writer creating school descriptions for parents in Germany.

Based ONLY on the known data provided, write two short but informative school descriptions (German and English).
Do not invent facts or features not supported by the data.

Guidelines:
- Each description should be 3-6 sentences (80-150 words)
- Focus on verifiable facts: school type, size, location, ownership, transit access
- Mention any notable data points (diversity, special programs if known)
- Tone: professional, factual, parent-friendly
- German description must be native-quality German

Respond ONLY with valid JSON in this exact format:
{"description_de": "German description here...", "description_en": "English description here..."}"""

    def fmt(v):
        return str(v) if pd.notna(v) and str(v).strip() not in ("", "nan", "Unknown") else "not available"

    ts = row.get("transit_accessibility_score")
    transit = f"{int(ts)}/100" if pd.notna(ts) else "not available"

    user = f"""Known data about this school:
- Name: {row['schulname']}
- Type: {fmt(row.get('school_type'))}
- City: {city}, Germany
- Ownership: {fmt(row.get('traegerschaft'))}
- Address: {fmt(row.get('strasse'))}, {fmt(row.get('plz'))} {city}
- Neighborhood: {fmt(row.get('ortsteil'))}
- Student count: {fmt(row.get('schueler_gesamt'))}
- Migration background %: {fmt(row.get('migration_2024_25'))}
- Transit accessibility: {transit}
- Special features: {fmt(row.get('besonderheiten'))}
- Languages: {fmt(row.get('sprachen'))}

Generate the two descriptions now."""

    return system, user


def run_pass1_openai(system, user, api_key, model="gpt-4o-mini", delay=1.0):
    """Call OpenAI chat completions for description generation."""
    import urllib.request

    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user}
        ],
        "temperature": 0.7,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    time.sleep(delay)
    raw = result["choices"][0]["message"]["content"].strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    return json.loads(raw)


# ---------------------------------------------------------------------------
# Pass 2 — Structured Data Extraction (OpenAI)
# ---------------------------------------------------------------------------

def build_pass2_messages(row, city, raw_research, citations=None):
    system = """You are a precise data extraction assistant. Your task is to extract specific structured data points about a German school from web research text.

Rules:
- Extract ONLY data explicitly mentioned in the research — do NOT guess, infer, or estimate
- Use null for any field where the data is not found or uncertain
- Numeric fields must be integers or floats, never strings
- If conflicting values appear, use the most recent or most credible one
- For website: only return a URL if you are confident it is the correct school's official website"""

    citations_hint = ""
    if citations:
        citations_hint = f"\n\nSource URLs used in the research (one may be the school's official website):\n" + "\n".join(f"  - {u}" for u in citations[:10])

    user = f"""Based on the following web research about **{row['schulname']}** ({row.get('school_type', '')}) in {city}, Germany, extract the structured data below.

Research text:
---
{raw_research}
---{citations_hint}

Return a JSON object with exactly these fields (use null for any field not found):

{{
  "website": "<official school website URL or null>",
  "gruendungsjahr": <founding year as integer or null>,
  "lehrer_2024_25": <total teaching staff in 2024/25 as integer or null>,
  "lehrer_2023_24": <total teaching staff in 2023/24 as integer or null>,
  "sprachen": "<language offerings string, e.g. 'Englisch ab Klasse 1' or null>",
  "besonderheiten": "<special features comma-separated string or null>",
  "ganztagsform": "<one of: offen, gebunden, teilgebunden, or null>",
  "schueler_gesamt_web": <student count from web research as integer or null>,
  "nachfrage_plaetze_2025_26": <available enrollment spots 2025/26 as integer or null>,
  "nachfrage_wuensche_2025_26": <enrollment applications 2025/26 as integer or null>,
  "nachfrage_plaetze_2024_25": <available enrollment spots 2024/25 as integer or null>,
  "nachfrage_wuensche_2024_25": <enrollment applications 2024/25 as integer or null>,
  "migration_2024_25": <migration background % in 2024/25 as float or null>,
  "migration_2023_24": <migration background % in 2023/24 as float or null>
}}

Return ONLY the JSON object — no explanations, no markdown fences, no extra text."""

    return system, user


def run_pass2_openai(system, user, api_key, model="gpt-4o-mini", delay=1.0):
    """Call OpenAI for structured data extraction."""
    import urllib.request

    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user}
        ],
        "temperature": 0.0,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    time.sleep(delay)
    raw = result["choices"][0]["message"]["content"].strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    return json.loads(raw)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_school(row, city, passes, cache, api_keys, config, force_rerun):
    """Run all requested passes for a single school. Updates cache in-place."""
    school_id = str(row.get("schulnummer", row["schulname"]))
    if school_id not in cache:
        cache[school_id] = {}

    entry = cache[school_id]
    model_cfg = config.get("models", {})
    delay = config.get("rate_limits", {}).get("delay_between_requests", 2.0)

    # --- Pass 0: Web Research ---
    if 0 in passes:
        if "pass0_raw" not in entry or force_rerun:
            perplexity_key = api_keys.get("perplexity")
            if not perplexity_key:
                logger.warning(f"  [{school_id}] No Perplexity key — skipping Pass 0")
            else:
                prompt = build_pass0_prompt(row, city)
                try:
                    raw, citations = run_pass0_perplexity(
                        prompt, perplexity_key,
                        model=model_cfg.get("perplexity", "sonar"),
                        delay=delay
                    )
                    entry["pass0_raw"] = raw
                    entry["pass0_citations"] = citations
                    logger.info(f"  [{school_id}] Pass 0 complete ({len(raw)} chars, {len(citations)} citations)")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 0 failed: {e}")
        else:
            logger.debug(f"  [{school_id}] Pass 0 cached")

    # --- Pass 1: Description Generation ---
    if 1 in passes:
        if ("pass1_de" not in entry or "pass1_en" not in entry) or force_rerun:
            openai_key = api_keys.get("openai")
            if not openai_key:
                logger.warning(f"  [{school_id}] No OpenAI key — skipping Pass 1")
            else:
                raw_research = entry.get("pass0_raw")
                if raw_research:
                    system, user = build_pass1_messages(row, city, raw_research)
                else:
                    logger.debug(f"  [{school_id}] No Pass 0 data, using fallback for Pass 1")
                    system, user = build_pass1_fallback_messages(row, city)
                try:
                    result = run_pass1_openai(
                        system, user, openai_key,
                        model=model_cfg.get("openai", "gpt-4o-mini"),
                        delay=delay
                    )
                    entry["pass1_de"] = result.get("description_de", "")
                    entry["pass1_en"] = result.get("description_en", "")
                    logger.info(f"  [{school_id}] Pass 1 complete")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 1 failed: {e}")
        else:
            logger.debug(f"  [{school_id}] Pass 1 cached")

    # --- Pass 2: Structured Extraction ---
    if 2 in passes:
        if "pass2" not in entry or force_rerun:
            openai_key = api_keys.get("openai")
            raw_research = entry.get("pass0_raw")
            if not openai_key:
                logger.warning(f"  [{school_id}] No OpenAI key — skipping Pass 2")
            elif not raw_research:
                logger.debug(f"  [{school_id}] No Pass 0 data — skipping Pass 2")
            else:
                citations = entry.get("pass0_citations", [])
                system, user = build_pass2_messages(row, city, raw_research, citations=citations)
                try:
                    result = run_pass2_openai(
                        system, user, openai_key,
                        model=model_cfg.get("openai", "gpt-4o-mini"),
                        delay=delay
                    )
                    entry["pass2"] = result
                    # Log what was found
                    found = [k for k, v in result.items() if v is not None and k not in CROSS_CHECK_ONLY]
                    logger.info(f"  [{school_id}] Pass 2 complete. Found: {found}")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 2 failed: {e}")
        else:
            logger.debug(f"  [{school_id}] Pass 2 cached")

    # --- Website Fallback: targeted search for schools still missing a website ---
    # Runs automatically when Pass 2 is requested and website is still null.
    # Uses citations from Pass 0 first, then fires a targeted Perplexity search if needed.
    if 2 in passes:
        p2_website = (entry.get("pass2") or {}).get("website")
        if not p2_website and "website_fallback" not in entry:
            perplexity_key = api_keys.get("perplexity")
            if perplexity_key:
                # First: try to derive website from existing Pass 0 citations
                citations = entry.get("pass0_citations", [])
                if citations:
                    derived = find_school_website_from_citations(
                        citations, row["schulname"], city
                    )
                    if derived:
                        entry["website_fallback"] = derived
                        logger.info(f"  [{school_id}] Website from citations: {derived}")
                # Second: if still nothing, run a targeted website search
                if "website_fallback" not in entry:
                    try:
                        found_url, cit2 = run_website_search_perplexity(
                            row["schulname"], city, perplexity_key,
                            model=model_cfg.get("perplexity", "sonar"),
                            delay=delay
                        )
                        entry["website_fallback"] = found_url  # may be None
                        # Merge any new citations
                        all_cits = list(set(citations + cit2))
                        entry["pass0_citations"] = all_cits
                        if found_url:
                            logger.info(f"  [{school_id}] Website from targeted search: {found_url}")
                        else:
                            logger.debug(f"  [{school_id}] Website not found via targeted search")
                    except Exception as e:
                        logger.warning(f"  [{school_id}] Website search failed: {e}")

    return entry


def apply_cache_to_dataframe(df, cache, passes):
    """Write cached pipeline results back into the DataFrame."""
    updated_descriptions = 0
    updated_structured = 0

    for idx, row in df.iterrows():
        school_id = str(row.get("schulnummer", row["schulname"]))
        entry = cache.get(school_id, {})
        if not entry:
            continue

        # Apply Pass 1: descriptions
        if 1 in passes:
            if entry.get("pass1_de"):
                df.at[idx, "description"] = entry["pass1_de"]
                updated_descriptions += 1
            if entry.get("pass1_en"):
                if "description_en" not in df.columns:
                    df["description_en"] = None
                df.at[idx, "description_en"] = entry["pass1_en"]

        # Apply Pass 2: structured data
        if 2 in passes and "pass2" in entry:
            p2 = entry["pass2"]
            for json_key, col_name in PASS2_COLUMN_MAP.items():
                if json_key in CROSS_CHECK_ONLY:
                    continue
                val = p2.get(json_key)
                if val is None:
                    # For website: fall through to website_fallback below
                    continue
                # Add column if it doesn't exist in schema
                if col_name not in df.columns:
                    df[col_name] = None
                # Only write if the column is currently null/empty
                current = df.at[idx, col_name]
                if pd.isna(current) or current == "" or current is None:
                    df.at[idx, col_name] = val
                    updated_structured += 1

            # Website fallback: use citation-derived or targeted-search URL if Pass 2 returned null
            fallback_url = entry.get("website_fallback")
            if fallback_url:
                if "website" not in df.columns:
                    df["website"] = None
                current_website = df.at[idx, "website"]
                if pd.isna(current_website) or current_website == "" or current_website is None:
                    df.at[idx, "website"] = fallback_url
                    updated_structured += 1

    logger.info(f"Applied: {updated_descriptions} description updates, {updated_structured} structured field fills")
    return df


def save_results(df, city, school_type, output_dir):
    """Save updated DataFrame to CSV and update the embeddings parquet in-place.

    The description pipeline reads _final.csv (no embeddings).  To keep the
    _final_with_embeddings.parquet in sync we merge the new column values into
    the existing parquet — preserving the embedding vectors — rather than
    overwriting it with null-embedding data.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    base = f"{city}_{school_type}_school_master_table_final"

    # 1. Always save the CSV (no embeddings)
    csv_path = output_dir / f"{base}.csv"
    csv_df = df.drop(columns=["embedding"], errors="ignore")
    csv_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Saved CSV: {csv_path} ({len(csv_df)} rows)")

    # 2. Merge into the existing embeddings parquet in-place
    parquet_path = output_dir / f"{base}_with_embeddings.parquet"
    if parquet_path.exists():
        try:
            pq_df = pd.read_parquet(parquet_path)
            # Identify key column for joining
            key = "schulnummer" if "schulnummer" in df.columns else df.columns[0]
            # Columns we want to update (all non-key, non-embedding cols from df)
            update_cols = [c for c in csv_df.columns if c != key]
            # Set index for fast alignment
            pq_df = pq_df.set_index(key)
            csv_indexed = csv_df.set_index(key)
            for col in update_cols:
                if col in csv_indexed.columns:
                    if col not in pq_df.columns:
                        pq_df[col] = None
                    # Update only rows present in both
                    pq_df.loc[csv_indexed.index, col] = csv_indexed[col]
            pq_df = pq_df.reset_index()
            pq_df.to_parquet(parquet_path, index=False)
            logger.info(f"Updated parquet in-place: {parquet_path} ({len(pq_df)} rows, embeddings preserved)")
        except Exception as e:
            logger.warning(f"Could not update parquet in-place: {e}. Skipping parquet update.")
    elif "embedding" in df.columns and df["embedding"].notna().any():
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Saved new parquet: {parquet_path}")
    else:
        logger.info("No existing embeddings parquet found. Re-run the embeddings generator after this.")


def print_summary(df, cache, passes, school_type):
    print(f"\n{'='*65}")
    print(f"DESCRIPTION PIPELINE SUMMARY — {school_type.upper()}")
    print(f"{'='*65}")
    print(f"  Schools processed: {len(df)}")
    print(f"  Schools in cache:  {len(cache)}")

    if 0 in passes:
        p0 = sum(1 for e in cache.values() if e.get("pass0_raw"))
        print(f"  Pass 0 (research): {p0}/{len(df)} complete")

    if 1 in passes:
        p1 = sum(1 for e in cache.values() if e.get("pass1_de"))
        print(f"  Pass 1 (descriptions): {p1}/{len(df)} complete")
        if "description_en" in df.columns:
            en_count = df["description_en"].notna().sum()
            print(f"    description_en filled: {en_count}/{len(df)}")

    if 2 in passes:
        p2 = sum(1 for e in cache.values() if e.get("pass2"))
        print(f"  Pass 2 (structured): {p2}/{len(df)} complete")
        # Show fill rates for key columns
        for col in ["lehrer_2024_25", "schueler_2024_25", "schueler_2023_24", "schueler_gesamt_web", "website", "gruendungsjahr", "sprachen", "besonderheiten"]:
            if col in df.columns:
                n = df[col].notna().sum()
                print(f"    {col}: {n}/{len(df)} filled ({100*n/len(df):.0f}%)")

    print(f"{'='*65}")
    print("NOTE: Run the embeddings generator to regenerate embeddings with new descriptions.")
    print(f"{'='*65}\n")


def main():
    parser = argparse.ArgumentParser(description="School Description Pipeline")
    parser.add_argument("--city",        required=True, help="City name (e.g. frankfurt, berlin, hamburg)")
    parser.add_argument("--school-type", required=True, choices=["primary", "secondary"], help="School type")
    parser.add_argument("--passes",      default="0,1,2",
                        help="Comma-separated passes to run: 0=research, 1=descriptions, 2=structured (default: 0,1,2)")
    parser.add_argument("--limit",       type=int, default=None,
                        help="Limit to first N schools (for testing)")
    parser.add_argument("--force-rerun", action="store_true",
                        help="Re-run all passes even if cached results exist")
    args = parser.parse_args()

    passes = set(int(p.strip()) for p in args.passes.split(","))
    city = args.city.lower()
    school_type = args.school_type.lower()

    logger.info(f"Starting description pipeline: city={city}, type={school_type}, passes={sorted(passes)}")

    # Load config + API keys
    config = load_config()
    api_keys = get_api_keys(config)

    if 0 in passes and not api_keys.get("perplexity"):
        logger.warning("No Perplexity API key found. Pass 0 will be skipped.")
    if (1 in passes or 2 in passes) and not api_keys.get("openai"):
        logger.warning("No OpenAI API key found. Passes 1 and 2 will be skipped.")

    # Load input data
    csv_path = find_input_csv(city, school_type)
    logger.info(f"Loading: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"Limited to {args.limit} schools for testing")

    # Load cache
    cache_path = PROJECT_ROOT / f"data_{city}" / "cache" / f"description_pipeline_{school_type}.json"
    cache = load_cache(cache_path)
    logger.info(f"Cache loaded: {len(cache)} existing entries")

    # Process each school
    total = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        school_id = str(row.get("schulnummer", row["schulname"]))
        logger.info(f"[{i+1}/{total}] {row['schulname']} ({school_id})")
        process_school(row, city, passes, cache, api_keys, config, args.force_rerun)

        # Save cache every 10 schools
        if (i + 1) % 10 == 0:
            save_cache(cache, cache_path)
            logger.info(f"Cache saved ({i+1}/{total})")

    # Final cache save
    save_cache(cache, cache_path)
    logger.info("Cache saved.")

    # Apply results to DataFrame
    df = apply_cache_to_dataframe(df, cache, passes)

    # Save output
    output_dir = PROJECT_ROOT / f"data_{city}" / "final"
    save_results(df, city, school_type, output_dir)

    # Print summary
    print_summary(df, cache, passes, school_type)


if __name__ == "__main__":
    main()
