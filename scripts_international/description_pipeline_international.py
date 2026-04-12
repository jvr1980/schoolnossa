#!/usr/bin/env python3
"""
International School Description Pipeline

Adapts the shared description pipeline (scripts_shared/generation/school_description_pipeline.py)
for international countries. Country-specific prompts, column mappings, and model config.

Uses o4-mini with reasoning (thinking) enabled for all passes by default.

This is a standard phase in ALL international pipelines — not optional.

Passes:
    0: Web research via Perplexity Sonar → raw research text per school
    1: o4-mini description generation → description_local + description_en
    2: o4-mini structured data extraction → fills gaps (students, teachers, website, etc.)

Usage:
    python description_pipeline_international.py --country NL --passes 0,1,2
    python description_pipeline_international.py --country GB --passes 0,1,2 --limit 10
    python description_pipeline_international.py --country NL --passes 2  # extraction only

Output:
    Updates data_{country_code}/intermediate/ or final/ CSV with descriptions and extracted data.
    Caches all API responses in data_{country_code}/cache/description_pipeline.json
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.schema.country_extensions import (
    COUNTRY_NAMES, COUNTRY_LANGUAGES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Default model config — o4-mini with reasoning for all OpenAI calls
# o4-mini is OpenAI's latest cost-effective reasoning model (supports reasoning.effort)
# o4-mini is not available; o4-mini is the closest match for mini + thinking
DEFAULT_MODELS = {
    "openai": "o4-mini",
    "perplexity": "sonar",
}

# Columns that Pass 2 can populate for international schools
# Mapped from JSON key → core schema column name
PASS2_COLUMN_MAP_INTERNATIONAL = {
    "website":              "website",
    "founding_year":        "founding_year",
    "teachers_current":     "teachers_current",
    "teachers_previous":    "teachers_previous",
    "students_current":     "students_current",
    "students_previous":    "students_previous",
    "languages_offered":    "languages_offered",
    "special_features":     "special_features",
    "principal":            "principal",
    "phone":                "phone",
    "email":                "email",
}

# Country-specific school type labels for prompts
SCHOOL_TYPE_LABELS = {
    "NL": "secondary schools (voortgezet onderwijs)",
    "GB": "secondary schools",
    "FR": "secondary schools (lycées and collèges)",
    "IT": "secondary schools (scuole superiori)",
    "ES": "secondary schools (institutos de educación secundaria)",
    "DE": "secondary schools (Gymnasien and Sekundarschulen)",
}


def load_api_keys():
    """Load API keys from config.yaml (preferred) then .env / environment."""
    keys = {"openai": None, "perplexity": None}

    # 1. Try config.yaml (same as shared pipeline)
    for config_path in [
        PROJECT_ROOT / "config.yaml",
        PROJECT_ROOT / "scripts_shared" / "generation" / "config.yaml",
    ]:
        if config_path.exists():
            try:
                import yaml
                with open(config_path) as f:
                    cfg = yaml.safe_load(f) or {}
                api_keys_cfg = cfg.get("api_keys", {})
                keys["openai"] = api_keys_cfg.get("openai") or keys["openai"]
                keys["perplexity"] = api_keys_cfg.get("perplexity") or keys["perplexity"]
                # Also load model overrides from config
                models_cfg = cfg.get("models", {})
                if models_cfg.get("openai"):
                    # Keep o4-mini as default unless user overrides
                    pass
                logger.info(f"  Loaded API keys from {config_path.name}")
            except Exception as e:
                logger.debug(f"  Could not load {config_path}: {e}")

    # 2. Override with environment / .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    keys["openai"] = os.environ.get("OPENAI_API_KEY") or keys["openai"]
    keys["perplexity"] = os.environ.get("PERPLEXITY_API_KEY") or keys["perplexity"]

    return keys


def call_openai_with_thinking(system: str, user: str, api_key: str,
                               model: str = "o4-mini",
                               temperature: float = 0.7,
                               delay: float = 1.0) -> str:
    """
    Call OpenAI API with reasoning model (o4-mini, o3-mini, etc.).

    For reasoning models (o3/o4 family): uses developer role + reasoning_effort.
    For standard models (gpt-4o, gpt-5.x): uses system role + temperature.
    """
    import urllib.request

    is_reasoning_model = any(prefix in model for prefix in ["o3", "o4"])

    if is_reasoning_model:
        # Reasoning models use "developer" role instead of "system"
        # and reasoning_effort instead of temperature
        payload = {
            "model": model,
            "messages": [
                {"role": "developer", "content": system},
                {"role": "user", "content": user},
            ],
            "reasoning_effort": "medium",
        }
    else:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
        }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    time.sleep(delay)
    raw = result["choices"][0]["message"]["content"].strip()

    # Strip markdown fences
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    return raw


def call_perplexity(prompt: str, api_key: str, model: str = "sonar",
                    delay: float = 2.0) -> tuple:
    """Call Perplexity API for web research. Returns (content, citations)."""
    import urllib.request

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
    content = result["choices"][0]["message"]["content"]
    citations = result.get("citations", [])
    return content, citations


# ===========================================================================
# Prompt Builders (country-agnostic)
# ===========================================================================

def build_pass0_prompt(row: dict, country_code: str) -> str:
    """Build web research prompt for any country."""
    country_name = COUNTRY_NAMES.get(country_code, country_code)
    city = row.get("city") or row.get("gemeente_name") or row.get("town") or row.get("local_authority") or ""

    website_line = (
        f"The school's website is: {row.get('website', '')}"
        if row.get("website") and str(row.get("website")) != "nan"
        else "The school does not have a known website."
    )

    def fmt(v):
        return str(v) if v and str(v) not in ("", "nan", "None") else "Unknown"

    return f"""Research the school "{row['school_name']}" in {city}, {country_name} using web search.
{website_line}

Known data about this school:
- Name: {row['school_name']}
- Type: {fmt(row.get('school_type_national', row.get('school_type')))}
- Ownership: {fmt(row.get('ownership_national', row.get('ownership')))}
- Region: {fmt(row.get('region', row.get('province')))}
- City/Town: {fmt(city)}
- Students: {fmt(row.get('students_current'))}
- Teachers: {fmt(row.get('teachers_current'))}
- Languages: {fmt(row.get('languages_offered'))}

INSTRUCTIONS:
1. Search the web for information about this school, especially from their website.
2. Write a comprehensive, factual, parent-friendly description in ENGLISH.
3. Only include verifiable information. Omit sections where you have no data.
4. Cover: educational approach, programs, languages, extracurriculars, campus, community.
5. Aim for 400-800 words. Output ONLY the description text."""


def build_pass1_prompt(row: dict, country_code: str, raw_research: str) -> tuple:
    """Build description generation prompt. Returns (system, user)."""
    country_name = COUNTRY_NAMES.get(country_code, country_code)
    lang_code = COUNTRY_LANGUAGES.get(country_code, "en")
    school_type_label = SCHOOL_TYPE_LABELS.get(country_code, "secondary schools")
    city = row.get("city") or row.get("town") or row.get("gemeente_name") or ""

    # Language names for prompts
    LANG_NAMES = {"nl": "Dutch", "en": "English", "fr": "French", "it": "Italian", "es": "Spanish", "de": "German"}
    local_lang = LANG_NAMES.get(lang_code, "local language")

    system = f"""You are an expert educational content writer creating school descriptions for parents searching for {school_type_label} in {country_name}.

Generate TWO polished descriptions from the raw research data:
1. A description in {local_lang} (native quality, not a translation)
2. A description in English

Guidelines:
- Each description: 5-10 sentences (150-300 words)
- Cover: educational philosophy, curriculum, extracurriculars, campus, community
- Professional, factual, parent-friendly tone
- Do NOT include JSON, URLs, citations, or markdown formatting

Respond ONLY with valid JSON:
{{"description_local": "{local_lang} description here...", "description_en": "English description here..."}}"""

    def fmt(v):
        return str(v) if v and str(v) not in ("", "nan", "None") else "not available"

    user = f"""Raw research about the school:
---
{raw_research}
---

Additional facts:
- School name: {row['school_name']}
- Type: {fmt(row.get('school_type_national'))}
- City: {city}, {country_name}
- Ownership: {fmt(row.get('ownership_national'))}
- Students: {fmt(row.get('students_current'))}

Generate the two descriptions now."""

    return system, user


def build_pass1_fallback_prompt(row: dict, country_code: str) -> tuple:
    """Fallback Pass 1 when no raw research is available."""
    country_name = COUNTRY_NAMES.get(country_code, country_code)
    lang_code = COUNTRY_LANGUAGES.get(country_code, "en")
    LANG_NAMES = {"nl": "Dutch", "en": "English", "fr": "French", "it": "Italian", "es": "Spanish", "de": "German"}
    local_lang = LANG_NAMES.get(lang_code, "local language")

    system = f"""You are an expert educational content writer creating school descriptions for parents in {country_name}.
Based ONLY on the known data provided, write two short but informative descriptions ({local_lang} and English).
Do not invent facts. Each description: 3-6 sentences (80-150 words).

Respond ONLY with valid JSON:
{{"description_local": "{local_lang} description...", "description_en": "English description..."}}"""

    def fmt(v):
        return str(v) if v and str(v) not in ("", "nan", "None") else "not available"

    city = row.get("city") or row.get("town") or ""
    user = f"""Known data:
- Name: {row['school_name']}
- Type: {fmt(row.get('school_type_national'))}
- City: {city}, {country_name}
- Ownership: {fmt(row.get('ownership_national'))}
- Students: {fmt(row.get('students_current'))}
- Teachers: {fmt(row.get('teachers_current'))}
- Website: {fmt(row.get('website'))}

Generate descriptions now."""

    return system, user


def build_pass2_prompt(row: dict, country_code: str, raw_research: str,
                       citations: list = None) -> tuple:
    """Build structured extraction prompt."""
    country_name = COUNTRY_NAMES.get(country_code, country_code)
    city = row.get("city") or row.get("town") or ""

    system = """You are a precise data extraction assistant. Extract specific structured data from web research about a school.

Rules:
- Extract ONLY data explicitly mentioned — do NOT guess
- Use null for any field not found
- Numeric fields: integers or floats, never strings
- website: only if confident it's the school's official site"""

    citations_hint = ""
    if citations:
        citations_hint = "\n\nSource URLs:\n" + "\n".join(f"  - {u}" for u in citations[:10])

    user = f"""Extract data about **{row['school_name']}** in {city}, {country_name}:

Research text:
---
{raw_research}
---{citations_hint}

Return JSON with exactly these fields (null if not found):
{{
  "website": "<official website URL or null>",
  "founding_year": <integer or null>,
  "teachers_current": <total teaching staff as integer or null>,
  "teachers_previous": <previous year teaching staff or null>,
  "students_current": <current student count as integer or null>,
  "students_previous": <previous year student count or null>,
  "languages_offered": "<comma-separated language list or null>",
  "special_features": "<key features comma-separated or null>",
  "principal": "<head of school name or null>",
  "phone": "<phone number or null>",
  "email": "<email address or null>"
}}"""

    return system, user


# ===========================================================================
# Pipeline Core
# ===========================================================================

def process_school(row: dict, country_code: str, passes: set, cache: dict,
                   api_keys: dict, force_rerun: bool = False) -> dict:
    """Process a single school through all requested passes."""
    school_id = str(row.get("school_id", row.get("school_name", "unknown")))
    if school_id not in cache:
        cache[school_id] = {}
    entry = cache[school_id]

    # --- Pass 0: Web Research ---
    if 0 in passes:
        if "pass0_raw" not in entry or force_rerun:
            pkey = api_keys.get("perplexity")
            if pkey:
                prompt = build_pass0_prompt(row, country_code)
                try:
                    raw, citations = call_perplexity(prompt, pkey, model=DEFAULT_MODELS["perplexity"])
                    entry["pass0_raw"] = raw
                    entry["pass0_citations"] = citations
                    logger.info(f"  [{school_id}] Pass 0: {len(raw)} chars, {len(citations)} citations")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 0 failed: {e}")

    # --- Pass 1: Description Generation ---
    if 1 in passes:
        if ("pass1_local" not in entry or "pass1_en" not in entry) or force_rerun:
            okey = api_keys.get("openai")
            if okey:
                raw_research = entry.get("pass0_raw")
                if raw_research:
                    system, user = build_pass1_prompt(row, country_code, raw_research)
                else:
                    system, user = build_pass1_fallback_prompt(row, country_code)
                try:
                    raw_resp = call_openai_with_thinking(
                        system, user, okey, model=DEFAULT_MODELS["openai"]
                    )
                    result = json.loads(raw_resp)
                    entry["pass1_local"] = result.get("description_local", "")
                    entry["pass1_en"] = result.get("description_en", "")
                    logger.info(f"  [{school_id}] Pass 1: descriptions generated")
                except json.JSONDecodeError as e:
                    logger.warning(f"  [{school_id}] Pass 1 JSON parse failed: {e}")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 1 failed: {e}")

    # --- Pass 2: Structured Extraction ---
    if 2 in passes:
        if "pass2" not in entry or force_rerun:
            okey = api_keys.get("openai")
            raw_research = entry.get("pass0_raw")
            if okey and raw_research:
                citations = entry.get("pass0_citations", [])
                system, user = build_pass2_prompt(row, country_code, raw_research, citations)
                try:
                    raw_resp = call_openai_with_thinking(
                        system, user, okey, model=DEFAULT_MODELS["openai"],
                        temperature=0.0
                    )
                    result = json.loads(raw_resp)
                    entry["pass2"] = result
                    found = [k for k, v in result.items() if v is not None]
                    logger.info(f"  [{school_id}] Pass 2: found {found}")
                except json.JSONDecodeError as e:
                    logger.warning(f"  [{school_id}] Pass 2 JSON parse failed: {e}")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 2 failed: {e}")

    return entry


def apply_cache_to_dataframe(df: pd.DataFrame, cache: dict, passes: set,
                             id_col: str = "school_id") -> pd.DataFrame:
    """Write cached results back into DataFrame."""
    updated_desc = 0
    updated_struct = 0

    for idx, row in df.iterrows():
        school_id = str(row.get(id_col, row.get("school_name", "")))
        entry = cache.get(school_id, {})
        if not entry:
            continue

        # Apply Pass 1: descriptions
        if 1 in passes:
            if entry.get("pass1_local"):
                if "description_local" not in df.columns:
                    df["description_local"] = None
                df.at[idx, "description_local"] = entry["pass1_local"]
                updated_desc += 1
            if entry.get("pass1_en"):
                if "description" not in df.columns:
                    df["description"] = None
                df.at[idx, "description"] = entry["pass1_en"]

        # Apply Pass 2: structured data
        if 2 in passes and "pass2" in entry:
            p2 = entry["pass2"]
            for json_key, col_name in PASS2_COLUMN_MAP_INTERNATIONAL.items():
                val = p2.get(json_key)
                if val is None:
                    continue
                if col_name not in df.columns:
                    df[col_name] = None
                current = df.at[idx, col_name]
                if pd.isna(current) or current == "" or current is None:
                    df.at[idx, col_name] = val
                    updated_struct += 1

    logger.info(f"Applied: {updated_desc} descriptions, {updated_struct} structured fields")
    return df


# ===========================================================================
# Main
# ===========================================================================

def find_input_csv(country_code: str) -> Path:
    """Find the best input CSV for a country."""
    code = country_code.lower()
    data_dir = PROJECT_ROOT / f"data_{code}"

    candidates = [
        data_dir / "final" / f"{code}_school_master_table_final.csv",
        data_dir / "intermediate" / f"{code}_schools_with_demographics.csv",
        data_dir / "intermediate" / f"{code}_schools_with_crime.csv",
        data_dir / "intermediate" / f"{code}_schools_with_transit.csv",
        data_dir / "intermediate" / f"{code}_school_master_geocoded.csv",
        data_dir / "intermediate" / f"{code}_school_master_base.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"No input data found for {country_code} in {data_dir}")


def run_description_pipeline(country_code: str, passes: set = None,
                              limit: int = None, force_rerun: bool = False):
    """Run the full description pipeline for a country."""
    if passes is None:
        passes = {0, 1, 2}

    code = country_code.upper()
    logger.info(f"{'='*60}")
    logger.info(f"International Description Pipeline — {COUNTRY_NAMES.get(code, code)}")
    logger.info(f"Model: {DEFAULT_MODELS['openai']} (with thinking)")
    logger.info(f"Passes: {sorted(passes)}")
    logger.info(f"{'='*60}")

    api_keys = load_api_keys()
    if 0 in passes and not api_keys.get("perplexity"):
        logger.warning("No PERPLEXITY_API_KEY — Pass 0 will be skipped")
    if (1 in passes or 2 in passes) and not api_keys.get("openai"):
        logger.warning("No OPENAI_API_KEY — Passes 1+2 will be skipped")

    # Load input
    csv_path = find_input_csv(code)
    logger.info(f"Loading: {csv_path.name}")
    df = pd.read_csv(csv_path, low_memory=False)
    if limit:
        df = df.head(limit)
        logger.info(f"Limited to {limit} schools")

    # Determine school ID column
    id_col = "school_id"
    if id_col not in df.columns:
        # Fallback for intermediate files that haven't been schema-transformed yet
        for fallback in ["vestiging_code", "urn", "school_name"]:
            if fallback in df.columns:
                id_col = fallback
                break

    # Load cache
    cache_dir = PROJECT_ROOT / f"data_{code.lower()}" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "description_pipeline.json"
    cache = {}
    if cache_path.exists():
        with open(cache_path) as f:
            cache = json.load(f)
    logger.info(f"Cache: {len(cache)} existing entries")

    # Process
    total = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        row_dict = row.to_dict()
        # Normalize school_name for the prompt
        if "school_name" not in row_dict:
            row_dict["school_name"] = row_dict.get("schulname", row_dict.get("school_name", "Unknown"))

        school_id = str(row_dict.get(id_col, row_dict.get("school_name", "")))
        logger.info(f"[{i+1}/{total}] {row_dict.get('school_name', school_id)}")
        process_school(row_dict, code, passes, cache, api_keys, force_rerun)

        if (i + 1) % 10 == 0:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            logger.info(f"  Cache saved ({i+1}/{total})")

    # Final cache save
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    # Apply to DataFrame
    df = apply_cache_to_dataframe(df, cache, passes, id_col=id_col)

    # Save
    output_path = csv_path  # Overwrite the input file
    df.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"DESCRIPTION PIPELINE SUMMARY — {COUNTRY_NAMES.get(code, code)}")
    logger.info(f"{'='*60}")
    logger.info(f"  Schools: {total}")
    if 0 in passes:
        p0 = sum(1 for e in cache.values() if e.get("pass0_raw"))
        logger.info(f"  Pass 0 (research): {p0}/{total}")
    if 1 in passes:
        p1 = sum(1 for e in cache.values() if e.get("pass1_local"))
        logger.info(f"  Pass 1 (descriptions): {p1}/{total}")
    if 2 in passes:
        p2 = sum(1 for e in cache.values() if e.get("pass2"))
        logger.info(f"  Pass 2 (extraction): {p2}/{total}")
        for col in ["students_current", "teachers_current", "website", "languages_offered"]:
            if col in df.columns:
                n = df[col].notna().sum()
                logger.info(f"    {col}: {n}/{total} ({100*n/total:.0f}%)")


def main():
    parser = argparse.ArgumentParser(description="International School Description Pipeline")
    parser.add_argument("--country", required=True, help="Country code (NL, GB, FR, IT, ES)")
    parser.add_argument("--passes", default="0,1,2", help="Passes to run: 0,1,2")
    parser.add_argument("--limit", type=int, help="Limit to N schools (testing)")
    parser.add_argument("--force-rerun", action="store_true", help="Ignore cache")
    args = parser.parse_args()

    passes = set(int(p.strip()) for p in args.passes.split(","))
    run_description_pipeline(args.country, passes=passes, limit=args.limit, force_rerun=args.force_rerun)


if __name__ == "__main__":
    main()
