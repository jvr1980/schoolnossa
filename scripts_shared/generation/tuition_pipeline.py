#!/usr/bin/env python3
"""
Tuition Fee Pipeline
====================
Three-pass pipeline to research, structure, and verify tuition fee data
for private schools. Only runs on schools where traegerschaft contains
'privat' or 'frei'. Public schools are left untouched.

Pass 1 (--passes 1): Gemini + Google Search → tuition_tier + tuition_monthly_eur
Pass 2 (--passes 2): Gemini + Google Search → 12-bracket income matrix + sibling discounts
Pass 3 (--passes 3): GPT-5.2 Responses API + web_search → verify flat matrices,
                      set income_based_tuition flag

Usage:
    python tuition_pipeline.py --city frankfurt --school-type secondary
    python tuition_pipeline.py --city frankfurt --school-type primary
    python tuition_pipeline.py --city frankfurt --school-type secondary --passes 1,2
    python tuition_pipeline.py --city frankfurt --school-type secondary --passes 3
    python tuition_pipeline.py --city frankfurt --school-type secondary --limit 5
    python tuition_pipeline.py --city frankfurt --school-type secondary --force-rerun

Output:
    - Adds tuition columns to the school master table CSV/parquet in data_{city}/final/
    - Caches all API responses in data_{city}/cache/tuition_pipeline_{school_type}.json
    - Non-private schools: all tuition columns remain null

New columns written:
    tuition_tier              text      low / medium / high / premium / ultra
    tuition_tier_reasoning    text      how the tier was determined
    tuition_monthly_eur       numeric   estimated monthly fee (single value, Pass 1)
    tuition_income_matrix     text/JSON {under20: €, 21-30: €, ..., over250: €}
    tuition_sibling_discounts text/JSON {child_2_pct, child_3_pct, child_4_plus_pct}
    tuition_granular_reasoning text     Pass 2/3 estimation reasoning
    tuition_granular_generated_at text  ISO timestamp of Pass 2/3 generation
    income_based_tuition      boolean   True if fees vary by income, False if flat

Author: SchoolNossa Pipeline
"""

import argparse
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# 12 income brackets as defined in the Lovable spec
INCOME_BRACKETS = [
    ("under20",  15000),
    ("21-30",    25500),
    ("31-40",    35500),
    ("41-50",    45500),
    ("51-75",    63000),
    ("76-100",   88000),
    ("101-125", 113000),
    ("126-150", 138000),
    ("151-175", 163000),
    ("176-200", 188000),
    ("201-250", 225500),
    ("over250", 300000),
]
BRACKET_KEYS = [b for b, _ in INCOME_BRACKETS]

TIER_THRESHOLDS = [
    ("low",     0,    100),
    ("medium",  101,  300),
    ("high",    301,  500),
    ("premium", 501,  750),
    ("ultra",   751,  999999),
]

PASS1_TIER_FALLBACK_BASE = {"low": 150, "medium": 300, "high": 500, "premium": 625, "ultra": 900}
DEFAULT_SIBLING_DISCOUNTS = {"child_2_pct": 10, "child_3_pct": 15, "child_4_plus_pct": 20}

GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
OPENAI_RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"


# ---------------------------------------------------------------------------
# Config & helpers
# ---------------------------------------------------------------------------

def load_config():
    p = SCRIPT_DIR / "config.yaml"
    return yaml.safe_load(open(p)) if p.exists() else {}


def get_api_keys(config):
    return {
        "gemini":  config.get("api_keys", {}).get("gemini")  or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_AI_API_KEY"),
        "openai":  config.get("api_keys", {}).get("openai")  or os.environ.get("OPENAI_API_KEY"),
    }


def load_cache(path):
    return json.load(open(path)) if path.exists() else {}


def save_cache(cache, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(cache, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


def is_private(row):
    t = str(row.get("traegerschaft", "") or "").lower()
    return "privat" in t or "frei" in t


def monthly_to_tier(monthly_eur):
    for tier, lo, hi in TIER_THRESHOLDS:
        if lo <= monthly_eur <= hi:
            return tier
    return "ultra"


def has_flat_matrix(matrix):
    """True if all 12 bracket values are identical (Pass 2 failed to find real variation)."""
    if not isinstance(matrix, dict):
        return True
    values = [v for v in matrix.values() if isinstance(v, (int, float))]
    return len(values) < 2 or all(v == values[0] for v in values)


def is_timeout_error(error):
    if not error:
        return False
    if hasattr(error, "name") and error.name == "AbortError":
        return True
    if isinstance(error, str):
        s = error.lower()
        return "soft-timeout" in s or "abort" in s
    if isinstance(error, Exception):
        s = str(error).lower()
        return "soft-timeout" in s or "timed out" in s or "abort" in s
    return "soft-timeout" in str(error).lower() or "abort" in str(error).lower()


def get_grade_scope(row, school_type):
    if school_type == "primary":
        return "primary school (Grundschule, Grades 1-4)"
    stype = str(row.get("school_type", "")).lower()
    if "gymnasium" in stype:
        return "Gymnasium (Grades 5-12/13)"
    if "gesamtschule" in stype:
        return "Gesamtschule (Grades 5-10)"
    if "realschule" in stype:
        return "Realschule (Grades 5-10)"
    return "secondary school"


def get_search_location(city):
    return f"{city.capitalize()}, Germany"


def make_request(url, payload, headers, timeout=60):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ---------------------------------------------------------------------------
# Pass 1 — Tier Classification (Gemini + Google Search)
# ---------------------------------------------------------------------------

PASS1_SCHEMA = {
    "type": "object",
    "properties": {
        "cost_tier": {"type": "string", "enum": ["low", "medium", "high", "premium", "ultra"]},
        "approximate_monthly_tuition_eur": {"type": "number"},
        "reasoning": {"type": "string"},
    },
    "required": ["cost_tier", "approximate_monthly_tuition_eur", "reasoning"],
}


def build_pass1_prompt(row, city, school_type):
    grade_scope = get_grade_scope(row, school_type)
    search_location = get_search_location(city)

    def f(v):
        return str(v) if pd.notna(v) and str(v).strip() not in ("", "nan") else "Unknown"

    return (
        f'Search the web for "{row["schulname"]}" tuition fees in {search_location}.\n\n'
        f"School info:\n"
        f"- Name: {row['schulname']}\n"
        f"- Type: {f(row.get('school_type'))}\n"
        f"- Category: {school_type}\n"
        f"- Grade scope: {grade_scope}\n"
        f"- Website: {f(row.get('website'))}\n"
        f"- Trägerschaft: {f(row.get('traegerschaft'))}\n"
        f"- Known monthly tuition: {f(row.get('tuition_monthly_eur'))}\n"
        f"- Known annual tuition: {f(row.get('tuition_annual_eur'))}\n"
        f"- Income-based: {f(row.get('income_based_tuition'))}\n"
        f"- Notes: {f(row.get('tuition_notes'))}\n\n"
        f"Categorize into cost tier based on MONTHLY tuition (excluding meals/aftercare):\n"
        f"- low: €0-100/month (Waldorf sliding scale, symbolic fees)\n"
        f"- medium: €101-300/month (German private schools, church schools)\n"
        f"- high: €301-500/month (bilingual, IB schools)\n"
        f"- premium: €501-750/month (international schools)\n"
        f"- ultra: €750+/month (elite international schools)\n\n"
        f"Rules:\n"
        f"- If income-based: use average German household income €45,000/year\n"
        f"- If grade-based: use {grade_scope} fees\n"
        f"- Convert annual to monthly (÷12), exclude one-time registration fees"
    )


def run_pass1_gemini(row, city, school_type, gemini_key, model, delay):
    # Deterministic path: if tuition_monthly_eur already known, derive tier directly
    existing_monthly = row.get("tuition_monthly_eur")
    if pd.notna(existing_monthly) and isinstance(existing_monthly, (int, float)) and existing_monthly > 0:
        tier = monthly_to_tier(existing_monthly)
        return {
            "cost_tier": tier,
            "approximate_monthly_tuition_eur": float(existing_monthly),
            "reasoning": f"Derived from known tuition_monthly_eur={existing_monthly}",
        }

    existing_annual = row.get("tuition_annual_eur")
    if pd.notna(existing_annual) and isinstance(existing_annual, (int, float)) and existing_annual > 0:
        monthly = existing_annual / 12
        tier = monthly_to_tier(monthly)
        return {
            "cost_tier": tier,
            "approximate_monthly_tuition_eur": round(monthly, 2),
            "reasoning": f"Derived from known tuition_annual_eur={existing_annual} (÷12)",
        }

    # AI path
    prompt = build_pass1_prompt(row, city, school_type)
    url = GEMINI_ENDPOINT.format(model=model, key=gemini_key)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": PASS1_SCHEMA,
        },
        "tools": [{"googleSearch": {}}],
    }
    headers = {"Content-Type": "application/json"}

    for attempt in range(3):
        try:
            resp = make_request(url, payload, headers, timeout=300)
            raw = resp["candidates"][0]["content"]["parts"][0]["text"]
            result = json.loads(raw)
            time.sleep(delay)
            return result
        except Exception as e:
            if attempt == 2:
                raise
            logger.warning(f"    Pass 1 attempt {attempt+1} failed: {e}")
            time.sleep(2)


# ---------------------------------------------------------------------------
# Pass 2 — Income Matrix (Gemini + Google Search)
# ---------------------------------------------------------------------------

def _build_income_matrix_schema():
    bracket_props = {k: {"type": "number", "description": f"Monthly tuition EUR for bracket {k}"} for k in BRACKET_KEYS}
    return {
        "type": "object",
        "properties": {
            "income_matrix": {
                "type": "object",
                "properties": bracket_props,
                "required": BRACKET_KEYS,
            },
            "sibling_discounts": {
                "type": "object",
                "properties": {
                    "child_2_pct": {"type": "number"},
                    "child_3_pct": {"type": "number"},
                    "child_4_plus_pct": {"type": "number"},
                },
                "required": ["child_2_pct", "child_3_pct", "child_4_plus_pct"],
            },
            "reasoning": {"type": "string"},
        },
        "required": ["income_matrix", "sibling_discounts", "reasoning"],
    }

PASS2_SCHEMA = _build_income_matrix_schema()


def build_pass2_known_info(row):
    lines = []
    tier = row.get("tuition_tier")
    if pd.notna(tier):
        lines.append(f"- Cost tier (Pass 1): {tier}")
    reasoning = row.get("tuition_tier_reasoning")
    if pd.notna(reasoning) and reasoning:
        lines.append(f"- Pass 1 reasoning: {reasoning}")
    monthly = row.get("tuition_monthly_eur")
    if pd.notna(monthly):
        lines.append(f"- Known monthly tuition: €{monthly}")
    annual = row.get("tuition_annual_eur")
    if pd.notna(annual):
        lines.append(f"- Known annual tuition: €{annual}")
    income_based = row.get("income_based_tuition")
    if pd.notna(income_based):
        lines.append(f"- Income-based: {income_based}")
    notes = row.get("tuition_notes")
    if pd.notna(notes) and notes:
        lines.append(f"- Notes: {notes}")
    return "\n".join(lines) if lines else "None available"


def build_pass2_prompt(row, city, school_type):
    grade_scope = get_grade_scope(row, school_type)
    search_location = get_search_location(city)
    known_info = build_pass2_known_info(row)

    def f(v):
        return str(v) if pd.notna(v) and str(v).strip() not in ("", "nan") else "Not available"

    bracket_lines = "\n".join(
        f"- {key}: {'<' if key == 'under20' else ''} €{mid:,} annual (midpoint)"
        for key, mid in INCOME_BRACKETS
    )

    return (
        f'Search the web for detailed tuition fee structure for "{row["schulname"]}" '
        f"(private {grade_scope} in {search_location}).\n"
        f"Use Google Search grounding. Prefer official sources (school website / fee schedule PDFs).\n\n"
        f"SCHOOL INFORMATION:\n"
        f"- Name: {row['schulname']}\n"
        f"- Type: {f(row.get('school_type'))}\n"
        f"- Website: {f(row.get('website'))}\n"
        f"- Trägerschaft: {f(row.get('traegerschaft'))}\n\n"
        f"KNOWN DATA:\n{known_info}\n\n"
        f"TASK: Estimate the monthly tuition fee (in EUR) for each household income bracket:\n\n"
        f"INCOME BRACKETS:\n{bracket_lines}\n\n"
        f"ESTIMATION GUIDELINES:\n"
        f"1. For income-based schools (Waldorf, Montessori, community schools):\n"
        f"   - Typically 1-3% of gross annual income, divided by 12\n"
        f"   - Usually have minimum (~€100-150) and maximum (~€500-800) caps\n"
        f"   - Create a realistic gradient across income levels\n\n"
        f"2. For flat-fee schools (IB, International, some private Gymnasien):\n"
        f"   - Same fee regardless of income — use the known monthly/annual fee if available\n"
        f"   - If unknown, estimate based on tier: low=€150-250, medium=€300-450, high=€500-800, premium+=€900+\n\n"
        f"3. For sibling discounts:\n"
        f"   - Common pattern: 2nd child 10-20%, 3rd child 20-30%, 4th+ 25-50%\n"
        f"   - If no info available, use conservative defaults: 10%, 15%, 20%\n\n"
        f"All values must be positive numbers."
    )


def run_pass2_gemini(row, city, school_type, gemini_key, model, delay):
    prompt = build_pass2_prompt(row, city, school_type)
    url = GEMINI_ENDPOINT.format(model=model, key=gemini_key)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": PASS2_SCHEMA,
            "temperature": 0.7,
        },
        "tools": [{"googleSearch": {}}],
    }
    headers = {"Content-Type": "application/json"}

    for attempt in range(3):
        try:
            resp = make_request(url, payload, headers, timeout=120)
            raw = resp["candidates"][0]["content"]["parts"][0]["text"]
            result = json.loads(raw)
            time.sleep(delay)
            return result
        except Exception as e:
            if attempt == 2:
                raise
            logger.warning(f"    Pass 2 attempt {attempt+1} failed: {e}")
            time.sleep(2)


def pass2_fallback(row):
    """Fallback matrix when Pass 2 API fails — estimate from tier."""
    tier = str(row.get("tuition_tier", "medium") or "medium")
    base = PASS1_TIER_FALLBACK_BASE.get(tier, 300)
    income_based = row.get("income_based_tuition")
    is_income = bool(income_based) if pd.notna(income_based) else False

    matrix = {}
    for i, (key, _) in enumerate(INCOME_BRACKETS):
        if is_income:
            factor = 0.6 + (i / (len(INCOME_BRACKETS) - 1)) * 0.8
            matrix[key] = round(base * factor)
        else:
            matrix[key] = base

    return {
        "income_matrix": matrix,
        "sibling_discounts": DEFAULT_SIBLING_DISCOUNTS.copy(),
        "reasoning": f"[Fallback] Estimated from tier={tier}, income_based={is_income}",
    }


# ---------------------------------------------------------------------------
# Pass 3 — Verification (GPT-5.2 Responses API)
# ---------------------------------------------------------------------------

PASS3_SYSTEM = """You are a research assistant that must verify claims with up-to-date primary sources.

You MUST:

1) Use web search to find the school's official tuition/fees page or downloadable fee schedule (PDF). Prefer official school domains, government pages, or parent handbook PDFs over third-party summaries.

2) Extract the relevant fee rules (income bands, grade bands, school year, billing frequency/months, mandatory extras like enrollment fees, lunch, transport).

3) Compute the requested monthly tuition for each provided income point, using the school's stated rules.

4) Present results clearly in tables, and include citations for every key factual claim (prices, bands, dates, definitions of income basis).

5) If the school does NOT have income-based tuition, say so and return the standard tuition (and any grade bands) for the requested year.

6) If multiple campuses exist, choose the one that best matches the given city; if ambiguous, pick the most likely and clearly state what you picked.

7) If the fee schedule depends on a definition (e.g., "household gross", "worldwide income"), quote/reflect that definition and apply it consistently.

8) Handle boundary values carefully (e.g., 50,000 exactly) and explain which bracket it falls into.

Output must be concise, numeric, and auditable."""


def build_pass3_user_prompt(row, city, school_type):
    grade_scope = get_grade_scope(row, school_type)
    income_points = ", ".join(str(mid) for _, mid in INCOME_BRACKETS)

    website_line = f"\nSchool website: {row['website']}" if pd.notna(row.get("website")) and row.get("website") else ""
    stype_line = f"\nSchool type: {row.get('school_type', '')}" if pd.notna(row.get("school_type")) else ""
    traeger_line = f"\nOperator: {row.get('traegerschaft', '')}" if pd.notna(row.get("traegerschaft")) else ""

    return (
        f"Find the CURRENT tuition fees for: {row['schulname']} in {city.capitalize()}, Germany."
        f"{website_line}{stype_line}{traeger_line}\n"
        f"Grade scope: {grade_scope}\n\n"
        f"I need MONTHLY tuition fees computed for these household gross annual incomes (in EUR): {income_points}.\n\n"
        f"If fees differ by grade/level, produce separate results for each level. "
        f"Use the fees most relevant for the {grade_scope} level.\n\n"
        f"Requirements:\n"
        f"- Use the school's official tuition schedule for the current or latest available school year.\n"
        f"- After your research, you MUST call the 'report_tuition_fees' function with the computed monthly tuition "
        f"for each income bracket and any sibling discounts found.\n"
        f"- If income-based tuition is not offered, return the same standard monthly tuition for all income brackets "
        f"and state 'not income-based' in the reasoning.\n\n"
        f"Be explicit about:\n"
        f"- how many billing months per year the school uses (if stated),\n"
        f"- any enrollment/registration fee,\n"
        f"- the precise definition of the income measure (e.g., household gross, worldwide income)."
    )


def _build_pass3_function_tool():
    bracket_props = {k: {"type": "number", "description": f"Monthly tuition EUR for income midpoint {mid}"} for k, mid in INCOME_BRACKETS}
    return {
        "type": "function",
        "name": "report_tuition_fees",
        "description": "Report the computed monthly tuition fees by income bracket and sibling discounts.",
        "parameters": {
            "type": "object",
            "properties": {
                "income_matrix": {
                    "type": "object",
                    "properties": bracket_props,
                    "required": BRACKET_KEYS,
                    "additionalProperties": False,
                },
                "sibling_discounts": {
                    "type": "object",
                    "properties": {
                        "child_2_pct": {"type": "number"},
                        "child_3_pct": {"type": "number"},
                        "child_4_plus_pct": {"type": "number"},
                    },
                    "required": ["child_2_pct", "child_3_pct", "child_4_plus_pct"],
                    "additionalProperties": False,
                },
                "reasoning": {"type": "string"},
            },
            "required": ["income_matrix", "sibling_discounts", "reasoning"],
            "additionalProperties": False,
        },
        "strict": True,
    }


def run_pass3_openai(row, city, school_type, openai_key, model, delay):
    user_prompt = build_pass3_user_prompt(row, city, school_type)
    payload = {
        "model": model,
        "instructions": PASS3_SYSTEM,
        "input": user_prompt,
        "tools": [
            {"type": "web_search"},
            _build_pass3_function_tool(),
        ],
        "reasoning": {"effort": "high"},
    }
    headers = {
        "Authorization": f"Bearer {openai_key}",
        "Content-Type": "application/json",
    }

    resp = make_request(OPENAI_RESPONSES_ENDPOINT, payload, headers, timeout=240)

    # Parse function_call from output array
    output = resp.get("output", [])
    fn_call = next(
        (item for item in output if item.get("type") == "function_call" and item.get("name") == "report_tuition_fees"),
        None
    )
    if not fn_call:
        raise ValueError(f"report_tuition_fees function not called in response. Output items: {[i.get('type') for i in output]}")

    result = json.loads(fn_call["arguments"])
    time.sleep(delay)
    return result


def validate_matrix(matrix):
    """Ensure all bracket values are positive numbers; default any invalid ones to 200."""
    validated = {}
    for key in BRACKET_KEYS:
        val = matrix.get(key, 200)
        validated[key] = val if isinstance(val, (int, float)) and val > 0 else 200
    return validated


def validate_sibling_discounts(discounts):
    defaults = {"child_2_pct": 10, "child_3_pct": 15, "child_4_plus_pct": 20}
    if not isinstance(discounts, dict):
        return defaults
    result = {}
    for k, default in defaults.items():
        val = discounts.get(k, default)
        result[k] = val if isinstance(val, (int, float)) and 0 <= val <= 100 else default
    return result


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_school(row, city, school_type, passes, cache, api_keys, config, force_rerun):
    school_id = str(row.get("schulnummer", row["schulname"]))
    if school_id not in cache:
        cache[school_id] = {}
    entry = cache[school_id]

    if not is_private(row):
        entry["skipped"] = "not private"
        return entry

    model_cfg = config.get("models", {})
    gemini_model = model_cfg.get("gemini_tuition", "gemini-3-pro-preview")
    openai_model = model_cfg.get("openai_tuition_pass3", "gpt-5.2")
    delay = config.get("rate_limits", {}).get("delay_between_requests", 2.0)
    gemini_key = api_keys.get("gemini")
    openai_key = api_keys.get("openai")

    # --- Pass 1: Tier Classification ---
    if 1 in passes:
        if "pass1" not in entry or force_rerun:
            if not gemini_key:
                logger.warning(f"  [{school_id}] No Gemini key — skipping Pass 1")
            else:
                try:
                    result = run_pass1_gemini(row, city, school_type, gemini_key, gemini_model, delay)
                    entry["pass1"] = result
                    logger.info(f"  [{school_id}] Pass 1: tier={result['cost_tier']} monthly=€{result['approximate_monthly_tuition_eur']}")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 1 failed: {e} — using fallback")
                    entry["pass1"] = {
                        "cost_tier": "medium",
                        "approximate_monthly_tuition_eur": 200,
                        "reasoning": f"[Fallback] Pass 1 error: {e}",
                    }
        else:
            logger.debug(f"  [{school_id}] Pass 1 cached")

    # --- Pass 2: Income Matrix ---
    if 2 in passes:
        if "pass2" not in entry or force_rerun:
            if "pass1" not in entry:
                logger.debug(f"  [{school_id}] No Pass 1 data — skipping Pass 2")
            elif not gemini_key:
                logger.warning(f"  [{school_id}] No Gemini key — skipping Pass 2")
            else:
                # Temporarily apply Pass 1 tier to the row for context
                row_copy = row.copy()
                row_copy["tuition_tier"] = entry["pass1"].get("cost_tier")
                row_copy["tuition_monthly_eur"] = entry["pass1"].get("approximate_monthly_tuition_eur")
                row_copy["tuition_tier_reasoning"] = entry["pass1"].get("reasoning")
                try:
                    result = run_pass2_gemini(row_copy, city, school_type, gemini_key, gemini_model, delay)
                    result["income_matrix"] = validate_matrix(result.get("income_matrix", {}))
                    result["sibling_discounts"] = validate_sibling_discounts(result.get("sibling_discounts", {}))
                    entry["pass2"] = result
                    flat = has_flat_matrix(result["income_matrix"])
                    logger.info(f"  [{school_id}] Pass 2 complete (flat={flat})")
                except Exception as e:
                    logger.warning(f"  [{school_id}] Pass 2 failed: {e} — using fallback")
                    fb = pass2_fallback(row_copy)
                    entry["pass2"] = fb
        else:
            logger.debug(f"  [{school_id}] Pass 2 cached")

    # --- Pass 3: Verification (only for flat matrices) ---
    if 3 in passes:
        p2 = entry.get("pass2")
        if not p2:
            logger.debug(f"  [{school_id}] No Pass 2 data — skipping Pass 3")
        elif not has_flat_matrix(p2.get("income_matrix", {})):
            # Pass 2 already found income-based variation — no need for Pass 3 verification
            entry["income_based_tuition"] = True
            logger.debug(f"  [{school_id}] Matrix already varied — income_based_tuition=True, skipping Pass 3")
        elif entry.get("income_based_tuition") is False:
            logger.debug(f"  [{school_id}] Already confirmed flat (income_based_tuition=False) — skipping Pass 3")
        elif "[Pass3 Timeout]" not in str(p2.get("reasoning", "")) and "pass3" in entry and not force_rerun:
            logger.debug(f"  [{school_id}] Pass 3 already run — skipping")
        elif not openai_key:
            logger.warning(f"  [{school_id}] No OpenAI key — skipping Pass 3")
        else:
            try:
                result = run_pass3_openai(row, city, school_type, openai_key, openai_model, delay)
                result["income_matrix"] = validate_matrix(result.get("income_matrix", {}))
                result["sibling_discounts"] = validate_sibling_discounts(result.get("sibling_discounts", {}))
                result["reasoning"] = "[GPT-5.2 Pass3] " + result.get("reasoning", "")
                still_flat = has_flat_matrix(result["income_matrix"])
                entry["pass3"] = result
                entry["income_based_tuition"] = not still_flat
                logger.info(f"  [{school_id}] Pass 3 complete (flat={still_flat} → income_based={not still_flat})")
            except Exception as e:
                if is_timeout_error(e):
                    logger.warning(f"  [{school_id}] Pass 3 timed out — will retry next run")
                    if "pass2" in entry:
                        entry["pass2"]["reasoning"] = "[Pass3 Timeout] " + entry["pass2"].get("reasoning", "")
                else:
                    logger.warning(f"  [{school_id}] Pass 3 error: {e} — marking as flat")
                    entry["income_based_tuition"] = False
                    if "pass2" in entry:
                        entry["pass2"]["reasoning"] = f"[Pass3 Error: {e}] " + entry["pass2"].get("reasoning", "")

    return entry


# ---------------------------------------------------------------------------
# Apply cache → DataFrame
# ---------------------------------------------------------------------------

def apply_cache_to_dataframe(df, cache, passes):
    # Ensure new columns exist
    new_cols = [
        "tuition_tier", "tuition_tier_reasoning",
        "tuition_income_matrix", "tuition_sibling_discounts",
        "tuition_granular_reasoning", "tuition_granular_generated_at",
        "income_based_tuition",
    ]
    for col in new_cols:
        if col not in df.columns:
            df[col] = None

    updated = 0
    for idx, row in df.iterrows():
        school_id = str(row.get("schulnummer", row["schulname"]))
        entry = cache.get(school_id, {})
        if not entry or entry.get("skipped"):
            continue

        p1 = entry.get("pass1")
        p2 = entry.get("pass3") or entry.get("pass2")   # Pass 3 overwrites Pass 2
        p3_income = entry.get("income_based_tuition")

        if 1 in passes and p1:
            df.at[idx, "tuition_tier"] = p1.get("cost_tier")
            df.at[idx, "tuition_tier_reasoning"] = p1.get("reasoning")
            # Only write monthly if not already set from official data
            existing = df.at[idx, "tuition_monthly_eur"]
            if pd.isna(existing) or not existing:
                df.at[idx, "tuition_monthly_eur"] = p1.get("approximate_monthly_tuition_eur")
            updated += 1

        if (2 in passes or 3 in passes) and p2:
            df.at[idx, "tuition_income_matrix"] = json.dumps(p2.get("income_matrix", {}), ensure_ascii=False)
            df.at[idx, "tuition_sibling_discounts"] = json.dumps(p2.get("sibling_discounts", {}), ensure_ascii=False)
            df.at[idx, "tuition_granular_reasoning"] = p2.get("reasoning")
            df.at[idx, "tuition_granular_generated_at"] = datetime.now(timezone.utc).isoformat()

        if p3_income is not None:
            df.at[idx, "income_based_tuition"] = p3_income

    logger.info(f"Applied tuition data to {updated} schools")
    return df


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def find_input_csv(city, school_type):
    d = PROJECT_ROOT / f"data_{city}" / "final"
    candidates = [
        d / f"{city}_{school_type}_school_master_table_final.csv",
        d / f"{city}_school_master_table_final.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"No input CSV for {city}/{school_type}")


def save_results(df, city, school_type):
    out_dir = PROJECT_ROOT / f"data_{city}" / "final"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = f"{city}_{school_type}_school_master_table_final"

    csv_path = out_dir / f"{base}.csv"
    csv_df = df.drop(columns=["embedding"], errors="ignore")
    csv_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Saved CSV: {csv_path}")

    parquet_path = out_dir / f"{base}_with_embeddings.parquet"
    if parquet_path.exists():
        try:
            existing = pd.read_parquet(parquet_path)
            tuition_cols = [c for c in df.columns if "tuition" in c or c == "income_based_tuition"]
            for col in tuition_cols:
                existing[col] = df[col].values
            existing.to_parquet(parquet_path, index=False)
            logger.info(f"Updated parquet: {parquet_path}")
        except Exception as e:
            logger.warning(f"Could not update parquet: {e}")


def print_summary(df, cache, passes, school_type):
    private_mask = df["traegerschaft"].str.contains("privat|frei", case=False, na=False)
    n_private = private_mask.sum()

    print(f"\n{'='*65}")
    print(f"TUITION PIPELINE SUMMARY — {school_type.upper()}")
    print(f"{'='*65}")
    print(f"  Total schools: {len(df)}  |  Private: {n_private}  |  Cache entries: {len(cache)}")

    if 1 in passes:
        n = df["tuition_tier"].notna().sum()
        print(f"  Pass 1 (tier): {n}/{n_private} private schools classified")
        if n > 0:
            print(f"    {df['tuition_tier'].value_counts().to_dict()}")

    if 2 in passes or 3 in passes:
        n = df["tuition_income_matrix"].notna().sum()
        print(f"  Pass 2/3 (matrix): {n}/{n_private} income matrices generated")
        n_ib = (df["income_based_tuition"] == True).sum()
        n_flat = (df["income_based_tuition"] == False).sum()
        print(f"    income_based=True: {n_ib}  |  income_based=False (confirmed flat): {n_flat}")

    print(f"{'='*65}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Tuition Fee Pipeline")
    parser.add_argument("--city",        required=True)
    parser.add_argument("--school-type", required=True, choices=["primary", "secondary"])
    parser.add_argument("--passes",      default="1,2,3",
                        help="Comma-separated passes: 1=tier, 2=matrix, 3=verify (default: 1,2,3)")
    parser.add_argument("--limit",       type=int, default=None)
    parser.add_argument("--force-rerun", action="store_true")
    args = parser.parse_args()

    passes = set(int(p.strip()) for p in args.passes.split(","))
    city = args.city.lower()
    school_type = args.school_type.lower()

    logger.info(f"Tuition pipeline: city={city}, type={school_type}, passes={sorted(passes)}")

    config = load_config()
    api_keys = get_api_keys(config)

    if (1 in passes or 2 in passes) and not api_keys.get("gemini"):
        logger.warning("No Gemini API key — Passes 1 and 2 will be skipped")
    if 3 in passes and not api_keys.get("openai"):
        logger.warning("No OpenAI API key — Pass 3 will be skipped")

    csv_path = find_input_csv(city, school_type)
    df = pd.read_csv(csv_path, low_memory=False)
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"Limited to {args.limit} schools")

    n_private = df["traegerschaft"].str.contains("privat|frei", case=False, na=False).sum()
    logger.info(f"Loaded {len(df)} schools ({n_private} private)")

    cache_path = PROJECT_ROOT / f"data_{city}" / "cache" / f"tuition_pipeline_{school_type}.json"
    cache = load_cache(cache_path)
    logger.info(f"Cache: {len(cache)} existing entries")

    for i, (_, row) in enumerate(df.iterrows()):
        school_id = str(row.get("schulnummer", row["schulname"]))
        if not is_private(row):
            continue
        logger.info(f"[{i+1}/{len(df)}] {row['schulname']} ({school_id})")
        process_school(row, city, school_type, passes, cache, api_keys, config, args.force_rerun)
        if (i + 1) % 5 == 0:
            save_cache(cache, cache_path)

    save_cache(cache, cache_path)
    logger.info("Cache saved.")

    df = apply_cache_to_dataframe(df, cache, passes)
    save_results(df, city, school_type)
    print_summary(df, cache, passes, school_type)


if __name__ == "__main__":
    main()
