#!/usr/bin/env python3
"""
Translate admission criteria + open day data from German to English.

Reads the per-city intermediate CSVs (produced by the admission enrichment),
translates German text fields to English via Gemini 2.5 Flash (no grounding,
just text-to-text), and writes updated CSVs with _en columns added.

Batches multiple schools per Gemini call for efficiency (~5 schools per call).

Usage:
    python scripts_shared/enrichment/translate_admission_data_to_english.py --dry-run
    python scripts_shared/enrichment/translate_admission_data_to_english.py
    python scripts_shared/enrichment/translate_admission_data_to_english.py --limit 10
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
CACHE_DIR = PROJECT_ROOT / "data_shared" / "cache" / "admission_open_days"
TRANSLATION_CACHE = CACHE_DIR / "translations_cache.json"
COMBINED_OUTPUT = PROJECT_ROOT / "data_shared" / "admission_open_days_all_german_cities.csv"

MODEL = "gemini-2.5-flash"
BATCH_SIZE = 5
REQUEST_DELAY = 1.0
SAVE_INTERVAL = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("translate_admission")

# .env loader
try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value

# Import city registry from main script
sys.path.insert(0, str(SCRIPT_DIR))
from enrich_german_schools_with_admission_and_open_days import (
    CITY_REGISTRY, OUTPUT_COLUMNS, _data_dir_for_city,
)

# English columns to add
EN_COLUMNS = [
    "admission_criteria_bullets_en",
    "admission_application_window_en",
    "admission_notes_en",
    "open_days_en",
]

TRANSLATE_PROMPT = """Translate the following German school admission data to English.
Keep the JSON structure identical — only translate the text values.
Do NOT translate dates, times, or schulnummer. Keep event_type values as English equivalents:
- "Tag der offenen Tür" → "Open Day"
- "Infoabend" → "Information Evening"
- "Schnuppertag" → "Trial Day"
- "Anmeldetag" → "Registration Day"
- "Sonstiges" → "Other"

Input (one JSON object per school, keyed by schulnummer):
{input_json}

Output the SAME structure with all German text translated to natural English.
Do NOT include citation markers like [cite: ...] or [source: ...] — output clean text only.
Return ONLY the JSON object, no markdown code blocks.
"""


def _init_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")
    from google import genai
    return genai.Client(api_key=api_key)


def _safe_json(value, default=None):
    if pd.isna(value) or value in ("", "null", "nan"):
        return default
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default


def _safe_str(value) -> Optional[str]:
    if pd.isna(value) or str(value).strip() in ("", "nan", "null"):
        return None
    return str(value).strip()


def _build_translation_input(rows: List[pd.Series]) -> Dict[str, dict]:
    """Build a dict of {schulnummer: {fields_to_translate}} for a batch."""
    batch = {}
    for row in rows:
        snr = str(row["schulnummer"])
        entry = {}

        bullets = _safe_json(row.get("admission_criteria_bullets"), [])
        if bullets:
            entry["admission_criteria_bullets"] = bullets

        window = _safe_json(row.get("admission_application_window"))
        if window and isinstance(window, dict):
            # Only translate the "notes" field
            notes = window.get("notes", "")
            if notes:
                entry["application_window_notes"] = notes

        notes_de = _safe_str(row.get("admission_notes_de"))
        if notes_de:
            entry["admission_notes_de"] = notes_de

        open_days = _safe_json(row.get("open_days"), [])
        if open_days:
            # Only translate event_type, audience, notes within each day
            translatable = []
            for od in open_days:
                translatable.append({
                    "event_type": od.get("event_type", ""),
                    "audience": od.get("audience", ""),
                    "notes": od.get("notes", ""),
                })
            entry["open_days_text"] = translatable

        if entry:
            batch[snr] = entry
    return batch


def _call_gemini_translate(client, batch_input: dict, retry: int = 0, max_retries: int = 2) -> Optional[dict]:
    """Call Gemini to translate a batch. Returns {schulnummer: translated_fields}."""
    from google.genai import types

    input_json = json.dumps(batch_input, ensure_ascii=False, indent=1)
    prompt = TRANSLATE_PROMPT.format(input_json=input_json)

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0),
        )
    except Exception as exc:
        msg = str(exc)
        if ("RATE_LIMIT" in msg.upper() or "429" in msg) and retry < 3:
            wait = 30 * (retry + 1)
            logger.warning(f"  Rate limited — waiting {wait}s")
            time.sleep(wait)
            return _call_gemini_translate(client, batch_input, retry + 1, max_retries)
        if ("500" in msg or "INTERNAL" in msg) and retry < max_retries:
            time.sleep(5)
            return _call_gemini_translate(client, batch_input, retry + 1, max_retries)
        logger.warning(f"  Gemini error: {exc}")
        return None

    text = getattr(response, "text", None)
    if not text:
        if retry < max_retries:
            time.sleep(3)
            return _call_gemini_translate(client, batch_input, retry + 1, max_retries)
        return None

    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    if "{" in text and "}" in text:
        text = text[text.index("{"):text.rindex("}") + 1]

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if retry < max_retries:
            time.sleep(3)
            return _call_gemini_translate(client, batch_input, retry + 1, max_retries)
        logger.warning(f"  JSON parse error after retries")
        return None


def _apply_translations(df: pd.DataFrame, translations: Dict[str, dict]) -> pd.DataFrame:
    """Add _en columns to the dataframe from translation results."""
    for col in EN_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    for idx, row in df.iterrows():
        snr = str(row["schulnummer"])
        tr = translations.get(snr)
        if not tr:
            continue

        # Bullets
        bullets_en = tr.get("admission_criteria_bullets", [])
        if bullets_en:
            df.at[idx, "admission_criteria_bullets_en"] = json.dumps(bullets_en, ensure_ascii=False)

        # Application window — reconstruct with translated notes
        window = _safe_json(row.get("admission_application_window"))
        window_notes_en = tr.get("application_window_notes", "")
        if window and isinstance(window, dict):
            window_en = {**window}
            if window_notes_en:
                window_en["notes"] = window_notes_en
            df.at[idx, "admission_application_window_en"] = json.dumps(window_en, ensure_ascii=False)

        # Notes
        notes_en = tr.get("admission_notes_de", "")
        if notes_en:
            df.at[idx, "admission_notes_en"] = notes_en

        # Open days — merge translated text fields back into the structured objects
        open_days = _safe_json(row.get("open_days"), [])
        od_text_en = tr.get("open_days_text", [])
        if open_days and od_text_en:
            merged = []
            for i, od in enumerate(open_days):
                od_en = dict(od)  # copy
                if i < len(od_text_en):
                    t = od_text_en[i]
                    od_en["event_type"] = t.get("event_type", od.get("event_type", ""))
                    od_en["audience"] = t.get("audience", od.get("audience", ""))
                    od_en["notes"] = t.get("notes", od.get("notes", ""))
                merged.append(od_en)
            df.at[idx, "open_days_en"] = json.dumps(merged, ensure_ascii=False)

    return df


# Cache
def _load_translation_cache() -> Dict[str, dict]:
    if TRANSLATION_CACHE.exists():
        try:
            with open(TRANSLATION_CACHE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            pass
    return {}


def _save_translation_cache(cache: Dict[str, dict]):
    TRANSLATION_CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = TRANSLATION_CACHE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2, ensure_ascii=False)
    tmp.replace(TRANSLATION_CACHE)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--refresh-cache", action="store_true")
    args = parser.parse_args(argv)

    # Load all per-city CSVs
    frames = []
    city_files = {}
    for cfg in CITY_REGISTRY:
        data_dir = _data_dir_for_city(cfg.key)
        csv_path = PROJECT_ROOT / f"data_{data_dir}" / "intermediate" / f"{cfg.key}_admission_open_days.csv"
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path, dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, dtype=str, encoding="latin-1")
        city_files[cfg.key] = csv_path
        frames.append((cfg.key, df))

    if not frames:
        logger.error("No per-city CSVs found")
        return 1

    # Combine for processing
    all_rows = pd.concat([df for _, df in frames], ignore_index=True)
    success = all_rows[all_rows["gemini_status"] == "success"].copy()
    success = success.drop_duplicates(subset="schulnummer", keep="first")
    logger.info(f"Total rows: {len(all_rows)}, success+unique: {len(success)}")

    # Filter to those with translatable content
    has_content = success[
        success["admission_criteria_bullets"].apply(lambda v: bool(_safe_json(v, []))) |
        success["admission_notes_de"].apply(lambda v: bool(_safe_str(v)))
    ]
    logger.info(f"Schools with translatable content: {len(has_content)}")

    if args.limit:
        has_content = has_content.head(args.limit)
        logger.info(f"--limit {args.limit} → {len(has_content)}")

    # Load translation cache
    cache = _load_translation_cache()
    logger.info(f"Translation cache: {len(cache)} entries")

    if args.dry_run:
        cached = sum(1 for _, r in has_content.iterrows() if str(r["schulnummer"]) in cache and not args.refresh_cache)
        logger.info(f"Would translate {len(has_content) - cached} schools ({cached} cached)")
        return 0

    client = _init_gemini_client()

    # Process in batches
    rows_list = [row for _, row in has_content.iterrows()]
    api_calls = 0
    cache_hits = 0

    for batch_start in range(0, len(rows_list), BATCH_SIZE):
        batch_rows = rows_list[batch_start:batch_start + BATCH_SIZE]

        # Check cache for each row
        uncached_rows = []
        for row in batch_rows:
            snr = str(row["schulnummer"])
            if snr in cache and not args.refresh_cache:
                cache_hits += 1
            else:
                uncached_rows.append(row)

        if not uncached_rows:
            continue

        batch_input = _build_translation_input(uncached_rows)
        if not batch_input:
            continue

        result = _call_gemini_translate(client, batch_input)
        api_calls += 1

        if result:
            for snr, translated in result.items():
                cache[snr] = translated
        else:
            logger.warning(f"  Batch {batch_start // BATCH_SIZE + 1} failed")

        if api_calls % SAVE_INTERVAL == 0:
            _save_translation_cache(cache)
            logger.info(f"  progress: {batch_start + len(batch_rows)}/{len(rows_list)} — {api_calls} API calls, {cache_hits} cache hits")

        time.sleep(REQUEST_DELAY)

    _save_translation_cache(cache)
    logger.info(f"Translation done: {api_calls} API calls, {cache_hits} cache hits, {len(cache)} total cached")

    # Now apply translations to each city's CSV and rewrite
    all_output_columns = list(OUTPUT_COLUMNS) + EN_COLUMNS
    combined_frames = []

    for city_key, df in frames:
        df = _apply_translations(df, cache)
        csv_path = city_files[city_key]
        df.to_csv(csv_path, index=False)
        try:
            df.to_parquet(csv_path.with_suffix(".parquet"), index=False)
        except Exception:
            pass
        logger.info(f"[{city_key}] rewrote {len(df)} rows with EN columns → {csv_path.name}")
        combined_frames.append(df)

    # Write combined
    combined = pd.concat(combined_frames, ignore_index=True)
    combined.to_csv(COMBINED_OUTPUT, index=False)
    logger.info(f"Wrote combined: {COMBINED_OUTPUT} ({len(combined)} rows)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
