#!/usr/bin/env python3
"""
Phase 6: Dresden Website Metadata & Description Enrichment
============================================================

Scrapes school websites using Gemini with URL context and Google Search
grounding to extract metadata (student/teacher counts, languages, etc.)
and generate bilingual descriptions.

Adapted from Leipzig pipeline (same Sachsen Schuldatenbank data model).

Phase A - Metadata extraction:
    schueler_2024_25, lehrer_2024_25, sprachen, gruendungsjahr,
    schulleitung, ganztag, besonderheiten,
    tuition_monthly_eur, scholarship_available

Phase B - Rich description generation:
    description (EN), description_de (DE),
    summary_en, summary_de

Results cached per schulnummer to data_dresden/cache/website_metadata/

Input: data_dresden/intermediate/dresden_schools_with_poi.csv (fallback chain)
Output: data_dresden/intermediate/dresden_schools_with_website_metadata.csv

Author: Dresden School Data Pipeline
Created: 2026-04-07
Updated: 2026-04-11 — full implementation from Leipzig template
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache" / "website_metadata"
ENV_FILE = PROJECT_ROOT / ".env"

WEBSITE_CACHE = CACHE_DIR / "website_metadata_cache.json"
DESCRIPTION_CACHE = CACHE_DIR / "website_description_cache.json"

REQUEST_DELAY = 1.5
SAVE_INTERVAL = 10

METADATA_MODEL = "gemini-2.5-flash"
DESCRIPTION_MODEL = "gemini-2.5-flash"

# Fallback input chain (most enriched first)
INPUT_FALLBACKS_SECONDARY = [
    INTERMEDIATE_DIR / "dresden_secondary_schools_with_pois.csv",
    INTERMEDIATE_DIR / "dresden_secondary_schools_with_crime.csv",
    INTERMEDIATE_DIR / "dresden_secondary_schools_with_transit.csv",
    INTERMEDIATE_DIR / "dresden_schools_with_poi.csv",
    INTERMEDIATE_DIR / "dresden_schools_with_crime.csv",
    INTERMEDIATE_DIR / "dresden_schools_with_transit.csv",
    RAW_DIR / "dresden_schools_raw.csv",
]

INPUT_FALLBACKS_PRIMARY = [
    INTERMEDIATE_DIR / "dresden_primary_schools_with_pois.csv",
    INTERMEDIATE_DIR / "dresden_primary_schools_with_crime.csv",
    INTERMEDIATE_DIR / "dresden_primary_schools_with_transit.csv",
    RAW_DIR / "dresden_primary_schools_raw.csv",
]

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    if key and key not in os.environ:
                        os.environ[key] = value

METADATA_PROMPT = """Durchsuche diese Schulwebsite gründlich und extrahiere folgende Informationen.
Suche auf der Hauptseite und allen verlinkten Unterseiten (Über uns, Unsere Schule,
Kollegium, Schulprofil, Zahlen und Fakten, etc.).

Schule: {schulname}
Schulform: {schultyp}
Trägerschaft: {traegerschaft}
Adresse: {strasse}, {plz} {stadt}

Extrahiere als JSON:
- schueler: Gesamtanzahl Schülerinnen und Schüler (int oder null). Suche nach "SuS", "Schülerinnen und Schüler", "ca. XXX Schüler", "lernen hier XXX Kinder"
- lehrer: Anzahl Lehrkräfte/Kollegium (int oder null). Suche nach "Kollegium", "Lehrerinnen und Lehrer", "Lehrkräfte"
- sprachen: Angebotene Fremdsprachen als Liste (z.B. ["Englisch", "Französisch"]) oder null
- gruendungsjahr: Gründungsjahr der Schule (int, z.B. 1920) oder null
- schulleitung: Name der Schulleitung (string) oder null. Suche nach "Schulleiter/in", "Rektor/in", "Direktor/in"
- ganztag: Ist es eine Ganztagsschule? (true/false/null). Suche nach "Ganztag", "GTA", "Ganztagsangebote", "Hort", "Betreuung"
- besonderheiten: Besondere Programme oder Schwerpunkte, max 150 Zeichen (string oder null). Z.B. MINT, Musik, Sport, UNESCO, Inklusion, bilingual, Montessori
- tuition_monthly_eur: Monatliches Schulgeld in Euro (int oder null). Nur für Privatschulen relevant.
- scholarship_available: Gibt es Stipendien oder Ermäßigungen? (true/false/null).

Wenn eine Information nicht eindeutig gefunden werden kann, setze den Wert auf null.
Antworte NUR mit dem JSON-Objekt, kein Markdown.

URL: {url}"""

DESCRIPTION_PROMPT = """Du bist ein Experte für Schulprofile. Basierend auf der Website dieser Schule
und allen verfügbaren Informationen, erstelle ein umfassendes Schulprofil.

Schule: {schulname}
Schulform: {schultyp}
Trägerschaft: {traegerschaft}
Adresse: {strasse}, {plz} {stadt}
Website: {url}

Zusätzliche Daten (falls verfügbar):
- Schüler: {schueler}
- Lehrkräfte: {lehrer}
- Sprachen: {sprachen}
- Gründungsjahr: {gruendungsjahr}
- Schulleitung: {schulleitung}
- Besonderheiten: {besonderheiten}

Antworte als JSON mit genau diesen Feldern:
- description_de: Umfassende deutsche Beschreibung der Schule (250-400 Wörter).
- description: Englische Übersetzung der deutschen Beschreibung (250-400 Wörter).
- summary_de: Kurze deutsche Zusammenfassung (2-3 Sätze, max 100 Wörter).
- summary_en: Kurze englische Zusammenfassung (2-3 Sätze, max 100 Wörter).

Antworte NUR mit dem JSON-Objekt, kein Markdown."""


def _load_cache(cache_path: Path) -> dict:
    if cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache: dict, cache_path: Path):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def _call_gemini(
    client,
    prompt: str,
    model: str,
    schulnummer: str,
    schulname: str,
    retry_count: int = 0,
    max_retries: int = 2,
) -> Optional[Dict]:
    """
    Call Gemini with URL context + Google Search grounding and parse JSON response.
    Retries on transient failures (empty response, JSON parse error, 500 errors).
    """
    from google.genai import types

    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[
                    types.Tool(url_context=types.UrlContext()),
                    types.Tool(google_search=types.GoogleSearch()),
                ],
                temperature=0,
            ),
        )

        # Handle cases where response has no text (e.g., only tool call results)
        text = response.text
        if not text:
            # Try to extract text from parts
            if response.candidates and response.candidates[0].content:
                parts = response.candidates[0].content.parts
                text_parts = [p.text for p in parts if hasattr(p, 'text') and p.text]
                text = "\n".join(text_parts) if text_parts else None
            if not text:
                if retry_count < max_retries:
                    logger.info(
                        f"  Empty response for {schulnummer} ({schulname}) — retry {retry_count + 1}/{max_retries}"
                    )
                    time.sleep(3)
                    return _call_gemini(
                        client, prompt, model, schulnummer, schulname,
                        retry_count + 1, max_retries,
                    )
                logger.warning(
                    f"  Empty response for {schulnummer} ({schulname}) — exhausted retries"
                )
                return None

        text = text.strip()
        # Clean markdown code block if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        data = json.loads(text)
        return data

    except json.JSONDecodeError as e:
        if retry_count < max_retries:
            logger.info(
                f"  JSON parse error for {schulnummer} ({schulname}) — retry {retry_count + 1}/{max_retries}"
            )
            time.sleep(3)
            return _call_gemini(
                client, prompt, model, schulnummer, schulname,
                retry_count + 1, max_retries,
            )
        logger.warning(
            f"  JSON parse error for {schulnummer} ({schulname}): {e} — exhausted retries"
        )
        return None
    except Exception as e:
        error_msg = str(e)
        if "URL_RETRIEVAL_STATUS_ERROR" in error_msg:
            logger.warning(
                f"  URL unreachable for {schulnummer} ({schulname})"
            )
        elif ("RATE_LIMIT" in error_msg.upper() or "429" in error_msg) and retry_count < 3:
            wait_time = 30 * (retry_count + 1)
            logger.warning(f"  Rate limited — waiting {wait_time}s...")
            time.sleep(wait_time)
            return _call_gemini(
                client, prompt, model, schulnummer, schulname,
                retry_count + 1, max_retries,
            )
        elif ("500" in error_msg or "INTERNAL" in error_msg) and retry_count < max_retries:
            logger.info(
                f"  Server error for {schulnummer} ({schulname}) — retry {retry_count + 1}/{max_retries}"
            )
            time.sleep(5)
            return _call_gemini(
                client, prompt, model, schulnummer, schulname,
                retry_count + 1, max_retries,
            )
        else:
            logger.warning(
                f"  Error for {schulnummer} ({schulname}): {e}"
            )
        return None


def _apply_metadata(df: pd.DataFrame, idx: int, data: dict) -> bool:
    """Apply extracted metadata fields (fill gaps only)."""
    any_filled = False

    if data.get("schueler") is not None and pd.isna(df.at[idx, "schueler_2024_25"]):
        try:
            df.at[idx, "schueler_2024_25"] = int(data["schueler"])
            any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("lehrer") is not None and pd.isna(df.at[idx, "lehrer_2024_25"]):
        try:
            df.at[idx, "lehrer_2024_25"] = int(data["lehrer"])
            any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("sprachen") is not None and pd.isna(df.at[idx, "sprachen"]):
        if isinstance(data["sprachen"], list):
            df.at[idx, "sprachen"] = ", ".join(data["sprachen"])
        else:
            df.at[idx, "sprachen"] = str(data["sprachen"])
        any_filled = True

    if data.get("gruendungsjahr") is not None and pd.isna(df.at[idx, "gruendungsjahr"]):
        try:
            year = int(data["gruendungsjahr"])
            if 1800 <= year <= 2026:
                df.at[idx, "gruendungsjahr"] = year
                any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("schulleitung") is not None and pd.isna(df.at[idx, "leitung"]):
        df.at[idx, "leitung"] = str(data["schulleitung"])
        any_filled = True

    if data.get("ganztag") is not None:
        existing = str(df.at[idx, "besonderheiten"] or "")
        if existing == "nan":
            existing = ""
        ganztag_str = "Ganztagsschule" if data["ganztag"] else ""
        if ganztag_str and ganztag_str not in existing:
            df.at[idx, "besonderheiten"] = (
                f"{existing}, {ganztag_str}".strip(", ") if existing else ganztag_str
            )
            any_filled = True

    if data.get("besonderheiten") is not None:
        existing = str(df.at[idx, "besonderheiten"] or "")
        if existing == "nan":
            existing = ""
        new_besond = str(data["besonderheiten"])
        if not existing or existing == "Ganztagsschule":
            df.at[idx, "besonderheiten"] = new_besond
        elif new_besond not in existing:
            df.at[idx, "besonderheiten"] = f"{existing}; {new_besond}"[:200]
        any_filled = True

    if data.get("tuition_monthly_eur") is not None and pd.isna(df.at[idx, "tuition_monthly_eur"]):
        try:
            df.at[idx, "tuition_monthly_eur"] = int(data["tuition_monthly_eur"])
            any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("scholarship_available") is not None and pd.isna(df.at[idx, "scholarship_available"]):
        df.at[idx, "scholarship_available"] = bool(data["scholarship_available"])
        any_filled = True

    return any_filled


def _apply_descriptions(df: pd.DataFrame, idx: int, desc_data: dict) -> bool:
    """Apply generated descriptions (fill gaps only)."""
    any_filled = False
    for field in ["description", "description_de", "summary_en", "summary_de"]:
        if desc_data.get(field):
            val = str(desc_data[field]).strip()
            if val and val != "null" and pd.isna(df.at[idx, field]):
                df.at[idx, field] = val
                any_filled = True
    return any_filled


def _init_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set")
    from google import genai
    return genai.Client(api_key=api_key)


def _get_school_info(row) -> dict:
    snr = row.get("schulnummer", "")
    try:
        snr = str(int(float(snr)))
    except (ValueError, TypeError):
        snr = str(snr)
    return {
        "schulnummer": snr,
        "schulname": str(row.get("schulname", "")),
        "url": str(row.get("website", row.get("homepage", ""))).strip(),
        "strasse": str(row.get("strasse", "")),
        "plz": str(row.get("plz", "")),
        "stadt": str(row.get("stadt", row.get("ort", "Dresden"))),
        "schultyp": str(row.get("schultyp", row.get("school_type", row.get("school_type_name", "")))),
        "traegerschaft": str(row.get("traegerschaft", "")),
    }


def enrich_schools(school_type: str = "secondary") -> pd.DataFrame:
    """Enrich Dresden schools with metadata and descriptions from school websites."""
    logger.info(f"Enriching Dresden {school_type} schools with website metadata + descriptions...")

    fallbacks = INPUT_FALLBACKS_PRIMARY if school_type == "primary" else INPUT_FALLBACKS_SECONDARY
    input_file = None
    for candidate in fallbacks:
        if candidate.exists():
            input_file = candidate
            break

    if not input_file:
        raise FileNotFoundError(f"No input file found for Dresden {school_type}")

    df = pd.read_csv(input_file)
    logger.info(f"  Loaded {len(df)} schools from {input_file.name}")

    try:
        client = _init_gemini_client()
    except (ValueError, ImportError) as e:
        logger.error(str(e))
        return df

    for col in ["schueler_2024_25", "lehrer_2024_25", "sprachen", "gruendungsjahr",
                 "leitung", "besonderheiten", "tuition_monthly_eur", "scholarship_available",
                 "description", "description_de", "summary_en", "summary_de"]:
        if col not in df.columns:
            df[col] = None

    # Use school_type-specific cache keys
    meta_cache_file = CACHE_DIR / f"metadata_cache_{school_type}.json"
    desc_cache_file = CACHE_DIR / f"description_cache_{school_type}.json"
    meta_cache = _load_cache(meta_cache_file)
    desc_cache = _load_cache(desc_cache_file)

    # Also check shared cache
    if not meta_cache and WEBSITE_CACHE.exists():
        meta_cache = _load_cache(WEBSITE_CACHE)
    if not desc_cache and DESCRIPTION_CACHE.exists():
        desc_cache = _load_cache(DESCRIPTION_CACHE)

    stats = {"meta_api": 0, "meta_cache": 0, "desc_api": 0, "desc_cache": 0,
             "meta_enriched": 0, "desc_enriched": 0, "errors": 0}

    # Determine website column
    url_col = "website" if "website" in df.columns else "homepage"
    if url_col not in df.columns:
        logger.warning(f"  No website/homepage column found — skipping")
        return df

    has_website = df[url_col].notna() & (df[url_col].astype(str).str.strip() != "") & (df[url_col].astype(str) != "nan")
    website_indices = df[has_website].index
    total = len(website_indices)
    logger.info(f"  Schools with website: {total}/{len(df)}")

    # ===== PHASE A: Metadata extraction =====
    logger.info("  --- PHASE A: Metadata Extraction ---")
    for i, idx in enumerate(website_indices):
        info = _get_school_info(df.loc[idx])
        snr = info["schulnummer"]

        if snr in meta_cache:
            data = meta_cache[snr]
            stats["meta_cache"] += 1
        else:
            prompt = METADATA_PROMPT.format(
                schulname=info["schulname"], schultyp=info["schultyp"],
                traegerschaft=info["traegerschaft"], strasse=info["strasse"],
                plz=info["plz"], stadt=info["stadt"], url=info["url"],
            )
            data = _call_gemini(client, prompt, METADATA_MODEL, snr, info["schulname"])
            if data:
                meta_cache[snr] = data
            else:
                stats["errors"] += 1
            stats["meta_api"] += 1
            time.sleep(REQUEST_DELAY)

            if stats["meta_api"] % SAVE_INTERVAL == 0:
                _save_cache(meta_cache, meta_cache_file)
                logger.info(f"  Metadata progress: {i + 1}/{total}")

        if data and _apply_metadata(df, idx, data):
            stats["meta_enriched"] += 1

    _save_cache(meta_cache, meta_cache_file)

    # ===== PHASE B: Description generation =====
    logger.info("  --- PHASE B: Description Generation ---")
    for i, idx in enumerate(website_indices):
        info = _get_school_info(df.loc[idx])
        snr = info["schulnummer"]

        if snr in desc_cache:
            desc_data = desc_cache[snr]
            stats["desc_cache"] += 1
        else:
            meta = meta_cache.get(snr, {})
            prompt = DESCRIPTION_PROMPT.format(
                schulname=info["schulname"], schultyp=info["schultyp"],
                traegerschaft=info["traegerschaft"], strasse=info["strasse"],
                plz=info["plz"], stadt=info["stadt"], url=info["url"],
                schueler=meta.get("schueler", "unbekannt"),
                lehrer=meta.get("lehrer", "unbekannt"),
                sprachen=meta.get("sprachen", "unbekannt"),
                gruendungsjahr=meta.get("gruendungsjahr", "unbekannt"),
                schulleitung=meta.get("schulleitung", "unbekannt"),
                besonderheiten=meta.get("besonderheiten", "unbekannt"),
            )
            desc_data = _call_gemini(client, prompt, DESCRIPTION_MODEL, snr, info["schulname"])
            if desc_data:
                desc_cache[snr] = desc_data
            else:
                stats["errors"] += 1
            stats["desc_api"] += 1
            time.sleep(REQUEST_DELAY)

            if stats["desc_api"] % SAVE_INTERVAL == 0:
                _save_cache(desc_cache, desc_cache_file)
                logger.info(f"  Description progress: {i + 1}/{total}")

        if desc_data and _apply_descriptions(df, idx, desc_data):
            stats["desc_enriched"] += 1

    _save_cache(desc_cache, desc_cache_file)

    output_file = INTERMEDIATE_DIR / f"dresden_{school_type}_schools_with_website_metadata.csv"
    # Also keep backward-compat name for secondary
    if school_type == "secondary":
        alt_output = INTERMEDIATE_DIR / "dresden_schools_with_website_metadata.csv"
        df.to_csv(alt_output, index=False, encoding="utf-8-sig")
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"  Saved: {output_file}")

    print(f"\n{'=' * 70}")
    print(f"DRESDEN WEBSITE METADATA ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Total: {len(df)} | Website: {total} | Enriched: {stats['meta_enriched']}")
    print(f"  Meta API: {stats['meta_api']} | Desc API: {stats['desc_api']} | Errors: {stats['errors']}")
    for field, col in [("Schüler", "schueler_2024_25"), ("Lehrer", "lehrer_2024_25"),
                        ("Description (DE)", "description_de")]:
        if col in df.columns:
            filled = df[col].notna().sum()
            print(f"  {field}: {filled}/{len(df)} ({filled/len(df)*100:.0f}%)")
    print(f"{'=' * 70}")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Website Metadata Enrichment")
    logger.info("=" * 60)

    for school_type in ["secondary", "primary"]:
        try:
            enrich_schools(school_type)
        except FileNotFoundError as e:
            logger.warning(f"  Skipping {school_type}: {e}")
        except Exception as e:
            logger.error(f"  Error enriching {school_type}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
