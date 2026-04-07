#!/usr/bin/env python3
"""
NRW Website Metadata Enrichment
=================================

Scrapes school websites using Gemini with URL context and Google Search
grounding to extract metadata not available in the NRW Schulministerium
open data.

Uses the new google-genai package with:
- URL context: Gemini reads the school's website directly (server-side)
- Google Search grounding: Gemini can search the web for additional info

Phase A - Metadata extraction:
- schueler_2024_25: Number of students
- lehrer_2024_25: Number of teachers
- sprachen: Languages offered (comma-separated)
- gruendungsjahr: Founding year
- schulleitung: Principal's name (Schulleiter/in)
- ganztag: Whether it's a Ganztagsschule (bool)
- besonderheiten: Special programs/features (max 150 chars)
- tuition_monthly_eur: Monthly tuition for private schools
- scholarship_available: Whether scholarships are offered

Phase B - Rich description generation:
- description: Comprehensive English school description (250-400 words)
- description_de: Comprehensive German school description (250-400 words)
- summary_en: Short English summary (2-3 sentences)
- summary_de: Short German summary (2-3 sentences)

Results are cached per schulnummer to avoid repeated API calls.

Author: NRW School Data Pipeline
Created: 2026-02-21
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nrw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"
ENV_FILE = PROJECT_ROOT / ".env"

# Cache files
WEBSITE_CACHE = CACHE_DIR / "website_metadata_cache.json"
DESCRIPTION_CACHE = CACHE_DIR / "website_description_cache.json"

# Rate limiting
REQUEST_DELAY = 1.5  # seconds between API calls
SAVE_INTERVAL = 10  # save cache every N schools

# Model selection - gemini-2.5-flash is fast and effective for metadata
# gemini-2.5-pro is better for rich description generation
METADATA_MODEL = "gemini-2.5-flash"
DESCRIPTION_MODEL = "gemini-2.5-flash"

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

# Fields to extract
EXTRACT_FIELDS = [
    "schueler",
    "lehrer",
    "sprachen",
    "gruendungsjahr",
    "schulleitung",
    "ganztag",
    "besonderheiten",
]

METADATA_PROMPT = """Durchsuche diese Schulwebsite gründlich und extrahiere folgende Informationen.
Suche auf der Hauptseite und allen verlinkten Unterseiten (Über uns, Unsere Schule,
Kollegium, Schulprofil, Zahlen und Fakten, etc.).

Schule: {schulname}
Schulform: {schulform}
Trägerschaft: {traegerschaft}
Adresse: {strasse}, {plz} {stadt}

Extrahiere als JSON:
- schueler: Gesamtanzahl Schülerinnen und Schüler (int oder null). Suche nach "SuS", "Schülerinnen und Schüler", "ca. XXX Schüler", "lernen hier XXX Kinder"
- lehrer: Anzahl Lehrkräfte/Kollegium (int oder null). Suche nach "Kollegium", "Lehrerinnen und Lehrer", "Lehrkräfte"
- sprachen: Angebotene Fremdsprachen als Liste (z.B. ["Englisch", "Französisch"]) oder null
- gruendungsjahr: Gründungsjahr der Schule (int, z.B. 1920) oder null
- schulleitung: Name der Schulleitung (string) oder null. Suche nach "Schulleiter/in", "Rektor/in", "Direktor/in"
- ganztag: Ist es eine Ganztagsschule? (true/false/null). Suche nach "Ganztag", "OGS", "Offene Ganztagsschule", "Betreuung"
- besonderheiten: Besondere Programme oder Schwerpunkte, max 150 Zeichen (string oder null). Z.B. MINT, Musik, Sport, UNESCO, Inklusion, bilingual, Montessori
- tuition_monthly_eur: Monatliches Schulgeld in Euro (int oder null). Nur für Privatschulen relevant. Suche nach "Schulgeld", "Beiträge", "Kosten", "Gebühren"
- scholarship_available: Gibt es Stipendien oder Ermäßigungen? (true/false/null). Suche nach "Stipendium", "Ermäßigung", "einkommensabhängig"

Wenn eine Information nicht eindeutig gefunden werden kann, setze den Wert auf null.
Antworte NUR mit dem JSON-Objekt, kein Markdown.

URL: {url}"""

DESCRIPTION_PROMPT = """Du bist ein Experte für Schulprofile. Basierend auf der Website dieser Schule
und allen verfügbaren Informationen, erstelle ein umfassendes Schulprofil.

Schule: {schulname}
Schulform: {schulform}
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
- Sozialindex: {sozialindex}

Antworte als JSON mit genau diesen Feldern:
- description_de: Umfassende deutsche Beschreibung der Schule (250-400 Wörter). Beschreibe Schulprofil,
  pädagogisches Konzept, besondere Angebote, Ausstattung, Lage und Umgebung, Betreuungsangebote.
  Schreibe sachlich-informativ, wie für einen Schulführer.
- description: Englische Übersetzung der deutschen Beschreibung (250-400 Wörter).
- summary_de: Kurze deutsche Zusammenfassung (2-3 Sätze, max 100 Wörter).
- summary_en: Kurze englische Zusammenfassung (2-3 Sätze, max 100 Wörter).

Antworte NUR mit dem JSON-Objekt, kein Markdown."""


def _load_cache(cache_path: Path = WEBSITE_CACHE) -> dict:
    """Load a JSON cache file."""
    if cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache: dict, cache_path: Path = WEBSITE_CACHE):
    """Save a JSON cache file."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def _call_gemini(
    client,
    prompt: str,
    model: str,
    schulnummer: str,
    schulname: str,
    retry_count: int = 0,
) -> Optional[Dict]:
    """
    Call Gemini with URL context + Google Search grounding and parse JSON response.
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
                logger.warning(
                    f"  Empty response for {schulnummer} ({schulname})"
                )
                return None

        text = text.strip()
        # Clean markdown code block if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        data = json.loads(text)
        return data

    except json.JSONDecodeError as e:
        logger.warning(
            f"  JSON parse error for {schulnummer} ({schulname}): {e}"
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
                retry_count + 1,
            )
        else:
            logger.warning(
                f"  Error for {schulnummer} ({schulname}): {e}"
            )
        return None


def scrape_school_metadata(
    client,
    schulnummer: str,
    schulname: str,
    url: str,
    strasse: str,
    plz: str,
    stadt: str,
    schulform: str = "",
    traegerschaft: str = "",
) -> Optional[Dict]:
    """
    Scrape metadata from a single school website using Gemini URL context.
    """
    prompt = METADATA_PROMPT.format(
        schulname=schulname,
        schulform=schulform,
        traegerschaft=traegerschaft,
        strasse=strasse,
        plz=plz,
        stadt=stadt,
        url=url,
    )
    return _call_gemini(client, prompt, METADATA_MODEL, schulnummer, schulname)


def generate_school_description(
    client,
    schulnummer: str,
    schulname: str,
    url: str,
    strasse: str,
    plz: str,
    stadt: str,
    schulform: str = "",
    traegerschaft: str = "",
    metadata: Optional[Dict] = None,
    sozialindex: str = "",
) -> Optional[Dict]:
    """
    Generate rich school description using Gemini URL context + grounding.
    """
    meta = metadata or {}
    prompt = DESCRIPTION_PROMPT.format(
        schulname=schulname,
        schulform=schulform,
        traegerschaft=traegerschaft,
        strasse=strasse,
        plz=plz,
        stadt=stadt,
        url=url,
        schueler=meta.get("schueler", "unbekannt"),
        lehrer=meta.get("lehrer", "unbekannt"),
        sprachen=meta.get("sprachen", "unbekannt"),
        gruendungsjahr=meta.get("gruendungsjahr", "unbekannt"),
        schulleitung=meta.get("schulleitung", "unbekannt"),
        besonderheiten=meta.get("besonderheiten", "unbekannt"),
        sozialindex=sozialindex or "unbekannt",
    )
    return _call_gemini(client, prompt, DESCRIPTION_MODEL, schulnummer, schulname)


def _apply_metadata(df: pd.DataFrame, idx: int, data: dict) -> bool:
    """Apply extracted metadata fields to a DataFrame row. Returns True if any field was set."""
    any_filled = False

    if data.get("schueler") is not None:
        try:
            df.at[idx, "schueler_2024_25"] = int(data["schueler"])
            any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("lehrer") is not None:
        try:
            df.at[idx, "lehrer_2024_25"] = int(data["lehrer"])
            any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("sprachen") is not None:
        if isinstance(data["sprachen"], list):
            df.at[idx, "sprachen"] = ", ".join(data["sprachen"])
        else:
            df.at[idx, "sprachen"] = str(data["sprachen"])
        any_filled = True

    if data.get("gruendungsjahr") is not None:
        try:
            year = int(data["gruendungsjahr"])
            if 1800 <= year <= 2026:
                df.at[idx, "gruendungsjahr"] = year
                any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("schulleitung") is not None:
        df.at[idx, "leitung"] = str(data["schulleitung"])
        any_filled = True

    # Ganztag → fold into besonderheiten
    if data.get("ganztag") is not None:
        if "besonderheiten" not in df.columns:
            df["besonderheiten"] = None
        existing = str(df.at[idx, "besonderheiten"] or "")
        if existing == "nan":
            existing = ""
        ganztag_str = "Ganztagsschule" if data["ganztag"] else ""
        if ganztag_str and ganztag_str not in existing:
            df.at[idx, "besonderheiten"] = (
                f"{existing}, {ganztag_str}".strip(", ")
                if existing
                else ganztag_str
            )
            any_filled = True

    if data.get("besonderheiten") is not None:
        if "besonderheiten" not in df.columns:
            df["besonderheiten"] = None
        existing = str(df.at[idx, "besonderheiten"] or "")
        if existing == "nan":
            existing = ""
        new_besond = str(data["besonderheiten"])
        if not existing or existing == "Ganztagsschule":
            df.at[idx, "besonderheiten"] = new_besond
        elif new_besond not in existing:
            combined = f"{existing}; {new_besond}"
            df.at[idx, "besonderheiten"] = combined[:200]
        any_filled = True

    # Tuition (private schools)
    if data.get("tuition_monthly_eur") is not None:
        try:
            df.at[idx, "tuition_monthly_eur"] = int(data["tuition_monthly_eur"])
            any_filled = True
        except (ValueError, TypeError):
            pass

    if data.get("scholarship_available") is not None:
        df.at[idx, "scholarship_available"] = bool(data["scholarship_available"])
        any_filled = True

    return any_filled


def _apply_descriptions(df: pd.DataFrame, idx: int, desc_data: dict) -> bool:
    """Apply generated descriptions to a DataFrame row. Returns True if any field was set."""
    any_filled = False

    for field in ["description", "description_de", "summary_en", "summary_de"]:
        if desc_data.get(field):
            val = str(desc_data[field]).strip()
            if val and val != "null":
                if field not in df.columns:
                    df[field] = None
                df.at[idx, field] = val
                any_filled = True

    return any_filled


def _init_gemini_client():
    """Initialize the Gemini client."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set")

    from google import genai
    return genai.Client(api_key=api_key)


def _get_school_info(row) -> dict:
    """Extract common school info from a DataFrame row."""
    return {
        "schulnummer": str(int(row["schulnummer"])),
        "schulname": str(row["schulname"]),
        "url": str(row["website"]).strip(),
        "strasse": str(row.get("strasse", "")),
        "plz": str(row.get("plz", "")),
        "stadt": str(row.get("stadt", row.get("ort", ""))),
        "schulform": str(row.get("schulform_name", row.get("school_type", ""))),
        "traegerschaft": str(row.get("traegerschaft", "")),
        "sozialindex": str(row.get("sozialindexstufe", "")),
    }


def enrich_schools(school_type: str = "secondary") -> pd.DataFrame:
    """
    Enrich NRW schools with metadata and descriptions scraped from school websites.

    Phase A: Extract factual metadata (schueler, lehrer, sprachen, etc.)
    Phase B: Generate rich descriptions (description, description_de, summaries)
    """
    logger.info(
        f"Enriching {school_type} schools with website metadata + descriptions..."
    )

    # Find input file (read from most enriched intermediate)
    input_file = None
    for suffix in ["with_anmeldezahlen", "with_pois", "with_crime", "with_transit"]:
        candidate = INTERMEDIATE_DIR / f"nrw_{school_type}_schools_{suffix}.csv"
        if candidate.exists():
            input_file = candidate
            break

    if not input_file:
        raise FileNotFoundError(f"No intermediate file found for {school_type}")

    df = pd.read_csv(input_file)
    logger.info(f"  Loaded {len(df)} {school_type} schools from {input_file.name}")

    # Initialize Gemini client
    try:
        client = _init_gemini_client()
    except (ValueError, ImportError) as e:
        logger.error(str(e))
        return df

    # Ensure columns exist
    for col in ["schueler_2024_25", "lehrer_2024_25", "sprachen", "gruendungsjahr",
                 "leitung", "besonderheiten", "tuition_monthly_eur", "scholarship_available",
                 "description", "description_de", "summary_en", "summary_de"]:
        if col not in df.columns:
            df[col] = None

    # Load caches
    meta_cache = _load_cache(WEBSITE_CACHE)
    desc_cache = _load_cache(DESCRIPTION_CACHE)

    # Stats
    stats = {"meta_api": 0, "meta_cache": 0, "desc_api": 0, "desc_cache": 0,
             "meta_enriched": 0, "desc_enriched": 0, "errors": 0}

    # Filter to schools with websites
    has_website = df["website"].notna() & (
        df["website"].astype(str).str.strip() != ""
    )
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
            data = scrape_school_metadata(
                client, snr, info["schulname"], info["url"],
                info["strasse"], info["plz"], info["stadt"],
                info["schulform"], info["traegerschaft"],
            )
            if data:
                meta_cache[snr] = data
            else:
                stats["errors"] += 1
            stats["meta_api"] += 1
            time.sleep(REQUEST_DELAY)

            if stats["meta_api"] % SAVE_INTERVAL == 0:
                _save_cache(meta_cache, WEBSITE_CACHE)
                logger.info(
                    f"  Metadata progress: {i + 1}/{total} "
                    f"(API: {stats['meta_api']}, cache: {stats['meta_cache']})"
                )

        if data and _apply_metadata(df, idx, data):
            stats["meta_enriched"] += 1

    _save_cache(meta_cache, WEBSITE_CACHE)
    logger.info(f"  Metadata phase complete: {stats['meta_enriched']} enriched, "
                f"{stats['meta_api']} API calls, {stats['meta_cache']} cache hits")

    # ===== PHASE B: Description generation =====
    logger.info("  --- PHASE B: Description Generation ---")
    for i, idx in enumerate(website_indices):
        info = _get_school_info(df.loc[idx])
        snr = info["schulnummer"]

        if snr in desc_cache:
            desc_data = desc_cache[snr]
            stats["desc_cache"] += 1
        else:
            # Use metadata from Phase A for context
            meta = meta_cache.get(snr, {})
            desc_data = generate_school_description(
                client, snr, info["schulname"], info["url"],
                info["strasse"], info["plz"], info["stadt"],
                info["schulform"], info["traegerschaft"],
                metadata=meta, sozialindex=info["sozialindex"],
            )
            if desc_data:
                desc_cache[snr] = desc_data
            else:
                stats["errors"] += 1
            stats["desc_api"] += 1
            time.sleep(REQUEST_DELAY)

            if stats["desc_api"] % SAVE_INTERVAL == 0:
                _save_cache(desc_cache, DESCRIPTION_CACHE)
                logger.info(
                    f"  Description progress: {i + 1}/{total} "
                    f"(API: {stats['desc_api']}, cache: {stats['desc_cache']})"
                )

        if desc_data and _apply_descriptions(df, idx, desc_data):
            stats["desc_enriched"] += 1

    _save_cache(desc_cache, DESCRIPTION_CACHE)
    logger.info(f"  Description phase complete: {stats['desc_enriched']} enriched, "
                f"{stats['desc_api']} API calls, {stats['desc_cache']} cache hits")

    # Save output
    output_file = (
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_website_metadata.csv"
    )
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"  Saved: {output_file}")

    # Summary
    print(f"\n{'=' * 70}")
    print(f"NRW WEBSITE METADATA ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Total schools: {len(df)}")
    print(f"  With website: {total}")
    print(f"  Metadata: {stats['meta_api']} API / {stats['meta_cache']} cache / {stats['meta_enriched']} enriched")
    print(f"  Descriptions: {stats['desc_api']} API / {stats['desc_cache']} cache / {stats['desc_enriched']} enriched")
    print(f"  Errors: {stats['errors']}")
    print()

    for field, col in [
        ("Schüler", "schueler_2024_25"),
        ("Lehrer", "lehrer_2024_25"),
        ("Sprachen", "sprachen"),
        ("Gründungsjahr", "gruendungsjahr"),
        ("Schulleitung", "leitung"),
        ("Besonderheiten", "besonderheiten"),
        ("Description (EN)", "description"),
        ("Description (DE)", "description_de"),
        ("Summary (EN)", "summary_en"),
        ("Summary (DE)", "summary_de"),
        ("Tuition", "tuition_monthly_eur"),
    ]:
        if col in df.columns:
            filled = df[col].notna().sum()
            if df[col].dtype == object:
                filled = (
                    df[col].notna()
                    & (df[col].astype(str).str.strip() != "")
                    & (df[col].astype(str) != "nan")
                ).sum()
            pct = filled / len(df) * 100
            print(f"  {field}: {filled}/{len(df)} ({pct:.1f}%)")

    print(f"{'=' * 70}")

    return df


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting NRW Website Metadata Enrichment")
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
