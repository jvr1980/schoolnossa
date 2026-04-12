#!/usr/bin/env python3
"""
NRW Tuition Fee Enrichment via Firecrawl + Gemini
==================================================
Deep-crawls private school websites using Firecrawl to find fee/tuition pages,
then extracts structured tuition data using Gemini.

Updates both primary and secondary parquet/CSV outputs in data_nrw/final/.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

# Paths
DATA_DIR = PROJECT_ROOT / "data_nrw"
FINAL_DIR = DATA_DIR / "final"
CACHE_DIR = DATA_DIR / "cache"
CACHE_FILE = CACHE_DIR / "tuition_firecrawl_cache.json"

# Tuition keywords for URL filtering
TUITION_KEYWORDS = [
    "schulgeld", "kosten", "gebühr", "gebuehr", "beitr", "aufnahme",
    "anmeldung", "finanz", "stipend", "fees", "tuition", "admission",
    "pricing", "elternbeitrag", "schulvertrag", "preise",
]

TUITION_PROMPT = """Du bist ein Experte für Schulfinanzierung. Analysiere den folgenden Webseiteninhalt
einer Privatschule und extrahiere ALLE verfügbaren Informationen zu Schulgebühren und Kosten.

Schule: {schulname}
Schulform: {schulform}
Stadt: {stadt}

Webseiteninhalt:
{content}

Extrahiere als JSON mit genau diesen Feldern:
- tuition_monthly_eur: Monatliches Schulgeld in EUR (float oder null). Bei Spannweite den Durchschnitt nehmen.
- tuition_annual_eur: Jährliches Schulgeld in EUR (float oder null). Berechne aus Monatsbetrag × 12 wenn nur monatlich angegeben.
- registration_fee_eur: Einmalige Aufnahme-/Anmeldegebühr in EUR (float oder null)
- material_fee_annual_eur: Jährliche Material-/Lernmittelgebühr in EUR (float oder null)
- meal_plan_monthly_eur: Monatliche Verpflegungskosten/Mittagessen in EUR (float oder null)
- after_school_care_monthly_eur: Monatliche Kosten für Nachmittagsbetreuung/OGS/Hort in EUR (float oder null)
- scholarship_available: Gibt es Stipendien oder Ermäßigungen? (true/false/null)
- income_based_tuition: Ist das Schulgeld einkommensabhängig? (true/false/null)
- tuition_notes: Wichtige Zusatzinfos zu Gebühren, max 200 Zeichen. Z.B. "Staffelung nach Einkommen: 150-400 EUR/Monat" oder "Geschwisterrabatt verfügbar" oder "Schulgeld auf Anfrage"
- found_tuition_info: Wurden auf der Website Informationen zu Schulgebühren gefunden? (true/false)

WICHTIG:
- Bei einkommensabhängigen Beiträgen: Nenne den Mittelwert als tuition_monthly_eur und beschreibe die Staffelung in tuition_notes
- Bei "auf Anfrage": Setze tuition_monthly_eur auf null und tuition_notes auf "Schulgeld auf Anfrage"
- Bei kirchlichen Ersatzschulen (Erzbistum etc.): Diese sind oft kostenlos oder haben minimale Beiträge
- Waldorfschulen: Haben typischerweise einkommensabhängige Beiträge
- Wenn KEINE Gebühreninformationen gefunden werden, setze found_tuition_info auf false und alle anderen Felder auf null

Antworte NUR mit dem JSON-Objekt, kein Markdown, keine Erklärung."""


def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def get_private_schools() -> pd.DataFrame:
    """Load all private schools from both primary and secondary datasets."""
    rows = []
    for school_type in ["primary", "secondary"]:
        pq_path = FINAL_DIR / f"nrw_{school_type}_school_master_table_final_with_embeddings.parquet"
        if not pq_path.exists():
            log.warning(f"Not found: {pq_path}")
            continue
        df = pd.read_parquet(pq_path)
        mask = df["traegerschaft"].astype(str).str.contains("rivat", na=False)
        priv = df[mask][["schulnummer", "schulname", "schulart", "website", "stadt",
                          "tuition_monthly_eur"]].copy()
        priv["school_type"] = school_type
        rows.append(priv)

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def discover_fee_urls(app, base_url: str) -> list[str]:
    """Use Firecrawl map to find fee-related pages on a school website."""
    try:
        result = app.map(base_url, limit=50, ignore_query_parameters=True)
        all_links = result.links or []
        log.info(f"  Map found {len(all_links)} URLs on {base_url}")

        # Filter for fee-related pages
        # LinkResult objects may have .url or be strings — normalize
        fee_urls = []
        for link in all_links:
            link_str = str(getattr(link, "url", link))
            link_lower = link_str.lower()
            if any(kw in link_lower for kw in TUITION_KEYWORDS):
                fee_urls.append(link_str)

        # Always include the base URL
        if base_url not in fee_urls:
            fee_urls.insert(0, base_url)

        # If no fee-specific URLs found, try common subpaths
        if len(fee_urls) <= 1:
            for suffix in ["/aufnahme", "/kosten", "/schulgeld", "/anmeldung",
                           "/eltern/schulgeld", "/ueber-uns/kosten", "/admission",
                           "/fees", "/finanzierung"]:
                candidate = base_url.rstrip("/") + suffix
                if candidate not in fee_urls:
                    fee_urls.append(candidate)

        return fee_urls[:8]  # Cap at 8 pages per school
    except Exception as e:
        log.warning(f"  Map failed for {base_url}: {e}")
        return [base_url]


def scrape_pages(app, urls: list[str]) -> str:
    """Scrape multiple URLs and combine their markdown content."""
    combined = []
    for url in urls:
        try:
            doc = app.scrape(url, formats=["markdown"], only_main_content=True, timeout=30000)
            md = doc.markdown or ""
            if md.strip():
                combined.append(f"--- PAGE: {url} ---\n{md[:5000]}")
                log.info(f"    Scraped {url}: {len(md)} chars")
            else:
                log.info(f"    Empty content from {url}")
        except Exception as e:
            log.warning(f"    Scrape failed for {url}: {e}")

    return "\n\n".join(combined)[:20000]  # Cap total content for Gemini


def extract_tuition_with_gemini(content: str, schulname: str, schulform: str, stadt: str) -> dict | None:
    """Use Gemini to extract structured tuition data from crawled content."""
    from google import genai

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log.error("GEMINI_API_KEY not set")
        return None

    client = genai.Client(api_key=api_key)

    prompt = TUITION_PROMPT.format(
        schulname=schulname,
        schulform=schulform,
        stadt=stadt,
        content=content,
    )

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        text = response.text.strip()
        # Strip markdown code fences if present
        text = re.sub(r"^```json\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception as e:
        log.error(f"  Gemini extraction failed for {schulname}: {e}")
        return None


def build_tuition_display(data: dict, schulname: str) -> str:
    """Build a human-readable tuition display string."""
    if not data or not data.get("found_tuition_info"):
        return "Privat (Details auf Schulwebsite)"

    parts = []
    monthly = data.get("tuition_monthly_eur")
    if monthly is not None and monthly > 0:
        parts.append(f"{monthly:.0f} EUR/Monat")
    elif monthly == 0:
        parts.append("Kostenfrei (private Ersatzschule)")

    if data.get("income_based_tuition"):
        parts.append("einkommensabhängig")
    if data.get("scholarship_available"):
        parts.append("Stipendien verfügbar")

    notes = data.get("tuition_notes", "")
    if notes and not parts:
        return notes[:100]

    if parts:
        return " | ".join(parts)
    return "Privat (Details auf Schulwebsite)"


def apply_tuition_to_parquet(school_type: str, tuition_results: dict):
    """Apply extracted tuition data to a parquet file."""
    pq_path = FINAL_DIR / f"nrw_{school_type}_school_master_table_final_with_embeddings.parquet"
    csv_path = FINAL_DIR / f"nrw_{school_type}_school_master_table_final.csv"

    if not pq_path.exists():
        log.warning(f"Not found: {pq_path}")
        return

    df = pd.read_parquet(pq_path)
    updated = 0

    tuition_cols = [
        "tuition_monthly_eur", "tuition_annual_eur", "registration_fee_eur",
        "material_fee_annual_eur", "meal_plan_monthly_eur", "after_school_care_monthly_eur",
        "scholarship_available", "income_based_tuition", "tuition_notes",
        "tuition_source_url", "tuition_display",
    ]
    for col in tuition_cols:
        if col not in df.columns:
            df[col] = None

    for snr_str, data in tuition_results.items():
        snr = int(snr_str)
        mask = df["schulnummer"] == snr
        if not mask.any():
            continue

        idx = df.index[mask][0]
        if not data.get("found_tuition_info"):
            continue

        for field in ["tuition_monthly_eur", "tuition_annual_eur", "registration_fee_eur",
                       "material_fee_annual_eur", "meal_plan_monthly_eur",
                       "after_school_care_monthly_eur"]:
            val = data.get(field)
            if val is not None:
                try:
                    df.at[idx, field] = float(val)
                except (ValueError, TypeError):
                    pass

        for field in ["scholarship_available", "income_based_tuition"]:
            val = data.get(field)
            if val is not None:
                df.at[idx, field] = bool(val)

        if data.get("tuition_notes"):
            df.at[idx, "tuition_notes"] = str(data["tuition_notes"])[:200]

        if data.get("source_url"):
            df.at[idx, "tuition_source_url"] = data["source_url"]

        display = build_tuition_display(data, df.at[idx, "schulname"])
        df.at[idx, "tuition_display"] = display
        updated += 1

    df.to_parquet(pq_path, index=False)
    # Also update CSV (without embedding column for readability)
    csv_cols = [c for c in df.columns if c != "embedding"]
    df[csv_cols].to_csv(csv_path, index=False)

    log.info(f"  Updated {updated} schools in {school_type} ({pq_path.name})")


def main():
    log.info("=" * 60)
    log.info("NRW TUITION FEE ENRICHMENT (Firecrawl + Gemini)")
    log.info("=" * 60)

    fc_key = os.environ.get("FIRECRAWL_API_KEY")
    if not fc_key:
        log.error("FIRECRAWL_API_KEY not set in .env")
        sys.exit(1)

    from firecrawl import FirecrawlApp
    app = FirecrawlApp(api_key=fc_key)

    # Load private schools
    schools = get_private_schools()
    log.info(f"Found {len(schools)} private school records")

    # Deduplicate by schulnummer (same school appears in both primary and secondary)
    unique = schools.drop_duplicates(subset="schulnummer")
    log.info(f"Unique schools to process: {len(unique)}")

    # Load cache
    cache = load_cache()
    log.info(f"Cache has {len(cache)} entries")

    results = {}
    processed = 0
    skipped_cache = 0
    skipped_no_url = 0

    for _, row in unique.iterrows():
        snr = str(int(row["schulnummer"]))
        url = str(row.get("website", ""))
        schulname = row["schulname"]
        schulform = row["schulart"]
        stadt = row["stadt"]

        # Skip if cached and has data
        if snr in cache:
            results[snr] = cache[snr]
            skipped_cache += 1
            continue

        # Skip if no website
        if not url or url == "None" or url == "nan":
            log.info(f"  [{snr}] {schulname}: No website, skipping")
            skipped_no_url += 1
            results[snr] = {"found_tuition_info": False}
            continue

        # Ensure URL has scheme
        if not url.startswith("http"):
            url = "https://" + url

        log.info(f"\n[{snr}] {schulname} ({schulform}) [{stadt}]")
        log.info(f"  URL: {url}")

        # Step 1: Map the website to find fee-related pages
        fee_urls = discover_fee_urls(app, url)
        log.info(f"  Fee-related URLs: {len(fee_urls)}")

        # Step 2: Scrape the fee pages
        content = scrape_pages(app, fee_urls)
        if not content.strip():
            log.warning(f"  No content scraped for {schulname}")
            data = {"found_tuition_info": False}
            results[snr] = data
            cache[snr] = data
            save_cache(cache)
            continue

        # Step 3: Extract tuition with Gemini
        data = extract_tuition_with_gemini(content, schulname, schulform, stadt)
        if data is None:
            data = {"found_tuition_info": False}

        # Track which URL had the best content
        if fee_urls:
            data["source_url"] = fee_urls[0]

        results[snr] = data
        cache[snr] = data
        save_cache(cache)
        processed += 1

        # Rate limiting
        time.sleep(1)

    log.info(f"\n{'=' * 60}")
    log.info(f"CRAWL COMPLETE")
    log.info(f"  Processed: {processed}")
    log.info(f"  From cache: {skipped_cache}")
    log.info(f"  No website: {skipped_no_url}")

    # Count results
    found = sum(1 for d in results.values() if d.get("found_tuition_info"))
    with_amount = sum(1 for d in results.values()
                      if d.get("tuition_monthly_eur") is not None and d.get("tuition_monthly_eur") != 0)
    log.info(f"  Found tuition info: {found}/{len(results)}")
    log.info(f"  With monthly amount: {with_amount}/{len(results)}")

    # Apply to both primary and secondary parquets
    log.info(f"\nApplying results to parquet files...")
    for school_type in ["primary", "secondary"]:
        apply_tuition_to_parquet(school_type, results)

    # Print summary
    log.info(f"\n{'=' * 60}")
    log.info(f"TUITION ENRICHMENT RESULTS")
    log.info(f"{'=' * 60}")
    for snr, data in sorted(results.items()):
        name = unique[unique["schulnummer"] == int(snr)]["schulname"].values
        name = name[0] if len(name) > 0 else f"SNR {snr}"
        found_str = "YES" if data.get("found_tuition_info") else "NO"
        monthly = data.get("tuition_monthly_eur", "—")
        notes = (data.get("tuition_notes") or "")[:60]
        log.info(f"  [{snr}] {name[:50]}")
        log.info(f"    Found: {found_str} | Monthly: {monthly} EUR | {notes}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
