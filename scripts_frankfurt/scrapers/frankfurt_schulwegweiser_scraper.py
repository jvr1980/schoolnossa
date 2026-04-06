#!/usr/bin/env python3
"""
Frankfurt Schulwegweiser PRIMARY Scraper

Scrapes frankfurt.de/schulwegweiser as the AUTHORITATIVE source for all
Frankfurt school data — replacing Hessen Verzeichnis 6 as Phase 1.

Covers all 4 school categories:
  - Grundschulen              (~120, pipeline_type=primary)
  - Weiterführende allgemein  (~100, pipeline_type=secondary)
  - Förderschulen             (~19,  pipeline_type=secondary)
  - Weiterführende beruflich  (~40,  pipeline_type=vocational)

Fields extracted per school:
  Core:        schulname, school_type, schulkategorie, schulform_raw, klassenstufe,
               traegerschaft, strasse, plz, ort, ortsteil (Stadtteil)
  Contact:     telefon, fax, email, website
  Staff:       schulleitung, stv_schulleitung
  Academic:    profil, fruehe_fremdsprache, erste/zweite/dritte_fremdsprache,
               unterrichtssprache, sprachen (combined)
  Enrichment:  ganztagsform (Einrichtungsart), besonderheiten (Besondere Angebote),
               besonderheiten_erlaeuterung, auszeichnungen, foerderverein
  Stats:       schueler_gesamt, klassenzahl, unterrichtszeit
  Special:     foerderschwerpunkt (Förderschulen), berufsbereiche + ausbildungsberufe
               (Berufliche Schulen), namensgebung, schulbibliothek
  Geocoding:   latitude, longitude (Nominatim from address)
  Meta:        sw_portal_url, sw_portal_slug, data_source, data_retrieved

Output:
  data_frankfurt/raw/frankfurt_primary_schools.csv    (Grundschulen)
  data_frankfurt/raw/frankfurt_secondary_schools.csv  (Weiterf. allgemein + Förderschulen)
  data_frankfurt/raw/frankfurt_vocational_schools.csv (Weiterf. beruflich)

Author: Frankfurt School Data Pipeline
Created: 2026-04-06 (rebuilt as primary source)
"""

import json
import logging
import re
import time
import requests
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR     = PROJECT_ROOT / "data_frankfurt"
RAW_DIR      = DATA_DIR / "raw"
CACHE_DIR    = DATA_DIR / "cache"

# ── Portal config ─────────────────────────────────────────────────────────────
BASE_URL            = "https://frankfurt.de"
SCHULWEGWEISER_BASE = f"{BASE_URL}/themen/arbeit-bildung-und-wissenschaft/bildung/schulwegweiser"

CATEGORIES = {
    "grundschulen": {
        "label":         "Grundschulen",
        "url":           f"{SCHULWEGWEISER_BASE}/grundschulen",
        "last_page":     5,   # pages 0-5 (6 pages × 20 = ~120)
        "pipeline_type": "primary",
    },
    "weiterfuehrende-allgemeinbildende-schulen": {
        "label":         "Weiterführende allgemeinbildende Schulen",
        "url":           f"{SCHULWEGWEISER_BASE}/weiterfuehrende-allgemeinbildende-schulen",
        "last_page":     4,   # pages 0-4 (5 pages × 20 = ~100)
        "pipeline_type": "secondary",
    },
    "foerderschulen": {
        "label":         "Förderschulen",
        "url":           f"{SCHULWEGWEISER_BASE}/foerderschulen",
        "last_page":     0,   # single page (~19)
        "pipeline_type": "secondary",
    },
    "weiterfuehrende-berufliche-schulen": {
        "label":         "Weiterführende berufliche Schulen",
        "url":           f"{SCHULWEGWEISER_BASE}/weiterfuehrende-berufliche-schulen",
        "last_page":     1,   # pages 0-1 (2 pages × 20 = ~40)
        "pipeline_type": "vocational",
    },
}

# Cache
CACHE_FILE = CACHE_DIR / "schulwegweiser_primary_cache.json"

# ── Field parser constants ─────────────────────────────────────────────────────
# All known label strings on detail pages
KNOWN_LABELS = {
    "Schulleitung", "Stellvertretende Schulleitung",
    "Schülerzahl", "Klassenzahl",
    "Unterrichtszeit", "Erläuterung der Unterrichtszeit",
    "Schulform", "Bemerkung zur Schulform", "Klassenstufe",
    "Trägerschaft", "Träger", "Form der Privatschule",
    "Profile", "Sonstige Profile",
    "Förderschwerpunkt",
    "Unterrichtssprache",
    "Frühe Fremdsprache",
    "Erste Fremdsprache", "Zweite Fremdsprache", "Dritte Fremdsprache",
    "Art des Angebots", "Einrichtungsart",
    "Besondere Angebote", "Erläuterung der Angebote",
    "Auszeichnungen", "Förderverein", "Schulbibliothek", "Namensgebung",
    "Berufsbereiche", "Ausbildungsberufe",
    "Stadtteil",
    # Contact block
    "Telefon", "Fax", "E-Mail", "Internet",
}

# Labels where only the first value matters
SINGLE_VALUE_LABELS = {
    "Schulleitung", "Stellvertretende Schulleitung",
    "Schülerzahl", "Klassenzahl",
    "Unterrichtszeit", "Erläuterung der Unterrichtszeit",
    "Schulform",  # actually multi but we join
    "Klassenstufe", "Trägerschaft",
    "Träger", "Form der Privatschule",
    "Unterrichtssprache",
    "Art des Angebots", "Einrichtungsart",
    "Erläuterung der Angebote",
    "Förderverein", "Schulbibliothek", "Namensgebung",
    "Bemerkung zur Schulform",
    "Stadtteil", "Telefon", "Fax", "E-Mail", "Internet",
}

# Lines to skip entirely
SKIP_LINES = {
    "INHALTE TEILEN", "teilen", "tweet", "mitteilen", "mail",
    "External Link", "Internal Link", "Stadtplan",
    "Änderungsmeldung Schulwegweiser",
    "Wenn Sie melden wollen, dass auf dieser Seite Änderungen nötig sind, klicken Sie bitte hier:",
    "ZUM ANFANG",
}


# ── Cache helpers ─────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ── Text parser ───────────────────────────────────────────────────────────────

def parse_detail_text(page_text: str, school_name: str) -> dict:
    """
    Parse the innerText of a Schulwegweiser detail page into a structured dict.

    Returns a dict mapping field names (German labels) to lists of string values.
    Special keys: '_strasse', '_plz', '_ort' for the address block.
    """
    lines = [l.strip() for l in page_text.split("\n") if l.strip()]

    # ── Locate content start ──────────────────────────────────────────────────
    # Strategy: find the school name in proper case that is followed by a known
    # label within the next 10 lines (accounts for optional photo-credit line).
    # Fallback: first known label after the page header (line 5+).
    content_start = None

    name_occurrences = [i for i, line in enumerate(lines) if line == school_name]
    for idx in name_occurrences:
        for j in range(idx + 1, min(idx + 12, len(lines))):
            if lines[j] in KNOWN_LABELS:
                content_start = j
                break
        if content_start is not None:
            break

    if content_start is None:
        # Fallback: first known label anywhere after line 4
        for i in range(4, len(lines)):
            if lines[i] in KNOWN_LABELS:
                content_start = i
                break

    if content_start is None:
        logger.warning(f"  Could not find content start for {school_name!r}")
        return {}

    # ── Locate share block (end of main content) ──────────────────────────────
    share_idx = len(lines)
    for i in range(content_start, len(lines)):
        if lines[i] == "INHALTE TEILEN":
            share_idx = i
            break

    # ── Parse main content block ──────────────────────────────────────────────
    raw = {}  # dict[str, list[str]]
    current_label = None

    for i in range(content_start, share_idx):
        line = lines[i]
        if line in SKIP_LINES:
            continue
        if line in KNOWN_LABELS:
            current_label = line
            if current_label not in raw:
                raw[current_label] = []
        elif current_label and line:
            # Exclude obvious photo-credit lines
            if "©" in line and "Foto:" in line:
                continue
            raw[current_label].append(line)

    # ── Parse contact block (after share section) ─────────────────────────────
    # Structure: [skip buttons] [category group] [school name] [street] [PLZ city] [Tel] ...
    contact_pos = share_idx + 1
    # Skip the share buttons (teilen, tweet, mitteilen, mail) and category group
    for i in range(contact_pos, min(contact_pos + 8, len(lines))):
        line = lines[i]
        if re.match(r"^\d{5}\s+", line):
            # Found PLZ+city line
            m = re.match(r"^(\d{5})\s+(.+)$", line)
            if m:
                raw["_plz"] = [m.group(1)]
                raw["_ort"] = [m.group(2)]
            # Street is one line before
            if i > 0 and lines[i - 1] not in SKIP_LINES and lines[i - 1] != school_name:
                raw["_strasse"] = [lines[i - 1]]
            # Continue parsing contact labels from here
            for j in range(i + 1, len(lines)):
                ln = lines[j]
                if ln in KNOWN_LABELS:
                    current_label = ln
                    if current_label not in raw:
                        raw[current_label] = []
                elif current_label and ln:
                    if ln in SKIP_LINES or ln.startswith("©") or ln in {"Stadtplan", "KONTAKTE"}:
                        break
                    raw[current_label].append(ln)
                if ln in {"KONTAKTE", "Stadtplan", "ZUM ANFANG"}:
                    break
            break

    return raw


def extract_record(raw: dict, school_name: str, category_slug: str, portal_url: str) -> dict:
    """Convert raw label→values dict into the final flat record."""

    def first(key: str, default=None):
        vals = raw.get(key, [])
        return vals[0] if vals else default

    def joined(key: str, sep: str = ", ", default=None):
        vals = raw.get(key, [])
        return sep.join(vals) if vals else default

    # Schulform: take first specific type as school_type
    schulform_vals = raw.get("Schulform", [])
    SPECIFIC_TYPES = {
        "Gymnasium", "Gesamtschule", "Realschule", "Hauptschule",
        "Grundschule", "Förderschule", "Berufliche Schule",
        "Fachoberschule", "Fachschule", "Berufsfachschule",
        "Abendgymnasium", "Abendhaupt- und Abendrealschule",
    }
    school_type = None
    for sf in schulform_vals:
        if sf in SPECIFIC_TYPES:
            school_type = sf
            break
    if school_type is None and schulform_vals:
        school_type = schulform_vals[0]
    # Fallback from category
    if school_type is None:
        school_type = {
            "grundschulen": "Grundschule",
            "foerderschulen": "Förderschule",
            "weiterfuehrende-berufliche-schulen": "Berufliche Schule",
        }.get(category_slug, "Weiterführende Schule")

    # Combine all Fremdsprachen into a single sprachen column
    sprachen_parts = []
    for key in ["Frühe Fremdsprache", "Erste Fremdsprache",
                "Zweite Fremdsprache", "Dritte Fremdsprache"]:
        sprachen_parts.extend(raw.get(key, []))
    sprachen = ", ".join(sprachen_parts) if sprachen_parts else None

    # schueler_gesamt: strip non-numeric
    schueler_raw = first("Schülerzahl")
    schueler = None
    if schueler_raw:
        m = re.search(r"\d+", str(schueler_raw).replace(".", ""))
        schueler = int(m.group()) if m else None

    # klassenzahl
    kl_raw = first("Klassenzahl")
    klassenzahl = None
    if kl_raw:
        m = re.search(r"\d+", str(kl_raw))
        klassenzahl = int(m.group()) if m else None

    return {
        # Identity
        "schulnummer":              None,  # filled by Verzeichnis 6 join
        "schulname":                school_name,
        "school_type":              school_type,
        "schulkategorie":           category_slug,
        "schulform_raw":            joined("Schulform"),
        "klassenstufe":             first("Klassenstufe"),
        # Location
        "strasse":                  first("_strasse"),
        "plz":                      first("_plz"),
        "ort":                      first("_ort") or "Frankfurt am Main",
        "stadt":                    "Frankfurt am Main",
        "bundesland":               "Hessen",
        "ortsteil":                 first("Stadtteil"),
        "latitude":                 None,  # geocoded later
        "longitude":                None,
        # Contact
        "telefon":                  first("Telefon"),
        "fax":                      first("Fax"),
        "email":                    first("E-Mail"),
        "website":                  first("Internet"),
        # Administrative
        "traegerschaft":            first("Trägerschaft"),
        "traeger_name":             first("Träger"),
        "form_der_privatschule":    first("Form der Privatschule"),
        # Staff
        "schulleitung":             first("Schulleitung"),
        "stv_schulleitung":         first("Stellvertretende Schulleitung"),
        # Statistics
        "schueler_gesamt":          schueler,
        "klassenzahl":              klassenzahl,
        "unterrichtszeit":          first("Unterrichtszeit"),
        "unterrichtszeit_erlaeuterung": first("Erläuterung der Unterrichtszeit"),
        # Academics
        "profile":                  joined("Profile"),
        "sonstige_profile":         joined("Sonstige Profile"),
        "foerderschwerpunkt":       joined("Förderschwerpunkt"),
        "unterrichtssprache":       first("Unterrichtssprache"),
        "fruehe_fremdsprache":      joined("Frühe Fremdsprache"),
        "erste_fremdsprache":       joined("Erste Fremdsprache"),
        "zweite_fremdsprache":      joined("Zweite Fremdsprache"),
        "dritte_fremdsprache":      joined("Dritte Fremdsprache"),
        "sprachen":                 sprachen,
        # Day-care / all-day
        "art_des_angebots":         joined("Art des Angebots"),
        "ganztagsform":             first("Einrichtungsart"),
        # Offerings
        "besonderheiten":           joined("Besondere Angebote"),
        "besonderheiten_erlaeuterung": first("Erläuterung der Angebote"),
        "auszeichnungen":           joined("Auszeichnungen"),
        "foerderverein":            first("Förderverein"),
        "schulbibliothek":          first("Schulbibliothek"),
        "namensgebung":             first("Namensgebung"),
        # Vocational only
        "berufsbereiche":           joined("Berufsbereiche"),
        "ausbildungsberufe":        joined("Ausbildungsberufe"),
        "schulform_bemerkung":      first("Bemerkung zur Schulform"),
        # ndH — filled by optional Verzeichnis 6 join
        "ndh_count":                None,
        # Portal meta
        "sw_portal_url":            portal_url,
        "sw_portal_slug":           portal_url.rstrip("/").split("/")[-1],
        "data_source":              "Frankfurt Schulwegweiser",
        "data_retrieved":           datetime.now().isoformat()[:10],
    }


# ── Geocoding ─────────────────────────────────────────────────────────────────

_GEOCODE_CACHE = {}  # dict[str, tuple]

def geocode_address(strasse: str, plz: str, city: str = "Frankfurt am Main"):
    """Nominatim geocoding with in-memory cache and 1 req/sec rate limiting."""
    if not strasse or not plz:
        return None
    key = f"{strasse}|{plz}"
    if key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[key]

    query = f"{strasse}, {plz} {city}, Deutschland"
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 1, "countrycodes": "de"}
    headers = {"User-Agent": "SchoolNossa/1.0 (schooldata@example.com)"}
    try:
        time.sleep(1.0)  # Nominatim rate limit
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        data = resp.json()
        if data:
            result = (float(data[0]["lat"]), float(data[0]["lon"]))
            _GEOCODE_CACHE[key] = result
            return result
    except Exception as exc:
        logger.warning(f"  Geocoding failed for {query!r}: {exc}")
    _GEOCODE_CACHE[key] = None
    return None


# ── Playwright scraping ───────────────────────────────────────────────────────

def scrape_category(category_slug, cat_cfg, force_rescrape=False):
    """
    Scrape all schools in one category.

    Returns list of raw school dicts (before geocoding).
    Uses/updates CACHE_FILE.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    cache = load_cache()
    cat_key = f"cat_{category_slug}"

    # Check if already fully scraped
    if not force_rescrape and cat_key in cache:
        logger.info(f"  Using cached data for {category_slug} ({len(cache[cat_key])} schools)")
        return cache[cat_key]

    cat_url   = cat_cfg["url"]
    last_page = cat_cfg["last_page"]
    n_pages   = last_page + 1

    records = []
    detail_links = []   # [{name, url, slug}]

    # Load slug→url→record cache for partial resumption
    scraped_slugs: set[str] = {r.get("sw_portal_slug", "") for r in cache.get(cat_key, [])}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="de-DE",
            viewport={"width": 1280, "height": 900},
        )
        _accepted_cookies = False

        def accept_cookies(page):
            nonlocal _accepted_cookies
            if _accepted_cookies:
                return
            try:
                page.click("button:has-text('Alle akzeptieren')", timeout=2_500)
                _accepted_cookies = True
                time.sleep(0.5)
            except Exception:
                pass

        # ── Phase A: collect all school links from list pages ─────────────────
        logger.info(f"  Crawling {n_pages} list pages for {category_slug}...")
        for page_num in range(n_pages):
            url = cat_url if page_num == 0 else f"{cat_url}?page={page_num}"
            page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                accept_cookies(page)
                page.wait_for_selector(f"a[href*='/{category_slug}/']", timeout=15_000)

                links = page.evaluate(f"""
                    () => [...document.querySelectorAll('a[href*="/{category_slug}/"]')]
                        .map(a => ({{text: a.textContent.trim(), href: a.href}}))
                        .filter(l => {{
                            const parts = l.href.split('/');
                            return parts.length >= 9 && parts[parts.length-2] === '{category_slug}'
                                && l.text.length > 2;
                        }})
                """)
                for link in links:
                    slug = link["href"].rstrip("/").split("/")[-1]
                    if not any(d["slug"] == slug for d in detail_links):
                        detail_links.append({"name": link["text"], "url": link["href"], "slug": slug})

                logger.info(f"    Page {page_num}: {len(links)} links (total: {len(detail_links)})")
            except PWTimeout:
                logger.warning(f"    Timeout on list page {page_num}")
            except Exception as exc:
                logger.warning(f"    Error on list page {page_num}: {exc}")
            finally:
                page.close()
            time.sleep(0.8)

        logger.info(f"  Found {len(detail_links)} schools in {category_slug}")

        # ── Phase B: scrape each detail page ──────────────────────────────────
        for i, school_link in enumerate(detail_links, 1):
            slug = school_link["slug"]
            name = school_link["name"]
            url  = school_link["url"]

            if slug in scraped_slugs:
                logger.info(f"  [{i}/{len(detail_links)}] {name} — cached, skipping")
                continue

            logger.info(f"  [{i}/{len(detail_links)}] {name}")
            page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                time.sleep(0.4)
                accept_cookies(page)

                page_text = page.evaluate("() => document.body.innerText")
                raw = parse_detail_text(page_text, name)

                if not raw:
                    logger.warning(f"    No data extracted for {name!r}")
                    continue

                record = extract_record(raw, name, category_slug, url)
                records.append(record)
                scraped_slugs.add(slug)

                logger.debug(
                    f"    school_type={record['school_type']!r}  "
                    f"ortsteil={record['ortsteil']!r}  "
                    f"telefon={record['telefon']!r}  "
                    f"website={record['website']!r}"
                )

            except PWTimeout:
                logger.warning(f"    Timeout on {slug}")
            except Exception as exc:
                logger.warning(f"    Error on {slug}: {exc}")
                import traceback; traceback.print_exc()
            finally:
                page.close()
            time.sleep(0.6)

        browser.close()

    # Merge with any previously cached records (partial resumption)
    existing = cache.get(cat_key, [])
    existing_slugs = {r.get("sw_portal_slug") for r in existing}
    merged = existing + [r for r in records if r.get("sw_portal_slug") not in existing_slugs]
    cache[cat_key] = merged
    save_cache(cache)

    logger.info(f"  Scraped {len(records)} new + {len(existing)} cached = {len(merged)} total for {category_slug}")
    return merged


# ── Geocode a list of records ─────────────────────────────────────────────────

def geocode_records(records):
    """Add latitude/longitude to records that don't have them yet."""
    need_geocode = [r for r in records if r.get("latitude") is None and r.get("strasse")]
    logger.info(f"  Geocoding {len(need_geocode)} addresses...")
    for r in need_geocode:
        coords = geocode_address(r["strasse"], r.get("plz", ""), r.get("ort", "Frankfurt am Main"))
        if coords:
            r["latitude"], r["longitude"] = coords
    geocoded = sum(1 for r in records if r.get("latitude") is not None)
    logger.info(f"  Geocoded: {geocoded}/{len(records)}")
    return records


# ── Main pipeline ─────────────────────────────────────────────────────────────

def build_dataframe(records):
    """Convert list of record dicts to a clean DataFrame."""
    df = pd.DataFrame(records)

    # Numeric coercion
    for col in ["schueler_gesamt", "klassenzahl"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # PLZ zero-pad
    if "plz" in df.columns:
        df["plz"] = df["plz"].astype(str).str.strip().str.zfill(5).replace("nan", None)

    return df


def main(force_rescrape: bool = False):
    logger.info("=" * 60)
    logger.info("Frankfurt Schulwegweiser PRIMARY Scraper")
    logger.info("=" * 60)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Collect records per pipeline_type
    pipeline_records = {
        "primary":    [],
        "secondary":  [],
        "vocational": [],
    }

    for slug, cfg in CATEGORIES.items():
        logger.info(f"\n── {cfg['label'].upper()} ──")
        records = scrape_category(slug, cfg, force_rescrape=force_rescrape)
        records = geocode_records(records)
        pipeline_records[cfg["pipeline_type"]].extend(records)
        logger.info(f"  → {len(records)} schools collected ({cfg['pipeline_type']})")

    # Save each pipeline type
    output_map = {
        "primary":    RAW_DIR / "frankfurt_primary_schools.csv",
        "secondary":  RAW_DIR / "frankfurt_secondary_schools.csv",
        "vocational": RAW_DIR / "frankfurt_vocational_schools.csv",
    }
    results = {}
    for ptype, records in pipeline_records.items():
        if not records:
            logger.info(f"  No records for {ptype}, skipping.")
            continue
        df = build_dataframe(records)
        out = output_map[ptype]
        df.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(f"  Saved {len(df)} {ptype} schools → {out.name}")
        results[ptype] = len(df)

        # Coverage report
        print(f"\n  {ptype.upper()} ({len(df)} schools):")
        for col, label in [
            ("website",        "Website"),
            ("email",          "Email"),
            ("latitude",       "Geocoded"),
            ("ganztagsform",   "Ganztagsform"),
            ("profile",        "Profile"),
            ("besonderheiten", "Besonderheiten"),
            ("auszeichnungen", "Auszeichnungen"),
        ]:
            if col in df.columns:
                n = df[col].notna().sum()
                pct = n / len(df) * 100
                s = "+" if pct > 50 else "~" if pct > 0 else "-"
                print(f"    {s} {label}: {n}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'='*60}")
    print("SCHULWEGWEISER PRIMARY SCRAPER COMPLETE")
    print(f"{'='*60}")
    for ptype, n in results.items():
        print(f"  {ptype}: {n} schools → {output_map[ptype].name}")
    print(f"{'='*60}")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Frankfurt Schulwegweiser Primary Scraper")
    parser.add_argument("--force-rescrape", action="store_true",
                        help="Ignore cache and re-scrape from scratch (~20-25 min)")
    parser.add_argument("--category", choices=list(CATEGORIES.keys()),
                        help="Scrape only one category (for testing)")
    args = parser.parse_args()

    if args.category:
        cfg = CATEGORIES[args.category]
        records = scrape_category(args.category, cfg, force_rescrape=args.force_rescrape)
        records = geocode_records(records)
        df = build_dataframe(records)
        out = RAW_DIR / f"frankfurt_{args.category}_schools_test.csv"
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"Saved {len(df)} schools → {out}")
    else:
        main(force_rescrape=args.force_rescrape)
