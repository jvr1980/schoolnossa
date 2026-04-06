#!/usr/bin/env python3
"""
Frankfurt Schulwegweiser Portal Scraper

Scrapes the Frankfurt city school portal (frankfurt.de/schulwegweiser) to enrich
school data with official websites, contact info, profiles, and other attributes.

Data extracted per school:
    - website          : official school URL (external link on detail page)
    - sw_email         : contact email from portal
    - sw_telefon       : phone number from portal
    - sw_schueler      : student count from portal
    - sw_schulleitung  : principal / Schulleitung name
    - sw_profile       : comma-joined list of school profiles / Schwerpunkte
    - sw_sprachen      : early foreign language (Frühe Fremdsprache)
    - sw_ganztagsform  : Einrichtungsart / all-day school type
    - sw_besonderheiten: Besondere Angebote text

URL structure:
    Grundschulen list  : https://frankfurt.de/.../schulwegweiser/grundschulen?page=N
    Weiterführende list: https://frankfurt.de/.../schulwegweiser/weiterfuehrende-allgemeinbildende-schulen?page=N
    Detail             : .../schulwegweiser/{type-slug}/{school-slug}

Cloudflare mitigation: Uses Playwright (chromium) for JS rendering.

Output:
    data_frankfurt/intermediate/frankfurt_primary_schools_with_schulwegweiser.csv
    data_frankfurt/intermediate/frankfurt_secondary_schools_with_schulwegweiser.csv

Author: Frankfurt School Data Pipeline
Created: 2026-04-06
"""

import json
import logging
import re
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR     = PROJECT_ROOT / "data_frankfurt"
RAW_DIR      = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR    = DATA_DIR / "cache"

# ── Portal URLs ───────────────────────────────────────────────────────────────
BASE_URL              = "https://frankfurt.de"
SCHULWEGWEISER_BASE   = "https://frankfurt.de/themen/arbeit-bildung-und-wissenschaft/bildung/schulwegweiser"
PRIMARY_LIST_URL      = f"{SCHULWEGWEISER_BASE}/grundschulen"
SECONDARY_LIST_URL    = f"{SCHULWEGWEISER_BASE}/weiterfuehrende-allgemeinbildende-schulen"

# Number of list pages per school type (each has 20 schools)
PRIMARY_PAGES   = 7   # 6 confirmed + 1 safety margin
SECONDARY_PAGES = 6   # 5 confirmed + 1 safety margin

# Cache for scraped data
CACHE_FILE = CACHE_DIR / "schulwegweiser_cache.json"

# Domains that are NOT official school websites
PORTAL_DOMAINS = {
    "frankfurt.de", "frankfurt.de/themen",
    "schulverwaltung.hessen.de", "schulesuche.nrw.de",
    "jedeschule.de", "google.de", "google.com",
    "wikipedia.org", "berlin.de", "hamburg.de",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Lowercase + collapse whitespace for fuzzy matching."""
    name = name.lower().strip()
    name = re.sub(r"\s+", " ", name)
    # Remove common suffixes that differ between sources
    name = re.sub(r"\s*(frankfurt\s*(am\s*main)?|ffm)$", "", name)
    return name


def name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def is_external_school_url(href: str) -> bool:
    """True if URL looks like an official school website (not a city portal)."""
    if not href or not href.startswith("http"):
        return False
    domain = urlparse(href).netloc.lstrip("www.")
    for blocked in PORTAL_DOMAINS:
        if domain == blocked or domain.endswith("." + blocked):
            return False
    return True


# ── Playwright scraping ───────────────────────────────────────────────────────

def scrape_with_playwright(list_url: str, n_pages: int, school_type_slug: str) -> dict[str, dict]:
    """
    Scrape all list pages and detail pages for one school type.

    Returns:
        dict mapping portal_slug → extracted school data dict
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    results: dict[str, dict] = {}
    detail_links: dict[str, str] = {}   # slug → full URL

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

        # ── Phase A: Collect detail page links from list pages ──────────────
        for page_num in range(1, n_pages + 1):
            url = list_url if page_num == 1 else f"{list_url}?page={page_num}"
            logger.info(f"  List page {page_num}/{n_pages}: {url}")
            page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                # Accept cookie banner if present
                try:
                    page.click("button:has-text('Alle akzeptieren')", timeout=3_000)
                    time.sleep(0.5)
                except Exception:
                    pass
                # Wait for school links to appear
                page.wait_for_selector("a[href*='/schulwegweiser/']", timeout=15_000)

                links = page.query_selector_all("a[href*='/schulwegweiser/']")
                found_on_page = 0
                for link in links:
                    href = link.get_attribute("href") or ""
                    # Only accept detail-level URLs (3 path segments deep)
                    parsed = urlparse(href if href.startswith("http") else BASE_URL + href)
                    parts = [p for p in parsed.path.split("/") if p]
                    # Pattern: .../schulwegweiser/<type>/<school-slug>
                    if len(parts) >= 5 and parts[-2] == school_type_slug:
                        slug = parts[-1]
                        full_url = (
                            href if href.startswith("http")
                            else BASE_URL + href
                        )
                        if slug not in detail_links:
                            detail_links[slug] = full_url
                            found_on_page += 1

                logger.info(f"    Found {found_on_page} new school links (total so far: {len(detail_links)})")
                if found_on_page == 0:
                    logger.info("    No new links on this page — stopping pagination.")
                    page.close()
                    break

            except PWTimeout:
                logger.warning(f"    Timeout loading list page {page_num}")
            except Exception as exc:
                logger.warning(f"    Error on list page {page_num}: {exc}")
            finally:
                page.close()
            time.sleep(1.0)

        logger.info(f"  Collected {len(detail_links)} school detail links")

        # ── Phase B: Scrape each detail page ─────────────────────────────────
        for i, (slug, detail_url) in enumerate(detail_links.items(), 1):
            logger.info(f"  Detail {i}/{len(detail_links)}: {slug}")
            page = context.new_page()
            try:
                page.goto(detail_url, wait_until="domcontentloaded", timeout=30_000)
                time.sleep(0.5)

                data: dict = {"slug": slug, "portal_url": detail_url}

                # School name — heading
                for sel in ["h1", ".page-title", ".title"]:
                    el = page.query_selector(sel)
                    if el:
                        data["portal_name"] = el.inner_text().strip()
                        break

                # External website link
                ext_links = page.query_selector_all("a[href^='http']")
                for a in ext_links:
                    href = a.get_attribute("href") or ""
                    if is_external_school_url(href):
                        data["website"] = href.rstrip("/")
                        break

                # Key-value pairs: divs with exactly 2 children (label + value)
                divs = page.query_selector_all("div")
                for div in divs:
                    children = div.query_selector_all(":scope > *")
                    if len(children) != 2:
                        continue
                    label_el, value_el = children[0], children[1]
                    label = label_el.inner_text().strip().lower().rstrip(":")
                    value = value_el.inner_text().strip()
                    if not label or not value:
                        continue

                    if "e-mail" in label or "email" in label:
                        data["sw_email"] = value
                    elif "telefon" in label or "tel." in label:
                        data["sw_telefon"] = value.replace("\u200b", "").strip()
                    elif "schülerzahl" in label or "schuelerzahl" in label or "schüler" == label:
                        m = re.search(r"\d+", value.replace(".", ""))
                        if m:
                            data["sw_schueler"] = int(m.group())
                    elif "schulleitung" in label or "leitung" == label:
                        data["sw_schulleitung"] = value
                    elif "frühe fremdsprache" in label or "fremdsprache" in label:
                        data["sw_sprachen"] = value
                    elif "einrichtungsart" in label or "ganztagsform" in label or "ganztag" in label:
                        data["sw_ganztagsform"] = value
                    elif "besondere angebote" in label or "besonderheiten" in label:
                        data["sw_besonderheiten"] = value
                    elif "profil" in label or "schwerpunkt" in label:
                        existing = data.get("sw_profile", "")
                        data["sw_profile"] = (existing + ", " + value).lstrip(", ")

                # Alternative: look for definition-list style (dt/dd)
                dts = page.query_selector_all("dt")
                for dt in dts:
                    dd = page.query_selector(f"dt:text-matches('{re.escape(dt.inner_text())}') + dd")
                    if not dd:
                        # Try next sibling approach
                        try:
                            dd = dt.evaluate("el => el.nextElementSibling")
                        except Exception:
                            continue
                    if not dd:
                        continue
                    label = dt.inner_text().strip().lower().rstrip(":")
                    value = dd.inner_text().strip() if hasattr(dd, "inner_text") else ""
                    if not value:
                        continue
                    if "e-mail" in label and "sw_email" not in data:
                        data["sw_email"] = value
                    elif "telefon" in label and "sw_telefon" not in data:
                        data["sw_telefon"] = value

                # Contact block: look for mailto: links
                if "sw_email" not in data:
                    mail_links = page.query_selector_all("a[href^='mailto:']")
                    if mail_links:
                        href = mail_links[0].get_attribute("href") or ""
                        data["sw_email"] = href.replace("mailto:", "").strip()

                logger.info(
                    f"    name={data.get('portal_name','?')!r}  "
                    f"website={data.get('website','—')}  "
                    f"email={data.get('sw_email','—')}"
                )
                results[slug] = data

            except PWTimeout:
                logger.warning(f"    Timeout on detail page: {slug}")
            except Exception as exc:
                logger.warning(f"    Error on {slug}: {exc}")
            finally:
                page.close()

            time.sleep(0.8)

        browser.close()

    return results


# ── Matching & merging ─────────────────────────────────────────────────────────

def match_and_merge(school_df: pd.DataFrame, portal_data: dict[str, dict]) -> pd.DataFrame:
    """
    Join Schulwegweiser data into the school DataFrame by fuzzy name match.

    Adds columns: website (if missing/empty), sw_email, sw_telefon, sw_schueler,
                  sw_schulleitung, sw_profile, sw_sprachen, sw_ganztagsform,
                  sw_besonderheiten, sw_portal_url, sw_portal_slug
    """
    df = school_df.copy()
    portal_list = list(portal_data.values())

    sw_cols = [
        "website", "sw_email", "sw_telefon", "sw_schueler",
        "sw_schulleitung", "sw_profile", "sw_sprachen",
        "sw_ganztagsform", "sw_besonderheiten",
        "sw_portal_url", "sw_portal_slug",
    ]
    for col in sw_cols:
        if col not in df.columns:
            df[col] = None

    matched = 0
    unmatched_portal = set(portal_data.keys())

    for idx, row in df.iterrows():
        school_name = str(row.get("schulname", ""))
        if not school_name:
            continue

        best_score = 0.0
        best_entry = None

        for entry in portal_list:
            portal_name = entry.get("portal_name", entry.get("slug", "").replace("-", " "))
            score = name_similarity(school_name, portal_name)
            if score > best_score:
                best_score = score
                best_entry = entry

        if best_score >= 0.65 and best_entry:
            slug = best_entry.get("slug", "")
            unmatched_portal.discard(slug)
            matched += 1

            # Merge: only fill empty fields (don't overwrite existing good data)
            for field, col in [
                ("website",          "website"),
                ("sw_email",         "sw_email"),
                ("sw_telefon",       "sw_telefon"),
                ("sw_schueler",      "sw_schueler"),
                ("sw_schulleitung",  "sw_schulleitung"),
                ("sw_profile",       "sw_profile"),
                ("sw_sprachen",      "sw_sprachen"),
                ("sw_ganztagsform",  "sw_ganztagsform"),
                ("sw_besonderheiten","sw_besonderheiten"),
            ]:
                val = best_entry.get(field)
                if val:
                    existing = df.at[idx, col]
                    # For website: only set if empty or current is a portal URL
                    if field == "website":
                        if not existing or not is_external_school_url(str(existing)):
                            df.at[idx, col] = val
                    elif pd.isna(existing) or existing is None or str(existing).strip() == "":
                        df.at[idx, col] = val

            df.at[idx, "sw_portal_url"]  = best_entry.get("portal_url", "")
            df.at[idx, "sw_portal_slug"] = slug

            logger.debug(
                f"  Matched {school_name!r} → {best_entry.get('portal_name','?')!r} (score={best_score:.2f})"
            )
        else:
            logger.debug(
                f"  No match for {school_name!r} (best score={best_score:.2f})"
            )

    logger.info(f"Matched {matched}/{len(df)} schools from portal")
    if unmatched_portal:
        logger.info(f"Unmatched portal entries ({len(unmatched_portal)}): {sorted(unmatched_portal)[:10]}")

    return df


# ── Per-school-type entry points ──────────────────────────────────────────────

def process_school_type(school_type: str, force_rescrape: bool = False) -> pd.DataFrame:
    """
    Full pipeline for one school type (primary or secondary).

    1. Load input CSV (raw schools from Verzeichnis 6)
    2. Load/update portal cache
    3. Scrape list + detail pages if not cached
    4. Match and merge portal data
    5. Write intermediate CSV
    """
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Load input
    input_file = RAW_DIR / f"frankfurt_{school_type}_schools.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Input not found: {input_file}")
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} {school_type} schools from {input_file.name}")

    # Load cache
    cache = load_cache()
    cache_key = f"{school_type}_portal"

    if force_rescrape or cache_key not in cache:
        # Determine list URL and page count
        if school_type == "primary":
            list_url    = PRIMARY_LIST_URL
            n_pages     = PRIMARY_PAGES
            type_slug   = "grundschulen"
        else:
            list_url    = SECONDARY_LIST_URL
            n_pages     = SECONDARY_PAGES
            type_slug   = "weiterfuehrende-allgemeinbildende-schulen"

        logger.info(f"Scraping Schulwegweiser portal for {school_type} schools...")
        portal_data = scrape_with_playwright(list_url, n_pages, type_slug)

        cache[cache_key] = portal_data
        save_cache(cache)
        logger.info(f"Cached {len(portal_data)} portal entries for {school_type}")
    else:
        portal_data = cache[cache_key]
        logger.info(f"Using cached portal data: {len(portal_data)} entries for {school_type}")

    # Match and merge
    enriched = match_and_merge(df, portal_data)

    # Report
    n_website   = enriched["website"].notna().sum() if "website" in enriched.columns else 0
    n_email     = enriched["sw_email"].notna().sum() if "sw_email" in enriched.columns else 0
    n_profile   = enriched["sw_profile"].notna().sum() if "sw_profile" in enriched.columns else 0
    n_ganztag   = enriched["sw_ganztagsform"].notna().sum() if "sw_ganztagsform" in enriched.columns else 0
    logger.info(
        f"{school_type}: website={n_website}/{len(enriched)} "
        f"email={n_email}/{len(enriched)} "
        f"profile={n_profile}/{len(enriched)} "
        f"ganztag={n_ganztag}/{len(enriched)}"
    )

    # Save
    out_path = INTERMEDIATE_DIR / f"frankfurt_{school_type}_schools_with_schulwegweiser.csv"
    enriched.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(f"Saved: {out_path}")

    return enriched


# ── main ──────────────────────────────────────────────────────────────────────

def main(force_rescrape: bool = False):
    logger.info("=" * 60)
    logger.info("Frankfurt Schulwegweiser Portal Scraper")
    logger.info("=" * 60)

    results = {}
    for school_type in ["primary", "secondary"]:
        logger.info(f"\n── {school_type.upper()} SCHOOLS ──")
        try:
            df = process_school_type(school_type, force_rescrape=force_rescrape)
            results[school_type] = len(df)
        except FileNotFoundError as e:
            logger.warning(str(e))
        except Exception as e:
            logger.error(f"Failed for {school_type}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*60}")
    print("SCHULWEGWEISER SCRAPER COMPLETE")
    print(f"{'='*60}")
    for st, n in results.items():
        print(f"  {st}: {n} schools enriched")
    print(f"{'='*60}")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Frankfurt Schulwegweiser Portal Scraper")
    parser.add_argument(
        "--force-rescrape",
        action="store_true",
        help="Ignore cache and re-scrape the portal (takes ~10-15 min)",
    )
    parser.add_argument(
        "--school-type",
        choices=["primary", "secondary", "both"],
        default="both",
        help="Which school type to process (default: both)",
    )
    args = parser.parse_args()

    if args.school_type == "both":
        main(force_rescrape=args.force_rescrape)
    else:
        process_school_type(args.school_type, force_rescrape=args.force_rescrape)
