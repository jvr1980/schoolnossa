#!/usr/bin/env python3
"""
Sitemap-Guided Re-Enrichment (fix pass for parse_error / empty schools)
========================================================================

The main enrichment script (enrich_german_schools_with_admission_and_open_days.py)
feeds Gemini only the school's homepage URL. For schools with JS-heavy landing
pages (e.g. canisius.de), the homepage has no content — Gemini has nothing to
read and returns parse_error or empty data.

This script is the targeted fix. For every school whose cached Gemini result
is unusable, it:

    1. Discovers the site's sitemap (sitemap.xml, sitemap_index.xml,
       wp-sitemap.xml, or via robots.txt).
    2. Follows nested sitemap indexes.
    3. Scores every <loc> URL against German admission + open-day keywords
       (anmeldung, aufnahme, bewerbung, tag-der-offenen-tuer, infoabend,
       schnupper, termine, veranstaltungen, …).
    4. Picks the top-N candidate subpage URLs and includes ALL of them in
       the Gemini prompt so the URL-context tool fetches each.
    5. If the site has no sitemap, falls back to canonical-path probing
       (/anmeldung, /aufnahme, /termine, …) with HEAD requests.
    6. Updates the shared cache in place with method="sitemap_guided".

Runs standalone, reads the main cache, rewrites per-city output CSVs when
done. Safe to run concurrently with the main enrichment script — cache
writes are atomic (tmp file + rename) and entries are keyed by URL so
there's no contention.

Usage:
    # dry-run: show which schools would be re-enriched and their candidate URLs
    python scripts_shared/enrichment/reenrich_admission_open_days_via_sitemap.py \\
        --statuses parse_error,no_admission_info --limit 5 --dry-run

    # real re-enrichment for all failed schools across all cities
    python scripts_shared/enrichment/reenrich_admission_open_days_via_sitemap.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
CACHE_FILE = PROJECT_ROOT / "data_shared" / "cache" / "admission_open_days" / "cache.json"

MODEL = "gemini-2.5-flash"
USER_AGENT = "Mozilla/5.0 (compatible; SchoolNossa/1.0; +https://schoolnossa.de)"
HTTP_TIMEOUT = 10
SITEMAP_TIMEOUT = 15
SAVE_INTERVAL = 5
REQUEST_DELAY_SECONDS = 2.0
TOP_K_CANDIDATE_URLS = 4
MAX_SITEMAP_URLS = 4000   # safety cap when a sitemap lists everything

# Re-use the main script's registry + helpers
sys.path.insert(0, str(SCRIPT_DIR))
from enrich_german_schools_with_admission_and_open_days import (  # type: ignore
    CITY_REGISTRY,
    CITY_BY_KEY,
    OUTPUT_COLUMNS,
    VALID_STATUSES,
    _normalize_url,
    _url_cache_key,
    _load_table,
    _row_to_school_info,
    _pick_website,
    _build_row,
    _skipped_row,
    _extract_text,
    _extract_grounding_urls,
    _data_dir_for_city,
)

# ---------------------------------------------------------------------------
# Logging + env
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("reenrich_sitemap")

try:
    from dotenv import load_dotenv  # type: ignore
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

# ---------------------------------------------------------------------------
# URL scoring
# ---------------------------------------------------------------------------

# (regex, weight) — higher weight = more relevant
URL_KEYWORDS: List[Tuple[re.Pattern[str], int]] = [
    (re.compile(r"tag[-_]?der[-_]?offenen[-_]?tuer", re.I), 100),
    (re.compile(r"offener?[-_]?tag", re.I), 90),
    (re.compile(r"schule[-_]?kennenlern", re.I), 90),
    (re.compile(r"schnuppert?ag|schnupperunterricht", re.I), 85),
    (re.compile(r"infoabend|info[-_]abend", re.I), 80),
    (re.compile(r"anmeld", re.I), 75),
    (re.compile(r"aufnahme", re.I), 75),
    (re.compile(r"bewerbung", re.I), 70),
    (re.compile(r"einschul", re.I), 65),
    (re.compile(r"kennenlernen", re.I), 55),
    (re.compile(r"termine?$|termine?/|/termine", re.I), 40),
    (re.compile(r"veranstaltung", re.I), 35),
    (re.compile(r"kalender", re.I), 30),
    (re.compile(r"quereinstieg|quereinsteiger", re.I), 30),
]

# Paths worth probing if no sitemap is available
CANONICAL_PATHS = [
    "anmeldung", "aufnahme", "bewerbung",
    "tag-der-offenen-tuer", "schule-kennenlernen",
    "termine", "veranstaltungen",
    "anmeldeverfahren", "aufnahmeverfahren",
    "schnuppertag", "infoabend",
]


def _score_url(url: str) -> int:
    score = 0
    for pattern, weight in URL_KEYWORDS:
        if pattern.search(url):
            score += weight
    return score


def _dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for it in items:
        if it and it not in seen:
            seen.add(it)
            out.append(it)
    return out

# ---------------------------------------------------------------------------
# Sitemap discovery + parsing
# ---------------------------------------------------------------------------

@dataclass
class SitemapResult:
    method: str                    # 'sitemap' | 'homepage_anchors' | 'canonical_probe' | 'none'
    candidates: List[str]          # subpage URLs to feed to Gemini (top-K)
    all_discovered: int = 0        # how many URLs the discovery mechanism found
    sitemap_url: Optional[str] = None


def _http_get(url: str, timeout: int = HTTP_TIMEOUT) -> Optional[requests.Response]:
    try:
        return requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            allow_redirects=True,
        )
    except requests.RequestException:
        return None


def _http_head(url: str) -> Optional[int]:
    try:
        resp = requests.head(
            url,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
        )
        return resp.status_code
    except requests.RequestException:
        return None


def _parse_sitemap_xml(xml_text: str) -> Tuple[List[str], List[str]]:
    """Return (page_urls, nested_sitemap_urls)."""
    page_urls: List[str] = []
    nested: List[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        # Fall back to regex for malformed XML
        locs = re.findall(r"<loc>([^<]+)</loc>", xml_text)
        for loc in locs:
            if loc.endswith(".xml") or "sitemap" in loc.lower():
                nested.append(loc)
            else:
                page_urls.append(loc)
        return page_urls, nested

    tag = root.tag.split("}", 1)[-1].lower()
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    if tag == "sitemapindex":
        for sm in root.findall(".//sm:sitemap/sm:loc", ns):
            if sm.text:
                nested.append(sm.text.strip())
    else:
        for loc in root.findall(".//sm:url/sm:loc", ns):
            if loc.text:
                page_urls.append(loc.text.strip())
    return page_urls, nested


def _discover_sitemap_urls(base_url: str) -> SitemapResult:
    """Find a sitemap for the site, parse it (following indexes), return ranked candidates."""
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"

    tried: List[str] = []

    # 1. robots.txt for explicit sitemap locations
    robots_urls: List[str] = []
    resp = _http_get(urljoin(root, "/robots.txt"))
    if resp is not None and resp.status_code == 200:
        for line in resp.text.splitlines():
            m = re.match(r"^\s*Sitemap:\s*(\S+)", line, re.I)
            if m:
                robots_urls.append(m.group(1).strip())

    candidate_sitemaps = _dedupe_preserve_order([
        *robots_urls,
        urljoin(root, "/sitemap.xml"),
        urljoin(root, "/sitemap_index.xml"),
        urljoin(root, "/wp-sitemap.xml"),
        urljoin(root, "/sitemap/sitemap-index.xml"),
    ])

    all_page_urls: List[str] = []
    used_sitemap: Optional[str] = None
    for sm_url in candidate_sitemaps:
        tried.append(sm_url)
        resp = _http_get(sm_url, timeout=SITEMAP_TIMEOUT)
        if resp is None or resp.status_code != 200:
            continue
        ct = resp.headers.get("content-type", "")
        if "xml" not in ct.lower() and "<urlset" not in resp.text[:500] and "<sitemapindex" not in resp.text[:500]:
            continue

        pages, nested = _parse_sitemap_xml(resp.text)
        all_page_urls.extend(pages)

        # Follow up to 10 nested sitemaps — prefer ones that hint at pages/events
        def _nested_priority(url: str) -> int:
            lower = url.lower()
            for kw, w in [("page", 3), ("termin", 3), ("event", 3), ("post", 1)]:
                if kw in lower:
                    return w
            return 0
        nested_sorted = sorted(nested, key=_nested_priority, reverse=True)[:10]
        for child in nested_sorted:
            cresp = _http_get(child, timeout=SITEMAP_TIMEOUT)
            if cresp is None or cresp.status_code != 200:
                continue
            cpages, _ = _parse_sitemap_xml(cresp.text)
            all_page_urls.extend(cpages)
            if len(all_page_urls) >= MAX_SITEMAP_URLS:
                break

        used_sitemap = sm_url
        if all_page_urls:
            break

    if all_page_urls:
        all_page_urls = _dedupe_preserve_order(all_page_urls)[:MAX_SITEMAP_URLS]
        ranked = [(u, _score_url(u)) for u in all_page_urls]
        ranked = [(u, s) for u, s in ranked if s > 0]
        ranked.sort(key=lambda x: x[1], reverse=True)
        top = [u for u, _ in ranked[:TOP_K_CANDIDATE_URLS]]
        return SitemapResult(
            method="sitemap",
            candidates=top,
            all_discovered=len(all_page_urls),
            sitemap_url=used_sitemap,
        )

    # 2. Homepage anchor scrape fallback
    home = _http_get(base_url)
    if home is not None and home.status_code == 200 and "text/html" in home.headers.get("content-type", "").lower():
        anchors = re.findall(r'href=["\']([^"\']+)["\']', home.text)
        resolved = []
        for href in anchors:
            if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            url = urljoin(base_url, href)
            if urlparse(url).netloc.endswith(parsed.netloc):
                resolved.append(url.split("#")[0])
        resolved = _dedupe_preserve_order(resolved)
        ranked = [(u, _score_url(u)) for u in resolved]
        ranked = [(u, s) for u, s in ranked if s > 0]
        ranked.sort(key=lambda x: x[1], reverse=True)
        top = [u for u, _ in ranked[:TOP_K_CANDIDATE_URLS]]
        if top:
            return SitemapResult(method="homepage_anchors", candidates=top, all_discovered=len(resolved))

    # 3. Canonical path probing
    probed: List[str] = []
    for slug in CANONICAL_PATHS:
        url = urljoin(root, f"/{slug}/")
        status = _http_head(url)
        if status and status < 400:
            probed.append(url)
        if len(probed) >= TOP_K_CANDIDATE_URLS:
            break
    if probed:
        return SitemapResult(method="canonical_probe", candidates=probed, all_discovered=len(probed))

    return SitemapResult(method="none", candidates=[], all_discovered=0)

# ---------------------------------------------------------------------------
# Gemini prompt (sitemap-aware)
# ---------------------------------------------------------------------------

SITEMAP_PROMPT_TEMPLATE = """Du recherchierst eine deutsche Schule, um
zwei Informationen zu finden:

  1. Aufnahme-/Anmeldekriterien (admission criteria)
  2. Termine für Tag der offenen Tür / Infoabend / Schnuppertag (open days)

Schule: {schulname}
Schulform: {schulart}
Adresse: {strasse}, {plz} {stadt}

UNTERSUCHE DIESE URLs (aus der Sitemap der Schulwebsite — sie enthalten
wahrscheinlich die gesuchten Infos):

{candidate_block}

ANWEISUNGEN
- Lies ALLE oben genannten URLs sorgfältig.
- Falls dort Infos fehlen, nutze zusätzlich Google-Suche nach dem exakten
  Schulnamen + "Anmeldung" oder "Tag der offenen Tür".
- Heutiges Datum: {today}. Verwerfe alle Termine VOR diesem Datum.
- Datumsformat strikt ISO YYYY-MM-DD. Uhrzeiten HH:MM (24h) oder null.
- Wenn nichts gefunden wird, gib leere Listen / null zurück — NIE raten.

ANTWORT-FORMAT (strikt JSON, keine Markdown-Code-Blöcke):
{{
  "admission_criteria_bullets": ["kurze Stichpunkte auf Deutsch"],
  "admission_application_window": {{"opens": "YYYY-MM-DD"|null, "closes": "YYYY-MM-DD"|null, "notes": "..."}} | null,
  "admission_notes_de": "kurzer ergänzender Absatz auf Deutsch",
  "open_days": [
    {{"date": "YYYY-MM-DD", "start_time": "HH:MM"|null, "end_time": "HH:MM"|null, "event_type": "Tag der offenen Tür"|"Infoabend"|"Schnuppertag"|"Anmeldetag"|"Sonstiges", "audience": "z.B. zukünftige Klasse 5", "notes": "..."}}
  ],
  "last_open_day_seen": "YYYY-MM-DD"|null,
  "source_grounding_urls": ["genaue URL(s), auf denen die Info stand"]
}}

Antworte NUR mit dem JSON-Objekt.
"""


def _build_sitemap_prompt(school: dict, candidates: List[str], homepage: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    urls_for_prompt = _dedupe_preserve_order([homepage, *candidates])[: TOP_K_CANDIDATE_URLS + 1]
    candidate_block = "\n".join(f"  - {u}" for u in urls_for_prompt)
    return SITEMAP_PROMPT_TEMPLATE.format(
        schulname=school.get("schulname") or "(unbekannt)",
        schulart=school.get("schulart") or "(unbekannt)",
        strasse=school.get("strasse") or "(unbekannt)",
        plz=school.get("plz") or "",
        stadt=school.get("stadt") or "",
        candidate_block=candidate_block,
        today=today,
    )

# ---------------------------------------------------------------------------
# Gemini call (local copy so we don't import private helpers that need types)
# ---------------------------------------------------------------------------

def _init_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")
    from google import genai  # type: ignore
    return genai.Client(api_key=api_key)


def _call_gemini(client, prompt: str, label: str, retry: int = 0, max_retries: int = 2) -> dict:
    from google.genai import types  # type: ignore
    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[
                    types.Tool(url_context=types.UrlContext()),
                    types.Tool(google_search=types.GoogleSearch()),
                ],
                temperature=0,
            ),
        )
    except Exception as exc:
        msg = str(exc)
        if "URL_RETRIEVAL" in msg:
            logger.warning(f"  [{label}] URL unreachable")
            return {"data": None, "status": "url_unreachable", "grounding_urls": []}
        if ("RATE_LIMIT" in msg.upper() or "429" in msg) and retry < 3:
            wait = 30 * (retry + 1)
            logger.warning(f"  [{label}] Rate limited — waiting {wait}s")
            time.sleep(wait)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        if ("500" in msg or "INTERNAL" in msg) and retry < max_retries:
            time.sleep(5)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        logger.warning(f"  [{label}] Gemini error: {exc}")
        return {"data": None, "status": "parse_error", "grounding_urls": []}

    text = _extract_text(response)
    grounding = _extract_grounding_urls(response)
    if not text:
        if retry < max_retries:
            time.sleep(3)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        return {"data": None, "status": "parse_error", "grounding_urls": grounding}

    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    # Belt-and-braces: strip any text before first `{` and after last `}`
    if "{" in text and "}" in text:
        text = text[text.index("{") : text.rindex("}") + 1]

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        if retry < max_retries:
            time.sleep(3)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        return {"data": None, "status": "parse_error", "grounding_urls": grounding}

    return {"data": data, "status": "success", "grounding_urls": grounding}

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            logger.warning("Cache corrupt — starting fresh (dangerous!)")
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2, ensure_ascii=False)
    tmp.replace(CACHE_FILE)

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _is_payload_empty(entry: dict) -> bool:
    """True if the cached result has success status but empty bullets + open_days."""
    result = entry.get("result") or {}
    if result.get("status") != "success":
        return False
    data = result.get("data") or {}
    bullets = data.get("admission_criteria_bullets") or []
    open_days = data.get("open_days") or []
    window = data.get("admission_application_window")
    notes = data.get("admission_notes_de") or ""
    return not bullets and not open_days and not window and not (notes and notes.strip())


def _is_failed_entry(entry: dict, statuses: set[str]) -> bool:
    result = entry.get("result") or {}
    status = result.get("status")
    if status in statuses:
        return True
    if "empty_success" in statuses and _is_payload_empty(entry):
        return True
    return False


def _collect_candidates(
    cache: dict,
    city_keys: List[str],
    statuses: set[str],
) -> List[Tuple[str, str, str, dict]]:
    """Return [(city_key, schulnummer, website_url, school_info), ...] for each failed school."""
    out: List[Tuple[str, str, str, dict]] = []
    for city_key in city_keys:
        cfg = CITY_BY_KEY[city_key]
        path = PROJECT_ROOT / cfg.final_path
        if not path.exists():
            logger.warning(f"[{city_key}] final table missing: {path}")
            continue
        df = _load_table(path)
        for _, row in df.iterrows():
            url = _pick_website(row, cfg)
            if not url:
                continue
            cache_key = _url_cache_key(url)
            entry = cache.get(cache_key)
            if not entry:
                continue
            if not _is_failed_entry(entry, statuses):
                continue
            school = _row_to_school_info(row, cfg, website=url)
            out.append((city_key, school["schulnummer"], url, school))
    return out


def _write_per_city_outputs(cities: List[str], cache: dict) -> None:
    """Rebuild per-city intermediate CSVs from the (now updated) cache."""
    for city_key in cities:
        cfg = CITY_BY_KEY[city_key]
        path = PROJECT_ROOT / cfg.final_path
        if not path.exists():
            continue
        df = _load_table(path)
        rows: List[dict] = []
        for _, row in df.iterrows():
            url = _pick_website(row, cfg)
            school = _row_to_school_info(row, cfg, website=url or "")
            if not url:
                rows.append(_skipped_row(school, city_key, "skipped_no_url"))
                continue
            entry = cache.get(_url_cache_key(url))
            if not entry:
                rows.append(_skipped_row(school, city_key, "skipped_no_url"))
                continue
            rows.append(_build_row(school, city_key, entry["result"]))
        out_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        out_dir = PROJECT_ROOT / f"data_{_data_dir_for_city(city_key)}" / "intermediate"
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"{city_key}_admission_open_days.csv"
        out_df.to_csv(csv_path, index=False)
        try:
            out_df.to_parquet(csv_path.with_suffix(".parquet"), index=False)
        except Exception:
            pass
        logger.info(f"[{city_key}] rewrote {len(out_df)} rows → {csv_path.name}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", default="all", help="'all' or comma-separated city keys")
    parser.add_argument(
        "--statuses",
        default="parse_error,no_admission_info,empty_success",
        help="Comma-separated statuses to re-try. Special value 'empty_success' catches successful but empty payloads.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Cap total schools (0 = no limit)")
    parser.add_argument("--dry-run", action="store_true", help="Discover candidate URLs and stop (no Gemini call)")
    parser.add_argument("--rewrite-outputs", action="store_true", help="Regenerate per-city intermediate CSVs from cache, even without re-enrichment")
    parser.add_argument("--one-url", help="Manually test one school by URL (skips city-registry scan)")
    args = parser.parse_args(argv)

    statuses = set(s.strip() for s in args.statuses.split(",") if s.strip())
    logger.info(f"Target statuses: {sorted(statuses)}")

    cache = _load_cache()
    logger.info(f"Loaded {len(cache)} cache entries from {CACHE_FILE}")

    # ---- one-URL debug mode ----
    if args.one_url:
        url = _normalize_url(args.one_url) or args.one_url
        result = _discover_sitemap_urls(url)
        print(f"\nmethod={result.method}  sitemap={result.sitemap_url}  discovered={result.all_discovered}")
        print("Top candidate URLs:")
        for u in result.candidates:
            print(f"  - {u}")
        return 0

    if args.cities == "all":
        cities = [c.key for c in CITY_REGISTRY]
    else:
        cities = [c.strip() for c in args.cities.split(",") if c.strip()]
        for c in cities:
            if c not in CITY_BY_KEY:
                raise SystemExit(f"Unknown city: {c}")

    if args.rewrite_outputs:
        _write_per_city_outputs(cities, cache)
        return 0

    targets = _collect_candidates(cache, cities, statuses)
    if args.limit:
        targets = targets[: args.limit]
    logger.info(f"{len(targets)} schools match re-enrichment criteria across {len(cities)} cities")

    if not targets:
        logger.info("Nothing to do.")
        return 0

    if args.dry_run:
        # Show what we WOULD do
        for city_key, snr, url, _school in targets[:25]:
            result = _discover_sitemap_urls(url)
            logger.info(
                f"[{city_key}/{snr}] {url}  →  method={result.method}  "
                f"discovered={result.all_discovered}  top={len(result.candidates)}"
            )
            for u in result.candidates:
                logger.info(f"    - {u}")
        return 0

    client = _init_gemini_client()

    method_counter: dict[str, int] = {}
    improved = 0
    still_failed = 0
    no_candidates = 0

    for n, (city_key, snr, url, school) in enumerate(targets, start=1):
        label = f"{city_key}/{snr}"
        site_result = _discover_sitemap_urls(url)
        method_counter[site_result.method] = method_counter.get(site_result.method, 0) + 1

        if not site_result.candidates:
            no_candidates += 1
            logger.info(f"[{label}] no candidate subpages discovered — leaving cache entry as-is")
            continue

        prompt = _build_sitemap_prompt(school, site_result.candidates, url)
        result = _call_gemini(client, prompt, label)

        # Annotate + store
        result_with_method = {
            **result,
            "method": f"sitemap_guided:{site_result.method}",
            "candidate_urls": site_result.candidates,
        }
        cache[_url_cache_key(url)] = {
            "result": result_with_method,
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": url,
        }

        # Heuristic: did we improve?
        data = result.get("data") or {}
        bullets = data.get("admission_criteria_bullets") or []
        open_days = data.get("open_days") or []
        window = data.get("admission_application_window")
        notes = data.get("admission_notes_de") or ""
        if result.get("status") == "success" and (bullets or open_days or window or notes.strip()):
            improved += 1
            logger.info(
                f"[{label}] ✓ improved via {site_result.method}: "
                f"bullets={len(bullets)} open_days={len(open_days)} window={bool(window)}"
            )
        else:
            still_failed += 1
            logger.info(f"[{label}] still empty after sitemap ({result.get('status')})")

        if n % SAVE_INTERVAL == 0:
            _save_cache(cache)

        time.sleep(REQUEST_DELAY_SECONDS)

    _save_cache(cache)
    logger.info("")
    logger.info("=== Re-enrichment summary ===")
    logger.info(f"Total processed:     {len(targets)}")
    logger.info(f"Improved:            {improved}")
    logger.info(f"Still failed:        {still_failed}")
    logger.info(f"No candidates found: {no_candidates}")
    logger.info(f"Discovery methods:   {method_counter}")

    # Regenerate per-city CSVs so they reflect the updated cache
    _write_per_city_outputs(cities, cache)
    return 0


if __name__ == "__main__":
    sys.exit(main())
