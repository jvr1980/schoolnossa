#!/usr/bin/env python3
"""
Cross-City Enrichment: Admission Criteria + Open Day Dates
==========================================================

For every German school whose final master table contains a usable website URL,
ask Gemini (with URL-context + Google-Search grounding) to extract:

  1. Admission criteria   — bullet list of who can apply, catchment rules, etc.
  2. Application window   — opens/closes ISO dates if stated.
  3. Open day dates       — Tag der offenen Tür / Infoabend / Schnuppertag,
                            structured with date + time + event type, future only.

Inputs (read-only):
    data_{city}/final/*_school_master_table_final.{parquet,csv}

Outputs:
    data_{city}/intermediate/{city_key}_admission_open_days.{csv,parquet}
    data_shared/cache/admission_open_days/cache.json   (shared, URL-keyed)
    data_shared/admission_open_days_all_german_cities.csv   (when --combine)

Skip rules:
    - Non-German cities (es/fr/gb/it/nl) are not in the registry.
    - Schools with empty / unusable website URLs are recorded with
      gemini_status='skipped_no_url' and no Gemini call is made.

Cache keyed by sha1(normalized website URL) so the same URL appearing across
two tables (e.g. Berlin all + Berlin primary) hits Gemini once. TTL 60 days
because open-day calendars change seasonally.

Usage:
    python scripts_shared/enrichment/enrich_german_schools_with_admission_and_open_days.py \\
        --cities stuttgart_primary --limit 3 --dry-run
    python scripts_shared/enrichment/enrich_german_schools_with_admission_and_open_days.py \\
        --cities all --combine
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

CACHE_DIR = PROJECT_ROOT / "data_shared" / "cache" / "admission_open_days"
CACHE_FILE = CACHE_DIR / "cache.json"
COMBINED_OUTPUT = PROJECT_ROOT / "data_shared" / "admission_open_days_all_german_cities.csv"

MODEL = "gemini-2.5-flash"
REQUEST_DELAY_SECONDS = 1.5
SAVE_INTERVAL = 10
CACHE_TTL_DAYS = 60

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("admission_open_days")

# ---------------------------------------------------------------------------
# .env loader (mirrors Frankfurt enrichment pattern)
# ---------------------------------------------------------------------------

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
# City registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CityConfig:
    key: str                     # short id used in CLI / output filenames
    final_path: str              # relative to PROJECT_ROOT
    website_col: str             # column holding the URL
    fallback_website_col: Optional[str] = None  # secondary column to consult
    default_city_name: str = ""  # used when row has no `stadt` field

CITY_REGISTRY: List[CityConfig] = [
    CityConfig("berlin",              "data_berlin/final/school_master_table_final_with_embeddings.parquet",            "website",        None,             "Berlin"),
    CityConfig("berlin_primary",      "data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet","schul_homepage", None,             "Berlin"),
    CityConfig("bremen",              "data_bremen/final/bremen_school_master_table_final.csv",                          "website",        None,             "Bremen"),
    CityConfig("dresden",             "data_dresden/final/dresden_school_master_table_final.csv",                        "website",        None,             "Dresden"),
    CityConfig("frankfurt_primary",   "data_frankfurt/final/frankfurt_primary_school_master_table_final.csv",            "website",        None,             "Frankfurt am Main"),
    CityConfig("frankfurt_secondary", "data_frankfurt/final/frankfurt_secondary_school_master_table_final.csv",          "website",        None,             "Frankfurt am Main"),
    CityConfig("hamburg",             "data_hamburg/final/hamburg_school_master_table_final.csv",                        "schul_homepage", None,             "Hamburg"),
    CityConfig("hamburg_primary",     "data_hamburg_primary/final/hamburg_primary_school_master_table_final.csv",        "schul_homepage", None,             "Hamburg"),
    CityConfig("leipzig",             "data_leipzig/final/leipzig_school_master_table_final.csv",                        "website",        None,             "Leipzig"),
    CityConfig("munich_primary",      "data_munich/final/munich_primary_school_master_table_final.csv",                  "website",        None,             "München"),
    CityConfig("munich_secondary",    "data_munich/final/munich_secondary_school_master_table_final.csv",                "website",        None,             "München"),
    CityConfig("nrw_primary",         "data_nrw/final/nrw_primary_school_master_table_final.csv",                        "schul_homepage", "website",        ""),
    CityConfig("nrw_secondary",       "data_nrw/final/nrw_secondary_school_master_table_final.csv",                      "website",        "schul_homepage", ""),
    CityConfig("stuttgart_primary",   "data_stuttgart/final/stuttgart_primary_school_master_table_final.csv",            "website",        None,             "Stuttgart"),
    CityConfig("stuttgart_secondary", "data_stuttgart/final/stuttgart_secondary_school_master_table_final.csv",          "website",        None,             "Stuttgart"),
]

CITY_BY_KEY = {c.key: c for c in CITY_REGISTRY}

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "schulnummer",
    "city_key",
    "schulname",
    "website_used",
    "admission_criteria_bullets",      # JSON-encoded list[str]
    "admission_application_window",    # JSON-encoded dict or null
    "admission_notes_de",
    "open_days",                       # JSON-encoded list[dict]
    "last_open_day_seen",              # ISO date or empty
    "source_grounding_urls",           # JSON-encoded list[str]
    "gemini_status",
    "fetched_at",
]

VALID_STATUSES = {
    "success",
    "no_admission_info",
    "no_open_days",
    "url_unreachable",
    "parse_error",
    "rate_limited",
    "skipped_no_url",
    "dry_run",
}

# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _normalize_url(value: Any) -> Optional[str]:
    """Return a usable URL string, or None if the value is unusable."""
    if value is None:
        return None
    try:
        text = str(value).strip()
    except Exception:
        return None
    if not text or text.lower() in {"nan", "none", "null", ""}:
        return None
    if " " in text or "." not in text:
        return None
    if not text.startswith(("http://", "https://")):
        text = "https://" + text
    # Drop trailing slash on bare host
    if text.count("/") == 3 and text.endswith("/"):
        text = text.rstrip("/")
    return text


def _url_cache_key(url: str) -> str:
    return hashlib.sha1(url.lower().encode("utf-8")).hexdigest()


def _pick_website(row: pd.Series, cfg: CityConfig) -> Optional[str]:
    primary = _normalize_url(row.get(cfg.website_col))
    if primary:
        return primary
    if cfg.fallback_website_col:
        return _normalize_url(row.get(cfg.fallback_website_col))
    return None

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _load_cache() -> Dict[str, dict]:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            logger.warning("Cache file corrupt — starting fresh")
    return {}


def _save_cache(cache: Dict[str, dict]) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2, ensure_ascii=False)
    tmp.replace(CACHE_FILE)


def _cache_is_fresh(entry: dict) -> bool:
    fetched_at = entry.get("fetched_at")
    if not fetched_at:
        return False
    try:
        ts = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts) < timedelta(days=CACHE_TTL_DAYS)

# ---------------------------------------------------------------------------
# Input loader
# ---------------------------------------------------------------------------

def _load_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, dtype=str, low_memory=False)


def _row_to_school_info(row: pd.Series, cfg: CityConfig, website: str) -> dict:
    def _str(col: str, default: str = "") -> str:
        if col not in row.index:
            return default
        val = row[col]
        if pd.isna(val):
            return default
        text = str(val).strip()
        return text if text and text.lower() != "nan" else default

    snr_raw = row.get("schulnummer", row.get("school_id", ""))
    try:
        snr = str(int(float(snr_raw)))
    except (ValueError, TypeError):
        snr = str(snr_raw).strip() if snr_raw is not None else ""

    schulart = _str("schulart") or _str("schulform") or _str("school_type")
    return {
        "schulnummer": snr,
        "schulname": _str("schulname") or _str("name"),
        "schulart": schulart,
        "strasse": _str("strasse") or _str("address"),
        "plz": _str("plz"),
        "stadt": _str("stadt") or _str("city") or cfg.default_city_name,
        "website": website,
    }

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

PROMPT_TEMPLATE = """Du recherchierst die offizielle Website einer deutschen Schule, um
zwei Informationen zu finden:

  1. Aufnahme-/Anmeldekriterien (admission criteria)
  2. Termine für Tag der offenen Tür / Infoabend / Schnuppertag (open days)

Schule: {schulname}
Schulform: {schulart}
Adresse: {strasse}, {plz} {stadt}
Website (Startpunkt): {website}

ANWEISUNGEN
- Lies die Hauptseite und folge Links wie "Anmeldung", "Aufnahme", "Termine",
  "Tag der offenen Tür", "Schule kennenlernen", "Schnuppertag", "Infoabend".
- Falls die Website wenig hergibt, ergänze mit gezielter Google-Suche nach
  dem exakten Schulnamen + "Tag der offenen Tür" oder "Anmeldung".
- Heutiges Datum: {today}. Verwerfe alle Termine, die VOR diesem Datum liegen.
- Datumsformat strikt ISO YYYY-MM-DD. Uhrzeiten als HH:MM (24h) oder null.
- Wenn nichts gefunden wird, gib leere Listen / null zurück — NIE raten.

ANTWORT-FORMAT (strikt JSON, keine Markdown-Code-Blöcke):
{{
  "admission_criteria_bullets": ["kurze Stichpunkte auf Deutsch"],
  "admission_application_window": {{"opens": "YYYY-MM-DD"|null, "closes": "YYYY-MM-DD"|null, "notes": "..."}} | null,
  "admission_notes_de": "ein kurzer ergänzender Absatz auf Deutsch",
  "open_days": [
    {{"date": "YYYY-MM-DD", "start_time": "HH:MM"|null, "end_time": "HH:MM"|null, "event_type": "Tag der offenen Tür"|"Infoabend"|"Schnuppertag"|"Anmeldetag"|"Sonstiges", "audience": "z.B. zukünftige Klasse 5", "notes": "..."}}
  ],
  "last_open_day_seen": "YYYY-MM-DD"|null,
  "source_grounding_urls": ["genaue URL(s) auf denen die Info stand"]
}}

Antworte NUR mit dem JSON-Objekt, kein Begleittext.
"""


def _build_prompt(school: dict) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return PROMPT_TEMPLATE.format(
        schulname=school["schulname"] or "(unbekannt)",
        schulart=school["schulart"] or "(unbekannt)",
        strasse=school["strasse"] or "(unbekannt)",
        plz=school["plz"] or "",
        stadt=school["stadt"] or "",
        website=school["website"],
        today=today,
    )

# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

def _init_gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY not set. Add it to .env (see .env.example)."
        )
    from google import genai  # type: ignore
    return genai.Client(api_key=api_key)


def _extract_text(response) -> Optional[str]:
    text = getattr(response, "text", None)
    if text:
        return text
    candidates = getattr(response, "candidates", None) or []
    if candidates and getattr(candidates[0], "content", None):
        parts = getattr(candidates[0].content, "parts", None) or []
        text_parts = [
            getattr(p, "text", None) for p in parts if getattr(p, "text", None)
        ]
        if text_parts:
            return "\n".join(text_parts)
    return None


def _extract_grounding_urls(response) -> List[str]:
    urls: List[str] = []
    candidates = getattr(response, "candidates", None) or []
    for cand in candidates:
        gm = getattr(cand, "grounding_metadata", None)
        if not gm:
            continue
        chunks = getattr(gm, "grounding_chunks", None) or []
        for chunk in chunks:
            web = getattr(chunk, "web", None)
            if web:
                uri = getattr(web, "uri", None)
                if uri:
                    urls.append(uri)
    # de-dupe preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _call_gemini(client, prompt: str, label: str, retry: int = 0, max_retries: int = 2) -> Dict[str, Any]:
    """Return {data, status, grounding_urls}. Status is one of VALID_STATUSES."""
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
    except Exception as exc:  # noqa: BLE001 — Gemini SDK raises plain Exception
        msg = str(exc)
        if "URL_RETRIEVAL_STATUS_ERROR" in msg or "URL_RETRIEVAL" in msg:
            logger.warning(f"  [{label}] URL unreachable")
            return {"data": None, "status": "url_unreachable", "grounding_urls": []}
        if ("RATE_LIMIT" in msg.upper() or "429" in msg) and retry < 3:
            wait = 30 * (retry + 1)
            logger.warning(f"  [{label}] Rate limited — waiting {wait}s")
            time.sleep(wait)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        if ("500" in msg or "INTERNAL" in msg) and retry < max_retries:
            logger.info(f"  [{label}] 5xx — retry {retry + 1}/{max_retries}")
            time.sleep(5)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        logger.warning(f"  [{label}] Gemini error: {exc}")
        return {"data": None, "status": "rate_limited" if "429" in msg else "parse_error", "grounding_urls": []}

    text = _extract_text(response)
    grounding = _extract_grounding_urls(response)
    if not text:
        if retry < max_retries:
            logger.info(f"  [{label}] empty response — retry {retry + 1}/{max_retries}")
            time.sleep(3)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        return {"data": None, "status": "parse_error", "grounding_urls": grounding}

    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        if retry < max_retries:
            logger.info(f"  [{label}] JSON parse error — retry {retry + 1}/{max_retries}")
            time.sleep(3)
            return _call_gemini(client, prompt, label, retry + 1, max_retries)
        logger.warning(f"  [{label}] JSON parse error after retries: {exc}")
        return {"data": None, "status": "parse_error", "grounding_urls": grounding}

    return {"data": data, "status": "success", "grounding_urls": grounding}

# ---------------------------------------------------------------------------
# Per-row processing
# ---------------------------------------------------------------------------

def _normalize_open_days(days: Any, today: str) -> List[dict]:
    if not isinstance(days, list):
        return []
    out = []
    for d in days:
        if not isinstance(d, dict):
            continue
        date_str = str(d.get("date", "")).strip()
        # Keep only well-formed future dates
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        if date_str < today:
            continue
        out.append({
            "date": date_str,
            "start_time": d.get("start_time") or None,
            "end_time": d.get("end_time") or None,
            "event_type": str(d.get("event_type", "Sonstiges") or "Sonstiges"),
            "audience": str(d.get("audience", "") or ""),
            "notes": str(d.get("notes", "") or ""),
        })
    out.sort(key=lambda x: x["date"])
    return out


def _build_row(school: dict, city_key: str, gemini_result: dict) -> dict:
    data = gemini_result.get("data") or {}
    status = gemini_result.get("status", "parse_error")
    today = datetime.now().strftime("%Y-%m-%d")

    bullets = data.get("admission_criteria_bullets") or []
    if not isinstance(bullets, list):
        bullets = []
    bullets = [str(b).strip() for b in bullets if str(b).strip()]

    window = data.get("admission_application_window")
    if window is not None and not isinstance(window, dict):
        window = None

    open_days = _normalize_open_days(data.get("open_days"), today)

    last_seen = data.get("last_open_day_seen")
    if last_seen:
        try:
            datetime.strptime(str(last_seen), "%Y-%m-%d")
            last_seen = str(last_seen)
        except ValueError:
            last_seen = ""
    else:
        last_seen = ""

    grounding = list(gemini_result.get("grounding_urls") or [])
    extra = data.get("source_grounding_urls") or []
    if isinstance(extra, list):
        for u in extra:
            if isinstance(u, str) and u not in grounding:
                grounding.append(u)

    # Refine status based on payload richness
    if status == "success" and not bullets and not window and not open_days:
        status = "no_admission_info" if not open_days else "no_open_days"
    elif status == "success" and not open_days and bullets:
        status = "success"  # admission only — still useful

    return {
        "schulnummer": school["schulnummer"],
        "city_key": city_key,
        "schulname": school["schulname"],
        "website_used": school["website"],
        "admission_criteria_bullets": json.dumps(bullets, ensure_ascii=False),
        "admission_application_window": json.dumps(window, ensure_ascii=False) if window else "",
        "admission_notes_de": str(data.get("admission_notes_de") or ""),
        "open_days": json.dumps(open_days, ensure_ascii=False),
        "last_open_day_seen": last_seen,
        "source_grounding_urls": json.dumps(grounding, ensure_ascii=False),
        "gemini_status": status if status in VALID_STATUSES else "parse_error",
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _skipped_row(school: dict, city_key: str, status: str) -> dict:
    return {
        "schulnummer": school["schulnummer"],
        "city_key": city_key,
        "schulname": school["schulname"],
        "website_used": school.get("website", ""),
        "admission_criteria_bullets": "[]",
        "admission_application_window": "",
        "admission_notes_de": "",
        "open_days": "[]",
        "last_open_day_seen": "",
        "source_grounding_urls": "[]",
        "gemini_status": status,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

# ---------------------------------------------------------------------------
# Per-city orchestration
# ---------------------------------------------------------------------------

def enrich_city(
    cfg: CityConfig,
    client,
    cache: Dict[str, dict],
    args: argparse.Namespace,
) -> pd.DataFrame:
    path = PROJECT_ROOT / cfg.final_path
    if not path.exists():
        logger.warning(f"[{cfg.key}] Final table missing: {path} — skipping city")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = _load_table(path)
    logger.info(f"[{cfg.key}] Loaded {len(df)} rows from {path.name}")

    rows_out: List[dict] = []
    eligible: List[tuple[int, str]] = []  # (df_index, normalized_url)
    for idx, row in df.iterrows():
        url = _pick_website(row, cfg)
        if not url:
            school = _row_to_school_info(row, cfg, website="")
            rows_out.append(_skipped_row(school, cfg.key, "skipped_no_url"))
            continue
        eligible.append((idx, url))

    logger.info(
        f"[{cfg.key}] {len(eligible)} schools have usable website URLs, "
        f"{len(df) - len(eligible)} skipped"
    )

    if args.limit:
        eligible = eligible[: args.limit]
        logger.info(f"[{cfg.key}] --limit {args.limit} → processing {len(eligible)}")

    if args.dry_run:
        for idx, url in eligible:
            school = _row_to_school_info(df.loc[idx], cfg, website=url)
            rows_out.append(_skipped_row(school, cfg.key, "dry_run"))
        return pd.DataFrame(rows_out, columns=OUTPUT_COLUMNS)

    cache_hits = 0
    api_calls = 0
    for n, (idx, url) in enumerate(eligible, start=1):
        school = _row_to_school_info(df.loc[idx], cfg, website=url)
        label = f"{cfg.key}/{school['schulnummer']}"
        cache_key = _url_cache_key(url)

        cached = cache.get(cache_key)
        if cached and not args.refresh_cache and _cache_is_fresh(cached):
            cache_hits += 1
            rows_out.append(_build_row(school, cfg.key, cached["result"]))
            continue

        prompt = _build_prompt(school)
        result = _call_gemini(client, prompt, label)
        api_calls += 1
        cache[cache_key] = {
            "result": result,
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": url,
        }
        rows_out.append(_build_row(school, cfg.key, result))

        if api_calls % SAVE_INTERVAL == 0:
            _save_cache(cache)
            logger.info(f"[{cfg.key}] progress {n}/{len(eligible)} — cache flushed")

        time.sleep(REQUEST_DELAY_SECONDS)

    if api_calls:
        _save_cache(cache)
    logger.info(
        f"[{cfg.key}] done — {api_calls} Gemini calls, {cache_hits} cache hits"
    )
    return pd.DataFrame(rows_out, columns=OUTPUT_COLUMNS)


def _write_outputs(city_key: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    out_dir = PROJECT_ROOT / f"data_{_data_dir_for_city(city_key)}" / "intermediate"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{city_key}_admission_open_days.csv"
    parquet_path = out_dir / f"{city_key}_admission_open_days.parquet"
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[{city_key}] parquet write failed ({exc}) — CSV only")
    logger.info(f"[{city_key}] wrote {len(df)} rows → {csv_path.name}")


def _data_dir_for_city(city_key: str) -> str:
    """Map city_key (e.g. 'frankfurt_primary') to data_{dir} (e.g. 'frankfurt')."""
    # Single-table cities map directly; primary/secondary share one data_ dir
    # except the *_primary cities that have a dedicated dir.
    dedicated_primary = {"berlin_primary", "hamburg_primary"}
    if city_key in dedicated_primary:
        return city_key
    return city_key.split("_")[0]

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_cities_arg(value: str) -> List[CityConfig]:
    if value == "all":
        return list(CITY_REGISTRY)
    keys = [k.strip() for k in value.split(",") if k.strip()]
    out: List[CityConfig] = []
    for k in keys:
        if k not in CITY_BY_KEY:
            raise SystemExit(
                f"Unknown city key: {k!r}. Valid keys: "
                + ", ".join(c.key for c in CITY_REGISTRY)
            )
        out.append(CITY_BY_KEY[k])
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", default="all", help="'all' or comma-separated city keys")
    parser.add_argument("--limit", type=int, default=0, help="Cap rows per city (0 = no limit)")
    parser.add_argument("--refresh-cache", action="store_true", help="Ignore cache; re-call Gemini")
    parser.add_argument("--dry-run", action="store_true", help="Parse + filter only; no Gemini calls")
    parser.add_argument("--combine", action="store_true", help="Write data_shared/admission_open_days_all_german_cities.csv after per-city runs")
    parser.add_argument("--list-cities", action="store_true", help="Print registry and exit")
    args = parser.parse_args(argv)

    if args.list_cities:
        for c in CITY_REGISTRY:
            print(f"{c.key:24s}  →  {c.final_path}  (col={c.website_col})")
        return 0

    cities = _parse_cities_arg(args.cities)
    logger.info(f"Cities to process: {[c.key for c in cities]}")

    cache = _load_cache()
    logger.info(f"Cache: {len(cache)} entries (TTL {CACHE_TTL_DAYS} days)")

    client = None
    if not args.dry_run:
        try:
            client = _init_gemini_client()
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to init Gemini client: {exc}")
            return 1

    combined_frames: List[pd.DataFrame] = []
    for cfg in cities:
        df = enrich_city(cfg, client, cache, args)
        if not args.dry_run or args.combine:
            _write_outputs(cfg.key, df)
        if args.combine:
            combined_frames.append(df)

    if args.combine:
        combined = pd.concat(combined_frames, ignore_index=True) if combined_frames else pd.DataFrame(columns=OUTPUT_COLUMNS)
        COMBINED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(COMBINED_OUTPUT, index=False)
        logger.info(f"Wrote combined output: {COMBINED_OUTPUT} ({len(combined)} rows)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
