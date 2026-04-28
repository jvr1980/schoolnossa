#!/usr/bin/env python3
"""
Munich: recover traegerschaft (and related metadata) for schools currently
without it by combining Places API + Gemini.

Two-stage flow per school:
  1. If the school lacks a website, run Places `searchText` with
     schulname + full address. Accept looser than the earlier contact
     enrichment: require distance <= 500 m, at least one non-generic
     token in common, and no non-school marker. We're OK with lower
     name similarity because these are the tail schools that already
     failed the strict pass; Places is still authoritative for
     schul-type and distance.
  2. For every eligible school (whether its website came from the raw
     source, the earlier contact enrichment, or step 1) call Gemini
     2.5-flash with URL context + Google Search grounding and extract
     structured metadata: traegerschaft, is_private_school, leitung,
     schueler_2024_25, lehrer_2024_25, sprachen, gruendungsjahr,
     besonderheiten.
  3. Schools that still have no website after step 1 get a Gemini
     Google-Search-only classification for traegerschaft.
  4. Writes outputs back into munich raw + final + berlin_schema
     parquet/CSV variants, gap-fill semantics — never overwrites a
     non-null existing value.

Inputs:
  data_munich/final/munich_{primary,secondary}_school_master_table_final.csv
  data_munich/intermediate/munich_{primary,secondary}_schools_with_places_contact.csv
    (prior Places matches — we read these so the new Places calls are
    only for schools that really lack a URL)

Outputs:
  data_munich/intermediate/munich_traegerschaft_recovery.csv
  data_munich/cache/traegerschaft_recovery/cache.json
  Final + berlin_schema parquet/csv files are patched in place with
  discovered websites and traegerschaft.

Usage:
  python3 scripts_munich/enrichment/munich_recover_missing_metadata.py \\
          --dry-run --limit 5
  python3 scripts_munich/enrichment/munich_recover_missing_metadata.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("munich_recover")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
ENV_FILE = PROJECT_ROOT / ".env"

CACHE_DIR = DATA_DIR / "cache" / "traegerschaft_recovery"
CACHE_FILE = CACHE_DIR / "cache.json"

# Load .env
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(ENV_FILE)
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    if k.strip() and k.strip() not in os.environ:
                        os.environ[k.strip()] = v.strip().strip('"').strip("'")

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

PLACES_TEXT_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_FIELDS = (
    "places.id,places.displayName,places.formattedAddress,"
    "places.location,places.types,places.websiteUri"
)

GEMINI_MODEL = "gemini-2.5-flash"
REQUEST_DELAY_SEC = 1.2
SAVE_INTERVAL = 10

# Looser Places acceptance thresholds (the strict pass already happened earlier)
LOOSE_DIST_MAX_M = 500
LOOSE_MIN_TOKEN_OVERLAP = 1

# Supabase
SUPABASE_URL = "https://whzvzoumldeqgyrqlilt.supabase.co/rest/v1"
SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoenZ6b3VtbGRlcWd5cnFsaWx0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg3OTQ0MzEsImV4cCI6MjA4NDM3MDQzMX0."
    "ex4S1up25OAcGD8hQoOSfzf3NVAG5qCmNriixYfAAKs"
)

# ---------------------------------------------------------------------------
# Token helpers (kept simple — not trying to replicate the strict matcher)
# ---------------------------------------------------------------------------

GENERIC_TOKENS = {
    "der", "die", "das", "und", "am", "im", "in", "an", "a.d.",
    "schule", "schulen", "grundschule", "mittelschule", "hauptschule",
    "realschule", "gymnasium", "wirtschaftsschule", "förderzentrum",
    "förderschule", "fachoberschule", "berufsschule", "berufsoberschule",
    "münchen", "muenchen", "staatliche", "städtische", "staatlich",
    "städtisch", "privat", "bayerische", "private", "staatl", "städt",
    "gmbh", "ev", "e.v", "e.v.", "straße", "strasse", "str", "weg",
    "allee", "platz", "ring", "gasse",
}

_TOKEN_RE = re.compile(r"[^\w\säöüß]+", re.UNICODE)

NON_SCHOOL_MARKERS = (
    "sporthalle", "turnhalle", "förderverein", "forderverein",
    "elternbeirat", "schwimmschule", "fahrschule", "musikschule",
    "sprachschule", "tanzschule", "reitschule", "volkshochschule",
)


def _tokens(s: str) -> set:
    s = (s or "").lower()
    s = _TOKEN_RE.sub(" ", s)
    return {t for t in s.split() if t and len(t) > 1}


def _distinctive(s: str) -> set:
    return _tokens(s) - GENERIC_TOKENS


def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ---------------------------------------------------------------------------
# Places API
# ---------------------------------------------------------------------------

def places_find_school(schulname: str, strasse: str, plz: str, stadt: str,
                       lat: float, lng: float) -> Optional[dict]:
    """Return the best Places match or None. Looser acceptance rules than the
    earlier strict contact enrichment."""
    if not GOOGLE_PLACES_API_KEY:
        return None

    query = f"{schulname} {strasse} {plz} {stadt}".strip()
    body = {
        "textQuery": query,
        "locationBias": {"circle": {"center": {"latitude": lat, "longitude": lng},
                                     "radius": 1500.0}},
        "maxResultCount": 10,
        "languageCode": "de",
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": PLACES_FIELDS,
    }
    try:
        req = urllib.request.Request(PLACES_TEXT_URL, data=json.dumps(body).encode(),
                                     headers=headers, method="POST")
        resp = urllib.request.urlopen(req, timeout=20)
        places = json.loads(resp.read().decode()).get("places", [])
    except Exception as e:
        logger.warning(f"  Places search failed for {schulname[:40]}: {e}")
        return None

    query_tokens = _distinctive(schulname)

    best = None
    for p in places:
        name = ""
        dn = p.get("displayName")
        if isinstance(dn, dict):
            name = dn.get("text", "")

        # Non-school markers always reject
        lname = name.lower()
        if any(marker in lname for marker in NON_SCHOOL_MARKERS):
            continue

        loc = p.get("location") or {}
        plat = loc.get("latitude")
        plng = loc.get("longitude")
        if plat is None or plng is None:
            continue
        dist = _haversine_m(lat, lng, plat, plng)
        if dist > LOOSE_DIST_MAX_M:
            continue

        # Require school-like type
        types = set(p.get("types") or [])
        looks_like_school = any(t in types for t in ("school", "primary_school", "secondary_school"))
        if not looks_like_school:
            continue

        # Token overlap (distinctive)
        overlap = len(query_tokens & _distinctive(name))
        if overlap < LOOSE_MIN_TOKEN_OVERLAP and "schule" not in lname:
            continue

        score = overlap * 10 - (dist / 100.0)
        if best is None or score > best[0]:
            best = (score, p, dist, overlap, name)

    return None if best is None else {
        "place_id": best[1].get("id"),
        "name": best[4],
        "distance_m": round(best[2], 1),
        "token_overlap": best[3],
        "website": best[1].get("websiteUri"),
    }

# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

PROMPT_WITH_WEBSITE = """Klassifiziere die Trägerschaft und extrahiere
Stammdaten dieser deutschen Schule.

Schule:  {schulname}
Adresse: {adresse}
Website: {website}

ANWEISUNGEN
- Nutze URL-Context (Website) und Google-Suche.
- Setze Werte nur, wenn du sie zuverlässig gefunden hast. Im Zweifel null.
- "traegerschaft" ist genau einer dieser Werte:
    "staatlich"   — Bundesland/Staat ist Träger
    "städtisch"   — Kommune/Stadt ist Träger
    "privat"      — freie/private Trägerschaft ohne Kirchenbezug
    "kirchlich"   — evangelisch, katholisch, Jesuiten, klarer Kirchenbezug
    null          — nicht sicher feststellbar
- Hinweise im Schulnamen sind oft ausreichend: "Staatliche", "Städtische",
  "Freie Waldorf", "Montessori", "kath.", "ev.".

ANTWORT-FORMAT (strikt JSON, kein Markdown):
{{
  "traegerschaft": "staatlich"|"städtisch"|"privat"|"kirchlich"|null,
  "schueler": int|null,
  "lehrer": int|null,
  "sprachen": ["Englisch"]|null,
  "gruendungsjahr": int|null,
  "leitung": "Vorname Nachname"|null,
  "besonderheiten": "string|null",
  "confidence": 0.0..1.0,
  "reasoning": "1–2 Sätze"
}}
"""

PROMPT_NO_WEBSITE = """Klassifiziere die Trägerschaft dieser deutschen Schule,
die keine bekannte Website hat. Recherchiere per Google-Suche.

Schule:  {schulname}
Adresse: {adresse}

Gleiche Regeln wie oben. Antworte mit dem JSON-Objekt mit den gleichen
Feldern; lass Zahlenfelder null, falls unsicher.
"""


def _init_gemini():
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set")
    from google import genai  # type: ignore
    return genai.Client(api_key=GEMINI_API_KEY)


def _call_gemini(client, prompt: str, label: str, tools_kind: str,
                 retry: int = 0, max_retries: int = 2) -> Dict[str, Any]:
    from google.genai import types  # type: ignore

    if tools_kind == "url":
        tools = [types.Tool(url_context=types.UrlContext()),
                 types.Tool(google_search=types.GoogleSearch())]
    else:
        tools = [types.Tool(google_search=types.GoogleSearch())]

    try:
        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(tools=tools, temperature=0),
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if ("RATE_LIMIT" in msg.upper() or "429" in msg) and retry < 3:
            time.sleep(30 * (retry + 1))
            return _call_gemini(client, prompt, label, tools_kind, retry + 1, max_retries)
        if ("500" in msg or "INTERNAL" in msg) and retry < max_retries:
            time.sleep(5)
            return _call_gemini(client, prompt, label, tools_kind, retry + 1, max_retries)
        logger.warning(f"  [{label}] gemini error: {exc}")
        return {"data": None, "status": "error"}

    text = getattr(resp, "text", None)
    if not text:
        candidates = getattr(resp, "candidates", None) or []
        if candidates and getattr(candidates[0], "content", None):
            parts = getattr(candidates[0].content, "parts", None) or []
            tp = [getattr(p, "text", None) for p in parts if getattr(p, "text", None)]
            text = "\n".join(tp) if tp else None
    if not text:
        if retry < max_retries:
            time.sleep(3)
            return _call_gemini(client, prompt, label, tools_kind, retry + 1, max_retries)
        return {"data": None, "status": "empty"}

    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        if retry < max_retries:
            time.sleep(3)
            return _call_gemini(client, prompt, label, tools_kind, retry + 1, max_retries)
        return {"data": None, "status": "parse_error"}

    return {"data": data, "status": "success"}

# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------

_VALID_TRAEGER = {"staatlich", "städtisch", "privat", "kirchlich"}


def _normalize(raw: Optional[dict]) -> dict:
    out = {
        "traegerschaft": None,
        "is_private_school": None,
        "schueler_2024_25": None,
        "lehrer_2024_25": None,
        "sprachen": None,
        "gruendungsjahr": None,
        "leitung": None,
        "besonderheiten": None,
        "confidence": None,
    }
    if not isinstance(raw, dict):
        return out
    t = raw.get("traegerschaft")
    if t:
        ts = str(t).strip().lower()
        if ts in _VALID_TRAEGER:
            out["traegerschaft"] = ts
            out["is_private_school"] = ts in {"privat", "kirchlich"}

    def _int(v, lo, hi):
        try:
            n = int(float(v))
        except (ValueError, TypeError):
            return None
        return n if lo <= n <= hi else None

    out["schueler_2024_25"] = _int(raw.get("schueler"), 1, 5000)
    out["lehrer_2024_25"] = _int(raw.get("lehrer"), 1, 500)
    g = _int(raw.get("gruendungsjahr"), 1500, 2026)
    out["gruendungsjahr"] = g
    leitung = raw.get("leitung")
    if leitung:
        s = str(leitung).strip()
        if s and s.lower() not in ("null", "none", "nan"):
            out["leitung"] = s
    besond = raw.get("besonderheiten")
    if besond:
        s = str(besond).strip()
        if s and s.lower() not in ("null", "none", "nan"):
            out["besonderheiten"] = s[:200]
    sprachen = raw.get("sprachen")
    if isinstance(sprachen, list):
        items = [str(x).strip() for x in sprachen if str(x).strip()]
        out["sprachen"] = ", ".join(items) if items else None
    elif sprachen:
        out["sprachen"] = str(sprachen).strip() or None
    try:
        c = raw.get("confidence")
        if c is not None:
            out["confidence"] = max(0.0, min(1.0, float(c)))
    except (ValueError, TypeError):
        pass
    return out

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _load_cache() -> Dict[str, dict]:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def _save_cache(cache: Dict[str, dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
    tmp.replace(CACHE_FILE)

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_munich_rows(school_type: str) -> pd.DataFrame:
    p = DATA_DIR / "final" / f"munich_{school_type}_school_master_table_final.csv"
    df = pd.read_csv(p, low_memory=False)
    df["_school_type"] = school_type
    df["_table"] = "primary_schools" if school_type == "primary" else "schools"
    return df


def _is_blank(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    return str(v).strip() in ("", "nan", "None", "NaN", "<NA>")


# ---------------------------------------------------------------------------
# Row processing
# ---------------------------------------------------------------------------

def process_row(client, row: pd.Series, cache: Dict[str, dict],
                dry_run: bool) -> dict:
    snr = str(row["schulnummer"])
    label = f"muenchen/{snr}"

    # Use cached if we have it
    if snr in cache and "result" in cache[snr]:
        cached = cache[snr]
        return {**cached["result"], "source": "cache",
                "website_used": cached.get("website_used"),
                "schulnummer": snr}

    schulname = str(row.get("schulname", "") or "")
    strasse = str(row.get("strasse", "") or "")
    plz = str(row.get("plz", "") or "")
    stadt = str(row.get("stadt", "München") or "München")
    adresse = f"{strasse}, {plz} {stadt}".strip(", ")
    lat = row.get("latitude")
    lng = row.get("longitude")

    current_website = str(row.get("website", "") or "").strip()
    if current_website.lower() in ("nan", "none", ""):
        current_website = ""

    discovered_website = ""
    discovered_via = None
    if not current_website and lat is not None and lng is not None and not pd.isna(lat) and not pd.isna(lng):
        if dry_run:
            discovered_via = "dry_run"
        else:
            match = places_find_school(schulname, strasse, plz, stadt, float(lat), float(lng))
            if match and match.get("website"):
                discovered_website = match["website"]
                discovered_via = "places_loose"
                logger.info(f"  [{label}] Places found website ({match['distance_m']}m, "
                            f"overlap={match['token_overlap']}): {match['name'][:50]}")
            time.sleep(0.1)

    website_used = current_website or discovered_website

    if dry_run:
        return {
            "schulnummer": snr,
            "schulname": schulname,
            "_table": row["_table"],
            "website_used": website_used,
            "source": "dry_run",
            "traegerschaft": None,
            "is_private_school": None,
            "schueler_2024_25": None,
            "lehrer_2024_25": None,
            "sprachen": None,
            "gruendungsjahr": None,
            "leitung": None,
            "besonderheiten": None,
            "confidence": None,
            "website_source": discovered_via or "existing" if current_website else None,
        }

    # Gemini call
    if website_used:
        prompt = PROMPT_WITH_WEBSITE.format(
            schulname=schulname or "(unbekannt)",
            adresse=adresse or "(unbekannt)",
            website=website_used,
        )
        gem = _call_gemini(client, prompt, label, tools_kind="url")
    else:
        prompt = PROMPT_NO_WEBSITE.format(
            schulname=schulname or "(unbekannt)",
            adresse=adresse or "(unbekannt)",
        )
        gem = _call_gemini(client, prompt, label, tools_kind="search_only")

    result = _normalize(gem.get("data"))
    cache[snr] = {
        "result": result,
        "website_used": website_used,
        "website_source": discovered_via or ("existing" if current_website else None),
        "gem_status": gem.get("status"),
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return {
        "schulnummer": snr,
        "schulname": schulname,
        "_table": row["_table"],
        "website_used": website_used,
        "website_source": discovered_via or ("existing" if current_website else None),
        "source": "fresh",
        **result,
    }

# ---------------------------------------------------------------------------
# Merge back into parquet/csv outputs
# ---------------------------------------------------------------------------

FINAL_PATHS = lambda t: [
    DATA_DIR / "raw" / f"munich_{t}_schools_raw.csv",
    DATA_DIR / "final" / f"munich_{t}_school_master_table.csv",
    DATA_DIR / "final" / f"munich_{t}_school_master_table.parquet",
    DATA_DIR / "final" / f"munich_{t}_school_master_table_final.csv",
    DATA_DIR / "final" / f"munich_{t}_school_master_table_final_with_embeddings.parquet",
    DATA_DIR / "final" / f"munich_{t}_school_master_table_berlin_schema.csv",
    DATA_DIR / "final" / f"munich_{t}_school_master_table_berlin_schema.parquet",
]

UPDATE_COLS = ["website", "traegerschaft", "schueler_2024_25", "lehrer_2024_25",
               "sprachen", "gruendungsjahr", "leitung", "besonderheiten"]


def _merge_into_parquet(path: Path, src_by_snr: Dict[str, dict]) -> str:
    if not path.exists():
        return "missing"
    df = pd.read_csv(path, low_memory=False) if path.suffix == ".csv" else pd.read_parquet(path)
    for c in UPDATE_COLS:
        if c not in df.columns:
            df[c] = pd.Series([pd.NA] * len(df), dtype=object)
        elif df[c].dtype != object:
            df[c] = df[c].astype(object)
    df["schulnummer"] = df["schulnummer"].astype(str)
    filled = {c: 0 for c in UPDATE_COLS}
    for idx, r in df.iterrows():
        src = src_by_snr.get(str(r["schulnummer"]))
        if not src:
            continue
        for c in UPDATE_COLS:
            if c == "website":
                src_val = src.get("website_used")
            else:
                src_val = src.get(c)
            if _is_blank(r.get(c)) and not _is_blank(src_val):
                df.at[idx, c] = src_val
                filled[c] += 1
    if path.suffix == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        df.to_parquet(path, index=False)
    return " ".join(f"{c}+{n}" for c, n in filled.items() if n)

# ---------------------------------------------------------------------------
# Supabase upload
# ---------------------------------------------------------------------------

def _upload(table: str, snr: str, payload: dict) -> int:
    if not payload:
        return 0
    # Build is_null filter chain so we only fill gaps
    filters = "&".join(f"{k}=is.null" for k in payload)
    req = urllib.request.Request(
        f"{SUPABASE_URL}/{table}?schulnummer=eq.{snr}&{filters}",
        data=json.dumps(payload).encode(),
        headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }, method="PATCH",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.status
    except urllib.error.HTTPError as e:
        return e.code

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only-missing", action="store_true", default=True,
                        help="Only process schools missing traegerschaft (default)")
    parser.add_argument("--all", action="store_true", help="Process every Munich school")
    parser.add_argument("--upload", action="store_true",
                        help="After recovery, PATCH Supabase (needs RLS policy on traegerschaft)")
    args = parser.parse_args(argv)

    if args.all:
        only_missing = False
    else:
        only_missing = args.only_missing

    frames = [_load_munich_rows("primary"), _load_munich_rows("secondary")]
    df = pd.concat(frames, ignore_index=True)
    logger.info(f"Loaded {len(df)} Munich rows")

    if only_missing:
        before = len(df)
        df = df[df["traegerschaft"].isna() | (df["traegerschaft"].astype(str).str.strip() == "")]
        logger.info(f"Missing-traegerschaft rows: {len(df)}/{before}")

    if args.limit:
        df = df.head(args.limit)
        logger.info(f"--limit {args.limit} → processing {len(df)}")

    cache = _load_cache()
    client = None if args.dry_run else _init_gemini()

    results: List[dict] = []
    fresh_calls = 0
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        r = process_row(client, row, cache, dry_run=args.dry_run)
        results.append(r)
        if r.get("source") == "fresh":
            fresh_calls += 1
            if fresh_calls % SAVE_INTERVAL == 0 and not args.dry_run:
                _save_cache(cache)
                logger.info(f"  progress {i}/{len(df)} — fresh={fresh_calls}")
            time.sleep(REQUEST_DELAY_SEC)

    if not args.dry_run:
        _save_cache(cache)

    out = pd.DataFrame(results)
    out_path = DATA_DIR / "intermediate" / "munich_traegerschaft_recovery.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(f"Saved: {out_path}")

    # Summary
    print("\n" + "=" * 68)
    print("MUNICH TRAEGERSCHAFT RECOVERY")
    print("=" * 68)
    print(f"  Rows processed:  {len(out)}")
    print(f"  Fresh calls:     {fresh_calls}")
    print(f"  Cache hits:      {(out['source'] == 'cache').sum()}")
    recovered_web = ((out["website_source"] == "places_loose")).sum() if "website_source" in out.columns else 0
    print(f"  Websites recovered via Places: {recovered_web}")
    got_traeger = out["traegerschaft"].notna().sum()
    print(f"  traegerschaft determined:     {got_traeger}/{len(out)}")
    if got_traeger:
        print(f"  distribution:")
        for t, n in out.loc[out["traegerschaft"].notna(), "traegerschaft"].value_counts().items():
            print(f"    {t}: {n}")
        priv = out["is_private_school"].eq(True).sum()
        print(f"  private/kirchlich: {priv}")

    # Merge into parquet/csv files
    if not args.dry_run:
        src_by_snr = {}
        for r in results:
            snr = r["schulnummer"]
            src = {c: r.get(c) for c in UPDATE_COLS if c != "website"}
            src["website_used"] = r.get("website_used") or ""
            src_by_snr[snr] = src
        print("\nMerging into Munich files (fill-gaps only):")
        for typ in ("primary", "secondary"):
            for p in FINAL_PATHS(typ):
                stat = _merge_into_parquet(p, src_by_snr)
                if stat not in ("missing", ""):
                    print(f"    {p.name}: {stat}")

    # Optional upload
    if args.upload and not args.dry_run:
        print("\nUploading to Supabase (fill-gaps):")
        stats = {"patched": 0, "non_null": 0, "error": 0}
        for r in results:
            snr = r["schulnummer"]
            table = r["_table"]
            payload = {}
            for c in UPDATE_COLS:
                if c == "website":
                    v = r.get("website_used")
                else:
                    v = r.get(c)
                if _is_blank(v):
                    continue
                payload[c] = v
            if not payload:
                continue
            code = _upload(table, snr, payload)
            if code == 204:
                stats["patched"] += 1
            elif code in (400,):
                stats["non_null"] += 1
            else:
                stats["error"] += 1
        print(f"  {stats}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
