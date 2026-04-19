#!/usr/bin/env python3
"""
Munich Places Contact Enrichment

Uses Google Places API to recover telefon + website for Munich schools.
Source (jedeschule.codefor.de) only provides name/address/coordinates.

Matching strategy (coords + address + name triple-check):
1. Places API v1 searchNearby with includedTypes=[school/primary_school/secondary_school],
   radius=150m around the school's coordinates.
2. Score candidates by (a) fuzzy name similarity (token-set ratio) and
   (b) haversine distance. Accept the top candidate only if
   name_similarity >= 0.5 AND distance <= 150m.
3. Fallback: searchText with `"{schulname} {strasse}"` biased to the coord,
   same acceptance threshold.
4. Reject with no_match (logged for manual review — never fill a bad match).

Output:
- data_munich/intermediate/munich_{type}_schools_with_places_contact.csv
  with telefon + website populated where we found a match.
- data_munich/cache/places/contact/{schulnummer}.json per school
- match_report.csv in the cache dir showing every school's match status.

CLI:
    --dry-run          Plan + match but do not touch CSVs (still caches API calls)
    --school-type X    primary | secondary | both (default: both)
    --limit N          Process only first N schools (for dry-runs)
    --force            Ignore cache, re-query Places for every school
"""

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# Config
# ============================================================================
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache" / "places" / "contact"

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')

NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Match acceptance thresholds
NEARBY_RADIUS_M = 150           # Places nearby search radius
ACCEPT_MAX_DISTANCE_M = 200     # Must be within this distance to accept
ACCEPT_MIN_NAME_SIM = 0.5       # Token-set ratio

# API etiquette
REQUEST_DELAY_S = 0.15
MAX_RETRIES = 3

# Fields we need — Places API v1 FieldMask
NEARBY_FIELDS = (
    "places.id,places.displayName,places.formattedAddress,places.location,"
    "places.types,places.nationalPhoneNumber,places.internationalPhoneNumber,"
    "places.websiteUri"
)

SCHOOL_TYPES_FOR_SEARCH = ["school", "primary_school", "secondary_school"]

# ============================================================================
# Helpers
# ============================================================================

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


_TOKEN_CLEAN = re.compile(r"[^\w\säöüß]+", re.UNICODE)

# Specific German school-type categories. Used to detect cross-type mismatches
# (e.g. a query for 'Mittelschule' must not match a 'Grundschule' result).
SCHOOL_TYPE_KEYWORDS = {
    "grundschule", "mittelschule", "hauptschule", "realschule",
    "gymnasium", "gemeinschaftsschule", "gesamtschule", "sekundarschule",
    "oberschule", "wirtschaftsschule", "waldorfschule", "montessorischule",
    "stadtteilschule",
    # Förder-/Sonder-schulen are a distinct bucket — must not match a regular
    # Grundschule or Gymnasium even if they share a street.
    "förderzentrum", "förderschule", "sonderschule", "sonderpädagogisches",
    # Vocational is its own thing
    "berufsschule", "fachoberschule", "berufsoberschule",
}

# Words that mark a place as non-school, even if it co-locates with a school.
# These are never academic schools despite having "schule" in the name.
NON_SCHOOL_MARKERS = (
    "sporthalle", "turnhalle", "förderverein", "forderverein",
    "elternbeirat", "stiftung e.v", "freundeskreis",
    "kindergarten", "hort ",  # Hort = after-school program, distinct from school
    "schwimmschule", "fahrschule", "musikschule", "sprachschule",
    "tanzschule", "reitschule", "kochschule", "yogaschule",
    "hundeschule", "volkshochschule",
)

# Generic words that shouldn't drive name-similarity on their own.
GENERIC_NAME_TOKENS = {
    "der", "die", "das", "und", "am", "im", "in", "an",
    "schule", "schul", "schulen",
    "grundschule", "mittelschule", "hauptschule", "realschule",
    "gymnasium", "gemeinschaftsschule", "gesamtschule", "sekundarschule",
    "oberschule", "wirtschaftsschule", "waldorfschule", "förderzentrum",
    "förderschule", "sonderschule", "fachoberschule", "berufsschule",
    "berufsoberschule", "sonderpädagogisches", "sonderpädagogisch",
    "münchen", "muenchen", "münchener", "staatliche", "städtische",
    "privat", "private", "privater", "bayerische", "staatlich",
    "gmbh", "ev", "e.v", "e.v.",
    # Street-generic tokens — appear in many unrelated schools on the same street
    "straße", "strasse", "str", "weg", "allee", "platz", "ring", "gasse",
}


_STREET_SCHOOL_SUFFIXES = (
    "straße", "strasse", "schule",
    "weg", "allee", "platz", "ring", "gasse",
)


def _strip_geo_suffix(token: str):
    """'klenzestraße' -> 'klenze', 'klenzeschule' -> 'klenze'. Returns None if
    no geo suffix is present so the caller can decide whether to keep it.
    """
    for suffix in _STREET_SCHOOL_SUFFIXES:
        if token.endswith(suffix) and len(token) > len(suffix) + 2:
            return token[: -len(suffix)]
    return None


def _tokens(s: str) -> set:
    s = (s or "").lower()
    s = _TOKEN_CLEAN.sub(" ", s)
    return {t for t in s.split() if t and len(t) > 1}


def _distinctive_tokens(s: str) -> set:
    """Non-generic tokens plus geo-suffix-stripped stems.

    Grundschule München, Klenzestraße 48  -> {klenzestraße, 48, klenze}
    Klenzeschule                          -> {klenzeschule, klenze}
    -> overlap {klenze} — same school.
    """
    out = set()
    for t in _tokens(s):
        if t in GENERIC_NAME_TOKENS:
            continue
        out.add(t)
        stem = _strip_geo_suffix(t)
        if stem and len(stem) >= 3:
            out.add(stem)
    return out


# Equivalent school-type groupings — return the canonical name so that
# 'hauptschule' and 'mittelschule' don't trigger a type-mismatch rejection
# (Bavaria renamed all Hauptschulen to Mittelschulen in 2011).
SCHOOL_TYPE_SYNONYMS = {
    'hauptschule': 'mittelschule',
}


def _school_type_of(s: str):
    toks = _tokens(s)
    for kw in SCHOOL_TYPE_KEYWORDS:
        if kw in toks:
            return SCHOOL_TYPE_SYNONYMS.get(kw, kw)
    return None


def is_non_school_entity(match_name: str) -> bool:
    n = (match_name or "").lower()
    return any(marker in n for marker in NON_SCHOOL_MARKERS)


def name_similarity(a: str, b: str) -> float:
    """Overlap coefficient on non-generic tokens: |A ∩ B| / min(|A|, |B|).

    Overlap is preferred over Jaccard because schulnames are often verbose
    ('Anni-Braun-Schule, Förderzentrum, Förderschwerpunkt Sprache ...')
    while the Places entry is a terse canonical name ('Anni-Braun-Schule').
    Jaccard punishes that shape; overlap rewards 'shorter is a subset'.
    Pairs with <2 distinctive tokens fall back to char-level similarity so
    single-word canonical names still resolve.
    """
    da, db = _distinctive_tokens(a), _distinctive_tokens(b)
    if len(da) >= 1 and len(db) >= 1:
        inter = len(da & db)
        return inter / min(len(da), len(db))
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()


def _cache_path(schulnummer: str) -> Path:
    safe = hashlib.sha1(str(schulnummer).encode()).hexdigest()[:16]
    return CACHE_DIR / f"{safe}.json"


def _load_cache(schulnummer: str):
    p = _cache_path(schulnummer)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def _save_cache(schulnummer: str, data: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(schulnummer).write_text(json.dumps(data, ensure_ascii=False, indent=2))


# ============================================================================
# Places API
# ============================================================================

def _post_with_retry(url, body, field_mask):
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": field_mask,
    }
    backoff = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=20)
            if resp.status_code == 200:
                return resp.json(), None
            # 4xx that isn't rate limiting — stop retrying
            if resp.status_code in (400, 401, 403, 404):
                return None, f"HTTP {resp.status_code}: {resp.text[:200]}"
            time.sleep(backoff)
            backoff = min(backoff * 2, 20)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                return None, f"Request error: {e}"
            time.sleep(backoff)
            backoff = min(backoff * 2, 20)
    return None, f"Exhausted {MAX_RETRIES} retries"


def nearby_search(lat, lon, radius_m=NEARBY_RADIUS_M):
    body = {
        "includedTypes": SCHOOL_TYPES_FOR_SEARCH,
        "maxResultCount": 20,
        "locationRestriction": {
            "circle": {"center": {"latitude": lat, "longitude": lon}, "radius": float(radius_m)}
        },
        "languageCode": "de",
    }
    data, err = _post_with_retry(NEARBY_SEARCH_URL, body, NEARBY_FIELDS)
    time.sleep(REQUEST_DELAY_S)
    if err:
        return [], err
    return data.get("places", []), None


def text_search(query, lat, lon, radius_m=500):
    body = {
        "textQuery": query,
        "locationBias": {
            "circle": {"center": {"latitude": lat, "longitude": lon}, "radius": float(radius_m)}
        },
        "maxResultCount": 10,
        "languageCode": "de",
    }
    data, err = _post_with_retry(TEXT_SEARCH_URL, body, NEARBY_FIELDS)
    time.sleep(REQUEST_DELAY_S)
    if err:
        return [], err
    return data.get("places", []), None


# ============================================================================
# Match evaluation
# ============================================================================

def _display_name(place):
    d = place.get("displayName")
    if isinstance(d, dict):
        return d.get("text", "")
    return str(d) if d else ""


def score_candidates(school_name, school_lat, school_lng, places):
    """Return list of (place, distance_m, name_sim) sorted by score desc."""
    scored = []
    for p in places:
        loc = p.get("location") or {}
        lat, lng = loc.get("latitude"), loc.get("longitude")
        if lat is None or lng is None:
            continue
        dist = haversine_m(school_lat, school_lng, lat, lng)
        sim = name_similarity(school_name, _display_name(p))
        scored.append((p, dist, sim))

    # Sort: primary by name similarity, secondary by proximity (inverse)
    scored.sort(key=lambda x: (-x[2], x[1]))
    return scored


def accept_candidate(school_name, match_name, dist, sim):
    """Strict acceptance: distance + name overlap + school-type consistency."""
    if dist > ACCEPT_MAX_DISTANCE_M:
        return False, "too_far"
    if sim < ACCEPT_MIN_NAME_SIM:
        return False, "name_sim_low"
    if is_non_school_entity(match_name):
        return False, "non_school_marker"
    # Cross-school-type rejection: if both sides have a specific type tag,
    # they must be the same tag.
    q_type = _school_type_of(school_name)
    m_type = _school_type_of(match_name)
    if q_type and m_type and q_type != m_type:
        return False, f"type_mismatch:{q_type}_vs_{m_type}"
    return True, "ok"


# ============================================================================
# Core match flow
# ============================================================================

def match_school(row, force=False):
    """Run the full match flow for one school. Returns a dict with outcome."""
    schulnummer = str(row.get("schulnummer", ""))
    schulname = str(row.get("schulname", "") or "")
    lat = row.get("latitude")
    lng = row.get("longitude")
    strasse = str(row.get("strasse", "") or "")

    if not force:
        cached = _load_cache(schulnummer)
        if cached is not None:
            cached["from_cache"] = True
            return cached

    result = {
        "schulnummer": schulnummer,
        "schulname": schulname,
        "raw_lat": lat, "raw_lng": lng,
        "status": None,
        "match_place_id": None,
        "match_name": None,
        "match_distance_m": None,
        "match_name_sim": None,
        "match_strategy": None,
        "telefon": None,
        "website": None,
        "api_calls": 0,
        "from_cache": False,
    }

    if pd.isna(lat) or pd.isna(lng):
        result["status"] = "no_coords"
        _save_cache(schulnummer, result)
        return result

    reject_reasons = []

    def _try_accept(scored, strategy):
        """Walk candidates top-down; accept the first that passes all rules."""
        for p, dist, sim in scored:
            ok, why = accept_candidate(schulname, _display_name(p), dist, sim)
            if ok:
                result.update(_extract_match(p, strategy, dist, sim))
                return True
            reject_reasons.append(f"{strategy}:{why}({_display_name(p)[:40]},"
                                  f"sim={sim:.2f},dist={dist:.0f}m)")
        return False

    # Strategy A: Nearby Search
    places, err = nearby_search(lat, lng)
    result["api_calls"] += 1
    if err:
        reject_reasons.append(f"nearby_error:{err[:80]}")
    else:
        scored = score_candidates(schulname, lat, lng, places)
        if _try_accept(scored, "nearby"):
            _save_cache(schulnummer, result)
            return result

    # Strategy B: Text Search fallback with name + street
    query = schulname.strip()
    if strasse:
        query = f"{schulname} {strasse}".strip()
    if query:
        places, err = text_search(query, lat, lng)
        result["api_calls"] += 1
        if err:
            reject_reasons.append(f"text_error:{err[:80]}")
        else:
            scored = score_candidates(schulname, lat, lng, places)
            if _try_accept(scored, "text"):
                _save_cache(schulnummer, result)
                return result

    # No acceptable match
    result["status"] = "no_match"
    result["reject_reasons"] = " | ".join(reject_reasons[:5]) if reject_reasons else ""
    _save_cache(schulnummer, result)
    return result


def _extract_match(place, strategy, dist, sim):
    phone = place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber")
    return {
        "status": "matched",
        "match_place_id": place.get("id"),
        "match_name": _display_name(place),
        "match_distance_m": round(dist, 1),
        "match_name_sim": round(sim, 3),
        "match_strategy": strategy,
        "telefon": phone,
        "website": place.get("websiteUri"),
    }


# ============================================================================
# Main
# ============================================================================

def process_school_type(school_type, *, dry_run=False, limit=None, force=False):
    src = RAW_DIR / f"munich_{school_type}_schools_raw.csv"
    if not src.exists():
        logger.error(f"Missing raw file: {src}")
        return None

    df = pd.read_csv(src, low_memory=False)
    if limit:
        df = df.head(limit)

    print(f"\n{'='*72}")
    print(f"MUNICH {school_type.upper()} — Places contact enrichment")
    print(f"{'='*72}")
    print(f"  Rows to process: {len(df)}")
    if dry_run:
        print(f"  DRY RUN — will NOT write output CSV")

    outcomes = []
    matched = no_match = no_coords = errored = 0
    api_calls = 0

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        r = match_school(row, force=force)
        outcomes.append(r)
        api_calls += r.get("api_calls", 0)
        if r["status"] == "matched":
            matched += 1
        elif r["status"] == "no_match":
            no_match += 1
        elif r["status"] == "no_coords":
            no_coords += 1
        else:
            errored += 1

        if i <= 10 or i % 25 == 0:
            tag = "cached" if r.get("from_cache") else "fresh "
            logger.info(f"  [{i:>3}/{len(df)}] {tag} {r['status']:<10} "
                        f"name_sim={r.get('match_name_sim')} "
                        f"dist={r.get('match_distance_m')}m  {r['schulname'][:70]}")

    print(f"\n  Summary:")
    print(f"    matched:   {matched}/{len(df)}")
    print(f"    no_match:  {no_match}/{len(df)}")
    print(f"    no_coords: {no_coords}/{len(df)}")
    print(f"    errored:   {errored}/{len(df)}")
    print(f"    api_calls: {api_calls}")
    got_phone = sum(1 for o in outcomes if o.get("telefon"))
    got_site = sum(1 for o in outcomes if o.get("website"))
    print(f"    phones filled:   {got_phone}/{len(df)}")
    print(f"    websites filled: {got_site}/{len(df)}")

    # Write match report
    report_path = CACHE_DIR / f"match_report_{school_type}.csv"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(outcomes).to_csv(report_path, index=False)
    print(f"    match report: {report_path.relative_to(PROJECT_ROOT)}")

    # Merge results back into the df and write intermediate output
    if not dry_run:
        enriched = df.copy()
        # Force object dtype so we can assign strings even to columns where
        # the raw CSV had the column all-NaN and pandas inferred float64.
        for col in ("telefon", "website"):
            if col not in enriched.columns:
                enriched[col] = pd.Series([pd.NA] * len(enriched), dtype=object)
            elif enriched[col].dtype != object:
                enriched[col] = enriched[col].astype(object)

        outcome_by_snr = {str(o["schulnummer"]): o for o in outcomes}

        def _is_blank(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return True
            return str(v).strip() in ("", "nan", "None", "NaN", "<NA>")

        for idx, r in enriched.iterrows():
            snr = str(r.get("schulnummer", ""))
            o = outcome_by_snr.get(snr)
            if not o or o["status"] != "matched":
                continue
            # Fill gaps only — never overwrite non-empty raw values. Treat
            # NaN/empty/'nan'-string as gaps.
            new_phone = o.get("telefon")
            if _is_blank(r.get("telefon")) and not _is_blank(new_phone):
                enriched.at[idx, "telefon"] = new_phone
            new_site = o.get("website")
            if _is_blank(r.get("website")) and not _is_blank(new_site):
                enriched.at[idx, "website"] = new_site

        out_path = INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_places_contact.csv"
        INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
        enriched.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"    output: {out_path.relative_to(PROJECT_ROOT)}")

    return outcomes


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--school-type", choices=["primary", "secondary", "both"], default="both")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--force", action="store_true",
                   help="Ignore cache, re-query Places for every school")
    return p.parse_args()


def main():
    args = parse_args()
    if not GOOGLE_PLACES_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY not set in environment")
        sys.exit(1)

    types = ["primary", "secondary"] if args.school_type == "both" else [args.school_type]
    for st in types:
        process_school_type(st, dry_run=args.dry_run, limit=args.limit, force=args.force)


if __name__ == "__main__":
    main()
