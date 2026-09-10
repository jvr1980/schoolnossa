#!/usr/bin/env python3
"""
NL Phase 3c: verified school-profile labels (bilingual / technasium / etc.)
and the Amsterdam contact backfill.

Two sources, both free:

**Nuffic TTO list** (national, ~130 schools) — the authoritative register of
tweetalig onderwijs. Page structure is `<h3>City</h3>` followed by
`<p><a href="school site">Name</a> - tvwo, thavo</p>`. No BRIN, so we match on
the school's own website domain against DUO's INTERNETADRES, then fall back to
name+city. Domain matching is the reliable half: Dutch school names collide
heavily ("Christelijk College ...") while domains are unique.

**Amsterdam Schoolwijzer API** (71 VO schools) — JSON:API, no auth, keyed on
`brin6`, which joins straight to our school_id. Carries the one thing DUO does
not publish anywhere: a school email address. Also lat/lon and municipality-
verified profile flags (tweetalig, technasium, gymnasium, cultuur/dans/kunst/
sport, kopklas).

These fill *verified* columns rather than overwriting the LLM-generated
`sprachen` / `besonderheiten` text, which is already ~88%/~100% populated and
would be degraded by a blind overwrite.

Input:  data_nl/intermediate/nl_schools_with_quality.csv
Output: data_nl/intermediate/nl_schools_with_profiles.csv
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

NUFFIC_URL = ("https://www.nuffic.nl/onderwijssectoren/voortgezet-onderwijs/"
              "tweetalig-onderwijs/alle-tto-scholen-in-nederland")
SCHOOLWIJZER_URL = "https://schoolwijzer.amsterdam.nl/api/v2/vestigingen"
USER_AGENT = "SchoolNossa/1.0 (school data aggregation; contact via schoolnossa.com)"

# Amsterdam flag -> the phrase we store in the verified profile list
SCHOOLWIJZER_FLAGS = {
    "heeft_tweetalig_onderwijs": "tweetalig onderwijs",
    "heeft_technasium": "technasium",
    "heeft_gymnasium": "gymnasium",
    "heeft_profiel_cultuur": "cultuurprofiel",
    "heeft_profiel_dans": "dansprofiel",
    "heeft_profiel_kunst": "kunstprofiel",
    "heeft_profiel_sport": "sportprofiel",
    "heeft_kopklas": "kopklas",
}


# TTO track -> the DUO onderwijsstructuur level it requires. A tvwo programme
# can only exist where the vestiging actually teaches VWO.
_TRACK_REQUIRES = {
    "tvwo": ("VWO",),
    "tgymnasium": ("VWO",),
    "thavo": ("HAVO",),
    "tvmbo": ("VBO", "MAVO", "VMBO"),
    "tmavo": ("MAVO", "VMBO"),
}


def _tracks_compatible(tracks: str, education_type: str) -> bool:
    """True if the vestiging teaches at least one level the TTO entry names."""
    levels = str(education_type or "").upper()
    if not levels:
        return False
    for track in str(tracks or "").split(","):
        for needed in _TRACK_REQUIRES.get(track.strip().lower(), ()):
            if needed in levels:
                return True
    return False


def _domain(url: str) -> str:
    """Normalised registrable-ish domain for matching (drops www, path, port)."""
    if not isinstance(url, str) or not url.strip():
        return ""
    raw = url.strip()
    if "://" not in raw:
        raw = "http://" + raw
    host = (urlparse(raw).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def fetch_tto() -> pd.DataFrame:
    """Parse the Nuffic TTO register into name/city/website/tracks."""
    cache = CACHE_DIR / "nuffic_tto.html"
    if cache.exists() and cache.stat().st_size > 10000:
        html = cache.read_text(encoding="utf-8", errors="replace")
        logger.info("  Using cached Nuffic page")
    else:
        logger.info("  Fetching Nuffic TTO register...")
        resp = requests.get(NUFFIC_URL, timeout=120, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        html = resp.text
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(html, encoding="utf-8")

    # Restrict to the body text block so navigation links are not mistaken for
    # schools, then walk city headings and the entries beneath each.
    body = html
    start = body.find("field--name-field-formatted-text")
    if start > 0:
        body = body[start:]

    rows = []
    city = None
    token = re.compile(
        r'<h3[^>]*>(?P<city>[^<]{2,60})</h3>'
        r'|<p>\s*<a[^>]+href="(?P<url>[^"]+)"[^>]*>(?P<name>.*?)</a>(?P<tail>[^<]{0,80})')
    for m in token.finditer(body):
        if m.group("city"):
            city = m.group("city").strip()
            continue
        name = re.sub(r"<[^>]+>", "", m.group("name") or "").strip()
        url = m.group("url") or ""
        tail = m.group("tail") or ""
        if not name or not url.startswith("http"):
            continue
        tracks = re.findall(r"\bt(?:vwo|havo|vmbo|mavo|gymnasium)\b", tail, re.I)
        if not tracks:
            continue  # not a TTO entry — skip stray links
        rows.append({
            "tto_name": name,
            "tto_city": city,
            "tto_url": url,
            "tto_domain": _domain(url),
            "nl_tto_tracks": ", ".join(sorted({t.lower() for t in tracks})),
        })

    df = pd.DataFrame(rows).drop_duplicates("tto_domain")
    logger.info(f"  Parsed {len(df)} TTO schools "
                f"({df['tto_city'].nunique()} cities)")
    return df


def fetch_schoolwijzer() -> pd.DataFrame:
    """All Amsterdam VO locations from the Schoolwijzer JSON:API."""
    cache = CACHE_DIR / "amsterdam_schoolwijzer_vo.json"
    if cache.exists() and cache.stat().st_size > 1000:
        records = json.loads(cache.read_text())
        logger.info("  Using cached Schoolwijzer response")
    else:
        logger.info("  Fetching Amsterdam Schoolwijzer...")
        records, page = [], 1
        while True:
            resp = requests.get(
                SCHOOLWIJZER_URL,
                params={"page[size]": 75, "page[number]": page},
                timeout=120, headers={"User-Agent": USER_AGENT},
            )
            resp.raise_for_status()
            payload = resp.json()
            chunk = payload.get("data", [])
            if not chunk:
                break
            records.extend(chunk)
            if len(chunk) < 75:
                break
            page += 1
            time.sleep(0.3)
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(records))

    rows = []
    for item in records:
        attrs = item.get("attributes", item)
        # The documented soort_onderwijs filter is ignored server-side, so the
        # response mixes primary and secondary — filter here.
        if attrs.get("soort_onderwijs") != "voortgezet-onderwijs":
            continue
        flags = [phrase for key, phrase in SCHOOLWIJZER_FLAGS.items() if attrs.get(key)]
        rows.append({
            "_brin6": str(attrs.get("brin6", "")).strip().upper(),
            "ams_email": attrs.get("email"),
            "ams_phone": attrs.get("telefoon"),
            "ams_website": attrs.get("website"),
            "ams_lat": attrs.get("latitude"),
            "ams_lon": attrs.get("longitude"),
            "ams_profiles": ", ".join(flags) if flags else None,
            "ams_tto": bool(attrs.get("heeft_tweetalig_onderwijs")),
        })

    df = pd.DataFrame(rows)
    df = df[df["_brin6"].str.len() == 6]
    logger.info(f"  {len(df)} Amsterdam VO locations "
                f"({df['ams_email'].notna().sum()} with email)")
    return df


def enrich(schools: pd.DataFrame) -> pd.DataFrame:
    key = "vestiging_code" if "vestiging_code" in schools.columns else "school_id"
    schools["_brin6"] = schools[key].astype(str).str.strip().str.upper()
    schools["_domain"] = schools["website"].map(_domain)

    logger.info("Nuffic TTO register...")
    tto = fetch_tto()
    if not tto.empty:
        city_col = "gemeente_name" if "gemeente_name" in schools.columns else "city"
        norm = lambda s: re.sub(r"[^a-z0-9]", "", str(s).lower())

        by_domain: dict[str, list] = {}
        for r in tto.itertuples():
            by_domain.setdefault(r.tto_domain, []).append(r)

        hit = pd.Series(index=schools.index, dtype="object")
        matched_domain = 0
        for idx, dom_ in schools["_domain"].items():
            entries = by_domain.get(dom_) if dom_ else None
            if not entries:
                continue
            # A scholengemeenschap runs many vestigingen off one domain, and TTO
            # is offered at specific locations and levels. Require the vestiging
            # to actually teach a level the TTO entry names, and prefer the
            # entry whose city matches, or the flag lands on VBO-only and
            # special-needs sites that cannot offer tvwo/thavo.
            school_city = norm(schools.at[idx, city_col])
            same_city = [e for e in entries if norm(e.tto_city) == school_city]
            chosen = (same_city or entries)[0]
            if len(entries) > 1 and not same_city:
                continue  # ambiguous group, no city evidence — leave unflagged
            if not _tracks_compatible(chosen.nl_tto_tracks,
                                      schools.at[idx, "education_type"]):
                continue
            hit.at[idx] = chosen.nl_tto_tracks
            matched_domain += 1

        # Fallback: name+city for schools whose DUO website is missing or moved.
        by_name = {(norm(r.tto_name), norm(r.tto_city)): r.nl_tto_tracks
                   for r in tto.itertuples()}
        for idx in schools.index[hit.isna()]:
            probe = (norm(schools.at[idx, "school_name"]), norm(schools.at[idx, city_col]))
            tracks = by_name.get(probe)
            if tracks and _tracks_compatible(tracks, schools.at[idx, "education_type"]):
                hit.at[idx] = tracks

        schools["nl_tto_tracks"] = hit
        schools["nl_bilingual_tto"] = hit.notna()
        total = int(hit.notna().sum())
        logger.info(f"  Matched {total} vestigingen against {len(tto)} TTO entries "
                    f"({matched_domain} by domain, {total - matched_domain} by name+city)")

    logger.info("Amsterdam Schoolwijzer...")
    ams = fetch_schoolwijzer()
    if not ams.empty:
        schools = schools.merge(ams, on="_brin6", how="left")
        # DUO publishes no email at all — this is a pure gain, never an overwrite.
        before_email = schools["email"].notna().sum() if "email" in schools.columns else 0
        if "email" not in schools.columns:
            schools["email"] = None
        schools["email"] = schools["email"].where(
            schools["email"].notna() & schools["email"].astype(str).str.strip().ne(""),
            schools["ams_email"])
        after_email = schools["email"].notna().sum()
        logger.info(f"  email: {before_email} -> {after_email} "
                    f"(+{after_email - before_email} from Amsterdam)")

        for col, src in (("phone", "ams_phone"), ("website", "ams_website")):
            if col in schools.columns:
                schools[col] = schools[col].where(
                    schools[col].notna() & schools[col].astype(str).str.strip().ne(""),
                    schools[src])

        schools["nl_verified_profiles"] = schools["ams_profiles"]
        # Amsterdam's own flag corroborates (or adds to) the Nuffic match.
        if "nl_bilingual_tto" in schools.columns:
            schools["nl_bilingual_tto"] = (
                schools["nl_bilingual_tto"].fillna(False).astype(bool)
                | schools["ams_tto"].fillna(False).astype(bool))
        got = schools["nl_verified_profiles"].notna().sum()
        logger.info(f"  Verified profile labels: {got} schools")

    if "nl_bilingual_tto" in schools.columns:
        logger.info(f"Bilingual (TTO) total: {int(schools['nl_bilingual_tto'].sum())}")

    return schools.drop(
        columns=["_brin6", "_domain", "ams_email", "ams_phone", "ams_website",
                 "ams_lat", "ams_lon", "ams_profiles", "ams_tto"],
        errors="ignore")


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 3c: verified profiles + Amsterdam contacts")
    logger.info("=" * 60)

    for candidate in ("nl_schools_with_quality.csv", "nl_schools_with_traffic.csv",
                      "nl_school_master_geocoded.csv"):
        input_path = INTERMEDIATE_DIR / candidate
        if input_path.exists():
            break
    else:
        logger.error("No input intermediate found")
        sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    enriched = enrich(schools)
    output = INTERMEDIATE_DIR / "nl_schools_with_profiles.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
