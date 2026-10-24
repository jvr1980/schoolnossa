#!/usr/bin/env python3
"""
NL: private (particulier / B3) secondary schools.

DUO's open data covers only OCW-funded schools ("Door OCW bekostigde Nederlandse
scholen"), so non-funded private schools have no BRIN and appear nowhere in the
registry we build everything else from. They are a small but high-intent segment
for families searching specifically for private education — the same situation as
the Munich private schools, which were added as synthetic-id inserts.

Source: Inspectie van het Onderwijs, rapporten particuliere scholen VO. Note this
lists schools *inspected since 1-4-2008*, not a complete registry — it is the
best public roster available, but treat it as a floor.

Ids are synthetic and stable: NLPRIV_<slug of name+city>. They must never
collide with a BRIN6, which is always 2 digits + 2 letters + 2 digits.

Output: data_nl/intermediate/nl_private_schools.csv
"""

import logging
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INSPECTIE_B3_URL = ("https://www.onderwijsinspectie.nl/onderwijssectoren/"
                    "particulier-onderwijs/rapporten-particuliere-scholen-vo")
USER_AGENT = "SchoolNossa/1.0 (school data aggregation; contact via schoolnossa.com)"


def _strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def _slug(*parts: str) -> str:
    joined = " ".join(p for p in parts if p)
    ascii_ = unicodedata.normalize("NFKD", joined).encode("ascii", "ignore").decode()
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", ascii_.lower())).strip("_")


def fetch_b3_schools() -> pd.DataFrame:
    cache = CACHE_DIR / "inspectie_b3_vo.html"
    if cache.exists() and cache.stat().st_size > 5000:
        html = cache.read_text(encoding="utf-8", errors="replace")
        logger.info("  Using cached Inspectie B3 page")
    else:
        logger.info("  Fetching Inspectie B3 list...")
        resp = requests.get(INSPECTIE_B3_URL, timeout=120,
                            headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        html = resp.text
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(html, encoding="utf-8")

    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        cells = [_strip_tags(c) for c in
                 re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
        if len(cells) < 2 or cells[0].lower().startswith("naam"):
            continue
        name, city = cells[0], cells[1]
        if not name or not city:
            continue
        status = cells[3] if len(cells) > 3 else ""
        # "positief advies rapport 29-09-25" -> verdict and report date
        date_match = re.search(r"(\d{2}-\d{2}-\d{2,4})", status)
        rows.append({
            "school_id": f"NLPRIV_{_slug(name, city)}",
            "school_name": name,
            "city": city,
            "inspection_type": cells[2] if len(cells) > 2 else None,
            "inspection_status": re.sub(r"\s*rapport.*$", "", status).strip() or None,
            "inspection_report_date": date_match.group(1) if date_match else None,
            "ownership": "private",
            "ownership_national": "particulier (B3, niet bekostigd)",
            "school_type": "secondary",
            "country_code": "NL",
            "metadata_source": "Onderwijsinspectie particuliere scholen VO",
        })

    df = pd.DataFrame(rows).drop_duplicates("school_id")
    logger.info(f"  Parsed {len(df)} private (B3) VO schools")
    return df


def geocode(df: pd.DataFrame) -> pd.DataFrame:
    """Best-effort coordinates. The roster has no street address, so this
    resolves to a city centroid — good enough to place a school on a map at
    city zoom, and explicitly flagged so it is never mistaken for a real fix."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts_international.nl.scrapers.geocode_schools import _pdok_lookup

    lats, lons = [], []
    for city in df["city"]:
        lat = lon = None
        try:
            lat, lon, _, _ = _pdok_lookup(str(city), "type:woonplaats")
        except Exception as e:
            logger.debug(f"  geocode failed for {city}: {e}")
        lats.append(lat)
        lons.append(lon)

    df["latitude"] = lats
    df["longitude"] = lons
    df["geocode_precision"] = "city_centroid"
    logger.info(f"  Geocoded {df['latitude'].notna().sum()}/{len(df)} to city centroid")
    return df


def main():
    logger.info("=" * 60)
    logger.info("NL: private (particulier / B3) secondary schools")
    logger.info("=" * 60)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    df = fetch_b3_schools()
    if df.empty:
        logger.error("No private schools parsed — refusing to write an empty roster")
        sys.exit(1)

    df = geocode(df)

    output = INTERMEDIATE_DIR / "nl_private_schools.csv"
    df.to_csv(output, index=False)
    logger.info(f"Saved: {output}")
    logger.info("\nNext: these rows carry name/city/inspection status only. Street "
                "address, website and tuition need the description/research pass "
                "before they are worth publishing.")


if __name__ == "__main__":
    main()
