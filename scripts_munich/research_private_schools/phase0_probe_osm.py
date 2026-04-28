#!/usr/bin/env python3
"""
Phase 0 — OSM / Overpass probe.

Goal:
  Confirm that Overpass returns private-schools rows for Stadt München AND
  Landkreis München, and get a sense of completeness (how many nodes/ways
  tagged amenity=school, how many have operator-type hints, how many match
  PRIVATE_NAME_KEYWORDS).

Reuses:
  PRIVATE_NAME_KEYWORDS, PRIVATE_OPERATOR_KEYWORDS from the existing
  munich_school_master_scraper.py (no re-definition).

Output:
  data_munich/cache/private_research/osm_probe_raw.json       (all schools)
  data_munich/intermediate/private_research/osm_probe_summary.json  (counts)

Runs one Overpass query covering both the city and the surrounding Landkreis.
No writes to any live pipeline files.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts_munich" / "scrapers"))

# Pull keyword lists from the existing scraper (never redefine)
from munich_school_master_scraper import (  # type: ignore
    PRIVATE_NAME_KEYWORDS,
    PRIVATE_OPERATOR_KEYWORDS,
    PUBLIC_OPERATOR_KEYWORDS,
    OVERPASS_URL_SCRAPER,
)

OUT_DIR_CACHE = PROJECT_ROOT / "data_munich" / "cache" / "private_research"
OUT_DIR_INTER = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"
OUT_DIR_CACHE.mkdir(parents=True, exist_ok=True)
OUT_DIR_INTER.mkdir(parents=True, exist_ok=True)

# Overpass query — bbox covering Stadt München + Landkreis München.
# Bbox is (south, west, north, east). Stadt Munich: ~48.06-48.25, 11.36-11.72
# Landkreis extends ~10km in each direction — conservative bbox below.
MUNICH_BBOX = "48.00,11.25,48.35,11.85"
OVERPASS_QUERY = f"""
[out:json][timeout:60];
(
  node["amenity"="school"]({MUNICH_BBOX});
  way["amenity"="school"]({MUNICH_BBOX});
  relation["amenity"="school"]({MUNICH_BBOX});
);
out center tags;
"""

# Mirrors to try in order
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]


def fetch_overpass() -> list[dict]:
    cache = OUT_DIR_CACHE / "osm_probe_raw.json"
    if cache.exists():
        age_days = (datetime.now().timestamp() - cache.stat().st_mtime) / 86400
        if age_days < 30:
            print(f"[cache] reading {cache} (age {age_days:.1f}d)")
            return json.loads(cache.read_text()).get("elements", [])

    last_err = None
    for url in OVERPASS_MIRRORS:
        print(f"[overpass] trying {url}")
        try:
            resp = requests.post(
                url,
                data={"data": OVERPASS_QUERY},
                timeout=120,
                headers={"User-Agent": "schoolnossa-research/0.1 (von.roth@gmail.com)"},
            )
            if resp.status_code in (429, 504):
                print(f"  {resp.status_code} — moving on")
                last_err = f"{resp.status_code}"
                continue
            resp.raise_for_status()
            payload = resp.json()
            cache.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
            return payload.get("elements", [])
        except requests.exceptions.RequestException as e:
            print(f"  error: {e}")
            last_err = str(e)
            continue
    raise RuntimeError(f"All Overpass mirrors failed: {last_err}")


def looks_private(name: str, operator: str) -> tuple[bool, list[str]]:
    """Returns (is_private, reasons)."""
    reasons = []
    n = (name or "").lower()
    o = (operator or "").lower()
    for kw in PRIVATE_NAME_KEYWORDS:
        if kw in n:
            reasons.append(f"name:{kw}")
    for kw in PRIVATE_OPERATOR_KEYWORDS:
        if kw in o:
            reasons.append(f"op:{kw}")
    # Negate if clearly public
    if any(k in o for k in PUBLIC_OPERATOR_KEYWORDS):
        reasons.append("NEG-public-op")
    if reasons and not any(r == "NEG-public-op" for r in reasons):
        return True, reasons
    return False, reasons


def main() -> int:
    elements = fetch_overpass()
    total = len(elements)
    with_name = 0
    with_operator = 0
    with_operator_type = 0
    with_addr = 0
    private_hits = 0
    private_rows = []

    for elem in elements:
        tags = elem.get("tags", {})
        name = tags.get("name", "")
        operator = tags.get("operator", "")
        op_type = tags.get("operator:type", "")
        addr = tags.get("addr:street")

        if name:
            with_name += 1
        if operator:
            with_operator += 1
        if op_type:
            with_operator_type += 1
        if addr:
            with_addr += 1

        is_priv, reasons = looks_private(name, operator)
        if is_priv or op_type in ("private", "religious"):
            private_hits += 1
            lat = elem.get("lat") or elem.get("center", {}).get("lat")
            lon = elem.get("lon") or elem.get("center", {}).get("lon")
            private_rows.append({
                "osm_id": f"{elem['type']}/{elem['id']}",
                "name": name,
                "operator": operator,
                "operator:type": op_type,
                "isced_level": tags.get("isced:level", ""),
                "school_type_tag": tags.get("school:type", ""),
                "addr": f"{tags.get('addr:street','')} {tags.get('addr:housenumber','')}".strip(),
                "plz": tags.get("addr:postcode", ""),
                "ort": tags.get("addr:city", ""),
                "website": tags.get("website", tags.get("contact:website", "")),
                "phone": tags.get("phone", tags.get("contact:phone", "")),
                "lat": lat,
                "lon": lon,
                "match_reasons": reasons,
            })

    summary = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "bbox_query": "Stadt München + Landkreis München",
        "total_elements": total,
        "with_name": with_name,
        "with_operator_tag": with_operator,
        "with_operator_type": with_operator_type,
        "with_address": with_addr,
        "flagged_private": private_hits,
        "private_sample_first_15": private_rows[:15],
    }
    out_path = OUT_DIR_INTER / "osm_probe_summary.json"
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\n[summary] written to {out_path.relative_to(PROJECT_ROOT)}")
    print(json.dumps({k: v for k, v in summary.items() if k != "private_sample_first_15"}, indent=2))
    print(f"\nFirst 10 flagged privates:")
    for r in private_rows[:10]:
        print(f"  - {r['name'][:60]}  ({r['addr']}, {r['plz']} {r['ort']})  "
              f"reasons={r['match_reasons']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
