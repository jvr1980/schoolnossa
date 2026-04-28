#!/usr/bin/env python3
"""
Phase 1 — OSM pilot.

Reads the cached Overpass response and emits a canonical candidate CSV.

Classification is more permissive than Phase 0's `looks_private` helper:
Phase 0 was just probing to count; Phase 1 captures every school tagged
as private, whether by operator:type, operator string, fee tag, or name
keyword match. We filter aggressively to primary+secondary only (no
Berufsschulen, no reine Förderschulen) — but keep dual-level (Grund- +
Mittelschule, Grund- + Realschule etc.) since SchoolNossa already
handles those as "both".

Output:
  data_munich/intermediate/private_research/osm_overpass.csv
"""
from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts_munich" / "scrapers"))
from munich_school_master_scraper import (  # type: ignore
    PRIVATE_NAME_KEYWORDS,
    PRIVATE_OPERATOR_KEYWORDS,
    PUBLIC_OPERATOR_KEYWORDS,
)

RAW = PROJECT_ROOT / "data_munich" / "cache" / "private_research" / "osm_probe_raw.json"
OUT = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research" / "osm_overpass.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

# ISCED levels we accept (primary + secondary per user scope)
#   1 = primary, 2 = lower secondary, 3 = upper secondary
ALLOWED_ISCED = {"0", "1", "2", "3", "01", "02", "03"}

# Tag-based exclusions: vocational / tertiary / pure Förder / adult ed
EXCLUDE_NAME_SUBSTR = [
    "berufsschule", "berufsfachschule", "fachoberschule", "berufsoberschule",
    "hochschule", "universität", "academy ", "akademie",
    "volkshochschule", "meisterschule", "sprachschule",
    "musikschule", "fahrschule", "tanzschule", "schwimmschule",
    "vhs ", "kursort",
]


def is_excluded_by_name(name: str) -> bool:
    ln = name.lower()
    return any(s in ln for s in EXCLUDE_NAME_SUBSTR)


def classify_private(tags: dict) -> tuple[bool, list[str]]:
    """Decide if an OSM element represents a private primary/secondary school."""
    name = (tags.get("name") or "").strip()
    operator = (tags.get("operator") or "").strip()
    op_type = (tags.get("operator:type") or "").strip().lower()
    fee = (tags.get("fee") or "").strip().lower()

    reasons: list[str] = []
    if op_type in ("private", "religious"):
        reasons.append(f"operator:type={op_type}")
    if fee == "yes":
        reasons.append("fee=yes")
    op_l = operator.lower()
    for kw in PRIVATE_OPERATOR_KEYWORDS:
        if kw in op_l:
            reasons.append(f"op:{kw}")
            break
    name_l = name.lower()
    for kw in PRIVATE_NAME_KEYWORDS:
        if kw in name_l:
            reasons.append(f"name:{kw}")
            break
    # Negate: if operator is clearly Freistaat Bayern / Landeshauptstadt, it's not private
    if any(k in op_l for k in PUBLIC_OPERATOR_KEYWORDS):
        return False, ["NEG-public-op"]
    # Must have at least one positive reason
    return (len(reasons) > 0), reasons


def classify_school_level(tags: dict, name: str) -> str:
    """Return 'primary' | 'secondary' | 'both' | 'other'. Filters by isced or name."""
    isced = str(tags.get("isced:level") or "").replace(" ", "")
    school_type = (tags.get("school:type") or tags.get("school") or "").lower()
    name_l = name.lower()

    has_primary = "1" in isced or "grundschule" in name_l or "primary" in name_l
    has_secondary = any(c in isced for c in "23") or any(
        k in name_l for k in (
            "gymnasium", "realschule", "mittelschule", "hauptschule",
            "gesamtschule", "sekundarschule", "secondary", "oberschule",
            "wirtschaftsschule",
        )
    ) or "sec" in school_type
    if has_primary and has_secondary:
        return "both"
    if has_primary:
        return "primary"
    if has_secondary:
        return "secondary"
    return "other"


def main() -> int:
    if not RAW.exists():
        print(f"missing {RAW}; run phase0_probe_osm.py first")
        return 1
    elements = json.loads(RAW.read_text()).get("elements", [])
    rows: list[dict] = []
    skipped_excluded = skipped_other_level = skipped_public = 0
    scraped_at = datetime.utcnow().isoformat() + "Z"
    for e in elements:
        tags = e.get("tags", {})
        name = (tags.get("name") or "").strip()
        if not name:
            continue
        if is_excluded_by_name(name):
            skipped_excluded += 1
            continue
        is_priv, reasons = classify_private(tags)
        if not is_priv:
            skipped_public += 1
            continue
        level = classify_school_level(tags, name)
        if level == "other":
            skipped_other_level += 1
            continue
        lat = e.get("lat") or (e.get("center") or {}).get("lat")
        lon = e.get("lon") or (e.get("center") or {}).get("lon")
        rows.append({
            "source_key": "osm",
            "source_ref": f"{e['type']}/{e['id']}",
            "schulname": name,
            "address_line1": f"{tags.get('addr:street','')} {tags.get('addr:housenumber','')}".strip(),
            "plz": tags.get("addr:postcode", ""),
            "ort": tags.get("addr:city", ""),
            "latitude": lat,
            "longitude": lon,
            "schulart": level,
            "schulart_detail": tags.get("school:type") or tags.get("school") or "",
            "traegerschaft_hint": "privat" if tags.get("operator:type") != "religious" else "kirchlich",
            "operator_raw": tags.get("operator", ""),
            "website": tags.get("website") or tags.get("contact:website") or "",
            "email": tags.get("email") or tags.get("contact:email") or "",
            "phone": tags.get("phone") or tags.get("contact:phone") or "",
            "source_url": f"https://www.openstreetmap.org/{e['type']}/{e['id']}",
            "match_reasons": "|".join(reasons),
            "scraped_at": scraped_at,
        })

    fieldnames = list(rows[0].keys()) if rows else [
        "source_key", "source_ref", "schulname", "address_line1", "plz", "ort",
        "latitude", "longitude", "schulart", "schulart_detail",
        "traegerschaft_hint", "operator_raw", "website", "email", "phone",
        "source_url", "match_reasons", "scraped_at",
    ]
    with OUT.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"\n=== Phase 1 OSM ===")
    print(f"  total OSM elements loaded:      {len(elements)}")
    print(f"  rejected by excluded name list: {skipped_excluded}")
    print(f"  rejected as public/non-private: {skipped_public}")
    print(f"  rejected by level=other:        {skipped_other_level}")
    print(f"  accepted as private primary/secondary: {len(rows)}")
    print(f"  → {OUT.relative_to(PROJECT_ROOT)}")
    if rows:
        print(f"\n  First 10:")
        for r in rows[:10]:
            print(f"    - {r['schulname'][:55]:<55} level={r['schulart']:<8} "
                  f"{r['plz']} {r['ort']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
