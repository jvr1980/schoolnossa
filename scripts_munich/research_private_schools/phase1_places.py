#!/usr/bin/env python3
"""
Phase 1 — Google Places pilot.

Runs a broader set of category queries via Places `searchText`, covering
both Stadt and Landkreis München (15 km bias from city center). Each
result is filtered to school-ish types and to primary/secondary level
(best-effort via Places' coarse type labels + schulname keywords).

Output:
  data_munich/intermediate/private_research/google_places.csv
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

OUT = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research" / "google_places.csv"
RAW_CACHE = PROJECT_ROOT / "data_munich" / "cache" / "private_research" / "places_phase1_raw.json"
OUT.parent.mkdir(parents=True, exist_ok=True)
RAW_CACHE.parent.mkdir(parents=True, exist_ok=True)

API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")
ENDPOINT = "https://places.googleapis.com/v1/places:searchText"
FIELDS = (
    "places.id,places.displayName,places.formattedAddress,places.location,"
    "places.types,places.websiteUri,places.nationalPhoneNumber,places.shortFormattedAddress"
)
CENTER = {"latitude": 48.1374, "longitude": 11.5755}
RADIUS_M = 22000.0  # ~22 km covers Stadt + entire Landkreis

QUERIES = [
    "Privatschule München",
    "Private Grundschule München",
    "Private Realschule München",
    "Privates Gymnasium München",
    "Waldorfschule München",
    "Waldorfschule Landkreis München",
    "Montessori Schule München",
    "Montessori Grundschule München",
    "International School Munich",
    "Bilinguale Schule München",
    "Katholische Schule München",
    "Katholisches Gymnasium München",
    "Evangelische Schule München",
    "Jüdische Schule München",
    "Freie Schule München",
    "Phorms München",
    "Sabel Schule München",
    "Lukas Schule München",
    "Obermenzinger Gymnasium",
    "Maria-Ward München",
    "Nymphenburger Schulen München",
]

PRIVATE_MARKERS = (
    "privat", "freie", "freier", "phorms", "waldorf", "montessori", "steiner",
    "international", "bilingual", "katholisch", "evangelisch", "jüdisch",
    "obermenzinger", "nymphenburger", "sabel", "isar-gymnasium", "isar-realschule",
    "maria-ward", "maria ward", "edith-stein", "lukas-", "sinai", "jules verne",
    "bavarian international", "munich international", "parzival", "christophorus",
    "samuel-heinicke", "ergänzungsschule", "ersatzschule", "heckscher",
)
PUBLIC_MARKERS = ("städtisch", "städtische", "staatliche ", "staatliches ", "staatlicher ")


def search_text(query: str) -> list[dict]:
    body = {
        "textQuery": query,
        "locationBias": {"circle": {"center": CENTER, "radius": RADIUS_M}},
        "maxResultCount": 20,
        "languageCode": "de",
        "includedType": "school",
    }
    r = requests.post(
        ENDPOINT,
        headers={"Content-Type": "application/json", "X-Goog-Api-Key": API_KEY,
                 "X-Goog-FieldMask": FIELDS},
        json=body, timeout=30,
    )
    r.raise_for_status()
    return r.json().get("places", [])


def classify_private_by_name(name: str) -> str:
    ln = (name or "").lower()
    if any(m in ln for m in PRIVATE_MARKERS):
        return "private"
    if any(m in ln for m in PUBLIC_MARKERS):
        return "public"
    return "unknown"


def classify_level(name: str, types: list[str]) -> str:
    ln = (name or "").lower()
    has_primary = ("primary_school" in types) or ("grundschul" in ln)
    has_secondary = ("secondary_school" in types) or any(
        k in ln for k in (
            "gymnasium", "realschule", "mittelschul", "gesamtschul",
            "sekundar", "wirtschaftsschul",
        )
    )
    if has_primary and has_secondary:
        return "both"
    if has_primary:
        return "primary"
    if has_secondary:
        return "secondary"
    # too vague — fallback label
    return "unknown"


def main() -> int:
    if not API_KEY:
        print("GOOGLE_PLACES_API_KEY not set")
        return 1

    raw_by_query: dict[str, list[dict]] = {}
    for q in QUERIES:
        print(f"[query] {q}")
        try:
            raw_by_query[q] = search_text(q)
        except Exception as e:
            print(f"  error: {e}")
            raw_by_query[q] = []
        time.sleep(0.6)

    RAW_CACHE.write_text(json.dumps(raw_by_query, indent=2, ensure_ascii=False))

    # Dedup by place ID across queries
    seen: dict[str, dict] = {}
    for q, items in raw_by_query.items():
        for p in items:
            pid = p.get("id")
            if not pid:
                continue
            if pid not in seen:
                name = (p.get("displayName") or {}).get("text") if isinstance(p.get("displayName"), dict) else p.get("displayName")
                addr = p.get("formattedAddress") or ""
                seen[pid] = {
                    "source_key": "google_places",
                    "source_ref": pid,
                    "schulname": name or "",
                    "address_line1": addr.split(",")[0].strip() if addr else "",
                    "plz": "",
                    "ort": "",
                    "latitude": (p.get("location") or {}).get("latitude"),
                    "longitude": (p.get("location") or {}).get("longitude"),
                    "schulart": classify_level(name or "", p.get("types", [])),
                    "schulart_detail": ",".join(p.get("types", [])),
                    "traegerschaft_hint": "",
                    "website": p.get("websiteUri") or "",
                    "phone": p.get("nationalPhoneNumber") or "",
                    "source_url": f"https://www.google.com/maps/place/?q=place_id:{pid}",
                    "full_address": addr,
                    "matched_queries": [q],
                }
            else:
                seen[pid]["matched_queries"].append(q)

    # Extract PLZ + city from full_address
    PLZ_RE = re.compile(r"\b(\d{5})\s+([^,]+)")
    final_rows = []
    scraped_at = datetime.utcnow().isoformat() + "Z"
    kept = skipped_public = skipped_level = skipped_not_private = 0
    for row in seen.values():
        m = PLZ_RE.search(row["full_address"])
        if m:
            row["plz"] = m.group(1)
            row["ort"] = m.group(2).strip()

        classification = classify_private_by_name(row["schulname"])
        if classification == "public":
            skipped_public += 1
            continue
        # Unknown needs at least one private-marker query hit
        if classification == "unknown":
            matched_q_lower = " ".join(row["matched_queries"]).lower()
            if any(
                k in matched_q_lower for k in
                ("privat", "waldorf", "montessori", "international",
                 "bilingual", "katholisch", "evangelisch", "jüdisch",
                 "freie", "phorms", "sabel", "lukas", "obermenzinger",
                 "maria-ward", "nymphenburger")
            ):
                classification = "private"  # promoted — query context was private
            else:
                skipped_not_private += 1
                continue

        if row["schulart"] == "unknown":
            skipped_level += 1
            continue
        if row["schulart"] == "other":
            skipped_level += 1
            continue

        row["traegerschaft_hint"] = "privat"  # default — categorization refined in combine step
        row["match_reasons"] = f"places_query:{row['matched_queries'][0]};queries_total={len(row['matched_queries'])}"
        row["scraped_at"] = scraped_at
        row["matched_queries"] = "|".join(row["matched_queries"])
        final_rows.append(row)
        kept += 1

    fieldnames = [
        "source_key", "source_ref", "schulname", "address_line1", "plz", "ort",
        "latitude", "longitude", "schulart", "schulart_detail",
        "traegerschaft_hint", "website", "phone", "source_url",
        "full_address", "matched_queries", "match_reasons", "scraped_at",
    ]
    with OUT.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in final_rows:
            w.writerow(r)

    print(f"\n=== Phase 1 Google Places ===")
    print(f"  queries run:                {len(QUERIES)}")
    print(f"  unique place IDs returned:  {len(seen)}")
    print(f"  kept as private primary/secondary: {kept}")
    print(f"  skipped as public:                 {skipped_public}")
    print(f"  skipped unknown level:             {skipped_level}")
    print(f"  skipped not clearly private:       {skipped_not_private}")
    print(f"  → {OUT.relative_to(PROJECT_ROOT)}")
    print(f"\n  First 10 kept:")
    for r in final_rows[:10]:
        print(f"    - {r['schulname'][:55]:<55}  level={r['schulart']:<8}  {r['plz']} {r['ort']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
