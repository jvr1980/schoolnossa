#!/usr/bin/env python3
"""
Phase 0 — Google Places probe.

Smoke test: run two focused `searchText` queries against the Places API:
  1. "Privatschule München"  — type=school, 5 km bias around city center
  2. "Waldorfschule München" — same

Goal is to confirm Places returns private-school results in Munich, check
what fields come back, and get a rough sense of noise. Full grid-search
happens in Phase 1.

Output:
  data_munich/cache/private_research/places_probe_raw.json
  data_munich/intermediate/private_research/places_probe_summary.json
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

OUT_CACHE = PROJECT_ROOT / "data_munich" / "cache" / "private_research"
OUT_INTER = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"
OUT_CACHE.mkdir(parents=True, exist_ok=True)
OUT_INTER.mkdir(parents=True, exist_ok=True)

API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")
ENDPOINT = "https://places.googleapis.com/v1/places:searchText"
FIELDS = (
    "places.id,places.displayName,places.formattedAddress,places.location,"
    "places.types,places.websiteUri,places.nationalPhoneNumber"
)
# Munich city center roughly
CENTER = {"latitude": 48.1374, "longitude": 11.5755}

QUERIES = [
    "Privatschule München",
    "Waldorfschule München",
    "Montessori Schule München",
    "International School München",
]


def search(query: str) -> list[dict]:
    body = {
        "textQuery": query,
        "locationBias": {"circle": {"center": CENTER, "radius": 15000.0}},  # 15 km covers Stadt + Landkreis
        "maxResultCount": 20,
        "languageCode": "de",
        "includedType": "school",
    }
    resp = requests.post(
        ENDPOINT,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": API_KEY,
            "X-Goog-FieldMask": FIELDS,
        },
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("places", [])


def main() -> int:
    if not API_KEY:
        print("GOOGLE_PLACES_API_KEY not set")
        return 1

    all_results: dict[str, list[dict]] = {}
    raw_by_query: dict[str, list[dict]] = {}
    for q in QUERIES:
        print(f"[query] {q}")
        try:
            places = search(q)
        except Exception as e:
            print(f"  error: {e}")
            places = []
        raw_by_query[q] = places
        all_results[q] = [
            {
                "id": p.get("id"),
                "name": (p.get("displayName") or {}).get("text") if isinstance(p.get("displayName"), dict) else p.get("displayName"),
                "address": p.get("formattedAddress"),
                "lat": (p.get("location") or {}).get("latitude"),
                "lng": (p.get("location") or {}).get("longitude"),
                "types": p.get("types", []),
                "website": p.get("websiteUri"),
                "phone": p.get("nationalPhoneNumber"),
            }
            for p in places
        ]
        print(f"  → {len(places)} results")
        time.sleep(1.0)  # polite pacing

    # Dedup by place_id across queries
    seen = {}
    for q, items in all_results.items():
        for it in items:
            pid = it["id"]
            if pid and pid not in seen:
                seen[pid] = {**it, "first_seen_via": q, "all_matched_queries": [q]}
            elif pid:
                seen[pid]["all_matched_queries"].append(q)

    unique = list(seen.values())

    summary = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "queries_run": QUERIES,
        "results_per_query": {q: len(v) for q, v in all_results.items()},
        "unique_place_ids": len(unique),
        "sample_first_20": unique[:20],
    }
    (OUT_CACHE / "places_probe_raw.json").write_text(json.dumps(raw_by_query, indent=2, ensure_ascii=False))
    (OUT_INTER / "places_probe_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    print("\n=== Places probe summary ===")
    print(f"  queries run:         {len(QUERIES)}")
    print(f"  results per query:   {summary['results_per_query']}")
    print(f"  unique place IDs:    {summary['unique_place_ids']}")
    print(f"\n  First 15 unique hits:")
    for it in unique[:15]:
        types = ",".join(t for t in it["types"] if "school" in t or "primary" in t or "secondary" in t)[:40]
        print(f"    - {(it['name'] or '')[:60]:<60}  types={types}")
        print(f"      {it['address']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
