#!/usr/bin/env python3
"""
Phase 1 — Gemini grounded pilot.

Reads the cached Phase 0 Gemini response (46 schools across 7
categories) and emits the canonical CSV. Also runs an optional
second Gemini pass constrained to the Landkreis München municipalities
that Phase 0 likely underweighted (Grünwald, Unterhaching, Haar,
Garching, Ottobrunn, Oberhaching, Kirchheim, Neubiberg, etc.).

Output:
  data_munich/intermediate/private_research/gemini_grounded.csv
  data_munich/cache/private_research/gemini_probe_raw.json  (prior)
  data_munich/cache/private_research/gemini_landkreis_raw.json  (new)
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

CACHE_DIR = PROJECT_ROOT / "data_munich" / "cache" / "private_research"
INTER_DIR = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"
INTER_DIR.mkdir(parents=True, exist_ok=True)

LANDKREIS_PROMPT = """Liste alle **privaten Grundschulen und
weiterführenden Schulen** im **Landkreis München (rings um die Stadt
München, ohne Stadtgebiet)** auf. Das umfasst u.a.:
Garching, Unterhaching, Ottobrunn, Oberhaching, Haar, Grünwald,
Kirchheim, Neubiberg, Taufkirchen, Ismaning, Unterschleißheim,
Pullach, Planegg, Gräfelfing, Aschheim, Aying, Brunnthal,
Höhenkirchen-Siegertsbrunn, Hohenbrunn, Putzbrunn, Sauerlach, Schäftlarn,
Straßlach-Dingharting.

Recherchiere per Google-Suche. Keine Berufsschulen, keine reinen
Förderschulen. Kategorien (wenn vorhanden):
Waldorf / Montessori / International / katholisch / evangelisch /
jüdisch / andere Privat.

ANTWORT-FORMAT (strikt JSON, kein Markdown):
{
  "schools": [
    {"name": "...", "category": "...", "levels": ["..."],
     "strasse": "...", "plz": "...", "ort": "...",
     "website": "...", "confidence": 0.0..1.0}
  ]
}
"""


def call_gemini(prompt: str) -> dict:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0,
        ),
    )
    text = getattr(resp, "text", "") or ""
    clean = text.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return {"raw": text, "parsed": json.loads(clean) if clean else {}}


def category_to_traegerschaft(cat: str) -> str:
    c = (cat or "").lower()
    if c == "katholisch" or c == "evangelisch" or c == "jüdisch":
        return "kirchlich"
    return "privat"  # Waldorf / Montessori / International / andere Privat


def determine_level(levels_list: list) -> str:
    if not levels_list:
        return "unknown"
    lv = [l.lower() for l in levels_list]
    has_primary = any("grundschul" in x or "primary" in x for x in lv)
    has_secondary = any(
        k in x for x in lv for k in (
            "gymnas", "realschul", "mittelschul", "hauptschul", "gesamt",
            "sekundar", "wirtschaftsschul",
        )
    )
    if has_primary and has_secondary:
        return "both"
    if has_primary:
        return "primary"
    if has_secondary:
        return "secondary"
    return "other"


def main() -> int:
    all_schools: list[dict] = []

    # Stadt München (reuse Phase 0 cache)
    stadt_raw = CACHE_DIR / "gemini_probe_raw.json"
    if stadt_raw.exists():
        payload = json.loads(stadt_raw.read_text())
        text = payload.get("response", "")
        clean = text.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        stadt_parsed = json.loads(clean) if clean else {}
        stadt_schools = stadt_parsed.get("schools", [])
        for s in stadt_schools:
            s["_source_sub"] = "stadt+landkreis_v1"
        all_schools.extend(stadt_schools)
        print(f"[stadt cache] {len(stadt_schools)} schools")

    # Landkreis-focused pass (new)
    landkreis_cache = CACHE_DIR / "gemini_landkreis_raw.json"
    if not landkreis_cache.exists():
        print("[gemini] Landkreis-focused pass...")
        out = call_gemini(LANDKREIS_PROMPT)
        landkreis_cache.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    lk = json.loads(landkreis_cache.read_text())
    lk_schools = (lk.get("parsed") or {}).get("schools", [])
    for s in lk_schools:
        s["_source_sub"] = "landkreis_focused"
    all_schools.extend(lk_schools)
    print(f"[landkreis cache] {len(lk_schools)} schools")

    # Canonical CSV emit
    scraped_at = datetime.utcnow().isoformat() + "Z"
    rows = []
    kept = skipped_outside = skipped_level = 0
    OUTSIDE_ORTE = {
        "starnberg", "haimhausen", "buchhof", "ebenhausen",
        "dachau", "fürstenfeldbruck", "freising", "erding", "ebersberg",
    }
    for s in all_schools:
        name = (s.get("name") or "").strip()
        if not name:
            continue
        cat = s.get("category", "")
        ort = (s.get("ort") or "").strip()
        if ort.lower() in OUTSIDE_ORTE:
            skipped_outside += 1
            continue
        level = determine_level(s.get("levels", []))
        if level not in ("primary", "secondary", "both"):
            skipped_level += 1
            continue
        rows.append({
            "source_key": "gemini_grounded",
            "source_ref": f"gemini:{hash(name) & 0xffffffff:x}",
            "schulname": name,
            "address_line1": s.get("strasse", ""),
            "plz": s.get("plz", ""),
            "ort": ort,
            "latitude": "",
            "longitude": "",
            "schulart": level,
            "schulart_detail": ", ".join(s.get("levels", [])),
            "traegerschaft_hint": category_to_traegerschaft(cat),
            "gemini_category": cat,
            "confidence": s.get("confidence", ""),
            "website": s.get("website", ""),
            "source_url": s.get("website", ""),
            "match_reasons": f"gemini_cat:{cat};sub:{s.get('_source_sub','')}",
            "scraped_at": scraped_at,
        })
        kept += 1

    out = INTER_DIR / "gemini_grounded.csv"
    fieldnames = [
        "source_key", "source_ref", "schulname", "address_line1", "plz", "ort",
        "latitude", "longitude", "schulart", "schulart_detail",
        "traegerschaft_hint", "gemini_category", "confidence", "website",
        "source_url", "match_reasons", "scraped_at",
    ]
    with out.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"\n=== Phase 1 Gemini ===")
    print(f"  total raw schools:             {len(all_schools)}")
    print(f"  kept primary/secondary:        {kept}")
    print(f"  rejected (outside scope ort):  {skipped_outside}")
    print(f"  rejected (level=other/unknown): {skipped_level}")
    print(f"  → {out.relative_to(PROJECT_ROOT)}")
    # Quick category summary
    cats: dict[str, int] = {}
    for r in rows:
        cats[r["gemini_category"]] = cats.get(r["gemini_category"], 0) + 1
    print(f"  by category: {cats}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
