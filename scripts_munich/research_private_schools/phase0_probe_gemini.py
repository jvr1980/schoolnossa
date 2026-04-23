#!/usr/bin/env python3
"""
Phase 0 — Gemini + Google Search grounding probe.

One focused prompt asking Gemini 2.5-flash with Google Search grounding
to enumerate private schools in Stadt + Landkreis München across several
categories (Waldorf/Montessori/International/confessional/other privat).

Output:
  data_munich/cache/private_research/gemini_probe_raw.json
  data_munich/intermediate/private_research/gemini_probe_summary.json
"""
from __future__ import annotations

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

OUT_CACHE = PROJECT_ROOT / "data_munich" / "cache" / "private_research"
OUT_INTER = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"
OUT_CACHE.mkdir(parents=True, exist_ok=True)
OUT_INTER.mkdir(parents=True, exist_ok=True)

PROMPT = """Erstelle eine möglichst vollständige Liste aller **privaten
Grundschulen und weiterführenden Schulen** (keine Berufsschulen, keine
reinen Förderschulen) in **Stadt München und Landkreis München**.

Recherchiere per Google-Suche. Verwende offizielle Quellen
(km.bayern.de, muenchen.de, privatschulen-in-bayern.de), wenn möglich.

Kategorien (für jede möglichst vollständig):
- Waldorfschulen
- Montessori-Schulen
- International Schools / bilinguale Schulen
- Katholische / evangelische Schulen (konfessionell)
- Jüdische Schulen
- Sonstige Privatschulen (Sabel, Phorms, Isar, Lukas, Obermenzinger, Nymphenburger, ...)

ANTWORT-FORMAT (strikt JSON, kein Markdown):
{
  "schools": [
    {
      "name": "...",
      "category": "Waldorf"|"Montessori"|"International"|"katholisch"|"evangelisch"|"jüdisch"|"andere Privat",
      "levels": ["Grundschule","Realschule","Gymnasium","Gesamtschule","Mittelschule", ...],
      "strasse": "... Straße 12",
      "plz": "80xxx",
      "ort": "München"|"Grünwald"|...,
      "website": "https://...",
      "confidence": 0.0..1.0,
      "source_hint": "km.bayern.de" | "school website" | "privatschulen-in-bayern.de" | ...
    }
  ],
  "notes": "optionale Hinweise zu Lücken oder Unsicherheiten"
}

Antworte NUR mit dem JSON-Objekt — bitte tatsächlich alle Schulen die
du findest, nicht nur eine Auswahl.
"""


def main() -> int:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("GEMINI_API_KEY not set")
        return 1

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)
    print("[gemini] calling 2.5-flash with Google Search grounding...")
    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=PROMPT,
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0,
        ),
    )
    text = getattr(resp, "text", "") or ""
    (OUT_CACHE / "gemini_probe_raw.json").write_text(
        json.dumps({"prompt_len": len(PROMPT), "response": text}, indent=2, ensure_ascii=False))

    # Strip markdown fences if present
    clean = text.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        data = json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"Parse error: {e}")
        print(f"Raw response:\n{text[:800]}")
        return 1

    schools = data.get("schools", [])
    by_cat = {}
    for s in schools:
        by_cat.setdefault(s.get("category", "?"), []).append(s)

    summary = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "total_schools": len(schools),
        "by_category": {k: len(v) for k, v in by_cat.items()},
        "notes": data.get("notes", ""),
        "sample_5_per_category": {k: v[:5] for k, v in by_cat.items()},
    }
    (OUT_INTER / "gemini_probe_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False))

    print("\n=== Gemini grounded probe ===")
    print(f"  total schools returned: {len(schools)}")
    print(f"  by category:            {summary['by_category']}")
    print(f"  notes:                  {data.get('notes', '')}")
    print(f"\n  First 3 per category:")
    for cat, lst in by_cat.items():
        print(f"    [{cat}] ({len(lst)})")
        for s in lst[:3]:
            print(f"      - {s.get('name','?')[:60]}  "
                  f"({s.get('plz','')} {s.get('ort','')})  "
                  f"conf={s.get('confidence')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
