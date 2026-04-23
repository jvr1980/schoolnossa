#!/usr/bin/env python3
"""
Phase 0 — km.bayern.de Schulsuche probe.

Performs one careful POST to /schulsuche-starten with Regierungsbezirk
Oberbayern + Landeshauptstadt München (MB1=1), captures all returned
school listings, and classifies each by name for public/private.

Outputs:
  data_munich/cache/private_research/km_bayern_probe_raw.html
  data_munich/intermediate/private_research/km_bayern_probe_summary.json

Respects polite scraping: single request per run, 5-second delay after
token harvest, identified User-Agent, cached raw response.
"""
from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUT_CACHE = PROJECT_ROOT / "data_munich" / "cache" / "private_research"
OUT_INTER = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"
OUT_CACHE.mkdir(parents=True, exist_ok=True)
OUT_INTER.mkdir(parents=True, exist_ok=True)

BASE = "https://www.km.bayern.de"
UA = "schoolnossa-research/0.1 (von.roth@gmail.com; polite single-query probe)"

PRIVATE_MARKERS = (
    "privat", "freie trägerschaft", "freier träger", "freier trägerschaft",
    "schule in freier trägerschaft",
    "staatlich anerkannte", "staatlich anerkannter",
    "staatlich genehmigte", "staatlich genehmigter",
    "priv.", "ersatzschule",
    "waldorf", "montessori", "steiner", "phorms",
)
PUBLIC_MARKERS = (
    "städtisch", "städtische", "städtischer", "städtisches",
    "staatliche ", "staatlicher ", "staatliches ",
)


def fetch_raw() -> tuple[str, str]:
    raw_path = OUT_CACHE / "km_bayern_probe_raw.html"
    if raw_path.exists():
        age_days = (datetime.now().timestamp() - raw_path.stat().st_mtime) / 86400
        if age_days < 7:
            print(f"[cache] using {raw_path} (age {age_days:.1f}d)")
            return raw_path.read_text(), "(cached)"

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})

    print("[step 1] GET /schulsuche")
    r1 = sess.get(f"{BASE}/schulsuche", timeout=30)
    r1.raise_for_status()
    tok1_match = re.search(r'id="schoolNumber"[^>]*value="([^"]+)"', r1.text)
    tok2_match = re.search(r'id="schoolNumber2"[^>]*value="([^"]+)"', r1.text)
    if not (tok1_match and tok2_match):
        raise RuntimeError("CSRF tokens not found in form")
    tok1 = tok1_match.group(1)
    tok2 = tok2_match.group(1)
    print(f"  tokens harvested")
    print("  cooling 5s before POST (polite)")
    time.sleep(5)

    print("[step 2] POST /schulsuche-starten  (Oberbayern + LH München)")
    form = {
        "schoolNumber": tok1,
        "schoolNumber2": tok2,
        "rxFormSchoolSearchTerm": "",
        "rxFormEntfernung": "0",
        "rxFormRegierungsbezirk": "1",   # Oberbayern
        "rxFormSchulart1": "0",          # alle
        "rxFormMB1": "1",                # Landeshauptstadt München
    }
    r2 = sess.post(
        f"{BASE}/schulsuche-starten",
        data=form,
        headers={"Referer": f"{BASE}/schulsuche"},
        timeout=45,
    )
    r2.raise_for_status()
    raw_path.write_text(r2.text)
    return r2.text, "(fresh)"


def parse(raw: str) -> list[dict]:
    # Response delimiter splits count/html/tokens
    parts = raw.split("--@--@--@--@--@--@--@--@--")
    html = parts[1] if len(parts) >= 2 else raw
    if "ERROR" in parts[0]:
        raise RuntimeError(f"km.bayern returned ERROR — try fresh session: {raw[:200]!r}")

    entries = []
    # Walk each <li> ... </li> block
    for li in re.findall(r"<li>(.*?)</li>", html, re.DOTALL):
        m_url = re.search(r'href="(/schule/\d+)"', li)
        m_title = re.search(r'<span class="rxTitle">(.*?)</span>', li, re.DOTALL)
        m_desc = re.search(r'<span class="rxDescription">(.*?)</span>', li, re.DOTALL)
        if not (m_url and m_title and m_desc):
            continue
        title = re.sub(r"<i[^>]*></i>", "", m_title.group(1))
        title = re.sub(r"<br/?>", " // ", title)
        title = re.sub(r"<[^>]+>", " ", title).strip()
        desc = re.sub(r"<br/?>", ", ", m_desc.group(1)).strip()
        # Split desc into address + plz+city
        parts_desc = [p.strip() for p in desc.split(",")]
        street = parts_desc[0] if parts_desc else ""
        plz_city = parts_desc[1] if len(parts_desc) > 1 else ""
        m_plz = re.search(r"^(\d{5})\s+(.+)$", plz_city)
        plz = m_plz.group(1) if m_plz else ""
        city = m_plz.group(2).strip() if m_plz else plz_city

        entries.append({
            "school_id": m_url.group(1).rsplit("/", 1)[-1],
            "detail_url": BASE + m_url.group(1),
            "name": title,
            "street": street,
            "plz": plz,
            "city": city,
        })
    return entries


def classify(name: str) -> str:
    ln = name.lower()
    # public markers first (they're more specific when prefixed)
    if any(m in ln for m in PUBLIC_MARKERS):
        # But private-staatlich-anerkannt trumps "staatliche" prefix
        if any(m in ln for m in ("staatlich anerkannt", "staatlich genehmigt",
                                   "freie trägerschaft", "privat", "freier träger")):
            return "private"
        return "public"
    if any(m in ln for m in PRIVATE_MARKERS):
        return "private"
    return "unknown"


def main() -> int:
    try:
        raw, origin = fetch_raw()
    except Exception as e:
        summary = {"status": "fetch_error", "error": str(e)}
        (OUT_INTER / "km_bayern_probe_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False))
        print(f"FETCH FAILED: {e}")
        return 1

    try:
        entries = parse(raw)
    except Exception as e:
        summary = {"status": "parse_error", "error": str(e), "origin": origin}
        (OUT_INTER / "km_bayern_probe_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False))
        print(f"PARSE FAILED: {e}")
        return 1

    by_class = {"public": [], "private": [], "unknown": []}
    for e in entries:
        e["classification"] = classify(e["name"])
        by_class[e["classification"]].append(e)

    summary = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "origin": origin,
        "query": "Regierungsbezirk Oberbayern + MB1=Landeshauptstadt München",
        "total_entries": len(entries),
        "by_classification": {k: len(v) for k, v in by_class.items()},
        "first_15_private": by_class["private"][:15],
        "first_15_public": by_class["public"][:5],
        "first_15_unknown": by_class["unknown"][:5],
    }
    out = OUT_INTER / "km_bayern_probe_summary.json"
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    print("\n=== km.bayern.de Schulsuche probe ===")
    print(f"  total entries parsed:     {summary['total_entries']}")
    print(f"  private classifications:  {summary['by_classification']['private']}")
    print(f"  public classifications:   {summary['by_classification']['public']}")
    print(f"  unknown:                  {summary['by_classification']['unknown']}")
    print(f"\n  Summary: {out.relative_to(PROJECT_ROOT)}")
    print(f"\n  First 10 private hits:")
    for e in by_class["private"][:10]:
        print(f"    - {e['name'][:70]}")
        print(f"      {e['street']}, {e['plz']} {e['city']}  →  {e['detail_url']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
