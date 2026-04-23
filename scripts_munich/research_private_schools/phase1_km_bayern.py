#!/usr/bin/env python3
"""
Phase 1 — km.bayern.de pilot.

Pipeline:
  1. Parse the cached probe HTML into 110 Stadt München listings.
  2. For schools whose names lack an obvious private marker, follow the
     /schule/{id} detail page and parse Rechtlicher Status / Schulart.
  3. Emit canonical CSV for primary+secondary privates only.

Landkreis München coverage: the probe used MB1=Landeshauptstadt München
(Stadt only). Oberbayern-Ost/West cover the Landkreis ring but also
much of eastern/western Bavaria, so we'd need to post-filter by PLZ. For
this pilot we rely on OSM / Gemini / Places for Landkreis; km.bayern is
the authoritative Stadt source.

Output:
  data_munich/intermediate/private_research/km_bayern.csv

Polite scraping: cached detail pages, 1-second sleep per request.
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data_munich" / "cache" / "private_research"
DETAIL_CACHE = CACHE_DIR / "km_bayern_details"
DETAIL_CACHE.mkdir(parents=True, exist_ok=True)
OUT = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research" / "km_bayern.csv"
RAW_LISTING = CACHE_DIR / "km_bayern_probe_raw.html"

BASE = "https://www.km.bayern.de"
UA = "schoolnossa-research/0.1 (von.roth@gmail.com; polite detail-page scrape)"

PRIVATE_MARKERS_NAME = (
    "privat", "priv.",
    "freie trägerschaft", "freier träger", "freier trägerschaft",
    "schule in freier trägerschaft",
    "staatlich anerkannte", "staatlich anerkannter",
    "staatlich genehmigte", "staatlich genehmigter",
    "ersatzschule", "ergänzungsschule",
    "waldorf", "montessori", "steiner", "phorms",
    "edith-stein", "maria-ward", "maria ward",
)
PUBLIC_MARKERS_NAME = (
    "städtisch", "städtische", "städtischer", "städtisches",
)


def parse_listing(html_blob: str) -> list[dict]:
    """Parse the probe_raw html into school dicts."""
    parts = html_blob.split("--@--@--@--@--@--@--@--@--")
    html = parts[1] if len(parts) >= 2 else html_blob
    entries = []
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
        parts_desc = [p.strip() for p in desc.split(",")]
        street = parts_desc[0] if parts_desc else ""
        plz_city = parts_desc[1] if len(parts_desc) > 1 else ""
        m_plz = re.search(r"^(\d{5})\s+(.+)$", plz_city)
        plz = m_plz.group(1) if m_plz else ""
        city = m_plz.group(2).strip() if m_plz else plz_city
        entries.append({
            "school_id": m_url.group(1).rsplit("/", 1)[-1],
            "detail_url": BASE + m_url.group(1),
            "name_listing": title,
            "street": street,
            "plz": plz,
            "city": city,
        })
    return entries


def classify_by_name(name: str) -> str:
    ln = name.lower()
    # "staatlich anerkannt/genehmigt" overrides the generic "staatliche" prefix
    if any(m in ln for m in PRIVATE_MARKERS_NAME):
        return "private"
    if any(m in ln for m in PUBLIC_MARKERS_NAME):
        return "public"
    if ln.startswith("staatliche ") or ln.startswith("staatliches ") or ln.startswith("staatlicher "):
        return "public"
    return "unknown"


def fetch_detail(session: requests.Session, url: str) -> str:
    cache_file = DETAIL_CACHE / (url.rsplit("/", 1)[-1] + ".html")
    if cache_file.exists():
        age = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
        if age < 7:
            return cache_file.read_text()
    r = session.get(url, timeout=30)
    r.raise_for_status()
    cache_file.write_text(r.text)
    time.sleep(1.0)
    return r.text


def extract_detail_fields(html: str) -> dict:
    # Strip html to plain text, then regex key/value extraction
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)

    def grab(label: str, window: int = 200) -> str:
        m = re.search(label + r"\s*[:\-]?\s*(.{1," + str(window) + r"}?)(?:Schulnummer|Schulart|Rechtlicher|Gemeinsame|Träger|Schulaufsicht|Anschrift|Kontakt|Ausbildungsrichtung|$)", text, re.IGNORECASE)
        return (m.group(1).strip() if m else "")

    return {
        "schulart_detail": grab(r"Schulart"),
        "rechtlicher_status": grab(r"Rechtlicher Status"),
        "schulnummer_offiziell": grab(r"Schulnummer", 40),
        "traeger": grab(r"(?:Schul)?[Tt]räger", 200),
        "text_blob": text[:10000],
    }


def derive_traegerschaft(rechtlicher_status: str) -> str:
    rs = rechtlicher_status.lower()
    if "kommunal" in rs:
        return "städtisch"
    if "ersatzschule" in rs or "ergänzungsschule" in rs or "privat" in rs:
        return "privat"
    if "öffentlich" in rs and "staatlich" in rs:
        return "staatlich"
    if "staatlich" in rs:
        return "staatlich"
    return ""


LEVEL_MAP = [
    ("primary",   re.compile(r"\bgrundschul", re.IGNORECASE)),
    ("secondary", re.compile(r"\bmittelschul|\bhauptschul|\brealschul|\bgymnasium|\bgesamtschul|\bsekundar|\bwirtschaftsschul", re.IGNORECASE)),
    ("other",     re.compile(r"\bberufs|\bfachoberschul|\bfachschul|\bhochschul|\bkolleg|\bförderzentr", re.IGNORECASE)),
]


def derive_level(name: str, schulart: str) -> str:
    combined = f"{name} {schulart}".lower()
    has_primary = "grundschul" in combined
    has_secondary = any(re.search(p, combined) for _, p in LEVEL_MAP if _ == "secondary" for _ in [None])
    # Simpler: just pattern-match
    has_primary = bool(re.search(r"\bgrundschul", combined))
    has_secondary = bool(re.search(
        r"\bmittelschul|\bhauptschul|\brealschul|\bgymnasium|\bgesamtschul|\bsekundar|\bwirtschaftsschul",
        combined))
    has_other = bool(re.search(r"\bberufs|\bfachoberschul|\bfachschul|\bhochschul|\bkolleg", combined))
    if has_primary and has_secondary:
        return "both"
    if has_primary:
        return "primary"
    if has_secondary:
        return "secondary"
    if has_other:
        return "other"
    # If we saw "Förderzentr", call it "other" to exclude
    if "förderzentr" in combined:
        return "other"
    return "unknown"


def main() -> int:
    if not RAW_LISTING.exists():
        print(f"missing {RAW_LISTING}; run phase0_probe_km_bayern.py first")
        return 1

    listings = parse_listing(RAW_LISTING.read_text())
    print(f"[listing] {len(listings)} Stadt München schools")

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    rows: list[dict] = []
    scraped_at = datetime.utcnow().isoformat() + "Z"
    detail_calls = 0
    for i, entry in enumerate(listings, start=1):
        name = entry["name_listing"]
        class_name = classify_by_name(name)
        detail = {}
        if class_name == "unknown":
            # Fetch detail page
            try:
                html = fetch_detail(session, entry["detail_url"])
                detail_calls += 1
                detail = extract_detail_fields(html)
            except Exception as e:
                detail = {"error": str(e)}
            rs = detail.get("rechtlicher_status", "")
            traegerschaft_hint = derive_traegerschaft(rs)
            if traegerschaft_hint == "privat":
                class_name = "private"
            elif traegerschaft_hint in ("staatlich", "städtisch"):
                class_name = "public"
        if class_name != "private":
            continue

        # Need level; prefer detail schulart if present
        schulart = detail.get("schulart_detail", "")
        level = derive_level(name, schulart)
        if level not in ("primary", "secondary", "both"):
            continue

        rs = detail.get("rechtlicher_status", "")
        rows.append({
            "source_key": "km_bayern",
            "source_ref": entry["school_id"],
            "schulname": name,
            "address_line1": entry["street"],
            "plz": entry["plz"],
            "ort": entry["city"],
            "latitude": "",
            "longitude": "",
            "schulart": level,
            "schulart_detail": schulart,
            "traegerschaft_hint": derive_traegerschaft(rs) or "privat",
            "rechtlicher_status": rs,
            "traeger_raw": detail.get("traeger", ""),
            "website": "",
            "email": "",
            "phone": "",
            "source_url": entry["detail_url"],
            "match_reasons": "|".join([
                f"name_class:{classify_by_name(name)}",
                f"detail_status:{rs[:60]}" if rs else "no_detail",
            ]),
            "scraped_at": scraped_at,
        })

    fieldnames = list(rows[0].keys()) if rows else [
        "source_key", "source_ref", "schulname", "address_line1", "plz", "ort",
        "latitude", "longitude", "schulart", "schulart_detail",
        "traegerschaft_hint", "rechtlicher_status", "traeger_raw",
        "website", "email", "phone", "source_url", "match_reasons", "scraped_at",
    ]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"\n=== Phase 1 km.bayern.de ===")
    print(f"  listings parsed:      {len(listings)}")
    print(f"  detail pages fetched: {detail_calls}")
    print(f"  private primary/secondary kept: {len(rows)}")
    print(f"  → {OUT.relative_to(PROJECT_ROOT)}")
    print(f"\n  Samples:")
    for r in rows[:10]:
        print(f"    - {r['schulname'][:60]:<60}  level={r['schulart']}  rs={r['rechtlicher_status'][:40]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
