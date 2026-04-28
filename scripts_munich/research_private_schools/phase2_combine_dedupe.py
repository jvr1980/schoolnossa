#!/usr/bin/env python3
"""
Phase 2 — combine + dedupe candidate CSVs, then diff against the current
Munich master tables.

Input:
  data_munich/intermediate/private_research/{osm_overpass,km_bayern,google_places,gemini_grounded}.csv

Dedup rule (per plan):
  - rapidfuzz token_set_ratio >= 85 on normalized schulname
  - AND either (a) same PLZ + same ort, OR (b) haversine distance <= 150 m

Authority stack for field reconciliation:
  km_bayern > osm > google_places > gemini_grounded

Output:
  data_munich/intermediate/private_research/munich_private_schools_candidates.csv
  data_munich/intermediate/private_research/munich_private_schools_new_vs_current.csv
"""
from __future__ import annotations

import csv
import math
import re
import sys
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INTER = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"

INPUTS = [
    ("km_bayern", INTER / "km_bayern.csv"),          # highest authority
    ("osm", INTER / "osm_overpass.csv"),
    ("google_places", INTER / "google_places.csv"),
    ("gemini_grounded", INTER / "gemini_grounded.csv"),  # lowest
]

# Current live Munich data — for diff vs. "already in pipeline"
CURRENT_FILES = [
    PROJECT_ROOT / "data_munich" / "final" / "munich_primary_school_master_table_final.csv",
    PROJECT_ROOT / "data_munich" / "final" / "munich_secondary_school_master_table_final.csv",
]

NAME_MATCH_THRESHOLD = 80
DISTANCE_THRESHOLD_M = 150

# Stadt + Landkreis München municipalities (user scope). Everything else
# is dropped — we already saw Starnberg / Dachau / Gilching seep in.
IN_SCOPE_ORTE = {
    # Stadt
    "muenchen", "münchen",
    # Landkreis — official gemeinden
    "aschheim", "aying", "baierbrunn", "brunnthal", "feldkirchen",
    "garching", "garching b.münchen", "garching bei münchen",
    "gräfelfing", "graefelfing",
    "grasbrunn", "grünwald", "gruenwald",
    "haar", "höhenkirchen-siegertsbrunn", "hoehenkirchen-siegertsbrunn",
    "höhenkirchen", "hoehenkirchen",
    "hohenbrunn", "ismaning", "kirchheim", "kirchheim b. münchen",
    "kirchheim bei münchen", "neubiberg",
    "neuried", "oberhaching", "oberschleißheim", "oberschleissheim",
    "ottobrunn", "planegg", "pullach", "pullach i. isartal",
    "pullach im isartal", "putzbrunn",
    "riemerling",  # district of Hohenbrunn — show up sometimes
    "sauerlach", "schäftlarn", "schaeftlarn",
    "straßlach-dingharting", "strasslach-dingharting",
    "taufkirchen", "unterföhring", "unterfoehring",
    "unterhaching", "unterschleißheim", "unterschleissheim",
}

# Schulart / schulname substrings that disqualify (scope: primary + sekundar only)
OUT_OF_SCOPE_NAME = (
    "sonderpäd", "sonderpaed", "förderzentr", "foerderzentr",
    "förderschul", "foerderschul", "förderschwerpunkt",
    "förderschwerpunkt",
    "berufsschul", "berufsfachschul", "fachoberschul", "berufsoberschul",
    "fachschul", "hochschul", "akademie", "kolleg",
    "volkshochschul", "vhs ",
    "sprachschul", "musikschul", "fahrschul", "tanzschul", "schwimmschul",
    "internat",  # pure boarding with no regular school — edge case
    "wirtschaftsschul",  # Wirtschaftsschule is Berufsschule-adjacent, different school form
)

# Columns we attempt to fill in the canonical row
CANONICAL_FIELDS = [
    "schulname", "address_line1", "plz", "ort", "latitude", "longitude",
    "schulart", "schulart_detail", "traegerschaft_hint",
    "website", "email", "phone",
]


def _normalize_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[äöü]", lambda m: {"ä": "ae", "ö": "oe", "ü": "ue"}[m.group(0)], s)
    s = re.sub(r"ß", "ss", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Drop common noise words
    for n in ("muenchen", "munich", "stadt", "der", "die", "das", "von", "am", "an"):
        s = re.sub(rf"\b{n}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _haversine_m(a, b) -> float:
    if None in a or None in b:
        return float("inf")
    try:
        lat1, lon1 = float(a[0]), float(a[1])
        lat2, lon2 = float(b[0]), float(b[1])
    except (TypeError, ValueError):
        return float("inf")
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def _coord_or_none(row):
    lat = row.get("latitude") or None
    lon = row.get("longitude") or None
    try:
        return (float(lat), float(lon)) if lat and lon else (None, None)
    except (TypeError, ValueError):
        return (None, None)


def _stem_core(name: str) -> str:
    """Reduce to a 'core' identifier — drop admin/prefix words that vary across sources."""
    n = _normalize_name(name)
    for noise in (
        "privat", "private", "priv", "privates",
        "bilinguale", "bilinguales",
        "staatl", "staatlich", "staatliche", "staatliches", "staatlicher",
        "staedtisch", "städtisch", "staedtische", "stadtisch",
        "anerkannte", "anerkannt", "genehmigte", "genehmigt",
        "ersatzschule", "ergaenzungsschule", "ergänzungsschule",
        "gmbh", "ggmbh", "gag", "ev", "e v",
        "schule", "schulen",
        "grundschule", "grund", "mittelschule", "mittel", "realschule", "real",
        "gymnasium", "gym",
        "strasse", "str", "strasse",
        "freier", "freie", "traegerschaft", "trägerschaft",
        "erzbischoefliches", "erzbischöfliches", "erzbischoefliche", "erzbischöfliche",
        "erzbischoeflichen", "erzbischöflichen",
        "evangelisches", "evangelische", "katholisches", "katholische",
        "juedisch", "jüdisch", "juedisches", "jüdisches",
        "campus",
    ):
        n = re.sub(rf"\b{noise}\b", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def _is_match(r1: dict, r2: dict) -> tuple[bool, str]:
    n1_full = _normalize_name(r1.get("schulname", ""))
    n2_full = _normalize_name(r2.get("schulname", ""))
    if not n1_full or not n2_full:
        return False, "empty-name"

    # Score 1: token_set_ratio on full normalized names
    score_full = fuzz.token_set_ratio(n1_full, n2_full)

    # Score 2: token_set_ratio on stemmed core — handles "Privat Realschule Huber" vs "Priv. Realschule Huber in München"
    core1 = _stem_core(r1.get("schulname", ""))
    core2 = _stem_core(r2.get("schulname", ""))
    score_core = fuzz.token_set_ratio(core1, core2) if core1 and core2 else 0

    name_score = max(score_full, score_core)
    if name_score < NAME_MATCH_THRESHOLD:
        return False, f"name_full={score_full},core={score_core}"

    plz_match = bool(r1.get("plz")) and r1.get("plz") == r2.get("plz")
    ort_match = bool(r1.get("ort")) and (r1.get("ort") or "").lower() == (r2.get("ort") or "").lower()
    if plz_match and ort_match:
        return True, f"name={name_score},plz+ort"
    c1 = _coord_or_none(r1)
    c2 = _coord_or_none(r2)
    dist = _haversine_m(c1, c2)
    if dist <= DISTANCE_THRESHOLD_M:
        return True, f"name={name_score},dist={dist:.0f}m"
    # Loose: high core score + either ort or PLZ
    if score_core >= 80 and (ort_match or plz_match):
        return True, f"core={score_core},ort_or_plz"
    # One side has no coords/address at all — fall back to very-high name only
    no_loc_1 = c1 == (None, None) and not r1.get("plz")
    no_loc_2 = c2 == (None, None) and not r2.get("plz")
    if (no_loc_1 or no_loc_2) and score_core >= 78:
        return True, f"core={score_core},one_side_no_loc"
    return False, f"name={name_score},dist={dist:.0f}m"


def _in_scope(row: dict) -> tuple[bool, str]:
    """Apply scope filters: primary+secondary only, Stadt+Landkreis München."""
    name = (row.get("schulname") or "").lower()
    # Out-of-scope name substrings
    for marker in OUT_OF_SCOPE_NAME:
        if marker in name:
            return False, f"name_excluded:{marker}"
    # Ort check (only if ort is populated)
    ort = (row.get("ort") or "").strip().lower()
    if ort:
        ort_norm = ort.replace("ß", "ss")
        if ort_norm not in IN_SCOPE_ORTE and ort not in IN_SCOPE_ORTE:
            return False, f"ort_out_of_scope:{ort}"
    # Level gate
    level = row.get("schulart", "")
    if level not in ("primary", "secondary", "both"):
        return False, f"level={level}"
    return True, "in_scope"


def _best_field(existing: str, new: str) -> str:
    if existing and existing.strip():
        return existing
    return new or ""


def _merge(into: dict, add: dict) -> None:
    for f in CANONICAL_FIELDS:
        into[f] = _best_field(into.get(f, ""), add.get(f, ""))


def load_inputs() -> list[tuple[str, pd.DataFrame]]:
    out = []
    for source_key, path in INPUTS:
        if not path.exists():
            print(f"  MISSING: {path.relative_to(PROJECT_ROOT)}")
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df["_source"] = source_key
        out.append((source_key, df))
        print(f"  loaded {source_key}: {len(df)} rows")
    return out


def load_current() -> pd.DataFrame:
    frames = []
    for p in CURRENT_FILES:
        if p.exists():
            frames.append(pd.read_csv(p, low_memory=False))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    print("=== Phase 2: combine + dedupe ===")
    inputs = load_inputs()

    # Walk in authority order; later sources merge into prior matches instead of
    # creating new rows.
    canonical: list[dict] = []
    scope_rejected = {"ort_out_of_scope": 0, "name_excluded": 0, "level": 0}
    scope_rejects: list[tuple[str, str, str]] = []  # (source, name, reason)
    for source_key, df in inputs:
        for _, row in df.iterrows():
            r = row.to_dict()
            # Strip empty placeholders
            for k, v in list(r.items()):
                if v == "nan":
                    r[k] = ""
            # Scope filter (applies to row BEFORE it enters the canonical set)
            ok, why = _in_scope(r)
            if not ok:
                bucket = why.split(":")[0] if ":" in why else why
                scope_rejected[bucket] = scope_rejected.get(bucket, 0) + 1
                scope_rejects.append((source_key, r.get("schulname", ""), why))
                continue
            # Try match against existing canonical rows
            matched_idx = None
            for i, existing in enumerate(canonical):
                is_match, reason = _is_match(existing, r)
                if is_match:
                    matched_idx = i
                    break
            if matched_idx is None:
                # create new canonical row, initialized from first encounter
                canonical.append({
                    **{f: r.get(f, "") for f in CANONICAL_FIELDS},
                    "sources": source_key,
                    "authority": source_key,  # first (highest-priority) to mention it
                    "source_refs": f"{source_key}:{r.get('source_ref', '')}",
                    "match_trail": f"[{source_key}] new",
                    "gemini_category": r.get("gemini_category", ""),
                    "confidence": r.get("confidence", ""),
                    "rechtlicher_status": r.get("rechtlicher_status", ""),
                })
            else:
                existing = canonical[matched_idx]
                _merge(existing, r)
                if source_key not in existing["sources"].split("|"):
                    existing["sources"] += f"|{source_key}"
                existing["source_refs"] += f"|{source_key}:{r.get('source_ref', '')}"
                existing["match_trail"] += f"; [{source_key}] merged ({reason})"
                # Fill in Gemini category if we got it
                if r.get("gemini_category") and not existing.get("gemini_category"):
                    existing["gemini_category"] = r.get("gemini_category")
                if r.get("rechtlicher_status") and not existing.get("rechtlicher_status"):
                    existing["rechtlicher_status"] = r.get("rechtlicher_status")

    print(f"\n  unique canonical rows: {len(canonical)}")
    print(f"  scope-rejected by bucket: {scope_rejected}")
    if scope_rejects[:5]:
        print(f"  first 5 rejects:")
        for src, name, why in scope_rejects[:5]:
            print(f"    [{src:15}] {name[:50]:<50} → {why}")

    # Compute source-count distribution
    dist = {}
    for row in canonical:
        n = len(row["sources"].split("|"))
        dist[n] = dist.get(n, 0) + 1
    print(f"  rows by #sources:      {dict(sorted(dist.items()))}")

    # Write combined
    combined_path = INTER / "munich_private_schools_candidates.csv"
    field_order = [
        "schulname", "address_line1", "plz", "ort", "latitude", "longitude",
        "schulart", "schulart_detail", "traegerschaft_hint",
        "gemini_category", "rechtlicher_status", "confidence",
        "website", "email", "phone",
        "sources", "authority", "source_refs", "match_trail",
    ]
    with combined_path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=field_order, extrasaction="ignore")
        w.writeheader()
        for r in canonical:
            w.writerow(r)
    print(f"  → {combined_path.relative_to(PROJECT_ROOT)}")

    # Diff vs current live data
    print("\n=== Diff vs. current Munich master tables ===")
    current = load_current()
    if current.empty:
        print("  (no current master tables found — skipping diff)")
        return 0

    current_rows = current[["schulnummer", "schulname", "strasse", "plz", "stadt",
                             "latitude", "longitude"]].to_dict("records")
    # Normalize current for matching
    new_rows = []
    already_in = []
    for row in canonical:
        found = None
        for c in current_rows:
            c_row = {
                "schulname": c.get("schulname", ""),
                "address_line1": c.get("strasse", ""),
                "plz": str(c.get("plz", "")),
                "ort": c.get("stadt", ""),
                "latitude": c.get("latitude"),
                "longitude": c.get("longitude"),
            }
            is_match, _ = _is_match(row, c_row)
            if is_match:
                found = c
                break
        if found:
            already_in.append({**row, "existing_schulnummer": found["schulnummer"],
                               "existing_schulname": found["schulname"]})
        else:
            new_rows.append(row)

    new_path = INTER / "munich_private_schools_new_vs_current.csv"
    with new_path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=field_order, extrasaction="ignore")
        w.writeheader()
        for r in new_rows:
            w.writerow(r)

    already_in_path = INTER / "munich_private_schools_already_in_dataset.csv"
    with already_in_path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=field_order + ["existing_schulnummer", "existing_schulname"],
            extrasaction="ignore",
        )
        w.writeheader()
        for r in already_in:
            w.writerow(r)

    print(f"  rows already in live Munich dataset: {len(already_in)}")
    print(f"  net-new private-school candidates:   {len(new_rows)}")
    print(f"  → {new_path.relative_to(PROJECT_ROOT)}")
    print(f"  → {already_in_path.relative_to(PROJECT_ROOT)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
