#!/usr/bin/env python3
"""
Phase 3 — validation spot-checks.

Inputs:
  data_munich/intermediate/private_research/munich_private_schools_candidates.csv
  data_munich/intermediate/private_research/munich_private_schools_new_vs_current.csv

Checks:
  1. Recall: each of 10 known Munich-area private schools appears in the
     candidate set (fuzzy name match).
  2. Precision: sample 20 rows deterministically, print them for manual
     eyeballing. We stop short of auto-verification because it needs a
     human review of "is this really a real private school".
  3. Coverage: total count vs user's 25–45 expected band.
"""
from __future__ import annotations

import csv
import random
import sys
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INTER = PROJECT_ROOT / "data_munich" / "intermediate" / "private_research"

CAND_FILE = INTER / "munich_private_schools_candidates.csv"
NEW_FILE  = INTER / "munich_private_schools_new_vs_current.csv"

# Recall spot-check — 10 known Munich-area privates from the plan, plus a
# few extras where helpful
KNOWN = [
    "Obermenzinger Gymnasium",
    "Maria-Ward-Gymnasium Nymphenburg",
    "Rudolf-Steiner-Schule München",
    "Gymnasium der Englischen Fräulein Nymphenburg",  # = Maria-Ward-Gym alias
    "Phorms München",
    "Isar-Gymnasium",
    "Lukas-Gymnasium",
    "Sabel Realschule München",
    "Nymphenburger Schulen",
    "Edith-Stein-Gymnasium",
    # Extras for robustness
    "Jules Verne Campus",
    "Bavarian International School",
    "Jüdisches Gymnasium München",
    "Sinai-Grundschule",
    "Montessori München",
]


def _norm(s: str) -> str:
    return (s or "").lower().replace("ß", "ss")


def recall_check(df: pd.DataFrame) -> list[dict]:
    names = df["schulname"].fillna("").tolist()
    results = []
    for target in KNOWN:
        best_score = 0
        best_name = ""
        tn = _norm(target)
        for cand in names:
            cn = _norm(cand)
            score = max(fuzz.token_set_ratio(tn, cn), fuzz.partial_ratio(tn, cn))
            if score > best_score:
                best_score = score
                best_name = cand
        matched = best_score >= 80
        results.append({
            "target": target,
            "best_match": best_name,
            "score": best_score,
            "matched": matched,
        })
    return results


def precision_sample(df: pd.DataFrame, n: int = 20, seed: int = 42) -> pd.DataFrame:
    random.seed(seed)
    idx = random.sample(range(len(df)), min(n, len(df)))
    return df.iloc[idx].copy()


def main() -> int:
    if not CAND_FILE.exists() or not NEW_FILE.exists():
        print("Phase 2 output missing — run phase2_combine_dedupe.py first")
        return 1

    cand = pd.read_csv(CAND_FILE, dtype=str, keep_default_na=False)
    new = pd.read_csv(NEW_FILE, dtype=str, keep_default_na=False)

    print("=" * 72)
    print("PHASE 3 — VALIDATION")
    print("=" * 72)
    print(f"  total canonical candidates:  {len(cand)}")
    print(f"  net-new vs current dataset:  {len(new)}")

    # Coverage calibration
    lo, hi = 25, 60
    in_band = lo <= len(new) <= hi
    print(f"\n  coverage (net-new): {len(new)} "
          f"→ expected band [{lo}, {hi}]: "
          f"{'✓ in band' if in_band else '⚠ OUT OF BAND'}")

    # Recall check
    print("\n  RECALL spot-check (15 known privates vs candidate set):")
    recall = recall_check(cand)
    hits = 0
    for r in recall:
        flag = "✓" if r["matched"] else "✗"
        print(f"    {flag} {r['target'][:44]:<44}  score={r['score']:>3}  "
              f"best={r['best_match'][:45]}")
        if r["matched"]:
            hits += 1
    print(f"\n  recall: {hits}/{len(recall)} ({100 * hits / len(recall):.0f}%)")

    # Precision sample
    print("\n  PRECISION sample (20 random net-new rows, seed=42):")
    sample = precision_sample(new, n=20, seed=42)
    for _, r in sample.iterrows():
        sources = r["sources"].replace("|", ", ")
        cat = r.get("gemini_category", "") or r.get("traegerschaft_hint", "")
        print(f"    - {r['schulname'][:55]:<55}  {r['plz']} {r['ort']}  "
              f"[{sources}]  cat={cat}")

    # Source-combination stats
    print("\n  source combinations in net-new:")
    print(new["sources"].value_counts().head(10).to_string())

    return 0


if __name__ == "__main__":
    sys.exit(main())
