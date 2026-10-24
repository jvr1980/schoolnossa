#!/usr/bin/env python3
"""
Flag columns that are populated but carry no information.

Every silent data bug found in September 2026 passed a null-rate coverage check
and would have been caught by this one:

  NL traffic       0/1626 rows, yet Phase 3 logged "[OK] OK"
  GB crime         crime_total_per_1000 = 0.0 for all 5,303 rows, coverage 100%
                   (cached HTTP 429s written to disk as {"_total": 0})
  NL descriptions  246 pass1_en cached as "" — present, therefore "complete"

A column that is 100% populated with one repeated value is almost always a
failed join, a swallowed error, or a fill-with-zero where NaN was meant.

Usage:
    python3 scripts_shared/qa/check_constant_columns.py data_nl/final/*.parquet
    python3 scripts_shared/qa/check_constant_columns.py --fail-on-constant data_gb/final/x.parquet
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Columns that are legitimately constant: provenance stamps, country metadata.
EXPECT_CONSTANT_SUFFIXES = ("_source", "_data_source", "_year", "_data_year")
EXPECT_CONSTANT_EXACT = {
    "country_code", "country_name", "language", "currency",
    "data_source_version", "metadata_source", "school_quality_source",
    "academic_data_source", "students_data_year", "education_type",
    "school_type", "geocode_precision",
    # Type descriptors are constant by construction in a single-level table
    # (the primary set is filtered to TYPE_PO=BO).
    "school_type_national", "school_subtype", "ownership_national",
    "geo_country",  # one country per table
}


def is_expected_constant(col: str) -> bool:
    return col in EXPECT_CONSTANT_EXACT or col.endswith(EXPECT_CONSTANT_SUFFIXES)


def check(path: Path, min_rows: int = 50) -> list[dict]:
    df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path, low_memory=False)
    findings = []
    # Vector columns must hold arrays. A CSV round-trip stringifies them into
    # 12k-char text that is 100% populated with distinct values — invisible to
    # the constant check below, and unloadable into Supabase's vector(768).
    if "embedding" in df.columns:
        sample = df["embedding"].dropna()
        if len(sample) and isinstance(sample.iloc[0], str):
            findings.append({
                "column": "embedding",
                "rows": len(sample),
                "coverage_pct": 100 * len(sample) / len(df),
                "value": "<stored as str, not an array>",
                "expected": False,
            })

    for col in df.columns:
        if col.startswith("embedding"):
            continue
        series = df[col].dropna()
        if len(series) < min_rows:
            continue
        distinct = series.nunique(dropna=True)
        if distinct > 1:
            continue
        value = series.iloc[0]
        findings.append({
            "column": col,
            "rows": len(series),
            "coverage_pct": 100 * len(series) / len(df),
            "value": value,
            "expected": is_expected_constant(col),
        })
    return findings


def main():
    parser = argparse.ArgumentParser(description="Flag populated-but-constant columns")
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--min-rows", type=int, default=50,
                        help="Ignore columns with fewer populated rows (default 50)")
    parser.add_argument("--fail-on-constant", action="store_true",
                        help="Exit non-zero if an unexpected constant is found (for CI)")
    args = parser.parse_args()

    total_suspect = 0
    for raw in args.paths:
        path = Path(raw)
        if not path.exists():
            print(f"!! {path} not found")
            continue
        findings = check(path, args.min_rows)
        suspect = [f for f in findings if not f["expected"]]
        total_suspect += len(suspect)

        print(f"\n=== {path.name} ===")
        if not findings:
            print("  no constant columns")
            continue
        expected = len(findings) - len(suspect)
        print(f"  {len(suspect)} suspect, {expected} expected (provenance/metadata)")
        for f in sorted(suspect, key=lambda x: -x["coverage_pct"]):
            print(f"  SUSPECT  {f['column']:38s} {f['coverage_pct']:5.1f}% populated, "
                  f"every value = {f['value']!r}")

    if total_suspect:
        print(f"\n{total_suspect} column(s) populated with a single repeated value — "
              f"check for a failed join, a swallowed error, or fill-with-zero.")
    if args.fail_on_constant and total_suspect:
        sys.exit(1)


if __name__ == "__main__":
    main()
