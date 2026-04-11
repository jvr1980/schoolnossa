#!/usr/bin/env python3
"""
Coverage Verification Script
==============================

Checks student/teacher data coverage across all city final outputs.
Reports fill rates for schueler_2024_25 and lehrer_2024_25.

Usage:
    python3 scripts_shared/verify_coverage.py
"""

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# City configurations: (city_name, data_dir, file_patterns)
CITIES = [
    ("Berlin (sec)", "data_berlin/final", "school_master_table_final*.csv"),
    ("Berlin (pri)", "data_berlin_primary/final", "grundschule_master_table_final*.csv"),
    ("Hamburg (sec)", "data_hamburg/final", "hamburg_secondary*final*.csv"),
    ("Hamburg (pri)", "data_hamburg_primary/final", "hamburg_primary*final*.csv"),
    ("NRW/Düsseldorf+Köln (sec)", "data_nrw/final", "nrw_secondary*final*.csv"),
    ("NRW/Düsseldorf+Köln (pri)", "data_nrw/final", "nrw_primary*final*.csv"),
    ("Frankfurt (sec)", "data_frankfurt/final", "frankfurt_secondary*final*.csv"),
    ("Frankfurt (pri)", "data_frankfurt/final", "frankfurt_primary*final*.csv"),
    ("München (sec)", "data_munich/final", "munich_secondary*final*.csv"),
    ("München (pri)", "data_munich/final", "munich_primary*final*.csv"),
    ("Stuttgart (sec)", "data_stuttgart/final", "stuttgart_secondary*final*.csv"),
    ("Stuttgart (pri)", "data_stuttgart/final", "stuttgart_primary*final*.csv"),
    ("Dresden (sec)", "data_dresden/final", "dresden_secondary*final*.csv"),
    ("Dresden (pri)", "data_dresden/final", "dresden_primary*final*.csv"),
    ("Leipzig", "data_leipzig/final", "leipzig*final*.csv"),
    ("Bremen", "data_bremen/final", "bremen*final*.csv"),
]

FIELDS = [
    "schueler_2024_25",
    "lehrer_2024_25",
    "sprachen",
    "description_de",
    "crime_safety_rank",
    "transit_accessibility_score",
    "belastungsstufe",
]


def check_coverage():
    print(f"\n{'='*90}")
    print("SCHOOLNOSSA DATA COVERAGE REPORT")
    print(f"{'='*90}")

    # Header
    header = f"{'City':<30}"
    for field in FIELDS:
        short = field.replace("_2024_25", "").replace("transit_accessibility_", "transit_").replace("crime_safety_", "crime_")
        header += f" {short:>12}"
    header += f" {'Total':>8}"
    print(header)
    print("-" * 90)

    for city_name, data_dir, pattern in CITIES:
        city_path = PROJECT_ROOT / data_dir
        if not city_path.exists():
            continue

        files = sorted(city_path.glob(pattern))
        if not files:
            # Try without the pattern filter
            files = sorted(city_path.glob("*final*.csv"))
        if not files:
            continue

        # Use first matching file
        df = pd.read_csv(files[0], low_memory=False)
        total = len(df)

        row = f"{city_name:<30}"
        for field in FIELDS:
            if field in df.columns:
                filled = df[field].notna().sum()
                if df[field].dtype == object:
                    filled = (df[field].notna() & (df[field].astype(str).str.strip() != "")
                              & (df[field].astype(str) != "nan")).sum()
                pct = filled / total * 100
                row += f" {filled:>4}/{total:<4} {pct:>3.0f}%"
            else:
                row += f"     --     "
        row += f" {total:>8}"
        print(row)

    print(f"{'='*90}")

    # Sanity checks on student/teacher values
    print(f"\nSANITY CHECKS:")
    for city_name, data_dir, pattern in CITIES:
        city_path = PROJECT_ROOT / data_dir
        if not city_path.exists():
            continue
        files = sorted(city_path.glob(pattern))
        if not files:
            files = sorted(city_path.glob("*final*.csv"))
        if not files:
            continue

        df = pd.read_csv(files[0], low_memory=False)
        for col in ["schueler_2024_25", "lehrer_2024_25"]:
            if col in df.columns:
                vals = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(vals) > 0:
                    outliers_low = (vals < 10).sum()
                    outliers_high = (vals > 5000).sum()
                    if outliers_low > 0 or outliers_high > 0:
                        print(f"  {city_name} {col}: {outliers_low} below 10, {outliers_high} above 5000")


if __name__ == "__main__":
    check_coverage()
