#!/usr/bin/env python3
"""
Transform international school data (core schema) to Berlin 265-column schema.

This is the international equivalent of {city}_to_berlin_schema.py — it takes
a DataFrame in core schema format (from any country pipeline) and produces a
Berlin-compatible parquet that the frontend can consume.

German pipelines continue to use their own *_to_berlin_schema.py files.
This script handles NL, GB, FR, IT, ES data.

Usage:
    python international_to_berlin_schema.py --country NL [--input path] [--output path]

Or as a library:
    from scripts_international.international_to_berlin_schema import transform_to_berlin
    berlin_df = transform_to_berlin(international_df, country_code="NL")
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path

# Add project root to path for imports
import sys
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.schema.core_schema import CORE_TO_BERLIN_MAP


# The canonical 265-column Berlin schema (read from reference at runtime)
BERLIN_REFERENCE = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"


def get_berlin_columns() -> list[str]:
    """Get the exact Berlin column order from the reference parquet."""
    if BERLIN_REFERENCE.exists():
        import pyarrow.parquet as pq
        schema = pq.read_schema(BERLIN_REFERENCE)
        return schema.names
    else:
        raise FileNotFoundError(
            f"Berlin reference parquet not found at {BERLIN_REFERENCE}. "
            "Needed to determine exact column order."
        )


# Berlin's crime columns are Häufigkeitszahlen — cases per 100,000 residents
# (see scripts_berlin/processing/convert_crime_xlsx_to_csv.py). The core schema
# carries rates per 1,000, so international values must be scaled before they
# land in a Berlin-named column the UI renders on a single axis.
CRIME_PER_1000_TO_PER_100K = 100.0

# Berlin's tertile vocabulary (rebuild_final_table): safe / moderate / elevated.
# Country enrichers emitting "high" must be normalised or UI filters miss rows.
CRIME_CATEGORY_ALIASES = {"high": "elevated", "hoog": "elevated", "low": "safe"}

# Fallback when a country pipeline does not stamp its student-data vintage.
DEFAULT_SCHOOL_YEAR = "2024_25"


def _student_vintage(df: pd.DataFrame) -> str:
    """School-year vintage of students_current/teachers_current, e.g. '2024_25'.

    Prefers the vintage the country pipeline recorded alongside the data; only
    falls back to the constant when the pipeline predates that field.
    """
    if "students_data_year" in df.columns:
        stamped = df["students_data_year"].dropna().astype(str)
        if len(stamped):
            return stamped.mode().iloc[0].replace("-", "_")
    return DEFAULT_SCHOOL_YEAR


def transform_to_berlin(df: pd.DataFrame, country_code: str) -> pd.DataFrame:
    """
    Transform a core-schema DataFrame to the Berlin reference schema.

    Args:
        df: DataFrame with core schema columns (from any country pipeline)
        country_code: ISO country code (NL, GB, FR, IT, ES)

    Returns:
        DataFrame with the Berlin reference columns in canonical order, plus the
        stable (year-agnostic) fields appended. Country-specific academic columns
        are NOT included (they stay in the core+extension output). Berlin-only
        columns (Abitur, MSA, PLZ traffic, detailed crime) are filled with None.

    Stable fields (schueler_current, data_school_year, ...) are derived here the
    same way every German mapper derives them, so the Lovable app can bind to one
    set of names across all countries.
    """
    berlin_columns = get_berlin_columns()

    # Map core columns to Berlin names
    output = pd.DataFrame(index=df.index)

    for core_col, berlin_col in CORE_TO_BERLIN_MAP.items():
        if core_col in df.columns and berlin_col in berlin_columns:
            output[berlin_col] = df[core_col]

    # --- Student/teacher vintage -------------------------------------------
    # CORE_TO_BERLIN_MAP pins students_current to schueler_2024_25. That is only
    # correct while the country's data really is 2024/25; after a refresh moves
    # it on, the value would be mislabelled — or silently dropped, if the Berlin
    # reference has no column for the new vintage. Write the year-suffixed column
    # matching the real vintage and let the stable fields below carry the value
    # regardless of which year columns the reference happens to have.
    vintage = _student_vintage(df)
    for core_col, prefix in (("students_current", "schueler"),
                             ("teachers_current", "lehrer")):
        if core_col not in df.columns:
            continue
        dated = f"{prefix}_{vintage}"
        default_col = f"{prefix}_{DEFAULT_SCHOOL_YEAR}"
        if vintage == DEFAULT_SCHOOL_YEAR:
            continue  # CORE_TO_BERLIN_MAP already placed it correctly
        if dated in berlin_columns:
            output[dated] = df[core_col]
        # Either way the default-year copy is now mislabelled.
        if default_col in output.columns:
            output[default_col] = None

    # --- Crime --------------------------------------------------------------
    # Berlin: cases per 100k (Häufigkeitszahl). Core schema: per 1,000.
    if "crime_total_per_1000" in df.columns:
        output["crime_total_crimes_avg"] = (
            pd.to_numeric(df["crime_total_per_1000"], errors="coerce")
            * CRIME_PER_1000_TO_PER_100K
        )
    if "crime_violent_per_1000" in df.columns:
        output["crime_violent_crime_avg"] = (
            pd.to_numeric(df["crime_violent_per_1000"], errors="coerce")
            * CRIME_PER_1000_TO_PER_100K
        )
    if "crime_safety_category" in output.columns:
        output["crime_safety_category"] = (
            output["crime_safety_category"].astype("object")
            .replace(CRIME_CATEGORY_ALIASES)
        )
    # Berlin carries crime_total_crimes_<year>; the core schema has no such
    # column, so write the year-suffixed slot matching the country's own crime
    # vintage. Without it add_stable_fields finds no candidate and both
    # crime_total_crimes_current and crime_data_year come out empty.
    if "crime_total_per_1000" in df.columns and "crime_data_year" in df.columns:
        scaled = (pd.to_numeric(df["crime_total_per_1000"], errors="coerce")
                  * CRIME_PER_1000_TO_PER_100K)
        for year in df["crime_data_year"].dropna().astype(str).str.slice(0, 4).unique():
            dated = f"crime_total_crimes_{year}"
            if dated in berlin_columns:
                rows = df["crime_data_year"].astype(str).str.startswith(year)
                if dated not in output.columns:
                    output[dated] = None
                output.loc[rows, dated] = scaled[rows]

    # --- Traffic ------------------------------------------------------------
    # Berlin's plz_* block is Telraam sensor data (CC-BY-NC): counts of observed
    # vehicles. Accident counts are a different measurement and must not be
    # written into it — traffic_accidents_* stay in the core/extension output.
    if "traffic_volume_index" in df.columns:
        output["plz_traffic_intensity"] = df["traffic_volume_index"]

    # Build final output with exact Berlin column order
    final = pd.DataFrame()
    for col in berlin_columns:
        if col in output.columns:
            final[col] = output[col]
        else:
            final[col] = None

    # --- Stable (year-agnostic) fields, as in every German mapper -----------
    from scripts_shared.schema.stable_fields import add_stable_fields
    final = add_stable_fields(final)

    # add_stable_fields walks the Berlin year-suffixed columns newest-first. When
    # the country's vintage has no column in the Berlin reference, the newest
    # column it can see is the one holding students_previous — so it would report
    # last year's figure as current, stamped with last year's label. The country
    # frame is authoritative about its own vintage, so override from it.
    for core_col, stable_col in (("students_current", "schueler_current"),
                                 ("teachers_current", "lehrer_current")):
        if core_col in df.columns:
            final[stable_col] = pd.to_numeric(df[core_col], errors="coerce").values
    if "students_current" in df.columns:
        known = pd.to_numeric(df["students_current"], errors="coerce").notna().values
        if "data_school_year" not in final.columns:
            final["data_school_year"] = None
        final["data_school_year"] = final["data_school_year"].astype("object")
        final.loc[known, "data_school_year"] = vintage
        final.loc[~known, "data_school_year"] = None

    # Stable fields are additive; the canonical block keeps its order.
    tail = [c for c in final.columns if c not in berlin_columns]
    final = final[berlin_columns + tail]
    assert list(final.columns)[:len(berlin_columns)] == berlin_columns, "Column order mismatch!"

    return final


def transform_file(country_code: str, input_path: str = None, output_path: str = None):
    """Transform a country's final core-schema output to Berlin format."""
    code = country_code.lower()

    if input_path is None:
        input_path = PROJECT_ROOT / f"data_{code}" / "final" / f"{code}_school_master_table_final.parquet"
    else:
        input_path = Path(input_path)

    if output_path is None:
        output_dir = PROJECT_ROOT / f"data_{code}" / "final"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{code}_school_master_table_berlin_schema.parquet"
    else:
        output_path = Path(output_path)

    print(f"Loading {country_code} data from {input_path}...")
    df = pd.read_parquet(input_path)
    print(f"  {len(df)} schools, {len(df.columns)} columns")

    print("Transforming to Berlin schema...")
    berlin_df = transform_to_berlin(df, country_code)

    # Count populated columns
    populated = sum(1 for col in berlin_df.columns if berlin_df[col].notna().any())
    print(f"  Berlin schema: {len(berlin_df.columns)} columns ({populated} with data)")

    # Save
    berlin_df.to_parquet(output_path, index=False)
    print(f"  Saved: {output_path}")

    csv_path = output_path.with_suffix(".csv")
    berlin_df.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    return berlin_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform international data to Berlin schema")
    parser.add_argument("--country", required=True, help="Country code (NL, GB, FR, IT, ES)")
    parser.add_argument("--input", help="Input parquet path (default: data_{code}/final/)")
    parser.add_argument("--output", help="Output parquet path (default: data_{code}/final/)")
    args = parser.parse_args()

    transform_file(args.country, args.input, args.output)
