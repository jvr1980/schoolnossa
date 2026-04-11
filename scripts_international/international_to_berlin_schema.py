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


def transform_to_berlin(df: pd.DataFrame, country_code: str) -> pd.DataFrame:
    """
    Transform a core-schema DataFrame to Berlin 265-column format.

    Args:
        df: DataFrame with core schema columns (from any country pipeline)
        country_code: ISO country code (NL, GB, FR, IT, ES)

    Returns:
        DataFrame with exactly 265 Berlin-schema columns, in canonical order.
        Country-specific academic columns are NOT included (they stay in the
        core+extension output). Berlin-only columns (Abitur, MSA, PLZ traffic,
        detailed crime) are filled with None.
    """
    berlin_columns = get_berlin_columns()

    # Map core columns to Berlin names
    output = pd.DataFrame(index=df.index)

    for core_col, berlin_col in CORE_TO_BERLIN_MAP.items():
        if core_col in df.columns and berlin_col in berlin_columns:
            output[berlin_col] = df[core_col]

    # Handle crime columns: map simplified crime to Berlin's detailed breakdown
    # The core schema has crime_total_per_1000 etc; Berlin has crime_total_crimes_2023 etc.
    # We populate what we can and leave the rest NULL.
    if "crime_total_per_1000" in df.columns:
        # Map to the _avg columns (most comparable)
        output["crime_total_crimes_avg"] = df["crime_total_per_1000"]
    if "crime_violent_per_1000" in df.columns:
        output["crime_violent_crime_avg"] = df["crime_violent_per_1000"]

    # Handle traffic: core has simplified metrics, Berlin has PLZ sensor data
    # Map what we can
    if "traffic_volume_index" in df.columns:
        output["plz_traffic_intensity"] = df["traffic_volume_index"]
    if "traffic_accidents_1000m" in df.columns:
        output["plz_observation_count"] = df["traffic_accidents_1000m"]

    # Build final output with exact Berlin column order
    final = pd.DataFrame()
    for col in berlin_columns:
        if col in output.columns:
            final[col] = output[col]
        else:
            final[col] = None

    # Verify
    assert list(final.columns) == berlin_columns, "Column order mismatch!"
    assert len(final.columns) == len(berlin_columns), "Column count mismatch!"

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
