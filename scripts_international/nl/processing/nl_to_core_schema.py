#!/usr/bin/env python3
"""
Transform NL school data to SchoolNossa core schema.

Maps the merged DUO data (nl_school_master_geocoded.csv) to the universal
core schema defined in scripts_shared/schema/core_schema.py, plus NL-specific
extension columns from scripts_shared/schema/country_extensions.py.

Input:  data_nl/intermediate/nl_school_master_geocoded.csv
Output: data_nl/final/nl_school_master_table_final.parquet
        data_nl/final/nl_school_master_table_final.csv

Usage:
    python nl_to_core_schema.py
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.schema.core_schema import CORE_COLUMNS, schema_coverage_report
from scripts_shared.schema.country_extensions import get_full_schema, NL_EXTENSION

DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"
FINAL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Education type mapping (Dutch → normalized)
SCHOOL_TYPE_MAP = {
    "PRO": "special",
    "VMBO": "secondary",
    "VMBO-T": "secondary",
    "HAVO": "secondary",
    "VWO": "secondary",
    "HAVO/VWO": "secondary",
    "VMBO/HAVO": "secondary",
    "VMBO/HAVO/VWO": "secondary",
}

# Ownership mapping
DENOMINATION_TO_OWNERSHIP = {
    "Openbaar": "public",
    "Rooms-Katholiek": "private",
    "Protestants-Christelijk": "private",
    "Gereformeerd Vrijgemaakt": "private",
    "Reformatorisch": "private",
    "Evangelisch": "private",
    "Interconfessioneel": "private",
    "Algemeen Bijzonder": "private",
    "Antroposofisch": "private",
    "Islamitisch": "private",
    "Joods": "private",
    "Hindoe": "private",
    "Samenwerkingsschool": "public",
}


def compute_academic_score(row) -> float:
    """
    Compute normalized 0-100 academic performance score from Dutch exam data.

    Dutch final exam grades are on a 1-10 scale (6.0 = pass, 10.0 = perfect).
    Map to 0-100: score = ((grade - 1) / 9) * 100, clamped to [0, 100].
    Also factor in pass rate.
    """
    grade = row.get("exam_avg_overall_2024_25") or row.get("exam_avg_overall_2023_24")
    pass_rate = row.get("exam_pass_rate_2024_25") or row.get("exam_pass_rate_2023_24")

    if pd.isna(grade) and pd.isna(pass_rate):
        return np.nan

    # Grade component (0-100 from 1-10 scale)
    grade_score = ((float(grade) - 1) / 9 * 100) if pd.notna(grade) else np.nan

    # Pass rate component (already 0-1, scale to 0-100)
    pass_score = float(pass_rate) * 100 if pd.notna(pass_rate) else np.nan

    # Weighted average (grade 70%, pass rate 30%)
    if pd.notna(grade_score) and pd.notna(pass_score):
        return round(grade_score * 0.7 + pass_score * 0.3, 1)
    elif pd.notna(grade_score):
        return round(grade_score, 1)
    elif pd.notna(pass_score):
        return round(pass_score, 1)
    return np.nan


def transform(input_path: Path = None) -> pd.DataFrame:
    """Transform NL data to core + extension schema."""
    if input_path is None:
        input_path = INTERMEDIATE_DIR / "nl_school_master_geocoded.csv"

    logger.info(f"Loading NL data from {input_path.name}...")
    df = pd.read_csv(input_path, low_memory=False)
    logger.info(f"  {len(df)} schools, {len(df.columns)} columns")

    # Get full schema (core + NL extension)
    full_schema = get_full_schema("NL")
    output = pd.DataFrame(columns=full_schema)

    # === COUNTRY METADATA ===
    output["country_code"] = "NL"
    output["country_name"] = "Netherlands"
    output["city"] = df["city"]
    output["language"] = "nl"
    output["currency"] = "EUR"
    output["data_source_version"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    # === IDENTITY ===
    output["school_id"] = df["vestiging_code"]
    output["school_id_national"] = df["vestiging_code"]
    output["school_name"] = df["school_name"]

    # Map school type
    output["school_type_national"] = df["education_type"]
    output["school_type"] = df["education_type"].map(SCHOOL_TYPE_MAP).fillna("secondary")
    output["school_subtype"] = df["education_type"]

    # Ownership
    output["ownership_national"] = df["denomination"]
    output["ownership"] = df["denomination"].map(DENOMINATION_TO_OWNERSHIP).fillna("public")

    output["street_address"] = df["street_address"]
    output["postal_code"] = df["postal_code"]
    output["district"] = df["gemeente_name"]
    output["region"] = df["province"]
    output["phone"] = df["phone"]
    output["website"] = df["website"]
    output["metadata_source"] = "DUO Open Onderwijsdata"

    # === GEO ===
    output["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    output["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # === STUDENT / TEACHER ===
    output["students_current"] = pd.to_numeric(df.get("students_2024_25"), errors="coerce")
    output["students_previous"] = pd.to_numeric(df.get("students_2023_24"), errors="coerce")
    output["teachers_current"] = pd.to_numeric(df.get("teachers_current"), errors="coerce")
    output["teachers_previous"] = pd.to_numeric(df.get("teachers_previous"), errors="coerce")

    teachers = output["teachers_current"].where(output["teachers_current"] > 0)
    output["student_teacher_ratio"] = (output["students_current"] / teachers).round(1)

    # === NORMALIZED ACADEMIC ===
    output["academic_performance_score"] = df.apply(compute_academic_score, axis=1)
    output["academic_data_source"] = "DUO Exam Results"

    # Year of data
    has_2025 = pd.to_numeric(df.get("exam_avg_overall_2024_25"), errors="coerce").notna()
    output["academic_data_year"] = np.where(has_2025, "2024-25", "2023-24")

    # Compute percentile within dataset
    scores = output["academic_performance_score"]
    output["academic_performance_percentile"] = scores.rank(pct=True).multiply(100).round(1)

    # Trend
    score_current = pd.to_numeric(df.get("exam_avg_overall_2024_25"), errors="coerce")
    score_prev = pd.to_numeric(df.get("exam_avg_overall_2023_24"), errors="coerce")
    output["academic_performance_trend"] = (score_current - score_prev).round(2)

    # CE-SE difference as value-added proxy
    ce = pd.to_numeric(df.get("exam_avg_ce_2024_25"), errors="coerce")
    se = pd.to_numeric(df.get("exam_avg_se_2024_25"), errors="coerce")
    output["academic_value_added"] = (ce - se).round(2)

    # === NL EXTENSION COLUMNS ===
    output["nl_exam_pass_rate"] = pd.to_numeric(df.get("exam_pass_rate_2024_25"), errors="coerce")
    output["nl_exam_avg_grade_ce"] = pd.to_numeric(df.get("exam_avg_ce_2024_25"), errors="coerce")
    output["nl_exam_avg_grade_se"] = pd.to_numeric(df.get("exam_avg_se_2024_25"), errors="coerce")
    output["nl_exam_ce_se_difference"] = output["nl_exam_avg_grade_ce"] - output["nl_exam_avg_grade_se"]
    output["nl_exam_year"] = "2024-25"
    output["nl_denomination"] = df["denomination"]
    output["nl_brin_code"] = df["brin_code"]
    output["nl_gemeente_code"] = df["gemeente_code"]

    # Ensure all schema columns exist
    for col in full_schema:
        if col not in output.columns:
            output[col] = None

    output = output[full_schema]

    logger.info(f"\n  Output: {len(output)} schools, {len(output.columns)} columns")
    return output


def main():
    """Run the schema transformation."""
    logger.info("=" * 60)
    logger.info("NL Schema Transform: DUO data → Core + NL Extension Schema")
    logger.info("=" * 60)

    output = transform()

    # Save
    parquet_path = FINAL_DIR / "nl_school_master_table_final.parquet"
    csv_path = FINAL_DIR / "nl_school_master_table_final.csv"

    output.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    output.to_csv(csv_path, index=False)
    logger.info(f"Saved: {csv_path}")

    # Coverage report
    schema_coverage_report(output)

    return output


if __name__ == "__main__":
    main()
