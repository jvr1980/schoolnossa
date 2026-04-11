#!/usr/bin/env python3
"""
Transform UK school data to SchoolNossa core + GB extension schema.

Maps GIAS + DfE + police.uk + STATS19 + NaPTAN data to the universal core
schema plus GB-specific extension columns (KS4/KS5, Ofsted, IMD).

Input:  data_gb/intermediate/gb_schools_with_*.csv (most enriched available)
Output: data_gb/final/gb_school_master_table_final.parquet
        data_gb/final/gb_school_master_table_berlin_schema.parquet
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.schema.core_schema import CORE_COLUMNS, schema_coverage_report
from scripts_shared.schema.country_extensions import get_full_schema, GB_EXTENSION

DATA_DIR = PROJECT_ROOT / "data_gb"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"
FINAL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Establishment type → ownership mapping
OWNERSHIP_MAP = {
    "Community school": "public",
    "Foundation school": "public",
    "Voluntary aided school": "public",
    "Voluntary controlled school": "public",
    "Academy sponsor led": "public",
    "Academy converter": "public",
    "Free schools": "public",
    "Studio schools": "public",
    "University technical college": "public",
    "City technology college": "private",
    "Other independent school": "private",
    "Non-maintained special school": "private",
}

# Ofsted → quality rating mapping
OFSTED_MAP = {
    "Outstanding": "excellent",
    "Good": "good",
    "Requires improvement": "adequate",
    "Inadequate": "inadequate",
    "Serious Weaknesses": "inadequate",
    "Special Measures": "inadequate",
}


def compute_academic_score(row) -> float:
    """
    Compute normalized 0-100 score from UK Attainment 8.
    Attainment 8 typically ranges 0-90 (national avg ~46).
    Map: score = (att8 / 90) * 100, capped at 100.
    """
    att8 = row.get("ks4_attainment8")
    if pd.notna(att8):
        return round(min(100, float(att8) / 90 * 100), 1)
    return np.nan


def find_best_input() -> Path:
    candidates = [
        INTERMEDIATE_DIR / "gb_schools_with_crime.csv",
        INTERMEDIATE_DIR / "gb_schools_with_transit.csv",
        INTERMEDIATE_DIR / "gb_schools_with_traffic.csv",
        INTERMEDIATE_DIR / "gb_school_master_base.csv",
    ]
    return next((p for p in candidates if p.exists()), None)


def transform(input_path: Path = None) -> pd.DataFrame:
    if input_path is None:
        input_path = find_best_input()
    if input_path is None:
        raise FileNotFoundError("No intermediate data found.")

    logger.info(f"Loading from {input_path.name}...")
    df = pd.read_csv(input_path, low_memory=False)
    logger.info(f"  {len(df)} schools, {len(df.columns)} columns")

    full_schema = get_full_schema("GB")
    output = pd.DataFrame(columns=full_schema)

    # === COUNTRY METADATA ===
    output["country_code"] = "GB"
    output["country_name"] = "United Kingdom"
    output["city"] = df["town"]
    output["language"] = "en"
    output["currency"] = "GBP"
    output["data_source_version"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    # === IDENTITY ===
    output["school_id"] = df["urn"].astype(str)
    output["school_id_national"] = df["urn"].astype(str)
    output["school_name"] = df["school_name"]
    output["school_type"] = "secondary"
    output["school_type_national"] = df["establishment_type"]
    output["school_subtype"] = df.get("phase", "Secondary")
    output["ownership"] = df["establishment_type"].map(OWNERSHIP_MAP).fillna("public")
    output["ownership_national"] = df["establishment_type"]
    output["street_address"] = df["street"]
    output["postal_code"] = df["postcode"]
    output["district"] = df["town"]
    output["region"] = df["local_authority"]
    output["phone"] = df["phone"]
    output["website"] = df["website"]
    output["principal"] = df.get("principal")
    output["metadata_source"] = "GIAS + DfE"

    # === GEO ===
    output["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    output["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")

    # === STUDENT/TEACHER ===
    output["students_current"] = pd.to_numeric(df.get("number_of_pupils"), errors="coerce")

    # === ACADEMIC ===
    output["academic_performance_score"] = df.apply(compute_academic_score, axis=1)
    scores = output["academic_performance_score"]
    output["academic_performance_percentile"] = scores.rank(pct=True).multiply(100).round(1)
    output["academic_data_source"] = "DfE KS4 Performance Tables"

    # Quality rating from Ofsted
    output["school_quality_rating"] = df.get("ofsted_rating", pd.Series(dtype=str)).map(OFSTED_MAP)
    output["school_quality_rating_national"] = df.get("ofsted_rating")

    # === SOCIOECONOMIC ===
    if "imd_decile" in df.columns:
        output["deprivation_index"] = pd.to_numeric(df["imd_decile"], errors="coerce")
        output["deprivation_index_national"] = df["imd_decile"].astype(str) + " (IMD decile)"
    if "fsm_pct" in df.columns:
        output["migration_background_pct"] = pd.to_numeric(df["fsm_pct"], errors="coerce")  # FSM as proxy

    # === TRAFFIC ===
    for col in ["traffic_accidents_500m", "traffic_accidents_1000m",
                 "traffic_accidents_fatal_1000m", "traffic_volume_index", "traffic_data_source"]:
        if col in df.columns:
            output[col] = df[col]

    # === CRIME ===
    for col in ["crime_total_per_1000", "crime_violent_per_1000", "crime_property_per_1000",
                 "crime_drug_per_1000", "crime_safety_rank", "crime_safety_category",
                 "crime_data_source", "crime_data_year"]:
        if col in df.columns:
            output[col] = df[col]

    # === TRANSIT ===
    for col in df.columns:
        if col.startswith("transit_") and col in full_schema:
            output[col] = df[col]

    # === GB EXTENSION ===
    output["gb_urn"] = df["urn"].astype(str)
    output["gb_establishment_type"] = df["establishment_type"]
    output["gb_phase"] = df.get("phase")
    output["gb_local_authority"] = df["local_authority"]
    output["gb_trust_name"] = df.get("trust_name")
    output["gb_lsoa_code"] = df.get("lsoa_code")
    output["gb_ofsted_overall"] = df.get("ofsted_rating")
    output["gb_ofsted_date"] = df.get("ofsted_date")

    if "ks4_attainment8" in df.columns:
        output["gb_ks4_attainment8"] = pd.to_numeric(df["ks4_attainment8"], errors="coerce")
    if "ks4_progress8" in df.columns:
        output["gb_ks4_progress8"] = pd.to_numeric(df["ks4_progress8"], errors="coerce")
    if "imd_decile" in df.columns:
        output["gb_imd_decile"] = pd.to_numeric(df["imd_decile"], errors="coerce")
    if "fsm_pct" in df.columns:
        output["gb_fsm_pct"] = pd.to_numeric(df["fsm_pct"], errors="coerce")

    # Ensure all columns present
    for col in full_schema:
        if col not in output.columns:
            output[col] = None
    output = output[full_schema]

    logger.info(f"  Output: {len(output)} schools, {len(output.columns)} columns")
    return output


def main():
    logger.info("=" * 60)
    logger.info("UK Schema Transform: GIAS+DfE → Core + GB Extension")
    logger.info("=" * 60)

    output = transform()

    parquet_path = FINAL_DIR / "gb_school_master_table_final.parquet"
    csv_path = FINAL_DIR / "gb_school_master_table_final.csv"
    output.to_parquet(parquet_path, index=False)
    output.to_csv(csv_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    # Berlin-compatible output
    try:
        from scripts_international.international_to_berlin_schema import transform_to_berlin
        berlin_df = transform_to_berlin(output, "GB")
        berlin_path = FINAL_DIR / "gb_school_master_table_berlin_schema.parquet"
        berlin_df.to_parquet(berlin_path, index=False)
        populated = sum(1 for c in berlin_df.columns if berlin_df[c].notna().any())
        logger.info(f"Berlin schema: {populated}/{len(berlin_df.columns)} cols with data")
    except Exception as e:
        logger.warning(f"Berlin transform failed: {e}")

    schema_coverage_report(output)
    return output


if __name__ == "__main__":
    main()
