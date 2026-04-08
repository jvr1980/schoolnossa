#!/usr/bin/env python3
"""
Phase 9: Enforce Berlin Schema on Leipzig Data
================================================

Transforms Leipzig school data to match the Berlin schema exactly,
enabling use with the same frontend application.

Adapted from NRW/Hamburg schema transformers.

Transformations:
1. Rename Leipzig columns to their Berlin equivalents
2. Map Leipzig crime data (Ortsteil-level) to Berlin crime columns
3. Map Leipzig-specific fields (Ortsteil demographics, student counts)
4. Set constant fields (metadata_source, leistungsdaten_quelle)
5. Ensure all Berlin columns exist (add as NULL if missing)
6. Order: Berlin columns first (exact order), then Leipzig-specific extras
7. Assert all Berlin columns present
8. Overwrite the final files

Input: data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet
Reference: data_berlin/final/school_master_table_final_with_embeddings.parquet
Output: overwrites input with Berlin-aligned schema

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
LEIPZIG_DATA_DIR = PROJECT_ROOT / "data_leipzig" / "final"

# Berlin reference schema (the ground truth)
BERLIN_SEC_REF = PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
BERLIN_PRI_REF = PROJECT_ROOT / "data_berlin_primary" / "final" / "grundschule_master_table_final_with_embeddings.parquet"


def get_berlin_schema(school_type: str = "secondary") -> list:
    """Load the exact column list from the Berlin reference parquet."""
    if school_type == "secondary":
        ref_path = BERLIN_SEC_REF
    else:
        ref_path = BERLIN_PRI_REF

    if not ref_path.exists():
        raise FileNotFoundError(f"Berlin reference not found: {ref_path}")

    berlin = pd.read_parquet(ref_path)
    return list(berlin.columns)


def main():
    """Transform Leipzig data to match Berlin schema."""
    raise NotImplementedError(
        "Phase 9: Leipzig schema transformer not yet implemented.\n"
        "TODO: Adapt scripts_nrw/nrw_to_berlin_schema.py\n"
        "  - Map Leipzig column names to Berlin equivalents\n"
        "  - Map Ortsteil crime data to Berlin crime columns\n"
        "  - Ensure all Berlin columns present, order matches\n"
        "  - Keep Leipzig-specific extras appended at end"
    )


if __name__ == "__main__":
    main()
