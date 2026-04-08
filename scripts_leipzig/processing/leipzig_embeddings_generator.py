#!/usr/bin/env python3
"""
Phase 8: Leipzig Embeddings Generator
=======================================

Generates embeddings for Leipzig schools and creates the final parquet output.
Adapted from NRW embeddings generator (OpenAI with Gemini fallback).

This script:
1. Loads the combined master table
2. Constructs text representations for each school
3. Generates embeddings via OpenAI text-embedding-3-large (or Gemini fallback)
4. Saves final CSV + parquet with embeddings

Input: data_leipzig/final/leipzig_school_master_table_final.csv
Output:
    data_leipzig/final/leipzig_school_master_table_final.csv (updated)
    data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet

Author: Leipzig School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_leipzig"
FINAL_DIR = DATA_DIR / "final"


def main():
    """Generate embeddings for Leipzig schools."""
    raise NotImplementedError(
        "Phase 8: Leipzig embeddings generator not yet implemented.\n"
        "TODO: Adapt scripts_nrw/processing/nrw_embeddings_generator.py\n"
        "  - Same OpenAI embedding pattern\n"
        "  - SKIP_EMBEDDINGS env var support\n"
        "  - Save parquet with embedding column"
    )


if __name__ == "__main__":
    main()
