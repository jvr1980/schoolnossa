#!/usr/bin/env python3
"""
Phase 8: Dresden Embeddings Generator

Generates text embeddings for school descriptions and creates final output files.
Reference: scripts_nrw/processing/nrw_embeddings_generator.py

Supports:
- OpenAI text-embedding-3-large (3072-dim) — primary
- Skip mode via SKIP_EMBEDDINGS env var or --skip-embeddings flag

Input: data_dresden/final/dresden_school_master_table_final.csv
Output:
  - data_dresden/final/dresden_school_master_table_final.csv (updated with embeddings)
  - data_dresden/final/dresden_school_master_table_final.parquet (updated)
"""

import pandas as pd
import numpy as np
import logging
import os
import time
import json
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_dresden"
FINAL_DIR = DATA_DIR / "final"


def create_school_text(row: pd.Series) -> str:
    """Create a text description of a school for embedding."""
    parts = []

    name = row.get('schulname', 'Unknown School')
    parts.append(f"{name}")

    if pd.notna(row.get('school_type_name')):
        parts.append(f"Type: {row['school_type_name']}")

    if pd.notna(row.get('traegerschaft')):
        parts.append(f"Operator: {row['traegerschaft']}")

    if pd.notna(row.get('ort')):
        parts.append(f"Location: {row['ort']}")

    if pd.notna(row.get('plz')):
        parts.append(f"PLZ: {row['plz']}")

    if pd.notna(row.get('transit_accessibility_score')):
        parts.append(f"Transit score: {row['transit_accessibility_score']:.0f}")

    if pd.notna(row.get('traffic_accidents_per_year')):
        parts.append(f"Traffic accidents/year: {row['traffic_accidents_per_year']:.1f}")

    if pd.notna(row.get('crime_rate_per_100k')):
        parts.append(f"Crime rate: {row['crime_rate_per_100k']:.0f}/100k")

    if pd.notna(row.get('description')):
        parts.append(str(row['description'])[:500])
    elif pd.notna(row.get('description_de')):
        parts.append(str(row['description_de'])[:500])

    return ". ".join(parts)


def generate_openai_embeddings(texts, model="text-embedding-3-large", batch_size=100):
    """Generate embeddings using OpenAI API."""
    try:
        import openai
        client = openai.OpenAI()
    except (ImportError, Exception) as e:
        logger.error(f"OpenAI not available: {e}")
        return None

    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        logger.info(f"Embedding batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")

        try:
            response = client.embeddings.create(input=batch, model=model)
            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Embedding batch failed: {e}")
            all_embeddings.extend([None] * len(batch))

    return all_embeddings


def main():
    logger.info("=" * 60)
    logger.info("Starting Dresden Embeddings Generator")
    logger.info("=" * 60)

    skip_embeddings = os.environ.get('SKIP_EMBEDDINGS', '0') == '1'

    input_file = FINAL_DIR / "dresden_school_master_table_final.csv"
    if not input_file.exists():
        raise FileNotFoundError(f"Final table not found: {input_file}")

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} schools")

    # Create text for embeddings
    df['embedding_text'] = df.apply(create_school_text, axis=1)

    if skip_embeddings:
        logger.info("Skipping embedding generation (SKIP_EMBEDDINGS=1)")
    else:
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set — skipping embeddings")
        else:
            texts = df['embedding_text'].tolist()
            embeddings = generate_openai_embeddings(texts)

            if embeddings:
                df['embedding'] = [json.dumps(e) if e else None for e in embeddings]
                valid = sum(1 for e in embeddings if e is not None)
                logger.info(f"Generated embeddings for {valid}/{len(df)} schools")

    # Save final outputs
    csv_path = FINAL_DIR / "dresden_school_master_table_final.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")

    parquet_path = FINAL_DIR / "dresden_school_master_table_final.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    print(f"\n{'='*70}")
    print("DRESDEN EMBEDDINGS GENERATOR - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(df)}")
    if 'embedding' in df.columns:
        print(f"Embeddings: {df['embedding'].notna().sum()}/{len(df)}")
    else:
        print("Embeddings: skipped")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
