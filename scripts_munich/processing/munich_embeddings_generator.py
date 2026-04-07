#!/usr/bin/env python3
"""
Phase 8: Munich Embeddings Generator

Generates text embeddings for school descriptions and creates final parquet.
Uses OpenAI text-embedding-3-large (3072-dim) with Gemini fallback.

Input: data_munich/final/munich_secondary_school_master_table.csv
Output: data_munich/final/munich_secondary_school_master_table_final.csv
        data_munich/final/munich_secondary_school_master_table_final_with_embeddings.parquet

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import pandas as pd
import numpy as np
import os
import json
import logging
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
FINAL_DIR = DATA_DIR / "final"

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_AI_API_KEY')
SKIP_EMBEDDINGS = os.getenv('SKIP_EMBEDDINGS', '0') == '1'

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 3072


def create_text_for_embedding(row):
    """Create text representation of a school for embedding."""
    parts = []

    name = str(row.get('schulname', '')).strip()
    if name and name != 'nan':
        parts.append(f"Schule: {name}")

    school_type = str(row.get('school_type', row.get('schulart', ''))).strip()
    if school_type and school_type != 'nan':
        parts.append(f"Schulart: {school_type}")

    ort = str(row.get('ort', 'München')).strip()
    parts.append(f"Stadt: {ort}")

    desc = str(row.get('description_de', '')).strip()
    if desc and desc != 'nan':
        parts.append(f"Beschreibung: {desc}")

    # Transit score
    transit = row.get('transit_accessibility_score')
    if pd.notna(transit):
        parts.append(f"ÖPNV-Anbindung: {transit}/100")

    # Traffic safety
    traffic = row.get('traffic_safety_score')
    if pd.notna(traffic):
        parts.append(f"Verkehrssicherheit: {traffic}/100")

    return ' | '.join(parts) if parts else name


def generate_openai_embeddings(texts, batch_size=100):
    """Generate embeddings using OpenAI API."""
    import requests

    embeddings = [None] * len(texts)
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        try:
            resp = requests.post(
                "https://api.openai.com/v1/embeddings",
                headers=headers,
                json={"model": EMBEDDING_MODEL, "input": batch},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            for item in data:
                embeddings[i + item["index"]] = item["embedding"]
            logger.info(f"  Embedded batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"OpenAI embedding error: {e}")

    return embeddings


def generate_gemini_embeddings(texts):
    """Fallback: generate embeddings using Gemini API."""
    import requests

    embeddings = []
    for text in texts:
        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={GEMINI_API_KEY}",
                json={"content": {"parts": [{"text": text}]}},
                timeout=30,
            )
            resp.raise_for_status()
            emb = resp.json()["embedding"]["values"]
            embeddings.append(emb)
            time.sleep(0.2)
        except Exception as e:
            logger.debug(f"Gemini embedding error: {e}")
            embeddings.append(None)

    return embeddings


def generate_for_school_type(school_type='secondary'):
    logger.info(f"Generating embeddings for {school_type} schools...")

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    input_path = FINAL_DIR / f"munich_{school_type}_school_master_table.csv"
    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}. Run Phase 7 first.")

    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} schools")

    final_csv = FINAL_DIR / f"munich_{school_type}_school_master_table_final.csv"
    df.to_csv(final_csv, index=False, encoding='utf-8-sig')
    logger.info(f"Saved final CSV: {final_csv}")

    if SKIP_EMBEDDINGS:
        logger.info("Skipping embedding generation (SKIP_EMBEDDINGS=1)")
        final_parquet = FINAL_DIR / f"munich_{school_type}_school_master_table_final_with_embeddings.parquet"
        df.to_parquet(final_parquet, index=False)
        logger.info(f"Saved parquet (no embeddings): {final_parquet}")
        return df

    texts = [create_text_for_embedding(row) for _, row in df.iterrows()]
    logger.info(f"Created embedding texts for {len(texts)} schools")

    embeddings = None
    if OPENAI_API_KEY:
        logger.info("Generating embeddings with OpenAI...")
        embeddings = generate_openai_embeddings(texts)
    elif GEMINI_API_KEY:
        logger.info("Generating embeddings with Gemini (fallback)...")
        embeddings = generate_gemini_embeddings(texts)
    else:
        logger.warning("No API key available for embeddings. Set OPENAI_API_KEY or GEMINI_API_KEY.")
        embeddings = [None] * len(df)

    df['embedding'] = embeddings

    final_parquet = FINAL_DIR / f"munich_{school_type}_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(final_parquet, index=False)
    logger.info(f"Saved parquet with embeddings: {final_parquet}")

    embedded_count = sum(1 for e in embeddings if e is not None)
    print(f"\n{'='*70}")
    print(f"MUNICH EMBEDDINGS GENERATOR ({school_type.upper()}) - COMPLETE")
    print(f"{'='*70}")
    print(f"Schools: {len(df)}")
    print(f"Embeddings: {embedded_count}/{len(df)}")
    print(f"Output: {final_parquet.name}")
    print(f"{'='*70}")

    return df


def main(school_type='secondary'):
    logger.info("=" * 60)
    logger.info(f"Phase 8: Munich Embeddings Generator ({school_type})")
    logger.info("=" * 60)
    return generate_for_school_type(school_type)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
