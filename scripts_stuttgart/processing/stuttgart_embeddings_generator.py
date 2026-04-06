#!/usr/bin/env python3
"""
Stuttgart School Embeddings Generator
Generates text descriptions and Gemini embeddings, computes similar schools.

Uses Gemini gemini-embedding-001 (768d) with batch support.
API key loaded from config.yaml.

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import pandas as pd
import numpy as np
import logging
import os
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
FINAL_DIR = DATA_DIR / "final"
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
ENV_FILE = PROJECT_ROOT / ".env"

# Load API keys from config.yaml and .env
try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)
except ImportError:
    pass

try:
    import yaml
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            cfg = yaml.safe_load(f) or {}
        api_keys = cfg.get("api_keys", {})
        _key_map = {
            "GEMINI_API_KEY": api_keys.get("gemini", ""),
            "OPENAI_API_KEY": api_keys.get("openai", ""),
        }
        for env_var, val in _key_map.items():
            if val and env_var not in os.environ:
                os.environ[env_var] = val
except Exception:
    pass

GEMINI_MODEL = "models/gemini-embedding-001"


def load_master_table(school_type):
    for ext, reader in [('.parquet', pd.read_parquet), ('.csv', pd.read_csv)]:
        p = FINAL_DIR / f"stuttgart_{school_type}_school_master_table{ext}"
        if p.exists():
            return reader(p)
    raise FileNotFoundError(f"No master table for {school_type}")


def create_description(row):
    """Create a text description for embedding."""
    parts = []
    name = row.get('schulname', 'Unbekannte Schule')
    schulart = row.get('schulart', row.get('school_type', ''))
    parts.append(f"{name} ist eine {schulart} in Stuttgart.")

    strasse = row.get('strasse', '')
    plz = row.get('plz', '')
    if pd.notna(strasse) and strasse and pd.notna(plz):
        parts.append(f"Adresse: {strasse}, {plz} Stuttgart.")

    ortsteil = row.get('ortsteil', '')
    if pd.notna(ortsteil) and ortsteil:
        parts.append(f"Stadtbezirk: {ortsteil}.")

    traeger = row.get('traegerschaft', '')
    if pd.notna(traeger):
        if 'privat' in str(traeger).lower():
            parts.append("Die Schule ist in privater Trägerschaft.")
        elif 'öffentlich' in str(traeger).lower():
            parts.append("Die Schule ist in öffentlicher Trägerschaft.")

    ts = row.get('transit_accessibility_score')
    if pd.notna(ts):
        if ts >= 80:
            parts.append("Hervorragende ÖPNV-Anbindung.")
        elif ts >= 60:
            parts.append("Gute ÖPNV-Anbindung.")
        elif ts >= 40:
            parts.append("Mittlere ÖPNV-Anbindung.")
        else:
            parts.append("Eingeschränkte ÖPNV-Anbindung.")

    apy = row.get('traffic_accidents_per_year')
    if pd.notna(apy) and apy > 0:
        if apy >= 20:
            parts.append(f"Erhöhtes Verkehrsunfallaufkommen ({apy:.0f}/Jahr).")
        elif apy >= 10:
            parts.append(f"Mittleres Verkehrsunfallaufkommen ({apy:.0f}/Jahr).")

    crime_idx = row.get('crime_bezirk_index')
    if pd.notna(crime_idx):
        if crime_idx <= 0.7:
            parts.append("Niedrige Kriminalitätsbelastung im Bezirk.")
        elif crime_idx >= 1.3:
            parts.append("Überdurchschnittliche Kriminalitätsbelastung im Bezirk.")

    return " ".join(parts)


def generate_embeddings_gemini(df):
    """Generate embeddings using Gemini gemini-embedding-001."""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        logger.error("GEMINI_API_KEY not found in environment or config.yaml")
        return None

    logger.info(f"Generating embeddings with Gemini ({GEMINI_MODEL})...")
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        embeddings = []
        descs = df['description'].tolist()
        batch_size = 20

        for i in range(0, len(descs), batch_size):
            batch = descs[i:i + batch_size]
            logger.info(f"  Batch {i + 1}-{min(i + batch_size, len(descs))}/{len(descs)}")

            valid = [(j, d) for j, d in enumerate(batch) if d and str(d).strip()]
            batch_embs = [None] * len(batch)

            if valid:
                try:
                    result = genai.embed_content(
                        model=GEMINI_MODEL,
                        content=[d for _, d in valid],
                        task_type="RETRIEVAL_DOCUMENT"
                    )
                    for (j, _), emb in zip(valid, result['embedding']):
                        batch_embs[j] = emb
                except Exception as e:
                    logger.warning(f"Batch failed: {e}")

            embeddings.extend(batch_embs)

            if i + batch_size < len(descs):
                time.sleep(0.5)

        df['embedding'] = embeddings
        count = sum(1 for e in embeddings if e is not None)
        logger.info(f"Generated {count}/{len(df)} Gemini embeddings")
        return df

    except ImportError:
        logger.error("google-generativeai not installed. Run: pip install google-generativeai")
        return None


def compute_similar_schools(df, top_n=3):
    """Compute most similar schools by cosine similarity."""
    logger.info("Computing similar schools...")
    df = df.copy()

    for i in range(1, top_n + 1):
        df[f'most_similar_school_{i:02d}'] = None

    if 'embedding' not in df.columns:
        return df

    valid = df['embedding'].apply(lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0)
    if not valid.any():
        return df

    matrix, indices = [], []
    for idx, row in df.iterrows():
        if valid[idx]:
            matrix.append(row['embedding'])
            indices.append(idx)

    if len(matrix) < 2:
        return df

    matrix = np.array(matrix)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    normalized = matrix / norms
    sim = np.dot(normalized, normalized.T)

    for i, idx in enumerate(indices):
        s = sim[i].copy()
        s[i] = -1
        top = np.argsort(s)[-top_n:][::-1]
        for rank, si in enumerate(top):
            df.at[idx, f'most_similar_school_{rank + 1:02d}'] = df.at[indices[si], 'schulnummer']

    logger.info(f"Computed similarities for {len(indices)} schools")
    return df


def process_school_type(school_type):
    """Process one school type: descriptions → embeddings → similarity → save."""
    df = load_master_table(school_type)
    logger.info(f"Loaded {len(df)} {school_type} schools")

    # Generate descriptions
    df['description'] = df.apply(create_description, axis=1)
    desc_count = df['description'].notna().sum()
    logger.info(f"Generated {desc_count} descriptions")

    # Generate embeddings (Gemini only)
    result = generate_embeddings_gemini(df)
    if result is None:
        logger.warning("Embedding generation failed — saving without embeddings")
        df['embedding'] = None
    else:
        df = result

    # Compute similar schools
    df = compute_similar_schools(df)

    # Save
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    pq = FINAL_DIR / f"stuttgart_{school_type}_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(pq, index=False)

    csv_df = df.drop(columns=['embedding'], errors='ignore')
    csv_path = FINAL_DIR / f"stuttgart_{school_type}_school_master_table_final.csv"
    csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    logger.info(f"Saved: {pq}")

    # Summary
    print(f"\n{'=' * 70}")
    print(f"STUTTGART {school_type.upper()} EMBEDDINGS - COMPLETE")
    print(f"{'=' * 70}")
    print(f"Schools: {len(df)}, Columns: {len(df.columns)}")
    for col, label in {'description': 'Descriptions', 'embedding': 'Embeddings',
                        'most_similar_school_01': 'Similar schools'}.items():
        if col in df.columns:
            if col == 'embedding':
                n = df[col].apply(lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0).sum()
            else:
                n = df[col].notna().sum()
            print(f"  {label}: {n}/{len(df)} ({100 * n / len(df):.0f}%)")
    print(f"{'=' * 70}")

    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Stuttgart Embeddings Generator (Gemini)")
    logger.info("=" * 60)
    for st in ['primary', 'secondary']:
        try:
            process_school_type(st)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
