#!/usr/bin/env python3
"""
Frankfurt School Embeddings Generator
Generates text embeddings and creates the final master parquet file.

Supports OpenAI text-embedding-3-large (3072d) or Gemini gemini-embedding-001 (768d).

Author: Frankfurt School Data Pipeline
Created: 2026-03-30
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
DATA_DIR = PROJECT_ROOT / "data_frankfurt"
FINAL_DIR = DATA_DIR / "final"
ENV_FILE = PROJECT_ROOT / ".env"
CONFIG_FILE = PROJECT_ROOT / "config.yaml"

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, _, v = line.partition('=')
                    if k.strip() and k.strip() not in os.environ:
                        os.environ[k.strip()] = v.strip()

# Fallback: load API keys from config.yaml if not already in environment
try:
    import yaml
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            cfg = yaml.safe_load(f) or {}
        api_keys = cfg.get("api_keys", {})
        _key_map = {
            "GEMINI_API_KEY":     api_keys.get("gemini", ""),
            "OPENAI_API_KEY":     api_keys.get("openai", ""),
            "GOOGLE_PLACES_KEY":  api_keys.get("google_places", ""),
        }
        for env_var, val in _key_map.items():
            if val and env_var not in os.environ:
                os.environ[env_var] = val
except Exception:
    pass

OPENAI_MODEL = "text-embedding-3-large"
GEMINI_MODEL = "models/gemini-embedding-001"


def load_master_table(school_type):
    for ext, reader in [('.parquet', pd.read_parquet), ('.csv', pd.read_csv)]:
        p = FINAL_DIR / f"frankfurt_{school_type}_school_master_table{ext}"
        if p.exists():
            return reader(p)
    raise FileNotFoundError(f"No master table for {school_type}")


def create_description(row):
    parts = []
    name = row.get('schulname', 'Unbekannte Schule')
    stype = row.get('school_type', '')
    stadt = row.get('stadt', 'Frankfurt am Main')
    parts.append(f"{name} ist eine {stype} in {stadt}.")

    strasse = row.get('strasse', '')
    plz = row.get('plz', '')
    if pd.notna(strasse) and strasse and pd.notna(plz):
        parts.append(f"Adresse: {strasse}, {plz} {stadt}.")

    ndh = row.get('ndh_count')
    total = row.get('schueler_gesamt')
    if pd.notna(ndh) and pd.notna(total) and total > 0:
        pct = ndh / total * 100
        parts.append(f"{pct:.0f}% der Schüler haben nichtdeutsche Herkunftssprache.")

    traeger = row.get('traegerschaft', '')
    if pd.notna(traeger):
        if 'privat' in str(traeger).lower():
            parts.append("Die Schule ist in privater Trägerschaft.")
        elif 'öffentlich' in str(traeger).lower():
            parts.append("Die Schule ist in öffentlicher Trägerschaft.")

    ts = row.get('transit_accessibility_score')
    if pd.notna(ts):
        if ts >= 80: parts.append("Hervorragende ÖPNV-Anbindung.")
        elif ts >= 60: parts.append("Gute ÖPNV-Anbindung.")
        elif ts >= 40: parts.append("Mittlere ÖPNV-Anbindung.")
        else: parts.append("Eingeschränkte ÖPNV-Anbindung.")

    apy = row.get('traffic_accidents_per_year')
    if pd.notna(apy) and apy > 0:
        if apy >= 20: parts.append(f"Erhöhtes Verkehrsunfallaufkommen ({apy:.0f}/Jahr).")
        elif apy >= 10: parts.append(f"Mittleres Verkehrsunfallaufkommen ({apy:.0f}/Jahr).")

    return " ".join(parts)


def generate_embeddings_openai(df):
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        return None
    logger.info(f"Generating embeddings with OpenAI ({OPENAI_MODEL})...")
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        embeddings = []
        for idx, row in df.iterrows():
            if idx % 20 == 0:
                logger.info(f"  {idx+1}/{len(df)}")
            desc = row.get('description', '')
            if not desc:
                embeddings.append(None)
                continue
            try:
                resp = client.embeddings.create(model=OPENAI_MODEL, input=desc)
                embeddings.append(resp.data[0].embedding)
            except Exception as e:
                logger.warning(f"Embedding failed {idx}: {e}")
                embeddings.append(None)
        df['embedding'] = embeddings
        count = sum(1 for e in embeddings if e is not None)
        logger.info(f"Generated {count}/{len(df)} OpenAI embeddings")
        return df
    except ImportError:
        return None


def generate_embeddings_gemini(df):
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None
    logger.info(f"Generating embeddings with Gemini ({GEMINI_MODEL})...")
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        embeddings = []
        descs = df['description'].tolist()
        bs = 20
        for i in range(0, len(descs), bs):
            batch = descs[i:i+bs]
            logger.info(f"  Batch {i+1}-{min(i+bs, len(descs))}/{len(descs)}")
            valid = [(j, d) for j, d in enumerate(batch) if d and str(d).strip()]
            batch_embs = [None] * len(batch)
            if valid:
                try:
                    result = genai.embed_content(model=GEMINI_MODEL,
                                                  content=[d for _, d in valid],
                                                  task_type="RETRIEVAL_DOCUMENT")
                    for (j, _), emb in zip(valid, result['embedding']):
                        batch_embs[j] = emb
                except Exception as e:
                    logger.warning(f"Batch failed: {e}")
            embeddings.extend(batch_embs)
            if i + bs < len(descs):
                time.sleep(0.5)
        df['embedding'] = embeddings
        count = sum(1 for e in embeddings if e is not None)
        logger.info(f"Generated {count}/{len(df)} Gemini embeddings")
        return df
    except ImportError:
        return None


def generate_embeddings(df):
    skip = os.environ.get('SKIP_EMBEDDINGS', '').strip()
    if skip == '1':
        logger.info("Skipping embeddings (SKIP_EMBEDDINGS=1)")
        df['embedding'] = None
        return df
    result = generate_embeddings_openai(df)
    if result is not None:
        return result
    result = generate_embeddings_gemini(df)
    if result is not None:
        return result
    logger.warning("No embedding API key found. Skipping.")
    df['embedding'] = None
    return df


def compute_similar_schools(df, top_n=3):
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
            df.at[idx, f'most_similar_school_{rank+1:02d}'] = df.at[indices[si], 'schulnummer']

    return df


def process_school_type(school_type):
    df = load_master_table(school_type)
    df['description'] = df.apply(create_description, axis=1)
    df = generate_embeddings(df)
    df = compute_similar_schools(df)

    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    pq = FINAL_DIR / f"frankfurt_{school_type}_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(pq, index=False)

    csv_df = df.drop(columns=['embedding'], errors='ignore')
    csv_path = FINAL_DIR / f"frankfurt_{school_type}_school_master_table_final.csv"
    csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    logger.info(f"Saved: {pq}")

    print(f"\n{'='*70}\nFRANKFURT {school_type.upper()} FINAL OUTPUT\n{'='*70}")
    print(f"Schools: {len(df)}, Columns: {len(df.columns)}")
    for col, label in {'description': 'Descriptions', 'embedding': 'Embeddings',
                        'transit_accessibility_score': 'Transit', 'traffic_accidents_total': 'Traffic'}.items():
        if col in df.columns:
            if col == 'embedding':
                n = df[col].apply(lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0).sum()
            else:
                n = df[col].notna().sum()
            print(f"  {label}: {n}/{len(df)} ({100*n/len(df):.0f}%)")
    print(f"{'='*70}")
    return df


def main():
    logger.info("=" * 60)
    logger.info("Starting Frankfurt Embeddings Generator")
    logger.info("=" * 60)
    for st in ['secondary', 'primary']:
        try:
            process_school_type(st)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
