#!/usr/bin/env python3
"""
Phase 8: Bremen Embeddings Generator

Generates text embeddings for each school and creates the final parquet output.
Uses OpenAI text-embedding-3-large (3072-dim) with Gemini fallback.

This script:
1. Loads the combined master table
2. Creates text descriptions for each school (preserving rich descriptions)
3. Generates embeddings (OpenAI or Gemini fallback)
4. Computes similar schools
5. Outputs final parquet and CSV files

Input:
    - data_bremen/final/bremen_school_master_table_final.csv (or .parquet)

Output:
    - data_bremen/final/bremen_school_master_table_final_with_embeddings.parquet
    - data_bremen/final/bremen_school_master_table_final.csv (updated)

Reference: scripts_nrw/processing/nrw_embeddings_generator.py
Author: Bremen School Data Pipeline
Created: 2026-04-08
"""

import pandas as pd
import numpy as np
import logging
import os
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_bremen"
FINAL_DIR = DATA_DIR / "final"
ENV_FILE = PROJECT_ROOT / ".env"

# Load .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    key = key.strip()
                    value = value.strip()
                    if key and key not in os.environ:
                        os.environ[key] = value

# Also try config.yaml
def _load_keys_from_config():
    """Load API keys from config.yaml if present."""
    config_path = PROJECT_ROOT / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
            api_keys = config.get("api_keys", config)
            for env_key in ["OPENAI_API_KEY", "GEMINI_API_KEY"]:
                val = api_keys.get(env_key) or api_keys.get(env_key.lower())
                if val and env_key not in os.environ:
                    os.environ[env_key] = val
        except Exception:
            pass

_load_keys_from_config()

# Embedding settings
OPENAI_EMBEDDING_MODEL = "text-embedding-3-large"
GEMINI_EMBEDDING_MODEL = "models/gemini-embedding-001"


def find_input_file() -> Path:
    """Find the master table to embed."""
    candidates = [
        FINAL_DIR / "bremen_school_master_table_final.parquet",
        FINAL_DIR / "bremen_school_master_table_final.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"No master table found. Checked:\n" +
        "\n".join(f"  - {p}" for p in candidates)
    )


def load_master_table() -> pd.DataFrame:
    """Load the combined master table."""
    path = find_input_file()
    if path.suffix == '.parquet':
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} schools from {path.name}")
    return df


def create_school_description(row: pd.Series) -> str:
    """Create a text description for a school (used for embeddings)."""
    parts = []

    name = row.get('schulname', 'Unbekannte Schule')
    schulform = row.get('schulform', row.get('school_type', ''))
    stadt = row.get('stadt', 'Bremen')

    parts.append(f"{name} ist eine {schulform} in {stadt}.")

    # Location
    stadtteil = row.get('stadtteil', '')
    if pd.notna(stadtteil) and stadtteil:
        parts.append(f"Die Schule liegt im Stadtteil {stadtteil}.")

    strasse = row.get('strasse', '')
    plz = row.get('plz', '')
    if pd.notna(strasse) and strasse and pd.notna(plz) and plz:
        parts.append(f"Adresse: {strasse}, {plz} {stadt}.")

    # Operator
    traeger = row.get('traegerschaft', '')
    if pd.notna(traeger) and traeger:
        if 'privat' in str(traeger).lower():
            parts.append("Die Schule ist in privater Traegerschaft.")
        elif 'oeffentlich' in str(traeger).lower() or 'stadt' in str(traeger).lower():
            parts.append("Die Schule ist in oeffentlicher Traegerschaft.")

    # Besonderheiten
    besond = row.get('besonderheiten', '')
    if pd.notna(besond) and besond and str(besond) != 'nan':
        parts.append(f"Besonderheiten: {besond}.")

    # Transit
    transit_score = row.get('transit_accessibility_score')
    if pd.notna(transit_score):
        if transit_score >= 80:
            parts.append("Hervorragende Anbindung an den oeffentlichen Nahverkehr.")
        elif transit_score >= 60:
            parts.append("Gute Anbindung an den oeffentlichen Nahverkehr.")
        elif transit_score >= 40:
            parts.append("Mittlere Anbindung an den oeffentlichen Nahverkehr.")
        else:
            parts.append("Eingeschraenkte Anbindung an den oeffentlichen Nahverkehr.")

    # Traffic safety
    traffic_score = row.get('traffic_safety_score')
    if pd.notna(traffic_score):
        if traffic_score >= 80:
            parts.append("Sehr gute Verkehrssicherheit im Schulumfeld.")
        elif traffic_score >= 50:
            parts.append("Durchschnittliche Verkehrssicherheit im Schulumfeld.")

    # Crime safety
    crime_cat = row.get('crime_safety_category', '')
    if pd.notna(crime_cat) and crime_cat and str(crime_cat) != 'nan':
        parts.append(f"Sicherheitsbewertung: {crime_cat}.")

    return " ".join(parts)


def add_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """Add text descriptions. Preserves existing rich descriptions from website enrichment."""
    logger.info("Generating school descriptions...")
    df = df.copy()

    # Generate auto-descriptions for ALL schools
    df['description_auto'] = df.apply(create_school_description, axis=1)

    # Only overwrite 'description' if it's empty/missing
    if 'description' in df.columns:
        has_rich = df['description'].notna() & (
            df['description'].astype(str).str.len() > 200
        )
        rich_count = has_rich.sum()
        logger.info(f"  Preserving {rich_count} rich descriptions from website enrichment")
        df.loc[~has_rich, 'description'] = df.loc[~has_rich, 'description_auto']
    else:
        df['description'] = df['description_auto']

    logger.info(f"  Sample: {df['description'].iloc[0][:200]}...")
    return df


def generate_embeddings_openai(df: pd.DataFrame) -> pd.DataFrame:
    """Generate embeddings using OpenAI text-embedding-3-large (3072 dimensions)."""
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        return None

    logger.info(f"Generating embeddings with OpenAI ({OPENAI_EMBEDDING_MODEL})...")

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        embeddings = []
        total = len(df)

        for i, (idx, row) in enumerate(df.iterrows()):
            if i % 20 == 0:
                logger.info(f"  Embedding {i + 1}/{total}")

            description = row.get('description', '')
            if not description or str(description) == 'nan':
                embeddings.append(None)
                continue

            try:
                response = client.embeddings.create(
                    model=OPENAI_EMBEDDING_MODEL,
                    input=str(description)
                )
                embeddings.append(response.data[0].embedding)
            except Exception as e:
                logger.warning(f"  Embedding failed for school {i}: {e}")
                embeddings.append(None)

        df['embedding'] = embeddings
        count = sum(1 for e in embeddings if e is not None)
        dim = len(embeddings[0]) if embeddings and embeddings[0] else '?'
        logger.info(f"Generated {count}/{total} OpenAI embeddings ({dim} dimensions)")
        return df

    except ImportError:
        logger.warning("openai package not installed")
        return None
    except Exception as e:
        logger.error(f"OpenAI embedding generation failed: {e}")
        return None


def generate_embeddings_gemini(df: pd.DataFrame) -> pd.DataFrame:
    """Generate embeddings using Gemini text-embedding-004 (768 dimensions)."""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None

    logger.info(f"Generating embeddings with Gemini ({GEMINI_EMBEDDING_MODEL})...")

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        embeddings = []
        total = len(df)
        batch_size = 20

        descriptions = df['description'].tolist()

        for i in range(0, total, batch_size):
            batch = descriptions[i:i + batch_size]
            logger.info(f"  Embedding batch {i + 1}-{min(i + batch_size, total)}/{total}")

            valid_batch = []
            valid_indices = []
            for j, desc in enumerate(batch):
                if desc and str(desc).strip() and str(desc) != 'nan':
                    valid_batch.append(str(desc))
                    valid_indices.append(j)

            batch_embeddings = [None] * len(batch)

            if valid_batch:
                try:
                    result = genai.embed_content(
                        model=GEMINI_EMBEDDING_MODEL,
                        content=valid_batch,
                        task_type="RETRIEVAL_DOCUMENT"
                    )
                    for j, emb in zip(valid_indices, result['embedding']):
                        batch_embeddings[j] = emb
                except Exception as e:
                    logger.warning(f"  Batch embedding failed at {i}: {e}")
                    for j, desc in zip(valid_indices, valid_batch):
                        try:
                            result = genai.embed_content(
                                model=GEMINI_EMBEDDING_MODEL,
                                content=desc,
                                task_type="RETRIEVAL_DOCUMENT"
                            )
                            batch_embeddings[j] = result['embedding']
                        except Exception as e2:
                            logger.warning(f"  Individual embedding failed: {e2}")

            embeddings.extend(batch_embeddings)

            if i + batch_size < total:
                time.sleep(0.5)

        df['embedding'] = embeddings
        count = sum(1 for e in embeddings if e is not None)
        dim = len(embeddings[0]) if embeddings and embeddings[0] else '?'
        logger.info(f"Generated {count}/{total} Gemini embeddings ({dim} dimensions)")
        return df

    except ImportError:
        logger.warning("google-generativeai package not installed")
        return None
    except Exception as e:
        logger.error(f"Gemini embedding generation failed: {e}")
        return None


def generate_embeddings(df: pd.DataFrame) -> pd.DataFrame:
    """Generate embeddings using OpenAI (preferred) or Gemini (fallback)."""
    skip = os.environ.get('SKIP_EMBEDDINGS', '').strip()
    if skip == '1':
        logger.info("Skipping embedding generation (SKIP_EMBEDDINGS=1)")
        df['embedding'] = None
        return df

    # Try OpenAI first
    result = generate_embeddings_openai(df)
    if result is not None:
        return result

    # Fall back to Gemini
    logger.info("OpenAI not available, trying Gemini...")
    result = generate_embeddings_gemini(df)
    if result is not None:
        return result

    # Neither available
    logger.warning("No embedding API key found. Set OPENAI_API_KEY or GEMINI_API_KEY.")
    logger.warning("Skipping embedding generation.")
    df['embedding'] = None
    return df


def compute_similar_schools(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """Compute most similar schools based on embeddings."""
    logger.info("Computing similar schools...")
    df = df.copy()

    for i in range(1, top_n + 1):
        df[f'most_similar_school_{i:02d}'] = None

    if 'embedding' not in df.columns:
        return df

    valid = df['embedding'].apply(
        lambda x: x is not None and (isinstance(x, list) and len(x) > 0)
    )

    if not valid.any():
        return df

    matrix = []
    indices = []
    for idx, row in df.iterrows():
        if valid[idx]:
            matrix.append(row['embedding'])
            indices.append(idx)

    if len(matrix) < 2:
        return df

    matrix = np.array(matrix)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1  # avoid division by zero
    normalized = matrix / norms
    similarity = np.dot(normalized, normalized.T)

    for i, idx in enumerate(indices):
        sims = similarity[i].copy()
        sims[i] = -1  # exclude self
        top = np.argsort(sims)[-top_n:][::-1]
        for rank, sim_idx in enumerate(top):
            orig_idx = indices[sim_idx]
            df.at[idx, f'most_similar_school_{rank + 1:02d}'] = df.at[orig_idx, 'schulnummer']

    logger.info(f"  Computed similarities for {len(indices)} schools")
    return df


def save_final(df: pd.DataFrame):
    """Save final output files."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Save parquet (with embeddings)
    parquet_path = FINAL_DIR / "bremen_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    # Save CSV (without embeddings column for readability)
    csv_df = df.drop(columns=['embedding'], errors='ignore')
    csv_path = FINAL_DIR / "bremen_school_master_table_final.csv"
    csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame):
    """Print final summary."""
    print(f"\n{'=' * 70}")
    print("BREMEN SCHOOL MASTER TABLE - FINAL OUTPUT")
    print(f"{'=' * 70}")
    print(f"\nTotal schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'schulform' in df.columns:
        print("\nBy Schulform:")
        for t, c in df['schulform'].value_counts().items():
            print(f"  - {t}: {c}")

    print("\nData coverage:")
    for col, label in {
        'description': 'Descriptions',
        'embedding': 'Embeddings',
        'transit_accessibility_score': 'Transit Score',
        'traffic_accidents_total': 'Traffic Data',
        'crime_total': 'Crime Data',
        'crime_safety_score': 'Crime Safety Score',
    }.items():
        if col in df.columns:
            if col == 'embedding':
                count = df[col].apply(
                    lambda x: x is not None and (isinstance(x, list) and len(x) > 0)
                ).sum()
            else:
                count = df[col].notna().sum()
            pct = 100 * count / len(df)
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'=' * 70}")


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting Bremen Embeddings Generator")
    logger.info("=" * 60)

    try:
        df = load_master_table()
        df = add_descriptions(df)
        df = generate_embeddings(df)
        df = compute_similar_schools(df)
        save_final(df)
        print_summary(df)
    except FileNotFoundError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Embeddings generation failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
