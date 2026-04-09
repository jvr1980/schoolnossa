#!/usr/bin/env python3
"""
Phase 8: Leipzig Embeddings Generator
=======================================

Generates embeddings for Leipzig schools and creates the final parquet output.
Adapted from NRW embeddings generator (OpenAI with Gemini fallback).

This script:
1. Loads the combined master table from Phase 7
2. Constructs text representations from: schulname, schultyp, adresse,
   ortsteil, description/description_de, besonderheiten, sprachen
3. Generates embeddings via OpenAI text-embedding-3-large (or Gemini fallback)
4. Computes similar schools via cosine similarity
5. Saves final CSV + parquet with embeddings

Input: data_leipzig/final/leipzig_school_master_table_final.csv
Output:
    data_leipzig/final/leipzig_school_master_table_final.csv (updated with descriptions)
    data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet

Author: Leipzig School Data Pipeline
Created: 2026-04-08
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
DATA_DIR = PROJECT_ROOT / "data_leipzig"
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

# Embedding settings
OPENAI_EMBEDDING_MODEL = "text-embedding-3-large"
GEMINI_EMBEDDING_MODEL = "models/gemini-embedding-001"


def load_master_table() -> pd.DataFrame:
    """Load the combined master table from Phase 7."""
    parquet_path = FINAL_DIR / "leipzig_school_master_table_final.parquet"
    csv_path = FINAL_DIR / "leipzig_school_master_table_final.csv"

    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
        logger.info(f"Loaded {len(df)} schools from {parquet_path.name} ({len(df.columns)} columns)")
        return df
    elif csv_path.exists():
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df)} schools from {csv_path.name} ({len(df.columns)} columns)")
        return df
    else:
        raise FileNotFoundError(
            f"No master table found. Run Phase 7 (data combiner) first.\n"
            f"Expected: {parquet_path} or {csv_path}"
        )


def create_school_text(row: pd.Series) -> str:
    """Create a text representation for embedding from key school fields.

    Combines: schulname, schultyp, adresse, ortsteil, description/description_de,
    besonderheiten, sprachen
    """
    parts = []

    # School name and type
    name = row.get('schulname', '')
    schultyp = row.get('schultyp', row.get('school_type', row.get('schulart', '')))
    if pd.notna(name) and name:
        intro = str(name)
        if pd.notna(schultyp) and schultyp:
            intro += f" ist eine {schultyp}"
        parts.append(intro)

    # Address and location
    adresse = row.get('adresse', row.get('strasse', ''))
    ort = row.get('ort', 'Leipzig')
    plz = row.get('plz', '')
    ortsteil = row.get('ortsteil', '')

    if pd.notna(adresse) and adresse:
        addr_str = str(adresse)
        if pd.notna(plz) and plz:
            addr_str += f", {plz}"
        if pd.notna(ort) and ort:
            addr_str += f" {ort}"
        parts.append(f"Adresse: {addr_str}")

    if pd.notna(ortsteil) and ortsteil:
        parts.append(f"Ortsteil: {ortsteil}")

    # Rich description (from website metadata enrichment)
    description_de = row.get('description_de', '')
    description_en = row.get('description', '')

    if pd.notna(description_de) and str(description_de).strip() and len(str(description_de)) > 50:
        parts.append(str(description_de))
    elif pd.notna(description_en) and str(description_en).strip() and len(str(description_en)) > 50:
        parts.append(str(description_en))

    # Special features
    besonderheiten = row.get('besonderheiten', '')
    if pd.notna(besonderheiten) and besonderheiten:
        parts.append(f"Besonderheiten: {besonderheiten}")

    # Languages
    sprachen = row.get('sprachen', '')
    if pd.notna(sprachen) and sprachen:
        parts.append(f"Sprachen: {sprachen}")

    # Operator
    traeger = row.get('traeger', row.get('traegerschaft', ''))
    if pd.notna(traeger) and traeger:
        parts.append(f"Schultraeger: {traeger}")

    # Transit info
    transit_score = row.get('transit_accessibility_score')
    if pd.notna(transit_score):
        if transit_score >= 80:
            parts.append("Hervorragende OEPNV-Anbindung.")
        elif transit_score >= 60:
            parts.append("Gute OEPNV-Anbindung.")
        elif transit_score >= 40:
            parts.append("Mittlere OEPNV-Anbindung.")
        else:
            parts.append("Eingeschraenkte OEPNV-Anbindung.")

    # Crime safety
    safety = row.get('crime_safety_category', '')
    if pd.notna(safety) and safety:
        parts.append(f"Sicherheitskategorie: {safety}")

    return ". ".join(parts) if parts else ""


def create_auto_description(row: pd.Series) -> str:
    """Create a short auto-description for schools without rich descriptions."""
    parts = []

    name = row.get('schulname', 'Unbekannte Schule')
    schultyp = row.get('schultyp', row.get('school_type', ''))
    ort = row.get('ort', 'Leipzig')

    parts.append(f"{name} ist eine {schultyp} in {ort}.")

    ortsteil = row.get('ortsteil', '')
    if pd.notna(ortsteil) and ortsteil:
        parts.append(f"Die Schule liegt im Ortsteil {ortsteil}.")

    adresse = row.get('adresse', row.get('strasse', ''))
    plz = row.get('plz', '')
    if pd.notna(adresse) and adresse and pd.notna(plz) and plz:
        parts.append(f"Adresse: {adresse}, {plz} {ort}.")

    traeger = row.get('traeger', row.get('traegerschaft', ''))
    if pd.notna(traeger) and traeger:
        if 'privat' in str(traeger).lower() or 'frei' in str(traeger).lower():
            parts.append("Die Schule ist in freier Traegerschaft.")
        else:
            parts.append("Die Schule ist in oeffentlicher Traegerschaft.")

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

    return " ".join(parts)


def add_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """Add text descriptions. Preserves existing rich descriptions from website enrichment."""
    logger.info("Generating school descriptions...")
    df = df.copy()

    # Generate auto-descriptions for all schools
    df['description_auto'] = df.apply(create_auto_description, axis=1)

    # Preserve rich descriptions from website metadata enrichment (Phase 6)
    if 'description' in df.columns:
        has_rich = df['description'].notna() & (
            df['description'].astype(str).str.len() > 200
        )
        rich_count = has_rich.sum()
        logger.info(f"  Preserving {rich_count} rich descriptions from website enrichment")
        # Fill auto-descriptions only where there's no rich description
        df.loc[~has_rich, 'description'] = df.loc[~has_rich, 'description_auto']
    else:
        df['description'] = df['description_auto']

    # Generate embedding text (combines multiple fields for richer representation)
    df['embedding_text'] = df.apply(create_school_text, axis=1)

    logger.info(f"  Sample description: {df['description'].iloc[0][:200]}...")
    logger.info(f"  Sample embedding text: {df['embedding_text'].iloc[0][:200]}...")

    return df


def generate_embeddings_openai(texts: list) -> list:
    """Generate embeddings using OpenAI text-embedding-3-large (3072 dimensions)."""
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        return None

    logger.info(f"Generating embeddings with OpenAI ({OPENAI_EMBEDDING_MODEL})...")

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        embeddings = []
        total = len(texts)

        for idx, text in enumerate(texts):
            if idx % 20 == 0:
                logger.info(f"  Embedding {idx + 1}/{total}")

            if not text or not str(text).strip():
                embeddings.append(None)
                continue

            try:
                response = client.embeddings.create(
                    model=OPENAI_EMBEDDING_MODEL,
                    input=str(text)
                )
                embeddings.append(response.data[0].embedding)
            except Exception as e:
                logger.warning(f"  Embedding failed for school {idx}: {e}")
                embeddings.append(None)

        count = sum(1 for e in embeddings if e is not None)
        dim = len(embeddings[0]) if embeddings and embeddings[0] else '?'
        logger.info(f"Generated {count}/{total} OpenAI embeddings ({dim} dimensions)")
        return embeddings

    except ImportError:
        logger.warning("openai package not installed")
        return None
    except Exception as e:
        logger.error(f"OpenAI embedding generation failed: {e}")
        return None


def generate_embeddings_gemini(texts: list) -> list:
    """Generate embeddings using Gemini embedding model (768 dimensions)."""
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None

    logger.info(f"Generating embeddings with Gemini ({GEMINI_EMBEDDING_MODEL})...")

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        embeddings = []
        total = len(texts)
        batch_size = 20

        for i in range(0, total, batch_size):
            batch = texts[i:i + batch_size]
            logger.info(f"  Embedding batch {i + 1}-{min(i + batch_size, total)}/{total}")

            # Filter out empty texts
            valid_batch = []
            valid_indices = []
            for j, text in enumerate(batch):
                if text and str(text).strip():
                    valid_batch.append(str(text))
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

        count = sum(1 for e in embeddings if e is not None)
        dim = len(embeddings[0]) if embeddings and embeddings[0] else '?'
        logger.info(f"Generated {count}/{total} Gemini embeddings ({dim} dimensions)")
        return embeddings

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

    # Use embedding_text for richer representation
    texts = df['embedding_text'].tolist()

    # Try OpenAI first
    embeddings = generate_embeddings_openai(texts)
    if embeddings is not None:
        df['embedding'] = embeddings
        return df

    # Fall back to Gemini
    logger.info("OpenAI not available, trying Gemini...")
    embeddings = generate_embeddings_gemini(texts)
    if embeddings is not None:
        df['embedding'] = embeddings
        return df

    # Neither available
    logger.warning("No embedding API key found. Set OPENAI_API_KEY or GEMINI_API_KEY.")
    logger.warning("Skipping embedding generation.")
    df['embedding'] = None
    return df


def compute_similar_schools(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """Compute most similar schools based on embedding cosine similarity."""
    logger.info("Computing similar schools...")
    df = df.copy()

    # Initialize columns
    for i in range(1, top_n + 1):
        df[f'most_similar_school_{i:02d}'] = None

    if 'embedding' not in df.columns:
        return df

    # Collect valid embeddings
    valid_mask = df['embedding'].apply(
        lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0
    )

    if not valid_mask.any():
        logger.warning("No valid embeddings found, skipping similarity computation")
        return df

    matrix = []
    indices = []
    for idx in df.index:
        if valid_mask[idx]:
            matrix.append(df.at[idx, 'embedding'])
            indices.append(idx)

    if len(matrix) < 2:
        logger.warning("Fewer than 2 valid embeddings, skipping similarity computation")
        return df

    matrix = np.array(matrix)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1  # Avoid division by zero
    normalized = matrix / norms
    similarity = np.dot(normalized, normalized.T)

    for i, idx in enumerate(indices):
        sims = similarity[i].copy()
        sims[i] = -1  # Exclude self
        top = np.argsort(sims)[-top_n:][::-1]
        for rank, sim_idx in enumerate(top):
            orig_idx = indices[sim_idx]
            df.at[idx, f'most_similar_school_{rank + 1:02d}'] = df.at[orig_idx, 'schulnummer']

    matched = sum(1 for idx in indices if df.at[idx, 'most_similar_school_01'] is not None)
    logger.info(f"Computed similar schools for {matched} schools")

    return df


def save_final(df: pd.DataFrame):
    """Save final output files."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Save parquet (with embeddings)
    parquet_path = FINAL_DIR / "leipzig_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    # Save CSV (without embeddings, without embedding_text)
    csv_df = df.drop(columns=['embedding', 'embedding_text', 'description_auto'], errors='ignore')
    csv_path = FINAL_DIR / "leipzig_school_master_table_final.csv"
    csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame):
    """Print final summary."""
    print(f"\n{'=' * 70}")
    print("LEIPZIG EMBEDDINGS GENERATOR (Phase 8) - COMPLETE")
    print(f"{'=' * 70}")

    print(f"\nTotal schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'schultyp' in df.columns:
        print("\nBy type:")
        for t, c in df['schultyp'].value_counts().items():
            print(f"  - {t}: {c}")
    elif 'school_type' in df.columns:
        print("\nBy type:")
        for t, c in df['school_type'].value_counts().items():
            print(f"  - {t}: {c}")

    print("\nData coverage:")
    coverage_items = {
        'description': 'Descriptions',
        'description_de': 'Descriptions (DE)',
        'embedding': 'Embeddings',
        'most_similar_school_01': 'Similar Schools',
        'transit_accessibility_score': 'Transit Score',
        'traffic_accidents_total': 'Traffic Data',
        'crime_total': 'Crime Stats',
        'sprachen': 'Languages',
        'besonderheiten': 'Special Features',
    }

    for col, label in coverage_items.items():
        if col in df.columns:
            if col == 'embedding':
                count = df[col].apply(
                    lambda x: x is not None and hasattr(x, '__len__') and len(x) > 0
                ).sum()
            else:
                count = df[col].notna().sum()
            pct = 100 * count / len(df) if len(df) > 0 else 0
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'=' * 70}")


def main():
    """Generate embeddings for Leipzig schools."""
    logger.info("=" * 60)
    logger.info("Starting Leipzig Embeddings Generator (Phase 8)")
    logger.info("=" * 60)

    try:
        df = load_master_table()
        df = add_descriptions(df)
        df = generate_embeddings(df)
        df = compute_similar_schools(df)
        save_final(df)
        print_summary(df)
        logger.info("Leipzig Embeddings Generator complete!")
        return df

    except Exception as e:
        logger.error(f"Embeddings generation failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
