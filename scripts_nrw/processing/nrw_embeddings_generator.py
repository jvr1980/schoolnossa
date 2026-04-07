#!/usr/bin/env python3
"""
NRW School Embeddings Generator
Generates text embeddings and creates the final master parquet file.

This script:
1. Loads the combined master table
2. Creates text descriptions for each school
3. Generates embeddings (OpenAI text-embedding-3-large or Gemini text-embedding-004)
4. Computes similar schools
5. Outputs final parquet and CSV files

Supports:
- OpenAI (OPENAI_API_KEY) - produces 3072-dim embeddings
- Gemini (GEMINI_API_KEY) - produces 768-dim embeddings (fallback)

Author: NRW School Data Pipeline
Created: 2026-02-15
"""

import pandas as pd
import numpy as np
import logging
import os
import time
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nrw"
FINAL_DIR = DATA_DIR / "final"
ENV_FILE = PROJECT_ROOT / ".env"

# Load .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)
except ImportError:
    # Manual .env loading fallback
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


def load_master_table(school_type: str) -> pd.DataFrame:
    """Load the combined master table."""
    parquet_path = FINAL_DIR / f"nrw_{school_type}_school_master_table.parquet"
    csv_path = FINAL_DIR / f"nrw_{school_type}_school_master_table.csv"

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    elif csv_path.exists():
        return pd.read_csv(csv_path)
    else:
        raise FileNotFoundError(f"No master table found for {school_type}")


def create_school_description(row: pd.Series) -> str:
    """Create a text description for a school."""
    parts = []

    name = row.get('schulname', 'Unbekannte Schule')
    school_type = row.get('school_type', row.get('schulform_name', ''))
    stadt = row.get('stadt', row.get('ort', ''))

    parts.append(f"{name} ist eine {school_type} in {stadt}.")

    # Location
    bezirk = row.get('crime_bezirk', '')
    if pd.notna(bezirk) and bezirk:
        parts.append(f"Die Schule liegt im Bezirk {bezirk}.")

    # PLZ
    plz = row.get('plz', '')
    strasse = row.get('strasse', '')
    if pd.notna(strasse) and strasse and pd.notna(plz) and plz:
        parts.append(f"Adresse: {strasse}, {plz} {stadt}.")

    # Schulsozialindex
    ssi = row.get('sozialindexstufe')
    if pd.notna(ssi):
        ssi = int(ssi)
        if ssi <= 3:
            parts.append(f"Schulsozialindex: {ssi} (niedrige soziale Belastung).")
        elif ssi <= 6:
            parts.append(f"Schulsozialindex: {ssi} (mittlere soziale Belastung).")
        else:
            parts.append(f"Schulsozialindex: {ssi} (hohe soziale Belastung).")

    # Operator
    traeger = row.get('traegerschaft', '')
    if pd.notna(traeger) and traeger:
        if 'privat' in str(traeger).lower():
            parts.append("Die Schule ist in privater Trägerschaft.")
        elif 'öffentlich' in str(traeger).lower():
            parts.append("Die Schule ist in öffentlicher Trägerschaft.")

    # Transit
    transit_score = row.get('transit_accessibility_score')
    if pd.notna(transit_score):
        if transit_score >= 80:
            parts.append("Hervorragende Anbindung an den öffentlichen Nahverkehr.")
        elif transit_score >= 60:
            parts.append("Gute Anbindung an den öffentlichen Nahverkehr.")
        elif transit_score >= 40:
            parts.append("Mittlere Anbindung an den öffentlichen Nahverkehr.")
        else:
            parts.append("Eingeschränkte Anbindung an den öffentlichen Nahverkehr.")

    # Traffic safety
    acc_per_year = row.get('traffic_accidents_per_year')
    if pd.notna(acc_per_year) and acc_per_year > 0:
        if acc_per_year >= 20:
            parts.append(f"Im Umfeld der Schule gibt es erhöhtes Verkehrsunfallaufkommen ({acc_per_year:.0f} Unfälle/Jahr).")
        elif acc_per_year >= 10:
            parts.append(f"Mittleres Verkehrsunfallaufkommen im Schulumfeld ({acc_per_year:.0f} Unfälle/Jahr).")

    # Crime
    crime_index = row.get('crime_bezirk_index')
    if pd.notna(crime_index):
        if crime_index >= 1.5:
            parts.append("Der Bezirk hat eine überdurchschnittliche Kriminalitätsbelastung.")
        elif crime_index <= 0.7:
            parts.append("Der Bezirk hat eine unterdurchschnittliche Kriminalitätsbelastung.")

    return " ".join(parts)


def add_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """Add text descriptions. Preserves existing rich descriptions from website enrichment."""
    logger.info("Generating school descriptions...")
    df = df.copy()

    # Generate auto-descriptions for ALL schools (used for embeddings if no rich description)
    df['description_auto'] = df.apply(create_school_description, axis=1)

    # Only overwrite 'description' if it's empty/missing (preserve rich Gemini descriptions)
    if 'description' in df.columns:
        has_rich = df['description'].notna() & (
            df['description'].astype(str).str.len() > 200
        )
        rich_count = has_rich.sum()
        logger.info(f"  Preserving {rich_count} rich descriptions from website enrichment")
        # Fill in auto-descriptions only where there's no rich description
        df.loc[~has_rich, 'description'] = df.loc[~has_rich, 'description_auto']
    else:
        df['description'] = df['description_auto']

    logger.info(f"Sample: {df['description'].iloc[0][:200]}...")
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

        for idx, row in df.iterrows():
            if idx % 20 == 0:
                logger.info(f"  Embedding {idx + 1}/{total}")

            description = row.get('description', '')
            if not description:
                embeddings.append(None)
                continue

            try:
                response = client.embeddings.create(model=OPENAI_EMBEDDING_MODEL, input=description)
                embeddings.append(response.data[0].embedding)
            except Exception as e:
                logger.warning(f"Embedding failed for school {idx}: {e}")
                embeddings.append(None)

        df['embedding'] = embeddings
        count = sum(1 for e in embeddings if e is not None)
        logger.info(f"Generated {count}/{total} OpenAI embeddings ({len(embeddings[0]) if embeddings and embeddings[0] else '?'} dimensions)")
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
        batch_size = 20  # Gemini supports batching

        descriptions = df['description'].tolist()

        for i in range(0, total, batch_size):
            batch = descriptions[i:i + batch_size]
            logger.info(f"  Embedding batch {i + 1}-{min(i + batch_size, total)}/{total}")

            # Filter out empty descriptions
            valid_batch = []
            valid_indices = []
            for j, desc in enumerate(batch):
                if desc and str(desc).strip():
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
                    logger.warning(f"Batch embedding failed at {i}: {e}")
                    # Fall back to individual embedding
                    for j, desc in zip(valid_indices, valid_batch):
                        try:
                            result = genai.embed_content(
                                model=GEMINI_EMBEDDING_MODEL,
                                content=desc,
                                task_type="RETRIEVAL_DOCUMENT"
                            )
                            batch_embeddings[j] = result['embedding']
                        except Exception as e2:
                            logger.warning(f"Individual embedding failed for school {i + j}: {e2}")

            embeddings.extend(batch_embeddings)

            # Rate limiting
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

    valid = df['embedding'].apply(lambda x: x is not None and len(x) > 0 if x else False)

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
    normalized = matrix / norms
    similarity = np.dot(normalized, normalized.T)

    for i, idx in enumerate(indices):
        sims = similarity[i].copy()
        sims[i] = -1
        top = np.argsort(sims)[-top_n:][::-1]
        for rank, sim_idx in enumerate(top):
            orig_idx = indices[sim_idx]
            df.at[idx, f'most_similar_school_{rank + 1:02d}'] = df.at[orig_idx, 'schulnummer']

    return df


def save_final(df: pd.DataFrame, school_type: str):
    """Save final output."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    parquet_path = FINAL_DIR / f"nrw_{school_type}_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    csv_df = df.drop(columns=['embedding'], errors='ignore')
    csv_path = FINAL_DIR / f"nrw_{school_type}_school_master_table_final.csv"
    csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame, school_type: str):
    """Print final summary."""
    print(f"\n{'=' * 70}")
    print(f"NRW {school_type.upper()} SCHOOL MASTER TABLE - FINAL OUTPUT")
    print(f"{'=' * 70}")
    print(f"\nTotal schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'school_type' in df.columns:
        print("\nBy type:")
        for t, c in df['school_type'].value_counts().items():
            print(f"  - {t}: {c}")

    print("\nData coverage:")
    for col, label in {
        'description': 'Descriptions',
        'embedding': 'Embeddings',
        'sozialindexstufe': 'Schulsozialindex',
        'transit_accessibility_score': 'Transit Score',
        'traffic_accidents_total': 'Traffic Data',
        'crime_haeufigkeitszahl_2023': 'Crime Stats',
    }.items():
        if col in df.columns:
            count = df[col].apply(lambda x: x is not None and (not isinstance(x, float) or not pd.isna(x))).sum()
            pct = 100 * count / len(df)
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'=' * 70}")


def process_school_type(school_type: str) -> pd.DataFrame:
    """Process a single school type through embeddings pipeline."""
    logger.info(f"Processing {school_type} schools...")

    df = load_master_table(school_type)
    df = add_descriptions(df)
    df = generate_embeddings(df)
    df = compute_similar_schools(df)
    save_final(df, school_type)
    print_summary(df, school_type)

    return df


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting NRW Embeddings Generator")
    logger.info("=" * 60)

    for school_type in ['secondary', 'primary']:
        try:
            process_school_type(school_type)
        except FileNotFoundError as e:
            logger.warning(str(e))


if __name__ == "__main__":
    main()
