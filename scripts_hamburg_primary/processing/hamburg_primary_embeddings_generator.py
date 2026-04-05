#!/usr/bin/env python3
"""
Hamburg Primary School Embeddings Generator
Generates text embeddings and creates the final master parquet file for Grundschulen.

This script:
1. Loads the combined primary school master table
2. Creates text descriptions for each Grundschule
3. Generates OpenAI embeddings (if API key available)
4. Computes similar schools
5. Outputs: hamburg_primary_school_master_table_final_with_embeddings.parquet

Author: Hamburg Primary School Data Pipeline
Created: 2026-04-04
"""

import pandas as pd
import numpy as np
import logging
import os
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
DATA_DIR = PROJECT_ROOT / "data_hamburg_primary"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

# OpenAI settings
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 3072


def load_master_table() -> pd.DataFrame:
    """Load the combined primary school master table."""
    logger.info("Loading primary school master table...")

    # Try parquet first
    parquet_path = FINAL_DIR / "hamburg_primary_school_master_table.parquet"
    csv_path = FINAL_DIR / "hamburg_primary_school_master_table.csv"

    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        raise FileNotFoundError(f"No primary school master table found in {FINAL_DIR}")

    logger.info(f"Loaded {len(df)} primary schools")
    return df


def create_school_description(row: pd.Series) -> str:
    """Create a text description for a primary school based on its data."""
    parts = []

    # School name - always Grundschule
    name = row.get('schulname', 'Unknown School')
    parts.append(f"{name} ist eine Grundschule in Hamburg.")

    # Location
    stadtteil = row.get('stadtteil', '')
    bezirk = row.get('bezirk', '')
    if stadtteil or bezirk:
        location = stadtteil if stadtteil else bezirk
        parts.append(f"Die Schule befindet sich in {location}.")

    # Student count
    schueler = row.get('schueler_gesamt') or row.get('anzahl_schueler_gesamt')
    if pd.notna(schueler) and schueler > 0:
        parts.append(f"Etwa {int(schueler)} Schueler besuchen diese Schule.")

    # Ganztag (important for primary schools)
    ganztag = row.get('ganztagsform', '')
    if pd.notna(ganztag) and ganztag:
        parts.append(f"Ganztagsform: {ganztag}.")

    # Vorschulklasse (primary-specific)
    vorschulklasse = row.get('vorschulklasse', '')
    if pd.notna(vorschulklasse) and vorschulklasse:
        parts.append(f"Vorschulklasse: {vorschulklasse}.")

    # Ferienbetreuung (primary-specific)
    ferienbetreuung = row.get('ferienbetreuung_anteil', '')
    if pd.notna(ferienbetreuung) and ferienbetreuung:
        parts.append(f"Ferienbetreuung: {ferienbetreuung}.")

    # Languages
    sprachen = row.get('fremdsprache', '')
    if pd.notna(sprachen) and sprachen:
        parts.append(f"Angebotene Fremdsprachen: {sprachen}.")

    # Bilingual
    bilingual = row.get('bilingual', '')
    if pd.notna(bilingual) and bilingual:
        parts.append(f"Bilingualer Unterricht: {bilingual}.")

    # School focus
    ausrichtung = row.get('schulische_ausrichtung', '')
    if pd.notna(ausrichtung) and ausrichtung:
        parts.append(f"Schulische Ausrichtung: {ausrichtung}.")

    # Transit accessibility
    transit_score = row.get('transit_accessibility_score')
    if pd.notna(transit_score):
        if transit_score >= 80:
            parts.append("Hervorragende Anbindung an den oeffentlichen Nahverkehr.")
        elif transit_score >= 60:
            parts.append("Gute Anbindung an den oeffentlichen Nahverkehr.")
        elif transit_score >= 40:
            parts.append("Mittlere Anbindung an den oeffentlichen Nahverkehr.")

    # Operator
    traeger = row.get('traeger_typ', row.get('traegerschaft', ''))
    if pd.notna(traeger) and traeger:
        if 'privat' in str(traeger).lower():
            parts.append("Die Schule ist in privater Traegerschaft.")
        elif 'staatlich' in str(traeger).lower() or 'oeffentlich' in str(traeger).lower():
            parts.append("Die Schule ist in staatlicher Traegerschaft.")

    return " ".join(parts)


def add_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """Add text descriptions to all schools."""
    logger.info("Generating school descriptions...")

    df = df.copy()
    df['description'] = df.apply(create_school_description, axis=1)

    # Log sample
    sample_desc = df['description'].iloc[0]
    logger.info(f"Sample description: {sample_desc[:200]}...")

    return df


def generate_embeddings(df: pd.DataFrame) -> pd.DataFrame:
    """Generate OpenAI embeddings for school descriptions."""
    logger.info("Checking for OpenAI API key...")

    api_key = os.environ.get('OPENAI_API_KEY')

    if not api_key:
        logger.warning("OPENAI_API_KEY not found. Skipping embedding generation.")
        logger.info("To generate embeddings, set the OPENAI_API_KEY environment variable.")
        # Add placeholder embedding column
        df['embedding'] = None
        return df

    logger.info("Generating embeddings with OpenAI...")

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        embeddings = []
        total = len(df)

        for idx, row in df.iterrows():
            if idx % 20 == 0:
                logger.info(f"Generating embedding {idx + 1}/{total}")

            description = row.get('description', '')
            if not description:
                embeddings.append(None)
                continue

            try:
                response = client.embeddings.create(
                    model=EMBEDDING_MODEL,
                    input=description
                )
                embedding = response.data[0].embedding
                embeddings.append(embedding)
            except Exception as e:
                logger.warning(f"Failed to generate embedding for school {idx}: {e}")
                embeddings.append(None)

        df['embedding'] = embeddings
        embedding_count = sum(1 for e in embeddings if e is not None)
        logger.info(f"Generated {embedding_count}/{total} embeddings")

    except ImportError:
        logger.warning("OpenAI package not installed. Run: pip install openai")
        df['embedding'] = None
    except Exception as e:
        logger.error(f"Embedding generation failed: {e}")
        df['embedding'] = None

    return df


def compute_similar_schools(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """Compute most similar schools based on embeddings."""
    logger.info("Computing similar schools...")

    df = df.copy()

    # Initialize similarity columns
    for i in range(1, top_n + 1):
        df[f'most_similar_school_{i:02d}'] = None

    # Check if we have embeddings
    if 'embedding' not in df.columns:
        logger.warning("No embeddings available for similarity computation")
        return df

    valid_embeddings = df['embedding'].apply(lambda x: x is not None and len(x) > 0 if x else False)

    if not valid_embeddings.any():
        logger.warning("No valid embeddings found")
        return df

    logger.info(f"Computing similarity for {valid_embeddings.sum()} schools with embeddings")

    # Convert embeddings to numpy array
    embedding_matrix = []
    embedding_indices = []

    for idx, row in df.iterrows():
        if valid_embeddings[idx]:
            embedding_matrix.append(row['embedding'])
            embedding_indices.append(idx)

    if len(embedding_matrix) < 2:
        logger.warning("Not enough embeddings for similarity computation")
        return df

    embedding_matrix = np.array(embedding_matrix)

    # Normalize embeddings
    norms = np.linalg.norm(embedding_matrix, axis=1, keepdims=True)
    normalized = embedding_matrix / norms

    # Compute cosine similarity
    similarity_matrix = np.dot(normalized, normalized.T)

    # Find top similar schools for each
    for i, idx in enumerate(embedding_indices):
        # Get similarities (excluding self)
        similarities = similarity_matrix[i].copy()
        similarities[i] = -1  # Exclude self

        # Get top N
        top_indices = np.argsort(similarities)[-top_n:][::-1]

        for rank, sim_idx in enumerate(top_indices):
            original_idx = embedding_indices[sim_idx]
            similar_school = df.at[original_idx, 'schulnummer']
            df.at[idx, f'most_similar_school_{rank + 1:02d}'] = similar_school

    logger.info("Similarity computation complete")
    return df


def save_final_output(df: pd.DataFrame):
    """Save the final master table with embeddings."""
    logger.info("Saving final output...")

    # Ensure directory exists
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Save as parquet (handles complex types like embeddings)
    parquet_path = FINAL_DIR / "hamburg_primary_school_master_table_final_with_embeddings.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    # Also save CSV (without embeddings for readability)
    csv_df = df.drop(columns=['embedding'], errors='ignore')
    csv_path = FINAL_DIR / "hamburg_primary_school_master_table_final.csv"
    csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")


def print_summary(df: pd.DataFrame):
    """Print final summary."""
    print("\n" + "="*70)
    print("HAMBURG PRIMARY SCHOOL MASTER TABLE - FINAL OUTPUT")
    print("="*70)

    print(f"\nTotal primary schools: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    if 'school_type' in df.columns:
        print("\nSchools by type:")
        for t, count in df['school_type'].value_counts().items():
            print(f"  - {t}: {count}")

    # Data coverage
    print("\nFinal data coverage:")

    coverage = {
        'description': 'Descriptions',
        'embedding': 'Embeddings',
        'transit_accessibility_score': 'Transit Score',
        'schueler_gesamt': 'Student Count',
        'ganztagsform': 'Ganztag Form',
        'vorschulklasse': 'Vorschulklasse',
    }

    for col, label in coverage.items():
        if col in df.columns:
            count = df[col].apply(lambda x: x is not None and (not isinstance(x, float) or not pd.isna(x))).sum()
            pct = 100 * count / len(df)
            print(f"  - {label}: {count}/{len(df)} ({pct:.0f}%)")

    print(f"\nOutput files:")
    print(f"  - {FINAL_DIR / 'hamburg_primary_school_master_table_final_with_embeddings.parquet'}")
    print(f"  - {FINAL_DIR / 'hamburg_primary_school_master_table_final.csv'}")

    print("\n" + "="*70)


def main():
    """Main function to generate embeddings and create final output for primary schools."""
    logger.info("="*60)
    logger.info("Starting Hamburg Primary School Embeddings Generator")
    logger.info("="*60)

    try:
        # Load master table
        df = load_master_table()

        # Add descriptions
        df = add_descriptions(df)

        # Generate embeddings
        df = generate_embeddings(df)

        # Compute similar schools
        df = compute_similar_schools(df)

        # Save final output
        save_final_output(df)

        # Print summary
        print_summary(df)

        logger.info("Embeddings generation complete!")
        return df

    except Exception as e:
        logger.error(f"Embeddings generation failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
