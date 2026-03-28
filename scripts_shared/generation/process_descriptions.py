#!/usr/bin/env python3
"""
Process School Descriptions:
1. Translate English descriptions to German
2. Create short summaries (English & German) for dashboard display
3. Generate embeddings for semantic clustering

Usage:
    python3 process_descriptions.py --input school_master_table_final.csv --output school_master_table_processed.csv
    python3 process_descriptions.py --embeddings-only  # Just generate embeddings
    python3 process_descriptions.py --translations-only  # Just translate
    python3 process_descriptions.py --summaries-only  # Just create summaries
"""

import os
import json
import time
import logging
import argparse
import requests
import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process_descriptions.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(config_path: Path = CONFIG_PATH) -> Dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


class LLMProcessor:
    """Use LLM for translation and summarization."""

    def __init__(self, config: Dict):
        self.config = config
        api_keys = config.get('api_keys', {})
        models = config.get('models', {})

        # Use OpenAI for translations/summaries (faster, cheaper)
        self.api_key = api_keys.get('openai')
        self.model = models.get('openai', 'gpt-4o-mini')
        self.base_url = "https://api.openai.com/v1/chat/completions"

        if not self.api_key:
            raise ValueError("OpenAI API key required for translations")

    def _call_llm(self, system_prompt: str, user_prompt: str, max_tokens: int = 2000) -> str:
        """Make LLM API call."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3
        }

        response = requests.post(self.base_url, headers=headers, json=data, timeout=120)
        response.raise_for_status()

        result = response.json()
        return result["choices"][0]["message"]["content"]

    def translate_to_german(self, english_text: str) -> str:
        """Translate English description to German."""
        system_prompt = """You are a professional translator specializing in educational content.
Translate the following school description from English to German.
- Maintain the same structure and formatting (headers, bullet points, etc.)
- Keep school-specific terms accurate (Abitur, MSA, IGCSE, etc. stay as-is)
- Use formal German (Sie-form) appropriate for official school information
- Preserve any JSON blocks at the end without translating them
- Keep markdown formatting intact"""

        user_prompt = f"Translate this school description to German:\n\n{english_text}"

        return self._call_llm(system_prompt, user_prompt, max_tokens=4000)

    def create_short_summary(self, description: str, language: str = "english") -> str:
        """Create a concise dashboard summary."""
        if language == "german":
            system_prompt = """Erstelle eine prägnante Zusammenfassung einer Schule für ein Dashboard.
Die Zusammenfassung sollte:
- 3-5 kurze Sätze (max. 150 Wörter)
- Die wichtigsten Merkmale hervorheben: Schultyp, Sprachen, besondere Programme, Abschlüsse
- Professionell und informativ sein
- Keine Überschriften oder Aufzählungszeichen verwenden
- Als Fließtext geschrieben sein"""
        else:
            system_prompt = """Create a concise school summary for a dashboard display.
The summary should be:
- 3-5 short sentences (max 150 words)
- Highlight key features: school type, languages, special programs, qualifications offered
- Professional and informative tone
- No headers or bullet points
- Written as flowing prose"""

        user_prompt = f"Create a {language} dashboard summary for this school:\n\n{description[:3000]}"

        return self._call_llm(system_prompt, user_prompt, max_tokens=300)


class EmbeddingGenerator:
    """Generate embeddings using OpenAI's embedding API."""

    def __init__(self, config: Dict):
        self.config = config
        api_keys = config.get('api_keys', {})

        self.api_key = api_keys.get('openai')
        self.model = "text-embedding-3-small"  # Good balance of quality and cost
        self.base_url = "https://api.openai.com/v1/embeddings"

        if not self.api_key:
            raise ValueError("OpenAI API key required for embeddings")

    def get_embedding(self, text: str) -> List[float]:
        """Get embedding for a single text."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Truncate text if too long (max ~8000 tokens for this model)
        text = text[:15000]

        data = {
            "model": self.model,
            "input": text,
            "encoding_format": "float"
        }

        response = requests.post(self.base_url, headers=headers, json=data, timeout=60)
        response.raise_for_status()

        result = response.json()
        return result["data"][0]["embedding"]

    def get_embeddings_batch(self, texts: List[str], batch_size: int = 20) -> List[List[float]]:
        """Get embeddings for multiple texts in batches with retry logic."""
        all_embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            # Truncate each text
            batch = [t[:15000] for t in batch]

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            data = {
                "model": self.model,
                "input": batch,
                "encoding_format": "float"
            }

            # Retry logic for each batch
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.post(self.base_url, headers=headers, json=data, timeout=120)
                    response.raise_for_status()
                    result = response.json()
                    batch_embeddings = [item["embedding"] for item in result["data"]]
                    all_embeddings.extend(batch_embeddings)
                    break
                except Exception as e:
                    logger.warning(f"Batch {i//batch_size + 1} attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(10 * (attempt + 1))  # Exponential backoff
                    else:
                        raise

            logger.info(f"Processed embeddings {i + len(batch)}/{len(texts)}")
            time.sleep(0.5)  # Rate limiting

        return all_embeddings


def process_translations(df: pd.DataFrame, processor: LLMProcessor,
                        force: bool = False, delay: float = 1.0) -> pd.DataFrame:
    """Translate all descriptions to German."""

    if 'description_de' not in df.columns:
        df['description_de'] = None

    to_translate = df[
        df['description'].notna() &
        (df['description'].str.len() > 100) &
        (force | df['description_de'].isna())
    ]

    logger.info(f"Translating {len(to_translate)} descriptions to German")

    for idx, row in to_translate.iterrows():
        school_name = row['schulname']
        description = row['description']

        try:
            logger.info(f"Translating: {school_name[:50]}")
            german_desc = processor.translate_to_german(description)
            df.at[idx, 'description_de'] = german_desc
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error translating {school_name}: {e}")
            df.at[idx, 'description_de'] = None

    return df


def process_summaries(df: pd.DataFrame, processor: LLMProcessor,
                     force: bool = False, delay: float = 0.5) -> pd.DataFrame:
    """Create short summaries in English and German."""

    if 'summary_en' not in df.columns:
        df['summary_en'] = None
    if 'summary_de' not in df.columns:
        df['summary_de'] = None

    # English summaries from English descriptions
    to_summarize_en = df[
        df['description'].notna() &
        (df['description'].str.len() > 100) &
        (force | df['summary_en'].isna())
    ]

    logger.info(f"Creating {len(to_summarize_en)} English summaries")

    for idx, row in to_summarize_en.iterrows():
        school_name = row['schulname']
        description = row['description']

        try:
            logger.info(f"Summarizing (EN): {school_name[:50]}")
            summary = processor.create_short_summary(description, "english")
            df.at[idx, 'summary_en'] = summary
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error summarizing {school_name}: {e}")

    # German summaries from German descriptions (or translate summary)
    to_summarize_de = df[
        (df['description_de'].notna() | df['description'].notna()) &
        (force | df['summary_de'].isna())
    ]

    logger.info(f"Creating {len(to_summarize_de)} German summaries")

    for idx, row in to_summarize_de.iterrows():
        school_name = row['schulname']

        # Use German description if available, otherwise use English
        description = row.get('description_de') or row.get('description')
        if not description or len(str(description)) < 100:
            continue

        try:
            logger.info(f"Summarizing (DE): {school_name[:50]}")
            summary = processor.create_short_summary(str(description), "german")
            df.at[idx, 'summary_de'] = summary
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error summarizing {school_name}: {e}")

    return df


def process_embeddings(df: pd.DataFrame, generator: EmbeddingGenerator,
                      force: bool = False) -> Tuple[pd.DataFrame, np.ndarray]:
    """Generate embeddings for all descriptions."""

    # Filter to schools with descriptions
    has_desc = df['description'].notna() & (df['description'].str.len() > 100)
    schools_with_desc = df[has_desc].copy()

    logger.info(f"Generating embeddings for {len(schools_with_desc)} schools")

    descriptions = schools_with_desc['description'].tolist()

    # Generate embeddings
    embeddings = generator.get_embeddings_batch(descriptions)

    # Create embeddings array
    embeddings_array = np.array(embeddings)

    # Add embedding index to dataframe
    df['embedding_idx'] = None
    for i, idx in enumerate(schools_with_desc.index):
        df.at[idx, 'embedding_idx'] = i

    return df, embeddings_array


def save_embeddings(embeddings: np.ndarray, schulnummers: List[str], output_path: str):
    """Save embeddings to a numpy file with metadata."""
    np.savez(
        output_path,
        embeddings=embeddings,
        schulnummers=schulnummers
    )
    logger.info(f"Saved embeddings to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Process school descriptions')
    parser.add_argument('--input', '-i', default='school_master_table_final.csv',
                       help='Input CSV file')
    parser.add_argument('--output', '-o', default='school_master_table_processed.csv',
                       help='Output CSV file')
    parser.add_argument('--embeddings-output', default='school_embeddings.npz',
                       help='Output file for embeddings')
    parser.add_argument('--translations-only', action='store_true',
                       help='Only do translations')
    parser.add_argument('--summaries-only', action='store_true',
                       help='Only create summaries')
    parser.add_argument('--embeddings-only', action='store_true',
                       help='Only generate embeddings')
    parser.add_argument('--force', '-f', action='store_true',
                       help='Force reprocessing even if data exists')
    parser.add_argument('--delay', '-d', type=float, default=1.0,
                       help='Delay between API calls')
    parser.add_argument('--limit', '-l', type=int, default=None,
                       help='Limit number of schools to process (for testing)')

    args = parser.parse_args()

    # Load config and data
    config = load_config()
    logger.info(f"Loading data from {args.input}")
    df = pd.read_csv(args.input, encoding='utf-8-sig')

    if args.limit:
        df = df.head(args.limit)
        logger.info(f"Limited to {args.limit} schools for testing")

    # Determine what to process
    do_all = not (args.translations_only or args.summaries_only or args.embeddings_only)
    do_translations = do_all or args.translations_only
    do_summaries = do_all or args.summaries_only
    do_embeddings = do_all or args.embeddings_only

    # Initialize processors
    if do_translations or do_summaries:
        processor = LLMProcessor(config)

    if do_embeddings:
        embedding_generator = EmbeddingGenerator(config)

    # Process translations
    if do_translations:
        logger.info("="*60)
        logger.info("TRANSLATING DESCRIPTIONS TO GERMAN")
        logger.info("="*60)
        df = process_translations(df, processor, force=args.force, delay=args.delay)

    # Process summaries
    if do_summaries:
        logger.info("="*60)
        logger.info("CREATING SHORT SUMMARIES")
        logger.info("="*60)
        df = process_summaries(df, processor, force=args.force, delay=args.delay)

    # Save intermediate results (translations + summaries) before embeddings
    if do_translations or do_summaries:
        intermediate_output = args.output.replace('.csv', '_pre_embeddings.csv')
        df.to_csv(intermediate_output, index=False, encoding='utf-8-sig')
        logger.info(f"Saved intermediate data to {intermediate_output}")

    # Process embeddings
    embeddings_array = None
    if do_embeddings:
        logger.info("="*60)
        logger.info("GENERATING EMBEDDINGS")
        logger.info("="*60)

        # Retry logic for embeddings
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df, embeddings_array = process_embeddings(df, embedding_generator, force=args.force)
                break
            except Exception as e:
                logger.error(f"Embedding attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in 30 seconds...")
                    time.sleep(30)
                else:
                    logger.error("All embedding attempts failed. Saving data without embeddings.")
                    embeddings_array = None

        # Save embeddings separately if successful
        if embeddings_array is not None:
            schools_with_embeddings = df[df['embedding_idx'].notna()]
            schulnummers = schools_with_embeddings['schulnummer'].tolist()
            save_embeddings(embeddings_array, schulnummers, args.embeddings_output)

    # Save processed dataframe
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    df.to_excel(args.output.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    logger.info(f"Saved processed data to {args.output}")

    # Print summary
    print("\n" + "="*70)
    print("PROCESSING SUMMARY")
    print("="*70)
    print(f"Total schools: {len(df)}")

    has_desc = df['description'].notna() & (df['description'].str.len() > 100)
    print(f"Schools with English descriptions: {has_desc.sum()}")

    if 'description_de' in df.columns:
        has_desc_de = df['description_de'].notna() & (df['description_de'].str.len() > 100)
        print(f"Schools with German descriptions: {has_desc_de.sum()}")

    if 'summary_en' in df.columns:
        has_summary_en = df['summary_en'].notna()
        print(f"Schools with English summaries: {has_summary_en.sum()}")

    if 'summary_de' in df.columns:
        has_summary_de = df['summary_de'].notna()
        print(f"Schools with German summaries: {has_summary_de.sum()}")

    if embeddings_array is not None:
        print(f"Embeddings generated: {len(embeddings_array)}")
        print(f"Embedding dimensions: {embeddings_array.shape[1]}")

    print(f"\nOutput: {args.output}")
    if embeddings_array is not None:
        print(f"Embeddings: {args.embeddings_output}")
    print("="*70)


if __name__ == "__main__":
    main()
