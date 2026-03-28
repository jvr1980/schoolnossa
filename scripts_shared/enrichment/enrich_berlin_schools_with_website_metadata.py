#!/usr/bin/env python3
"""
Berlin School Website Metadata Enrichment Script
=================================================

Extracts metadata from school websites using LLM (Gemini) and adds it to
existing school data. Performs ADDITIVE enrichment.

Target fields to extract:
- sprachen (languages offered)
- gruendungsjahr (founding year)
- besonderheiten (special features/programs)

Usage:
    # Add metadata to existing primary school parquet (in-place)
    python3 enrich_berlin_schools_with_website_metadata.py \
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

    # Add metadata to existing secondary school parquet (in-place)
    python3 enrich_berlin_schools_with_website_metadata.py \
        --input data_berlin/final/school_master_table_final_with_embeddings.parquet

    # Specify custom output path
    python3 enrich_berlin_schools_with_website_metadata.py \
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet \
        --output data_berlin_primary/final/enriched_with_website_metadata.parquet

    # Only process schools missing data
    python3 enrich_berlin_schools_with_website_metadata.py \
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet \
        --only-missing

Requirements:
    - google-generativeai (pip install google-generativeai)
    - GEMINI_API_KEY environment variable or in config.yaml
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script directory and project root
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# OpenAI configuration
OPENAI_MODEL = "gpt-4o-mini"  # Fast and cheap for extraction tasks

# Request settings
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
}

# Processing settings
MAX_WORKERS = 3  # Lower for LLM rate limits
REQUEST_DELAY = 0.5
MAX_RETRIES = 2
SAVE_INTERVAL = 20

# Thread-safe resources
api_lock = Lock()
results_lock = Lock()

# Global OpenAI client
openai_client = None


def get_openai_api_key() -> Optional[str]:
    """Get OpenAI API key from environment or config.yaml."""
    # Try environment variable first
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        return api_key

    # Try config.yaml
    config_path = PROJECT_ROOT / "config.yaml"
    if config_path.exists() and YAML_AVAILABLE:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            api_key = config.get('api_keys', {}).get('openai')
            if api_key:
                return api_key
        except Exception as e:
            logger.warning(f"Failed to read config.yaml: {e}")

    return None


def init_openai(api_key: str) -> bool:
    """Initialize the OpenAI client."""
    global openai_client
    try:
        openai_client = openai.OpenAI(api_key=api_key)
        logger.info(f"Initialized OpenAI with model: {OPENAI_MODEL}")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI: {e}")
        return False


def safe_request(url: str, timeout: int = 15) -> Optional[requests.Response]:
    """Make a safe HTTP request with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1 * (attempt + 1))
            else:
                logger.debug(f"Request failed for {url}: {e}")
                return None
    return None


def clean_html_for_llm(html_content: str, max_chars: int = 12000) -> str:
    """Clean HTML content for LLM processing."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove non-content elements
    for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe', 'noscript', 'meta', 'link']):
        element.decompose()

    # Get text
    text = soup.get_text(separator=' ', strip=True)

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)

    # Truncate if too long
    if len(text) > max_chars:
        text = text[:max_chars] + "..."

    return text


def extract_metadata_with_openai(text_content: str, schulname: str, schulnummer: str, school_type: str) -> Dict[str, Any]:
    """
    Use OpenAI to extract school metadata from website text.
    Extracts: sprachen, gruendungsjahr, besonderheiten
    """
    # Adjust prompt based on school type
    if 'grundschule' in school_type.lower() or (len(schulnummer) >= 3 and schulnummer[2] == 'G'):
        school_context = "einer Berliner Grundschule (Klassen 1-6)"
        language_hint = "Bei Grundschulen: typischerweise Englisch ab Klasse 3, manchmal auch Französisch oder andere Sprachen"
    else:
        school_context = "einer Berliner weiterführenden Schule"
        language_hint = "Erste, zweite, dritte Fremdsprache und ggf. AGs"

    prompt = f"""Analysiere den folgenden Text von der Website {school_context} und extrahiere die folgenden Informationen.
Antworte NUR im JSON-Format ohne zusätzlichen Text oder Markdown-Formatierung.

Schule: {schulname} (Schulnummer: {schulnummer})

Zu extrahierende Felder:
- sprachen: Angebotene Fremdsprachen ({language_hint}). Kommagetrennt, z.B. "Englisch, Französisch, Spanisch". Nur Sprachen, keine Sprachzertifikate.
- gruendungsjahr: Gründungsjahr der Schule (nur 4-stellige Jahreszahl, z.B. 1920)
- besonderheiten: Besondere Programme, Schwerpunkte oder Auszeichnungen (kurz, max 150 Zeichen). Z.B. "Musikalische Grundschule, Klimaschule, JÜL-Konzept"

Wenn eine Information nicht eindeutig gefunden werden kann, setze den Wert auf null.

Website-Text:
{text_content}

Antworte nur mit dem JSON-Objekt:"""

    try:
        with api_lock:
            response = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=500
            )
            time.sleep(0.2)  # Rate limiting

        response_text = response.choices[0].message.content.strip()

        # Try to extract JSON (handle markdown code blocks)
        if response_text.startswith("```"):
            # Remove markdown code block
            response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
            response_text = re.sub(r'\n?```$', '', response_text)

        json_match = re.search(r'\{[^{}]*\}', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

        data = json.loads(response_text)
        return data

    except json.JSONDecodeError as e:
        logger.debug(f"JSON parse error for {schulnummer}: {e}")
        return {}
    except Exception as e:
        logger.debug(f"OpenAI API error for {schulnummer}: {e}")
        return {}


def process_school(row: Dict) -> Dict[str, Any]:
    """Process a single school: fetch website and extract metadata."""
    schulnummer = row['schulnummer']
    schulname = row.get('schulname', '')
    website = row.get('website', '')
    school_type = row.get('school_type', 'Schule')

    result = {
        'schulnummer': schulnummer,
        'extracted_sprachen': None,
        'extracted_gruendungsjahr': None,
        'extracted_besonderheiten': None,
        'extraction_source': None,
    }

    # Skip if no website
    if not website or pd.isna(website) or website == '':
        return result

    # Clean URL
    url = str(website).split(';')[0].strip()
    if not url.startswith('http'):
        url = 'https://' + url

    try:
        time.sleep(REQUEST_DELAY)
        response = safe_request(url)

        if not response:
            return result

        # Clean HTML for LLM
        text_content = clean_html_for_llm(response.text)

        if len(text_content) < 100:
            return result

        # Extract with OpenAI
        extracted = extract_metadata_with_openai(text_content, schulname, schulnummer, school_type)

        if extracted:
            result['extraction_source'] = 'openai_website'

            # Map extracted data with validation
            if extracted.get('sprachen'):
                sprachen = str(extracted['sprachen']).strip()
                # Basic validation - should contain language names
                if sprachen and sprachen.lower() != 'null' and len(sprachen) > 2:
                    result['extracted_sprachen'] = sprachen

            if extracted.get('gruendungsjahr'):
                try:
                    year = int(extracted['gruendungsjahr'])
                    if 1800 <= year <= datetime.now().year:
                        result['extracted_gruendungsjahr'] = year
                except (ValueError, TypeError):
                    pass

            if extracted.get('besonderheiten'):
                besonderheiten = str(extracted['besonderheiten']).strip()
                if besonderheiten and besonderheiten.lower() != 'null' and len(besonderheiten) > 3:
                    result['extracted_besonderheiten'] = besonderheiten[:200]

    except Exception as e:
        logger.debug(f"Error processing {schulnummer}: {e}")

    return result


def enrich_with_website_metadata(
    input_path: Path,
    output_path: Optional[Path] = None,
    only_missing: bool = True,
    max_schools: Optional[int] = None
) -> pd.DataFrame:
    """
    Extract metadata from school websites and add to school data.

    Args:
        input_path: Path to school data (parquet or CSV)
        output_path: Path to save enriched data (default: overwrite input)
        only_missing: Only process schools missing sprachen/gruendungsjahr/besonderheiten
        max_schools: Maximum number of schools to process (for testing)

    Returns:
        Enriched DataFrame
    """
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path else input_path

    # Check OpenAI availability
    if not OPENAI_AVAILABLE:
        logger.error("openai not installed. Install with: pip install openai")
        return None

    api_key = get_openai_api_key()
    if not api_key:
        logger.error("OPENAI_API_KEY not found in environment or config.yaml")
        return None

    if not init_openai(api_key):
        return None

    # Load school data
    logger.info(f"Loading school data from: {input_path}")
    if input_path.suffix == '.parquet':
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path)

    logger.info(f"  Loaded {len(df)} schools")

    # Ensure target columns exist
    for col in ['sprachen', 'gruendungsjahr', 'besonderheiten']:
        if col not in df.columns:
            df[col] = None

    # Filter schools to process
    if only_missing:
        # Schools missing any of the three target fields
        missing_mask = (
            (df['sprachen'].isna() | (df['sprachen'] == '')) |
            (df['gruendungsjahr'].isna()) |
            (df['besonderheiten'].isna() | (df['besonderheiten'] == ''))
        )
        # Also need a website to process
        has_website = df['website'].notna() & (df['website'] != '')
        to_process = df[missing_mask & has_website].copy()
        logger.info(f"  Schools missing metadata (with website): {len(to_process)}")
    else:
        has_website = df['website'].notna() & (df['website'] != '')
        to_process = df[has_website].copy()
        logger.info(f"  Schools with website: {len(to_process)}")

    if max_schools:
        to_process = to_process.head(max_schools)
        logger.info(f"  Limited to {max_schools} schools for testing")

    if len(to_process) == 0:
        logger.info("No schools to process!")
        return df

    # Process schools
    schools_to_process = to_process.to_dict('records')
    results = []

    logger.info(f"\nProcessing {len(schools_to_process)} schools...")

    if TQDM_AVAILABLE:
        pbar = tqdm(total=len(schools_to_process), desc="Extracting metadata", unit="school")
    else:
        pbar = None

    # Process with thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_school = {
            executor.submit(process_school, school): school
            for school in schools_to_process
        }

        for future in as_completed(future_to_school):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.debug(f"Error: {e}")

            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    # Create results DataFrame and merge
    df_results = pd.DataFrame(results)

    # Merge extracted data back into main DataFrame
    # Only update fields that are currently empty
    for _, row in df_results.iterrows():
        schulnummer = row['schulnummer']
        idx = df[df['schulnummer'] == schulnummer].index

        if len(idx) == 0:
            continue

        idx = idx[0]

        # Update sprachen if extracted and currently empty
        if pd.notna(row['extracted_sprachen']) and (pd.isna(df.at[idx, 'sprachen']) or df.at[idx, 'sprachen'] == ''):
            df.at[idx, 'sprachen'] = row['extracted_sprachen']

        # Update gruendungsjahr if extracted and currently empty
        if pd.notna(row['extracted_gruendungsjahr']) and pd.isna(df.at[idx, 'gruendungsjahr']):
            df.at[idx, 'gruendungsjahr'] = row['extracted_gruendungsjahr']

        # Update besonderheiten if extracted and currently empty
        if pd.notna(row['extracted_besonderheiten']) and (pd.isna(df.at[idx, 'besonderheiten']) or df.at[idx, 'besonderheiten'] == ''):
            df.at[idx, 'besonderheiten'] = row['extracted_besonderheiten']

    # Save output
    logger.info(f"\nSaving enriched data to: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix == '.parquet':
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)

    return df


def print_summary(df: pd.DataFrame, df_results: pd.DataFrame = None):
    """Print summary of enrichment results."""
    print("\n" + "="*60)
    print("WEBSITE METADATA ENRICHMENT SUMMARY")
    print("="*60)

    print(f"\nTotal schools: {len(df)}")

    # Coverage for target columns
    print("\nColumn Coverage:")
    for col in ['sprachen', 'gruendungsjahr', 'besonderheiten']:
        if col in df.columns:
            if col == 'gruendungsjahr':
                count = df[col].notna().sum()
            else:
                count = (df[col].notna() & (df[col] != '')).sum()
            pct = 100 * count / len(df)
            print(f"  - {col}: {count}/{len(df)} ({pct:.1f}%)")

    # Sample data
    print("\nSample schools with extracted data:")
    sample_cols = ['schulname', 'schulnummer', 'sprachen', 'gruendungsjahr', 'besonderheiten']
    available_cols = [c for c in sample_cols if c in df.columns]

    # Show schools with sprachen filled
    sample = df[df['sprachen'].notna() & (df['sprachen'] != '')][available_cols].head(5)
    if len(sample) > 0:
        print(sample.to_string(index=False))

    print("\n" + "="*60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract metadata from Berlin school websites using LLM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Add metadata to primary school parquet (only missing)
    python3 enrich_berlin_schools_with_website_metadata.py \\
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet

    # Process all schools (not just missing)
    python3 enrich_berlin_schools_with_website_metadata.py \\
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet \\
        --all

    # Test with limited schools
    python3 enrich_berlin_schools_with_website_metadata.py \\
        --input data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet \\
        --max-schools 10
        """
    )

    parser.add_argument(
        "--input", "-i",
        type=str,
        required=True,
        help="Path to school data file (parquet or CSV)"
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Path to save enriched data (default: overwrite input)"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all schools, not just those missing data"
    )

    parser.add_argument(
        "--max-schools",
        type=int,
        default=None,
        help="Maximum number of schools to process (for testing)"
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip printing summary statistics"
    )

    args = parser.parse_args()

    try:
        # Resolve paths
        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = PROJECT_ROOT / input_path

        output_path = None
        if args.output:
            output_path = Path(args.output)
            if not output_path.is_absolute():
                output_path = PROJECT_ROOT / output_path

        # Run enrichment
        df_enriched = enrich_with_website_metadata(
            input_path=input_path,
            output_path=output_path,
            only_missing=not args.all,
            max_schools=args.max_schools
        )

        if df_enriched is not None:
            # Print summary
            if not args.no_summary:
                print_summary(df_enriched)

            logger.info("Website metadata enrichment completed successfully")
            return 0
        else:
            return 1

    except Exception as e:
        logger.error(f"Error during enrichment: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
