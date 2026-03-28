#!/usr/bin/env python3
"""
Scrapes metadata for schools missing data using Gemini 2.5 Flash LLM.

Features:
- Uses Gemini 2.5 Flash to intelligently extract school metadata from websites
- Progress bar with tqdm
- Parallelization with ThreadPoolExecutor
- Automatic retries on failures
- Interim saving every N schools
- Resume capability from last checkpoint

Target fields to extract:
- schueler (student count)
- lehrer (teacher count)
- sprachen (languages offered)
- bezirk (district)
- abitur_durchschnitt (Abitur average grade)
- gruendungsjahr (founding year)
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import re
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import traceback
from dotenv import load_dotenv

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Install with: pip install tqdm")

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("Error: google-generativeai not installed. Install with: pip install google-generativeai")

# Load environment variables
load_dotenv()

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "scraped_missing_metadata_llm.csv")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "scraper_llm_checkpoint.json")
LOG_FILE = os.path.join(BASE_DIR, "scraper_llm_log.txt")

# Gemini configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.5-flash"

# Request settings
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
}

# Parallelization settings
MAX_WORKERS = 3  # Lower for LLM rate limits
REQUEST_DELAY = 1.0  # Seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 2
SAVE_INTERVAL = 5  # Save more frequently with LLM

# Thread-safe resources
log_lock = Lock()
results_lock = Lock()
gemini_lock = Lock()  # Serialize Gemini API calls to avoid rate limits

# Initialize Gemini
gemini_model = None


def init_gemini():
    """Initialize the Gemini model."""
    global gemini_model
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not set. Please set it in .env file or environment.")

    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel(GEMINI_MODEL)
    return gemini_model


def log(message, also_print=False):
    """Thread-safe logging to file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    with log_lock:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_msg + '\n')
        if also_print:
            print(log_msg)


def safe_request(url, timeout=15, retries=MAX_RETRIES):
    """Make a safe HTTP request with retries."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                log(f"Request failed after {retries} attempts for {url}: {e}")
                return None
    return None


def clean_html_for_llm(html_content, max_chars=15000):
    """
    Clean HTML content for LLM processing.
    Remove scripts, styles, and extract meaningful text.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe', 'noscript']):
        element.decompose()

    # Get text
    text = soup.get_text(separator=' ', strip=True)

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)

    # Truncate if too long
    if len(text) > max_chars:
        text = text[:max_chars] + "..."

    return text


def determine_traegerschaft(schulnummer, schulname):
    """Determine if school is public or private based on schulnummer pattern."""
    if not schulnummer:
        return None

    schulnummer = str(schulnummer).upper().strip()

    if len(schulnummer) >= 3:
        type_char = schulnummer[2]
        if type_char == 'P':
            return 'Privat'
        elif type_char in ['K', 'Y', 'S', 'B', 'A']:
            return 'Öffentlich'

    return 'Öffentlich'


def extract_with_gemini(text_content, schulname, schulnummer):
    """
    Use Gemini 2.5 Flash to extract school metadata from website text.
    """
    prompt = f"""Analysiere den folgenden Text von der Website einer Berliner Schule und extrahiere die folgenden Informationen.
Antworte NUR im JSON-Format ohne zusätzlichen Text oder Markdown-Formatierung.

Schule: {schulname} (Schulnummer: {schulnummer})

Zu extrahierende Felder:
- schueler: Anzahl der Schüler/Schülerinnen (nur Zahl, z.B. 850)
- lehrer: Anzahl der Lehrer/Lehrkräfte (nur Zahl, z.B. 65)
- sprachen: Angebotene Fremdsprachen (kommagetrennt, z.B. "Englisch, Französisch, Spanisch")
- gruendungsjahr: Gründungsjahr der Schule (nur Jahr, z.B. 1920)
- abitur_durchschnitt: Durchschnittliche Abiturnote (z.B. 2.3)
- besonderheiten: Besondere Programme oder Schwerpunkte (kurz, max 100 Zeichen)

Wenn eine Information nicht gefunden werden kann, setze den Wert auf null.

Website-Text:
{text_content}

Antworte nur mit dem JSON-Objekt:"""

    try:
        with gemini_lock:  # Serialize API calls
            response = gemini_model.generate_content(prompt)
            time.sleep(0.5)  # Small delay between API calls

        # Extract JSON from response
        response_text = response.text.strip()

        # Try to find JSON in response
        json_match = re.search(r'\{[^{}]*\}', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

        # Parse JSON
        data = json.loads(response_text)
        return data

    except json.JSONDecodeError as e:
        log(f"JSON parse error for {schulnummer}: {e}")
        log(f"Response was: {response_text[:500] if 'response_text' in dir() else 'N/A'}")
        return {}
    except Exception as e:
        log(f"Gemini API error for {schulnummer}: {e}")
        return {}


def scrape_school_with_llm(row):
    """Scrape school website and extract metadata using Gemini."""
    schulnummer = row['schulnummer']
    schulname = row['schulname']
    website = row.get('website', '')

    log(f"Processing: {schulnummer} - {schulname[:50]}")

    combined_data = {
        'schulnummer': schulnummer,
        'scraped_schueler': None,
        'scraped_lehrer': None,
        'scraped_sprachen': None,
        'scraped_bezirk': None,
        'scraped_abitur': None,
        'scraped_traegerschaft': None,
        'scraped_gruendungsjahr': None,
        'scraped_besonderheiten': None,
        'data_source': None,
    }

    # Always set traegerschaft from schulnummer
    combined_data['scraped_traegerschaft'] = determine_traegerschaft(schulnummer, schulname)

    # Skip if no website
    if not website or pd.isna(website) or website == '':
        log(f"  No website for {schulnummer}")
        return combined_data

    # Clean URL
    url = str(website).split(';')[0].strip()
    if not url.startswith('http'):
        url = 'https://' + url

    try:
        # Fetch website
        time.sleep(REQUEST_DELAY)
        response = safe_request(url)

        if not response:
            log(f"  Failed to fetch website for {schulnummer}")
            return combined_data

        # Clean HTML for LLM
        text_content = clean_html_for_llm(response.text)

        if len(text_content) < 100:
            log(f"  Website content too short for {schulnummer}")
            return combined_data

        # Extract with Gemini
        extracted = extract_with_gemini(text_content, schulname, schulnummer)

        if extracted:
            combined_data['data_source'] = 'gemini_website'

            # Map extracted data
            if extracted.get('schueler'):
                try:
                    combined_data['scraped_schueler'] = int(extracted['schueler'])
                except (ValueError, TypeError):
                    pass

            if extracted.get('lehrer'):
                try:
                    combined_data['scraped_lehrer'] = int(extracted['lehrer'])
                except (ValueError, TypeError):
                    pass

            if extracted.get('sprachen'):
                combined_data['scraped_sprachen'] = str(extracted['sprachen'])

            if extracted.get('gruendungsjahr'):
                try:
                    year = int(extracted['gruendungsjahr'])
                    if 1800 <= year <= datetime.now().year:
                        combined_data['scraped_gruendungsjahr'] = year
                except (ValueError, TypeError):
                    pass

            if extracted.get('abitur_durchschnitt'):
                try:
                    grade = float(str(extracted['abitur_durchschnitt']).replace(',', '.'))
                    if 1.0 <= grade <= 4.0:
                        combined_data['scraped_abitur'] = grade
                except (ValueError, TypeError):
                    pass

            if extracted.get('besonderheiten'):
                combined_data['scraped_besonderheiten'] = str(extracted['besonderheiten'])[:200]

        # Log what was found
        found = [k.replace('scraped_', '') for k, v in combined_data.items()
                 if v is not None and k.startswith('scraped_') and k != 'scraped_traegerschaft']
        log(f"  Found for {schulnummer}: {found if found else 'nothing'}")

    except Exception as e:
        log(f"Error processing {schulnummer}: {str(e)}\n{traceback.format_exc()}")

    return combined_data


def save_checkpoint(results, processed_ids):
    """Save checkpoint for resume capability."""
    checkpoint = {
        'processed_ids': list(processed_ids),
        'timestamp': datetime.now().isoformat(),
    }
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f)

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')


def load_checkpoint():
    """Load checkpoint if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def main():
    """Main function to scrape missing school metadata using Gemini."""

    # Check dependencies
    if not GEMINI_AVAILABLE:
        print("Please install google-generativeai: pip install google-generativeai")
        return

    if not GEMINI_API_KEY:
        print("Please set GEMINI_API_KEY in .env file or environment variable.")
        print("Get your API key from: https://aistudio.google.com/app/apikey")
        return

    # Initialize Gemini
    print("Initializing Gemini 2.5 Flash...")
    try:
        init_gemini()
        print(f"  Model: {GEMINI_MODEL}")
    except Exception as e:
        print(f"Failed to initialize Gemini: {e}")
        return

    # Initialize log file
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"LLM Scraping started at {datetime.now()}\n")
        f.write(f"Model: {GEMINI_MODEL}\n")
        f.write("=" * 60 + "\n\n")

    print("=" * 70)
    print("Scraping Missing School Metadata with Gemini 2.5 Flash")
    print("=" * 70)
    print(f"Workers: {MAX_WORKERS} | Save interval: {SAVE_INTERVAL}")

    # Load the combined data
    df = pd.read_csv(INPUT_FILE)

    # Filter to schools missing metadata
    missing = df[df['metadata_source'].isna()].copy()

    print(f"\nTotal schools missing metadata: {len(missing)}")

    # Check for checkpoint (resume capability)
    checkpoint = load_checkpoint()
    processed_ids = set()
    results = []

    if checkpoint:
        processed_ids = set(checkpoint.get('processed_ids', []))
        if os.path.exists(OUTPUT_FILE):
            existing_results = pd.read_csv(OUTPUT_FILE)
            results = existing_results.to_dict('records')
        print(f"Resuming from checkpoint: {len(processed_ids)} already processed")

    # Filter out already processed
    to_process = missing[~missing['schulnummer'].isin(processed_ids)]

    print(f"Schools to process: {len(to_process)}")
    print()

    if len(to_process) == 0:
        print("All schools already processed!")
        return

    # Convert to list of dicts
    schools_to_process = to_process.to_dict('records')

    # Progress bar
    if TQDM_AVAILABLE:
        pbar = tqdm(total=len(schools_to_process), desc="Scraping with LLM", unit="school")
    else:
        pbar = None

    # Process with thread pool (lower concurrency for LLM)
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_school = {executor.submit(scrape_school_with_llm, school): school
                           for school in schools_to_process}

        for future in as_completed(future_to_school):
            school = future_to_school[future]
            try:
                result = future.result()
                with results_lock:
                    results.append(result)
                    processed_ids.add(school['schulnummer'])
                    completed += 1

                    # Interim save
                    if completed % SAVE_INTERVAL == 0:
                        save_checkpoint(results, processed_ids)
                        log(f"Checkpoint saved: {completed}/{len(schools_to_process)}", also_print=False)

            except Exception as e:
                log(f"Error processing {school['schulnummer']}: {e}", also_print=True)

            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    # Final save
    save_checkpoint(results, processed_ids)

    # Create results dataframe
    df_results = pd.DataFrame(results)

    # Summary
    print("\n" + "=" * 70)
    print("SCRAPING COMPLETE")
    print("=" * 70)
    print(f"Total schools processed: {len(df_results)}")
    print(f"\nData found:")
    print(f"  - schueler (students):      {df_results['scraped_schueler'].notna().sum():3d} / {len(df_results)}")
    print(f"  - lehrer (teachers):        {df_results['scraped_lehrer'].notna().sum():3d} / {len(df_results)}")
    print(f"  - sprachen (languages):     {df_results['scraped_sprachen'].notna().sum():3d} / {len(df_results)}")
    print(f"  - abitur (grade avg):       {df_results['scraped_abitur'].notna().sum():3d} / {len(df_results)}")
    print(f"  - traegerschaft (pub/priv): {df_results['scraped_traegerschaft'].notna().sum():3d} / {len(df_results)}")
    print(f"  - gruendungsjahr (founded): {df_results['scraped_gruendungsjahr'].notna().sum():3d} / {len(df_results)}")
    print(f"  - besonderheiten (special): {df_results['scraped_besonderheiten'].notna().sum():3d} / {len(df_results)}")

    print(f"\nResults saved to: {OUTPUT_FILE}")
    print(f"Log saved to: {LOG_FILE}")

    # Clean up checkpoint on success
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint file cleaned up.")

    return df_results


if __name__ == "__main__":
    main()
