#!/usr/bin/env python3
"""
Scrapes metadata for schools missing data in combined_schools_with_metadata.csv

Features:
- Progress bar with tqdm
- Parallelization with ThreadPoolExecutor
- Automatic retries on failures
- Interim saving every N schools
- Resume capability from last checkpoint

Data sources (in priority order):
1. bildung.berlin.de Schulporträt - Official government source
2. schulen.de - Comprehensive school database
3. Direct school websites - Fallback for remaining data

Target fields to collect:
- schueler (student count)
- lehrer (teacher count)
- sprachen (languages offered)
- bezirk (district)
- abitur_durchschnitt (Abitur average grade)
- traegerschaft (public/private indicator)
- gruendungsjahr (founding year)
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import re
import json
from urllib.parse import quote, urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import traceback

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Install with: pip install tqdm")

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "scraped_missing_metadata.csv")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "scraper_checkpoint.json")
LOG_FILE = os.path.join(BASE_DIR, "scraper_log.txt")

# Request settings
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
}

# Parallelization settings
MAX_WORKERS = 5  # Number of parallel threads
REQUEST_DELAY = 0.5  # Seconds between requests per thread
MAX_RETRIES = 3  # Number of retries per request
RETRY_DELAY = 2  # Seconds to wait before retry
SAVE_INTERVAL = 10  # Save checkpoint every N schools

# Thread-safe logging
log_lock = Lock()
results_lock = Lock()


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
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                log(f"Request failed after {retries} attempts for {url}: {e}")
                return None
    return None


def extract_number(text):
    """Extract first number from text."""
    if not text:
        return None
    match = re.search(r'(\d+[\.,]?\d*)', str(text).replace('.', '').replace(',', '.'))
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def determine_traegerschaft(schulnummer, schulname, page_text=''):
    """
    Determine if school is public or private based on schulnummer pattern and name.

    Schulnummer patterns:
    - XXK## = Public ISS (Kommunal)
    - XXY## = Public Gymnasium
    - XXS## = Public Sonderschule (special education)
    - XXB## = Public Berufschule
    - XXA## = Public (Willkommensschule)
    - XXP## = Private school (Privatschule)
    """
    if not schulnummer:
        return None

    # Check schulnummer pattern
    schulnummer = str(schulnummer).upper()

    if len(schulnummer) >= 3:
        type_char = schulnummer[2] if len(schulnummer) > 2 else ''

        if type_char == 'P':
            return 'Privat'
        elif type_char in ['K', 'Y', 'S', 'B', 'A']:
            return 'Öffentlich'

    # Fallback: check name for keywords
    name_lower = schulname.lower() if schulname else ''
    private_keywords = ['privat', 'freie schule', 'freie ', 'katholisch', 'evangelisch',
                        'jüdisch', 'christlich', 'waldorf', 'montessori', 'phorms',
                        'best-sabel', 'kant-schule', 'international school']

    for keyword in private_keywords:
        if keyword in name_lower:
            return 'Privat'

    # Check page text for clues
    if page_text:
        text_lower = page_text.lower()
        if 'freier träger' in text_lower or 'privatschule' in text_lower:
            return 'Privat'
        if 'öffentliche schule' in text_lower or 'staatliche schule' in text_lower:
            return 'Öffentlich'

    return 'Öffentlich'  # Default assumption


def extract_founding_year(page_text, schulname=''):
    """Extract founding/establishment year from text."""
    if not page_text:
        return None

    # Common patterns for founding year
    patterns = [
        r'gegründet\s+(?:im\s+(?:Jahr(?:e)?\s+)?)?(\d{4})',
        r'seit\s+(\d{4})',
        r'besteht\s+seit\s+(\d{4})',
        r'eröffnet\s+(?:im\s+(?:Jahr(?:e)?\s+)?)?(\d{4})',
        r'gründung(?:sjahr)?[:\s]+(\d{4})',
        r'(?:im\s+)?(?:jahr(?:e)?\s+)?(\d{4})\s+gegründet',
        r'(?:im\s+)?(?:jahr(?:e)?\s+)?(\d{4})\s+eröffnet',
        r'schulgeschichte.*?(\d{4})',
        r'tradition\s+seit\s+(\d{4})',
    ]

    for pattern in patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            # Sanity check: school founding year should be between 1800 and current year
            if 1800 <= year <= datetime.now().year:
                return year

    return None


# =============================================================================
# Source 1: bildung.berlin.de Schulporträt
# =============================================================================

def find_schulportrait_id(schulnummer, schulname):
    """Search for school on bildung.berlin.de and get the IDSchulzweig."""
    # Try searching by school number first
    search_url = f"https://www.bildung.berlin.de/Schulverzeichnis/SchulListe.aspx?SchulNr={schulnummer}"

    response = safe_request(search_url)
    if not response:
        # Try by name
        search_url = f"https://www.bildung.berlin.de/Schulverzeichnis/SchulListe.aspx?SchulName={quote(schulname[:30])}"
        response = safe_request(search_url)

    if not response:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Look for links to Schulportrait.aspx
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'Schulportrait.aspx' in href and 'IDSchulzweig=' in href:
            match = re.search(r'IDSchulzweig=(\d+)', href)
            if match:
                return match.group(1)

    return None


def scrape_bildung_berlin(schulnummer, schulname):
    """Scrape school data from bildung.berlin.de Schulporträt."""
    log(f"[bildung.berlin.de] Searching for {schulnummer}")

    portrait_id = find_schulportrait_id(schulnummer, schulname)

    if not portrait_id:
        return {}

    log(f"[bildung.berlin.de] Found portrait ID: {portrait_id} for {schulnummer}")

    portrait_url = f"https://www.bildung.berlin.de/Schulverzeichnis/Schulportrait.aspx?IDSchulzweig={portrait_id}"
    response = safe_request(portrait_url)

    if not response:
        return {}

    soup = BeautifulSoup(response.text, 'html.parser')
    data = {'source': 'bildung.berlin.de'}

    page_text = soup.get_text(separator=' ', strip=True)

    # Bezirk
    bezirk_match = re.search(r'Berlin\s*\(([^)]+)\)', page_text)
    if bezirk_match:
        data['bezirk'] = bezirk_match.group(1)

    # Student numbers
    schueler_match = re.search(r'Schüler(?:innen)?[:\s]+(\d+)', page_text, re.IGNORECASE)
    if schueler_match:
        data['schueler'] = int(schueler_match.group(1))

    # Teacher numbers
    lehrer_match = re.search(r'Lehrer(?:innen)?[:\s]+(\d+)', page_text, re.IGNORECASE)
    if lehrer_match:
        data['lehrer'] = int(lehrer_match.group(1))

    # Languages
    sprachen_patterns = [
        r'Fremdsprachen?[:\s]+([^.]+)',
        r'Sprachen[:\s]+([^.]+)',
    ]
    for pattern in sprachen_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            data['sprachen'] = match.group(1).strip()[:200]
            break

    # Traegerschaft
    if 'freier träger' in page_text.lower() or 'privatschule' in page_text.lower():
        data['traegerschaft'] = 'Privat'
    elif 'öffentliche' in page_text.lower():
        data['traegerschaft'] = 'Öffentlich'

    # Founding year
    founding_year = extract_founding_year(page_text, schulname)
    if founding_year:
        data['gruendungsjahr'] = founding_year

    return data


# =============================================================================
# Source 2: schulen.de
# =============================================================================

def search_schulen_de(schulname, bezirk=None):
    """Search for school on schulen.de and return its URL."""
    search_name = schulname.replace('(Integrierte Sekundarschule)', '').replace('(Gymnasium)', '').strip()
    search_name = search_name[:50]

    slug = search_name.lower()
    slug = re.sub(r'[^a-z0-9äöüß\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug)
    slug = slug.strip('-')

    possible_urls = [
        f"https://schulen.de/schulen/{slug}-berlin/",
        f"https://schulen.de/schulen/{slug}/",
    ]

    for url in possible_urls:
        response = safe_request(url, retries=1)
        if response and response.status_code == 200:
            return url

    return None


def scrape_schulen_de(schulnummer, schulname, bezirk=None):
    """Scrape school data from schulen.de."""
    log(f"[schulen.de] Searching for {schulname[:40]}")

    url = search_schulen_de(schulname, bezirk)

    if not url:
        return {}

    log(f"[schulen.de] Found: {url}")

    response = safe_request(url)
    if not response:
        return {}

    soup = BeautifulSoup(response.text, 'html.parser')
    data = {'source': 'schulen.de'}

    page_text = soup.get_text(separator=' ', strip=True)

    # Student count
    schueler_patterns = [
        r'(\d+)\s*Schüler',
        r'Schüler(?:zahl)?[:\s]+(\d+)',
        r'(\d+)\s*(?:Schülerinnen und Schüler|SchülerInnen)',
    ]
    for pattern in schueler_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            data['schueler'] = int(match.group(1))
            break

    # Teacher count
    lehrer_patterns = [
        r'(\d+)\s*Lehrer',
        r'Lehrer(?:zahl)?[:\s]+(\d+)',
        r'(\d+)\s*(?:Lehrerinnen und Lehrer|LehrerInnen)',
    ]
    for pattern in lehrer_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            data['lehrer'] = int(match.group(1))
            break

    # Languages
    languages_found = []
    language_keywords = ['Englisch', 'Französisch', 'Spanisch', 'Latein', 'Russisch',
                         'Italienisch', 'Chinesisch', 'Japanisch', 'Türkisch', 'Hebräisch']
    for lang in language_keywords:
        if lang.lower() in page_text.lower():
            languages_found.append(lang)
    if languages_found:
        data['sprachen'] = ', '.join(languages_found)

    # Founding year
    founding_year = extract_founding_year(page_text, schulname)
    if founding_year:
        data['gruendungsjahr'] = founding_year

    # Traegerschaft
    if 'privatschule' in page_text.lower() or 'freier träger' in page_text.lower():
        data['traegerschaft'] = 'Privat'

    return data


# =============================================================================
# Source 3: Direct School Website
# =============================================================================

def scrape_school_website(url, schulname):
    """Scrape data directly from school's website."""
    if not url or url == 'NO WEBSITE':
        return {}

    url = url.split(';')[0].strip()
    if not url.startswith('http'):
        url = 'https://' + url

    log(f"[website] Fetching {url}")

    response = safe_request(url)
    if not response:
        return {}

    soup = BeautifulSoup(response.text, 'html.parser')
    data = {'source': 'school_website'}

    page_text = soup.get_text(separator=' ', strip=True)

    # Student numbers
    schueler_patterns = [
        r'(?:etwa|ca\.?|rund|über|ungefähr)?\s*(\d+)\s*(?:Schüler|Kinder|Jugendliche)',
        r'Schüler(?:zahl|innen)?[:\s]+(?:etwa|ca\.?|rund)?\s*(\d+)',
        r'(\d+)\s*Schülerinnen und Schüler',
    ]
    for pattern in schueler_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            num = int(match.group(1))
            if 10 < num < 5000:
                data['schueler'] = num
                break

    # Teacher numbers
    lehrer_patterns = [
        r'(\d+)\s*(?:Lehrer|Lehrkräfte|Pädagog)',
        r'Lehrer(?:zahl)?[:\s]+(\d+)',
        r'Kollegium[:\s]+(\d+)',
    ]
    for pattern in lehrer_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            num = int(match.group(1))
            if 5 < num < 500:
                data['lehrer'] = num
                break

    # Languages
    languages_found = []
    language_keywords = ['Englisch', 'Französisch', 'Spanisch', 'Latein', 'Russisch',
                         'Italienisch', 'Chinesisch', 'Japanisch', 'Türkisch', 'Hebräisch',
                         'Polnisch', 'Arabisch', 'Griechisch']
    for lang in language_keywords:
        if lang.lower() in page_text.lower():
            languages_found.append(lang)
    if languages_found:
        data['sprachen'] = ', '.join(languages_found)

    # Abitur average
    abitur_patterns = [
        r'Abitur(?:durchschnitt|note)?[:\s]+(\d[,\.]\d+)',
        r'Durchschnitt(?:snote)?[:\s]+(\d[,\.]\d+)',
    ]
    for pattern in abitur_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            grade = float(match.group(1).replace(',', '.'))
            if 1.0 <= grade <= 4.0:
                data['abitur_durchschnitt'] = grade
                break

    # Founding year
    founding_year = extract_founding_year(page_text, schulname)
    if founding_year:
        data['gruendungsjahr'] = founding_year

    return data


# =============================================================================
# Main Scraper Logic
# =============================================================================

def scrape_school(row):
    """Scrape all available data for a single school from multiple sources."""
    schulnummer = row['schulnummer']
    schulname = row['schulname']
    website = row.get('website', '')
    ortsteil = row.get('ortsteil', '')

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
        'data_sources': [],
    }

    all_page_text = ''

    try:
        # Source 1: bildung.berlin.de
        time.sleep(REQUEST_DELAY)
        bildung_data = scrape_bildung_berlin(schulnummer, schulname)
        if bildung_data:
            combined_data['data_sources'].append('bildung.berlin.de')
            for key in ['schueler', 'lehrer', 'sprachen', 'bezirk', 'traegerschaft', 'gruendungsjahr']:
                scraped_key = f'scraped_{key}' if key != 'traegerschaft' and key != 'gruendungsjahr' else f'scraped_{key}'
                if key in bildung_data and combined_data.get(f'scraped_{key}') is None:
                    combined_data[f'scraped_{key}'] = bildung_data[key]

        # Source 2: schulen.de
        if combined_data['scraped_schueler'] is None or combined_data['scraped_lehrer'] is None:
            time.sleep(REQUEST_DELAY)
            schulen_data = scrape_schulen_de(schulnummer, schulname, ortsteil)
            if schulen_data:
                combined_data['data_sources'].append('schulen.de')
                for key in ['schueler', 'lehrer', 'sprachen', 'traegerschaft', 'gruendungsjahr']:
                    if key in schulen_data and combined_data.get(f'scraped_{key}') is None:
                        combined_data[f'scraped_{key}'] = schulen_data[key]

        # Source 3: Direct school website
        if combined_data['scraped_schueler'] is None or combined_data['scraped_sprachen'] is None:
            if website and pd.notna(website):
                time.sleep(REQUEST_DELAY)
                website_data = scrape_school_website(website, schulname)
                if website_data:
                    combined_data['data_sources'].append('school_website')
                    for key in ['schueler', 'lehrer', 'sprachen', 'abitur_durchschnitt', 'gruendungsjahr']:
                        target_key = 'scraped_abitur' if key == 'abitur_durchschnitt' else f'scraped_{key}'
                        if key in website_data and combined_data.get(target_key) is None:
                            combined_data[target_key] = website_data[key]

        # Determine traegerschaft from schulnummer if not found
        if combined_data['scraped_traegerschaft'] is None:
            combined_data['scraped_traegerschaft'] = determine_traegerschaft(schulnummer, schulname, all_page_text)

    except Exception as e:
        log(f"Error processing {schulnummer}: {str(e)}\n{traceback.format_exc()}")

    combined_data['data_sources'] = ', '.join(combined_data['data_sources']) if combined_data['data_sources'] else None

    return combined_data


def save_checkpoint(results, processed_ids):
    """Save checkpoint for resume capability."""
    checkpoint = {
        'processed_ids': list(processed_ids),
        'timestamp': datetime.now().isoformat(),
    }
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f)

    # Save results
    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')


def load_checkpoint():
    """Load checkpoint if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def main():
    """Main function to scrape missing school metadata."""

    # Initialize log file
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"Scraping started at {datetime.now()}\n")
        f.write("=" * 60 + "\n\n")

    print("=" * 70)
    print("Scraping Missing School Metadata")
    print("=" * 70)
    print(f"Workers: {MAX_WORKERS} | Retries: {MAX_RETRIES} | Save interval: {SAVE_INTERVAL}")

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
        # Load existing results
        if os.path.exists(OUTPUT_FILE):
            existing_results = pd.read_csv(OUTPUT_FILE)
            results = existing_results.to_dict('records')
        print(f"Resuming from checkpoint: {len(processed_ids)} already processed")

    # Filter out already processed
    to_process = missing[~missing['schulnummer'].isin(processed_ids)]

    print(f"Schools to process: {len(to_process)}")
    print(f"Sources: bildung.berlin.de -> schulen.de -> school websites")
    print()

    if len(to_process) == 0:
        print("All schools already processed!")
        return

    # Convert to list of dicts for processing
    schools_to_process = to_process.to_dict('records')

    # Progress bar setup
    if TQDM_AVAILABLE:
        pbar = tqdm(total=len(schools_to_process), desc="Scraping", unit="school")
    else:
        pbar = None

    # Process with thread pool
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_school = {executor.submit(scrape_school, school): school for school in schools_to_process}

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
    print(f"  - schueler (students):    {df_results['scraped_schueler'].notna().sum():3d} / {len(df_results)}")
    print(f"  - lehrer (teachers):      {df_results['scraped_lehrer'].notna().sum():3d} / {len(df_results)}")
    print(f"  - sprachen (languages):   {df_results['scraped_sprachen'].notna().sum():3d} / {len(df_results)}")
    print(f"  - bezirk (district):      {df_results['scraped_bezirk'].notna().sum():3d} / {len(df_results)}")
    print(f"  - abitur (grade avg):     {df_results['scraped_abitur'].notna().sum():3d} / {len(df_results)}")
    print(f"  - traegerschaft (pub/priv): {df_results['scraped_traegerschaft'].notna().sum():3d} / {len(df_results)}")
    print(f"  - gruendungsjahr (founded): {df_results['scraped_gruendungsjahr'].notna().sum():3d} / {len(df_results)}")

    print(f"\nResults saved to: {OUTPUT_FILE}")
    print(f"Log saved to: {LOG_FILE}")

    # Clean up checkpoint file on success
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint file cleaned up.")

    return df_results


if __name__ == "__main__":
    main()
