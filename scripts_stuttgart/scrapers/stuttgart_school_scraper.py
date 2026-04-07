#!/usr/bin/env python3
"""
Stuttgart School Data Scraper
Scrapes school data from the official Stuttgart city directory at
https://www.stuttgart.de/organigramm/adressen

Strategy:
1. RSS feed search for "Schule" → get all directory entry URLs (~790)
2. Scrape each detail page for JSON metadata + HTML fields
3. Filter by Schulart to keep only actual schools (~258)
4. Classify into primary/secondary/sbbz/vocational
5. Supplement with jedeschule.codefor.de for coordinate fallback

Data fields per school:
- JSON: id, name, coordinates (WKT POINT), phone, email, address
- HTML: Schulart, Stadtbezirk, website URL, fax

Author: Stuttgart School Data Pipeline
Created: 2026-04-06
"""

import pandas as pd
import requests
import re
import json
import math
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_stuttgart"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
}

RSS_SEARCH_URL = (
    'https://www.stuttgart.de/organigramm/adressen'
    '?sp%3Afulltext%5B0%5D=Schule'
    '&sp%3Aout=rss'
    '&sp%3Acmp=search-1-0-searchResult'
    '&action=submit'
)

# School types to include in our pipeline
PRIMARY_SCHULARTEN = ['Grundschule']
SECONDARY_SCHULARTEN = [
    'Gymnasium', 'Realschule', 'Gemeinschaftsschule',
    'Werkrealschule', 'Waldorfschule',
]
COMBINED_SCHULARTEN = ['Grund- und Werkrealschule']  # both primary & secondary
SBBZ_SCHULARTEN = [
    'Förderschule (Lernen)', 'Förderschule (geistige Entwicklung)',
    'Förderschule (Sprache)', 'Förderschule (Hören, körperlich/motorisch)',
    'Sonderpädagogisches Bildungs- und Beratungszentrum',
]

# Rate limiting
SCRAPE_DELAY = 0.15  # seconds between detail page requests
MAX_WORKERS = 10


def get_all_directory_urls() -> List[str]:
    """Fetch all directory entry URLs via RSS feed search."""
    logger.info("Fetching directory URLs via RSS feed...")

    r = requests.get(RSS_SEARCH_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    urls = re.findall(
        r'<link>(https://www\.stuttgart\.de/organigramm/adresse/[^<]+)</link>',
        r.text
    )
    logger.info(f"Found {len(urls)} directory entries")
    return urls


def scrape_detail_page(url: str) -> Optional[Dict]:
    """Scrape a single detail page for school data."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None

        html = r.text
        school = {'url': url}

        # 1. Extract JSON from data-content attribute
        data_matches = re.findall(r'data-content="([^"]+)"', html)
        for dm in data_matches:
            try:
                j = json.loads(dm.replace('&quot;', '"').replace('&amp;', '&'))
                if j.get('objecttype') == 'citygovOrganisation':
                    school['id'] = j.get('id')
                    school['name'] = j.get('name', '').strip()
                    school['phone'] = j.get('citygov_phone', '').strip()
                    school['email'] = j.get('citygov_email', '').strip()
                    school['address_raw'] = j.get('citygov_address', '').strip()

                    # Parse coordinates from WKT POINT
                    geo = j.get('geo_points', [])
                    if geo and isinstance(geo, list):
                        wkt = geo[0]
                        m = re.match(r'POINT\(([\d.]+)\s+([\d.]+)\)', wkt)
                        if m:
                            school['longitude'] = float(m.group(1))
                            school['latitude'] = float(m.group(2))
                    break
            except (json.JSONDecodeError, KeyError):
                continue

        if not school.get('name'):
            return None

        # 2. Extract Schulart from HTML table
        schulart_match = re.search(
            r'<th[^>]*>\s*Schulart:?\s*</th>\s*<td[^>]*>\s*(.*?)\s*</td>',
            html, re.DOTALL | re.IGNORECASE
        )
        if schulart_match:
            school['schulart'] = re.sub(r'<[^>]+>', '', schulart_match.group(1)).strip()

        # 3. Extract Stadtbezirk from HTML table
        bezirk_match = re.search(
            r'<th[^>]*>\s*Stadtbezirk:?\s*</th>\s*<td[^>]*>\s*(.*?)\s*</td>',
            html, re.DOTALL | re.IGNORECASE
        )
        if bezirk_match:
            school['stadtbezirk'] = re.sub(r'<[^>]+>', '', bezirk_match.group(1)).strip()

        # 4. Extract external website URL (first non-stuttgart.de, non-google, non-vvs link)
        external_links = re.findall(r'href="(https?://[^"]+)"', html)
        skip_domains = ['stuttgart.de', 'google.com', 'vvs.de', 'maps.stuttgart.de',
                        'radroutenplaner', 'matomo']
        for link in external_links:
            link_clean = link.replace('&amp;', '&')
            if not any(d in link_clean.lower() for d in skip_domains):
                school['website'] = link_clean
                break

        # 5. Extract fax from HTML
        fax_match = re.search(r'Fax:?\s*([\d\s\-/]+)', html)
        if fax_match:
            school['fax'] = fax_match.group(1).strip()

        # 6. Parse address into street + PLZ
        addr = school.get('address_raw', '')
        if addr:
            # Format: "Straße Nr" or "Straße Nr, PLZ Stadt"
            plz_match = re.search(r'(\d{5})\s*(Stuttgart)?', html)
            if plz_match:
                school['plz'] = plz_match.group(1)

            # The citygov_address usually just has the street
            school['strasse'] = addr.strip()

        # If PLZ not found in address, search the full page text
        if 'plz' not in school:
            plz_matches = re.findall(r'(70\d{3})\s+Stuttgart', html)
            if plz_matches:
                school['plz'] = plz_matches[0]

        return school

    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return None


def scrape_all_schools() -> List[Dict]:
    """Scrape all school entries from the Stuttgart directory."""
    # Check cache first
    cache_file = CACHE_DIR / "stuttgart_directory_schools.json"
    if cache_file.exists():
        age = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
        if age < 7:
            logger.info(f"Using cached directory data ({age:.0f} days old)")
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)

    # Get all URLs
    all_urls = get_all_directory_urls()

    # Scrape all detail pages
    logger.info(f"Scraping {len(all_urls)} detail pages...")
    all_entries = []

    def scrape_with_delay(url):
        time.sleep(SCRAPE_DELAY)
        return scrape_detail_page(url)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_with_delay, url): url for url in all_urls}

        iterator = as_completed(futures)
        if TQDM_AVAILABLE:
            iterator = tqdm(iterator, total=len(futures), desc="Scraping directory")

        for future in iterator:
            result = future.result()
            if result:
                all_entries.append(result)

    logger.info(f"Scraped {len(all_entries)} entries total")

    # Filter to actual schools (entries with Schulart field)
    schools = [e for e in all_entries if e.get('schulart')]
    logger.info(f"Entries with Schulart: {len(schools)}")

    # Cache results
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(schools, f, ensure_ascii=False, indent=2)

    return schools


def classify_school(schulart: str) -> Optional[str]:
    """Classify school into primary/secondary based on Schulart."""
    if not schulart:
        return None

    schulart_lower = schulart.lower()

    if schulart in PRIMARY_SCHULARTEN:
        return 'primary'
    if schulart in SECONDARY_SCHULARTEN:
        return 'secondary'
    if schulart in COMBINED_SCHULARTEN:
        return 'secondary'  # Grund- und Werkrealschule → count as secondary
    if any(s.lower() in schulart_lower for s in SBBZ_SCHULARTEN):
        return 'sbbz'
    if any(kw in schulart_lower for kw in ['förderschule', 'sonderpädagogisch']):
        return 'sbbz'
    if any(kw in schulart_lower for kw in ['beruf', 'gewerblich', 'kaufmännisch',
                                            'hauswirtschaft', 'landwirtschaft']):
        return 'vocational'

    # Check by name keywords
    if 'grundschule' in schulart_lower:
        return 'primary'
    if any(kw in schulart_lower for kw in ['gymnasium', 'realschule', 'gemeinschafts', 'waldorf']):
        return 'secondary'

    return None


def build_dataframes(schools: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build primary and secondary DataFrames from scraped data."""
    primary_records = []
    secondary_records = []

    for school in schools:
        schulart = school.get('schulart', '')
        classification = classify_school(schulart)

        if classification not in ('primary', 'secondary'):
            continue

        # Clean phone (take first number)
        phone = school.get('phone', '')
        if ',' in phone:
            phone = phone.split(',')[0].strip()

        # Clean website
        website = school.get('website', '')
        if website and not website.startswith('http'):
            website = f'https://{website}'

        record = {
            'schulnummer': f"STG-{school.get('id', 0)}",
            'schulname': school.get('name', ''),
            'school_type': classification,
            'schulart': schulart,
            'strasse': school.get('strasse', ''),
            'plz': school.get('plz', ''),
            'ort': 'Stuttgart',
            'ortsteil': school.get('stadtbezirk', ''),
            'bundesland': 'Baden-Württemberg',
            'stadt': 'Stuttgart',
            'latitude': school.get('latitude'),
            'longitude': school.get('longitude'),
            'telefon': phone,
            'fax': school.get('fax', ''),
            'email': school.get('email', ''),
            'website': website,
            'schulleitung': '',  # Not in directory; can add later
            'traegerschaft': 'Privat' if any(kw in school.get('name', '').lower()
                                              for kw in ['freie', 'waldorf', 'montessori',
                                                         'privat', 'kolping', 'merz',
                                                         'freien', 'evangelisch', 'katholisch'])
                             else 'Öffentlich',
            'data_source': 'stuttgart.de Adressverzeichnis',
            'data_retrieved': datetime.now().strftime('%Y-%m-%d'),
        }

        if classification == 'primary':
            primary_records.append(record)
        else:
            secondary_records.append(record)

    # Also add primary portion of Grund- und Werkrealschulen
    for school in schools:
        schulart = school.get('schulart', '')
        if schulart in COMBINED_SCHULARTEN:
            phone = school.get('phone', '')
            if ',' in phone:
                phone = phone.split(',')[0].strip()
            website = school.get('website', '')
            if website and not website.startswith('http'):
                website = f'https://{website}'

            record = {
                'schulnummer': f"STG-{school.get('id', 0)}-GS",
                'schulname': school.get('name', '') + ' (Grundschule)',
                'school_type': 'primary',
                'schulart': 'Grundschule',
                'strasse': school.get('strasse', ''),
                'plz': school.get('plz', ''),
                'ort': 'Stuttgart',
                'ortsteil': school.get('stadtbezirk', ''),
                'bundesland': 'Baden-Württemberg',
                'stadt': 'Stuttgart',
                'latitude': school.get('latitude'),
                'longitude': school.get('longitude'),
                'telefon': phone,
                'fax': school.get('fax', ''),
                'email': school.get('email', ''),
                'website': website,
                'schulleitung': '',
                'traegerschaft': 'Öffentlich',
                'data_source': 'stuttgart.de Adressverzeichnis',
                'data_retrieved': datetime.now().strftime('%Y-%m-%d'),
            }
            primary_records.append(record)

    primary_df = pd.DataFrame(primary_records)
    secondary_df = pd.DataFrame(secondary_records)

    # Dedup
    if len(primary_df) > 0:
        primary_df = primary_df.drop_duplicates(subset=['schulname'], keep='first')
    if len(secondary_df) > 0:
        secondary_df = secondary_df.drop_duplicates(subset=['schulname'], keep='first')

    return primary_df, secondary_df


def save_and_summarize(df: pd.DataFrame, school_type: str):
    """Save and print summary."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RAW_DIR / f"stuttgart_{school_type}_schools.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {csv_path}")

    print(f"\n{'='*70}")
    print(f"STUTTGART {school_type.upper()} SCHOOLS - SCRAPE COMPLETE")
    print(f"{'='*70}")
    print(f"Total schools: {len(df)}")
    print(f"With coordinates: {df['latitude'].notna().sum()}/{len(df)} "
          f"({100*df['latitude'].notna().sum()/max(len(df),1):.0f}%)")
    print(f"With phone: {(df['telefon'] != '').sum()}/{len(df)}")
    print(f"With email: {(df['email'] != '').sum()}/{len(df)}")
    print(f"With website: {(df['website'] != '').sum()}/{len(df)}")

    if 'schulart' in df.columns:
        print(f"\nBy Schulart:")
        for t, c in df['schulart'].value_counts().items():
            print(f"  {t}: {c}")

    if 'ortsteil' in df.columns:
        filled = (df['ortsteil'] != '').sum()
        print(f"\nDistrict coverage: {filled}/{len(df)}")
        if filled > 0:
            top = df[df['ortsteil'] != '']['ortsteil'].value_counts()
            for d, c in top.head(10).items():
                print(f"  {d}: {c}")

    if 'traegerschaft' in df.columns:
        print(f"\nBy operator:")
        for t, c in df['traegerschaft'].value_counts().items():
            print(f"  {t}: {c}")

    print(f"{'='*70}")


def main() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape both primary and secondary schools."""
    logger.info("=" * 60)
    logger.info("Starting Stuttgart School Scraper (stuttgart.de directory)")
    logger.info("=" * 60)

    schools = scrape_all_schools()

    # Log Schulart distribution
    schulart_counts = {}
    for s in schools:
        sa = s.get('schulart', 'Unknown')
        schulart_counts[sa] = schulart_counts.get(sa, 0) + 1
    logger.info("Schulart distribution:")
    for sa, c in sorted(schulart_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {sa}: {c}")

    primary_df, secondary_df = build_dataframes(schools)

    save_and_summarize(primary_df, 'primary')
    save_and_summarize(secondary_df, 'secondary')

    return primary_df, secondary_df


if __name__ == "__main__":
    main()
