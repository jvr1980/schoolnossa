#!/usr/bin/env python3
"""
Hamburg Abitur Statistics Scraper (Comprehensive Version)
Phase 2: Scrapes complete school data from gymnasium-hamburg.net

Data Source: https://gymnasium-hamburg.net
- Abitur grade averages from /alumni/{year} pages
- School details from individual school pages
- School IDs from /schulnummern
- Addresses from individual school pages

This script:
1. Scrapes full Abitur rankings for multiple years (61+ schools per year)
2. Scrapes individual school pages for detailed info
3. Extracts: school ID, address, PLZ, district, status (public/private),
   homepage, email, languages, student numbers
4. Outputs: hamburg_abitur_statistics.csv with comprehensive data

Author: Hamburg School Data Pipeline
Created: 2026-02-01
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data source URLs
BASE_URL = "https://gymnasium-hamburg.net"

# Headers for requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
}

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

# Years to scrape for Abitur data
YEARS_TO_SCRAPE = [2024, 2023, 2022, 2021, 2020, 2019]


def get_soup(url: str, retries: int = 3) -> Optional[BeautifulSoup]:
    """Fetch URL and return BeautifulSoup object."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Failed to fetch {url} after {retries} attempts")
                return None
    return None


def get_page_text(url: str) -> str:
    """Get plain text content from a URL."""
    soup = get_soup(url)
    if soup:
        return soup.get_text(separator=' ', strip=True)
    return ""


def scrape_abitur_year(year: int) -> List[Dict]:
    """
    Scrape full Abitur rankings for a specific year.
    Returns list of dicts with school name, district, and grade.
    """
    url = f"{BASE_URL}/alumni/{year}"
    logger.info(f"Scraping Abitur {year} from {url}")

    text = get_page_text(url)
    if not text:
        logger.warning(f"Failed to get content for {year}")
        return []

    schools = []

    # Pattern to match entries like: "1 Gymnasium OberalsterSasel 1,94"
    # The text format concatenates school name + district + grade

    # First, find the Abiturnotendurchschnitt section
    if f'Abiturnotendurchschnitt {year}' not in text:
        logger.warning(f"Abitur section not found for {year}")
        return []

    # Extract just the Abitur section
    abitur_start = text.find(f'Abiturnotendurchschnitt {year}')
    # Find where it ends (next major section or end markers)
    abitur_end = text.find('Abitur-Durchschnittsnoten', abitur_start + 10)
    if abitur_end == -1:
        abitur_end = text.find('Chronik', abitur_start)
    if abitur_end == -1:
        abitur_end = len(text)

    abitur_text = text[abitur_start:abitur_end]

    # Use a different approach: find all grade patterns and extract surrounding text
    # Each entry is: Rank SchoolNameDistrict Grade
    # Grade format: 1,94 or 2,00

    # Pattern: capture rank (number), school+district (text), and grade
    # The pattern looks for: number + space + text + space + grade
    pattern = r'(\d{1,2})\s+([A-Za-zäöüßÄÖÜ\-\.\s]+?[A-Za-zäöüßÄÖÜ]+)\s+(\d,\d{2})'
    matches = re.findall(pattern, abitur_text)

    for match in matches:
        rank_str, school_district, grade_str = match

        rank = int(rank_str)
        school_district = school_district.strip()

        # Skip if rank doesn't make sense
        if rank < 1 or rank > 100:
            continue

        # Parse school name and district
        school_name, district = extract_school_and_district(school_district)

        try:
            grade = float(grade_str.replace(',', '.'))
            if 1.0 <= grade <= 4.0:
                schools.append({
                    'rank': rank,
                    'schulname_abitur': school_name,
                    'stadtteil_abitur': district,
                    f'abitur_durchschnitt_{year}': grade,
                    'year': year
                })
        except ValueError:
            continue

    logger.info(f"Found {len(schools)} schools with {year} Abitur data")
    return schools


def extract_school_and_district(text: str) -> Tuple[str, str]:
    """
    Extract school name and district from concatenated string.
    E.g., "Gymnasium OberalsterSasel" -> ("Gymnasium Oberalster", "Sasel")
    """
    # Common Hamburg districts
    districts = [
        'Alsterdorf', 'Altona-Altstadt', 'Altona-Nord', 'Bahrenfeld', 'Barmbek-Nord',
        'Barmbek-Süd', 'Bergedorf', 'Bergstedt', 'Billbrook', 'Billstedt', 'Blankenese',
        'Borgfelde', 'Bramfeld', 'Dulsberg', 'Eidelstedt', 'Eilbek', 'Eimsbüttel',
        'Eißendorf', 'Eppendorf', 'Farmsen-Berne', 'Finkenwerder', 'Fuhlsbüttel',
        'Groß Borstel', 'Groß Flottbek', 'Hamm', 'Hammerbrook', 'Harburg', 'Harvestehude',
        'Hausbruch', 'Heimfeld', 'Hoheluft-Ost', 'Hoheluft-West', 'Horn', 'Hummelsbüttel',
        'Jenfeld', 'Kirchwerder', 'Langenhorn', 'Lohbrügge', 'Lokstedt', 'Lurup',
        'Marienthal', 'Marmstorf', 'Neuallermöhe', 'Neugraben-Fischbek', 'Neustadt',
        'Niendorf', 'Nienstedten', 'Ohlsdorf', 'Osdorf', 'Othmarschen', 'Ottensen',
        'Poppenbüttel', 'Rahlstedt', 'Rissen', 'Rotherbaum', 'Sasel', 'Schnelsen',
        'St. Georg', 'St. Pauli', 'Steilshoop', 'Stellingen', 'Tonndorf', 'Uhlenhorst',
        'Veddel', 'Volksdorf', 'Wandsbek', 'Wellingsbüttel', 'Wilhelmsburg', 'Wilstorf',
        'Winterhude', 'Wohldorf-Ohlstedt'
    ]

    # Sort by length descending to match longer names first
    districts_sorted = sorted(districts, key=len, reverse=True)

    for district in districts_sorted:
        # Check if text ends with the district (case-insensitive)
        if text.lower().endswith(district.lower()):
            school_name = text[:-len(district)].strip()
            # Add space before last word if it got concatenated
            # E.g., "Gymnasium Oberalster" from "Gymnasium OberalsterSasel" -> "Gymnasium Oberalster"
            return school_name, district

    # If no district found, return as-is
    return text, ""


def scrape_school_list() -> List[Dict]:
    """
    Scrape the list of all schools from /liste page.
    Returns list of school names and their slugs (URL paths).
    """
    url = f"{BASE_URL}/liste"
    logger.info(f"Scraping school list from {url}")

    soup = get_soup(url)
    if not soup:
        return []

    schools = []

    # Find all school links - they are in the main content area
    # Pattern: links to /schulname-gymnasium or similar
    for link in soup.find_all('a', href=True):
        href = link['href']
        name = link.get_text(strip=True)

        # Filter for school pages (not category/navigation links)
        if href.startswith('/') and not href.startswith('/#'):
            # Skip known non-school pages
            skip_paths = ['/liste', '/bezirke', '/kategorien', '/sprachen', '/abiturnoten',
                         '/schueler', '/adressliste', '/map', '/schulnummern', '/ferien',
                         '/alumni', '/links', '/impressum', '/datenschutz']

            if any(href == skip or href.startswith(skip + '/') for skip in skip_paths):
                continue

            # Check if it looks like a school name
            if ('gymnasium' in name.lower() or 'schule' in name.lower() or
                'kolleg' in name.lower() or 'christianeum' in name.lower() or
                'gelehrtenschule' in name.lower()):
                schools.append({
                    'schulname': name,
                    'url_slug': href.strip('/')
                })

    # Remove duplicates
    seen = set()
    unique_schools = []
    for s in schools:
        if s['url_slug'] not in seen:
            seen.add(s['url_slug'])
            unique_schools.append(s)

    logger.info(f"Found {len(unique_schools)} unique schools")
    return unique_schools


def scrape_school_details(url_slug: str) -> Optional[Dict]:
    """
    Scrape detailed information from an individual school page.
    """
    url = f"{BASE_URL}/{url_slug}"

    soup = get_soup(url)
    if not soup:
        return None

    details = {'url_slug': url_slug}

    # Get page text for parsing
    text = soup.get_text(separator='\n', strip=True)

    # Extract school name (usually in h2 or the page title)
    h2 = soup.find('h2')
    if h2:
        # Often format: "Gymnasium Name (Abbreviation)"
        name_text = h2.get_text(strip=True)
        # Remove abbreviation in parentheses
        name_match = re.match(r'^(.+?)\s*\([A-Z]+\)$', name_text)
        if name_match:
            details['schulname_detail'] = name_match.group(1).strip()
            details['abbreviation'] = re.search(r'\(([A-Z]+)\)', name_text).group(1)
        else:
            details['schulname_detail'] = name_text

    # Parse key fields from page text
    lines = text.split('\n')

    for i, line in enumerate(lines):
        line = line.strip()

        # Address (usually 2 lines: street, then PLZ city)
        if re.match(r'^[A-Za-zäöüßÄÖÜ\-\s]+\s+\d+[a-z]?$', line):
            # This looks like a street address
            details['adresse_strasse'] = line
            # Next line should be PLZ + city
            if i + 1 < len(lines):
                plz_match = re.match(r'^(\d{5})\s+(.+)$', lines[i + 1].strip())
                if plz_match:
                    details['plz'] = plz_match.group(1)
                    details['ort'] = plz_match.group(2)

        # Bezirk
        if line.startswith('Bezirk:'):
            details['bezirk'] = line.replace('Bezirk:', '').strip()
        elif 'Bezirk:' in lines[i-1] if i > 0 else False:
            bezirk_val = line.strip()
            if bezirk_val and len(bezirk_val) < 30:
                details['bezirk'] = bezirk_val

        # Stadtteil
        if line.startswith('Stadtteil:'):
            details['stadtteil'] = line.replace('Stadtteil:', '').strip()

        # Schulform
        if line.startswith('Schulform:'):
            details['schulform'] = line.replace('Schulform:', '').strip()

        # Status (Staatlich/Privat)
        if line.startswith('Status:'):
            details['status'] = line.replace('Status:', '').strip()

        # Schulnummer
        if line.startswith('Schulnummer:'):
            num_match = re.search(r'(\d+)', line)
            if num_match:
                details['schulnummer'] = num_match.group(1)

        # Sprachen
        if line.startswith('Sprachen:'):
            details['sprachen'] = line.replace('Sprachen:', '').strip()

        # Homepage
        if line.startswith('Homepage:'):
            homepage = line.replace('Homepage:', '').strip()
            if homepage and not homepage.startswith('http'):
                homepage = 'https://' + homepage
            details['homepage'] = homepage

        # Email
        if line.startswith('Email:') or line.startswith('E-Mail:'):
            email = line.replace('Email:', '').replace('E-Mail:', '').strip()
            details['email'] = email

        # Gegründet (founded year)
        if line.startswith('Gegründet:'):
            year_match = re.search(r'(\d{4})', line)
            if year_match:
                details['gruendungsjahr'] = int(year_match.group(1))

    # Try to extract from structured format if not found above
    # Look for patterns in the full text

    # Schulnummer pattern
    if 'schulnummer' not in details:
        num_match = re.search(r'Schulnummer[:\s]+(\d+)', text)
        if num_match:
            details['schulnummer'] = num_match.group(1)

    # Status pattern
    if 'status' not in details:
        if 'Staatlich' in text:
            details['status'] = 'Staatlich'
        elif 'Privat' in text:
            details['status'] = 'Privat'

    # Bezirk from breadcrumb or sidebar
    if 'bezirk' not in details:
        bezirk_match = re.search(r'Bezirke\s*>\s*([A-Za-zäöüßÄÖÜ\-]+)\s*>', text)
        if bezirk_match:
            details['bezirk'] = bezirk_match.group(1)

    # Student numbers (Schülerzahlen)
    # Pattern: "19/20 20/21 21/22 22/23 23/24" followed by numbers
    schueler_pattern = re.search(
        r'Schülerzahlen.*?(\d{2}/\d{2})\s+(\d{2}/\d{2})\s+(\d{2}/\d{2})\s+(\d{2}/\d{2})\s+(\d{2}/\d{2})\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)',
        text, re.DOTALL
    )
    if schueler_pattern:
        details['schueler_23_24'] = int(schueler_pattern.group(10))
        details['schueler_22_23'] = int(schueler_pattern.group(9))

    return details


def scrape_schulnummern() -> Dict[str, str]:
    """
    Scrape school numbers from /schulnummern page.
    Returns dict mapping school name to schulnummer.
    """
    url = f"{BASE_URL}/schulnummern"
    logger.info(f"Scraping school numbers from {url}")

    soup = get_soup(url)
    if not soup:
        return {}

    schulnummern = {}

    # Find the table with school numbers
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                num_text = cells[0].get_text(strip=True)
                name_text = cells[1].get_text(strip=True)

                # Validate it's a school number (4 digits)
                if re.match(r'^\d{4}$', num_text):
                    schulnummern[name_text.lower()] = num_text

    logger.info(f"Found {len(schulnummern)} school numbers")
    return schulnummern


def combine_abitur_data(all_years_data: List[List[Dict]]) -> pd.DataFrame:
    """
    Combine Abitur data from multiple years into a single DataFrame.
    """
    if not all_years_data:
        return pd.DataFrame()

    # Flatten all data
    all_schools = {}

    for year_data in all_years_data:
        for school in year_data:
            name = school['schulname_abitur']

            if name not in all_schools:
                all_schools[name] = {
                    'schulname_abitur': name,
                    'stadtteil_abitur': school.get('stadtteil_abitur', '')
                }

            # Add year-specific data
            for key, value in school.items():
                if key.startswith('abitur_durchschnitt_'):
                    all_schools[name][key] = value

    df = pd.DataFrame(list(all_schools.values()))

    # Sort by most recent Abitur grade
    if 'abitur_durchschnitt_2024' in df.columns:
        df = df.sort_values('abitur_durchschnitt_2024', na_position='last')

    return df


def enrich_with_school_details(abitur_df: pd.DataFrame, max_schools: int = None) -> pd.DataFrame:
    """
    Enrich Abitur data with detailed school information.
    """
    logger.info("Enriching with school details...")

    # Get school list to find URL slugs
    school_list = scrape_school_list()

    # Create mapping from school name to URL slug
    name_to_slug = {}
    for school in school_list:
        name_lower = school['schulname'].lower()
        name_to_slug[name_lower] = school['url_slug']

    # Get schulnummern
    schulnummern = scrape_schulnummern()

    # Add columns for detailed info
    detail_columns = ['schulnummer', 'status', 'bezirk', 'homepage', 'email',
                      'sprachen', 'gruendungsjahr', 'schueler_23_24']

    for col in detail_columns:
        if col not in abitur_df.columns:
            abitur_df[col] = None

    # Scrape details for each school
    schools_processed = 0
    for idx, row in abitur_df.iterrows():
        if max_schools and schools_processed >= max_schools:
            break

        school_name = row['schulname_abitur']

        # Find matching URL slug
        slug = None
        name_lower = school_name.lower()

        # Try exact match first
        if name_lower in name_to_slug:
            slug = name_to_slug[name_lower]
        else:
            # Try partial match
            for list_name, list_slug in name_to_slug.items():
                if name_lower in list_name or list_name in name_lower:
                    slug = list_slug
                    break

        if slug:
            time.sleep(0.5)  # Be polite
            details = scrape_school_details(slug)

            if details:
                for col in detail_columns:
                    if col in details and details[col]:
                        abitur_df.at[idx, col] = details[col]

                schools_processed += 1
                if schools_processed % 10 == 0:
                    logger.info(f"Processed {schools_processed} school detail pages")

        # Also try to match schulnummer
        if not abitur_df.at[idx, 'schulnummer']:
            for sname, snum in schulnummern.items():
                if name_lower in sname or sname in name_lower:
                    abitur_df.at[idx, 'schulnummer'] = snum
                    break

    logger.info(f"Enriched {schools_processed} schools with detailed information")
    return abitur_df


def save_outputs(df: pd.DataFrame):
    """Save the Abitur data to output files."""
    logger.info("Saving Abitur data outputs...")

    # Ensure directory exists
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    # Save to intermediate folder
    output_path = INTERMEDIATE_DIR / "hamburg_abitur_statistics.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path}")

    # Also save as parquet
    parquet_path = INTERMEDIATE_DIR / "hamburg_abitur_statistics.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved: {parquet_path}")

    return output_path


def print_summary(df: pd.DataFrame):
    """Print summary of Abitur data."""
    print("\n" + "="*70)
    print("HAMBURG ABITUR STATISTICS SCRAPER - PHASE 2 COMPLETE")
    print("="*70)

    print(f"\nTotal schools with Abitur data: {len(df)}")

    # Show column coverage
    abitur_cols = sorted([c for c in df.columns if c.startswith('abitur_durchschnitt')])
    print("\nAbitur data by year:")
    for col in abitur_cols:
        count = df[col].notna().sum()
        year = col.split('_')[-1]
        print(f"  - {year}: {count} schools")

    # Show enrichment coverage
    print("\nData enrichment coverage:")
    enrichment_cols = ['schulnummer', 'status', 'homepage', 'sprachen']
    for col in enrichment_cols:
        if col in df.columns:
            count = df[col].notna().sum()
            pct = 100 * count / len(df)
            print(f"  - {col}: {count}/{len(df)} ({pct:.0f}%)")

    # Show top schools
    if 'abitur_durchschnitt_2024' in df.columns:
        top_schools = df.nsmallest(5, 'abitur_durchschnitt_2024')[['schulname_abitur', 'abitur_durchschnitt_2024']]
        print("\nTop 5 schools (2024 Abitur):")
        for _, row in top_schools.iterrows():
            print(f"  - {row['schulname_abitur']}: {row['abitur_durchschnitt_2024']:.2f}")

    # Show school type breakdown
    if 'status' in df.columns:
        print("\nSchools by status:")
        for status, count in df['status'].value_counts().items():
            print(f"  - {status}: {count}")

    print("\n" + "="*70)


def main():
    """Main function to run the comprehensive Abitur scraper."""
    logger.info("="*60)
    logger.info("Starting Hamburg Abitur Statistics Scraper (Phase 2)")
    logger.info("Comprehensive version - scraping full data from gymnasium-hamburg.net")
    logger.info("="*60)

    try:
        # Ensure directories exist
        INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

        # Step 1: Scrape Abitur data for all years
        all_years_data = []
        for year in YEARS_TO_SCRAPE:
            time.sleep(1)  # Be polite
            year_data = scrape_abitur_year(year)
            if year_data:
                all_years_data.append(year_data)

        # Combine all years
        combined_df = combine_abitur_data(all_years_data)

        if combined_df.empty:
            logger.error("No Abitur data collected")
            return pd.DataFrame()

        logger.info(f"Combined Abitur data: {len(combined_df)} schools")

        # Step 2: Enrich with school details (optional - can be slow)
        # Set max_schools=None to process all, or a number to limit
        combined_df = enrich_with_school_details(combined_df, max_schools=None)

        # Save outputs
        save_outputs(combined_df)

        # Print summary
        print_summary(combined_df)

        logger.info("Phase 2 complete!")
        return combined_df

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


if __name__ == "__main__":
    main()
