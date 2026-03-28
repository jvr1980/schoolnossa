#!/usr/bin/env python3
"""
Scraper for Grundschulen (Primary Schools) from bildung.berlin.de

This script scrapes school data from the Berlin education portal, collecting:
- School name
- School code (Schulnummer) - extracted from school name suffix (format: XXG##, e.g., 01G01)
- Address (Street, PLZ, District)
- Phone number
- Email
- External website (the school's own website, NOT the bildung.berlin.de profile)
- Principal name (Leitung)

Note: Primary schools use "G" in their schulnummer pattern (e.g., 01G01)
      - XXG## = Public Grundschule
      - XXP## = Private Grundschule

Output: bildung_berlin_grundschulen.csv and bildung_berlin_grundschulen.xlsx

Data source: https://www.bildung.berlin.de/Schulverzeichnis/
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import os
from urllib.parse import urljoin
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base directories
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_berlin_primary" / "raw"

BASE_URL = "https://www.bildung.berlin.de/Schulverzeichnis/"
SEARCH_URL = BASE_URL + "SchulListe.aspx"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# School type codes from bildung.berlin.de dropdown
# Values extracted from the website's DropDownListSchulart options
SCHULART_GRUNDSCHULE = "11"  # Grundschule (Primary School)


def get_session_with_search(schulart_code: str) -> tuple[requests.Session, list[dict]]:
    """
    Get a session and perform search for the given school type.
    Returns session and list of school links with their IDs.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    # First, get the main page to get any necessary form fields
    logger.info("Fetching search page...")
    response = session.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    # Get ASP.NET form fields
    viewstate = soup.find('input', {'name': '__VIEWSTATE'})
    viewstate_value = viewstate['value'] if viewstate else ''

    viewstate_gen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
    viewstate_gen_value = viewstate_gen['value'] if viewstate_gen else ''

    eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})
    eventvalidation_value = eventvalidation['value'] if eventvalidation else ''

    # Prepare form data for search - using actual form field names from the website
    form_data = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': viewstate_value,
        '__VIEWSTATEGENERATOR': viewstate_gen_value,
        '__EVENTVALIDATION': eventvalidation_value,
        'txtSuchbegriff': '',
        'DropDownListSchuljahre': '16',  # 2025/26
        'DropDownListBezirk': '0',
        'DropDownListSchulart': schulart_code,
        'DropDownListFremdsprache': '0',
        'DropDownListSpracheArt': 'Sprache',
        'DropDownListKategorie': '0',
        'DropDownListAngebot': '0',
        'DropDownListThema': '0',
        'btnSuchen': 'Suchen'
    }

    # Submit search
    logger.info(f"Submitting search for Schulart code: {schulart_code} (Grundschule)")
    response = session.post(BASE_URL, data=form_data)
    response.raise_for_status()

    # Parse the result page
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all school links
    school_links = soup.find_all('a', href=re.compile(r'Schulportrait\.aspx\?IDSchulzweig='))

    schools = []
    for link in school_links:
        href = link.get('href', '')
        name = link.get_text(strip=True)

        # Extract the IDSchulzweig parameter
        id_match = re.search(r'IDSchulzweig=\s*(\d+)', href)
        if id_match:
            school_id = id_match.group(1)
            schools.append({
                'name': name,
                'id': school_id,
                'url': urljoin(BASE_URL, href)
            })

    logger.info(f"Found {len(schools)} Grundschulen")
    return session, schools


def scrape_school_detail(session: requests.Session, school_url: str, school_name: str, schulart: str = 'Grundschule') -> dict:
    """
    Scrape detailed information from a school's profile page.

    IMPORTANT: Extracts the school's OWN external website, not the bildung.berlin.de profile link.

    The page uses ASP.NET controls with specific IDs for each field.
    """
    result = {
        'schulname': school_name,
        'schulnummer': None,
        'strasse': None,
        'plz': None,
        'bezirk': None,
        'ortsteil': None,
        'telefon': None,
        'email': None,
        'website': None,  # The school's own external website
        'leitung': None,
        'schulart': schulart
    }

    try:
        response = session.get(school_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract school name and code from ASP.NET controls
        # ID: ContentPlaceHolderMenuListe_lblSchulname contains "School Name - XXG##"
        schulname_el = soup.find(id='ContentPlaceHolderMenuListe_lblSchulname')
        if schulname_el:
            full_name = schulname_el.get_text(strip=True)
            # Extract school code (format: ##X## or ##G##, e.g., 01G01, 02G15)
            # Primary schools use G for Grundschule, P for private
            code_match = re.search(r'(\d{2}[A-Z]\d{2})', full_name)
            if code_match:
                result['schulnummer'] = code_match.group(1)
            # Extract school name (without code)
            name_match = re.match(r'(.+?)\s*-\s*\d{2}[A-Z]\d{2}', full_name)
            if name_match:
                result['schulname'] = name_match.group(1).strip()
            else:
                result['schulname'] = full_name

        # Extract address from ASP.NET controls
        strasse_el = soup.find(id='ContentPlaceHolderMenuListe_lblStrasse')
        if strasse_el:
            result['strasse'] = strasse_el.get_text(strip=True)

        # Extract PLZ and Ortsteil from "12683 Berlin (Biesdorf)" format
        ort_el = soup.find(id='ContentPlaceHolderMenuListe_lblOrt')
        if ort_el:
            ort_text = ort_el.get_text(strip=True)
            # Parse "12683 Berlin (Biesdorf)"
            ort_match = re.match(r'(\d{5})\s*Berlin\s*\(([^)]+)\)', ort_text)
            if ort_match:
                result['plz'] = ort_match.group(1)
                result['ortsteil'] = ort_match.group(2)

        # Extract phone number
        telefon_el = soup.find(id='ContentPlaceHolderMenuListe_lblTelefon')
        if telefon_el:
            result['telefon'] = telefon_el.get_text(strip=True)

        # Extract email - look for hyperlink with ID HLinkEMail
        email_link = soup.find(id='ContentPlaceHolderMenuListe_HLinkEMail')
        if email_link:
            href = email_link.get('href', '')
            # Clean up mailto: prefix and any whitespace/special chars
            email = href.replace('mailto:', '').strip()
            # Remove any leading/trailing whitespace encoded chars
            email = re.sub(r'^[\s\t\n\r]+', '', email)
            result['email'] = email

        # Extract website - look for hyperlink with ID HLinkWeb
        # IMPORTANT: This is the school's OWN external website
        web_link = soup.find(id='ContentPlaceHolderMenuListe_HLinkWeb')
        if web_link:
            href = web_link.get('href', '')
            # Only use if it's not a bildung.berlin.de link
            if href and 'bildung.berlin.de' not in href and 'berlin.de/sen' not in href:
                result['website'] = href
                # Ensure URL has protocol
                if result['website'] and not result['website'].startswith('http'):
                    result['website'] = 'https://' + result['website']

        # Extract principal name (Leitung)
        leitung_el = soup.find(id='ContentPlaceHolderMenuListe_lblLeitung')
        if leitung_el:
            result['leitung'] = leitung_el.get_text(strip=True)

    except Exception as e:
        logger.error(f"Error scraping {school_url}: {e}")

    return result


def scrape_all_grundschulen() -> pd.DataFrame:
    """
    Scrape all Grundschule (primary) schools from bildung.berlin.de
    """
    # Get session and school list
    session, schools = get_session_with_search(SCHULART_GRUNDSCHULE)

    if not schools:
        logger.error("No schools found!")
        return pd.DataFrame()

    logger.info(f"Starting to scrape {len(schools)} Grundschulen...")

    all_data = []
    for i, school in enumerate(schools, 1):
        logger.info(f"[{i}/{len(schools)}] Scraping: {school['name']}")

        data = scrape_school_detail(session, school['url'], school['name'])
        all_data.append(data)

        # Be polite to the server
        time.sleep(0.5)

    df = pd.DataFrame(all_data)

    # Reorder columns
    column_order = [
        'schulnummer', 'schulname', 'schulart', 'strasse', 'plz',
        'ortsteil', 'bezirk', 'telefon', 'email', 'website', 'leitung'
    ]
    df = df[[c for c in column_order if c in df.columns]]

    return df


def main():
    """Main entry point"""
    logger.info("Starting bildung.berlin.de Grundschulen scraper...")

    df = scrape_all_grundschulen()

    if df.empty:
        logger.error("No data scraped!")
        return

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Save to CSV and Excel
    csv_path = DATA_DIR / 'bildung_berlin_grundschulen.csv'
    xlsx_path = DATA_DIR / 'bildung_berlin_grundschulen.xlsx'

    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    df.to_excel(xlsx_path, index=False)

    logger.info(f"Scraped {len(df)} schools")
    logger.info(f"Saved to: {csv_path} and {xlsx_path}")

    # Show summary
    print("\n" + "="*60)
    print(f"SUMMARY: {len(df)} Grundschule (primary) schools scraped")
    print("="*60)
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSchools with websites: {df['website'].notna().sum()}")
    print(f"Schools with email: {df['email'].notna().sum()}")
    print(f"Schools with school code: {df['schulnummer'].notna().sum()}")

    # Count by schulnummer pattern
    if 'schulnummer' in df.columns:
        public_count = df['schulnummer'].str.contains(r'\d{2}G\d{2}', na=False).sum()
        private_count = df['schulnummer'].str.contains(r'\d{2}P\d{2}', na=False).sum()
        print(f"\nPublic Grundschulen (XXG##): {public_count}")
        print(f"Private Grundschulen (XXP##): {private_count}")

    # Show first few rows
    print("\nFirst 5 schools:")
    print(df.head().to_string())


if __name__ == '__main__':
    main()
