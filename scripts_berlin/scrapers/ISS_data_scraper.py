#!/usr/bin/env python3
"""
ISS Data Scraper for Sekundarschulen Berlin
Scrapes data from sekundarschulen-berlin.de and combines into a master table.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urljoin
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.sekundarschulen-berlin.de"

# Headers to mimic browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
}


def get_soup(url: str, retries: int = 3) -> BeautifulSoup:
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
                raise
    return None


def is_valid_school_name(name: str) -> bool:
    """Check if a name looks like a valid school name."""
    if not name or len(name) < 5:
        return False
    # Must contain school-related keywords
    school_keywords = ['schule', 'gymnasium', 'oberschule', 'campus', 'gemeinschaft',
                       'kolleg', 'zentrum', 'slzb', 'sportschule']
    return any(kw in name.lower() for kw in school_keywords)


def is_valid_school_slug(slug: str) -> bool:
    """Check if a slug looks like a valid school page slug."""
    # Exclude known non-school pages
    excluded = ['/statistik', '/lehrerzahlen', '/migrationshintergrund', '/sozialindex',
                '/nachfrage', '/abitur', '/belastungsstufen', '/adressliste', '/bezirke',
                '/listen', '/sprachen', '/profile', '/schulsystem', '/ferien', '/links',
                '/impressum', '/home', '/karte', '/private', '/oberstufenzentren',
                '/gymnasien', '/gemeinschaftsschulen', '/europaschulen',
                '/sekundarschulen-von-a-bis-z', '/mit-gymnasialer-oberstufe',
                '/schulabschluesse', '/schulbuecher', '/anmeldeverfahren',
                '/tage-der-offenen-tuer', '/oberschulmessen', '/schulbroschueren']
    return not any(ex in slug for ex in excluded)


def scrape_views_table(url: str, value_columns: list) -> pd.DataFrame:
    """
    Generic scraper for tables using the 'views-table' class structure.
    The site uses a consistent table format with:
    - First column: School name (link) + Bezirk/Ortsteil
    - Subsequent columns: Data values

    Args:
        url: The page URL to scrape
        value_columns: List of column names to assign to the data values
    """
    logger.info(f"Scraping table from {url}...")
    soup = get_soup(url)

    schools = []

    # Find the views-table
    table = soup.find('table', class_='views-table')
    if not table:
        logger.warning(f"No views-table found at {url}")
        return pd.DataFrame()

    # Process each row
    rows = table.find_all('tr')
    for row in rows:
        # Skip header rows
        if row.find('th'):
            continue

        cells = row.find_all('td')
        if not cells:
            continue

        # First cell contains school name (link) and Bezirk
        first_cell = cells[0]
        link = first_cell.find('a', href=True)

        if not link:
            continue

        school_name = link.get_text(strip=True)
        href = link.get('href', '')

        if not school_name or not href.startswith('/'):
            continue

        if not is_valid_school_name(school_name) or not is_valid_school_slug(href):
            continue

        school_slug = href.strip('/')

        # Get Bezirk/Ortsteil from the cell (usually after <br>)
        cell_text = first_cell.get_text(separator='|', strip=True)
        parts = [p.strip() for p in cell_text.split('|') if p.strip()]
        bezirk = parts[1] if len(parts) > 1 else None

        school_data = {
            'Schulname': school_name,
            'school_slug': school_slug,
            'Bezirk_Ortsteil': bezirk
        }

        # Extract values from remaining cells
        for i, col_name in enumerate(value_columns):
            if i + 1 < len(cells):
                cell_text = cells[i + 1].get_text(strip=True)
                # Clean the value (remove dots as thousand separators, handle commas)
                clean_value = cell_text.replace('.', '').replace(',', '.').strip()
                if clean_value:
                    school_data[col_name] = clean_value

        schools.append(school_data)

    df = pd.DataFrame(schools)
    if not df.empty:
        df = df.drop_duplicates(subset=['school_slug'])

    logger.info(f"Found {len(df)} schools from {url}")
    return df


def scrape_school_details(school_url: str, school_name: str = None) -> dict:
    """Scrape individual school page for Schulnummer, Sprachen, Homepage, Address."""
    try:
        soup = get_soup(school_url)

        details = {
            'Schulnummer': None,
            'Bezirk': None,
            'Ortsteil': None,
            'Adresse': None,
            'Sprachen': None,
            'Homepage': None
        }

        # Find the main content area - the h2 with school name
        school_header = soup.find('h2')
        if school_header:
            # Address is in the <p> tag right after the h2
            next_p = school_header.find_next('p')
            if next_p:
                address_text = next_p.get_text(separator=', ', strip=True)
                # Check if it looks like an address (contains "Berlin")
                if 'Berlin' in address_text:
                    details['Adresse'] = address_text

        # Find Schulnummer - look for "Schulnummer:" followed by code like 12K11
        schulnummer_match = re.search(r'Schulnummer:\s*(\d{2}[A-Z]\d{2})', soup.get_text())
        if schulnummer_match:
            details['Schulnummer'] = schulnummer_match.group(1)

        # Find Bezirk - look for the link that goes to a bezirk page
        # The structure is: <strong>Bezirk: </strong><a href="/reinickendorf">Reinickendorf</a>
        bezirk_strong = soup.find('strong', string=re.compile(r'Bezirk:', re.IGNORECASE))
        if bezirk_strong:
            bezirk_link = bezirk_strong.find_next('a')
            if bezirk_link:
                details['Bezirk'] = bezirk_link.get_text(strip=True)

        # Find Ortsteil - look for "Ortsteil:" text
        ortsteil_strong = soup.find('strong', string=re.compile(r'Ortsteil:', re.IGNORECASE))
        if ortsteil_strong:
            # Get the text after this element
            next_text = ortsteil_strong.next_sibling
            if next_text:
                ortsteil = str(next_text).strip()
                # Clean up - remove any leading/trailing punctuation
                ortsteil = re.sub(r'^[\s:]+|[\s]+$', '', ortsteil)
                if ortsteil:
                    details['Ortsteil'] = ortsteil

        # Find Fremdsprachen section - look for the field div
        fremdsprachen_div = soup.find('div', class_='field-name-field-fremdsprachen')
        if fremdsprachen_div:
            field_item = fremdsprachen_div.find('div', class_='field-item')
            if field_item:
                details['Sprachen'] = field_item.get_text(strip=True)
        else:
            # Fallback: try finding in text
            text = soup.get_text()
            sprachen_match = re.search(r'Fremdsprachen:\s*([^\n]+?)(?:Leistungskurse|Nachfrage|Schwerpunkte|$)', text)
            if sprachen_match:
                sprachen = sprachen_match.group(1).strip()
                sprachen = re.sub(r'[,\s]+$', '', sprachen)
                if sprachen:
                    details['Sprachen'] = sprachen

        # Find Homepage - look for the field with homepage link
        homepage_field = soup.find('td', class_='views-field-field-homepage')
        if homepage_field:
            link = homepage_field.find('a', href=True)
            if link:
                href = link.get('href', '')
                if href and 'http' in href:
                    details['Homepage'] = href
        else:
            # Fallback: look for table with "Homepage" header
            homepage_table = soup.find('td', string=re.compile(r'Homepage', re.IGNORECASE))
            if homepage_table:
                parent_table = homepage_table.find_parent('table')
                if parent_table:
                    link = parent_table.find_next('a', href=True)
                    if link:
                        href = link.get('href', '')
                        if href and ('http' in href or href.startswith('www')):
                            details['Homepage'] = href if href.startswith('http') else f'https://{href}'

        return details
    except Exception as e:
        logger.error(f"Error scraping {school_url}: {e}")
        return {}


def scrape_schueler_zahlen() -> pd.DataFrame:
    """Scrape student numbers from /statistik."""
    url = f"{BASE_URL}/statistik"
    return scrape_views_table(url, ['Schueler_2024_25', 'Schueler_2023_24'])


def scrape_lehrer_zahlen() -> pd.DataFrame:
    """Scrape teacher numbers from /lehrerzahlen."""
    url = f"{BASE_URL}/lehrerzahlen"
    return scrape_views_table(url, ['Lehrer_2024_25', 'Lehrer_2023_24'])


def scrape_nachfrage() -> pd.DataFrame:
    """Scrape demand data from /nachfrage."""
    url = f"{BASE_URL}/nachfrage"
    return scrape_views_table(url, ['Nachfrage_Plaetze_2025_26', 'Nachfrage_Wuensche_2025_26', 'Nachfrage_Prozent_2025_26'])


def scrape_abitur(year: int = 2024) -> pd.DataFrame:
    """Scrape Abitur grades from /abitur/YEAR."""
    logger.info(f"Scraping Abitur {year}...")
    url = f"{BASE_URL}/abitur/{year}"
    soup = get_soup(url)

    schools = []

    # Find the views-table
    table = soup.find('table', class_='views-table')
    if not table:
        logger.warning(f"No views-table found at {url}")
        return pd.DataFrame()

    # Process each row
    rows = table.find_all('tr')
    for row in rows:
        # Skip header rows
        if row.find('th'):
            continue

        cells = row.find_all('td')
        if len(cells) < 2:
            continue

        # Abitur table has: Rank, School (link + Bezirk), Grade
        # Find the cell with the school link
        school_cell = None
        grade_cell = None

        for cell in cells:
            if cell.find('a', href=True):
                school_cell = cell
            elif 'abi' in str(cell.get('class', [])).lower():
                grade_cell = cell

        if not school_cell:
            continue

        link = school_cell.find('a', href=True)
        school_name = link.get_text(strip=True)
        href = link.get('href', '')

        if not school_name or not href.startswith('/'):
            continue

        if not is_valid_school_name(school_name) or not is_valid_school_slug(href):
            continue

        school_slug = href.strip('/')

        # Get Bezirk from the cell
        cell_text = school_cell.get_text(separator='|', strip=True)
        parts = [p.strip() for p in cell_text.split('|') if p.strip()]
        bezirk = parts[1] if len(parts) > 1 else None

        school_data = {
            'Schulname': school_name,
            'school_slug': school_slug,
            'Bezirk_Ortsteil': bezirk
        }

        # Extract grade
        if grade_cell:
            grade_text = grade_cell.get_text(strip=True)
            # Convert comma to dot for decimal
            grade = grade_text.replace(',', '.')
            school_data[f'Abitur_Durchschnitt_{year}'] = grade

        schools.append(school_data)

    df = pd.DataFrame(schools)
    if not df.empty:
        df = df.drop_duplicates(subset=['school_slug'])

    logger.info(f"Found {len(df)} schools with Abitur {year}")
    return df


def scrape_belastungsstufen() -> pd.DataFrame:
    """Scrape stress levels from /belastungsstufen."""
    url = f"{BASE_URL}/belastungsstufen"
    return scrape_views_table(url, ['Belastungsstufe'])


def scrape_migrationshintergrund() -> pd.DataFrame:
    """Scrape migration background from /migrationshintergrund."""
    url = f"{BASE_URL}/migrationshintergrund"
    return scrape_views_table(url, ['Migration_2024_25', 'Migration_2023_24'])


def scrape_address_list() -> pd.DataFrame:
    """Scrape address list to get all schools with their addresses."""
    logger.info("Scraping address list...")
    url = f"{BASE_URL}/adressliste"
    soup = get_soup(url)

    schools = []

    # The address list has a different structure - it's not a table
    # Each school is a link followed by address text
    content = soup.find('div', {'id': 'content'}) or soup

    # Find all school links
    for link in content.find_all('a', href=True):
        href = link.get('href', '')

        if not href.startswith('/') or not is_valid_school_slug(href):
            continue

        school_name = link.get_text(strip=True)
        if not is_valid_school_name(school_name):
            continue

        school_slug = href.strip('/')

        # Get address from surrounding text
        parent = link.find_parent(['div', 'td', 'li'])
        address = None
        if parent:
            # Get text after the link
            parent_text = parent.get_text(separator='\n', strip=True)
            lines = [l.strip() for l in parent_text.split('\n') if l.strip()]

            # Find the school name line and get subsequent address lines
            for i, line in enumerate(lines):
                if school_name in line:
                    remaining = lines[i+1:i+3]  # Usually 2 lines: street and postal code
                    address_parts = []
                    for part in remaining:
                        # Skip if it looks like another school name
                        if is_valid_school_name(part):
                            break
                        address_parts.append(part)
                        if 'Berlin' in part:
                            break
                    if address_parts:
                        address = ', '.join(address_parts)
                    break

        schools.append({
            'Schulname': school_name,
            'school_slug': school_slug,
            'Adresse_list': address
        })

    df = pd.DataFrame(schools)
    if not df.empty:
        df = df.drop_duplicates(subset=['school_slug'])

    logger.info(f"Found {len(df)} schools from address list")
    return df


def scrape_all_school_details(schools_df: pd.DataFrame, max_schools: int = None) -> pd.DataFrame:
    """Scrape details for all schools from their individual pages."""
    logger.info("Scraping individual school details...")

    details_list = []
    total = len(schools_df) if max_schools is None else min(len(schools_df), max_schools)

    for idx, row in schools_df.head(total).iterrows():
        school_url = f"{BASE_URL}/{row['school_slug']}"
        school_name = row.get('Schulname', '')
        logger.info(f"Scraping details {idx + 1}/{total}: {school_name}")

        details = scrape_school_details(school_url, school_name)
        details['school_slug'] = row['school_slug']
        details_list.append(details)

        # Be polite to the server
        time.sleep(0.3)

    return pd.DataFrame(details_list)


def create_master_table(scrape_details: bool = True) -> pd.DataFrame:
    """Create the master table by combining all scraped data."""
    logger.info("Creating master table...")

    # Scrape all data sources
    schueler_df = scrape_schueler_zahlen()
    lehrer_df = scrape_lehrer_zahlen()
    nachfrage_df = scrape_nachfrage()
    abitur_2024_df = scrape_abitur(2024)
    abitur_2023_df = scrape_abitur(2023)
    belastung_df = scrape_belastungsstufen()
    migration_df = scrape_migrationshintergrund()
    address_df = scrape_address_list()

    # Start with Schülerzahlen as base (usually most complete)
    if not schueler_df.empty:
        master_df = schueler_df.copy()
    else:
        master_df = address_df.copy()

    merge_key = 'school_slug'

    # Merge address list for additional schools
    if not address_df.empty:
        # Get schools that are in address list but not in master
        new_schools = address_df[~address_df[merge_key].isin(master_df[merge_key])]
        if not new_schools.empty:
            master_df = pd.concat([master_df, new_schools], ignore_index=True)

    # Scrape individual school details if requested
    if scrape_details:
        details_df = scrape_all_school_details(master_df)
        if not details_df.empty:
            master_df = master_df.merge(details_df, on=merge_key, how='left', suffixes=('', '_detail'))

    # Merge Lehrer data
    if not lehrer_df.empty:
        cols_to_merge = [merge_key] + [c for c in lehrer_df.columns if c.startswith('Lehrer_')]
        master_df = master_df.merge(lehrer_df[cols_to_merge], on=merge_key, how='left')

    # Merge Nachfrage data
    if not nachfrage_df.empty:
        cols_to_merge = [merge_key] + [c for c in nachfrage_df.columns if c.startswith('Nachfrage_')]
        master_df = master_df.merge(nachfrage_df[cols_to_merge], on=merge_key, how='left')

    # Merge Abitur data
    if not abitur_2024_df.empty:
        cols_to_merge = [merge_key] + [c for c in abitur_2024_df.columns if c.startswith('Abitur_')]
        master_df = master_df.merge(abitur_2024_df[cols_to_merge], on=merge_key, how='left')

    if not abitur_2023_df.empty:
        cols_to_merge = [merge_key] + [c for c in abitur_2023_df.columns if c.startswith('Abitur_')]
        master_df = master_df.merge(abitur_2023_df[cols_to_merge], on=merge_key, how='left')

    # Merge Belastungsstufen
    if not belastung_df.empty:
        cols_to_merge = [merge_key] + [c for c in belastung_df.columns if c.startswith('Belastung')]
        master_df = master_df.merge(belastung_df[cols_to_merge], on=merge_key, how='left')

    # Merge Migration data
    if not migration_df.empty:
        cols_to_merge = [merge_key] + [c for c in migration_df.columns if c.startswith('Migration_')]
        master_df = master_df.merge(migration_df[cols_to_merge], on=merge_key, how='left')

    # Use Adresse from details if available, otherwise from address list
    if 'Adresse' in master_df.columns and 'Adresse_list' in master_df.columns:
        master_df['Adresse'] = master_df['Adresse'].fillna(master_df['Adresse_list'])
        master_df = master_df.drop(columns=['Adresse_list'], errors='ignore')
    elif 'Adresse_list' in master_df.columns:
        master_df = master_df.rename(columns={'Adresse_list': 'Adresse'})

    # Clean up Bezirk column - use from details if available
    if 'Bezirk' in master_df.columns and 'Bezirk_Ortsteil' in master_df.columns:
        master_df['Bezirk'] = master_df['Bezirk'].fillna(master_df['Bezirk_Ortsteil'])
    elif 'Bezirk_Ortsteil' in master_df.columns:
        master_df = master_df.rename(columns={'Bezirk_Ortsteil': 'Bezirk'})

    # Reorder columns for final output
    column_order = [
        'Schulnummer',
        'Schulname',
        'Bezirk',
        'Adresse',
        'Schueler_2024_25',
        'Lehrer_2024_25',
        'Schueler_2023_24',
        'Lehrer_2023_24',
        'Sprachen',
        'Homepage',
        'Nachfrage_Plaetze_2025_26',
        'Nachfrage_Wuensche_2025_26',
        'Nachfrage_Prozent_2025_26',
        'Abitur_Durchschnitt_2024',
        'Abitur_Durchschnitt_2023',
        'Belastungsstufe',
        'Migration_2024_25',
        'Migration_2023_24',
        'Ortsteil',
    ]

    # Add only columns that exist
    final_columns = [col for col in column_order if col in master_df.columns]
    # Add any remaining columns not in order (except internal ones)
    internal_cols = ['school_slug', 'school_url', 'Bezirk_Ortsteil', 'Adresse_list']
    remaining = [col for col in master_df.columns
                 if col not in final_columns
                 and col not in internal_cols
                 and not col.endswith('_detail')]
    final_columns.extend(remaining)

    master_df = master_df[[c for c in final_columns if c in master_df.columns]]

    return master_df


def main():
    """Main function to run the scraper."""
    logger.info("Starting ISS Data Scraper...")

    try:
        master_df = create_master_table(scrape_details=True)

        # Save to CSV
        output_file = 'ISS_master_table.csv'
        master_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Saved master table to {output_file}")

        # Also save to Excel
        excel_file = 'ISS_master_table.xlsx'
        master_df.to_excel(excel_file, index=False)
        logger.info(f"Saved master table to {excel_file}")

        # Print summary
        print(f"\n{'='*60}")
        print(f"ISS Data Scraper Complete!")
        print(f"{'='*60}")
        print(f"Total schools scraped: {len(master_df)}")
        print(f"Columns: {list(master_df.columns)}")
        print(f"\nOutput files:")
        print(f"  - {output_file}")
        print(f"  - {excel_file}")

        # Show first few rows
        print(f"\nFirst 5 schools:")
        print(master_df.head().to_string())

        return master_df

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        raise


if __name__ == "__main__":
    main()
