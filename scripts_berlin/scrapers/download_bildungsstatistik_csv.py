#!/usr/bin/env python3
"""
Download Berlin School Statistics CSV from bildungsstatistik.berlin.de

Downloads student and teacher statistics for all Berlin public schools from:
- https://www.bildungsstatistik.berlin.de/statistik/ListGen/SVZ_Fakt5.aspx (current year)
- https://www.bildungsstatistik.berlin.de/statistik/ListGen/SVZ_Fakt5_2023_24.aspx (previous year)

The data includes:
- BSN (Schulnummer) - school identifier
- NAME - school name
- Schüler (m/w/d) - total students
- Schüler (w) - female students
- Schüler (m) - male students
- Lehrkräfte (m,w,d) - total teachers
- Lehrkräfte (w) - female teachers
- Lehrkräfte (m) - male teachers

Output files:
- data_berlin/raw/bildungsstatistik_2024_25.csv
- data_berlin/raw/bildungsstatistik_2023_24.csv

Usage:
    python3 download_bildungsstatistik_csv.py
    python3 download_bildungsstatistik_csv.py --year 2024_25
    python3 download_bildungsstatistik_csv.py --all
"""

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script directory and project root
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_berlin" / "raw"

# URLs for the statistics pages
URLS = {
    '2024_25': 'https://www.bildungsstatistik.berlin.de/statistik/ListGen/SVZ_Fakt5.aspx',
    '2023_24': 'https://www.bildungsstatistik.berlin.de/statistik/ListGen/SVZ_Fakt5_2023_24.aspx',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
}


def download_csv(url: str, output_path: Path) -> bool:
    """
    Download CSV from the bildungsstatistik.berlin.de page.

    The page uses ASP.NET WebForms, so we need to:
    1. GET the page to extract __VIEWSTATE and __EVENTVALIDATION
    2. POST with the CSV-Export button to trigger download

    Args:
        url: The statistics page URL
        output_path: Path to save the CSV file

    Returns:
        True if successful, False otherwise
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    logger.info(f"Fetching page: {url}")

    try:
        # Step 1: GET the page to extract form fields
        response = session.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract ASP.NET form fields
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        viewstate_value = viewstate['value'] if viewstate else ''

        viewstate_gen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        viewstate_gen_value = viewstate_gen['value'] if viewstate_gen else ''

        eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})
        eventvalidation_value = eventvalidation['value'] if eventvalidation else ''

        # Find the CSV export button name
        csv_button = soup.find('input', {'value': 'CSV-Export'})
        if not csv_button:
            # Try finding button element
            csv_button = soup.find('button', string=re.compile('CSV-Export'))

        button_name = csv_button.get('name', 'btnCSV') if csv_button else 'btnCSV'
        logger.info(f"Found CSV button: {button_name}")

        # Step 2: POST to trigger CSV download
        form_data = {
            '__VIEWSTATE': viewstate_value,
            '__VIEWSTATEGENERATOR': viewstate_gen_value,
            '__EVENTVALIDATION': eventvalidation_value,
            button_name: 'CSV-Export',
        }

        logger.info("Requesting CSV export...")
        response = session.post(url, data=form_data, timeout=60)
        response.raise_for_status()

        # Check if we got CSV data (should have content-type text/csv or start with CSV-like content)
        content_type = response.headers.get('Content-Type', '')
        content = response.text

        # Verify it looks like CSV data
        if 'text/csv' in content_type or 'application/csv' in content_type:
            logger.info("Received CSV via Content-Type header")
        elif content.startswith('Schuljahr') or 'BSN' in content[:500]:
            logger.info("Received CSV data (detected by content)")
        else:
            # Sometimes the response is HTML with the data in a different format
            # Try to extract table data from HTML if CSV export didn't work directly
            logger.warning("CSV export may not have worked directly, checking response...")

            # Check if it's HTML
            if '<html' in content.lower():
                logger.warning("Received HTML instead of CSV. The site may require JavaScript.")
                logger.info("Attempting to parse HTML table instead...")
                return download_from_html_table(response.text, output_path)

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save CSV
        # Handle potential BOM and encoding issues
        encoding = response.encoding or 'utf-8'
        if encoding.lower() == 'iso-8859-1':
            # Re-encode to UTF-8
            content = response.content.decode('iso-8859-1')

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Saved CSV to: {output_path}")

        # Verify the file
        line_count = sum(1 for _ in open(output_path, encoding='utf-8'))
        logger.info(f"CSV contains {line_count} lines (including header)")

        return True

    except requests.exceptions.SSLError as e:
        logger.error(f"SSL Error (certificate issue): {e}")
        logger.info("Trying with SSL verification disabled...")
        return download_csv_no_verify(url, output_path, session)

    except Exception as e:
        logger.error(f"Error downloading CSV: {e}")
        import traceback
        traceback.print_exc()
        return False


def download_csv_no_verify(url: str, output_path: Path, session: requests.Session) -> bool:
    """
    Fallback download without SSL verification.
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        response = session.get(url, timeout=30, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        viewstate_value = viewstate['value'] if viewstate else ''

        viewstate_gen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        viewstate_gen_value = viewstate_gen['value'] if viewstate_gen else ''

        eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})
        eventvalidation_value = eventvalidation['value'] if eventvalidation else ''

        form_data = {
            '__VIEWSTATE': viewstate_value,
            '__VIEWSTATEGENERATOR': viewstate_gen_value,
            '__EVENTVALIDATION': eventvalidation_value,
            'btnCSV': 'CSV-Export',
        }

        response = session.post(url, data=form_data, timeout=60, verify=False)
        response.raise_for_status()

        content = response.text

        # Check if we got HTML instead of CSV
        if '<html' in content.lower() or '<table' in content.lower():
            return download_from_html_table(content, output_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Saved CSV to: {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error in fallback download: {e}")
        return False


def download_from_html_table(html_content: str, output_path: Path) -> bool:
    """
    Extract data from HTML table if CSV export doesn't work directly.
    """
    import pandas as pd

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the data table
        tables = soup.find_all('table')

        for table in tables:
            # Look for a table with the expected headers
            headers = table.find_all('th')
            header_texts = [h.get_text(strip=True) for h in headers]

            if 'BSN' in header_texts or 'Schuljahr' in header_texts:
                logger.info(f"Found data table with headers: {header_texts[:5]}...")

                # Parse table to DataFrame
                rows = []
                for tr in table.find_all('tr'):
                    cells = tr.find_all(['td', 'th'])
                    row = [cell.get_text(strip=True) for cell in cells]
                    if row:
                        rows.append(row)

                if len(rows) > 1:
                    # First row is headers
                    df = pd.DataFrame(rows[1:], columns=rows[0])

                    # Save to CSV
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(output_path, index=False, encoding='utf-8')

                    logger.info(f"Extracted {len(df)} rows from HTML table")
                    logger.info(f"Saved to: {output_path}")
                    return True

        logger.error("Could not find data table in HTML")
        return False

    except Exception as e:
        logger.error(f"Error extracting from HTML: {e}")
        return False


def download_all_years(years: list = None, output_dir: Path = None) -> dict:
    """
    Download CSV files for specified years.

    Args:
        years: List of year keys (e.g., ['2024_25', '2023_24']). If None, downloads all.
        output_dir: Directory to save files. If None, uses DATA_DIR.

    Returns:
        Dictionary mapping year to success status
    """
    if years is None:
        years = list(URLS.keys())

    if output_dir is None:
        output_dir = DATA_DIR

    results = {}

    for year in years:
        if year not in URLS:
            logger.warning(f"Unknown year: {year}, skipping")
            continue

        url = URLS[year]
        output_path = output_dir / f"bildungsstatistik_{year}.csv"

        logger.info(f"\n{'='*60}")
        logger.info(f"Downloading data for school year {year.replace('_', '/')}")
        logger.info(f"{'='*60}")

        success = download_csv(url, output_path)
        results[year] = success

        # Be polite between requests
        if len(years) > 1:
            time.sleep(2)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download Berlin school statistics CSV from bildungsstatistik.berlin.de",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all available years
    python3 download_bildungsstatistik_csv.py --all

    # Download specific year
    python3 download_bildungsstatistik_csv.py --year 2024_25

    # Download both years explicitly
    python3 download_bildungsstatistik_csv.py --year 2024_25 --year 2023_24
        """
    )

    parser.add_argument(
        "--year", "-y",
        action="append",
        dest="years",
        choices=list(URLS.keys()),
        help="School year to download (can be specified multiple times)"
    )

    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Download all available years"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=f"Output directory (default: {DATA_DIR})"
    )

    args = parser.parse_args()

    # Update output directory if specified
    output_dir = DATA_DIR
    if args.output_dir:
        output_dir = Path(args.output_dir)

    # Determine which years to download
    if args.all:
        years = list(URLS.keys())
    elif args.years:
        years = args.years
    else:
        # Default to all years
        years = list(URLS.keys())

    logger.info(f"Will download data for years: {years}")
    logger.info(f"Output directory: {output_dir}")

    # Download
    results = download_all_years(years, output_dir=output_dir)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("DOWNLOAD SUMMARY")
    logger.info(f"{'='*60}")

    for year, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"  {year.replace('_', '/')}: {status}")

    # Return exit code
    all_success = all(results.values())
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
