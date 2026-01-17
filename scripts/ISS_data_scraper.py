#!/usr/bin/env python
"""
ISS Data Scraper - Scrapes school data from sekundarschulen-berlin.de

This script collects data from multiple pages on sekundarschulen-berlin.de
and combines them into a single consolidated CSV file.

Data sources:
- nachfrage: https://www.sekundarschulen-berlin.de/nachfrage
- migrationshintergrund: https://www.sekundarschulen-berlin.de/migrationshintergrund
- belastung: https://www.sekundarschulen-berlin.de/belastungsstufen
- abitur: https://www.sekundarschulen-berlin.de/abitur
- lehrerzahlen: https://www.sekundarschulen-berlin.de/lehrerzahlen
- schülerzahlen: https://www.sekundarschulen-berlin.de/statistik
- addresses: https://www.sekundarschulen-berlin.de/adressliste

Usage:
    python scripts/ISS_data_scraper.py [--output OUTPUT_PATH] [--debug]
"""

import sys
import os
import argparse
import logging
import time
import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, Browser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

BASE_URL = "https://www.sekundarschulen-berlin.de"

# Data source URLs and their identifiers
DATA_SOURCES = {
    "addresses": f"{BASE_URL}/adressliste",
    "nachfrage": f"{BASE_URL}/nachfrage",
    "migrationshintergrund": f"{BASE_URL}/migrationshintergrund",
    "belastung": f"{BASE_URL}/belastungsstufen",
    "abitur": f"{BASE_URL}/abitur",
    "lehrerzahlen": f"{BASE_URL}/lehrerzahlen",
    "schuelerzahlen": f"{BASE_URL}/statistik",
}

# Column mappings for final output
FINAL_COLUMNS = [
    "Schulnummer",
    "Schulname",
    "Bezirk",
    "Adresse",
    "Schüler_2024/25",
    "Lehrer_2024/25",
    "Schüler_2023/24",
    "Lehrer_2023/24",
    "Schüler_2022/23",
    "Lehrer_2022/23",
    "Sprachen",
    "Homepage",
    "Nachfrage_Plätze_2024/25",
    "Nachfrage_Wünsche_2024/25",
    "Nachfrage_Plätze_2025/26",
    "Nachfrage_Wünsche_2025/26",
    "Abiturnotendurchschnitt_2024",
    "Abiturnotendurchschnitt_2025",
    "Abiturerfolgsquote_2024",
    "Abiturerfolgsquote_2025",
    "Migrationshintergrund_%",
    "Belastungsstufe",
]


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SchoolData:
    """Container for all scraped school data"""
    schulnummer: Optional[str] = None
    schulname: Optional[str] = None
    bezirk: Optional[str] = None
    adresse: Optional[str] = None
    homepage: Optional[str] = None
    sprachen: Optional[str] = None
    detail_url: Optional[str] = None

    # Student/Teacher counts by year
    schueler_2024_25: Optional[int] = None
    lehrer_2024_25: Optional[int] = None
    schueler_2023_24: Optional[int] = None
    lehrer_2023_24: Optional[int] = None
    schueler_2022_23: Optional[int] = None
    lehrer_2022_23: Optional[int] = None

    # Nachfrage (demand) data
    nachfrage_plaetze_2024_25: Optional[int] = None
    nachfrage_wuensche_2024_25: Optional[int] = None
    nachfrage_plaetze_2025_26: Optional[int] = None
    nachfrage_wuensche_2025_26: Optional[int] = None

    # Abitur data
    abitur_notendurchschnitt_2024: Optional[float] = None
    abitur_notendurchschnitt_2025: Optional[float] = None
    abitur_erfolgsquote_2024: Optional[float] = None
    abitur_erfolgsquote_2025: Optional[float] = None

    # Additional metrics
    migrationshintergrund_pct: Optional[float] = None
    belastungsstufe: Optional[str] = None


# ============================================================================
# Scraper Classes
# ============================================================================

class ISSDataScraper:
    """Main scraper class for sekundarschulen-berlin.de"""

    def __init__(self, headless: bool = True, debug: bool = False):
        self.headless = headless
        self.debug = debug
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.schools: Dict[str, SchoolData] = {}  # keyed by school name

    def __enter__(self):
        self.start_browser()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()

    def start_browser(self):
        """Initialize Playwright browser"""
        logger.info("Starting browser...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.page = self.context.new_page()
        logger.info("Browser started successfully")

    def close_browser(self):
        """Close browser and cleanup"""
        if self.browser:
            self.browser.close()
        if hasattr(self, 'playwright'):
            self.playwright.stop()
        logger.info("Browser closed")

    def wait_and_get_content(self, url: str, wait_selector: str = "table") -> str:
        """Navigate to URL and wait for content to load"""
        logger.info(f"Fetching: {url}")
        self.page.goto(url, wait_until="networkidle")

        try:
            self.page.wait_for_selector(wait_selector, timeout=10000)
        except Exception as e:
            logger.warning(f"Timeout waiting for {wait_selector} on {url}: {e}")

        # Small delay to ensure dynamic content loads
        time.sleep(1)

        content = self.page.content()

        if self.debug:
            # Save HTML for debugging
            debug_dir = "debug_html"
            os.makedirs(debug_dir, exist_ok=True)
            filename = url.split("/")[-1] or "index"
            with open(f"{debug_dir}/{filename}.html", "w", encoding="utf-8") as f:
                f.write(content)
            logger.debug(f"Saved debug HTML to {debug_dir}/{filename}.html")

        return content

    def parse_table(self, html: str) -> List[Dict[str, Any]]:
        """Parse HTML table into list of dictionaries"""
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')

        if not tables:
            logger.warning("No tables found in HTML")
            return []

        # Usually the main data table is the largest one
        main_table = max(tables, key=lambda t: len(t.find_all('tr')))

        rows = main_table.find_all('tr')
        if not rows:
            return []

        # Extract headers
        header_row = rows[0]
        headers = []
        for th in header_row.find_all(['th', 'td']):
            header_text = th.get_text(strip=True)
            headers.append(header_text)

        if self.debug:
            logger.debug(f"Found headers: {headers}")

        # Extract data rows
        data = []
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) == 0:
                continue

            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    # Check for links (school detail pages)
                    link = cell.find('a')
                    if link and link.get('href'):
                        row_data[f"{headers[i]}_link"] = link.get('href')
                    row_data[headers[i]] = cell.get_text(strip=True)

            if row_data:
                data.append(row_data)

        logger.info(f"Parsed {len(data)} rows from table")
        return data

    def get_or_create_school(self, school_name: str) -> SchoolData:
        """Get existing school data or create new entry"""
        # Normalize school name for matching
        normalized = school_name.strip()

        if normalized not in self.schools:
            self.schools[normalized] = SchoolData(schulname=normalized)

        return self.schools[normalized]

    def scrape_addresses(self):
        """Scrape address list - this is the base data with school links"""
        logger.info("=" * 60)
        logger.info("Scraping address list...")

        html = self.wait_and_get_content(DATA_SOURCES["addresses"])
        data = self.parse_table(html)

        for row in data:
            # Find school name column (might be "Schule", "Schulname", "Name", etc.)
            school_name = None
            for key in ["Schule", "Schulname", "Name", "Schule / Name"]:
                if key in row:
                    school_name = row[key]
                    break

            if not school_name:
                # Try first column
                school_name = list(row.values())[0] if row else None

            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            # Extract address info
            school.bezirk = row.get("Bezirk") or row.get("Ortsteil")
            school.adresse = row.get("Adresse") or row.get("Straße") or row.get("Anschrift")

            # Get detail page link
            for key in row:
                if key.endswith("_link"):
                    href = row[key]
                    if href and not href.startswith("http"):
                        href = BASE_URL + href
                    school.detail_url = href
                    break

        logger.info(f"Found {len(self.schools)} schools from address list")

    def scrape_school_details(self):
        """Scrape individual school pages for Schulnummer and Sprachen"""
        logger.info("=" * 60)
        logger.info("Scraping individual school detail pages...")

        total = len(self.schools)
        for i, (name, school) in enumerate(self.schools.items(), 1):
            if not school.detail_url:
                logger.warning(f"No detail URL for {name}")
                continue

            logger.info(f"[{i}/{total}] Fetching details for: {name}")

            try:
                html = self.wait_and_get_content(school.detail_url, wait_selector="body")
                soup = BeautifulSoup(html, 'lxml')

                # Look for Schulnummer (school code)
                # Common patterns: "Schulnummer: 07K05" or in a definition list
                text = soup.get_text()

                # Try regex patterns for school number
                patterns = [
                    r'Schulnummer[:\s]+(\d{2}[A-Z]\d{2})',
                    r'BSN[:\s]+(\d{2}[A-Z]\d{2})',
                    r'(\d{2}[A-Z]\d{2})',  # Standard Berlin school number format
                ]

                for pattern in patterns:
                    match = re.search(pattern, text)
                    if match:
                        school.schulnummer = match.group(1)
                        break

                # Look for languages (Sprachen/Fremdsprachen)
                sprachen_patterns = [
                    r'(?:Fremd)?[Ss]prachen[:\s]+([^\n]+)',
                    r'Sprachenangebot[:\s]+([^\n]+)',
                ]

                for pattern in sprachen_patterns:
                    match = re.search(pattern, text)
                    if match:
                        school.sprachen = match.group(1).strip()
                        break

                # Look for homepage
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if href.startswith('http') and 'sekundarschulen-berlin' not in href:
                        # Likely the school's own website
                        school.homepage = href
                        break

                # Small delay to be polite to the server
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error scraping details for {name}: {e}")
                continue

    def scrape_schuelerzahlen(self):
        """Scrape student numbers (Statistik page)"""
        logger.info("=" * 60)
        logger.info("Scraping student numbers...")

        html = self.wait_and_get_content(DATA_SOURCES["schuelerzahlen"])
        data = self.parse_table(html)

        for row in data:
            school_name = self._find_school_name(row)
            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            # Try to extract student counts for different years
            # Column names might be like "2024/25", "Schüler 2024/25", etc.
            for key, value in row.items():
                value_clean = self._parse_number(value)
                if value_clean is None:
                    continue

                if "2024" in key and "25" in key:
                    school.schueler_2024_25 = value_clean
                elif "2023" in key and "24" in key:
                    school.schueler_2023_24 = value_clean
                elif "2022" in key and "23" in key:
                    school.schueler_2022_23 = value_clean

    def scrape_lehrerzahlen(self):
        """Scrape teacher numbers"""
        logger.info("=" * 60)
        logger.info("Scraping teacher numbers...")

        html = self.wait_and_get_content(DATA_SOURCES["lehrerzahlen"])
        data = self.parse_table(html)

        for row in data:
            school_name = self._find_school_name(row)
            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            for key, value in row.items():
                value_clean = self._parse_number(value)
                if value_clean is None:
                    continue

                if "2024" in key and "25" in key:
                    school.lehrer_2024_25 = value_clean
                elif "2023" in key and "24" in key:
                    school.lehrer_2023_24 = value_clean
                elif "2022" in key and "23" in key:
                    school.lehrer_2022_23 = value_clean

    def scrape_nachfrage(self):
        """Scrape demand/application data"""
        logger.info("=" * 60)
        logger.info("Scraping demand (Nachfrage) data...")

        html = self.wait_and_get_content(DATA_SOURCES["nachfrage"])
        data = self.parse_table(html)

        for row in data:
            school_name = self._find_school_name(row)
            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            for key, value in row.items():
                key_lower = key.lower()
                value_clean = self._parse_number(value)

                if value_clean is None:
                    continue

                # Match patterns like "Plätze 2024/25", "Wünsche 2024/25"
                if "2024" in key and "25" in key:
                    if "plätze" in key_lower or "platz" in key_lower:
                        school.nachfrage_plaetze_2024_25 = value_clean
                    elif "wünsche" in key_lower or "wunsch" in key_lower:
                        school.nachfrage_wuensche_2024_25 = value_clean
                elif "2025" in key and "26" in key:
                    if "plätze" in key_lower or "platz" in key_lower:
                        school.nachfrage_plaetze_2025_26 = value_clean
                    elif "wünsche" in key_lower or "wunsch" in key_lower:
                        school.nachfrage_wuensche_2025_26 = value_clean

    def scrape_abitur(self):
        """Scrape Abitur (graduation) data"""
        logger.info("=" * 60)
        logger.info("Scraping Abitur data...")

        html = self.wait_and_get_content(DATA_SOURCES["abitur"])
        data = self.parse_table(html)

        for row in data:
            school_name = self._find_school_name(row)
            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            for key, value in row.items():
                key_lower = key.lower()

                # Parse as float for grades/percentages
                value_clean = self._parse_float(value)
                if value_clean is None:
                    continue

                if "2024" in key:
                    if "durchschnitt" in key_lower or "note" in key_lower:
                        school.abitur_notendurchschnitt_2024 = value_clean
                    elif "quote" in key_lower or "erfolg" in key_lower:
                        school.abitur_erfolgsquote_2024 = value_clean
                elif "2025" in key:
                    if "durchschnitt" in key_lower or "note" in key_lower:
                        school.abitur_notendurchschnitt_2025 = value_clean
                    elif "quote" in key_lower or "erfolg" in key_lower:
                        school.abitur_erfolgsquote_2025 = value_clean

    def scrape_migrationshintergrund(self):
        """Scrape migration background data"""
        logger.info("=" * 60)
        logger.info("Scraping migration background data...")

        html = self.wait_and_get_content(DATA_SOURCES["migrationshintergrund"])
        data = self.parse_table(html)

        for row in data:
            school_name = self._find_school_name(row)
            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            # Look for percentage column
            for key, value in row.items():
                if "%" in key or "anteil" in key.lower() or "migration" in key.lower():
                    value_clean = self._parse_float(value)
                    if value_clean is not None:
                        school.migrationshintergrund_pct = value_clean
                        break

    def scrape_belastungsstufen(self):
        """Scrape load level (Belastungsstufe) data"""
        logger.info("=" * 60)
        logger.info("Scraping load level data...")

        html = self.wait_and_get_content(DATA_SOURCES["belastung"])
        data = self.parse_table(html)

        for row in data:
            school_name = self._find_school_name(row)
            if not school_name:
                continue

            school = self.get_or_create_school(school_name)

            # Look for Belastungsstufe column
            for key, value in row.items():
                if "belastung" in key.lower() or "stufe" in key.lower():
                    school.belastungsstufe = value.strip()
                    break

    def _find_school_name(self, row: Dict[str, Any]) -> Optional[str]:
        """Find school name in a row dictionary"""
        for key in ["Schule", "Schulname", "Name", "Schule / Name", "ISS"]:
            if key in row and row[key]:
                return row[key]
        # Try first non-link column
        for key, value in row.items():
            if not key.endswith("_link") and value:
                return value
        return None

    def _parse_number(self, value: Any) -> Optional[int]:
        """Parse string to integer, handling German number formats"""
        if value is None:
            return None
        try:
            # Remove thousand separators and whitespace
            cleaned = str(value).replace(".", "").replace(" ", "").replace(",", "").strip()
            if cleaned and cleaned.isdigit():
                return int(cleaned)
        except (ValueError, AttributeError):
            pass
        return None

    def _parse_float(self, value: Any) -> Optional[float]:
        """Parse string to float, handling German number formats"""
        if value is None:
            return None
        try:
            # German uses comma as decimal separator
            cleaned = str(value).replace(" ", "").replace("%", "").strip()
            cleaned = cleaned.replace(",", ".")
            if cleaned:
                return float(cleaned)
        except (ValueError, AttributeError):
            pass
        return None

    def scrape_all(self):
        """Run all scrapers in sequence"""
        # Start with addresses to get base school list and detail URLs
        self.scrape_addresses()

        # Scrape individual school pages for codes and languages
        self.scrape_school_details()

        # Scrape all data tables
        self.scrape_schuelerzahlen()
        self.scrape_lehrerzahlen()
        self.scrape_nachfrage()
        self.scrape_abitur()
        self.scrape_migrationshintergrund()
        self.scrape_belastungsstufen()

        logger.info("=" * 60)
        logger.info(f"Scraping complete. Total schools: {len(self.schools)}")

    def to_dataframe(self) -> pd.DataFrame:
        """Convert scraped data to pandas DataFrame"""
        records = []

        for name, school in self.schools.items():
            records.append({
                "Schulnummer": school.schulnummer,
                "Schulname": school.schulname,
                "Bezirk": school.bezirk,
                "Adresse": school.adresse,
                "Schüler_2024/25": school.schueler_2024_25,
                "Lehrer_2024/25": school.lehrer_2024_25,
                "Schüler_2023/24": school.schueler_2023_24,
                "Lehrer_2023/24": school.lehrer_2023_24,
                "Schüler_2022/23": school.schueler_2022_23,
                "Lehrer_2022/23": school.lehrer_2022_23,
                "Sprachen": school.sprachen,
                "Homepage": school.homepage,
                "Nachfrage_Plätze_2024/25": school.nachfrage_plaetze_2024_25,
                "Nachfrage_Wünsche_2024/25": school.nachfrage_wuensche_2024_25,
                "Nachfrage_Plätze_2025/26": school.nachfrage_plaetze_2025_26,
                "Nachfrage_Wünsche_2025/26": school.nachfrage_wuensche_2025_26,
                "Abiturnotendurchschnitt_2024": school.abitur_notendurchschnitt_2024,
                "Abiturnotendurchschnitt_2025": school.abitur_notendurchschnitt_2025,
                "Abiturerfolgsquote_2024": school.abitur_erfolgsquote_2024,
                "Abiturerfolgsquote_2025": school.abitur_erfolgsquote_2025,
                "Migrationshintergrund_%": school.migrationshintergrund_pct,
                "Belastungsstufe": school.belastungsstufe,
            })

        df = pd.DataFrame(records)

        # Reorder columns to match expected output
        cols = [c for c in FINAL_COLUMNS if c in df.columns]
        other_cols = [c for c in df.columns if c not in cols]
        df = df[cols + other_cols]

        return df

    def to_csv(self, output_path: str):
        """Export data to CSV file"""
        df = self.to_dataframe()
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"Data exported to: {output_path}")
        return df


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape ISS (Integrierte Sekundarschulen) data from Berlin"
    )
    parser.add_argument(
        "--output", "-o",
        default="data/iss_berlin_schools.csv",
        help="Output CSV file path (default: data/iss_berlin_schools.csv)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (saves HTML files for inspection)"
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (not headless)"
    )

    args = parser.parse_args()

    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    logger.info("Starting ISS Data Scraper")
    logger.info(f"Output will be saved to: {args.output}")

    try:
        with ISSDataScraper(headless=not args.visible, debug=args.debug) as scraper:
            scraper.scrape_all()
            df = scraper.to_csv(args.output)

            # Print summary
            print("\n" + "=" * 60)
            print("SCRAPING SUMMARY")
            print("=" * 60)
            print(f"Total schools scraped: {len(df)}")
            print(f"Schools with Schulnummer: {df['Schulnummer'].notna().sum()}")
            print(f"Schools with Sprachen: {df['Sprachen'].notna().sum()}")
            print(f"Schools with Abitur data: {df['Abiturnotendurchschnitt_2024'].notna().sum()}")
            print(f"\nData saved to: {args.output}")
            print("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
