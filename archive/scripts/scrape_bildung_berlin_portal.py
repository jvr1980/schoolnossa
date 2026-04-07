#!/usr/bin/env python3
"""
Bildung Berlin Portal Scraper - Second Pass

This script navigates the bildung.berlin.de school directory to fill in missing data
for schools that have incomplete information in the master table.

Features:
1. Searches for schools by schulnummer
2. Navigates to school detail pages
3. Extracts data from sub-sections (Schüler/Lehrer, Sprachen, Besonderheiten, etc.)
4. Uses LLM vision to analyze screenshots when structured scraping fails
5. Handles ISS-Gymnasium dual classification detection

Target columns to fill:
- schueler_2024_25, lehrer_2024_25 (and historical years)
- sprachen
- abitur_durchschnitt_2024/2023/2025, abitur_erfolgsquote_2024/2025
- nachfrage_* columns
- belastungsstufe
- migration_2024_25/2023_24
- besonderheiten
- school_type (update to ISS-Gymnasium if applicable)

Usage:
    python3 scrape_bildung_berlin_portal.py --input school_master_table_processed.csv --output school_master_table_enriched.csv
    python3 scrape_bildung_berlin_portal.py --schulnummer 03B08  # Test single school
    python3 scrape_bildung_berlin_portal.py --missing-only  # Only process schools with missing data
"""

import os
import re
import json
import time
import base64
import logging
import argparse
import requests
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from urllib.parse import urljoin, urlencode, quote
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import yaml
from bs4 import BeautifulSoup

# Optional: Selenium for JavaScript-rendered pages
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Optional: Firecrawl for advanced scraping
try:
    from firecrawl import FirecrawlApp
    FIRECRAWL_AVAILABLE = True
except ImportError:
    FIRECRAWL_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scrape_bildung_berlin.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"
BASE_URL = "https://www.bildung.berlin.de/Schulverzeichnis/"
SEARCH_URL = BASE_URL + "SchulListe.aspx"


@dataclass
class SchoolData:
    """Container for scraped school data."""
    schulnummer: str
    schulname: Optional[str] = None

    # School type (may be updated to ISS-Gymnasium)
    detected_school_types: List[str] = field(default_factory=list)
    is_iss_gymnasium: bool = False

    # Student/Teacher counts
    schueler_2024_25: Optional[int] = None
    lehrer_2024_25: Optional[int] = None
    schueler_2023_24: Optional[int] = None
    lehrer_2023_24: Optional[int] = None
    schueler_2022_23: Optional[int] = None
    lehrer_2022_23: Optional[int] = None

    # Languages
    sprachen: Optional[str] = None

    # Abitur results
    abitur_durchschnitt_2024: Optional[float] = None
    abitur_durchschnitt_2023: Optional[float] = None
    abitur_durchschnitt_2025: Optional[float] = None
    abitur_erfolgsquote_2024: Optional[float] = None
    abitur_erfolgsquote_2025: Optional[float] = None

    # Demand/Application stats
    nachfrage_plaetze_2025_26: Optional[int] = None
    nachfrage_wuensche_2025_26: Optional[int] = None
    nachfrage_prozent_2025_26: Optional[float] = None
    nachfrage_plaetze_2024_25: Optional[int] = None
    nachfrage_wuensche_2024_25: Optional[int] = None

    # Other stats
    belastungsstufe: Optional[str] = None
    migration_2024_25: Optional[float] = None
    migration_2023_24: Optional[float] = None
    besonderheiten: Optional[str] = None

    # Metadata
    scrape_source: str = "bildung.berlin.de"
    scrape_timestamp: Optional[str] = None
    scrape_error: Optional[str] = None


def load_config(config_path: Path = CONFIG_PATH) -> Dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


class VisionLLM:
    """Use LLM with vision capabilities to analyze screenshots."""

    def __init__(self, config: Dict):
        self.config = config
        api_keys = config.get('api_keys', {})
        models = config.get('models', {})

        # Use OpenAI GPT-4o or GPT-4o-mini for vision
        self.api_key = api_keys.get('openai')
        self.model = models.get('openai_vision', 'gpt-4o-mini')
        self.base_url = "https://api.openai.com/v1/chat/completions"

        if not self.api_key:
            logger.warning("OpenAI API key not found - vision analysis disabled")

    def analyze_screenshot(self, image_data: bytes, prompt: str) -> Optional[str]:
        """Analyze a screenshot using vision LLM."""
        if not self.api_key:
            return None

        # Encode image to base64
        base64_image = base64.b64encode(image_data).decode('utf-8')

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{base64_image}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 2000,
            "temperature": 0.1
        }

        try:
            response = requests.post(self.base_url, headers=headers, json=data, timeout=60)
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Vision analysis failed: {e}")
            return None

    def extract_school_data_from_screenshot(self, image_data: bytes, section_name: str) -> Dict[str, Any]:
        """Extract structured data from a screenshot of a school page section."""

        prompt = f"""Analyze this screenshot from the Berlin school directory (bildung.berlin.de).
This is the "{section_name}" section of a school's detail page.

Extract any of the following data you can find. Return ONLY a valid JSON object with the data found.
Use null for any fields not visible or not applicable.

Expected fields depending on the section:
- For "Schüler und Personal" section:
  - schueler_2024_25: number of students 2024/25
  - lehrer_2024_25: number of teachers 2024/25
  - schueler_2023_24: number of students 2023/24
  - lehrer_2023_24: number of teachers 2023/24

- For "Sprachen" or "Fremdsprachen" section:
  - sprachen: comma-separated list of languages offered

- For "Abitur" or "Prüfungsergebnisse" section:
  - abitur_durchschnitt_2024: average Abitur grade 2024
  - abitur_durchschnitt_2023: average Abitur grade 2023
  - abitur_erfolgsquote_2024: Abitur success rate 2024 (percentage)

- For "Anmeldungen" or "Nachfrage" section:
  - nachfrage_plaetze_2025_26: available places
  - nachfrage_wuensche_2025_26: number of applications/wishes
  - nachfrage_prozent_2025_26: oversubscription percentage

- For "Besonderheiten" or "Profil" section:
  - besonderheiten: special features, programs, focus areas (as text)

- For school type detection:
  - detected_types: list of school types found (e.g., ["ISS", "Gymnasium"])

Return ONLY the JSON object, no explanations."""

        result = self.analyze_screenshot(image_data, prompt)
        if not result:
            return {}

        # Parse JSON from response
        try:
            # Extract JSON from response (may be wrapped in markdown code blocks)
            json_match = re.search(r'\{[\s\S]*\}', result)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse vision response as JSON: {e}")

        return {}


class BildungBerlinScraper:
    """Scraper for bildung.berlin.de school directory."""

    def __init__(self, config: Dict, use_selenium: bool = True, use_firecrawl: bool = False):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.use_firecrawl = use_firecrawl and FIRECRAWL_AVAILABLE
        self.driver = None
        self.firecrawl = None
        self.vision_llm = VisionLLM(config)

        if self.use_firecrawl:
            api_keys = config.get('api_keys', {})
            firecrawl_key = api_keys.get('firecrawl')
            if firecrawl_key:
                self.firecrawl = FirecrawlApp(api_key=firecrawl_key)

    def _init_selenium(self):
        """Initialize Selenium WebDriver."""
        if self.driver:
            return

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-gpu')

        try:
            self.driver = webdriver.Chrome(options=options)
            logger.info("Selenium WebDriver initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Selenium: {e}")
            self.use_selenium = False

    def _close_selenium(self):
        """Close Selenium WebDriver."""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def _take_screenshot(self) -> Optional[bytes]:
        """Take a screenshot of the current page."""
        if not self.driver:
            return None
        try:
            return self.driver.get_screenshot_as_png()
        except Exception as e:
            logger.error(f"Screenshot failed: {e}")
            return None

    def search_school_by_number(self, schulnummer: str) -> Optional[str]:
        """
        Search for a school by its number and return the detail page URL.
        The portal uses ASP.NET with postback, so we need Selenium.
        """
        if not self.use_selenium:
            logger.warning("Selenium not available - cannot search portal")
            return None

        self._init_selenium()

        try:
            # Go to the search page
            self.driver.get(BASE_URL + "index.aspx")
            time.sleep(2)

            # Find the school number input field and search button
            # The field name may vary - we'll try common patterns
            search_field = None
            for field_id in ['txtSchulnr', 'ctl00_ContentPlaceHolder1_txtSchulnr',
                            'txtSchulnummer', 'txtBSN']:
                try:
                    search_field = self.driver.find_element(By.ID, field_id)
                    break
                except NoSuchElementException:
                    continue

            if not search_field:
                # Try by name
                try:
                    search_field = self.driver.find_element(By.NAME, 'ctl00$ContentPlaceHolder1$txtSchulnr')
                except NoSuchElementException:
                    pass

            if not search_field:
                # Take screenshot for debugging
                screenshot = self._take_screenshot()
                if screenshot:
                    with open(f'debug_search_page_{schulnummer}.png', 'wb') as f:
                        f.write(screenshot)
                logger.error("Could not find search field on page")
                return None

            # Enter school number
            search_field.clear()
            search_field.send_keys(schulnummer)

            # Find and click search button
            search_button = None
            for btn_id in ['btnSuchen', 'ctl00_ContentPlaceHolder1_btnSuchen', 'btnSearch']:
                try:
                    search_button = self.driver.find_element(By.ID, btn_id)
                    break
                except NoSuchElementException:
                    continue

            if not search_button:
                # Try finding by text
                try:
                    search_button = self.driver.find_element(By.XPATH, "//input[@value='Suchen']")
                except NoSuchElementException:
                    try:
                        search_button = self.driver.find_element(By.XPATH, "//button[contains(text(),'Suchen')]")
                    except NoSuchElementException:
                        pass

            if search_button:
                search_button.click()
                time.sleep(3)

            # Now we should be on the results page
            # Look for links to the school detail page
            current_url = self.driver.current_url
            logger.info(f"After search, URL: {current_url}")

            # Take screenshot of results
            screenshot = self._take_screenshot()
            if screenshot:
                with open(f'debug_results_{schulnummer}.png', 'wb') as f:
                    f.write(screenshot)

            # Look for school links in the results
            # Usually these are links containing the schulnummer or leading to SchulSteckbrief.aspx
            school_links = self.driver.find_elements(By.XPATH, f"//a[contains(@href, '{schulnummer}')]")

            if not school_links:
                # Try finding any link to SchulSteckbrief
                school_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'SchulSteckbrief')]")

            if not school_links:
                # Try finding by clicking on the school name in a table/list
                school_links = self.driver.find_elements(By.XPATH, "//table//a[contains(@href, '.aspx')]")

            if school_links:
                # Click the first matching link
                detail_url = school_links[0].get_attribute('href')
                logger.info(f"Found school detail link: {detail_url}")
                return detail_url

            logger.warning(f"No detail page link found for {schulnummer}")
            return None

        except Exception as e:
            logger.error(f"Search failed for {schulnummer}: {e}")
            return None

    def get_school_detail_page(self, schulnummer: str) -> Tuple[Optional[str], List[Dict]]:
        """
        Navigate to school detail page and collect data from all sub-sections.
        Returns (main_page_html, list of sub-section data)
        """
        detail_url = self.search_school_by_number(schulnummer)
        if not detail_url:
            return None, []

        if not self.use_selenium:
            return None, []

        try:
            # Navigate to detail page
            self.driver.get(detail_url)
            time.sleep(2)

            main_html = self.driver.page_source
            sub_sections = []

            # Take screenshot of main page
            main_screenshot = self._take_screenshot()
            if main_screenshot:
                with open(f'screenshots/{schulnummer}_main.png', 'wb') as f:
                    f.write(main_screenshot)

            # Find all navigation links on the left sidebar
            # These typically lead to sub-sections like "Schüler und Personal", "Sprachen", etc.
            nav_links = self.driver.find_elements(By.XPATH, "//div[@class='leftNav']//a | //ul[@class='navList']//a | //nav//a")

            if not nav_links:
                # Try other common patterns
                nav_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'SchulSteckbrief') or contains(@href, 'Schulprofil')]")

            logger.info(f"Found {len(nav_links)} navigation links")

            # Collect info about each sub-section
            section_urls = []
            for link in nav_links:
                try:
                    href = link.get_attribute('href')
                    text = link.text.strip()
                    if href and text:
                        section_urls.append((text, href))
                except:
                    continue

            # Visit each sub-section
            for section_name, section_url in section_urls:
                try:
                    logger.info(f"Visiting section: {section_name}")
                    self.driver.get(section_url)
                    time.sleep(1.5)

                    # Get page content
                    section_html = self.driver.page_source

                    # Take screenshot
                    screenshot = self._take_screenshot()
                    screenshot_path = None
                    if screenshot:
                        safe_name = re.sub(r'[^\w\-]', '_', section_name)
                        screenshot_path = f'screenshots/{schulnummer}_{safe_name}.png'
                        os.makedirs('screenshots', exist_ok=True)
                        with open(screenshot_path, 'wb') as f:
                            f.write(screenshot)

                    sub_sections.append({
                        'name': section_name,
                        'url': section_url,
                        'html': section_html,
                        'screenshot_path': screenshot_path,
                        'screenshot_data': screenshot
                    })

                except Exception as e:
                    logger.warning(f"Failed to visit section {section_name}: {e}")

            return main_html, sub_sections

        except Exception as e:
            logger.error(f"Failed to get detail page for {schulnummer}: {e}")
            return None, []

    def parse_student_teacher_data(self, html: str) -> Dict[str, Any]:
        """Parse student and teacher counts from HTML."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')

        # Common patterns for student/teacher data
        # Look for tables with year data
        tables = soup.find_all('table')

        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True).lower()
                    value = cells[-1].get_text(strip=True)

                    # Try to extract numbers
                    numbers = re.findall(r'\d+', value)
                    if numbers:
                        num = int(numbers[0])

                        if 'schüler' in header or 'schueler' in header:
                            if '2024' in header or '24/25' in header:
                                data['schueler_2024_25'] = num
                            elif '2023' in header or '23/24' in header:
                                data['schueler_2023_24'] = num
                            elif '2022' in header or '22/23' in header:
                                data['schueler_2022_23'] = num

                        elif 'lehrer' in header or 'lehrkräfte' in header or 'personal' in header:
                            if '2024' in header or '24/25' in header:
                                data['lehrer_2024_25'] = num
                            elif '2023' in header or '23/24' in header:
                                data['lehrer_2023_24'] = num
                            elif '2022' in header or '22/23' in header:
                                data['lehrer_2022_23'] = num

        # Also look for specific divs/spans
        text = soup.get_text()

        # Pattern: "Schülerinnen und Schüler: 842"
        schueler_match = re.search(r'Schüler(?:innen und Schüler)?[:\s]+(\d+)', text)
        if schueler_match and 'schueler_2024_25' not in data:
            data['schueler_2024_25'] = int(schueler_match.group(1))

        lehrer_match = re.search(r'Lehrer(?:innen und Lehrer)?|Lehrkräfte[:\s]+(\d+)', text)
        if lehrer_match and lehrer_match.group(1) and 'lehrer_2024_25' not in data:
            data['lehrer_2024_25'] = int(lehrer_match.group(1))

        return data

    def parse_languages(self, html: str) -> Optional[str]:
        """Parse languages offered from HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        languages = set()
        common_languages = ['Englisch', 'Französisch', 'Spanisch', 'Latein', 'Russisch',
                          'Italienisch', 'Japanisch', 'Chinesisch', 'Polnisch', 'Türkisch',
                          'Arabisch', 'Griechisch', 'Portugiesisch', 'Hebräisch']

        text = soup.get_text()

        for lang in common_languages:
            if lang.lower() in text.lower():
                languages.add(lang)

        # Also look for language lists
        lists = soup.find_all(['ul', 'ol'])
        for lst in lists:
            items = lst.find_all('li')
            for item in items:
                item_text = item.get_text(strip=True)
                for lang in common_languages:
                    if lang.lower() in item_text.lower():
                        languages.add(lang)

        if languages:
            return ', '.join(sorted(languages))
        return None

    def parse_abitur_data(self, html: str) -> Dict[str, Any]:
        """Parse Abitur results from HTML."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Look for average grade patterns
        # Pattern: "Durchschnittsnote 2024: 2,3" or "Abiturdurchschnitt: 2.4"
        avg_patterns = [
            r'(?:Durchschnitt(?:snote)?|Abiturdurchschnitt)[:\s]*(\d[,\.]\d)',
            r'(\d[,\.]\d)\s*(?:Durchschnitt|Notendurchschnitt)',
        ]

        for pattern in avg_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Convert comma to dot and parse
                grade = float(matches[0].replace(',', '.'))
                if 1.0 <= grade <= 4.0:  # Valid Abitur range
                    if '2024' in text[:text.find(matches[0])+50]:
                        data['abitur_durchschnitt_2024'] = grade
                    elif '2023' in text[:text.find(matches[0])+50]:
                        data['abitur_durchschnitt_2023'] = grade
                    elif '2025' in text[:text.find(matches[0])+50]:
                        data['abitur_durchschnitt_2025'] = grade

        # Look for success rate
        # Pattern: "Erfolgsquote: 95%" or "95% bestanden"
        success_patterns = [
            r'Erfolgsquote[:\s]*(\d{1,3})[,\.]?(\d?)%',
            r'(\d{1,3})[,\.]?(\d?)%\s*(?:bestanden|Erfolg)',
        ]

        for pattern in success_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                rate = float(f"{matches[0][0]}.{matches[0][1] or '0'}")
                if 0 <= rate <= 100:
                    if '2024' in text:
                        data['abitur_erfolgsquote_2024'] = rate
                    elif '2025' in text:
                        data['abitur_erfolgsquote_2025'] = rate

        return data

    def parse_demand_data(self, html: str) -> Dict[str, Any]:
        """Parse school demand/application statistics from HTML."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Look for places and applications
        # Pattern: "Plätze: 120" or "Kapazität: 120"
        places_match = re.search(r'(?:Plätze|Kapazität)[:\s]*(\d+)', text, re.IGNORECASE)
        if places_match:
            if '2025' in text or '25/26' in text:
                data['nachfrage_plaetze_2025_26'] = int(places_match.group(1))
            elif '2024' in text or '24/25' in text:
                data['nachfrage_plaetze_2024_25'] = int(places_match.group(1))

        # Pattern: "Anmeldungen: 180" or "Wünsche: 180"
        wishes_match = re.search(r'(?:Anmeldungen|Wünsche|Erstwünsche)[:\s]*(\d+)', text, re.IGNORECASE)
        if wishes_match:
            if '2025' in text or '25/26' in text:
                data['nachfrage_wuensche_2025_26'] = int(wishes_match.group(1))
            elif '2024' in text or '24/25' in text:
                data['nachfrage_wuensche_2024_25'] = int(wishes_match.group(1))

        # Calculate percentage if we have both
        if 'nachfrage_plaetze_2025_26' in data and 'nachfrage_wuensche_2025_26' in data:
            places = data['nachfrage_plaetze_2025_26']
            wishes = data['nachfrage_wuensche_2025_26']
            if places > 0:
                data['nachfrage_prozent_2025_26'] = round(100 * wishes / places, 1)

        return data

    def parse_besonderheiten(self, html: str) -> Optional[str]:
        """Parse special features/profile from HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Look for sections with keywords
        keywords = ['Besonderheiten', 'Schulprofil', 'Profil', 'Schwerpunkt', 'Angebot']

        for keyword in keywords:
            # Find headings or divs containing the keyword
            elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'div', 'p'],
                                    string=lambda x: x and keyword.lower() in x.lower())

            for elem in elements:
                # Get the next sibling or child content
                next_content = elem.find_next(['ul', 'ol', 'p', 'div'])
                if next_content:
                    text = next_content.get_text(separator=', ', strip=True)
                    if len(text) > 20:  # Meaningful content
                        return text[:500]  # Limit length

        return None

    def parse_migration_data(self, html: str) -> Dict[str, Any]:
        """Parse migration/diversity statistics from HTML."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Look for migration percentage
        # Pattern: "Migrationshintergrund: 45%" or "ndH: 45%"
        patterns = [
            r'(?:Migrationshintergrund|ndH|nichtdeutscher Herkunft)[:\s]*(\d{1,3})[,\.]?(\d?)%',
            r'(\d{1,3})[,\.]?(\d?)%\s*(?:Migrationshintergrund|ndH)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                rate = float(f"{matches[0][0]}.{matches[0][1] or '0'}")
                if 0 <= rate <= 100:
                    if '2024' in text or '24/25' in text:
                        data['migration_2024_25'] = rate
                    elif '2023' in text or '23/24' in text:
                        data['migration_2023_24'] = rate

        return data

    def detect_school_types(self, html: str) -> List[str]:
        """Detect school types from page content (ISS, Gymnasium, etc.)."""
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text().lower()

        types = []

        if 'integrierte sekundarschule' in text or 'iss' in text:
            types.append('ISS')

        if 'gymnasium' in text:
            types.append('Gymnasium')

        if 'gemeinschaftsschule' in text:
            types.append('Gemeinschaftsschule')

        return types

    def scrape_school(self, schulnummer: str) -> SchoolData:
        """
        Scrape all available data for a school.
        Combines HTML parsing with vision-based extraction as fallback.
        """
        school_data = SchoolData(
            schulnummer=schulnummer,
            scrape_timestamp=datetime.now().isoformat()
        )

        logger.info(f"Scraping school {schulnummer}")

        # Get the detail page and sub-sections
        main_html, sub_sections = self.get_school_detail_page(schulnummer)

        if not main_html:
            school_data.scrape_error = "Could not access school detail page"
            return school_data

        # Parse main page for school types
        detected_types = self.detect_school_types(main_html)
        school_data.detected_school_types = detected_types
        school_data.is_iss_gymnasium = ('ISS' in detected_types and 'Gymnasium' in detected_types)

        # Process each sub-section
        for section in sub_sections:
            section_name = section['name'].lower()
            html = section['html']

            # Parse based on section type
            if 'schüler' in section_name or 'personal' in section_name:
                data = self.parse_student_teacher_data(html)
                for key, value in data.items():
                    if value is not None:
                        setattr(school_data, key, value)

            elif 'sprach' in section_name or 'fremdsprach' in section_name:
                languages = self.parse_languages(html)
                if languages:
                    school_data.sprachen = languages

            elif 'abitur' in section_name or 'prüfung' in section_name:
                data = self.parse_abitur_data(html)
                for key, value in data.items():
                    if value is not None:
                        setattr(school_data, key, value)

            elif 'anmeld' in section_name or 'nachfrage' in section_name:
                data = self.parse_demand_data(html)
                for key, value in data.items():
                    if value is not None:
                        setattr(school_data, key, value)

            elif 'besonderheit' in section_name or 'profil' in section_name:
                besonderheiten = self.parse_besonderheiten(html)
                if besonderheiten:
                    school_data.besonderheiten = besonderheiten

            elif 'migration' in section_name or 'herkunft' in section_name:
                data = self.parse_migration_data(html)
                for key, value in data.items():
                    if value is not None:
                        setattr(school_data, key, value)

            # If we didn't get data from HTML parsing, try vision
            if section.get('screenshot_data'):
                # Check if we got meaningful data from this section
                section_data = {}
                if 'schüler' in section_name and school_data.schueler_2024_25 is None:
                    section_data = self.vision_llm.extract_school_data_from_screenshot(
                        section['screenshot_data'], section['name']
                    )
                elif 'sprach' in section_name and school_data.sprachen is None:
                    section_data = self.vision_llm.extract_school_data_from_screenshot(
                        section['screenshot_data'], section['name']
                    )

                # Apply vision-extracted data
                for key, value in section_data.items():
                    if value is not None and hasattr(school_data, key):
                        current = getattr(school_data, key)
                        if current is None:
                            setattr(school_data, key, value)

        return school_data

    def close(self):
        """Clean up resources."""
        self._close_selenium()


def merge_scraped_data(df: pd.DataFrame, school_data: SchoolData) -> pd.DataFrame:
    """Merge scraped data into the dataframe, only filling missing values."""

    idx = df[df['schulnummer'] == school_data.schulnummer].index
    if len(idx) == 0:
        logger.warning(f"School {school_data.schulnummer} not found in dataframe")
        return df

    idx = idx[0]

    # Fields to potentially update
    fields = [
        'schueler_2024_25', 'lehrer_2024_25', 'schueler_2023_24', 'lehrer_2023_24',
        'schueler_2022_23', 'lehrer_2022_23', 'sprachen',
        'abitur_durchschnitt_2024', 'abitur_durchschnitt_2023', 'abitur_durchschnitt_2025',
        'abitur_erfolgsquote_2024', 'abitur_erfolgsquote_2025',
        'nachfrage_plaetze_2025_26', 'nachfrage_wuensche_2025_26', 'nachfrage_prozent_2025_26',
        'nachfrage_plaetze_2024_25', 'nachfrage_wuensche_2024_25',
        'belastungsstufe', 'migration_2024_25', 'migration_2023_24', 'besonderheiten'
    ]

    for field in fields:
        if not hasattr(school_data, field):
            continue

        new_value = getattr(school_data, field)
        if new_value is None:
            continue

        # Only update if the current value is missing
        if field in df.columns:
            current = df.at[idx, field]
            if pd.isna(current) or current == '' or current is None:
                df.at[idx, field] = new_value
                logger.info(f"Updated {field} for {school_data.schulnummer}: {new_value}")

    # Update school_type if it's an ISS-Gymnasium
    if school_data.is_iss_gymnasium:
        current_type = df.at[idx, 'school_type']
        if current_type == 'ISS':
            df.at[idx, 'school_type'] = 'ISS-Gymnasium'
            logger.info(f"Updated school_type for {school_data.schulnummer}: ISS -> ISS-Gymnasium")

    # Update metadata_source
    if 'metadata_source' in df.columns:
        current_source = df.at[idx, 'metadata_source']
        if pd.isna(current_source) or current_source == '':
            df.at[idx, 'metadata_source'] = school_data.scrape_source
        else:
            df.at[idx, 'metadata_source'] = f"{current_source}, {school_data.scrape_source}"

    return df


def get_schools_with_missing_data(df: pd.DataFrame, key_columns: List[str]) -> List[str]:
    """Get list of schulnummers for schools with missing data in key columns."""

    # Count missing values per row for key columns
    missing_mask = df[key_columns].isna().any(axis=1)
    missing_schools = df[missing_mask]['schulnummer'].tolist()

    logger.info(f"Found {len(missing_schools)} schools with missing data in key columns")
    return missing_schools


def main():
    parser = argparse.ArgumentParser(description='Scrape bildung.berlin.de for missing school data')
    parser.add_argument('--input', '-i', default='school_master_table_processed.csv',
                       help='Input CSV file')
    parser.add_argument('--output', '-o', default='school_master_table_enriched.csv',
                       help='Output CSV file')
    parser.add_argument('--schulnummer', '-s', type=str, default=None,
                       help='Process single school by number')
    parser.add_argument('--missing-only', action='store_true',
                       help='Only process schools with missing data')
    parser.add_argument('--limit', '-l', type=int, default=None,
                       help='Limit number of schools to process')
    parser.add_argument('--delay', '-d', type=float, default=2.0,
                       help='Delay between schools (seconds)')
    parser.add_argument('--no-selenium', action='store_true',
                       help='Disable Selenium (limits functionality)')
    parser.add_argument('--use-firecrawl', action='store_true',
                       help='Use Firecrawl for scraping')

    args = parser.parse_args()

    # Load config and data
    config = load_config()
    logger.info(f"Loading data from {args.input}")
    df = pd.read_csv(args.input, encoding='utf-8-sig')

    # Create screenshots directory
    os.makedirs('screenshots', exist_ok=True)

    # Determine which schools to process
    key_columns = [
        'schueler_2024_25', 'lehrer_2024_25', 'sprachen',
        'abitur_durchschnitt_2024', 'migration_2024_25', 'besonderheiten'
    ]
    # Filter to columns that exist
    key_columns = [c for c in key_columns if c in df.columns]

    if args.schulnummer:
        schools_to_process = [args.schulnummer]
    elif args.missing_only:
        schools_to_process = get_schools_with_missing_data(df, key_columns)
    else:
        schools_to_process = df['schulnummer'].tolist()

    if args.limit:
        schools_to_process = schools_to_process[:args.limit]

    logger.info(f"Processing {len(schools_to_process)} schools")

    # Initialize scraper
    scraper = BildungBerlinScraper(
        config,
        use_selenium=not args.no_selenium,
        use_firecrawl=args.use_firecrawl
    )

    try:
        # Process each school
        for i, schulnummer in enumerate(schools_to_process):
            logger.info(f"Processing {i+1}/{len(schools_to_process)}: {schulnummer}")

            try:
                school_data = scraper.scrape_school(schulnummer)
                df = merge_scraped_data(df, school_data)

                # Save progress periodically
                if (i + 1) % 10 == 0:
                    df.to_csv(args.output.replace('.csv', '_partial.csv'),
                             index=False, encoding='utf-8-sig')
                    logger.info(f"Saved partial progress ({i+1} schools)")

            except Exception as e:
                logger.error(f"Failed to process {schulnummer}: {e}")

            time.sleep(args.delay)

    finally:
        scraper.close()

    # Save final results
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    df.to_excel(args.output.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    logger.info(f"Saved enriched data to {args.output}")

    # Print summary
    print("\n" + "="*70)
    print("SCRAPING SUMMARY")
    print("="*70)
    print(f"Schools processed: {len(schools_to_process)}")

    # Show improvement in data completeness
    for col in key_columns:
        missing_before = df[col].isna().sum()
        print(f"{col}: {missing_before} still missing ({100*missing_before/len(df):.1f}%)")

    # Show ISS-Gymnasium updates
    iss_gy_count = (df['school_type'] == 'ISS-Gymnasium').sum()
    print(f"\nISS-Gymnasium schools: {iss_gy_count}")

    print("="*70)


if __name__ == "__main__":
    main()
