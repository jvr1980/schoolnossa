#!/usr/bin/env python3
"""
Bildung Berlin Portal Scraper V2 - Improved Version

This script scrapes the bildung.berlin.de school directory using the correct URL patterns
discovered through manual portal exploration.

URL Patterns:
- School list by district: SchulListe.aspx?BezNr={district_code}
- School portrait: Schulportrait.aspx?IDSchulzweig={id}
- Student data: schuelerschaft.aspx (after visiting portrait)
- Migration data: schuelerschaft.aspx?view=ndh
- Personnel data: schulpersonal.aspx
- Exam results: PruefErgebnisse.aspx
- Abitur PDF: DokLoader.aspx?Abiturdaten={schulnummer}_A_{year}.pdf

Key Discovery:
- Schools use IDSchulzweig (internal ID), not schulnummer directly
- Same school can have multiple IDSchulzweig entries (ISS, Gymnasium branches)
- We need to first find the IDSchulzweig by searching the school list

Usage:
    python3 scrape_bildung_berlin_v2.py --input school_master_table_processed.csv --output school_master_table_enriched.csv
    python3 scrape_bildung_berlin_v2.py --schulnummer 03B08  # Test single school
    python3 scrape_bildung_berlin_v2.py --missing-only --limit 10
"""

import os
import re
import json
import time
import base64
import logging
import argparse
import requests
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import yaml
from bs4 import BeautifulSoup

# Optional: Selenium for screenshots
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Warning: Selenium not available. Install with: pip install selenium")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scrape_bildung_berlin_v2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"
BASE_URL = "https://www.bildung.berlin.de/Schulverzeichnis/"

# Screenshots directory
SCREENSHOTS_DIR = Path(__file__).parent / "screenshots"


class VisionLLM:
    """Use OpenAI GPT-5-mini with vision to analyze screenshots."""

    def __init__(self, api_key: str, model: str = "gpt-5-mini-2025-08-07"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.openai.com/v1/chat/completions"

    def analyze_screenshot(self, image_data: bytes, prompt: str) -> Optional[str]:
        """Analyze a screenshot using vision LLM."""
        if not self.api_key:
            logger.warning("No OpenAI API key - vision analysis skipped")
            return None

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
                        {"type": "text", "text": prompt},
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
            "max_completion_tokens": 2000
        }

        try:
            response = requests.post(self.base_url, headers=headers, json=data, timeout=60)
            if response.status_code != 200:
                logger.error(f"Vision API error {response.status_code}: {response.text[:500]}")
                return None
            result = response.json()
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Vision API exception: {e}")
            return None

    def extract_student_data(self, image_data: bytes, schulnummer: str, schulname: str) -> Dict[str, Any]:
        """Extract student count data from a screenshot of the student data page."""
        prompt = f"""Analyze this screenshot from the Berlin school directory (bildung.berlin.de).
This shows student data ("Schülerinnen und Schüler") for school: {schulname} ({schulnummer})

Extract the following information from the table:
1. Total number of students for this Schulzweig (look for "Schülerinnen und Schüler" column)
2. The school year shown (e.g., 2024/25 or 2025/26)
3. The Schulzweig/Bildungsgang type (e.g., Berufliches Gymnasium, Berufsfachschulen, ISS, etc.)

IMPORTANT:
- Look for the rightmost column labeled "Schülerinnen und Schüler" which contains the student count
- If there are multiple rows (different Bildungsgänge), sum them up for the total
- Report the exact numbers you see, do not estimate

Return ONLY a valid JSON object with these fields:
{{
    "schueler_total": <total student count as integer>,
    "school_year": "<year like 2024/25>",
    "schulzweig_type": "<type of school branch>",
    "rows_found": [
        {{"bildungsgang": "<name>", "count": <number>}},
        ...
    ],
    "raw_observation": "<brief description of what you see in the table>"
}}

Return ONLY the JSON, no other text."""

        result = self.analyze_screenshot(image_data, prompt)
        if not result:
            return {}

        try:
            json_match = re.search(r'\{[\s\S]*\}', result)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse vision response: {e}")
            logger.debug(f"Raw response: {result[:500]}")

        return {}

    def extract_personnel_data(self, image_data: bytes, schulnummer: str) -> Dict[str, Any]:
        """Extract teacher/personnel count from screenshot."""
        prompt = f"""Analyze this screenshot from the Berlin school directory (bildung.berlin.de).
This shows personnel data ("Personal der Schule") for school {schulnummer}.

Extract the total number of teachers/Lehrkräfte.
Look for rows mentioning "Lehrkräfte" or "Lehrer" and find the count.

Return ONLY a valid JSON object:
{{
    "lehrer_total": <total teacher count as integer or null if not found>,
    "school_year": "<year like 2024/25>",
    "raw_observation": "<brief description of what you see>"
}}

Return ONLY the JSON, no other text."""

        result = self.analyze_screenshot(image_data, prompt)
        if not result:
            return {}

        try:
            json_match = re.search(r'\{[\s\S]*\}', result)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse vision response: {e}")

        return {}

    def extract_migration_data(self, image_data: bytes, schulnummer: str) -> Dict[str, Any]:
        """Extract migration/ndH percentage from screenshot."""
        prompt = f"""Analyze this screenshot from the Berlin school directory (bildung.berlin.de).
This shows migration data ("Nichtdeutsche Herkunftssprache" or ndH) for school {schulnummer}.

Extract the percentage of students with non-German native language (ndH).
Look for percentage values in the table.

Return ONLY a valid JSON object:
{{
    "migration_pct": <percentage as float, e.g. 45.2, or null if not found>,
    "school_year": "<year like 2024/25>",
    "raw_observation": "<brief description of what you see>"
}}

Return ONLY the JSON, no other text."""

        result = self.analyze_screenshot(image_data, prompt)
        if not result:
            return {}

        try:
            json_match = re.search(r'\{[\s\S]*\}', result)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse vision response: {e}")

        return {}


# District codes (first 2 digits of schulnummer)
DISTRICT_CODES = {
    '01': 'Mitte',
    '02': 'Friedrichshain-Kreuzberg',
    '03': 'Pankow',
    '04': 'Charlottenburg-Wilmersdorf',
    '05': 'Spandau',
    '06': 'Steglitz-Zehlendorf',
    '07': 'Tempelhof-Schöneberg',
    '08': 'Neukölln',
    '09': 'Treptow-Köpenick',
    '10': 'Marzahn-Hellersdorf',
    '11': 'Lichtenberg',
    '12': 'Reinickendorf',
}


@dataclass
class SchoolData:
    """Container for scraped school data."""
    schulnummer: str
    schulname: Optional[str] = None

    # IDSchulzweig mappings found
    id_schulzweig_list: List[Dict] = field(default_factory=list)

    # School type detection
    detected_school_types: List[str] = field(default_factory=list)
    is_iss_gymnasium: bool = False

    # Student/Teacher counts (current year)
    schueler_2024_25: Optional[int] = None
    lehrer_2024_25: Optional[int] = None
    schueler_2023_24: Optional[int] = None
    lehrer_2023_24: Optional[int] = None

    # Languages
    sprachen: Optional[str] = None

    # Abitur results
    abitur_durchschnitt_2024: Optional[float] = None
    abitur_durchschnitt_2023: Optional[float] = None
    abitur_durchschnitt_2025: Optional[float] = None
    abitur_erfolgsquote_2024: Optional[float] = None
    abitur_erfolgsquote_2025: Optional[float] = None

    # Migration data
    migration_2024_25: Optional[float] = None
    migration_2023_24: Optional[float] = None

    # Special features
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


class BildungBerlinScraperV2:
    """Improved scraper for bildung.berlin.de using vision-based extraction."""

    def __init__(self, config: Dict, use_vision: bool = True):
        self.config = config
        self.use_vision = use_vision and SELENIUM_AVAILABLE
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

        # Cache for IDSchulzweig lookups
        self._id_cache: Dict[str, List[Dict]] = {}

        # Selenium driver for screenshots
        self.driver = None

        # Vision LLM for screenshot analysis
        api_keys = config.get('api_keys', {})
        openai_key = api_keys.get('openai', '')
        self.vision_llm = VisionLLM(openai_key, model="gpt-5-mini-2025-08-07") if openai_key else None

        # Create screenshots directory
        SCREENSHOTS_DIR.mkdir(exist_ok=True)

    def _init_selenium(self):
        """Initialize Selenium WebDriver for screenshots."""
        if self.driver:
            return True

        if not SELENIUM_AVAILABLE:
            logger.warning("Selenium not available - vision extraction disabled")
            return False

        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1920,1200')
            options.add_argument('--disable-gpu')
            options.add_argument('--lang=de-DE')

            self.driver = webdriver.Chrome(options=options)
            logger.info("Selenium WebDriver initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Selenium: {e}")
            self.use_vision = False
            return False

    def _close_selenium(self):
        """Close Selenium WebDriver."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _take_screenshot(self, url: str, wait_seconds: float = 2.0) -> Optional[bytes]:
        """Navigate to URL and take screenshot."""
        if not self.driver:
            if not self._init_selenium():
                return None

        try:
            self.driver.get(url)
            time.sleep(wait_seconds)  # Wait for page to fully render
            return self.driver.get_screenshot_as_png()
        except Exception as e:
            logger.error(f"Screenshot failed for {url}: {e}")
            return None

    def _save_screenshot(self, image_data: bytes, schulnummer: str, section: str) -> Path:
        """Save screenshot to disk for debugging."""
        safe_section = re.sub(r'[^\w\-]', '_', section)
        path = SCREENSHOTS_DIR / f"{schulnummer}_{safe_section}.png"
        with open(path, 'wb') as f:
            f.write(image_data)
        return path

    def fetch_page(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch a page with retry logic."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
        return None

    def get_district_code(self, schulnummer: str) -> str:
        """Extract district code from schulnummer."""
        return schulnummer[:2]

    def find_school_ids(self, schulnummer: str, schulname: Optional[str] = None) -> List[Dict]:
        """
        Find all IDSchulzweig entries for a school by searching the district list.
        Returns list of {id, name, type} dicts.

        Uses school name for matching since schulnummer doesn't appear in the list page.
        """
        cache_key = schulnummer
        if cache_key in self._id_cache:
            return self._id_cache[cache_key]

        district = self.get_district_code(schulnummer)
        list_url = f"{BASE_URL}SchulListe.aspx?BezNr={district}"

        html = self.fetch_page(list_url)
        if not html:
            logger.error(f"Could not fetch school list for district {district}")
            return []

        soup = BeautifulSoup(html, 'html.parser')

        # Find all school links
        results = []
        links = soup.find_all('a', href=True)

        # Build a search pattern from school name
        search_terms = []
        if schulname:
            # Clean the name and create search terms
            clean_name = schulname.lower()
            # Remove common suffixes for matching
            clean_name = re.sub(r'\s*\(.*?\)\s*$', '', clean_name)  # Remove parenthetical
            search_terms.append(clean_name)

            # Also try significant unique words (at least 6 chars, not common school words)
            words = clean_name.replace('-', ' ').split()
            common_words = {'schule', 'gymnasium', 'oberschule', 'grundschule', 'berlin',
                          'staatliche', 'freie', 'integrierte', 'sekundarschule', 'school'}
            for word in words:
                # Only use distinctive words that are long enough
                if len(word) >= 6 and word not in common_words:
                    search_terms.append(word)

        for link in links:
            href = link.get('href', '')
            if 'Schulportrait.aspx' in href and 'IDSchulzweig' in href:
                # Extract IDSchulzweig (note: there may be a space after =)
                id_match = re.search(r'IDSchulzweig=\s*(\d+)', href)
                if id_match:
                    school_id = id_match.group(1)
                    link_name = link.get_text(strip=True).lower()

                    # Check if this matches our school by name
                    matched = False
                    for term in search_terms:
                        if term in link_name:
                            matched = True
                            break

                    if matched:
                        # Get the parent context for type detection
                        parent = link.find_parent(['div', 'td', 'li', 'tr'])
                        context_text = parent.get_text() if parent else link.get_text()
                        school_type = self._detect_type_from_text(context_text)

                        results.append({
                            'id': school_id,
                            'name': link.get_text(strip=True),
                            'type': school_type,
                            'url': f"{BASE_URL}Schulportrait.aspx?IDSchulzweig={school_id}"
                        })

        # Deduplicate by ID
        seen_ids = set()
        unique_results = []
        for r in results:
            if r['id'] not in seen_ids:
                seen_ids.add(r['id'])
                unique_results.append(r)

        self._id_cache[cache_key] = unique_results
        return unique_results

    def _detect_type_from_text(self, text: str) -> str:
        """Detect school type from text content."""
        text_lower = text.lower()
        types = []

        if 'gymnasium' in text_lower:
            types.append('Gymnasium')
        if 'integrierte sekundarschule' in text_lower or 'iss' in text_lower:
            types.append('ISS')
        if 'gemeinschaftsschule' in text_lower:
            types.append('Gemeinschaftsschule')
        if 'grundschule' in text_lower:
            types.append('Grundschule')
        if 'berufsfachschule' in text_lower:
            types.append('Berufsfachschule')
        if 'berufsschule' in text_lower:
            types.append('Berufsschule')
        if 'förderzentrum' in text_lower or 'förderschule' in text_lower:
            types.append('Förderschule')

        return ', '.join(types) if types else 'Unknown'

    def scrape_school_portrait(self, school_id: str) -> Dict[str, Any]:
        """Scrape the main school portrait page."""
        url = f"{BASE_URL}Schulportrait.aspx?IDSchulzweig={school_id}"
        html = self.fetch_page(url)

        if not html:
            return {'error': 'Could not fetch portrait page'}

        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        # Extract school name and number from the header
        header = soup.find(['h1', 'h2', 'h3'], string=lambda x: x and '-' in x if x else False)
        if header:
            header_text = header.get_text(strip=True)
            # Pattern: "School Name - 03B08"
            match = re.search(r'(.+?)\s*-\s*(\d{2}[A-Z]\d{2})', header_text)
            if match:
                data['schulname'] = match.group(1).strip()
                data['schulnummer'] = match.group(2)

        # Extract school type
        type_elem = soup.find(string=lambda x: x and ('Gymnasium' in x or 'Sekundarschule' in x or
                                                       'Grundschule' in x) if x else False)
        if type_elem:
            data['schulart'] = type_elem.strip()

        # Extract contact info
        web_link = soup.find('a', href=lambda x: x and x.startswith('http') and 'bildung.berlin' not in x if x else False)
        if web_link:
            data['website'] = web_link.get('href')

        email_link = soup.find('a', href=lambda x: x and x.startswith('mailto:') if x else False)
        if email_link:
            data['email'] = email_link.get('href').replace('mailto:', '')

        # Save navigation links for sub-pages
        nav_links = {}
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True).lower()

            if 'schuelerschaft' in href.lower():
                nav_links['schuelerschaft'] = href
            elif 'schulpersonal' in href.lower():
                nav_links['personal'] = href
            elif 'pruefergebnisse' in href.lower():
                nav_links['pruefungen'] = href
            elif 'schulprogramm' in href.lower():
                nav_links['schulprogramm'] = href

        data['nav_links'] = nav_links

        return data

    def scrape_student_data(self, school_id: str, schulnummer: str = "", schulname: str = "") -> Dict[str, Any]:
        """Scrape student count data using vision-based extraction."""
        # Build the URL for student data page
        portrait_url = f"{BASE_URL}Schulportrait.aspx?IDSchulzweig={school_id}"
        student_url = f"{BASE_URL}schuelerschaft.aspx"

        data = {}

        # Primary method: Vision-based extraction using screenshots
        if self.use_vision and self.vision_llm:
            logger.info(f"  Using vision extraction for student data (ID: {school_id})")

            # Take screenshot - need to visit portrait first, then student page
            if self._init_selenium():
                try:
                    # Visit portrait to establish session context
                    self.driver.get(portrait_url)
                    time.sleep(1.5)

                    # Now visit student data page
                    self.driver.get(student_url)
                    time.sleep(2.0)

                    # Take screenshot
                    screenshot = self.driver.get_screenshot_as_png()

                    if screenshot:
                        # Save for debugging
                        self._save_screenshot(screenshot, schulnummer or school_id, "schueler")

                        # Use vision LLM to extract data
                        vision_data = self.vision_llm.extract_student_data(
                            screenshot, schulnummer, schulname
                        )

                        if vision_data:
                            logger.info(f"  Vision extracted: {vision_data.get('raw_observation', 'N/A')[:100]}")

                            if vision_data.get('schueler_total'):
                                data['schueler_2024_25'] = int(vision_data['schueler_total'])
                                logger.info(f"  Student count from vision: {data['schueler_2024_25']}")

                            # Store detailed breakdown for reference
                            if vision_data.get('rows_found'):
                                data['_vision_breakdown'] = vision_data['rows_found']

                except Exception as e:
                    logger.error(f"  Vision extraction failed: {e}")

        # Fallback: HTML parsing (less reliable but works without Selenium)
        if 'schueler_2024_25' not in data:
            logger.info(f"  Falling back to HTML parsing for student data")
            self.fetch_page(portrait_url)
            time.sleep(0.5)
            html = self.fetch_page(student_url)

            if html:
                soup = BeautifulSoup(html, 'html.parser')
                tables = soup.find_all('table')
                for table in tables:
                    text = table.get_text()
                    if '2024/25' in text or '2025/26' in text:
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 2:
                                for cell in cells[1:]:
                                    cell_text = cell.get_text(strip=True)
                                    numbers = re.findall(r'\d+', cell_text)
                                    if numbers:
                                        count = int(numbers[0])
                                        if 'schüler' in text.lower() and count > 0:
                                            if 'schueler_2024_25' not in data:
                                                data['schueler_2024_25'] = 0
                                            data['schueler_2024_25'] += count

        return data

    def scrape_migration_data(self, school_id: str, schulnummer: str = "") -> Dict[str, Any]:
        """Scrape migration/ndH data using vision-based extraction."""
        portrait_url = f"{BASE_URL}Schulportrait.aspx?IDSchulzweig={school_id}"
        migration_url = f"{BASE_URL}schuelerschaft.aspx?view=ndh"

        data = {}

        # Primary: Vision-based extraction
        if self.use_vision and self.vision_llm:
            logger.info(f"  Using vision extraction for migration data (ID: {school_id})")

            if self._init_selenium():
                try:
                    self.driver.get(portrait_url)
                    time.sleep(1.5)
                    self.driver.get(migration_url)
                    time.sleep(2.0)

                    screenshot = self.driver.get_screenshot_as_png()
                    if screenshot:
                        self._save_screenshot(screenshot, schulnummer or school_id, "migration")
                        vision_data = self.vision_llm.extract_migration_data(screenshot, schulnummer)

                        if vision_data and vision_data.get('migration_pct') is not None:
                            data['migration_2024_25'] = float(vision_data['migration_pct'])
                            logger.info(f"  Migration % from vision: {data['migration_2024_25']}")

                except Exception as e:
                    logger.error(f"  Vision extraction failed for migration: {e}")

        # Fallback: HTML parsing
        if 'migration_2024_25' not in data:
            self.fetch_page(portrait_url)
            time.sleep(0.5)
            html = self.fetch_page(migration_url)

            if html:
                soup = BeautifulSoup(html, 'html.parser')
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        for cell in cells:
                            cell_text = cell.get_text(strip=True)
                            pct_match = re.search(r'(\d{1,3})[,.](\d)', cell_text)
                            if pct_match and ('Insg' in cell_text or '%' in cell_text):
                                pct = float(f"{pct_match.group(1)}.{pct_match.group(2)}")
                                if 0 < pct <= 100:
                                    data['migration_2024_25'] = pct
                                    break

        return data

    def scrape_personnel_data(self, school_id: str, schulnummer: str = "") -> Dict[str, Any]:
        """Scrape teacher/personnel data using vision-based extraction."""
        portrait_url = f"{BASE_URL}Schulportrait.aspx?IDSchulzweig={school_id}"
        personnel_url = f"{BASE_URL}schulpersonal.aspx"

        data = {}

        # Primary: Vision-based extraction
        if self.use_vision and self.vision_llm:
            logger.info(f"  Using vision extraction for personnel data (ID: {school_id})")

            if self._init_selenium():
                try:
                    self.driver.get(portrait_url)
                    time.sleep(1.5)
                    self.driver.get(personnel_url)
                    time.sleep(2.0)

                    screenshot = self.driver.get_screenshot_as_png()
                    if screenshot:
                        self._save_screenshot(screenshot, schulnummer or school_id, "personal")
                        vision_data = self.vision_llm.extract_personnel_data(screenshot, schulnummer)

                        if vision_data and vision_data.get('lehrer_total') is not None:
                            data['lehrer_2024_25'] = int(vision_data['lehrer_total'])
                            logger.info(f"  Teacher count from vision: {data['lehrer_2024_25']}")

                except Exception as e:
                    logger.error(f"  Vision extraction failed for personnel: {e}")

        # Fallback: HTML parsing
        if 'lehrer_2024_25' not in data:
            self.fetch_page(portrait_url)
            time.sleep(0.5)
            html = self.fetch_page(personnel_url)

            if html:
                soup = BeautifulSoup(html, 'html.parser')
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            label = cells[0].get_text(strip=True).lower()
                            if 'lehrkräfte' in label or 'lehrer' in label:
                                for cell in cells[1:]:
                                    cell_text = cell.get_text(strip=True)
                                    numbers = re.findall(r'\d+', cell_text)
                                    if numbers:
                                        count = int(numbers[0])
                                        if count > 0 and count < 1000:
                                            data['lehrer_2024_25'] = count
                                            break

        return data

    def scrape_languages(self, school_id: str) -> Optional[str]:
        """Scrape language offerings from the portrait page."""
        portrait_url = f"{BASE_URL}Schulportrait.aspx?IDSchulzweig={school_id}"
        html = self.fetch_page(portrait_url)

        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        languages = set()
        common_languages = {
            'Englisch': ['englisch', 'english'],
            'Französisch': ['französisch', 'french'],
            'Spanisch': ['spanisch', 'spanish'],
            'Latein': ['latein', 'latin'],
            'Russisch': ['russisch', 'russian'],
            'Italienisch': ['italienisch', 'italian'],
            'Japanisch': ['japanisch', 'japanese'],
            'Chinesisch': ['chinesisch', 'chinese'],
        }

        # Look for language section
        text = soup.get_text().lower()

        for lang_name, variants in common_languages.items():
            for variant in variants:
                if variant in text:
                    languages.add(lang_name)
                    break

        return ', '.join(sorted(languages)) if languages else None

    def scrape_school(self, schulnummer: str, schulname: Optional[str] = None) -> SchoolData:
        """Scrape all available data for a school."""
        school_data = SchoolData(
            schulnummer=schulnummer,
            scrape_timestamp=datetime.now().isoformat()
        )

        logger.info(f"Scraping school {schulnummer}" + (f" ({schulname})" if schulname else ""))

        # Step 1: Find all IDSchulzweig entries for this school
        school_ids = self.find_school_ids(schulnummer, schulname)

        if not school_ids:
            # Try direct URL approach as fallback
            logger.warning(f"No IDSchulzweig found for {schulnummer} in district list")
            school_data.scrape_error = "School not found in district list"
            return school_data

        logger.info(f"Found {len(school_ids)} entries for {schulnummer}")
        school_data.id_schulzweig_list = school_ids

        # Detect if this is an ISS-Gymnasium (has both types)
        all_types = set()
        for entry in school_ids:
            types = entry.get('type', '').split(', ')
            all_types.update(t for t in types if t and t != 'Unknown')

        school_data.detected_school_types = list(all_types)
        school_data.is_iss_gymnasium = 'ISS' in all_types and 'Gymnasium' in all_types

        if school_data.is_iss_gymnasium:
            logger.info(f"{schulnummer} detected as ISS-Gymnasium")

        # Step 2: Scrape data from school branches
        # Note: For schools with multiple IDSchulzweig (like ISS-Gymnasium), the portal often
        # shows the SAME school-wide totals on each branch page. So we only need to scrape
        # the first branch that returns data, not sum all branches.

        students_found = None
        teachers_found = None
        migration_found = None

        for entry in school_ids:
            school_id = entry['id']
            entry_name = entry.get('name', schulname or '')
            logger.info(f"Scraping IDSchulzweig {school_id}: {entry_name}")

            # Portrait data
            portrait_data = self.scrape_school_portrait(school_id)
            if 'schulname' in portrait_data and not school_data.schulname:
                school_data.schulname = portrait_data['schulname']

            # Student data - only scrape if we don't have it yet
            if students_found is None:
                student_data = self.scrape_student_data(school_id, schulnummer, entry_name)
                if 'schueler_2024_25' in student_data:
                    students_found = student_data['schueler_2024_25']
                    logger.info(f"  Got student count: {students_found}")

            # Personnel data - only scrape if we don't have it yet
            if teachers_found is None:
                personnel_data = self.scrape_personnel_data(school_id, schulnummer)
                if 'lehrer_2024_25' in personnel_data:
                    teachers_found = personnel_data['lehrer_2024_25']
                    logger.info(f"  Got teacher count: {teachers_found}")

            # Migration data - only scrape if we don't have it yet
            if migration_found is None:
                migration_data = self.scrape_migration_data(school_id, schulnummer)
                if 'migration_2024_25' in migration_data:
                    migration_found = migration_data['migration_2024_25']
                    logger.info(f"  Got migration %: {migration_found}")

            # Languages (only need from one branch)
            if not school_data.sprachen:
                languages = self.scrape_languages(school_id)
                if languages:
                    school_data.sprachen = languages

            # If we have all data, no need to scrape more branches
            if students_found is not None and teachers_found is not None and migration_found is not None:
                logger.info(f"  All data found, skipping remaining branches")
                break

            time.sleep(1)  # Rate limiting

        # Store data
        if students_found is not None:
            school_data.schueler_2024_25 = students_found

        if teachers_found is not None:
            school_data.lehrer_2024_25 = teachers_found

        if migration_found is not None:
            school_data.migration_2024_25 = migration_found

        return school_data


def merge_scraped_data(df: pd.DataFrame, school_data: SchoolData) -> pd.DataFrame:
    """Merge scraped data into the dataframe."""
    idx = df[df['schulnummer'] == school_data.schulnummer].index
    if len(idx) == 0:
        logger.warning(f"School {school_data.schulnummer} not found in dataframe")
        return df

    idx = idx[0]
    updated_fields = []

    # Fields to potentially update
    field_mapping = {
        'schueler_2024_25': 'schueler_2024_25',
        'lehrer_2024_25': 'lehrer_2024_25',
        'sprachen': 'sprachen',
        'migration_2024_25': 'migration_2024_25',
    }

    for attr, col in field_mapping.items():
        if col not in df.columns:
            continue

        new_value = getattr(school_data, attr, None)
        if new_value is None:
            continue

        current = df.at[idx, col]
        if pd.isna(current) or current == '' or current is None:
            df.at[idx, col] = new_value
            updated_fields.append(col)

    # Update school_type if ISS-Gymnasium detected
    if school_data.is_iss_gymnasium and 'school_type' in df.columns:
        current_type = df.at[idx, 'school_type']
        if current_type == 'ISS':
            df.at[idx, 'school_type'] = 'ISS-Gymnasium'
            updated_fields.append('school_type')
            logger.info(f"Updated {school_data.schulnummer} school_type: ISS -> ISS-Gymnasium")

    if updated_fields:
        logger.info(f"Updated {school_data.schulnummer}: {', '.join(updated_fields)}")

    return df


def get_schools_needing_data(df: pd.DataFrame, columns: List[str]) -> List[str]:
    """Get schools with missing data in specified columns."""
    existing_cols = [c for c in columns if c in df.columns]
    mask = df[existing_cols].isna().any(axis=1)
    return df[mask]['schulnummer'].tolist()


def main():
    parser = argparse.ArgumentParser(description='Scrape bildung.berlin.de V2 with Vision')
    parser.add_argument('--input', '-i', default='school_master_table_processed.csv')
    parser.add_argument('--output', '-o', default='school_master_table_enriched_v2.csv')
    parser.add_argument('--schulnummer', '-s', type=str, default=None)
    parser.add_argument('--missing-only', action='store_true')
    parser.add_argument('--limit', '-l', type=int, default=None)
    parser.add_argument('--delay', '-d', type=float, default=2.0)
    parser.add_argument('--no-vision', action='store_true', help='Disable vision-based extraction')

    args = parser.parse_args()

    config = load_config()
    logger.info(f"Loading data from {args.input}")
    df = pd.read_csv(args.input, encoding='utf-8-sig')

    # Check vision requirements
    use_vision = not args.no_vision
    if use_vision:
        if not SELENIUM_AVAILABLE:
            logger.warning("Selenium not available - vision disabled")
            use_vision = False
        elif not config.get('api_keys', {}).get('openai'):
            logger.warning("OpenAI API key not found - vision disabled")
            use_vision = False
        else:
            logger.info("Vision extraction enabled with gpt-5-mini-2025-08-07")

    # Determine schools to process
    key_columns = ['schueler_2024_25', 'lehrer_2024_25', 'sprachen', 'migration_2024_25']

    if args.schulnummer:
        schools = [args.schulnummer]
    elif args.missing_only:
        schools = get_schools_needing_data(df, key_columns)
    else:
        schools = df['schulnummer'].tolist()

    if args.limit:
        schools = schools[:args.limit]

    logger.info(f"Processing {len(schools)} schools")

    scraper = BildungBerlinScraperV2(config, use_vision=use_vision)

    stats = {'processed': 0, 'updated': 0, 'errors': 0, 'iss_gymnasium': 0}

    for i, schulnummer in enumerate(schools):
        # Get school name from dataframe for matching
        schulname = None
        row = df[df['schulnummer'] == schulnummer]
        if len(row) > 0:
            schulname = row.iloc[0].get('schulname')

        logger.info(f"Processing {i+1}/{len(schools)}: {schulnummer}")

        try:
            school_data = scraper.scrape_school(schulnummer, schulname)

            if school_data.scrape_error:
                stats['errors'] += 1
                logger.warning(f"Error for {schulnummer}: {school_data.scrape_error}")
            else:
                old_df = df.copy()
                df = merge_scraped_data(df, school_data)

                if not df.equals(old_df):
                    stats['updated'] += 1

                if school_data.is_iss_gymnasium:
                    stats['iss_gymnasium'] += 1

            stats['processed'] += 1

            # Save progress
            if (i + 1) % 5 == 0:
                df.to_csv(args.output.replace('.csv', '_partial.csv'),
                         index=False, encoding='utf-8-sig')

        except Exception as e:
            logger.error(f"Failed {schulnummer}: {e}")
            stats['errors'] += 1

        time.sleep(args.delay)

    # Cleanup Selenium
    scraper._close_selenium()

    # Save final
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    logger.info(f"Saved to {args.output}")

    print("\n" + "="*60)
    print("SCRAPING SUMMARY")
    print("="*60)
    print(f"Processed: {stats['processed']}")
    print(f"Updated: {stats['updated']}")
    print(f"Errors: {stats['errors']}")
    print(f"ISS-Gymnasium found: {stats['iss_gymnasium']}")
    print(f"Vision extraction: {'enabled' if use_vision else 'disabled'}")
    print("="*60)


if __name__ == "__main__":
    main()
