#!/usr/bin/env python3
"""
Bildung Berlin Portal Scraper - Simple Version (No Selenium Required)

This script scrapes the bildung.berlin.de school directory using direct HTTP requests.
It constructs URLs directly and parses the HTML responses.

The portal has predictable URL structure:
- School detail page: https://www.bildung.berlin.de/Schulverzeichnis/SchulSteckbrief.aspx?ID={schulnummer}
- Sub-sections use tab parameters

Usage:
    python3 scrape_bildung_berlin_simple.py --input school_master_table_processed.csv --output school_master_table_enriched.csv
    python3 scrape_bildung_berlin_simple.py --schulnummer 03B08  # Test single school
    python3 scrape_bildung_berlin_simple.py --missing-only --limit 10  # Process 10 schools with missing data
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
from urllib.parse import urljoin, urlencode, quote
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import yaml
from bs4 import BeautifulSoup

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
    """Use LLM with vision capabilities to analyze screenshots or HTML content."""

    def __init__(self, config: Dict):
        self.config = config
        api_keys = config.get('api_keys', {})
        models = config.get('models', {})

        self.api_key = api_keys.get('openai')
        self.model = models.get('openai_vision', 'gpt-4o-mini')
        self.base_url = "https://api.openai.com/v1/chat/completions"

        if not self.api_key:
            logger.warning("OpenAI API key not found - LLM analysis disabled")

    def analyze_html_content(self, html: str, section_name: str, schulnummer: str) -> Dict[str, Any]:
        """Analyze HTML content using LLM to extract structured data."""
        if not self.api_key:
            return {}

        # Clean HTML to reduce tokens
        soup = BeautifulSoup(html, 'html.parser')
        # Remove scripts, styles
        for tag in soup(['script', 'style', 'meta', 'link']):
            tag.decompose()

        # Get text content with some structure
        text_content = soup.get_text(separator='\n', strip=True)
        # Limit to reasonable size
        text_content = text_content[:8000]

        prompt = f"""Analyze this content from the Berlin school directory (bildung.berlin.de) for school {schulnummer}.
This is content from the "{section_name}" section/page.

Extract any of the following data you can find. Return ONLY a valid JSON object.
Use null for any fields not found.

Look for these fields:
{{
    "schulname": "full school name if visible",
    "schueler_2024_25": "number of students for 2024/25 school year (integer or null)",
    "lehrer_2024_25": "number of teachers/staff for 2024/25 (integer or null)",
    "schueler_2023_24": "students 2023/24 (integer or null)",
    "lehrer_2023_24": "teachers 2023/24 (integer or null)",
    "sprachen": "comma-separated list of foreign languages offered (string or null)",
    "abitur_durchschnitt_2024": "average Abitur grade 2024 (float like 2.3 or null)",
    "abitur_durchschnitt_2023": "average Abitur grade 2023 (float or null)",
    "abitur_erfolgsquote_2024": "Abitur success/pass rate 2024 in percent (float or null)",
    "migration_2024_25": "percentage of students with migration background 2024/25 (float or null)",
    "migration_2023_24": "percentage with migration background 2023/24 (float or null)",
    "nachfrage_plaetze_2025_26": "available school places for 2025/26 (integer or null)",
    "nachfrage_wuensche_2025_26": "number of applications/first choices for 2025/26 (integer or null)",
    "besonderheiten": "special features, programs, focus areas (string or null)",
    "detected_school_types": ["list of school types found, e.g. 'ISS', 'Gymnasium', 'Gemeinschaftsschule'"]
}}

Content to analyze:
{text_content}

Return ONLY the JSON object, no explanations. Use null for missing data, not empty strings."""

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 1500,
            "temperature": 0.1
        }

        try:
            response = requests.post(self.base_url, headers=headers, json=data, timeout=60)
            response.raise_for_status()
            result = response.json()
            content = result["choices"][0]["message"]["content"]

            # Parse JSON from response
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                parsed = json.loads(json_match.group())
                logger.info(f"LLM extracted data for {schulnummer}: {list(parsed.keys())}")
                return parsed
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")

        return {}


class BildungBerlinScraper:
    """Scraper for bildung.berlin.de school directory using HTTP requests."""

    def __init__(self, config: Dict, use_llm: bool = True):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

        self.use_llm = use_llm
        self.vision_llm = VisionLLM(config) if use_llm else None

    def get_school_page_url(self, schulnummer: str) -> str:
        """Construct the URL for a school's detail page."""
        # The portal uses BSN (Berlin School Number) as ID
        # URL format: SchulSteckbrief.aspx?ID=03B08
        return f"{BASE_URL}SchulSteckbrief.aspx?ID={schulnummer}"

    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page and return its HTML content."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def parse_student_teacher_data(self, html: str) -> Dict[str, Any]:
        """Parse student and teacher counts from HTML."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Patterns for student counts
        # German format: "Schülerinnen und Schüler: 842" or "Schülerzahl: 842"
        student_patterns = [
            r'Schüler(?:innen)?\s*(?:und\s*Schüler)?(?:zahl)?[:\s]+(\d+)',
            r'(\d+)\s*Schüler(?:innen)?',
            r'Anzahl\s*(?:der\s*)?Schüler[:\s]+(\d+)',
        ]

        for pattern in student_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['schueler_2024_25'] = int(match.group(1))
                break

        # Patterns for teacher counts
        teacher_patterns = [
            r'Lehrer(?:innen)?\s*(?:und\s*Lehrer)?[:\s]+(\d+)',
            r'Lehrkräfte[:\s]+(\d+)',
            r'(\d+)\s*Lehrkräfte',
            r'Anzahl\s*(?:der\s*)?Lehrer[:\s]+(\d+)',
            r'Pädagogisches\s*Personal[:\s]+(\d+)',
        ]

        for pattern in teacher_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['lehrer_2024_25'] = int(match.group(1))
                break

        # Look in tables for year-specific data
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True).lower()

                    # Find values in subsequent cells
                    for i, cell in enumerate(cells[1:], 1):
                        value_text = cell.get_text(strip=True)
                        numbers = re.findall(r'\d+', value_text)
                        if not numbers:
                            continue
                        num = int(numbers[0])

                        if 'schüler' in header:
                            # Check table headers for year info
                            if i < len(cells) and ('2024' in cells[i].get_text() or '24/25' in cells[i].get_text()):
                                data['schueler_2024_25'] = num
                            elif i < len(cells) and ('2023' in cells[i].get_text() or '23/24' in cells[i].get_text()):
                                data['schueler_2023_24'] = num

        return data

    def parse_languages(self, html: str) -> Optional[str]:
        """Parse languages offered from HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        languages = set()
        common_languages = {
            'Englisch': ['englisch', 'english'],
            'Französisch': ['französisch', 'french', 'francais'],
            'Spanisch': ['spanisch', 'spanish', 'español'],
            'Latein': ['latein', 'latin'],
            'Russisch': ['russisch', 'russian'],
            'Italienisch': ['italienisch', 'italian'],
            'Japanisch': ['japanisch', 'japanese'],
            'Chinesisch': ['chinesisch', 'chinese', 'mandarin'],
            'Polnisch': ['polnisch', 'polish'],
            'Türkisch': ['türkisch', 'turkish'],
            'Arabisch': ['arabisch', 'arabic'],
            'Griechisch': ['griechisch', 'greek'],
            'Portugiesisch': ['portugiesisch', 'portuguese'],
            'Hebräisch': ['hebräisch', 'hebrew'],
        }

        text = soup.get_text().lower()

        for lang_name, variants in common_languages.items():
            for variant in variants:
                if variant in text:
                    languages.add(lang_name)
                    break

        # Look for language sections
        for section_text in ['fremdsprache', 'sprachen', 'sprachunterricht']:
            section = soup.find(string=lambda t: t and section_text in t.lower() if t else False)
            if section:
                # Get parent and look for list items
                parent = section.find_parent()
                if parent:
                    next_list = parent.find_next(['ul', 'ol'])
                    if next_list:
                        for item in next_list.find_all('li'):
                            item_text = item.get_text().lower()
                            for lang_name, variants in common_languages.items():
                                for variant in variants:
                                    if variant in item_text:
                                        languages.add(lang_name)
                                        break

        if languages:
            return ', '.join(sorted(languages))
        return None

    def parse_abitur_data(self, html: str) -> Dict[str, Any]:
        """Parse Abitur results from HTML."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Average grade patterns
        avg_patterns = [
            r'Durchschnitt(?:snote)?[:\s]*(\d[,\.]\d)',
            r'Abiturdurchschnitt[:\s]*(\d[,\.]\d)',
            r'Notendurchschnitt[:\s]*(\d[,\.]\d)',
            r'(\d[,\.]\d)\s*(?:Durchschnitt|Notenschnitt)',
        ]

        for pattern in avg_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                grade = float(match.replace(',', '.'))
                if 1.0 <= grade <= 4.0:
                    # Try to determine year from context
                    context_start = max(0, text.find(match) - 100)
                    context = text[context_start:text.find(match) + 50]

                    if '2024' in context:
                        data['abitur_durchschnitt_2024'] = grade
                    elif '2023' in context:
                        data['abitur_durchschnitt_2023'] = grade
                    elif '2025' in context:
                        data['abitur_durchschnitt_2025'] = grade
                    elif 'abitur_durchschnitt_2024' not in data:
                        # Default to current year if no year specified
                        data['abitur_durchschnitt_2024'] = grade

        # Success rate patterns
        success_patterns = [
            r'Erfolgsquote[:\s]*(\d{1,3})[,\.]?(\d?)\s*%',
            r'Bestehensquote[:\s]*(\d{1,3})[,\.]?(\d?)\s*%',
            r'(\d{1,3})[,\.]?(\d?)\s*%\s*(?:bestanden|erfolgreich)',
        ]

        for pattern in success_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                main, decimal = matches[0]
                rate = float(f"{main}.{decimal or '0'}")
                if 0 <= rate <= 100:
                    data['abitur_erfolgsquote_2024'] = rate
                    break

        return data

    def parse_migration_data(self, html: str) -> Dict[str, Any]:
        """Parse migration background statistics."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        patterns = [
            r'(?:Migrationshintergrund|ndH|nichtdeutscher\s+Herkunft(?:ssprache)?)[:\s]*(\d{1,3})[,\.]?(\d?)\s*%',
            r'(\d{1,3})[,\.]?(\d?)\s*%\s*(?:Migrationshintergrund|ndH)',
            r'ndH[:\s]*(\d{1,3})[,\.]?(\d?)\s*%',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                main, decimal = matches[0]
                rate = float(f"{main}.{decimal or '0'}")
                if 0 <= rate <= 100:
                    # Default to most recent year
                    data['migration_2024_25'] = rate
                    break

        return data

    def parse_besonderheiten(self, html: str) -> Optional[str]:
        """Parse special features and programs."""
        soup = BeautifulSoup(html, 'html.parser')

        keywords = ['Besonderheiten', 'Schulprofil', 'Profil', 'Schwerpunkt',
                   'Angebote', 'Programme', 'Ganztag', 'Förderung']

        found_items = []

        for keyword in keywords:
            # Find sections with these keywords
            elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'strong', 'b'],
                                    string=lambda x: x and keyword.lower() in x.lower() if x else False)

            for elem in elements:
                # Get subsequent content
                next_elem = elem.find_next(['ul', 'ol', 'p'])
                if next_elem:
                    if next_elem.name in ['ul', 'ol']:
                        items = [li.get_text(strip=True) for li in next_elem.find_all('li')]
                        found_items.extend(items)
                    else:
                        text = next_elem.get_text(strip=True)
                        if len(text) > 10:
                            found_items.append(text)

        if found_items:
            # Deduplicate and join
            unique_items = list(dict.fromkeys(found_items))[:10]  # Max 10 items
            return '; '.join(unique_items)[:500]  # Limit total length

        return None

    def parse_demand_data(self, html: str) -> Dict[str, Any]:
        """Parse school demand/application statistics."""
        data = {}
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Places/capacity patterns
        places_patterns = [
            r'(?:Plätze|Kapazität|Aufnahmekapazität)[:\s]*(\d+)',
            r'(\d+)\s*(?:Plätze|Schulplätze)',
        ]

        for pattern in places_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                places = int(match.group(1))
                # Determine year from context
                context = text[max(0, match.start()-100):match.end()+100]
                if '2025' in context or '25/26' in context:
                    data['nachfrage_plaetze_2025_26'] = places
                else:
                    data['nachfrage_plaetze_2024_25'] = places
                break

        # Applications/wishes patterns
        wishes_patterns = [
            r'(?:Anmeldungen|Erstwünsche|Wünsche|Anmeldezahlen)[:\s]*(\d+)',
            r'(\d+)\s*(?:Anmeldungen|Erstwünsche)',
        ]

        for pattern in wishes_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                wishes = int(match.group(1))
                context = text[max(0, match.start()-100):match.end()+100]
                if '2025' in context or '25/26' in context:
                    data['nachfrage_wuensche_2025_26'] = wishes
                else:
                    data['nachfrage_wuensche_2024_25'] = wishes
                break

        # Calculate percentage
        if 'nachfrage_plaetze_2025_26' in data and 'nachfrage_wuensche_2025_26' in data:
            if data['nachfrage_plaetze_2025_26'] > 0:
                data['nachfrage_prozent_2025_26'] = round(
                    100 * data['nachfrage_wuensche_2025_26'] / data['nachfrage_plaetze_2025_26'], 1
                )

        return data

    def detect_school_types(self, html: str, schulnummer: str) -> Tuple[List[str], bool]:
        """
        Detect school types from the page content.
        Returns (list of types, is_iss_gymnasium boolean)
        """
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text().lower()

        types = []

        # Check for ISS
        if any(term in text for term in ['integrierte sekundarschule', 'iss', 'sekundarschule']):
            if 'iss' not in types:
                types.append('ISS')

        # Check for Gymnasium
        if 'gymnasium' in text:
            if 'Gymnasium' not in types:
                types.append('Gymnasium')

        # Check for Gemeinschaftsschule
        if 'gemeinschaftsschule' in text:
            types.append('Gemeinschaftsschule')

        # Special case: if schulnummer contains both markers
        # Some schools appear multiple times in the directory with different types
        # but the same schulnummer

        is_iss_gymnasium = 'ISS' in types and 'Gymnasium' in types

        return types, is_iss_gymnasium

    def scrape_school(self, schulnummer: str) -> SchoolData:
        """Scrape all available data for a school."""
        school_data = SchoolData(
            schulnummer=schulnummer,
            scrape_timestamp=datetime.now().isoformat()
        )

        logger.info(f"Scraping school {schulnummer}")

        # Fetch main school page
        url = self.get_school_page_url(schulnummer)
        html = self.fetch_page(url)

        if not html:
            school_data.scrape_error = "Could not fetch school page"
            return school_data

        # Save HTML for debugging
        os.makedirs('scraped_html', exist_ok=True)
        with open(f'scraped_html/{schulnummer}.html', 'w', encoding='utf-8') as f:
            f.write(html)

        # Detect school types
        types, is_iss_gy = self.detect_school_types(html, schulnummer)
        school_data.detected_school_types = types
        school_data.is_iss_gymnasium = is_iss_gy

        # Parse data using rule-based extraction
        student_data = self.parse_student_teacher_data(html)
        for key, value in student_data.items():
            setattr(school_data, key, value)

        languages = self.parse_languages(html)
        if languages:
            school_data.sprachen = languages

        abitur_data = self.parse_abitur_data(html)
        for key, value in abitur_data.items():
            setattr(school_data, key, value)

        migration_data = self.parse_migration_data(html)
        for key, value in migration_data.items():
            setattr(school_data, key, value)

        demand_data = self.parse_demand_data(html)
        for key, value in demand_data.items():
            setattr(school_data, key, value)

        besonderheiten = self.parse_besonderheiten(html)
        if besonderheiten:
            school_data.besonderheiten = besonderheiten

        # If we didn't get much data, use LLM for analysis
        fields_found = sum(1 for f in [
            school_data.schueler_2024_25, school_data.lehrer_2024_25,
            school_data.sprachen, school_data.abitur_durchschnitt_2024,
            school_data.besonderheiten
        ] if f is not None)

        if fields_found < 2 and self.use_llm and self.vision_llm:
            logger.info(f"Using LLM to analyze {schulnummer} (only {fields_found} fields found)")
            llm_data = self.vision_llm.analyze_html_content(html, "main page", schulnummer)

            # Apply LLM-extracted data for missing fields only
            llm_field_mapping = {
                'schueler_2024_25': 'schueler_2024_25',
                'lehrer_2024_25': 'lehrer_2024_25',
                'schueler_2023_24': 'schueler_2023_24',
                'lehrer_2023_24': 'lehrer_2023_24',
                'sprachen': 'sprachen',
                'abitur_durchschnitt_2024': 'abitur_durchschnitt_2024',
                'abitur_durchschnitt_2023': 'abitur_durchschnitt_2023',
                'abitur_erfolgsquote_2024': 'abitur_erfolgsquote_2024',
                'migration_2024_25': 'migration_2024_25',
                'migration_2023_24': 'migration_2023_24',
                'besonderheiten': 'besonderheiten',
                'nachfrage_plaetze_2025_26': 'nachfrage_plaetze_2025_26',
                'nachfrage_wuensche_2025_26': 'nachfrage_wuensche_2025_26',
            }

            for llm_key, attr_name in llm_field_mapping.items():
                if llm_key in llm_data and llm_data[llm_key] is not None:
                    current_value = getattr(school_data, attr_name, None)
                    if current_value is None:
                        setattr(school_data, attr_name, llm_data[llm_key])

            # Update school types from LLM
            if 'detected_school_types' in llm_data and llm_data['detected_school_types']:
                llm_types = llm_data['detected_school_types']
                for t in llm_types:
                    if t not in school_data.detected_school_types:
                        school_data.detected_school_types.append(t)
                school_data.is_iss_gymnasium = (
                    'ISS' in school_data.detected_school_types and
                    'Gymnasium' in school_data.detected_school_types
                )

        return school_data


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

    updated_fields = []
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
                updated_fields.append(field)

    if updated_fields:
        logger.info(f"Updated {school_data.schulnummer}: {', '.join(updated_fields)}")

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
        elif school_data.scrape_source not in str(current_source):
            df.at[idx, 'metadata_source'] = f"{current_source}, {school_data.scrape_source}"

    return df


def get_schools_with_missing_data(df: pd.DataFrame, key_columns: List[str], min_missing: int = 2) -> List[str]:
    """Get list of schulnummers for schools with missing data in key columns."""

    existing_cols = [c for c in key_columns if c in df.columns]
    missing_counts = df[existing_cols].isna().sum(axis=1)
    missing_mask = missing_counts >= min_missing
    missing_schools = df[missing_mask]['schulnummer'].tolist()

    logger.info(f"Found {len(missing_schools)} schools with {min_missing}+ missing key columns")
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
    parser.add_argument('--min-missing', type=int, default=2,
                       help='Minimum number of missing columns to trigger processing')
    parser.add_argument('--limit', '-l', type=int, default=None,
                       help='Limit number of schools to process')
    parser.add_argument('--delay', '-d', type=float, default=1.5,
                       help='Delay between schools (seconds)')
    parser.add_argument('--no-llm', action='store_true',
                       help='Disable LLM-based extraction')

    args = parser.parse_args()

    # Load config and data
    config = load_config()
    logger.info(f"Loading data from {args.input}")
    df = pd.read_csv(args.input, encoding='utf-8-sig')

    # Create directories
    os.makedirs('scraped_html', exist_ok=True)

    # Determine which schools to process
    key_columns = [
        'schueler_2024_25', 'lehrer_2024_25', 'sprachen',
        'abitur_durchschnitt_2024', 'migration_2024_25', 'besonderheiten'
    ]

    if args.schulnummer:
        schools_to_process = [args.schulnummer]
    elif args.missing_only:
        schools_to_process = get_schools_with_missing_data(df, key_columns, args.min_missing)
    else:
        schools_to_process = df['schulnummer'].tolist()

    if args.limit:
        schools_to_process = schools_to_process[:args.limit]

    logger.info(f"Processing {len(schools_to_process)} schools")

    # Initialize scraper
    scraper = BildungBerlinScraper(config, use_llm=not args.no_llm)

    # Track statistics
    stats = {
        'processed': 0,
        'updated': 0,
        'errors': 0,
        'iss_gymnasium_found': 0,
    }

    try:
        for i, schulnummer in enumerate(schools_to_process):
            logger.info(f"Processing {i+1}/{len(schools_to_process)}: {schulnummer}")

            try:
                school_data = scraper.scrape_school(schulnummer)

                if school_data.scrape_error:
                    stats['errors'] += 1
                else:
                    # Check if we got any new data
                    old_df = df.copy()
                    df = merge_scraped_data(df, school_data)

                    if not df.equals(old_df):
                        stats['updated'] += 1

                    if school_data.is_iss_gymnasium:
                        stats['iss_gymnasium_found'] += 1

                stats['processed'] += 1

                # Save progress periodically
                if (i + 1) % 10 == 0:
                    partial_output = args.output.replace('.csv', '_partial.csv')
                    df.to_csv(partial_output, index=False, encoding='utf-8-sig')
                    logger.info(f"Saved partial progress ({i+1} schools)")

            except Exception as e:
                logger.error(f"Failed to process {schulnummer}: {e}")
                stats['errors'] += 1

            time.sleep(args.delay)

    except KeyboardInterrupt:
        logger.info("Interrupted by user - saving progress...")

    # Save final results
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    try:
        df.to_excel(args.output.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    except Exception as e:
        logger.warning(f"Could not save Excel file: {e}")

    logger.info(f"Saved enriched data to {args.output}")

    # Print summary
    print("\n" + "="*70)
    print("SCRAPING SUMMARY")
    print("="*70)
    print(f"Schools processed: {stats['processed']}")
    print(f"Schools updated: {stats['updated']}")
    print(f"Errors: {stats['errors']}")
    print(f"ISS-Gymnasium schools found: {stats['iss_gymnasium_found']}")

    # Show improvement in data completeness
    print("\nData completeness after scraping:")
    for col in key_columns:
        if col in df.columns:
            missing = df[col].isna().sum()
            print(f"  {col}: {missing} missing ({100*missing/len(df):.1f}%)")

    # Show ISS-Gymnasium updates
    iss_gy_count = (df['school_type'] == 'ISS-Gymnasium').sum()
    print(f"\nTotal ISS-Gymnasium schools: {iss_gy_count}")

    print("="*70)


if __name__ == "__main__":
    main()
