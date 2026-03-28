#!/usr/bin/env python3
"""
Generate Rich School Descriptions Using LLM with Web Scraping and PDF Extraction (V4)

Improvements over v3:
1. Much more thorough site crawling - follows links deeper into the site
2. Expanded multilingual keyword lists (English + German) for fee-related pages
3. Searches entire site for PDF links, not just specific pages
4. Better handling of international school sites with /en/ paths
5. Recursive link following to find fee pages that are nested deeper

This script:
1. Thoroughly crawls school websites looking for fee/tuition information
2. Downloads and parses PDFs to extract tuition information
3. Uses LLM to analyze all found content
"""

import os
import io
import json
import time
import logging
import re
import requests
import yaml
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import urljoin, urlparse
from collections import deque
import pandas as pd
from bs4 import BeautifulSoup

# Try to import PDF libraries
try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('school_descriptions_v4.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"
PDF_CACHE_DIR = Path(__file__).parent / "pdf_cache"

# Expanded multilingual keywords for finding fee-related content
FEE_KEYWORDS_EN = [
    'fee', 'fees', 'tuition', 'pricing', 'price', 'prices', 'cost', 'costs',
    'payment', 'payments', 'financial', 'finance', 'finances', 'funding',
    'scholarship', 'scholarships', 'bursary', 'bursaries', 'aid',
    'school-fees', 'tuition-fees', 'annual-fees', 'monthly-fees',
    'admission-fee', 'registration-fee', 'enrollment-fee',
]

FEE_KEYWORDS_DE = [
    'gebühr', 'gebühren', 'gebuehr', 'gebuehren',
    'schulgeld', 'schulbeitrag', 'schulbeiträge', 'schulbeitraege',
    'kosten', 'preise', 'preis', 'beitrag', 'beiträge', 'beitraege',
    'beitragsordnung', 'gebührenordnung', 'gebuehrenordnung',
    'finanzen', 'finanzierung', 'finanzielle', 'foerderung', 'förderung',
    'stipendium', 'stipendien', 'ermäßigung', 'ermaessigung',
    'monatsbeitrag', 'jahresbeitrag', 'aufnahmegebühr', 'aufnahmegebuehr',
    'anmeldegebühr', 'anmeldegebuehr', 'einschreibegebühr', 'einschreibegebuehr',
    'essensgeld', 'verpflegung', 'mittagessen', 'hort', 'hortgebühr',
    'betreuung', 'nachmittagsbetreuung', 'ganztagsbetreuung',
    'einkommensabhängig', 'einkommensabhaengig', 'sozial gestaffelt',
    'materialgeld', 'lehrmittel', 'büchergeld', 'buechergeld',
    'vertrag', 'schulvertrag', 'contract',
]

ALL_FEE_KEYWORDS = FEE_KEYWORDS_EN + FEE_KEYWORDS_DE

# Keywords for other relevant pages
ADMISSIONS_KEYWORDS = [
    'admission', 'admissions', 'apply', 'application', 'enroll', 'enrollment', 'enrolment',
    'aufnahme', 'anmeldung', 'bewerbung', 'einschreibung', 'anmelden',
    'join-us', 'join us', 'how-to-apply', 'wie-anmelden',
]

ABOUT_KEYWORDS = [
    'about', 'about-us', 'über', 'ueber', 'profil', 'profile', 'leitbild',
    'mission', 'vision', 'philosophy', 'geschichte', 'history',
    'schule', 'school', 'wir', 'our-school', 'unsere-schule',
]

CURRICULUM_KEYWORDS = [
    'curriculum', 'program', 'programme', 'programm', 'academic', 'academics',
    'unterricht', 'education', 'fächer', 'faecher', 'subjects',
    'primary', 'secondary', 'gymnasium', 'sekundarstufe', 'grundschule',
    'abitur', 'ib', 'igcse', 'msa', 'oberstufe',
]


def load_config(config_path: Path = CONFIG_PATH) -> Dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


try:
    CONFIG = load_config()
except FileNotFoundError as e:
    logger.warning(f"Config not loaded: {e}")
    CONFIG = {}


class PDFExtractor:
    """Download and extract text from PDFs."""

    def __init__(self, cache_dir: Path = PDF_CACHE_DIR, timeout: int = 30):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        })

    def _get_cache_path(self, url: str) -> Path:
        """Get cache file path for a PDF URL."""
        safe_name = re.sub(r'[^\w\-.]', '_', urlparse(url).path.split('/')[-1])
        if not safe_name.endswith('.pdf'):
            safe_name += '.pdf'
        url_hash = str(hash(url) % 10**8)
        return self.cache_dir / f"{url_hash}_{safe_name}"

    def download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file, using cache if available."""
        cache_path = self._get_cache_path(url)

        if cache_path.exists():
            logger.debug(f"Using cached PDF: {cache_path}")
            return cache_path.read_bytes()

        try:
            logger.info(f"Downloading PDF: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            content_type = response.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not response.content[:4] == b'%PDF':
                logger.warning(f"Not a PDF: {url} (content-type: {content_type})")
                return None

            cache_path.write_bytes(response.content)
            logger.info(f"Cached PDF to: {cache_path}")

            return response.content

        except Exception as e:
            logger.warning(f"Error downloading PDF {url}: {e}")
            return None

    def extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF content."""
        text = ""

        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n\n"
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                if row:
                                    text += " | ".join([str(cell) if cell else "" for cell in row]) + "\n"
                            text += "\n"
                if text.strip():
                    return text[:25000]
            except Exception as e:
                logger.warning(f"pdfplumber extraction failed: {e}")

        if HAS_PYPDF2 and not text.strip():
            try:
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
                if text.strip():
                    return text[:25000]
            except Exception as e:
                logger.warning(f"PyPDF2 extraction failed: {e}")

        return text.strip()

    def extract_text_from_url(self, url: str) -> Optional[str]:
        """Download PDF and extract text."""
        pdf_content = self.download_pdf(url)
        if pdf_content:
            return self.extract_text_from_pdf(pdf_content)
        return None


class ThoroughWebScraper:
    """Thoroughly scrape school websites to find all fee-related content."""

    def __init__(self, timeout: int = 30, max_pages: int = 30, max_depth: int = 3):
        self.timeout = timeout
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
        })

    def _normalize_url(self, url: str, base_url: str) -> Optional[str]:
        """Normalize and validate a URL."""
        if not url or url.startswith('#') or url.startswith('javascript:') or url.startswith('mailto:'):
            return None

        full_url = urljoin(base_url, url)

        # Only follow links on the same domain
        base_domain = urlparse(base_url).netloc
        url_domain = urlparse(full_url).netloc

        if url_domain != base_domain:
            return None

        # Remove fragments
        parsed = urlparse(full_url)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"

        return clean_url

    def _matches_keywords(self, text: str, href: str, keywords: List[str]) -> bool:
        """Check if text or href matches any keywords."""
        text_lower = text.lower()
        href_lower = href.lower()
        return any(kw in text_lower or kw in href_lower for kw in keywords)

    def _categorize_link(self, text: str, href: str) -> Optional[str]:
        """Categorize a link based on keywords."""
        if self._matches_keywords(text, href, ALL_FEE_KEYWORDS):
            return 'fees'
        if self._matches_keywords(text, href, ADMISSIONS_KEYWORDS):
            return 'admissions'
        if self._matches_keywords(text, href, ABOUT_KEYWORDS):
            return 'about'
        if self._matches_keywords(text, href, CURRICULUM_KEYWORDS):
            return 'curriculum'
        return None

    def fetch_page(self, url: str) -> Tuple[str, str, List[Dict]]:
        """
        Fetch a page and return (text_content, html, all_links).
        all_links is a list of dicts with 'url', 'text', 'category'.
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract all links
            all_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                normalized = self._normalize_url(href, url)

                if normalized:
                    category = self._categorize_link(link_text, href)
                    all_links.append({
                        'url': normalized,
                        'text': link_text,
                        'category': category,
                        'is_pdf': '.pdf' in href.lower()
                    })

            # Remove script and style elements for text extraction
            for element in soup(['script', 'style', 'nav', 'footer']):
                element.decompose()

            text = soup.get_text(separator='\n', strip=True)
            text = re.sub(r'\n{3,}', '\n\n', text)

            return text[:15000], response.text, all_links

        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return "", "", []

    def _is_fee_related_pdf(self, pdf_info: Dict) -> bool:
        """Check if a PDF is likely fee-related."""
        text = pdf_info.get('text', '').lower()
        url = pdf_info.get('url', '').lower()
        return any(kw in text or kw in url for kw in ALL_FEE_KEYWORDS)

    def crawl_for_fees(self, start_url: str) -> Dict[str, Any]:
        """
        Crawl a website thoroughly looking for fee/tuition information.

        Returns dict with:
        - pages: dict of category -> content
        - all_pdfs: list of all PDF links found
        - fee_pdfs: list of fee-related PDFs
        - fee_pages_urls: list of URLs identified as fee pages
        """
        if not start_url:
            return {'pages': {}, 'all_pdfs': [], 'fee_pdfs': [], 'fee_pages_urls': []}

        if not start_url.startswith('http'):
            start_url = 'https://' + start_url

        visited: Set[str] = set()
        to_visit: deque = deque()  # (url, depth, priority)

        pages = {
            'main': '',
            'fees': '',
            'admissions': '',
            'about': '',
            'curriculum': '',
        }

        all_pdfs: List[Dict] = []
        fee_pages_urls: List[str] = []
        fee_page_contents: List[str] = []

        # Start with main page
        to_visit.append((start_url, 0, 'main'))

        pages_crawled = 0

        while to_visit and pages_crawled < self.max_pages:
            url, depth, priority = to_visit.popleft()

            if url in visited:
                continue

            visited.add(url)
            pages_crawled += 1

            logger.debug(f"Crawling [{pages_crawled}/{self.max_pages}] depth={depth}: {url}")

            text, html, links = self.fetch_page(url)

            if not text:
                continue

            # Store page content by priority category
            if priority == 'main' and not pages['main']:
                pages['main'] = text
            elif priority == 'fees':
                fee_pages_urls.append(url)
                fee_page_contents.append(f"--- Content from {url} ---\n{text}")
            elif priority in pages and not pages[priority]:
                pages[priority] = text

            # Collect PDFs
            page_pdfs = [l for l in links if l.get('is_pdf')]
            for pdf in page_pdfs:
                pdf['found_on'] = url
                if pdf not in all_pdfs:
                    all_pdfs.append(pdf)

            # Add new links to crawl queue (if not at max depth)
            if depth < self.max_depth:
                for link in links:
                    if link['url'] not in visited:
                        link_category = link.get('category')

                        # Prioritize fee-related links
                        if link_category == 'fees':
                            to_visit.appendleft((link['url'], depth + 1, 'fees'))
                        elif link_category in ['admissions', 'about', 'curriculum']:
                            to_visit.append((link['url'], depth + 1, link_category))
                        elif depth < 2:
                            # Only follow uncategorized links at shallow depth
                            to_visit.append((link['url'], depth + 1, 'other'))

            time.sleep(0.3)  # Be polite

        # Combine fee page contents
        if fee_page_contents:
            pages['fees'] = '\n\n'.join(fee_page_contents)[:20000]

        # Identify fee-related PDFs
        fee_pdfs = [p for p in all_pdfs if self._is_fee_related_pdf(p)]

        logger.info(f"Crawled {pages_crawled} pages, found {len(all_pdfs)} PDFs ({len(fee_pdfs)} fee-related)")

        return {
            'pages': pages,
            'all_pdfs': all_pdfs,
            'fee_pdfs': fee_pdfs,
            'fee_pages_urls': fee_pages_urls,
            'pages_crawled': pages_crawled
        }


class LLMAnalyzer:
    """Use LLM to analyze scraped content."""

    def __init__(self, config: Dict):
        self.config = config
        api_keys = config.get('api_keys', {})
        models = config.get('models', {})

        self.api_key = api_keys.get('perplexity')
        self.model = models.get('perplexity', 'sonar')
        self.base_url = "https://api.perplexity.ai/chat/completions"

        if not self.api_key:
            self.api_key = api_keys.get('openai')
            self.model = models.get('openai', 'gpt-4o-mini')
            self.base_url = "https://api.openai.com/v1/chat/completions"

    def analyze(self, prompt: str) -> str:
        """Send prompt to LLM and get response."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You are an expert education researcher analyzing school information. Be factual and cite the content provided. Extract ALL fee/tuition information with specific EUR amounts where available. Always include the JSON block at the end."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 6000,
            "temperature": 0.2
        }

        response = requests.post(self.base_url, headers=headers, json=data, timeout=120)
        response.raise_for_status()

        result = response.json()
        return result["choices"][0]["message"]["content"]


# Improved prompt with better fee extraction instructions
PRIVATE_SCHOOL_PROMPT_V4 = """Analyze the following PRIVATE school website content and create a comprehensive school profile.

## School Information:
- Name: {school_name}
- Website: {website}
- Type: {school_type}
- District (Bezirk): {bezirk}
- Address: {address}
- Ownership: {traegerschaft} (PRIVATE SCHOOL)

## Scraped Website Content:

### Main Page:
{main_content}

### Fee/Tuition Pages Found ({num_fee_pages} pages):
{fees_content}

### Admissions Page:
{admissions_content}

### About Page:
{about_content}

### Curriculum Page:
{curriculum_content}

### PDF Documents Found:
{pdf_links}

### PDF Content Extracted:
{pdf_content}

---

## IMPORTANT INSTRUCTIONS FOR FEE EXTRACTION:

Look very carefully for tuition/fee amounts. They may be expressed in various ways:
- Monthly (Monatsbeitrag, monatlich, pro Monat, per month)
- Annual (Jahresbeitrag, jährlich, pro Jahr, per year, per annum)
- In tables with different categories or income brackets
- As ranges (e.g., "200-500 EUR depending on income")
- For different school levels (Grundschule, Sekundarstufe, Oberstufe)

Common German fee terms:
- Schulgeld = school fees/tuition
- Beitrag = contribution/fee
- Gebühr = fee/charge
- Aufnahmegebühr = registration/enrollment fee
- Essensgeld/Mittagessen = meal plan
- Hort/Nachmittagsbetreuung = after-school care
- Materialgeld = materials fee
- Einkommensabhängig = income-based

If fees vary by income, extract the BASE or STANDARD fee (not the reduced rate).
If fees vary by school level, extract the SECONDARY school (Sekundarstufe/Oberstufe) fees.

---

Based on the above content, create a structured profile with these sections:

### 1. MISSION AND EDUCATIONAL PHILOSOPHY
Extract mission statement, values, educational approach from the content.

### 2. HISTORY, COMMUNITY AND FACILITIES
Extract founding info, location details, facilities mentioned.

### 3. CURRICULUM AND ACADEMIC PROGRAMS
Extract grades offered, languages, special programs, graduation pathways (Abitur, MSA, BBR).

### 4. STUDENT LEARNING EXPERIENCES
Extract extracurriculars, sports, arts, exchanges mentioned.

### 5. SCHOOL ACHIEVEMENTS AND OUTCOMES
Extract any awards, rankings, certifications mentioned.

### 6. ADMISSIONS AND FEES
Extract enrollment process, requirements, and ALL fee information including specific EUR amounts.

At the end, include this JSON block with extracted tuition data:

```json
{{
    "is_private_school": true,
    "tuition_monthly_eur": <number or null - the standard monthly tuition in EUR>,
    "tuition_annual_eur": <number or null - the standard annual tuition in EUR>,
    "registration_fee_eur": <number or null>,
    "material_fee_annual_eur": <number or null>,
    "meal_plan_monthly_eur": <number or null>,
    "after_school_care_monthly_eur": <number or null>,
    "scholarship_available": <true/false/null>,
    "income_based_tuition": <true/false/null>,
    "tuition_notes": "<detailed summary of fee structure including any ranges or variations>",
    "tuition_source_url": "<URL where fees were found or PDF name>"
}}
```
"""


TUITION_ONLY_PROMPT_V4 = """Analyze the following content from a private school to extract tuition/fee information.

## School Information:
- Name: {school_name}
- Website: {website}

## Fee Pages Content:
{fee_pages_content}

## PDF Content:
{pdf_content}

---

## EXTRACTION INSTRUCTIONS:

Look very carefully for tuition/fee amounts. They may be expressed as:
- Monthly (Monatsbeitrag, monatlich, pro Monat, per month)
- Annual (Jahresbeitrag, jährlich, pro Jahr, per year)
- In tables with different categories or income brackets
- As ranges (e.g., "200-500 EUR depending on income")

Common German fee terms:
- Schulgeld = school fees/tuition
- Beitrag = contribution/fee
- Gebühr = fee/charge
- Aufnahmegebühr = registration/enrollment fee
- Essensgeld/Mittagessen = meal plan
- Hort/Nachmittagsbetreuung = after-school care
- Materialgeld = materials fee
- Einkommensabhängig = income-based

If fees vary by income, extract the BASE or STANDARD fee (not the reduced rate).
If fees vary by school level, extract the SECONDARY school fees.

Return ONLY a JSON block with the extracted data:

```json
{{
    "is_private_school": true,
    "tuition_monthly_eur": <number or null - the standard monthly tuition in EUR>,
    "tuition_annual_eur": <number or null - the standard annual tuition in EUR>,
    "registration_fee_eur": <number or null>,
    "material_fee_annual_eur": <number or null>,
    "meal_plan_monthly_eur": <number or null>,
    "after_school_care_monthly_eur": <number or null>,
    "scholarship_available": <true/false/null>,
    "income_based_tuition": <true/false/null>,
    "tuition_notes": "<detailed summary of fee structure>",
    "tuition_source_url": "{source_url}"
}}
```
"""


PUBLIC_SCHOOL_PROMPT_V4 = """Analyze the following PUBLIC school website content and create a comprehensive school profile.

## School Information:
- Name: {school_name}
- Website: {website}
- Type: {school_type}
- District (Bezirk): {bezirk}
- Address: {address}
- Ownership: {traegerschaft} (PUBLIC SCHOOL - tuition-free)

## Scraped Website Content:

### Main Page:
{main_content}

### About Page:
{about_content}

### Curriculum Page:
{curriculum_content}

---

Based on the above content, create a structured profile with these sections:

### 1. MISSION AND EDUCATIONAL PHILOSOPHY
### 2. HISTORY, COMMUNITY AND FACILITIES
### 3. CURRICULUM AND ACADEMIC PROGRAMS
### 4. STUDENT LEARNING EXPERIENCES
### 5. SCHOOL ACHIEVEMENTS AND OUTCOMES
### 6. ADMISSIONS

At the end, include this JSON block:

```json
{{
    "is_private_school": false,
    "tuition_monthly_eur": null,
    "tuition_annual_eur": null,
    "registration_fee_eur": null,
    "material_fee_annual_eur": null,
    "meal_plan_monthly_eur": null,
    "after_school_care_monthly_eur": null,
    "scholarship_available": null,
    "income_based_tuition": null,
    "tuition_notes": "Public school - tuition-free",
    "tuition_source_url": null
}}
```
"""


def extract_tuition_json(text: str, is_private: bool) -> Dict:
    """Extract tuition JSON from LLM response."""
    default = {
        'is_private_school': is_private,
        'tuition_monthly_eur': None,
        'tuition_annual_eur': None,
        'registration_fee_eur': None,
        'material_fee_annual_eur': None,
        'meal_plan_monthly_eur': None,
        'after_school_care_monthly_eur': None,
        'scholarship_available': None,
        'income_based_tuition': None,
        'tuition_notes': None,
        'tuition_source_url': None
    }

    patterns = [
        r'```json\s*(\{[^`]+\})\s*```',
        r'```\s*(\{[^`]+\})\s*```',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text, re.DOTALL)
        for match in matches:
            try:
                json_str = re.sub(r',\s*}', '}', match.strip())
                parsed = json.loads(json_str)
                if 'is_private_school' in parsed:
                    for key in default:
                        if key not in parsed:
                            parsed[key] = default[key]
                    return parsed
            except json.JSONDecodeError:
                continue

    return default


def has_complete_tuition_data(row: pd.Series) -> bool:
    """Check if a school already has complete tuition data."""
    if row.get('traegerschaft') != 'Privat':
        return True

    has_tuition = pd.notna(row.get('tuition_monthly_eur')) or pd.notna(row.get('tuition_annual_eur'))
    return has_tuition


def has_description(row: pd.Series) -> bool:
    """Check if a school already has a description."""
    desc = row.get('description')
    return pd.notna(desc) and desc and len(str(desc)) > 100


def needs_processing(row: pd.Series, force_tuition: bool = False, force_description: bool = False) -> Tuple[bool, bool]:
    """Determine what processing a school needs."""
    needs_desc = force_description or not has_description(row)
    needs_tuit = force_tuition or not has_complete_tuition_data(row)
    return needs_desc, needs_tuit


def process_school_v4(
    scraper: ThoroughWebScraper,
    pdf_extractor: PDFExtractor,
    analyzer: LLMAnalyzer,
    school_info: Dict,
    existing_data: Dict,
    needs_description: bool,
    needs_tuition: bool
) -> Dict[str, Any]:
    """Process a single school with thorough crawling."""
    is_private = school_info.get('traegerschaft') == 'Privat'

    result = {
        'school_name': school_info['schulname'],
        'description': existing_data.get('description'),
        'tuition_data': {
            'is_private_school': is_private,
            'tuition_monthly_eur': existing_data.get('tuition_monthly_eur'),
            'tuition_annual_eur': existing_data.get('tuition_annual_eur'),
            'registration_fee_eur': existing_data.get('registration_fee_eur'),
            'material_fee_annual_eur': existing_data.get('material_fee_annual_eur'),
            'meal_plan_monthly_eur': existing_data.get('meal_plan_monthly_eur'),
            'after_school_care_monthly_eur': existing_data.get('after_school_care_monthly_eur'),
            'scholarship_available': existing_data.get('scholarship_available'),
            'income_based_tuition': existing_data.get('income_based_tuition'),
            'tuition_notes': existing_data.get('tuition_notes'),
            'tuition_source_url': existing_data.get('tuition_source_url'),
        },
        'pages_crawled': 0,
        'fee_pages_found': [],
        'pdf_links_found': [],
        'pdfs_extracted': [],
        'error': None,
        'generated_at': datetime.now().isoformat()
    }

    website = school_info.get('website', '')
    if not website or pd.isna(website):
        result['error'] = "No website available"
        if is_private:
            result['tuition_data']['tuition_notes'] = "No website"
        else:
            result['tuition_data']['tuition_notes'] = "Public school - tuition-free"
        return result

    if not needs_description and not needs_tuition:
        logger.info(f"Skipping {school_info['schulname']} - already has all data")
        return result

    # Thorough crawl
    logger.info(f"Thoroughly crawling {website} ({'private' if is_private else 'public'})")
    crawl_result = scraper.crawl_for_fees(website)

    result['pages_crawled'] = crawl_result.get('pages_crawled', 0)
    result['fee_pages_found'] = crawl_result.get('fee_pages_urls', [])
    result['pdf_links_found'] = crawl_result.get('all_pdfs', [])

    pages = crawl_result.get('pages', {})
    fee_pdfs = crawl_result.get('fee_pdfs', [])

    # Download and extract fee-related PDFs
    pdf_content_combined = ""
    if is_private and needs_tuition:
        # First try fee-related PDFs
        pdfs_to_try = fee_pdfs[:5]

        # If no fee PDFs found, try all PDFs
        if not pdfs_to_try:
            pdfs_to_try = crawl_result.get('all_pdfs', [])[:5]

        for pdf_info in pdfs_to_try:
            pdf_text = pdf_extractor.extract_text_from_url(pdf_info['url'])
            if pdf_text:
                result['pdfs_extracted'].append(pdf_info['url'])
                pdf_content_combined += f"\n\n--- PDF: {pdf_info.get('text', 'Unknown')} ({pdf_info['url']}) ---\n{pdf_text}"

    # Process with LLM
    if needs_description:
        # Full description
        if is_private:
            pdf_links_text = "None found"
            if crawl_result.get('all_pdfs'):
                pdf_links_text = "\n".join([
                    f"- {pdf.get('text', 'Unknown')}: {pdf['url']} (fee-related: {pdf in fee_pdfs})"
                    for pdf in crawl_result['all_pdfs'][:15]
                ])

            prompt = PRIVATE_SCHOOL_PROMPT_V4.format(
                school_name=school_info['schulname'],
                website=website,
                school_type=school_info.get('school_type', 'Unknown'),
                bezirk=school_info.get('bezirk', ''),
                address=f"{school_info.get('strasse', '')}, {school_info.get('plz', '')} Berlin",
                traegerschaft=school_info.get('traegerschaft', 'Privat'),
                main_content=pages.get('main', 'Not available')[:5000],
                num_fee_pages=len(crawl_result.get('fee_pages_urls', [])),
                fees_content=pages.get('fees', 'Not found')[:8000],
                admissions_content=pages.get('admissions', 'Not found')[:3000],
                about_content=pages.get('about', 'Not found')[:3000],
                curriculum_content=pages.get('curriculum', 'Not found')[:3000],
                pdf_links=pdf_links_text,
                pdf_content=pdf_content_combined[:12000] if pdf_content_combined else "No PDF content extracted"
            )
        else:
            prompt = PUBLIC_SCHOOL_PROMPT_V4.format(
                school_name=school_info['schulname'],
                website=website,
                school_type=school_info.get('school_type', 'Unknown'),
                bezirk=school_info.get('bezirk', ''),
                address=f"{school_info.get('strasse', '')}, {school_info.get('plz', '')} Berlin",
                traegerschaft=school_info.get('traegerschaft', 'Öffentlich'),
                main_content=pages.get('main', 'Not available')[:5000],
                about_content=pages.get('about', 'Not found')[:3000],
                curriculum_content=pages.get('curriculum', 'Not found')[:3000]
            )

        try:
            logger.info(f"Analyzing {school_info['schulname']} (full description)")
            description = analyzer.analyze(prompt)
            result['description'] = description
            result['tuition_data'] = extract_tuition_json(description, is_private)
        except Exception as e:
            logger.error(f"Error analyzing {school_info['schulname']}: {e}")
            result['error'] = str(e)

    elif needs_tuition and is_private:
        # Tuition-only extraction
        fee_content = pages.get('fees', '')

        if fee_content or pdf_content_combined:
            source_url = result['fee_pages_found'][0] if result['fee_pages_found'] else (
                result['pdfs_extracted'][0] if result['pdfs_extracted'] else website
            )

            prompt = TUITION_ONLY_PROMPT_V4.format(
                school_name=school_info['schulname'],
                website=website,
                fee_pages_content=fee_content[:10000] if fee_content else "No fee pages found",
                pdf_content=pdf_content_combined[:12000] if pdf_content_combined else "No PDF content extracted",
                source_url=source_url
            )

            try:
                logger.info(f"Extracting tuition for {school_info['schulname']}")
                response = analyzer.analyze(prompt)
                tuition_data = extract_tuition_json(response, is_private)

                for key, value in tuition_data.items():
                    if value is not None:
                        result['tuition_data'][key] = value
            except Exception as e:
                logger.error(f"Error extracting tuition for {school_info['schulname']}: {e}")
                result['error'] = str(e)

    return result


def process_schools_v4(
    input_file: str,
    output_dir: str = "school_descriptions_v4",
    force_tuition: bool = False,
    force_description: bool = False,
    only_private: bool = False,
    delay_seconds: float = 2.0,
    max_pages_per_school: int = 25
) -> pd.DataFrame:
    """Process schools with thorough crawling."""

    config = load_config()
    os.makedirs(output_dir, exist_ok=True)

    scraper = ThoroughWebScraper(max_pages=max_pages_per_school)
    pdf_extractor = PDFExtractor()
    analyzer = LLMAnalyzer(config)

    logger.info(f"Loading schools from {input_file}")
    df = pd.read_csv(input_file, encoding='utf-8-sig')

    if only_private:
        df = df[df['traegerschaft'] == 'Privat']
        logger.info(f"Filtered to {len(df)} private schools")

    # Count what needs processing
    needs_desc_count = 0
    needs_tuit_count = 0

    for idx, row in df.iterrows():
        n_desc, n_tuit = needs_processing(row, force_tuition, force_description)
        if n_desc:
            needs_desc_count += 1
        if n_tuit and row.get('traegerschaft') == 'Privat':
            needs_tuit_count += 1

    logger.info(f"Schools needing description: {needs_desc_count}")
    logger.info(f"Private schools needing tuition: {needs_tuit_count}")

    results = []
    processed_count = 0

    for idx, row in df.iterrows():
        school_info = row.to_dict()
        n_desc, n_tuit = needs_processing(row, force_tuition, force_description)

        if not n_desc and not n_tuit:
            results.append({
                'schulnummer': school_info.get('schulnummer'),
                'school_name': school_info.get('schulname'),
                'description': school_info.get('description'),
                'tuition_data': {
                    'is_private_school': school_info.get('traegerschaft') == 'Privat',
                    'tuition_monthly_eur': school_info.get('tuition_monthly_eur'),
                    'tuition_annual_eur': school_info.get('tuition_annual_eur'),
                    'registration_fee_eur': school_info.get('registration_fee_eur'),
                    'material_fee_annual_eur': school_info.get('material_fee_annual_eur'),
                    'meal_plan_monthly_eur': school_info.get('meal_plan_monthly_eur'),
                    'after_school_care_monthly_eur': school_info.get('after_school_care_monthly_eur'),
                    'scholarship_available': school_info.get('scholarship_available'),
                    'income_based_tuition': school_info.get('income_based_tuition'),
                    'tuition_notes': school_info.get('tuition_notes'),
                    'tuition_source_url': school_info.get('tuition_source_url'),
                },
                'pages_crawled': 0,
                'fee_pages_found': [],
                'pdf_links_found': [],
                'pdfs_extracted': [],
                'error': None,
                'skipped': True
            })
            continue

        safe_filename = re.sub(r'[^\w\-]', '_', school_info['schulname'])[:50]
        output_file = os.path.join(output_dir, f"{safe_filename}.json")

        existing_data = {
            'description': school_info.get('description'),
            'tuition_monthly_eur': school_info.get('tuition_monthly_eur'),
            'tuition_annual_eur': school_info.get('tuition_annual_eur'),
            'registration_fee_eur': school_info.get('registration_fee_eur'),
            'material_fee_annual_eur': school_info.get('material_fee_annual_eur'),
            'meal_plan_monthly_eur': school_info.get('meal_plan_monthly_eur'),
            'after_school_care_monthly_eur': school_info.get('after_school_care_monthly_eur'),
            'scholarship_available': school_info.get('scholarship_available'),
            'income_based_tuition': school_info.get('income_based_tuition'),
            'tuition_notes': school_info.get('tuition_notes'),
            'tuition_source_url': school_info.get('tuition_source_url'),
        }

        result = process_school_v4(
            scraper, pdf_extractor, analyzer, school_info, existing_data, n_desc, n_tuit
        )

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

        result['schulnummer'] = school_info.get('schulnummer', '')
        results.append(result)

        processed_count += 1
        time.sleep(delay_seconds)

    # Create summary
    summary_data = []
    for r in results:
        tuition = r.get('tuition_data', {}) or {}

        summary_data.append({
            'schulnummer': r.get('schulnummer'),
            'schulname': r.get('school_name'),
            'description': r.get('description'),
            'description_generated': r.get('description') is not None and r.get('error') is None,
            'pages_crawled': r.get('pages_crawled', 0),
            'fee_pages_found': len(r.get('fee_pages_found', [])),
            'pdfs_found': len(r.get('pdf_links_found', [])),
            'pdfs_extracted': ', '.join(r.get('pdfs_extracted', [])) if r.get('pdfs_extracted') else None,
            'error': r.get('error'),
            'skipped': r.get('skipped', False),
            'is_private_school': tuition.get('is_private_school'),
            'tuition_monthly_eur': tuition.get('tuition_monthly_eur'),
            'tuition_annual_eur': tuition.get('tuition_annual_eur'),
            'registration_fee_eur': tuition.get('registration_fee_eur'),
            'material_fee_annual_eur': tuition.get('material_fee_annual_eur'),
            'meal_plan_monthly_eur': tuition.get('meal_plan_monthly_eur'),
            'after_school_care_monthly_eur': tuition.get('after_school_care_monthly_eur'),
            'scholarship_available': tuition.get('scholarship_available'),
            'income_based_tuition': tuition.get('income_based_tuition'),
            'tuition_notes': tuition.get('tuition_notes'),
            'tuition_source_url': tuition.get('tuition_source_url'),
            'generated_at': r.get('generated_at')
        })

    summary_df = pd.DataFrame(summary_data)
    summary_file = os.path.join(output_dir, 'descriptions_summary.csv')
    summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
    logger.info(f"Saved summary to {summary_file}")

    return summary_df


def merge_to_master(
    master_file: str,
    descriptions_dir: str,
    output_file: str
) -> pd.DataFrame:
    """Merge descriptions and tuition data into master table."""

    logger.info(f"Merging into master table")
    df = pd.read_csv(master_file, encoding='utf-8-sig')

    summary_file = os.path.join(descriptions_dir, 'descriptions_summary.csv')
    if os.path.exists(summary_file):
        summary_df = pd.read_csv(summary_file, encoding='utf-8-sig')

        merge_cols = [
            'schulnummer', 'description',
            'tuition_monthly_eur', 'tuition_annual_eur',
            'registration_fee_eur', 'material_fee_annual_eur',
            'meal_plan_monthly_eur', 'after_school_care_monthly_eur',
            'scholarship_available', 'income_based_tuition',
            'tuition_notes', 'tuition_source_url'
        ]
        merge_cols = [c for c in merge_cols if c in summary_df.columns]

        cols_to_drop = [c for c in merge_cols if c in df.columns and c != 'schulnummer']
        df = df.drop(columns=cols_to_drop, errors='ignore')

        df = df.merge(summary_df[merge_cols], on='schulnummer', how='left')

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    df.to_excel(output_file.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    logger.info(f"Saved to {output_file}")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate school descriptions with THOROUGH crawling (V4)')
    parser.add_argument('--input', '-i', default='combined_schools_with_metadata_msa_with_descriptions_v2_v3.csv',
                       help='Input master table file')
    parser.add_argument('--output-dir', '-o', default='school_descriptions_v4')
    parser.add_argument('--force-tuition', '-t', action='store_true',
                       help='Force re-extraction of tuition data even if exists')
    parser.add_argument('--force-description', '-f', action='store_true',
                       help='Force regeneration of descriptions even if exists')
    parser.add_argument('--private-only', '-p', action='store_true',
                       help='Only process private schools')
    parser.add_argument('--delay', '-d', type=float, default=2.0)
    parser.add_argument('--max-pages', type=int, default=25,
                       help='Maximum pages to crawl per school')
    parser.add_argument('--merge', '-m', action='store_true',
                       help='Merge results into master table')

    args = parser.parse_args()

    if not HAS_PYPDF2 and not HAS_PDFPLUMBER:
        logger.warning("No PDF library available. Install with: pip install pdfplumber PyPDF2")

    if args.merge:
        output_file = args.input.replace('.csv', '_v4.csv')
        merge_to_master(args.input, args.output_dir, output_file)
    else:
        summary_df = process_schools_v4(
            input_file=args.input,
            output_dir=args.output_dir,
            force_tuition=args.force_tuition,
            force_description=args.force_description,
            only_private=args.private_only,
            delay_seconds=args.delay,
            max_pages_per_school=args.max_pages
        )

        print("\n" + "="*70)
        print("SCHOOL DESCRIPTION GENERATION V4 SUMMARY (THOROUGH CRAWL)")
        print("="*70)
        print(f"Total in dataset: {len(summary_df)}")
        print(f"Skipped (already complete): {summary_df['skipped'].sum()}")
        print(f"Processed: {(~summary_df['skipped']).sum()}")
        print(f"Successfully generated: {summary_df['description_generated'].sum()}")
        print(f"Errors: {summary_df['error'].notna().sum()}")

        with_tuition = summary_df[
            summary_df['tuition_monthly_eur'].notna() |
            summary_df['tuition_annual_eur'].notna()
        ]
        print(f"Schools with tuition amounts: {len(with_tuition)}")

        total_pages = summary_df['pages_crawled'].sum()
        total_fee_pages = summary_df['fee_pages_found'].sum()
        total_pdfs = summary_df['pdfs_found'].sum()
        print(f"Total pages crawled: {total_pages}")
        print(f"Total fee pages found: {total_fee_pages}")
        print(f"Total PDFs found: {total_pdfs}")

        print(f"\nOutput: {args.output_dir}")
        print("="*70)


if __name__ == "__main__":
    main()
