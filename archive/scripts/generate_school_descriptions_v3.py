#!/usr/bin/env python3
"""
Generate Rich School Descriptions Using LLM with Web Scraping and PDF Extraction (V3)

Improvements over v2:
1. Downloads and parses PDFs to extract tuition information
2. Skips schools that already have complete tuition data
3. Incremental processing - only reprocess what's missing
4. Caches downloaded PDFs to avoid re-downloading

This script:
1. Checks existing data to see what's missing
2. Scrapes school websites and downloads fee PDFs
3. Extracts text from PDFs and sends to LLM for analysis
4. Updates only the missing data in the master table
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
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, urlparse
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
        logging.FileHandler('school_descriptions_v3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"
PDF_CACHE_DIR = Path(__file__).parent / "pdf_cache"


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
        # Create a safe filename from URL
        safe_name = re.sub(r'[^\w\-.]', '_', urlparse(url).path.split('/')[-1])
        if not safe_name.endswith('.pdf'):
            safe_name += '.pdf'
        # Add hash of full URL to avoid collisions
        url_hash = str(hash(url) % 10**8)
        return self.cache_dir / f"{url_hash}_{safe_name}"

    def download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file, using cache if available."""
        cache_path = self._get_cache_path(url)

        # Check cache first
        if cache_path.exists():
            logger.debug(f"Using cached PDF: {cache_path}")
            return cache_path.read_bytes()

        try:
            logger.info(f"Downloading PDF: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Verify it's actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not response.content[:4] == b'%PDF':
                logger.warning(f"Not a PDF: {url} (content-type: {content_type})")
                return None

            # Cache the PDF
            cache_path.write_bytes(response.content)
            logger.info(f"Cached PDF to: {cache_path}")

            return response.content

        except Exception as e:
            logger.warning(f"Error downloading PDF {url}: {e}")
            return None

    def extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF content."""
        text = ""

        # Try pdfplumber first (better for tables)
        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n\n"
                        # Also try to extract tables
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                if row:
                                    text += " | ".join([str(cell) if cell else "" for cell in row]) + "\n"
                            text += "\n"
                if text.strip():
                    return text[:20000]  # Limit size
            except Exception as e:
                logger.warning(f"pdfplumber extraction failed: {e}")

        # Fallback to PyPDF2
        if HAS_PYPDF2 and not text.strip():
            try:
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
                if text.strip():
                    return text[:20000]
            except Exception as e:
                logger.warning(f"PyPDF2 extraction failed: {e}")

        return text.strip()

    def extract_text_from_url(self, url: str) -> Optional[str]:
        """Download PDF and extract text."""
        pdf_content = self.download_pdf(url)
        if pdf_content:
            return self.extract_text_from_pdf(pdf_content)
        return None


class WebScraper:
    """Scrape school websites to extract actual content."""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
        })

    def fetch_page(self, url: str) -> Tuple[str, str]:
        """Fetch a page and return (text_content, html)."""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()

            text = soup.get_text(separator='\n', strip=True)
            text = re.sub(r'\n{3,}', '\n\n', text)

            return text[:15000], response.text

        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return "", ""

    def find_relevant_links(self, html: str, base_url: str, include_fees: bool = True) -> Dict[str, str]:
        """Find links to fees, admissions, about pages."""
        relevant_links = {}

        if include_fees:
            keywords = {
                'fees': ['fee', 'fees', 'gebühr', 'kosten', 'schulgeld', 'tuition', 'pricing', 'price', 'contract'],
                'admissions': ['admission', 'admissions', 'aufnahme', 'anmeldung', 'enrollment', 'enrolment', 'apply', 'join-us', 'join us'],
                'about': ['about', 'über', 'ueber', 'profil', 'profile', 'school', 'schule', 'leitbild', 'mission'],
                'curriculum': ['curriculum', 'program', 'programm', 'unterricht', 'education', 'academic', 'fächer'],
            }
        else:
            keywords = {
                'about': ['about', 'über', 'ueber', 'profil', 'profile', 'school', 'schule', 'leitbild', 'mission'],
                'curriculum': ['curriculum', 'program', 'programm', 'unterricht', 'education', 'academic', 'fächer'],
            }

        try:
            soup = BeautifulSoup(html, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True).lower()
                href_lower = href.lower()

                for category, words in keywords.items():
                    if category not in relevant_links:
                        for word in words:
                            if word in text or word in href_lower:
                                full_url = urljoin(base_url, href)
                                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                                    relevant_links[category] = full_url
                                    break
        except Exception as e:
            logger.warning(f"Error parsing links: {e}")

        return relevant_links

    def find_pdf_links(self, html: str, base_url: str) -> List[Dict[str, str]]:
        """Find PDF links on a page, especially for fees/tuition documents."""
        pdf_links = []
        fee_keywords = ['fee', 'gebühr', 'kosten', 'schulgeld', 'tuition', 'price', 'contract', 'vertrag', 'beitrag']

        try:
            soup = BeautifulSoup(html, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if '.pdf' in href.lower():
                    text = link.get_text(strip=True).lower()
                    href_lower = href.lower()
                    full_url = urljoin(base_url, href)

                    is_fee_related = any(kw in text or kw in href_lower for kw in fee_keywords)

                    pdf_links.append({
                        'url': full_url,
                        'text': link.get_text(strip=True),
                        'category': 'fees' if is_fee_related else 'other'
                    })
        except Exception as e:
            logger.warning(f"Error finding PDF links: {e}")

        return pdf_links

    def scrape_school(self, website: str, is_private: bool = True) -> Dict[str, Any]:
        """Scrape a school website and return content from relevant pages."""
        content = {
            'main': '',
            'fees': '',
            'admissions': '',
            'about': '',
            'curriculum': '',
            'pdf_links': []
        }

        if not website:
            return content

        if not website.startswith('http'):
            website = 'https://' + website

        main_text, main_html = self.fetch_page(website)
        content['main'] = main_text

        if not main_html:
            return content

        links = self.find_relevant_links(main_html, website, include_fees=is_private)

        if is_private:
            content['pdf_links'] = self.find_pdf_links(main_html, website)

        for category, url in links.items():
            if category in content and url != website:
                text, page_html = self.fetch_page(url)
                content[category] = text[:8000]

                if is_private and category in ['fees', 'admissions'] and page_html:
                    page_pdfs = self.find_pdf_links(page_html, url)
                    content['pdf_links'].extend(page_pdfs)

                time.sleep(0.5)

        # Deduplicate PDF links
        seen_urls = set()
        unique_pdfs = []
        for pdf in content['pdf_links']:
            if pdf['url'] not in seen_urls:
                seen_urls.add(pdf['url'])
                unique_pdfs.append(pdf)
        content['pdf_links'] = unique_pdfs

        return content


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
                {"role": "system", "content": "You are an expert education researcher analyzing school information. Be factual and cite the content provided. Always include the JSON block at the end."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 6000,
            "temperature": 0.2
        }

        response = requests.post(self.base_url, headers=headers, json=data, timeout=120)
        response.raise_for_status()

        result = response.json()
        return result["choices"][0]["message"]["content"]


# Prompts for different scenarios
PRIVATE_SCHOOL_PROMPT = """Analyze the following PRIVATE school website content and create a comprehensive school profile.

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

### Fees/Tuition Page:
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
Extract enrollment process, requirements, and ALL fee information including:
- Tuition amounts (monthly/annual)
- Registration fees
- Material fees
- Meal costs
- After-school care costs
- Scholarship info
- Income-based tuition options

Be VERY specific about fees - extract exact EUR amounts from the PDF content if available.

At the end, include this JSON block with extracted tuition data:

```json
{{
    "is_private_school": true,
    "tuition_monthly_eur": <number or null>,
    "tuition_annual_eur": <number or null>,
    "registration_fee_eur": <number or null>,
    "material_fee_annual_eur": <number or null>,
    "meal_plan_monthly_eur": <number or null>,
    "after_school_care_monthly_eur": <number or null>,
    "scholarship_available": <true/false/null>,
    "income_based_tuition": <true/false/null>,
    "tuition_notes": "<summary of fee structure>",
    "tuition_source_url": "<URL where fees were found>"
}}
```
"""


TUITION_ONLY_PROMPT = """Analyze the following PDF content from a private school to extract tuition/fee information.

## School Information:
- Name: {school_name}
- Website: {website}

## PDF Content:
{pdf_content}

---

Extract ALL fee information from the PDF including:
- Tuition amounts (monthly/annual) - look for Schulgeld, Monatsbeitrag, Jahresbeitrag
- Registration fees (Aufnahmegebühr, Anmeldegebühr)
- Material fees (Materialgeld)
- Meal costs (Essensgeld, Mittagessen)
- After-school care costs (Hort, Nachmittagsbetreuung)
- Scholarship info
- Income-based tuition options (einkommensabhängig)

Look for tables with fee amounts in EUR.

Return ONLY a JSON block with the extracted data:

```json
{{
    "is_private_school": true,
    "tuition_monthly_eur": <number or null>,
    "tuition_annual_eur": <number or null>,
    "registration_fee_eur": <number or null>,
    "material_fee_annual_eur": <number or null>,
    "meal_plan_monthly_eur": <number or null>,
    "after_school_care_monthly_eur": <number or null>,
    "scholarship_available": <true/false/null>,
    "income_based_tuition": <true/false/null>,
    "tuition_notes": "<summary of fee structure found in PDF>",
    "tuition_source_url": "{pdf_url}"
}}
```
"""


PUBLIC_SCHOOL_PROMPT = """Analyze the following PUBLIC school website content and create a comprehensive school profile.

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
Extract mission statement, values, educational approach from the content.

### 2. HISTORY, COMMUNITY AND FACILITIES
Extract founding info, location details, facilities mentioned.

### 3. CURRICULUM AND ACADEMIC PROGRAMS
Extract grades offered, languages, special programs, graduation pathways (Abitur, MSA, BBR).

### 4. STUDENT LEARNING EXPERIENCES
Extract extracurriculars, sports, arts, exchanges mentioned.

### 5. SCHOOL ACHIEVEMENTS AND OUTCOMES
Extract any awards, rankings, certifications mentioned.

### 6. ADMISSIONS
Extract enrollment process and requirements (note: as a public school, there is no tuition fee).

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
    # For public schools, we don't need tuition data
    if row.get('traegerschaft') != 'Privat':
        return True

    # For private schools, check if we have at least monthly or annual tuition
    has_tuition = pd.notna(row.get('tuition_monthly_eur')) or pd.notna(row.get('tuition_annual_eur'))

    # Or if we have notes indicating fees are available
    notes = row.get('tuition_notes', '')
    if pd.notna(notes) and notes and 'Public school' not in str(notes) and 'No website' not in str(notes) and 'Error' not in str(notes):
        # Has some tuition notes - consider it processed
        has_notes = True
    else:
        has_notes = False

    return has_tuition or has_notes


def has_description(row: pd.Series) -> bool:
    """Check if a school already has a description."""
    desc = row.get('description')
    return pd.notna(desc) and desc and len(str(desc)) > 100


def needs_processing(row: pd.Series, force_tuition: bool = False, force_description: bool = False) -> Tuple[bool, bool]:
    """
    Determine what processing a school needs.

    Returns: (needs_description, needs_tuition)
    """
    needs_desc = force_description or not has_description(row)
    needs_tuit = force_tuition or not has_complete_tuition_data(row)

    return needs_desc, needs_tuit


def process_school_incremental(
    scraper: WebScraper,
    pdf_extractor: PDFExtractor,
    analyzer: LLMAnalyzer,
    school_info: Dict,
    existing_data: Dict,
    needs_description: bool,
    needs_tuition: bool
) -> Dict[str, Any]:
    """
    Process a single school incrementally - only fetch what's needed.
    """
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
        'scraped_pages': [],
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

    # Only scrape if we need something
    if not needs_description and not needs_tuition:
        logger.info(f"Skipping {school_info['schulname']} - already has all data")
        return result

    # Scrape website
    logger.info(f"Scraping {website} ({'private - deep' if is_private else 'public - basic'})")
    content = scraper.scrape_school(website, is_private=is_private)
    result['scraped_pages'] = [k for k, v in content.items() if v and k != 'pdf_links']
    result['pdf_links_found'] = content.get('pdf_links', [])

    # For private schools, download and extract fee-related PDFs
    pdf_content_combined = ""
    if is_private and needs_tuition and content.get('pdf_links'):
        fee_pdfs = [p for p in content['pdf_links'] if p.get('category') == 'fees']

        for pdf_info in fee_pdfs[:3]:  # Limit to 3 PDFs
            pdf_text = pdf_extractor.extract_text_from_url(pdf_info['url'])
            if pdf_text:
                result['pdfs_extracted'].append(pdf_info['url'])
                pdf_content_combined += f"\n\n--- PDF: {pdf_info['text']} ({pdf_info['url']}) ---\n{pdf_text}"

    # Determine what to process
    if needs_description:
        # Full processing for description
        if is_private:
            pdf_links_text = "None found"
            if content.get('pdf_links'):
                pdf_links_text = "\n".join([
                    f"- {pdf['text']}: {pdf['url']} (fee-related: {pdf['category'] == 'fees'})"
                    for pdf in content['pdf_links'][:10]
                ])

            prompt = PRIVATE_SCHOOL_PROMPT.format(
                school_name=school_info['schulname'],
                website=website,
                school_type=school_info.get('school_type', 'Unknown'),
                bezirk=school_info.get('bezirk', ''),
                address=f"{school_info.get('strasse', '')}, {school_info.get('plz', '')} Berlin",
                traegerschaft=school_info.get('traegerschaft', 'Privat'),
                main_content=content.get('main', 'Not available')[:5000],
                fees_content=content.get('fees', 'Not found')[:5000],
                admissions_content=content.get('admissions', 'Not found')[:3000],
                about_content=content.get('about', 'Not found')[:3000],
                curriculum_content=content.get('curriculum', 'Not found')[:3000],
                pdf_links=pdf_links_text,
                pdf_content=pdf_content_combined[:10000] if pdf_content_combined else "No PDF content extracted"
            )
        else:
            prompt = PUBLIC_SCHOOL_PROMPT.format(
                school_name=school_info['schulname'],
                website=website,
                school_type=school_info.get('school_type', 'Unknown'),
                bezirk=school_info.get('bezirk', ''),
                address=f"{school_info.get('strasse', '')}, {school_info.get('plz', '')} Berlin",
                traegerschaft=school_info.get('traegerschaft', 'Öffentlich'),
                main_content=content.get('main', 'Not available')[:5000],
                about_content=content.get('about', 'Not found')[:3000],
                curriculum_content=content.get('curriculum', 'Not found')[:3000]
            )

        try:
            logger.info(f"Analyzing {school_info['schulname']} (full description)")
            description = analyzer.analyze(prompt)
            result['description'] = description
            result['tuition_data'] = extract_tuition_json(description, is_private)
        except Exception as e:
            logger.error(f"Error analyzing {school_info['schulname']}: {e}")
            result['error'] = str(e)

    elif needs_tuition and is_private and pdf_content_combined:
        # Only need tuition extraction from PDFs
        pdf_url = result['pdfs_extracted'][0] if result['pdfs_extracted'] else website

        prompt = TUITION_ONLY_PROMPT.format(
            school_name=school_info['schulname'],
            website=website,
            pdf_content=pdf_content_combined[:15000],
            pdf_url=pdf_url
        )

        try:
            logger.info(f"Extracting tuition for {school_info['schulname']} from PDFs")
            response = analyzer.analyze(prompt)
            tuition_data = extract_tuition_json(response, is_private)

            # Merge with existing data (only update non-null values)
            for key, value in tuition_data.items():
                if value is not None:
                    result['tuition_data'][key] = value
        except Exception as e:
            logger.error(f"Error extracting tuition for {school_info['schulname']}: {e}")
            result['error'] = str(e)

    return result


def process_schools_incremental(
    input_file: str,
    output_dir: str = "school_descriptions_v3",
    force_tuition: bool = False,
    force_description: bool = False,
    only_private: bool = False,
    delay_seconds: float = 3.0
) -> pd.DataFrame:
    """Process schools incrementally - only process what's missing."""

    config = load_config()
    os.makedirs(output_dir, exist_ok=True)

    scraper = WebScraper()
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

        # Skip if nothing to do
        if not n_desc and not n_tuit:
            # Still include in results with existing data
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
                'scraped_pages': [],
                'pdf_links_found': [],
                'pdfs_extracted': [],
                'error': None,
                'skipped': True
            })
            continue

        # Check for cached result
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

        result = process_school_incremental(
            scraper, pdf_extractor, analyzer, school_info, existing_data, n_desc, n_tuit
        )

        # Save individual result
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        result['schulnummer'] = school_info.get('schulnummer', '')
        results.append(result)

        processed_count += 1
        time.sleep(delay_seconds)

    # Create summary
    summary_data = []
    for r in results:
        tuition = r.get('tuition_data', {}) or {}
        pdf_links = r.get('pdf_links_found', [])
        fee_pdfs = [p['url'] for p in pdf_links if p.get('category') == 'fees']

        summary_data.append({
            'schulnummer': r.get('schulnummer'),
            'schulname': r.get('school_name'),
            'description': r.get('description'),
            'description_generated': r.get('description') is not None and r.get('error') is None,
            'scraped_pages': ', '.join(r.get('scraped_pages', [])),
            'pdfs_extracted': ', '.join(r.get('pdfs_extracted', [])),
            'fee_pdfs_found': ', '.join(fee_pdfs) if fee_pdfs else None,
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

        # Columns to merge - excluding 'skipped'
        merge_cols = [
            'schulnummer', 'description',
            'tuition_monthly_eur', 'tuition_annual_eur',
            'registration_fee_eur', 'material_fee_annual_eur',
            'meal_plan_monthly_eur', 'after_school_care_monthly_eur',
            'scholarship_available', 'income_based_tuition',
            'tuition_notes', 'tuition_source_url'
        ]
        merge_cols = [c for c in merge_cols if c in summary_df.columns]

        # Drop existing columns that will be replaced
        cols_to_drop = [c for c in merge_cols if c in df.columns and c != 'schulnummer']
        df = df.drop(columns=cols_to_drop, errors='ignore')

        df = df.merge(summary_df[merge_cols], on='schulnummer', how='left')

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    df.to_excel(output_file.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    logger.info(f"Saved to {output_file}")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate school descriptions with PDF extraction (V3)')
    parser.add_argument('--input', '-i', default='combined_schools_with_metadata_msa_with_descriptions_v2.csv',
                       help='Input master table file')
    parser.add_argument('--output-dir', '-o', default='school_descriptions_v3')
    parser.add_argument('--force-tuition', '-t', action='store_true',
                       help='Force re-extraction of tuition data even if exists')
    parser.add_argument('--force-description', '-f', action='store_true',
                       help='Force regeneration of descriptions even if exists')
    parser.add_argument('--private-only', '-p', action='store_true',
                       help='Only process private schools')
    parser.add_argument('--delay', '-d', type=float, default=3.0)
    parser.add_argument('--merge', '-m', action='store_true',
                       help='Merge results into master table')

    args = parser.parse_args()

    # Check PDF library availability
    if not HAS_PYPDF2 and not HAS_PDFPLUMBER:
        logger.warning("No PDF library available. Install with: pip install pdfplumber PyPDF2")
        logger.warning("PDF extraction will not work!")

    if args.merge:
        output_file = args.input.replace('.csv', '_v3.csv')
        merge_to_master(args.input, args.output_dir, output_file)
    else:
        summary_df = process_schools_incremental(
            input_file=args.input,
            output_dir=args.output_dir,
            force_tuition=args.force_tuition,
            force_description=args.force_description,
            only_private=args.private_only,
            delay_seconds=args.delay
        )

        print("\n" + "="*70)
        print("SCHOOL DESCRIPTION GENERATION V3 SUMMARY")
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

        pdfs_extracted = summary_df[summary_df['pdfs_extracted'].notna() & (summary_df['pdfs_extracted'] != '')]
        print(f"Schools with PDFs extracted: {len(pdfs_extracted)}")

        print(f"\nOutput: {args.output_dir}")
        print("="*70)


if __name__ == "__main__":
    main()
