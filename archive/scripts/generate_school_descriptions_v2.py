#!/usr/bin/env python3
"""
Generate Rich School Descriptions Using LLM with Direct Web Scraping (V2)

Improvements over v1:
1. Actually fetches website content before sending to LLM
2. Follows links to find fees/admissions pages
3. Adds description as a column to master table
4. Better structured output

This script:
1. Directly scrapes school websites to get actual content
2. Sends the scraped content to LLM for analysis and structuring
3. Produces comprehensive descriptions with tuition data
"""

import os
import json
import time
import logging
import re
import requests
import yaml
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, urlparse
import pandas as pd
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('school_descriptions_v2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"


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
            # Clean up excessive whitespace
            text = re.sub(r'\n{3,}', '\n\n', text)

            return text[:15000], response.text  # Limit text length

        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return "", ""

    def find_relevant_links(self, html: str, base_url: str, include_fees: bool = True) -> Dict[str, str]:
        """Find links to fees, admissions, about pages.

        Args:
            html: Page HTML content
            base_url: Base URL for resolving relative links
            include_fees: If False, skip searching for fees/tuition links (for public schools)
        """
        relevant_links = {}

        # Different keyword sets based on whether we need fees info
        if include_fees:
            keywords = {
                'fees': ['fee', 'fees', 'gebühr', 'kosten', 'schulgeld', 'tuition', 'pricing', 'price', 'contract'],
                'admissions': ['admission', 'admissions', 'aufnahme', 'anmeldung', 'enrollment', 'enrolment', 'apply', 'join-us', 'join us'],
                'about': ['about', 'über', 'ueber', 'profil', 'profile', 'school', 'schule', 'leitbild', 'mission'],
                'curriculum': ['curriculum', 'program', 'programm', 'unterricht', 'education', 'academic', 'fächer'],
                'contact': ['contact', 'kontakt', 'impressum']
            }
        else:
            # For public schools - skip fees keywords entirely
            keywords = {
                'about': ['about', 'über', 'ueber', 'profil', 'profile', 'school', 'schule', 'leitbild', 'mission'],
                'curriculum': ['curriculum', 'program', 'programm', 'unterricht', 'education', 'academic', 'fächer'],
                'contact': ['contact', 'kontakt', 'impressum']
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
        """Find PDF links on a page, especially for fees/tuition documents.

        Returns list of dicts with 'url', 'text', 'category' keys.
        """
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

                    # Check if this PDF is likely about fees
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
        """Scrape a school website and return content from relevant pages.

        Args:
            website: School website URL
            is_private: If True, do deep search for fees/tuition info. If False, skip fees search.
        """
        content = {
            'main': '',
            'fees': '',
            'admissions': '',
            'about': '',
            'curriculum': '',
            'pdf_links': []  # Store found PDF links for reference
        }

        if not website:
            return content

        # Ensure URL has scheme
        if not website.startswith('http'):
            website = 'https://' + website

        # Fetch main page
        main_text, main_html = self.fetch_page(website)
        content['main'] = main_text

        if not main_html:
            return content

        # Find and fetch relevant subpages (include_fees=True only for private schools)
        links = self.find_relevant_links(main_html, website, include_fees=is_private)

        # For private schools, also look for PDF links on main page
        if is_private:
            content['pdf_links'] = self.find_pdf_links(main_html, website)

        for category, url in links.items():
            if category in content and url != website:
                text, page_html = self.fetch_page(url)
                content[category] = text[:8000]  # Limit each section

                # For private schools, look for PDFs on fees/admissions pages too
                if is_private and category in ['fees', 'admissions'] and page_html:
                    page_pdfs = self.find_pdf_links(page_html, url)
                    content['pdf_links'].extend(page_pdfs)

                time.sleep(0.5)  # Be polite

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

        # Use Perplexity by default
        self.api_key = api_keys.get('perplexity')
        self.model = models.get('perplexity', 'sonar')
        self.base_url = "https://api.perplexity.ai/chat/completions"

        if not self.api_key:
            # Fallback to OpenAI
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

### PDF Documents Found (may contain fee information):
{pdf_links}

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

Be VERY specific about fees - extract exact EUR amounts if mentioned.
If exact amounts are not on the page but PDFs are listed that likely contain fee info, note this in tuition_notes and provide the PDF URL.

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
    "tuition_notes": "<summary of fee structure or note that fees are in PDF>",
    "tuition_source_url": "<URL where fees were found or PDF URL>"
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

    # Find JSON block
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


def process_school(
    scraper: WebScraper,
    analyzer: LLMAnalyzer,
    school_info: Dict
) -> Dict[str, Any]:
    """Process a single school.

    Uses different scraping depth and prompts for private vs public schools:
    - Private: Deep scrape including fees/admissions pages, PDF link extraction
    - Public: Basic scrape (main, about, curriculum), no fee search
    """

    is_private = school_info.get('traegerschaft') == 'Privat'

    result = {
        'school_name': school_info['schulname'],
        'description': None,
        'tuition_data': None,
        'scraped_pages': [],
        'pdf_links_found': [],
        'error': None,
        'generated_at': datetime.now().isoformat()
    }

    website = school_info.get('website', '')
    if not website or pd.isna(website):
        result['error'] = "No website available"
        result['tuition_data'] = {
            'is_private_school': is_private,
            'tuition_monthly_eur': None, 'tuition_annual_eur': None,
            'registration_fee_eur': None, 'material_fee_annual_eur': None,
            'meal_plan_monthly_eur': None, 'after_school_care_monthly_eur': None,
            'scholarship_available': None, 'income_based_tuition': None,
            'tuition_notes': "No website" if is_private else "Public school - tuition-free",
            'tuition_source_url': None
        }
        return result

    # Scrape website - deep scrape for private, basic for public
    logger.info(f"Scraping {website} ({'private - deep' if is_private else 'public - basic'})")
    content = scraper.scrape_school(website, is_private=is_private)

    # Track what was scraped
    result['scraped_pages'] = [k for k, v in content.items() if v and k != 'pdf_links']
    result['pdf_links_found'] = content.get('pdf_links', [])

    # Build prompt based on school type
    if is_private:
        # Format PDF links for the prompt
        pdf_links_text = "None found"
        if content.get('pdf_links'):
            pdf_links_text = "\n".join([
                f"- {pdf['text']}: {pdf['url']} (likely fee-related: {pdf['category'] == 'fees'})"
                for pdf in content['pdf_links'][:10]  # Limit to 10
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
            pdf_links=pdf_links_text
        )
    else:
        # Public school - simpler prompt, no fees search
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

    # Analyze with LLM
    try:
        logger.info(f"Analyzing {school_info['schulname']} ({'private' if is_private else 'public'})")
        description = analyzer.analyze(prompt)
        result['description'] = description
        result['tuition_data'] = extract_tuition_json(description, is_private)
    except Exception as e:
        logger.error(f"Error analyzing {school_info['schulname']}: {e}")
        result['error'] = str(e)
        result['tuition_data'] = {
            'is_private_school': is_private,
            'tuition_monthly_eur': None, 'tuition_annual_eur': None,
            'registration_fee_eur': None, 'material_fee_annual_eur': None,
            'meal_plan_monthly_eur': None, 'after_school_care_monthly_eur': None,
            'scholarship_available': None, 'income_based_tuition': None,
            'tuition_notes': f"Error: {e}" if is_private else "Public school - tuition-free",
            'tuition_source_url': None
        }

    return result


def process_schools(
    input_file: str,
    output_dir: str = "school_descriptions_v2",
    start_index: int = 0,
    limit: Optional[int] = None,
    only_private: bool = False,
    delay_seconds: float = 3.0
) -> pd.DataFrame:
    """Process schools and generate descriptions."""

    config = load_config()
    os.makedirs(output_dir, exist_ok=True)

    scraper = WebScraper()
    analyzer = LLMAnalyzer(config)

    logger.info(f"Loading schools from {input_file}")
    df = pd.read_csv(input_file, encoding='utf-8-sig')

    if only_private:
        df = df[df['traegerschaft'] == 'Privat']
        logger.info(f"Filtered to {len(df)} private schools")

    if limit:
        df = df.iloc[start_index:start_index + limit]
    else:
        df = df.iloc[start_index:]

    logger.info(f"Processing {len(df)} schools")

    results = []

    for idx, row in df.iterrows():
        school_info = row.to_dict()

        safe_filename = re.sub(r'[^\w\-]', '_', school_info['schulname'])[:50]
        output_file = os.path.join(output_dir, f"{safe_filename}.json")

        if os.path.exists(output_file):
            logger.info(f"Skipping {school_info['schulname']} - already processed")
            with open(output_file, 'r', encoding='utf-8') as f:
                result = json.load(f)
        else:
            result = process_school(scraper, analyzer, school_info)

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            time.sleep(delay_seconds)

        result['schulnummer'] = school_info.get('schulnummer', '')
        results.append(result)

    # Create summary with descriptions
    summary_data = []
    for r in results:
        tuition = r.get('tuition_data', {}) or {}
        pdf_links = r.get('pdf_links_found', [])
        fee_pdfs = [p['url'] for p in pdf_links if p.get('category') == 'fees']

        summary_data.append({
            'schulnummer': r.get('schulnummer'),
            'schulname': r.get('school_name'),
            'description': r.get('description'),  # Include full description!
            'description_generated': r.get('description') is not None and r.get('error') is None,
            'scraped_pages': ', '.join(r.get('scraped_pages', [])),
            'fee_pdfs_found': ', '.join(fee_pdfs) if fee_pdfs else None,
            'error': r.get('error'),
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
            'schulnummer', 'description',  # Include description!
            'tuition_monthly_eur', 'tuition_annual_eur',
            'registration_fee_eur', 'material_fee_annual_eur',
            'meal_plan_monthly_eur', 'after_school_care_monthly_eur',
            'scholarship_available', 'income_based_tuition',
            'tuition_notes', 'tuition_source_url'
        ]
        merge_cols = [c for c in merge_cols if c in summary_df.columns]

        df = df.merge(summary_df[merge_cols], on='schulnummer', how='left')

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    df.to_excel(output_file.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    logger.info(f"Saved to {output_file}")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate school descriptions with web scraping (V2)')
    parser.add_argument('--input', '-i', default='combined_schools_with_metadata_msa.csv')
    parser.add_argument('--output-dir', '-o', default='school_descriptions_v2')
    parser.add_argument('--start', '-s', type=int, default=0)
    parser.add_argument('--limit', '-l', type=int, default=None)
    parser.add_argument('--private-only', '-p', action='store_true')
    parser.add_argument('--delay', '-d', type=float, default=3.0)
    parser.add_argument('--merge', '-m', action='store_true')

    args = parser.parse_args()

    if args.merge:
        output_file = args.input.replace('.csv', '_with_descriptions_v2.csv')
        merge_to_master(args.input, args.output_dir, output_file)
    else:
        summary_df = process_schools(
            input_file=args.input,
            output_dir=args.output_dir,
            start_index=args.start,
            limit=args.limit,
            only_private=args.private_only,
            delay_seconds=args.delay
        )

        print("\n" + "="*70)
        print("SCHOOL DESCRIPTION GENERATION V2 SUMMARY")
        print("="*70)
        print(f"Total processed: {len(summary_df)}")
        print(f"Successfully generated: {summary_df['description_generated'].sum()}")
        print(f"Errors: {summary_df['error'].notna().sum()}")

        if 'tuition_monthly_eur' in summary_df.columns:
            with_tuition = summary_df[
                summary_df['tuition_monthly_eur'].notna() |
                summary_df['tuition_annual_eur'].notna()
            ]
            print(f"Schools with tuition data: {len(with_tuition)}")

        print(f"\nOutput: {args.output_dir}")
        print("="*70)


if __name__ == "__main__":
    main()
