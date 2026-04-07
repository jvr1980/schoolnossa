#!/usr/bin/env python3
"""
Generate Rich School Descriptions Using LLM with Web Search

This script generates comprehensive, grounded school descriptions by:
1. Fetching and analyzing the school's website content
2. Using an LLM with web search to gather additional information
3. Structuring the output with clearly parseable sections

Supports multiple LLM providers (configured in config.yaml):
- OpenAI (with web search via Responses API)
- Perplexity (built-in web search)
- Google Gemini (with grounding/search)

Output includes:
- Rich narrative descriptions covering all key dimensions
- Structured JSON for tuition/fees (easily parseable for master table)
- Source URLs for verification

Dimensions covered:
1. Mission and educational philosophy
2. History, community and facilities
3. Curriculum and academic programs
4. Student learning experiences and opportunities
5. School achievements and outcomes
6. Admissions and parent resources (including tuition/fees)
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
from typing import Optional, Dict, Any, List
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('school_descriptions.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Default config path
CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(config_path: Path = CONFIG_PATH) -> Dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Please create config.yaml with your API keys."
        )

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    return config


# Load config at module level
try:
    CONFIG = load_config()
except FileNotFoundError as e:
    logger.warning(f"Config not loaded: {e}")
    CONFIG = {}


DESCRIPTION_PROMPT = """You are an expert education researcher creating comprehensive school profiles for parents considering schools in Berlin, Germany.

Your task is to create a detailed, well-researched description of the following school. You MUST ground your description in actual information from the school's website and other reliable sources. Do NOT make up information.

## School Information:
- Name: {school_name}
- Website: {website}
- Type: {school_type}
- District (Bezirk): {bezirk}
- Address: {address}
- Ownership: {traegerschaft} (Public/Private)

## Instructions:

1. **Search and Research**: Search the web to find information about this school from:
   - The school's official website ({website})
   - Berlin education authority (Senatsverwaltung für Bildung)
   - News articles or reviews about the school
   - Any published school profiles or reports

2. **Write the Description**: Create a comprehensive description covering ALL of the following sections. Each section should be clearly marked with a header.

3. **Be Accurate**: Only include information you can verify from your search. If information is not available for a section, state "Information not publicly available" rather than making assumptions.

4. **Language**: Write in English, but preserve German terms where appropriate (e.g., Abitur, Mittlerer Schulabschluss, Ganztagsschule).

## Required Sections:

### 1. MISSION AND EDUCATIONAL PHILOSOPHY
- School's mission statement (quote if available)
- Educational philosophy and pedagogical approach
- Core values and "Portrait of a Graduate" vision
- What distinguishes this school's educational approach

### 2. HISTORY, COMMUNITY AND FACILITIES
- Founding year and brief history
- Community served (demographics, neighborhood context)
- Campus description and facilities:
  - Classrooms and learning spaces
  - Science labs, computer facilities
  - Library/media center
  - Sports facilities (gym, fields, pool)
  - Arts spaces (music rooms, art studios, theater)
  - Cafeteria/mensa
- Any unique campus features or recent renovations

### 3. CURRICULUM AND ACADEMIC PROGRAMS
- School structure (grades offered: Klasse 7-10, 7-13, etc.)
- Core curriculum overview
- Graduation pathways offered:
  - BBR (Berufsbildungsreife)
  - MSA (Mittlerer Schulabschluss)
  - Abitur (if applicable)
- Languages of instruction
- Foreign languages taught (1st, 2nd, 3rd foreign language options)
- Special academic programs:
  - Bilingual/international programs
  - STEM/MINT focus
  - Arts or music emphasis
  - Vocational training partnerships
- Grading and assessment approach
- Class sizes (if available)

### 4. STUDENT LEARNING EXPERIENCES AND OPPORTUNITIES
- Extracurricular activities and clubs
- Sports teams and athletics programs
- Arts programs (music, theater, visual arts)
- Internship and career orientation programs (Berufsorientierung)
- International exchanges and partnerships
- Service learning and community involvement
- Student government and leadership opportunities
- After-school programs (Ganztagsangebote)

### 5. SCHOOL ACHIEVEMENTS AND OUTCOMES
- Academic performance metrics (if available):
  - Abitur results/averages
  - MSA pass rates
  - Any published rankings
- Awards and recognitions
- Notable alumni (if publicly known)
- University/career placement outcomes
- Special certifications (e.g., MINT-freundliche Schule, Europaschule)

### 6. ADMISSIONS AND PARENT RESOURCES
- Enrollment process and timeline
- Admission requirements or selection criteria
- Application deadlines
- Open house/information events
- **For private schools - TUITION AND FEES (be very specific and search thoroughly):**
  - Monthly tuition amount in EUR
  - Annual tuition amount in EUR
  - Registration/enrollment fees
  - Material fees
  - Meal plan costs
  - After-school care costs
  - Scholarship availability
  - Income-based tuition options
- Contact information
- Parent involvement opportunities
- School policies overview

## Output Format:

After the narrative description, you MUST include a structured JSON block with tuition information. Even for public schools, include this block with is_private_school: false.

The JSON block must be formatted exactly like this, wrapped in ```json code blocks:

```json
{{
    "is_private_school": true/false,
    "tuition_monthly_eur": <number or null>,
    "tuition_annual_eur": <number or null>,
    "registration_fee_eur": <number or null>,
    "material_fee_annual_eur": <number or null>,
    "meal_plan_monthly_eur": <number or null>,
    "after_school_care_monthly_eur": <number or null>,
    "scholarship_available": true/false/null,
    "income_based_tuition": true/false/null,
    "tuition_notes": "<any additional fee details or context>",
    "tuition_source_url": "<URL where you found tuition information>"
}}
```

## Important Notes:
- Cite your sources by mentioning where information came from
- If the website is not accessible or has limited information, note this
- For public schools, tuition fields should be null but note any costs (materials, trips, etc.)
- Be thorough but concise - aim for 800-1200 words for the narrative
- Focus on facts, not marketing language
- ALWAYS include the JSON block at the end
"""


class LLMProvider:
    """Base class for LLM providers with web search."""

    def generate(self, prompt: str) -> str:
        raise NotImplementedError


class OpenAIProvider(LLMProvider):
    """OpenAI provider using the Responses API with web search."""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.openai.com/v1/responses"

    def generate(self, prompt: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Using the Responses API with web_search_preview tool
        data = {
            "model": self.model,
            "tools": [{"type": "web_search_preview"}],
            "input": prompt
        }

        response = requests.post(self.base_url, headers=headers, json=data, timeout=180)
        response.raise_for_status()

        result = response.json()

        # Extract text from response
        output_text = ""
        for item in result.get("output", []):
            if item.get("type") == "message":
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        output_text += content.get("text", "")

        return output_text


class PerplexityProvider(LLMProvider):
    """Perplexity provider with built-in web search."""

    def __init__(self, api_key: str, model: str = "sonar"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.perplexity.ai/chat/completions"

    def generate(self, prompt: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert education researcher. Always cite your sources and be factual. Always include the JSON block at the end of your response."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 8000,
            "temperature": 0.2,
            "search_recency_filter": "year"
        }

        response = requests.post(self.base_url, headers=headers, json=data, timeout=180)
        response.raise_for_status()

        result = response.json()
        return result["choices"][0]["message"]["content"]


class GeminiProvider(LLMProvider):
    """Google Gemini provider with grounding/search."""

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        self.api_key = api_key
        self.model = model
        self.base_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def generate(self, prompt: str) -> str:
        headers = {
            "Content-Type": "application/json"
        }

        # Gemini API with Google Search grounding
        data = {
            "contents": [{
                "parts": [{
                    "text": prompt
                }]
            }],
            "tools": [{
                "google_search": {}
            }],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 8000
            }
        }

        url = f"{self.base_url}?key={self.api_key}"
        response = requests.post(url, headers=headers, json=data, timeout=180)
        response.raise_for_status()

        result = response.json()

        # Extract text from Gemini response
        try:
            candidates = result.get("candidates", [])
            if candidates:
                content = candidates[0].get("content", {})
                parts = content.get("parts", [])
                text_parts = [p.get("text", "") for p in parts if "text" in p]
                return "\n".join(text_parts)
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing Gemini response: {e}")
            logger.debug(f"Response: {result}")

        return ""


def get_provider(provider_name: str = None, config: Dict = None) -> LLMProvider:
    """Get the appropriate LLM provider based on config."""

    if config is None:
        config = CONFIG

    api_keys = config.get("api_keys", {})
    models = config.get("models", {})
    preferences = config.get("provider_preference", ["perplexity", "gemini", "openai"])

    providers_map = {
        "openai": (OpenAIProvider, api_keys.get("openai"), models.get("openai", "gpt-4o-mini")),
        "perplexity": (PerplexityProvider, api_keys.get("perplexity"), models.get("perplexity", "sonar")),
        "gemini": (GeminiProvider, api_keys.get("gemini"), models.get("gemini", "gemini-2.0-flash")),
    }

    if provider_name:
        # Use specified provider
        if provider_name.lower() in providers_map:
            provider_class, api_key, model = providers_map[provider_name.lower()]
            if not api_key:
                raise ValueError(f"API key not found for {provider_name} in config.yaml")
            logger.info(f"Using {provider_name} provider with model {model}")
            return provider_class(api_key, model)
        raise ValueError(f"Unknown provider: {provider_name}")

    # Auto-detect based on preference order
    for pref in preferences:
        if pref in providers_map:
            provider_class, api_key, model = providers_map[pref]
            if api_key:
                logger.info(f"Using {pref} provider with model {model}")
                return provider_class(api_key, model)

    raise ValueError(
        "No valid API key found in config.yaml. "
        "Please add at least one API key (openai, perplexity, or gemini)."
    )


def search_and_generate_description(
    provider: LLMProvider,
    school_name: str,
    website: str,
    school_type: str,
    bezirk: str,
    address: str,
    traegerschaft: str,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Generate a comprehensive school description using LLM with web search.
    """
    result = {
        'school_name': school_name,
        'description': None,
        'tuition_data': None,
        'sources': [],
        'error': None,
        'generated_at': datetime.now().isoformat()
    }

    if not website or pd.isna(website):
        result['error'] = "No website available"
        result['description'] = f"No website available for {school_name}. Limited information can be gathered."
        result['tuition_data'] = {
            'is_private_school': traegerschaft == 'Privat',
            'tuition_monthly_eur': None,
            'tuition_annual_eur': None,
            'registration_fee_eur': None,
            'material_fee_annual_eur': None,
            'meal_plan_monthly_eur': None,
            'after_school_care_monthly_eur': None,
            'scholarship_available': None,
            'income_based_tuition': None,
            'tuition_notes': "No website available for research",
            'tuition_source_url': None
        }
        return result

    prompt = DESCRIPTION_PROMPT.format(
        school_name=school_name,
        website=website,
        school_type=school_type,
        bezirk=bezirk,
        address=address,
        traegerschaft=traegerschaft
    )

    for attempt in range(max_retries):
        try:
            logger.info(f"Generating description for {school_name} (attempt {attempt + 1})")

            full_text = provider.generate(prompt)
            result['description'] = full_text

            # Extract JSON tuition data from response
            tuition_data = extract_tuition_json(full_text, traegerschaft)
            result['tuition_data'] = tuition_data

            # Extract any cited sources
            result['sources'] = extract_sources(full_text)

            logger.info(f"Successfully generated description for {school_name}")
            return result

        except Exception as e:
            logger.error(f"Error generating description for {school_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                result['error'] = str(e)

    return result


def extract_tuition_json(text: str, traegerschaft: str) -> Dict:
    """Extract structured tuition JSON from the response text."""

    default_tuition = {
        'is_private_school': traegerschaft == 'Privat',
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

    # Try to find JSON block in the text
    json_patterns = [
        r'```json\s*(\{[^`]+\})\s*```',
        r'```\s*(\{[^`]+\})\s*```',
        r'\{["\']?is_private_school["\']?\s*:\s*(?:true|false)[^}]+\}',
    ]

    for pattern in json_patterns:
        matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
        for match in matches:
            try:
                json_str = match.strip() if isinstance(match, str) else match
                # Clean up common JSON issues
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                json_str = re.sub(r':\s*null\s*,', ': null,', json_str)

                parsed = json.loads(json_str)

                if 'is_private_school' in parsed:
                    for key in default_tuition:
                        if key not in parsed:
                            parsed[key] = default_tuition[key]
                    return parsed

            except json.JSONDecodeError:
                continue

    # Fallback: try to extract tuition from text
    tuition_data = default_tuition.copy()

    # Look for tuition amounts
    monthly_patterns = [
        r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:€|EUR|Euro)\s*(?:per|pro|\/|a)\s*(?:month|Monat|monthly|monatlich)',
        r'(?:monthly|monatlich|Monatsbeitrag|Schulgeld)[^\d€]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:€|EUR|Euro)',
        r'(\d{1,3}(?:[.,]\d{3})*)\s*(?:€|EUR)\s*(?:mtl|mon)',
    ]

    annual_patterns = [
        r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:€|EUR|Euro)\s*(?:per|pro|\/|a)\s*(?:year|Jahr|annual|jährlich)',
        r'(?:annual|jährlich|Jahresbeitrag)[^\d€]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:€|EUR|Euro)',
    ]

    for pattern in monthly_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                amount_str = match.group(1).replace('.', '').replace(',', '.')
                amount = float(amount_str)
                if 50 < amount < 5000:
                    tuition_data['tuition_monthly_eur'] = amount
                    break
            except:
                pass

    for pattern in annual_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                amount_str = match.group(1).replace('.', '').replace(',', '.')
                amount = float(amount_str)
                if 500 < amount < 50000:
                    tuition_data['tuition_annual_eur'] = amount
                    break
            except:
                pass

    return tuition_data


def extract_sources(text: str) -> List[str]:
    """Extract URLs mentioned in the text as sources."""
    url_pattern = r'https?://[^\s<>"\')\]]+[^\s<>"\')\].,;:]'
    urls = re.findall(url_pattern, text)
    return list(set(urls))


def process_schools(
    input_file: str,
    output_dir: str = "school_descriptions",
    start_index: int = 0,
    limit: Optional[int] = None,
    only_private: bool = False,
    delay_seconds: float = None,
    provider_name: str = None,
    config: Dict = None
) -> pd.DataFrame:
    """
    Process schools and generate descriptions.
    """
    if config is None:
        config = CONFIG

    if delay_seconds is None:
        delay_seconds = config.get("rate_limits", {}).get("delay_between_requests", 2.0)

    os.makedirs(output_dir, exist_ok=True)

    # Get LLM provider
    provider = get_provider(provider_name, config)

    # Load master table
    logger.info(f"Loading schools from {input_file}")
    df = pd.read_csv(input_file, encoding='utf-8-sig')

    if only_private:
        df = df[df['traegerschaft'] == 'Privat']
        logger.info(f"Filtered to {len(df)} private schools")

    if limit:
        df = df.iloc[start_index:start_index + limit]
    else:
        df = df.iloc[start_index:]

    logger.info(f"Processing {len(df)} schools starting from index {start_index}")

    results = []

    for idx, row in df.iterrows():
        school_name = row['schulname']
        website = row.get('website', '')
        school_type = row.get('school_type', 'Unknown')
        bezirk = row.get('bezirk', '')
        address = f"{row.get('strasse', '')}, {row.get('plz', '')} Berlin"
        traegerschaft = row.get('traegerschaft', 'Öffentlich')

        logger.info(f"Processing [{idx}]: {school_name}")

        # Check if already processed
        safe_filename = re.sub(r'[^\w\-]', '_', school_name)[:50]
        output_file = os.path.join(output_dir, f"{safe_filename}.json")

        if os.path.exists(output_file):
            logger.info(f"Skipping {school_name} - already processed")
            with open(output_file, 'r', encoding='utf-8') as f:
                result = json.load(f)
        else:
            result = search_and_generate_description(
                provider=provider,
                school_name=school_name,
                website=website,
                school_type=school_type,
                bezirk=bezirk,
                address=address,
                traegerschaft=traegerschaft,
                max_retries=config.get("rate_limits", {}).get("max_retries", 3)
            )

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            time.sleep(delay_seconds)

        result['schulnummer'] = row.get('schulnummer', '')
        result['original_index'] = idx
        results.append(result)

    # Create summary DataFrame
    summary_data = []
    for r in results:
        tuition = r.get('tuition_data', {}) or {}
        summary_data.append({
            'schulnummer': r.get('schulnummer'),
            'schulname': r.get('school_name'),
            'description_generated': r.get('description') is not None and r.get('error') is None,
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
            'sources': '; '.join(r.get('sources', [])),
            'generated_at': r.get('generated_at')
        })

    summary_df = pd.DataFrame(summary_data)

    summary_file = os.path.join(output_dir, 'descriptions_summary.csv')
    summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
    logger.info(f"Saved summary to {summary_file}")

    return summary_df


def merge_descriptions_to_master(
    master_file: str,
    descriptions_dir: str,
    output_file: str
) -> pd.DataFrame:
    """Merge generated descriptions and tuition data back into the master table."""
    logger.info(f"Merging descriptions into master table")

    df = pd.read_csv(master_file, encoding='utf-8-sig')

    summary_file = os.path.join(descriptions_dir, 'descriptions_summary.csv')
    if os.path.exists(summary_file):
        summary_df = pd.read_csv(summary_file, encoding='utf-8-sig')

        merge_cols = [
            'schulnummer',
            'tuition_monthly_eur',
            'tuition_annual_eur',
            'registration_fee_eur',
            'material_fee_annual_eur',
            'meal_plan_monthly_eur',
            'after_school_care_monthly_eur',
            'scholarship_available',
            'income_based_tuition',
            'tuition_notes',
            'tuition_source_url'
        ]

        merge_cols = [c for c in merge_cols if c in summary_df.columns]

        df = df.merge(
            summary_df[merge_cols],
            on='schulnummer',
            how='left'
        )

        logger.info(f"Merged tuition data for {summary_df['schulnummer'].notna().sum()} schools")

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    df.to_excel(output_file.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    logger.info(f"Saved merged master table to {output_file}")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Generate school descriptions using LLM with web search',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all private schools
  python generate_school_descriptions.py --private-only

  # Process first 5 schools for testing
  python generate_school_descriptions.py --limit 5

  # Resume from school index 50
  python generate_school_descriptions.py --start 50

  # Use specific provider
  python generate_school_descriptions.py --provider perplexity

  # Merge results back to master table
  python generate_school_descriptions.py --merge

Configuration:
  API keys and models are configured in config.yaml
  Default models (cost-effective):
    - OpenAI: gpt-4o-mini
    - Perplexity: sonar
    - Gemini: gemini-2.0-flash
        """
    )
    parser.add_argument('--input', '-i', default='combined_schools_with_metadata_msa.csv',
                        help='Input master table CSV file')
    parser.add_argument('--output-dir', '-o', default='school_descriptions',
                        help='Output directory for description files')
    parser.add_argument('--start', '-s', type=int, default=0,
                        help='Start index for processing')
    parser.add_argument('--limit', '-l', type=int, default=None,
                        help='Limit number of schools to process')
    parser.add_argument('--private-only', '-p', action='store_true',
                        help='Only process private schools')
    parser.add_argument('--delay', '-d', type=float, default=None,
                        help='Delay between API calls in seconds (default from config)')
    parser.add_argument('--provider', type=str, default=None,
                        choices=['perplexity', 'openai', 'gemini'],
                        help='LLM provider to use (default: auto-detect from config)')
    parser.add_argument('--merge', '-m', action='store_true',
                        help='Merge descriptions back into master table')
    parser.add_argument('--config', '-c', type=str, default=None,
                        help='Path to config YAML file (default: config.yaml)')

    args = parser.parse_args()

    # Load config
    config_path = Path(args.config) if args.config else CONFIG_PATH
    try:
        config = load_config(config_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    if args.merge:
        output_file = args.input.replace('.csv', '_with_descriptions.csv')
        merge_descriptions_to_master(args.input, args.output_dir, output_file)
    else:
        summary_df = process_schools(
            input_file=args.input,
            output_dir=args.output_dir,
            start_index=args.start,
            limit=args.limit,
            only_private=args.private_only,
            delay_seconds=args.delay,
            provider_name=args.provider,
            config=config
        )

        print("\n" + "="*70)
        print("SCHOOL DESCRIPTION GENERATION SUMMARY")
        print("="*70)
        print(f"Total schools processed: {len(summary_df)}")
        print(f"Successfully generated: {summary_df['description_generated'].sum()}")
        print(f"Errors: {summary_df['error'].notna().sum()}")

        if args.private_only or summary_df['is_private_school'].any():
            private_with_tuition = summary_df[
                (summary_df['is_private_school'] == True) &
                (summary_df['tuition_monthly_eur'].notna() | summary_df['tuition_annual_eur'].notna())
            ]
            print(f"Private schools with tuition data: {len(private_with_tuition)}")

        print(f"\nOutput directory: {args.output_dir}")
        print("\nTo merge results into master table, run:")
        print(f"  python generate_school_descriptions.py --merge")
        print("="*70)


if __name__ == "__main__":
    main()
