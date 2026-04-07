#!/usr/bin/env python3
"""
Phase 6: Munich Website Metadata & Descriptions Enrichment

Scrapes school websites for metadata and generates bilingual descriptions
using Gemini API with Google Search grounding.

Adapted from NRW pipeline (scripts_nrw/enrichment/nrw_website_metadata_enrichment.py).

Input: data_munich/intermediate/munich_secondary_schools_with_pois.csv
       (fallback chain: earlier intermediate files)
Output: data_munich/intermediate/munich_secondary_schools_with_metadata.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import pandas as pd
import requests
import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_AI_API_KEY')
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Munich school data pipeline, educational project)',
}


def fetch_website_content(url, timeout=15):
    """Fetch and extract text content from a school website."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        from html.parser import HTMLParser

        class TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.texts = []
                self.skip = False

            def handle_starttag(self, tag, attrs):
                if tag in ('script', 'style', 'nav', 'footer', 'header'):
                    self.skip = True

            def handle_endtag(self, tag):
                if tag in ('script', 'style', 'nav', 'footer', 'header'):
                    self.skip = False

            def handle_data(self, data):
                if not self.skip:
                    text = data.strip()
                    if text:
                        self.texts.append(text)

        parser = TextExtractor()
        parser.feed(resp.text)
        return ' '.join(parser.texts)[:5000]
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None


def generate_description(school_name, school_type, website_text, city="München"):
    """Generate bilingual description using Gemini API."""
    if not GEMINI_API_KEY:
        return None, None

    prompt = f"""Generate a concise school description (2-3 sentences each) in German and English for:
School: {school_name}
Type: {school_type}
City: {city}, Bayern

Website content excerpt: {website_text[:2000] if website_text else 'No website content available'}

Return JSON: {{"description_de": "...", "description_en": "..."}}
Focus on: academic profile, special programs, languages offered, notable features."""

    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048}},
            timeout=30,
        )
        resp.raise_for_status()
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        # Try to parse JSON from response
        text = text.strip()
        if text.startswith('```'):
            text = text.split('\n', 1)[1].rsplit('```', 1)[0]
        data = json.loads(text)
        return data.get("description_de"), data.get("description_en")
    except Exception as e:
        logger.debug(f"Gemini API error: {e}")
        return None, None


def find_input_file(school_type='secondary'):
    candidates = [
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_pois.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_crime.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_transit.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_traffic.csv",
        INTERMEDIATE_DIR / f"munich_{school_type}_schools.csv",
    ]
    for f in candidates:
        if f.exists():
            return f
    raise FileNotFoundError(f"No {school_type} school data found. Run earlier phases first.")


def enrich_schools(school_type='secondary'):
    logger.info(f"Enriching {school_type} schools with website metadata...")

    input_file = find_input_file(school_type)
    df = pd.read_csv(input_file, dtype=str)
    logger.info(f"Loaded {len(df)} schools from {input_file.name}")

    # Initialize columns
    for col in ['website_content_summary', 'description_de', 'description_en',
                'school_programs', 'languages_offered']:
        if col not in df.columns:
            df[col] = None

    # Load description cache (separate per school type)
    desc_cache_file = CACHE_DIR / f"description_cache_{school_type}.json"
    # Also check legacy cache file for backward compat
    legacy_cache_file = CACHE_DIR / "description_cache.json"
    desc_cache = {}
    if desc_cache_file.exists():
        with open(desc_cache_file) as f:
            desc_cache = json.load(f)
    elif school_type == 'secondary' and legacy_cache_file.exists():
        with open(legacy_cache_file) as f:
            desc_cache = json.load(f)

    processed = 0
    for idx, row in df.iterrows():
        schulnummer = str(row.get('schulnummer', idx))
        if schulnummer in desc_cache:
            cached = desc_cache[schulnummer]
            df.at[idx, 'description_de'] = cached.get('de')
            df.at[idx, 'description_en'] = cached.get('en')
            processed += 1
            continue

        website = str(row.get('website', '')).strip()
        website_text = None
        if website and website not in ('nan', 'None', ''):
            website_text = fetch_website_content(website)
            if website_text:
                df.at[idx, 'website_content_summary'] = website_text[:500]

        school_name = str(row.get('schulname', '')).strip()
        school_type_str = str(row.get('school_type', row.get('schulart', ''))).strip()

        if not school_name or school_name == 'nan':
            continue

        desc_de, desc_en = generate_description(school_name, school_type_str, website_text)
        if desc_de:
            df.at[idx, 'description_de'] = desc_de
            df.at[idx, 'description_en'] = desc_en
            desc_cache[schulnummer] = {'de': desc_de, 'en': desc_en}
            processed += 1

        time.sleep(0.5)

        if processed % 20 == 0:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(desc_cache_file, 'w') as f:
                json.dump(desc_cache, f, ensure_ascii=False)
            logger.info(f"  Processed {processed}/{len(df)} descriptions")

    # Save cache
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(desc_cache_file, 'w') as f:
        json.dump(desc_cache, f, ensure_ascii=False)

    output_path = INTERMEDIATE_DIR / f"munich_{school_type}_schools_with_metadata.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path}")

    desc_count = df['description_de'].notna().sum()
    print(f"\nWebsite metadata enrichment ({school_type}): {desc_count}/{len(df)} schools with descriptions")

    return df


def main(school_type='secondary'):
    logger.info("=" * 60)
    logger.info(f"Phase 6: Munich Website Metadata & Descriptions ({school_type})")
    logger.info("=" * 60)
    return enrich_schools(school_type)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
