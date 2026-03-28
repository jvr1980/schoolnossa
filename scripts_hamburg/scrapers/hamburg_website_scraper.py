#!/usr/bin/env python3
"""
Hamburg School Website Scraper

Uses Playwright to scrape teacher counts (Kollegium) and additional metadata
from individual school websites.

This script:
1. Loads the Hamburg school master table with website URLs
2. Visits each school website using Playwright
3. Searches for teacher/Kollegium information
4. Extracts teacher counts, profiles, and other metadata
5. Saves enriched data back to CSV

Author: Hamburg School Data Pipeline
Created: 2026-02-03
"""

import asyncio
import pandas as pd
import re
import logging
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

try:
    from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Warning: playwright not installed. Run: pip install playwright && playwright install")

try:
    from tqdm.asyncio import tqdm as async_tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_hamburg"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
FINAL_DIR = DATA_DIR / "final"

# Scraper settings
MAX_CONCURRENT = 3  # Concurrent browser pages
PAGE_TIMEOUT_MS = 30000  # 30 seconds per page
NAVIGATION_TIMEOUT_MS = 20000  # 20 seconds for navigation
SAVE_INTERVAL = 10  # Save progress every N schools

# Checkpoint file
CHECKPOINT_FILE = INTERMEDIATE_DIR / "website_scraper_checkpoint.json"


class SchoolWebsiteScraper:
    """Scrapes teacher and metadata from school websites using Playwright."""

    def __init__(self):
        self.browser: Optional[Browser] = None
        self.results: Dict[str, Dict] = {}
        self.errors: List[str] = []

    async def init_browser(self):
        """Initialize Playwright browser."""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=['--disable-dev-shm-usage', '--no-sandbox']
        )
        logger.info("Browser initialized")

    async def close_browser(self):
        """Close Playwright browser."""
        if self.browser:
            await self.browser.close()
            logger.info("Browser closed")

    async def scrape_school_website(self, schulnummer: str, url: str, schulname: str) -> Dict:
        """
        Scrape a single school website for teacher data and metadata.

        Returns dict with:
        - lehrer_anzahl: Number of teachers found
        - lehrer_source: Where the teacher count was found
        - profile: School profiles/specializations
        - additional_info: Any other relevant info
        """
        result = {
            'schulnummer': schulnummer,
            'website_scraped': False,
            'lehrer_anzahl': None,
            'lehrer_source': None,
            'schulprofil_website': None,
            'besonderheiten_website': None,
            'scrape_error': None,
            'scrape_timestamp': datetime.now().isoformat()
        }

        if not url or pd.isna(url):
            result['scrape_error'] = "No URL provided"
            return result

        # Normalize URL
        if not url.startswith('http'):
            url = 'https://' + url

        try:
            context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                viewport={'width': 1280, 'height': 720}
            )
            page = await context.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)

            # Navigate to main page
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=NAVIGATION_TIMEOUT_MS)
                await asyncio.sleep(1)  # Wait for dynamic content
            except PlaywrightTimeout:
                result['scrape_error'] = "Page load timeout"
                await context.close()
                return result
            except Exception as e:
                result['scrape_error'] = f"Navigation error: {str(e)[:100]}"
                await context.close()
                return result

            result['website_scraped'] = True

            # Get page text content
            main_content = await self.get_page_text(page)

            # Try to find teacher count on main page
            teacher_count, teacher_source = self.extract_teacher_count(main_content)

            # If not found, look for Kollegium/Team page
            if teacher_count is None:
                kollegium_url = await self.find_kollegium_link(page, url)
                if kollegium_url:
                    try:
                        await page.goto(kollegium_url, wait_until='domcontentloaded', timeout=NAVIGATION_TIMEOUT_MS)
                        await asyncio.sleep(1)
                        kollegium_content = await self.get_page_text(page)
                        teacher_count, teacher_source = self.extract_teacher_count(kollegium_content)
                        if teacher_count:
                            teacher_source = f"Kollegium page: {teacher_source}"
                    except:
                        pass

            # If still not found, try to count teacher names/photos
            if teacher_count is None:
                teacher_count = await self.count_teacher_elements(page)
                if teacher_count and teacher_count > 5:
                    teacher_source = "Counted from page elements"
                else:
                    teacher_count = None

            result['lehrer_anzahl'] = teacher_count
            result['lehrer_source'] = teacher_source

            # Extract school profiles/specializations
            result['schulprofil_website'] = self.extract_profiles(main_content)

            # Extract any special features mentioned
            result['besonderheiten_website'] = self.extract_special_features(main_content)

            await context.close()

        except Exception as e:
            result['scrape_error'] = f"Scrape error: {str(e)[:100]}"
            logger.warning(f"Error scraping {schulname}: {e}")

        return result

    async def get_page_text(self, page: Page) -> str:
        """Extract text content from page."""
        try:
            # Get body text
            text = await page.evaluate('() => document.body.innerText')
            return text or ""
        except:
            return ""

    def extract_teacher_count(self, text: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Extract teacher count from text using various patterns.

        Returns (count, source_description) or (None, None) if not found.
        """
        if not text:
            return None, None

        text_lower = text.lower()

        # Pattern 1: "XX Lehrkräfte" or "XX Lehrer"
        patterns = [
            r'(\d{1,3})\s*(?:lehrkräfte|lehrer(?:innen)?|lehrerinnen und lehrer|pädagog)',
            r'(?:kollegium|team).*?(\d{1,3})\s*(?:lehrkräfte|lehrer|mitglieder|personen)',
            r'(?:ca\.?|etwa|rund|über|ungefähr)?\s*(\d{1,3})\s*(?:lehrkräfte|lehrer)',
            r'(\d{1,3})\s*(?:kolleginnen und kollegen|mitarbeiter)',
            r'lehrerkollegium.*?(\d{1,3})',
            r'unser team.*?(\d{1,3})',
        ]

        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                # Sanity check: teacher count should be between 10 and 200
                if 10 <= count <= 200:
                    return count, f"Pattern: {pattern[:30]}..."

        # Pattern 2: Look for teacher count in structured data
        # "Lehrkräfte: 45" or similar
        structured_patterns = [
            r'lehrkräfte[:\s]+(\d{1,3})',
            r'lehrer[:\s]+(\d{1,3})',
            r'anzahl.{0,20}lehrer[:\s]+(\d{1,3})',
            r'personal[:\s]+(\d{1,3})',
        ]

        for pattern in structured_patterns:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                if 10 <= count <= 200:
                    return count, f"Structured: {pattern[:30]}..."

        return None, None

    async def find_kollegium_link(self, page: Page, base_url: str) -> Optional[str]:
        """Find link to Kollegium/Team page."""
        try:
            # Look for common link texts
            link_texts = [
                'kollegium', 'team', 'lehrer', 'lehrkräfte',
                'mitarbeiter', 'personal', 'über uns', 'wir'
            ]

            links = await page.evaluate('''() => {
                const anchors = document.querySelectorAll('a[href]');
                return Array.from(anchors).map(a => ({
                    href: a.href,
                    text: a.innerText.toLowerCase().trim()
                }));
            }''')

            for link in links:
                link_text = link.get('text', '')
                href = link.get('href', '')

                for search_text in link_texts:
                    if search_text in link_text or search_text in href.lower():
                        # Make sure it's a valid URL
                        if href.startswith('http') or href.startswith('/'):
                            if href.startswith('/'):
                                href = urljoin(base_url, href)
                            return href

            return None

        except Exception as e:
            logger.debug(f"Error finding Kollegium link: {e}")
            return None

    async def count_teacher_elements(self, page: Page) -> Optional[int]:
        """Count teacher elements (photos, cards, list items) on page."""
        try:
            # Look for teacher cards/items
            count = await page.evaluate('''() => {
                // Try various selectors for teacher listings
                const selectors = [
                    '.teacher', '.lehrer', '.mitarbeiter', '.person', '.team-member',
                    '[class*="teacher"]', '[class*="lehrer"]', '[class*="kolleg"]',
                    '.card', '.profile'
                ];

                let maxCount = 0;
                for (const selector of selectors) {
                    const elements = document.querySelectorAll(selector);
                    if (elements.length > maxCount && elements.length < 200) {
                        maxCount = elements.length;
                    }
                }

                // Also try counting images that might be teacher photos
                const images = document.querySelectorAll('img[alt*="lehrer" i], img[alt*="teacher" i]');
                if (images.length > maxCount) {
                    maxCount = images.length;
                }

                return maxCount;
            }''')

            return count if count > 0 else None

        except:
            return None

    def extract_profiles(self, text: str) -> Optional[str]:
        """Extract school profiles/specializations from text."""
        if not text:
            return None

        profiles = []
        text_lower = text.lower()

        # Look for common profile keywords
        profile_keywords = {
            'mint': 'MINT',
            'musik': 'Musik',
            'sport': 'Sport',
            'kunst': 'Kunst',
            'theater': 'Theater',
            'sprach': 'Sprachen',
            'bilingual': 'Bilingual',
            'digital': 'Digital',
            'umwelt': 'Umwelt',
            'europa': 'Europa',
            'international': 'International',
        }

        for keyword, profile in profile_keywords.items():
            if keyword in text_lower:
                # Check if it's part of a profile description
                pattern = rf'{keyword}[a-zäöü]*[\s\-]*(profil|schwerpunkt|zweig|klasse)'
                if re.search(pattern, text_lower):
                    profiles.append(profile)

        return ', '.join(profiles) if profiles else None

    def extract_special_features(self, text: str) -> Optional[str]:
        """Extract special features/programs from text."""
        if not text:
            return None

        features = []
        text_lower = text.lower()

        # Look for special programs
        feature_patterns = [
            (r'ganztag', 'Ganztagsschule'),
            (r'inklusion', 'Inklusion'),
            (r'begabt', 'Begabtenförderung'),
            (r'förder', 'Förderangebote'),
            (r'ag\s+angebot|arbeitsgemeinschaft', 'AGs'),
            (r'austausch', 'Schüleraustausch'),
            (r'praktikum', 'Praktikumsprogramm'),
            (r'berufsorientierung', 'Berufsorientierung'),
        ]

        for pattern, feature in feature_patterns:
            if re.search(pattern, text_lower):
                features.append(feature)

        return ', '.join(features[:5]) if features else None


async def scrape_all_schools(df: pd.DataFrame, max_schools: Optional[int] = None) -> pd.DataFrame:
    """Scrape all schools and return enriched DataFrame."""

    scraper = SchoolWebsiteScraper()
    await scraper.init_browser()

    # Filter schools with websites
    schools_with_urls = df[df['schul_homepage'].notna()].copy()

    if max_schools:
        schools_with_urls = schools_with_urls.head(max_schools)

    total = len(schools_with_urls)
    logger.info(f"Scraping {total} school websites...")

    # Load checkpoint if exists
    processed = set()
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                processed = set(checkpoint.get('processed', []))
                logger.info(f"Resuming from checkpoint: {len(processed)} already processed")
        except:
            pass

    results = []

    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def scrape_with_semaphore(row):
        async with semaphore:
            schulnummer = str(row.get('schulnummer', ''))
            if schulnummer in processed:
                return None

            result = await scraper.scrape_school_website(
                schulnummer=schulnummer,
                url=row.get('schul_homepage'),
                schulname=row.get('schulname', '')
            )
            return result

    # Process schools
    tasks = [scrape_with_semaphore(row) for _, row in schools_with_urls.iterrows()]

    if TQDM_AVAILABLE:
        for i, coro in enumerate(async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping")):
            result = await coro
            if result:
                results.append(result)
                processed.add(result['schulnummer'])

                # Save checkpoint
                if len(results) % SAVE_INTERVAL == 0:
                    with open(CHECKPOINT_FILE, 'w') as f:
                        json.dump({'processed': list(processed)}, f)
    else:
        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            if result:
                results.append(result)
                processed.add(result['schulnummer'])

            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i + 1}/{total}")

    await scraper.close_browser()

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    # Merge with original data
    if not results_df.empty:
        results_df['schulnummer'] = results_df['schulnummer'].astype(str)
        df['schulnummer'] = df['schulnummer'].astype(str)

        # Merge results
        enriched_df = df.merge(
            results_df[['schulnummer', 'lehrer_anzahl', 'lehrer_source',
                       'schulprofil_website', 'besonderheiten_website', 'scrape_error']],
            on='schulnummer',
            how='left'
        )

        # Count successes
        teacher_count = enriched_df['lehrer_anzahl'].notna().sum()
        logger.info(f"Found teacher counts for {teacher_count}/{total} schools")

        return enriched_df

    return df


def main():
    """Main function to scrape school websites."""
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright not available. Install with: pip install playwright && playwright install")
        return

    logger.info("="*60)
    logger.info("Hamburg School Website Scraper")
    logger.info("="*60)

    # Load school data
    input_file = FINAL_DIR / "hamburg_school_master_table_final.csv"
    if not input_file.exists():
        input_file = FINAL_DIR / "hamburg_school_master_table.csv"

    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return

    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} schools from {input_file}")

    # Run scraper
    enriched_df = asyncio.run(scrape_all_schools(df, max_schools=None))

    # Save results
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    output_file = INTERMEDIATE_DIR / "hamburg_schools_with_website_data.csv"
    enriched_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_file}")

    # Clean up checkpoint
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    # Print summary
    print("\n" + "="*60)
    print("SCRAPING SUMMARY")
    print("="*60)

    if 'lehrer_anzahl' in enriched_df.columns:
        teacher_found = enriched_df['lehrer_anzahl'].notna().sum()
        print(f"Teacher counts found: {teacher_found}/{len(enriched_df)}")

        if teacher_found > 0:
            avg_teachers = enriched_df['lehrer_anzahl'].mean()
            print(f"Average teachers per school: {avg_teachers:.1f}")

    if 'schulprofil_website' in enriched_df.columns:
        profiles_found = enriched_df['schulprofil_website'].notna().sum()
        print(f"School profiles found: {profiles_found}/{len(enriched_df)}")

    if 'scrape_error' in enriched_df.columns:
        errors = enriched_df['scrape_error'].notna().sum()
        print(f"Scraping errors: {errors}/{len(enriched_df)}")

    print("="*60)


if __name__ == "__main__":
    main()
