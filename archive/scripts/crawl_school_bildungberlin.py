#!/usr/bin/env python3
"""
School Information Crawler for Berlin Education Website
Crawls bildung.berlin.de school directory and stores content in Google Cloud Storage
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse, parse_qs
import hashlib

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from google.cloud import storage
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm as async_tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BildungBerlinCrawler:
    """Crawls Berlin school directory and stores in Google Cloud Storage"""

    def __init__(
        self,
        base_url: str,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        gcs_credentials_path: Optional[str] = None,
        max_concurrent: int = 3,
        local_backup: bool = True
    ):
        self.base_url = base_url
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.gcs_credentials_path = gcs_credentials_path
        self.max_concurrent = max_concurrent
        self.local_backup = local_backup
        self.storage_client = None
        self.bucket = None
        self.crawled_urls: Set[str] = set()

        # Create timestamped archive folder
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.archive_dir = Path("data_archive") / f"bildung_berlin_{self.run_timestamp}"
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Archive directory created: {self.archive_dir}")

        # Create local backup directory if needed
        if self.local_backup:
            self.backup_dir = Path("crawled_schools")
            self.backup_dir.mkdir(exist_ok=True)

    def initialize_gcs(self):
        """Initialize Google Cloud Storage client"""
        try:
            # Initialize storage client with credentials
            if self.gcs_credentials_path:
                from google.oauth2 import service_account
                credentials = service_account.Credentials.from_service_account_file(
                    self.gcs_credentials_path
                )
                self.storage_client = storage.Client(
                    project=self.gcs_project_id,
                    credentials=credentials
                )
            elif self.gcs_project_id:
                self.storage_client = storage.Client(project=self.gcs_project_id)
            else:
                self.storage_client = storage.Client()

            self.bucket = self.storage_client.bucket(self.gcs_bucket_name)
            logger.info(f"Initialized GCS bucket: {self.gcs_bucket_name}")
        except Exception as e:
            logger.warning(f"GCS initialization failed: {e}. Will use local backup only.")
            self.bucket = None

    async def get_school_list_page(self, crawler: AsyncWebCrawler) -> Optional[str]:
        """Fetch the main school list page"""
        logger.info(f"Fetching school list from {self.base_url}")

        try:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=30000,
                delay_before_return_html=3.0,
            )

            result = await crawler.arun(
                url=self.base_url,
                config=config
            )

            if not result.success:
                logger.error(f"Failed to fetch school list: {result.error_message}")
                return None

            return result.html

        except Exception as e:
            logger.error(f"Error fetching school list: {e}")
            return None

    def extract_school_urls(self, html: str) -> List[Dict[str, str]]:
        """Extract all school detail page URLs from the list page"""
        soup = BeautifulSoup(html, 'html.parser')
        school_urls = []

        # Find all links to school portrait pages
        links = soup.find_all('a', href=re.compile(r'Schulportrait\.aspx\?IDSchulzweig='))

        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)

                # Extract school ID from URL and strip any whitespace
                parsed = urlparse(full_url)
                query_params = parse_qs(parsed.query)
                school_id = query_params.get('IDSchulzweig', ['unknown'])[0].strip()

                # Get school name from link text
                school_name = link.get_text(strip=True)

                # Rebuild URL with cleaned school_id to avoid spaces
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?IDSchulzweig={school_id}"

                school_urls.append({
                    'url': clean_url,
                    'school_id': school_id,
                    'school_name': school_name
                })

        # Remove duplicates based on school_id
        seen = set()
        unique_urls = []
        for school in school_urls:
            if school['school_id'] not in seen:
                seen.add(school['school_id'])
                unique_urls.append(school)

        logger.info(f"Found {len(unique_urls)} unique school URLs")
        return unique_urls

    def generate_blob_name(self, school_data: Dict[str, str]) -> str:
        """Generate a unique blob name for the school"""
        school_id = school_data.get('school_id', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d')

        # Sanitize school name for filename
        school_name = school_data.get('school_name', 'unknown')
        safe_name = re.sub(r'[^\w\s-]', '', school_name).strip()
        safe_name = re.sub(r'[-\s]+', '_', safe_name)[:50]  # Limit length

        return f"bildung_berlin/schools/{school_id}_{safe_name}_{timestamp}.json"

    def is_error_page(self, html: str, title: str) -> bool:
        """Check if the page is an error page"""
        error_indicators = [
            'Hoppla',
            'Da hat etwas nicht geklappt',
            'Fehler aufgetreten',
            'Error',
            'nicht gefunden',
            'not found'
        ]

        # Check title and HTML content
        content_to_check = (title + ' ' + html[:500]).lower()
        return any(indicator.lower() in content_to_check for indicator in error_indicators)

    async def crawl_page(self, crawler: AsyncWebCrawler, url: str) -> Optional[Dict]:
        """Crawl a single page and extract content"""
        try:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=30000,
                delay_before_return_html=2.0,
            )

            result = await crawler.arun(
                url=url,
                config=config
            )

            if not result.success:
                logger.error(f"Failed to crawl {url}: {result.error_message}")
                return None

            # Get title early for error detection
            title = result.metadata.get('title', '') if result.metadata else ''

            # Check if this is an error page
            if self.is_error_page(result.html, title):
                logger.warning(f"Error page detected for {url} - skipping")
                return None

            # Parse HTML to extract structured data
            soup = BeautifulSoup(result.html, 'html.parser')

            # Extract all table data (school information is usually in tables)
            tables_data = []
            for table in soup.find_all('table'):
                table_content = []
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        table_content.append([cell.get_text(strip=True) for cell in cells])
                if table_content:
                    tables_data.append(table_content)

            # Extract markdown content
            markdown_content = ''
            if hasattr(result, 'markdown') and result.markdown:
                markdown_content = result.markdown.raw_markdown if hasattr(result.markdown, 'raw_markdown') else str(result.markdown)

            return {
                'url': url,
                'title': title,
                'description': result.metadata.get('description', '') if result.metadata else '',
                'keywords': result.metadata.get('keywords', '') if result.metadata else '',
                'markdown': markdown_content,
                'html': result.html if hasattr(result, 'html') else '',
                'cleaned_text': result.cleaned_html if hasattr(result, 'cleaned_html') else '',
                'tables': tables_data,
                'links': {
                    'internal': result.links.get('internal', []) if hasattr(result, 'links') and result.links else [],
                    'external': result.links.get('external', []) if hasattr(result, 'links') and result.links else []
                }
            }

        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return None

    def extract_school_tabs(self, html: str, base_url: str, school_id: str) -> List[Dict[str, str]]:
        """Extract all tab/sub-page URLs for a school"""
        soup = BeautifulSoup(html, 'html.parser')
        tabs = []

        # Common tab parameter patterns in bildung.berlin.de
        # Look for links that reference the same school ID with different modes/sections
        links = soup.find_all('a', href=True)

        for link in links:
            href = link.get('href')
            # Check if the link contains the school ID and has query parameters
            if school_id in href and '?' in href:
                full_url = urljoin(base_url, href)
                tab_name = link.get_text(strip=True)

                # Skip if it's the same as the main page
                if full_url != base_url and full_url not in [t['url'] for t in tabs]:
                    tabs.append({
                        'url': full_url,
                        'name': tab_name
                    })

        logger.info(f"Found {len(tabs)} additional tabs/pages for school {school_id}")
        return tabs

    async def crawl_school_detail(
        self,
        crawler: AsyncWebCrawler,
        school_data: Dict[str, str]
    ) -> Optional[Dict]:
        """Crawl a single school detail page and all its sub-pages"""
        url = school_data['url']
        school_id = school_data['school_id']

        if url in self.crawled_urls:
            logger.info(f"Already crawled: {url}")
            return None

        logger.info(f"Crawling school: {school_data['school_name']} ({school_id})")

        # Crawl main page
        main_page_data = await self.crawl_page(crawler, url)

        if not main_page_data:
            return None

        # Extract sub-pages/tabs
        tabs = self.extract_school_tabs(main_page_data['html'], url, school_id)

        # Crawl all tabs
        tabs_data = {}
        for tab in tabs:
            await asyncio.sleep(0.5)  # Small delay between tab requests
            tab_data = await self.crawl_page(crawler, tab['url'])
            if tab_data:
                tabs_data[tab['name']] = tab_data
                logger.info(f"  - Crawled tab: {tab['name']}")

        # Combine all data
        data = {
            'school_id': school_id,
            'school_name': school_data['school_name'],
            'main_url': url,
            'main_page': main_page_data,
            'tabs': tabs_data,
            'metadata': {
                'crawled_at': datetime.now().isoformat(),
                'success': True,
                'source': 'bildung.berlin.de',
                'total_pages_crawled': 1 + len(tabs_data)
            }
        }

        self.crawled_urls.add(url)
        return data

    def save_local_backup(self, filename: str, data: Dict) -> bool:
        """Save data to local filesystem as backup"""
        if not self.local_backup:
            return False

        try:
            filepath = self.backup_dir / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved local backup: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to save local backup {filename}: {e}")
            return False

    def save_to_archive(self, filename: str, data: Dict) -> bool:
        """Save data to timestamped archive directory"""
        try:
            filepath = self.archive_dir / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            return True
        except Exception as e:
            logger.error(f"Failed to save archive {filename}: {e}")
            return False

    def upload_to_gcs(self, blob_name: str, data: Dict) -> bool:
        """Upload crawled data to Google Cloud Storage"""
        if not self.bucket:
            logger.warning("GCS bucket not initialized, skipping upload")
            return False

        try:
            blob = self.bucket.blob(blob_name)

            # Convert to JSON
            json_data = json.dumps(data, ensure_ascii=False, indent=2)

            # Upload with metadata
            blob.upload_from_string(
                json_data,
                content_type='application/json'
            )

            logger.info(f"Uploaded to GCS: {blob_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {e}")
            return False

    async def crawl_and_store(
        self,
        school_data: Dict[str, str],
        crawler: AsyncWebCrawler,
        semaphore: asyncio.Semaphore,
        pbar=None
    ):
        """Crawl a school page and store it"""
        async with semaphore:
            data = await self.crawl_school_detail(crawler, school_data)

            if data:
                blob_name = self.generate_blob_name(school_data)
                filename = blob_name.replace('/', '_')

                # Save to timestamped archive
                self.save_to_archive(filename, data)

                # Save local backup
                if self.local_backup:
                    self.save_local_backup(filename, data)

                # Upload to GCS
                self.upload_to_gcs(blob_name, data)

                # Small delay to be respectful to the server
                await asyncio.sleep(1)

            # Update progress bar
            if pbar:
                pbar.update(1)

    async def run(self):
        """Main crawler execution"""
        logger.info("Starting Berlin school directory crawler")

        # Initialize GCS
        self.initialize_gcs()

        # Create crawler and fetch school list
        async with AsyncWebCrawler(verbose=True) as crawler:
            # Get the main school list page
            html = await self.get_school_list_page(crawler)

            if not html:
                logger.error("Failed to fetch school list page")
                return

            # Extract all school URLs
            school_urls = self.extract_school_urls(html)

            if not school_urls:
                logger.error("No school URLs found")
                return

            logger.info(f"Found {len(school_urls)} schools to crawl")

            # Create semaphore for rate limiting
            semaphore = asyncio.Semaphore(self.max_concurrent)

            # Create progress bar
            print(f"\n{'='*60}")
            print(f"Crawling {len(school_urls)} schools from bildung.berlin.de")
            print(f"{'='*60}\n")

            # Crawl all schools with progress bar
            tasks = [
                self.crawl_and_store(school_data, crawler, semaphore)
                for school_data in school_urls
            ]

            # Use tqdm to track progress
            await async_tqdm.gather(*tasks, desc="Crawling schools", total=len(tasks))

        logger.info(f"Crawling completed! Total schools crawled: {len(self.crawled_urls)}")

        # Create manifest file
        manifest = {
            'crawl_date': datetime.now().isoformat(),
            'run_timestamp': self.run_timestamp,
            'total_schools': len(school_urls),
            'successfully_crawled': len(self.crawled_urls),
            'base_url': self.base_url,
            'schools': school_urls
        }

        # Save manifest to archive
        archive_manifest_path = self.archive_dir / 'manifest.json'
        with open(archive_manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        logger.info(f"Manifest saved to archive: {archive_manifest_path}")

        # Save manifest locally
        if self.local_backup:
            manifest_path = self.backup_dir / 'manifest.json'
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            logger.info(f"Manifest saved locally: {manifest_path}")

        # Upload manifest to GCS
        if self.bucket:
            manifest_blob = self.bucket.blob('bildung_berlin/manifest.json')
            manifest_blob.upload_from_string(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            logger.info("Manifest uploaded to GCS")


async def main():
    """Main entry point"""
    # Configuration
    BASE_URL = "https://www.bildung.berlin.de/schulverzeichnis/SchulListe.aspx"
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    GCS_CREDENTIALS_PATH = "schoolnossa-e1698305cfcb.json"  # Service account key file
    MAX_CONCURRENT = 3  # Be respectful to the server
    LOCAL_BACKUP = True  # Save files locally as backup

    crawler = BildungBerlinCrawler(
        base_url=BASE_URL,
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        gcs_credentials_path=GCS_CREDENTIALS_PATH,
        max_concurrent=MAX_CONCURRENT,
        local_backup=LOCAL_BACKUP
    )

    await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
