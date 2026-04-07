#!/usr/bin/env python3
"""
Kita Detail Page Crawler
Reads existing page JSON files and crawls all kita detail pages
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from google.cloud import storage
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm as async_tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KitaDetailCrawler:
    """Crawls kita detail pages from existing archive data"""

    def __init__(
        self,
        archive_dir: str,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        max_concurrent: int = 5,
        local_backup: bool = True
    ):
        self.archive_dir = Path(archive_dir)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.max_concurrent = max_concurrent
        self.local_backup = local_backup
        self.storage_client = None
        self.bucket = None
        self.crawled_urls: Set[str] = set()

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create output directories
        if self.local_backup:
            self.backup_dir = Path("crawled_kitas") / "details"
            self.backup_dir.mkdir(parents=True, exist_ok=True)

        # Create archive directory for detail pages
        self.detail_archive_dir = Path("data_archive") / f"kita_details_{self.run_timestamp}"
        self.detail_archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Detail archive directory created: {self.detail_archive_dir}")

    def initialize_gcs(self):
        """Initialize Google Cloud Storage client"""
        try:
            if self.gcs_project_id:
                self.storage_client = storage.Client(project=self.gcs_project_id)
            else:
                self.storage_client = storage.Client()

            self.bucket = self.storage_client.bucket(self.gcs_bucket_name)
            logger.info(f"Initialized GCS bucket: {self.gcs_bucket_name}")
        except Exception as e:
            logger.warning(f"GCS initialization failed: {e}. Will use local backup only.")
            self.bucket = None

    def load_kita_urls_from_archive(self) -> List[Dict[str, str]]:
        """Load all kita detail URLs from archived JSON files"""
        logger.info(f"Loading kita URLs from archive: {self.archive_dir}")

        all_kita_urls = []
        seen_ids = set()

        # Find all JSON files in the archive
        json_files = list(self.archive_dir.glob("*.json"))

        logger.info(f"Found {len(json_files)} JSON files to process")

        for json_file in json_files:
            if json_file.name == 'manifest.json':
                continue

            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    page_data = json.load(f)

                # Extract kitas from page data
                kitas = page_data.get('kitas', [])

                for kita in kitas:
                    if 'detail_url' in kita and kita['detail_url']:
                        detail_url = kita['detail_url']

                        # Extract kita ID from URL
                        kita_id = kita.get('kita_id', 'unknown')
                        if match := re.search(r'/einrichtungen/(\d+)', detail_url):
                            kita_id = match.group(1)

                        # Skip if this is a search page URL or doesn't have a valid kita ID
                        if '/einrichtungen/' in detail_url and kita_id != 'unknown' and kita_id not in seen_ids:
                            seen_ids.add(kita_id)
                            all_kita_urls.append({
                                'url': detail_url,
                                'kita_id': kita_id,
                                'kita_name': kita.get('name', 'Unknown')
                            })

            except Exception as e:
                logger.error(f"Error loading {json_file}: {e}")

        logger.info(f"Loaded {len(all_kita_urls)} unique kita detail URLs")
        return all_kita_urls

    async def crawl_kita_detail(
        self,
        crawler: AsyncWebCrawler,
        kita_data: Dict[str, str]
    ) -> Optional[Dict]:
        """Crawl a single kita detail page"""
        url = kita_data['url']

        if url in self.crawled_urls:
            logger.debug(f"Already crawled: {url}")
            return None

        logger.info(f"Crawling kita: {kita_data.get('kita_name', 'Unknown')} ({kita_data.get('kita_id', 'Unknown')})")

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

            # Parse HTML to extract structured data
            soup = BeautifulSoup(result.html, 'html.parser')

            # Extract address
            address = ''
            address_elem = soup.find(class_=re.compile(r'(address|adresse)', re.I))
            if address_elem:
                address = address_elem.get_text(strip=True)

            # Extract contact information
            contact_info = {}
            contact_elem = soup.find(class_=re.compile(r'(contact|kontakt)', re.I))
            if contact_elem:
                contact_info['text'] = contact_elem.get_text(strip=True)

            # Extract opening hours
            opening_hours = ''
            hours_elem = soup.find(string=re.compile(r'öffnungszeit', re.I))
            if hours_elem:
                opening_hours = hours_elem.parent.get_text(strip=True)

            # Extract all table data
            tables_data = []
            for table in soup.find_all('table'):
                table_content = []
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        table_content.append([cell.get_text(strip=True) for cell in cells])
                if table_content:
                    tables_data.append(table_content)

            # Build data structure
            data = {
                'kita_id': kita_data['kita_id'],
                'kita_name': kita_data.get('kita_name', ''),
                'url': url,
                'title': result.metadata.get('title', '') if result.metadata else '',
                'description': result.metadata.get('description', '') if result.metadata else '',
                'keywords': result.metadata.get('keywords', '') if result.metadata else '',
                'address': address,
                'contact': contact_info,
                'opening_hours': opening_hours,
                'markdown': result.markdown.raw_markdown if (hasattr(result, 'markdown') and result.markdown) else '',
                'html': result.html,
                'cleaned_text': result.cleaned_html if hasattr(result, 'cleaned_html') else '',
                'tables': tables_data,
                'links': {
                    'internal': result.links.get('internal', []) if result.links else [],
                    'external': result.links.get('external', []) if result.links else []
                },
                'metadata': {
                    'crawled_at': datetime.now().isoformat(),
                    'success': result.success,
                    'source': 'kita-navigator.berlin.de'
                }
            }

            self.crawled_urls.add(url)
            return data

        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return None

    def save_local_backup(self, filename: str, data: Dict) -> bool:
        """Save data to local filesystem as backup"""
        if not self.local_backup:
            return False

        try:
            filepath = self.backup_dir / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            return True
        except Exception as e:
            logger.error(f"Failed to save local backup {filename}: {e}")
            return False

    def save_to_archive(self, filename: str, data: Dict) -> bool:
        """Save data to timestamped archive directory"""
        try:
            filepath = self.detail_archive_dir / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            return True
        except Exception as e:
            logger.error(f"Failed to save to archive {filename}: {e}")
            return False

    def upload_to_gcs(self, blob_name: str, data: Dict) -> bool:
        """Upload crawled data to Google Cloud Storage"""
        if not self.bucket:
            return False

        try:
            blob = self.bucket.blob(blob_name)
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            blob.upload_from_string(json_data, content_type='application/json')
            return True
        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {e}")
            return False

    async def crawl_and_store_detail(
        self,
        kita_data: Dict[str, str],
        crawler: AsyncWebCrawler,
        semaphore: asyncio.Semaphore
    ):
        """Crawl a kita detail page and store it"""
        async with semaphore:
            data = await self.crawl_kita_detail(crawler, kita_data)

            if data:
                kita_id = kita_data['kita_id']
                timestamp = datetime.now().strftime('%Y%m%d')
                filename = f"kita_{kita_id}_{timestamp}.json"
                blob_name = f"kita_navigator/details/{filename}"

                # Save to archive
                self.save_to_archive(filename, data)

                # Save local backup
                if self.local_backup:
                    self.save_local_backup(filename, data)

                # Upload to GCS
                self.upload_to_gcs(blob_name, data)

                # Small delay to be respectful to the server
                await asyncio.sleep(0.5)

    async def run(self):
        """Main crawler execution"""
        logger.info("Starting Kita Detail Page Crawler")

        # Initialize GCS
        self.initialize_gcs()

        # Load kita URLs from archive
        kita_urls = self.load_kita_urls_from_archive()

        if not kita_urls:
            logger.error("No kita URLs found in archive")
            return

        logger.info(f"Found {len(kita_urls)} kitas to crawl")

        # Create crawler
        async with AsyncWebCrawler(verbose=True) as crawler:
            # Create semaphore for rate limiting
            semaphore = asyncio.Semaphore(self.max_concurrent)

            # Crawl all kita detail pages with progress bar
            tasks = [
                self.crawl_and_store_detail(kita_data, crawler, semaphore)
                for kita_data in kita_urls
            ]

            # Use tqdm for async tasks
            await async_tqdm.gather(*tasks, desc="Crawling kita details", unit="kita")

        logger.info(f"Crawling completed! Total kitas crawled: {len(self.crawled_urls)}")

        # Create manifest file
        manifest = {
            'crawl_date': datetime.now().isoformat(),
            'run_timestamp': self.run_timestamp,
            'source_archive': str(self.archive_dir),
            'total_kitas': len(kita_urls),
            'successfully_crawled': len(self.crawled_urls),
            'failed_crawls': len(kita_urls) - len(self.crawled_urls)
        }

        # Save manifest to archive
        manifest_path = self.detail_archive_dir / 'manifest.json'
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        logger.info(f"Manifest saved to archive: {manifest_path}")

        # Upload manifest to GCS
        if self.bucket:
            manifest_blob = self.bucket.blob('kita_navigator/details/manifest.json')
            manifest_blob.upload_from_string(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            logger.info("Manifest uploaded to GCS")


async def main():
    """Main entry point"""
    # Configuration
    ARCHIVE_DIR = "data_archive/kita_navigator_20251116_175844"  # Use the latest archive
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    MAX_CONCURRENT = 5  # Be respectful to the server
    LOCAL_BACKUP = True  # Save files locally as backup

    crawler = KitaDetailCrawler(
        archive_dir=ARCHIVE_DIR,
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        max_concurrent=MAX_CONCURRENT,
        local_backup=LOCAL_BACKUP
    )

    await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
