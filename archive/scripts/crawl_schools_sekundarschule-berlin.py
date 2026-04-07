#!/usr/bin/env python3
"""
School Information Crawler for RAG System
Crawls sekundarschulen-berlin.de using sitemap and stores content in Google Cloud Storage
"""

import asyncio
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse
import hashlib
import os

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from google.cloud import storage
import aiohttp
from tqdm.asyncio import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SchoolDataCrawler:
    """Crawls school information and stores in Google Cloud Storage"""

    def __init__(
        self,
        sitemap_url: str,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        max_concurrent: int = 5,
        archive_run_timestamp: Optional[str] = None
    ):
        self.sitemap_url = sitemap_url
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.max_concurrent = max_concurrent
        self.storage_client = None
        self.bucket = None
        self.progress_bar = None
        self.successful_crawls = 0
        self.failed_crawls = 0

        # Create timestamped archive folder
        self.run_timestamp = archive_run_timestamp or datetime.now().strftime('%Y%m%d_%H%M%S')
        self.archive_dir = Path("data_archive") / f"sekundarschulen_{self.run_timestamp}"
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Archive directory created: {self.archive_dir}")

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
            logger.error(f"Failed to initialize GCS: {e}")
            raise

    async def fetch_sitemap(self) -> List[Dict[str, str]]:
        """Fetch and parse sitemap XML"""
        logger.info(f"Fetching sitemap from {self.sitemap_url}")

        async with aiohttp.ClientSession() as session:
            async with session.get(self.sitemap_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch sitemap: {response.status}")

                content = await response.text()

        # Parse XML
        root = ET.fromstring(content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        urls = []
        for url_element in root.findall('ns:url', namespace):
            loc = url_element.find('ns:loc', namespace)
            lastmod = url_element.find('ns:lastmod', namespace)
            changefreq = url_element.find('ns:changefreq', namespace)
            priority = url_element.find('ns:priority', namespace)

            if loc is not None:
                urls.append({
                    'url': loc.text,
                    'lastmod': lastmod.text if lastmod is not None else None,
                    'changefreq': changefreq.text if changefreq is not None else None,
                    'priority': priority.text if priority is not None else None
                })

        logger.info(f"Found {len(urls)} URLs in sitemap")
        return urls

    def filter_school_urls(self, urls: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter URLs to focus on school-related content"""
        # Patterns to exclude
        exclude_patterns = [
            '/news/',
            '/tag-der-offenen-tuer/',
            '/abitur/',
            '/sitemap.xml',
            '/impressum',
            '/datenschutz',
            '/kontakt',
        ]

        # Patterns to prioritize (schools and districts)
        filtered_urls = []
        for url_data in urls:
            url = url_data['url']
            path = urlparse(url).path

            # Skip excluded patterns
            if any(pattern in path for pattern in exclude_patterns):
                continue

            # Include everything else (school pages, districts, rankings, etc.)
            filtered_urls.append(url_data)

        logger.info(f"Filtered to {len(filtered_urls)} school-related URLs")
        return filtered_urls

    def generate_blob_name(self, url: str) -> str:
        """Generate a unique blob name for the URL"""
        # Use URL path for structure
        parsed = urlparse(url)
        path = parsed.path.strip('/')

        if not path or path == '':
            path = 'homepage'

        # Create timestamp for versioning
        timestamp = datetime.now().strftime('%Y%m%d')

        # Create hash of URL for uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

        return f"schools/{path}/{timestamp}_{url_hash}.json"

    async def crawl_url(self, crawler: AsyncWebCrawler, url_data: Dict[str, str]) -> Optional[Dict]:
        """Crawl a single URL and extract content"""
        url = url_data['url']
        logger.info(f"Crawling: {url}")

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

            # Extract and structure data
            markdown_content = ''
            if hasattr(result, 'markdown') and result.markdown:
                markdown_content = result.markdown.raw_markdown if hasattr(result.markdown, 'raw_markdown') else str(result.markdown)

            data = {
                'url': url,
                'title': result.metadata.get('title', '') if result.metadata else '',
                'description': result.metadata.get('description', '') if result.metadata else '',
                'keywords': result.metadata.get('keywords', '') if result.metadata else '',
                'markdown': markdown_content,
                'html': result.html if hasattr(result, 'html') else '',
                'links': result.links.get('internal', []) if hasattr(result, 'links') and result.links else [],
                'metadata': {
                    'lastmod': url_data.get('lastmod'),
                    'changefreq': url_data.get('changefreq'),
                    'priority': url_data.get('priority'),
                    'crawled_at': datetime.now().isoformat(),
                    'success': result.success,
                },
                'extracted_content': {
                    'text': markdown_content if markdown_content else (result.cleaned_html if hasattr(result, 'cleaned_html') else ''),
                }
            }

            return data

        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return None

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

    def upload_to_gcs(self, blob_name: str, data: Dict, archive_blob_name: str = None) -> bool:
        """Upload crawled data to Google Cloud Storage"""
        try:
            # Upload to main location
            blob = self.bucket.blob(blob_name)

            # Convert to JSON
            json_data = json.dumps(data, ensure_ascii=False, indent=2)

            # Upload with metadata
            blob.upload_from_string(
                json_data,
                content_type='application/json'
            )

            logger.info(f"Uploaded to GCS: {blob_name}")

            # Also upload to archive if archive_blob_name is provided
            if archive_blob_name:
                archive_blob = self.bucket.blob(archive_blob_name)
                archive_blob.upload_from_string(
                    json_data,
                    content_type='application/json'
                )

            return True

        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {e}")
            return False

    async def crawl_and_store(self, url_data: Dict[str, str], crawler: AsyncWebCrawler, semaphore: asyncio.Semaphore):
        """Crawl a URL and store it in GCS"""
        async with semaphore:
            data = await self.crawl_url(crawler, url_data)

            if data:
                blob_name = self.generate_blob_name(url_data['url'])
                filename = blob_name.replace('/', '_')

                # Save to local archive
                self.save_to_archive(filename, data)

                # Generate archive blob name for GCS
                archive_blob_name = f"data_archive/{self.run_timestamp}/{blob_name}"

                success = self.upload_to_gcs(blob_name, data, archive_blob_name)
                if success:
                    self.successful_crawls += 1
                else:
                    self.failed_crawls += 1
            else:
                self.failed_crawls += 1

            # Update progress bar
            if self.progress_bar:
                self.progress_bar.update(1)
                self.progress_bar.set_postfix({
                    'success': self.successful_crawls,
                    'failed': self.failed_crawls
                })

    async def run(self):
        """Main crawler execution"""
        logger.info("Starting school data crawler")

        # Initialize GCS
        self.initialize_gcs()

        # Fetch sitemap
        urls = await self.fetch_sitemap()

        # Filter for school-related content
        school_urls = self.filter_school_urls(urls)

        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(self.max_concurrent)

        # Initialize progress bar
        self.progress_bar = tqdm(
            total=len(school_urls),
            desc="Crawling schools",
            unit="page",
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}'
        )

        # Crawl all URLs
        async with AsyncWebCrawler(verbose=False) as crawler:
            tasks = [
                self.crawl_and_store(url_data, crawler, semaphore)
                for url_data in school_urls
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

        # Close progress bar
        self.progress_bar.close()

        logger.info(f"Crawling completed! Success: {self.successful_crawls}, Failed: {self.failed_crawls}")

        # Create manifest file
        manifest = {
            'crawl_date': datetime.now().isoformat(),
            'run_timestamp': self.run_timestamp,
            'total_urls': len(school_urls),
            'successful_crawls': self.successful_crawls,
            'failed_crawls': self.failed_crawls,
            'sitemap_url': self.sitemap_url,
            'urls': school_urls
        }

        manifest_json = json.dumps(manifest, ensure_ascii=False, indent=2)

        # Save manifest to local archive
        archive_manifest_path = self.archive_dir / 'manifest.json'
        with open(archive_manifest_path, 'w', encoding='utf-8') as f:
            f.write(manifest_json)
        logger.info(f"Manifest saved to archive: {archive_manifest_path}")

        # Upload main manifest to GCS
        manifest_blob = self.bucket.blob('schools/manifest.json')
        manifest_blob.upload_from_string(
            manifest_json,
            content_type='application/json'
        )
        logger.info("Manifest uploaded to GCS")

        # Upload archive manifest to GCS
        archive_manifest_blob = self.bucket.blob(f'data_archive/{self.run_timestamp}/schools/manifest.json')
        archive_manifest_blob.upload_from_string(
            manifest_json,
            content_type='application/json'
        )
        logger.info(f"Archive manifest uploaded to GCS: data_archive/{self.run_timestamp}/schools/manifest.json")


async def main():
    """Main entry point"""
    # Set up Google Cloud credentials
    credentials_path = os.path.join(os.path.dirname(__file__), "schoolnossa-e1698305cfcb.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    # Configuration
    SITEMAP_URL = "https://www.sekundarschulen-berlin.de/sitemap.xml"
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    MAX_CONCURRENT = 5  # Adjust based on your needs

    crawler = SchoolDataCrawler(
        sitemap_url=SITEMAP_URL,
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        max_concurrent=MAX_CONCURRENT
    )

    await crawler.run()

    logger.info(f"Data archived in: {crawler.archive_dir}")


if __name__ == "__main__":
    asyncio.run(main())
