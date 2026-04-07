#!/usr/bin/env python3
"""
Kita Information Crawler for Kita-Navigator Berlin
Crawls kita-navigator.berlin.de and stores content in Google Cloud Storage
"""

import asyncio
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from google.cloud import storage
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm as async_tqdm
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KitaNavigatorCrawler:
    """Crawls Kita-Navigator Berlin and stores in Google Cloud Storage"""

    def __init__(
        self,
        base_url: str,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        max_concurrent: int = 3,
        local_backup: bool = True,
        max_pages: Optional[int] = None,
        enable_archive: bool = True
    ):
        self.base_url = base_url
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.max_concurrent = max_concurrent
        self.local_backup = local_backup
        self.max_pages = max_pages
        self.enable_archive = enable_archive
        self.storage_client = None
        self.bucket = None
        self.crawled_urls: Set[str] = set()
        self.crawled_kitas: List[Dict] = []

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create local backup directory if needed
        if self.local_backup:
            self.backup_dir = Path("crawled_kitas")
            self.backup_dir.mkdir(exist_ok=True)

        # Create archive directory if enabled
        if self.enable_archive:
            self.archive_base_dir = Path("data_archive")
            self.archive_base_dir.mkdir(exist_ok=True)
            self.archive_dir = self.archive_base_dir / f"kita_navigator_{self.run_timestamp}"
            self.archive_dir.mkdir(exist_ok=True, parents=True)
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
            logger.warning(f"GCS initialization failed: {e}. Will use local backup only.")
            self.bucket = None

    def build_search_url(self, page: int = 1, index: int = 0) -> str:
        """Build search URL with pagination parameters"""
        parsed = urlparse(self.base_url)
        query_params = parse_qs(parsed.query)

        # Update page and index parameters
        query_params['seite'] = [str(page)]
        query_params['index'] = [str(index)]

        # Rebuild URL
        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment
        ))

        return new_url

    async def get_kita_list_page(self, crawler: AsyncWebCrawler, page: int = 1) -> Optional[str]:
        """Fetch a page of kita listings"""
        url = self.build_search_url(page=page)
        logger.info(f"Fetching kita list page {page}: {url}")

        try:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=30000,
                delay_before_return_html=3.0,
            )

            result = await crawler.arun(
                url=url,
                config=config
            )

            if not result.success:
                logger.error(f"Failed to fetch kita list page {page}: {result.error_message}")
                return None

            return result.html

        except Exception as e:
            logger.error(f"Error fetching kita list page {page}: {e}")
            return None

    def extract_kita_urls(self, html: str) -> List[Dict[str, str]]:
        """Extract all kita detail page URLs from the list page"""
        soup = BeautifulSoup(html, 'html.parser')
        kita_urls = []

        # Look for links to individual kita detail pages
        # Common patterns: links containing 'einrichtung/', 'detail', or ID parameters
        links = soup.find_all('a', href=True)

        for link in links:
            href = link.get('href')

            # Filter for kita detail pages
            # Adjust these patterns based on actual site structure
            if (href and
                ('einrichtung/' in href.lower() or
                 'detail' in href.lower() or
                 re.search(r'id=\d+', href.lower()))):

                full_url = urljoin(self.base_url, href)

                # Extract kita ID from URL if possible
                kita_id = None
                parsed = urlparse(full_url)
                query_params = parse_qs(parsed.query)

                if 'id' in query_params:
                    kita_id = query_params['id'][0]
                elif match := re.search(r'/einrichtung/(\d+)', full_url):
                    kita_id = match.group(1)

                # Get kita name from link text
                kita_name = link.get_text(strip=True)

                if kita_id:
                    kita_urls.append({
                        'url': full_url,
                        'kita_id': kita_id,
                        'kita_name': kita_name or 'Unknown'
                    })

        # Remove duplicates based on URL
        seen = set()
        unique_urls = []
        for kita in kita_urls:
            if kita['url'] not in seen:
                seen.add(kita['url'])
                unique_urls.append(kita)

        logger.info(f"Found {len(unique_urls)} kita URLs on this page")
        return unique_urls

    def extract_kita_data_from_list_page(self, html: str) -> List[Dict[str, any]]:
        """Extract kita data directly from list page (cards/entries)"""
        soup = BeautifulSoup(html, 'html.parser')
        kitas = []

        # Look for common container patterns for kita entries
        # These selectors will need adjustment based on actual site HTML structure
        possible_containers = [
            soup.find_all('div', class_=re.compile(r'(kita|card|entry|item|result)', re.I)),
            soup.find_all('article'),
            soup.find_all('li', class_=re.compile(r'(kita|entry|item|result)', re.I))
        ]

        for containers in possible_containers:
            for container in containers:
                try:
                    # Extract data from container
                    kita_data = {}

                    # Try to find name
                    name_elem = (
                        container.find(['h1', 'h2', 'h3', 'h4'], class_=re.compile(r'(name|title|heading)', re.I)) or
                        container.find('a', class_=re.compile(r'(name|title)', re.I)) or
                        container.find(['h1', 'h2', 'h3', 'h4'])
                    )
                    if name_elem:
                        kita_data['name'] = name_elem.get_text(strip=True)

                    # Try to find address
                    address_elem = container.find(class_=re.compile(r'(address|adresse|location)', re.I))
                    if address_elem:
                        kita_data['address'] = address_elem.get_text(strip=True)

                    # Try to find detail link
                    link_elem = container.find('a', href=True)
                    if link_elem:
                        href = link_elem.get('href')
                        kita_data['detail_url'] = urljoin(self.base_url, href)

                        # Extract ID from URL
                        if match := re.search(r'id=(\d+)', href):
                            kita_data['kita_id'] = match.group(1)
                        elif match := re.search(r'/einrichtung/(\d+)', href):
                            kita_data['kita_id'] = match.group(1)

                    # Extract all text content for additional info
                    kita_data['full_text'] = container.get_text(separator=' ', strip=True)

                    # Only add if we have at least a name or ID
                    if kita_data.get('name') or kita_data.get('kita_id'):
                        kitas.append(kita_data)

                except Exception as e:
                    logger.debug(f"Error extracting kita data from container: {e}")
                    continue

        logger.info(f"Extracted {len(kitas)} kita entries from page")
        return kitas

    def check_pagination(self, html: str, current_page: int = 1) -> Dict[str, any]:
        """Check if there are more pages and extract pagination info"""
        soup = BeautifulSoup(html, 'html.parser')

        pagination_info = {
            'has_next': False,
            'current_page': current_page,
            'total_pages': None,
            'total_results': None
        }

        # Look for result count - this is the most reliable way to determine total pages
        result_count_elem = soup.find(string=re.compile(r'(\d+)\s*(Ergebnisse|Einrichtungen|Kitas|results)', re.I))
        if result_count_elem:
            if match := re.search(r'(\d+)\s*(Ergebnisse|Einrichtungen|Kitas|results)', result_count_elem, re.I):
                total_results = int(match.group(1))
                pagination_info['total_results'] = total_results

                # Calculate total pages based on results (12 kitas per page)
                results_per_page = 12
                pagination_info['total_pages'] = (total_results + results_per_page - 1) // results_per_page

                # Check if there are more pages
                if current_page < pagination_info['total_pages']:
                    pagination_info['has_next'] = True

                logger.info(f"Found {total_results} total kitas, estimated {pagination_info['total_pages']} pages (current page: {current_page})")

        return pagination_info

    def generate_blob_name(self, kita_data: Dict[str, str], page: int = None) -> str:
        """Generate a unique blob name for the kita"""
        kita_id = kita_data.get('kita_id', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d')

        # Sanitize kita name for filename
        kita_name = kita_data.get('name', kita_data.get('kita_name', 'unknown'))
        safe_name = re.sub(r'[^\w\s-]', '', kita_name).strip()
        safe_name = re.sub(r'[-\s]+', '_', safe_name)[:50]  # Limit length

        if page:
            return f"kita_navigator/pages/page_{page}_{timestamp}.json"
        else:
            return f"kita_navigator/kitas/{kita_id}_{safe_name}_{timestamp}.json"

    async def crawl_kita_detail(
        self,
        crawler: AsyncWebCrawler,
        kita_data: Dict[str, str]
    ) -> Optional[Dict]:
        """Crawl a single kita detail page"""
        url = kita_data['url']

        if url in self.crawled_urls:
            logger.info(f"Already crawled: {url}")
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

            # Extract contact information
            contact_info = {}
            contact_elements = soup.find_all(string=re.compile(r'(telefon|email|phone|mail|kontakt)', re.I))
            for elem in contact_elements:
                parent = elem.parent
                if parent:
                    text = parent.get_text(strip=True)
                    if 'telefon' in text.lower() or 'phone' in text.lower():
                        contact_info['phone'] = text
                    elif 'email' in text.lower() or 'mail' in text.lower():
                        contact_info['email'] = text

            # Extract address
            address_elem = soup.find(class_=re.compile(r'(address|adresse)', re.I))
            address = address_elem.get_text(strip=True) if address_elem else ''

            # Extract opening hours
            hours_elem = soup.find(string=re.compile(r'(öffnungszeiten|opening hours)', re.I))
            opening_hours = hours_elem.parent.get_text(strip=True) if hours_elem and hours_elem.parent else ''

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

            # Extract and structure data
            data = {
                'kita_id': kita_data.get('kita_id', ''),
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

            # Also save to archive if enabled
            if self.enable_archive:
                self.save_to_archive(filename, data)

            return True
        except Exception as e:
            logger.error(f"Failed to save local backup {filename}: {e}")
            return False

    def save_to_archive(self, filename: str, data: Dict) -> bool:
        """Save data to timestamped archive directory"""
        if not self.enable_archive:
            return False

        try:
            archive_filepath = self.archive_dir / filename
            archive_filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(archive_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            return True
        except Exception as e:
            logger.error(f"Failed to save to archive {filename}: {e}")
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
                blob_name = self.generate_blob_name(kita_data)

                # Save local backup
                if self.local_backup:
                    filename = blob_name.replace('/', '_')
                    self.save_local_backup(filename, data)

                # Upload to GCS
                self.upload_to_gcs(blob_name, data)

                self.crawled_kitas.append({
                    'kita_id': kita_data.get('kita_id'),
                    'kita_name': kita_data.get('kita_name'),
                    'url': kita_data.get('url')
                })

                # Small delay to be respectful to the server
                await asyncio.sleep(1)

    async def run(self):
        """Main crawler execution"""
        logger.info("Starting Kita-Navigator Berlin crawler")

        # Initialize GCS
        self.initialize_gcs()

        all_kitas = []
        all_kita_urls = []
        current_page = 1

        async with AsyncWebCrawler(verbose=True) as crawler:
            # First, determine total pages if possible
            logger.info("Fetching first page to determine pagination...")
            first_html = await self.get_kita_list_page(crawler, page=1)

            if not first_html:
                logger.error("Failed to fetch first page")
                return

            pagination_info = self.check_pagination(first_html, current_page=1)
            total_pages = pagination_info.get('total_pages') or self.max_pages or 100  # Default estimate

            if self.max_pages:
                total_pages = min(total_pages, self.max_pages)

            logger.info(f"Expected to crawl up to {total_pages} pages")

            # Create progress bar for pages
            with tqdm(total=total_pages, desc="Crawling pages", unit="page") as pbar:
                while True:
                    # Check if we've reached max pages
                    if self.max_pages and current_page > self.max_pages:
                        logger.info(f"Reached maximum page limit: {self.max_pages}")
                        break

                    # Get the current page (use cached first page if current_page == 1)
                    if current_page == 1:
                        html = first_html
                    else:
                        html = await self.get_kita_list_page(crawler, page=current_page)

                    if not html:
                        logger.error(f"Failed to fetch page {current_page}")
                        break

                    # Extract kitas from the list page
                    page_kitas = self.extract_kita_data_from_list_page(html)

                    # Extract detail URLs from page_kitas data
                    kita_urls = []
                    for kita in page_kitas:
                        if 'detail_url' in kita and kita['detail_url']:
                            # Only add kitas with valid detail URLs and skip duplicates
                            detail_url = kita['detail_url']
                            # Extract kita ID from URL
                            kita_id = kita.get('kita_id', 'unknown')
                            if match := re.search(r'/einrichtungen/(\d+)', detail_url):
                                kita_id = match.group(1)

                            # Skip if this is a search page URL or doesn't have a valid kita ID
                            if '/einrichtungen/' in detail_url and kita_id != 'unknown':
                                kita_urls.append({
                                    'url': detail_url,
                                    'kita_id': kita_id,
                                    'kita_name': kita.get('name', 'Unknown')
                                })

                    # Remove duplicates based on kita_id
                    seen_ids = set()
                    unique_kita_urls = []
                    for kita_url in kita_urls:
                        if kita_url['kita_id'] not in seen_ids:
                            seen_ids.add(kita_url['kita_id'])
                            unique_kita_urls.append(kita_url)

                    all_kita_urls.extend(unique_kita_urls)

                    # Store page data
                    page_data = {
                        'page_number': current_page,
                        'kitas': page_kitas,
                        'kita_urls': unique_kita_urls,
                        'html': html,
                        'crawled_at': datetime.now().isoformat()
                    }

                    # Save page data
                    blob_name = self.generate_blob_name({}, page=current_page)
                    if self.local_backup:
                        filename = blob_name.replace('/', '_')
                        self.save_local_backup(filename, page_data)
                    if self.bucket:
                        self.upload_to_gcs(blob_name, page_data)

                    all_kitas.extend(page_kitas)

                    # Update progress bar
                    pbar.update(1)
                    pbar.set_postfix({
                        'kitas': len(all_kitas),
                        'detail_urls': len(all_kita_urls)
                    })

                    # Check pagination
                    pagination_info = self.check_pagination(html, current_page=current_page)

                    if not pagination_info['has_next']:
                        logger.info(f"No more pages found after page {current_page}")
                        break

                    current_page += 1

                    # Small delay between pages
                    await asyncio.sleep(2)

            logger.info(f"Collected {len(all_kitas)} kitas from {current_page} pages")
            logger.info(f"Found {len(all_kita_urls)} total detail URLs")

            # If we have detail URLs, crawl them
            if all_kita_urls:
                logger.info(f"Starting to crawl {len(all_kita_urls)} kita detail pages...")

                # Create semaphore for rate limiting
                semaphore = asyncio.Semaphore(self.max_concurrent)

                # Crawl all kita detail pages with progress bar
                tasks = [
                    self.crawl_and_store_detail(kita_data, crawler, semaphore)
                    for kita_data in all_kita_urls
                ]

                # Use tqdm for async tasks
                await async_tqdm.gather(*tasks, desc="Crawling detail pages", unit="kita")

        logger.info(f"Crawling completed! Total kitas processed: {len(all_kitas)}")
        logger.info(f"Total detail pages crawled: {len(self.crawled_urls)}")

        # Create manifest file
        manifest = {
            'crawl_date': datetime.now().isoformat(),
            'total_kitas': len(all_kitas),
            'total_pages': current_page,
            'successfully_crawled_details': len(self.crawled_urls),
            'base_url': self.base_url,
            'kitas': all_kitas[:100],  # Store first 100 in manifest to avoid huge files
            'statistics': {
                'pages_processed': current_page,
                'kitas_found': len(all_kitas),
                'detail_pages_crawled': len(self.crawled_urls)
            }
        }

        # Save manifest locally
        if self.local_backup:
            manifest_path = self.backup_dir / 'manifest.json'
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            logger.info(f"Manifest saved locally: {manifest_path}")

            # Also save manifest to archive
            if self.enable_archive:
                archive_manifest_path = self.archive_dir / 'manifest.json'
                with open(archive_manifest_path, 'w', encoding='utf-8') as f:
                    json.dump(manifest, f, ensure_ascii=False, indent=2)
                logger.info(f"Manifest saved to archive: {archive_manifest_path}")

        # Upload manifest to GCS
        if self.bucket:
            manifest_blob = self.bucket.blob('kita_navigator/manifest.json')
            manifest_blob.upload_from_string(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            logger.info("Manifest uploaded to GCS")


async def main():
    """Main entry point"""
    # Configuration
    BASE_URL = "https://kita-navigator.berlin.de/einrichtungen?input=&betb=10-2025&einfacheSuche=true&entfernung=50&seite=1&index=0"
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    MAX_CONCURRENT = 3  # Be respectful to the server
    LOCAL_BACKUP = True  # Save files locally as backup
    MAX_PAGES = None  # Set to a number to limit pages, or None for all pages

    crawler = KitaNavigatorCrawler(
        base_url=BASE_URL,
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        max_concurrent=MAX_CONCURRENT,
        local_backup=LOCAL_BACKUP,
        max_pages=MAX_PAGES
    )

    await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
