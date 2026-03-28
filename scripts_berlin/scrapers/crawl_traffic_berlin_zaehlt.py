#!/usr/bin/env python3
"""
Berlin Traffic Data Downloader (Berlin Zählt Mobilität)
Downloads car and bicycle traffic data from berlin-zaehlt.de and stores in Google Cloud Storage
"""

import asyncio
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from google.cloud import storage
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


class TrafficDataDownloader:
    """Downloads Berlin traffic counting data and stores in Google Cloud Storage"""

    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        local_backup: bool = True,
        max_concurrent: int = 5,
        enable_archive: bool = True
    ):
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.local_backup = local_backup
        self.max_concurrent = max_concurrent
        self.enable_archive = enable_archive
        self.storage_client = None
        self.bucket = None
        self.base_url = "https://berlin-zaehlt.de/csv/"

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create local backup directory if needed
        if self.local_backup:
            self.backup_dir = Path("traffic_data")
            self.backup_dir.mkdir(exist_ok=True)
            (self.backup_dir / "telraam").mkdir(exist_ok=True)
            (self.backup_dir / "ecocounter").mkdir(exist_ok=True)
            (self.backup_dir / "metadata").mkdir(exist_ok=True)

        # Create archive directory if enabled
        if self.enable_archive:
            self.archive_base_dir = Path("data_archive")
            self.archive_base_dir.mkdir(exist_ok=True)
            self.archive_dir = self.archive_base_dir / f"traffic_data_{self.run_timestamp}"
            self.archive_dir.mkdir(exist_ok=True, parents=True)
            (self.archive_dir / "telraam").mkdir(exist_ok=True)
            (self.archive_dir / "ecocounter").mkdir(exist_ok=True)
            (self.archive_dir / "metadata").mkdir(exist_ok=True)
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

    async def fetch_file_list(self, session: aiohttp.ClientSession) -> List[Dict[str, str]]:
        """Fetch list of available CSV files from the directory listing"""
        logger.info(f"Fetching file list from {self.base_url}")

        try:
            async with session.get(self.base_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    files = []
                    links = soup.find_all('a', href=True)

                    for link in links:
                        href = link.get('href')
                        filename = href.strip('/')

                        # Filter for car traffic (Telraam) and bicycle traffic (Ecocounter) data
                        if filename.startswith('bzm_telraam_') and filename.endswith('.csv.gz'):
                            files.append({
                                'filename': filename,
                                'url': urljoin(self.base_url, href),
                                'type': 'telraam',
                                'category': 'car_traffic',
                                'description': 'Car and vehicle traffic counts (Telraam sensors)'
                            })
                        elif filename.startswith('bzm_ecocounter_') and filename.endswith('.csv.gz'):
                            files.append({
                                'filename': filename,
                                'url': urljoin(self.base_url, href),
                                'type': 'ecocounter',
                                'category': 'bicycle_traffic',
                                'description': 'Bicycle and pedestrian counts (Ecocounter sensors)'
                            })
                        # Also get metadata files
                        elif filename == 'bzm_telraam_segments.geojson':
                            files.append({
                                'filename': filename,
                                'url': urljoin(self.base_url, href),
                                'type': 'metadata',
                                'category': 'telraam_metadata',
                                'description': 'Geographic metadata for Telraam sensors'
                            })
                        elif filename == 'bzm_ecocounter_segments.geojson':
                            files.append({
                                'filename': filename,
                                'url': urljoin(self.base_url, href),
                                'type': 'metadata',
                                'category': 'ecocounter_metadata',
                                'description': 'Geographic metadata for Ecocounter sensors'
                            })
                        elif filename == 'READ_ME':
                            files.append({
                                'filename': filename,
                                'url': urljoin(self.base_url, href),
                                'type': 'documentation',
                                'category': 'readme',
                                'description': 'Documentation file'
                            })

                    logger.info(f"Found {len(files)} files to download")
                    logger.info(f"  - Telraam (car traffic): {len([f for f in files if f['type'] == 'telraam'])} files")
                    logger.info(f"  - Ecocounter (bicycle): {len([f for f in files if f['type'] == 'ecocounter'])} files")
                    logger.info(f"  - Metadata files: {len([f for f in files if f['type'] == 'metadata'])} files")

                    return files
                else:
                    logger.error(f"Failed to fetch file list: HTTP {response.status}")
                    return []

        except Exception as e:
            logger.error(f"Error fetching file list: {e}")
            return []

    async def download_file(
        self,
        session: aiohttp.ClientSession,
        file_info: Dict[str, str],
        semaphore: asyncio.Semaphore
    ) -> Optional[bytes]:
        """Download a single file"""
        async with semaphore:
            url = file_info['url']
            filename = file_info['filename']

            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as response:
                    if response.status == 200:
                        total_size = int(response.headers.get('content-length', 0))
                        chunk_size = 1024 * 1024  # 1MB chunks
                        downloaded_data = bytearray()

                        # Download without individual progress bars (we'll use overall progress)
                        async for chunk in response.content.iter_chunked(chunk_size):
                            downloaded_data.extend(chunk)

                        return bytes(downloaded_data)
                    else:
                        logger.error(f"Failed to download {filename}: HTTP {response.status}")
                        return None

            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")
                return None

    def save_local_backup(self, file_info: Dict[str, str], data: bytes) -> bool:
        """Save data to local filesystem as backup"""
        if not self.local_backup:
            return False

        try:
            filename = file_info['filename']
            file_type = file_info['type']

            # Organize files by type
            if file_type == 'telraam':
                filepath = self.backup_dir / "telraam" / filename
            elif file_type == 'ecocounter':
                filepath = self.backup_dir / "ecocounter" / filename
            elif file_type == 'metadata':
                filepath = self.backup_dir / "metadata" / filename
            else:
                filepath = self.backup_dir / filename

            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'wb') as f:
                f.write(data)

            # Also save to archive if enabled
            if self.enable_archive:
                self.save_to_archive(file_info, data)

            return True
        except Exception as e:
            logger.error(f"Failed to save local backup {file_info['filename']}: {e}")
            return False

    def save_to_archive(self, file_info: Dict[str, str], data: bytes) -> bool:
        """Save data to timestamped archive directory"""
        if not self.enable_archive:
            return False

        try:
            filename = file_info['filename']
            file_type = file_info['type']

            # Organize files by type in archive
            if file_type == 'telraam':
                archive_filepath = self.archive_dir / "telraam" / filename
            elif file_type == 'ecocounter':
                archive_filepath = self.archive_dir / "ecocounter" / filename
            elif file_type == 'metadata':
                archive_filepath = self.archive_dir / "metadata" / filename
            else:
                archive_filepath = self.archive_dir / filename

            archive_filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(archive_filepath, 'wb') as f:
                f.write(data)

            return True
        except Exception as e:
            logger.error(f"Failed to save to archive {file_info['filename']}: {e}")
            return False

    def upload_to_gcs(self, file_info: Dict[str, str], data: bytes) -> bool:
        """Upload data to Google Cloud Storage"""
        if not self.bucket:
            return False

        try:
            filename = file_info['filename']
            file_type = file_info['type']

            # Organize in GCS by type
            if file_type == 'telraam':
                blob_name = f"traffic_data/telraam/{filename}"
                content_type = 'application/gzip'
            elif file_type == 'ecocounter':
                blob_name = f"traffic_data/ecocounter/{filename}"
                content_type = 'application/gzip'
            elif file_type == 'metadata':
                blob_name = f"traffic_data/metadata/{filename}"
                content_type = 'application/geo+json' if filename.endswith('.geojson') else 'application/octet-stream'
            else:
                blob_name = f"traffic_data/{filename}"
                content_type = 'text/plain'

            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(data, content_type=content_type)

            return True

        except Exception as e:
            logger.error(f"Failed to upload {file_info['filename']}: {e}")
            return False

    async def download_and_store(
        self,
        session: aiohttp.ClientSession,
        file_info: Dict[str, str],
        semaphore: asyncio.Semaphore
    ) -> bool:
        """Download a file and store it"""
        data = await self.download_file(session, file_info, semaphore)

        if not data:
            return False

        # Save local backup
        if self.local_backup:
            self.save_local_backup(file_info, data)

        # Upload to GCS
        self.upload_to_gcs(file_info, data)

        return True

    async def run(self):
        """Main downloader execution"""
        logger.info("Starting Berlin Traffic Data Downloader")

        # Initialize GCS
        self.initialize_gcs()

        # Create aiohttp session
        async with aiohttp.ClientSession() as session:
            # Fetch list of available files
            files = await self.fetch_file_list(session)

            if not files:
                logger.error("No files found to download")
                return

            # Create semaphore for rate limiting
            semaphore = asyncio.Semaphore(self.max_concurrent)

            # Download all files with progress bar
            logger.info(f"Starting download of {len(files)} files...")

            tasks = [
                self.download_and_store(session, file_info, semaphore)
                for file_info in files
            ]

            # Use tqdm for progress tracking
            results = []
            for task in async_tqdm(
                asyncio.as_completed(tasks),
                desc="Downloading traffic data",
                unit="file",
                total=len(tasks)
            ):
                try:
                    result = await task
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    results.append(False)

            # Count successes
            successful = sum(1 for r in results if r is True)
            logger.info(f"Downloaded {successful}/{len(files)} files successfully")

        # Create manifest file
        manifest = {
            'download_date': datetime.now().isoformat(),
            'total_files': len(files),
            'successful_downloads': successful,
            'gcs_bucket': self.gcs_bucket_name,
            'data_source': 'https://berlin-zaehlt.de/csv/',
            'files_by_category': {
                'telraam_car_traffic': len([f for f in files if f['type'] == 'telraam']),
                'ecocounter_bicycle': len([f for f in files if f['type'] == 'ecocounter']),
                'metadata': len([f for f in files if f['type'] == 'metadata'])
            },
            'files': files
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
            manifest_blob = self.bucket.blob('traffic_data/manifest.json')
            manifest_blob.upload_from_string(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            logger.info("Manifest uploaded to GCS")

        logger.info(f"Download completed! Files saved to: {self.backup_dir if self.local_backup else 'GCS only'}")


async def main():
    """Main entry point"""
    # Configuration
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    LOCAL_BACKUP = True  # Save files locally as backup
    MAX_CONCURRENT = 5  # Number of concurrent downloads

    downloader = TrafficDataDownloader(
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        local_backup=LOCAL_BACKUP,
        max_concurrent=MAX_CONCURRENT
    )

    await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())
