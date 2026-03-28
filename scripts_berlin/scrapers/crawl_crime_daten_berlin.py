#!/usr/bin/env python3
"""
Berlin Crime Data Downloader
Downloads crime statistics Excel files from daten.berlin.de and stores them in Google Cloud Storage
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
from urllib.parse import urlparse, unquote

import aiohttp
from google.cloud import storage
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


class CrimeDataDownloader:
    """Downloads Berlin crime statistics and stores in Google Cloud Storage"""

    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        local_backup: bool = True,
        enable_archive: bool = True
    ):
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.local_backup = local_backup
        self.enable_archive = enable_archive
        self.storage_client = None
        self.bucket = None

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Crime data URLs
        self.crime_data_urls = [
            {
                'url': 'https://www.kriminalitaetsatlas.berlin.de/K-Atlas/bezirke/Fallzahlen&HZ 2015-2024.xlsx',
                'name': 'Crime_Statistics_Districts_2015-2024',
                'description': 'Absolute case numbers and frequency rates for Berlin districts (2015-2024)'
            }
        ]

        # Create local backup directory if needed
        if self.local_backup:
            self.backup_dir = Path("crime_data")
            self.backup_dir.mkdir(exist_ok=True)

        # Create archive directory if enabled
        if self.enable_archive:
            self.archive_base_dir = Path("data_archive")
            self.archive_base_dir.mkdir(exist_ok=True)
            self.archive_dir = self.archive_base_dir / f"crime_data_{self.run_timestamp}"
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

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe storage"""
        # Remove or replace invalid characters
        filename = re.sub(r'[^\w\s.-]', '_', filename)
        filename = re.sub(r'\s+', '_', filename)
        return filename

    def extract_filename_from_url(self, url: str, data_name: str) -> str:
        """Extract and sanitize filename from URL"""
        parsed = urlparse(url)
        path = parsed.path

        # Get filename from URL path
        url_filename = unquote(path.split('/')[-1])

        # Get file extension
        if '.' in url_filename:
            extension = url_filename.split('.')[-1]
        else:
            extension = 'xlsx'  # Default to xlsx

        # Create sanitized filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d')
        safe_name = self.sanitize_filename(data_name)
        filename = f"{safe_name}_{timestamp}.{extension}"

        return filename

    async def download_file(
        self,
        session: aiohttp.ClientSession,
        url: str,
        filename: str
    ) -> Optional[bytes]:
        """Download a file from URL"""
        logger.info(f"Downloading: {url}")

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as response:
                if response.status == 200:
                    total_size = int(response.headers.get('content-length', 0))

                    # Download with progress bar
                    chunk_size = 1024 * 1024  # 1MB chunks
                    downloaded_data = bytearray()

                    with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        desc=f"Downloading {filename}"
                    ) as pbar:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            downloaded_data.extend(chunk)
                            pbar.update(len(chunk))

                    logger.info(f"Successfully downloaded {filename} ({len(downloaded_data)} bytes)")
                    return bytes(downloaded_data)
                else:
                    logger.error(f"Failed to download {url}: HTTP {response.status}")
                    return None

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    def save_local_backup(self, filename: str, data: bytes) -> bool:
        """Save data to local filesystem as backup"""
        if not self.local_backup:
            return False

        try:
            filepath = self.backup_dir / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, 'wb') as f:
                f.write(data)

            # Also save to archive if enabled
            if self.enable_archive:
                self.save_to_archive(filename, data)

            return True
        except Exception as e:
            logger.error(f"Failed to save local backup {filename}: {e}")
            return False

    def save_to_archive(self, filename: str, data: bytes) -> bool:
        """Save data to timestamped archive directory"""
        if not self.enable_archive:
            return False

        try:
            archive_filepath = self.archive_dir / filename
            archive_filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(archive_filepath, 'wb') as f:
                f.write(data)

            return True
        except Exception as e:
            logger.error(f"Failed to save to archive {filename}: {e}")
            return False

    def upload_to_gcs(self, blob_name: str, data: bytes, content_type: str = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') -> bool:
        """Upload data to Google Cloud Storage"""
        if not self.bucket:
            logger.warning("GCS bucket not initialized, skipping upload")
            return False

        try:
            blob = self.bucket.blob(blob_name)

            # Upload with metadata
            blob.upload_from_string(
                data,
                content_type=content_type
            )

            logger.info(f"Uploaded to GCS: {blob_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {e}")
            return False

    async def download_and_store(
        self,
        session: aiohttp.ClientSession,
        data_info: Dict[str, str]
    ):
        """Download a file and store it"""
        url = data_info['url']
        name = data_info['name']
        description = data_info.get('description', '')

        # Generate filename
        filename = self.extract_filename_from_url(url, name)

        # Download file
        file_data = await self.download_file(session, url, filename)

        if not file_data:
            logger.error(f"Failed to download {name}")
            return

        # Create metadata
        metadata = {
            'original_url': url,
            'name': name,
            'description': description,
            'downloaded_at': datetime.now().isoformat(),
            'file_size': len(file_data),
            'filename': filename
        }

        # Save local backup (Excel file)
        if self.local_backup:
            self.save_local_backup(filename, file_data)

            # Also save metadata as JSON
            metadata_filename = filename.rsplit('.', 1)[0] + '_metadata.json'
            metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2).encode('utf-8')
            self.save_local_backup(metadata_filename, metadata_json)

        # Upload to GCS
        blob_name = f"crime_data/{filename}"

        # Determine content type
        if filename.endswith('.xlsx'):
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        elif filename.endswith('.xls'):
            content_type = 'application/vnd.ms-excel'
        elif filename.endswith('.csv'):
            content_type = 'text/csv'
        else:
            content_type = 'application/octet-stream'

        self.upload_to_gcs(blob_name, file_data, content_type)

        # Upload metadata to GCS
        if self.bucket:
            metadata_blob_name = f"crime_data/{filename.rsplit('.', 1)[0]}_metadata.json"
            metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2).encode('utf-8')
            self.upload_to_gcs(metadata_blob_name, metadata_json, 'application/json')

    async def run(self):
        """Main downloader execution"""
        logger.info("Starting Berlin Crime Data Downloader")

        # Initialize GCS
        self.initialize_gcs()

        # Create aiohttp session
        async with aiohttp.ClientSession() as session:
            # Download all files
            for data_info in self.crime_data_urls:
                logger.info(f"Processing: {data_info['name']}")
                await self.download_and_store(session, data_info)

                # Small delay between downloads
                await asyncio.sleep(1)

        logger.info(f"Download completed! Files saved to: {self.backup_dir if self.local_backup else 'GCS only'}")

        # Create manifest file
        manifest = {
            'download_date': datetime.now().isoformat(),
            'total_files': len(self.crime_data_urls),
            'gcs_bucket': self.gcs_bucket_name,
            'files': self.crime_data_urls
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
            manifest_blob = self.bucket.blob('crime_data/manifest.json')
            manifest_blob.upload_from_string(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            logger.info("Manifest uploaded to GCS")


async def main():
    """Main entry point"""
    # Configuration
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    LOCAL_BACKUP = True  # Save files locally as backup

    downloader = CrimeDataDownloader(
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        local_backup=LOCAL_BACKUP
    )

    await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())
