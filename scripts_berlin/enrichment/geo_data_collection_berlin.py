#!/usr/bin/env python3
"""
Berlin Geographic Data Collection Script
Downloads Ortsteile and PLZ data from ODIS Berlin and stores in Google Cloud Storage
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BerlinGeoDataCollector:
    """Downloads Berlin geographic data and uploads to Google Cloud Storage"""

    # Dataset URLs
    ORTSTEILE_URL = "https://daten.odis-berlin.de/de/dataset/ortsteile/"
    PLZ_URL = "https://daten.odis-berlin.de/de/dataset/plz/"

    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        base_path: str = "geo_data/germany/berlin"
    ):
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.base_path = base_path
        self.storage_client = None
        self.bucket = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

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
            logger.error(f"GCS initialization failed: {e}")
            raise

    def get_download_links(self, dataset_url: str) -> Dict[str, str]:
        """
        Scrape the dataset page to find CSV and KML download links

        Args:
            dataset_url: URL of the ODIS dataset page

        Returns:
            Dictionary with 'csv' and 'kml' download URLs
        """
        logger.info(f"Fetching download links from: {dataset_url}")

        try:
            response = self.session.get(dataset_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            download_links = {}

            # Look for resource links - ODIS uses specific patterns
            # Find all resource items or download links
            resource_links = soup.find_all('a', href=True)

            for link in resource_links:
                href = link.get('href', '')
                text = link.get_text(strip=True).lower()

                # Look for CSV files
                if href.endswith('.csv') or 'csv' in text:
                    full_url = urljoin(dataset_url, href)
                    download_links['csv'] = full_url
                    logger.info(f"Found CSV link: {full_url}")

                # Look for KML files
                elif href.endswith('.kml') or 'kml' in text:
                    full_url = urljoin(dataset_url, href)
                    download_links['kml'] = full_url
                    logger.info(f"Found KML link: {full_url}")

                # Sometimes shapefiles are in .zip
                elif href.endswith('.zip') and 'shape' in text:
                    full_url = urljoin(dataset_url, href)
                    download_links['shapefile'] = full_url
                    logger.info(f"Found shapefile (zip) link: {full_url}")

            # Also look in resource list items
            resource_items = soup.find_all('li', class_=lambda x: x and 'resource-item' in x)
            for item in resource_items:
                format_tag = item.find(class_=lambda x: x and 'format' in str(x).lower())
                if format_tag:
                    format_text = format_tag.get_text(strip=True).lower()
                    download_link = item.find('a', class_=lambda x: x and ('resource-url-analytics' in str(x) or 'download' in str(x)))

                    if download_link:
                        href = download_link.get('href', '')
                        full_url = urljoin(dataset_url, href)

                        if 'csv' in format_text:
                            download_links['csv'] = full_url
                            logger.info(f"Found CSV resource: {full_url}")
                        elif 'kml' in format_text:
                            download_links['kml'] = full_url
                            logger.info(f"Found KML resource: {full_url}")

            return download_links

        except Exception as e:
            logger.error(f"Error fetching download links from {dataset_url}: {e}")
            return {}

    def download_file(self, url: str, local_path: Path) -> bool:
        """
        Download a file from URL to local path

        Args:
            url: URL to download from
            local_path: Local path to save file

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Downloading: {url}")

        try:
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()

            # Get total file size if available
            total_size = int(response.headers.get('content-length', 0))

            # Download with progress
            with open(local_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Progress: {progress:.1f}%")

            file_size = local_path.stat().st_size
            logger.info(f"Downloaded {file_size:,} bytes to {local_path}")
            return True

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return False

    def upload_to_gcs(self, local_path: Path, gcs_path: str) -> bool:
        """
        Upload a file to Google Cloud Storage

        Args:
            local_path: Local file path
            gcs_path: GCS destination path

        Returns:
            True if successful, False otherwise
        """
        if not self.bucket:
            logger.error("GCS bucket not initialized")
            return False

        try:
            blob = self.bucket.blob(gcs_path)

            # Determine content type based on file extension
            content_type = 'application/octet-stream'
            if local_path.suffix == '.csv':
                content_type = 'text/csv'
            elif local_path.suffix == '.kml':
                content_type = 'application/vnd.google-earth.kml+xml'
            elif local_path.suffix == '.zip':
                content_type = 'application/zip'

            blob.upload_from_filename(
                str(local_path),
                content_type=content_type
            )

            logger.info(f"Uploaded to GCS: {gcs_path}")
            return True

        except Exception as e:
            logger.error(f"Error uploading to GCS {gcs_path}: {e}")
            return False

    def process_dataset(
        self,
        dataset_name: str,
        dataset_url: str,
        subfolder: str
    ) -> Dict[str, bool]:
        """
        Process a dataset: download files and upload to GCS

        Args:
            dataset_name: Name of the dataset (for logging)
            dataset_url: URL of the dataset page
            subfolder: Subfolder within base_path (e.g., 'districts', 'postcodes')

        Returns:
            Dictionary with results for each file type
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing dataset: {dataset_name}")
        logger.info(f"{'='*60}")

        results = {}

        # Get download links
        download_links = self.get_download_links(dataset_url)

        if not download_links:
            logger.error(f"No download links found for {dataset_name}")
            return results

        # Create temporary directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Process each file type
            for file_type, url in download_links.items():
                logger.info(f"\nProcessing {file_type} file...")

                # Determine file extension
                if url.endswith('.csv'):
                    ext = '.csv'
                elif url.endswith('.kml'):
                    ext = '.kml'
                elif url.endswith('.zip'):
                    ext = '.zip'
                else:
                    # Try to guess from URL
                    ext = Path(url.split('?')[0]).suffix or f'.{file_type}'

                # Create local filename
                local_filename = f"{dataset_name}_{file_type}{ext}"
                local_path = temp_path / local_filename

                # Download file
                if self.download_file(url, local_path):
                    # Upload to GCS
                    gcs_path = f"{self.base_path}/{subfolder}/{local_filename}"
                    success = self.upload_to_gcs(local_path, gcs_path)
                    results[file_type] = success
                else:
                    results[file_type] = False

        return results

    def run(self):
        """Main execution method"""
        logger.info("Starting Berlin Geographic Data Collection")
        logger.info(f"GCS Bucket: {self.gcs_bucket_name}")
        logger.info(f"Base Path: {self.base_path}")

        # Initialize GCS
        self.initialize_gcs()

        all_results = {}

        # Process Ortsteile (Districts) dataset
        ortsteile_results = self.process_dataset(
            dataset_name="ortsteile",
            dataset_url=self.ORTSTEILE_URL,
            subfolder="districts"
        )
        all_results['ortsteile'] = ortsteile_results

        # Process PLZ (Postcodes) dataset
        plz_results = self.process_dataset(
            dataset_name="plz",
            dataset_url=self.PLZ_URL,
            subfolder="postcodes"
        )
        all_results['plz'] = plz_results

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("COLLECTION SUMMARY")
        logger.info("="*60)

        for dataset, results in all_results.items():
            logger.info(f"\n{dataset.upper()}:")
            for file_type, success in results.items():
                status = "✓ SUCCESS" if success else "✗ FAILED"
                logger.info(f"  {file_type}: {status}")

        # Check if all successful
        all_successful = all(
            all(results.values())
            for results in all_results.values()
            if results
        )

        if all_successful:
            logger.info("\n🎉 All files downloaded and uploaded successfully!")
        else:
            logger.warning("\n⚠️  Some files failed to download or upload")

        logger.info(f"\nFiles stored in GCS at: gs://{self.gcs_bucket_name}/{self.base_path}/")


def main():
    """Main entry point"""
    # Configuration from environment variables
    GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'schoolnossa-berlin')
    GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID', 'schoolnossa')

    collector = BerlinGeoDataCollector(
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        base_path="geo_data/germany/berlin"
    )

    collector.run()


if __name__ == "__main__":
    main()
