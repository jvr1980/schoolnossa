#!/usr/bin/env python3
"""
School Master Table Builder - Apify Google Maps Scraper
Scrapes school data from Google Maps using Apify actor for each Berlin postcode
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SchoolScraperApify:
    """Scrapes school data using Apify Google Maps Scraper"""

    # Apify Actor ID for Google Maps Scraper
    ACTOR_ID = "nwua9Gu5YrADL7ZDj"  # compass/crawler-google-places

    # School types to search for in German
    SCHOOL_TYPES = {
        'kitas': 'Kita',
        'grundschulen': 'Grundschule',
        'gymnasien': 'Gymnasium',
        'sekundarschulen': 'Integrierte Sekundarschule',
        'private_schools': 'Privatschule'
    }

    def __init__(
        self,
        apify_api_token: str,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        output_dir: str = "school_data_apify",
        max_results_per_search: int = 100
    ):
        self.apify_api_token = apify_api_token
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.output_dir = Path(output_dir)
        self.max_results_per_search = max_results_per_search
        self.storage_client = None
        self.bucket = None
        self.session = requests.Session()

        # Create output directory
        self.output_dir.mkdir(exist_ok=True)

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

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

    def load_berlin_postcodes(self) -> List[str]:
        """Load Berlin postcodes from GCS"""
        logger.info("Loading Berlin postcodes from GCS...")

        try:
            blob = self.bucket.blob("geo_data/germany/berlin/postcodes/plz_csv.csv")
            csv_content = blob.download_as_text()

            # Parse CSV - skip header, extract postcodes
            postcodes = []
            for line in csv_content.strip().split('\n')[1:]:  # Skip header
                postcode = line.strip().strip('"')
                if postcode and postcode.isdigit():
                    postcodes.append(postcode)

            logger.info(f"Loaded {len(postcodes)} postcodes from GCS")
            return postcodes

        except Exception as e:
            logger.error(f"Error loading postcodes from GCS: {e}")
            raise

    def create_actor_input(
        self,
        school_type: str,
        postcode: str,
        max_results: int = 100
    ) -> Dict:
        """
        Create input configuration for Apify actor

        Args:
            school_type: Type of school to search for (in German)
            postcode: Berlin postcode
            max_results: Maximum results per search

        Returns:
            Dictionary with actor input configuration
        """
        # Construct search query with school type, postcode, and country
        search_query = f"{school_type} {postcode} Berlin Germany"

        actor_input = {
            "searchStringsArray": [search_query],
            "locationQuery": f"{postcode}, Berlin, Germany",
            "maxCrawledPlacesPerSearch": max_results,
            "language": "de",
            "countryCode": "de",
            "maxReviews": 0,  # Don't scrape reviews to save time
            "maxImages": 5,
            "exportPlaceUrls": False,
            "includeWebResults": False,
            "scrapeDirectories": False,
            "scrapeReviewersInfo": False
        }

        return actor_input

    def run_actor(self, actor_input: Dict) -> Optional[str]:
        """
        Run Apify actor and return dataset ID

        Args:
            actor_input: Actor input configuration

        Returns:
            Dataset ID if successful, None otherwise
        """
        url = f"https://api.apify.com/v2/acts/{self.ACTOR_ID}/runs"
        params = {"token": self.apify_api_token}

        try:
            logger.info("Starting Apify actor run...")
            response = self.session.post(
                url,
                json=actor_input,
                params=params,
                timeout=30
            )
            response.raise_for_status()

            run_data = response.json()
            run_id = run_data['data']['id']
            logger.info(f"Actor run started with ID: {run_id}")

            # Wait for the run to complete
            return self.wait_for_run(run_id)

        except Exception as e:
            logger.error(f"Error running actor: {e}")
            return None

    def wait_for_run(self, run_id: str, timeout: int = 600) -> Optional[str]:
        """
        Wait for actor run to complete

        Args:
            run_id: Actor run ID
            timeout: Maximum wait time in seconds

        Returns:
            Dataset ID if successful, None otherwise
        """
        url = f"https://api.apify.com/v2/actor-runs/{run_id}"
        params = {"token": self.apify_api_token}

        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                logger.error(f"Run {run_id} timed out after {timeout} seconds")
                return None

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                run_info = response.json()
                status = run_info['data']['status']

                if status == 'SUCCEEDED':
                    dataset_id = run_info['data']['defaultDatasetId']
                    logger.info(f"Run {run_id} completed successfully")
                    return dataset_id

                elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                    logger.error(f"Run {run_id} failed with status: {status}")
                    return None

                else:
                    logger.info(f"Run {run_id} status: {status}, waiting...")
                    time.sleep(10)

            except Exception as e:
                logger.error(f"Error checking run status: {e}")
                time.sleep(10)

    def get_dataset_items(self, dataset_id: str) -> List[Dict]:
        """
        Retrieve all items from Apify dataset

        Args:
            dataset_id: Dataset ID

        Returns:
            List of dataset items
        """
        url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
        params = {"token": self.apify_api_token, "format": "json"}

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()

            items = response.json()
            logger.info(f"Retrieved {len(items)} items from dataset {dataset_id}")
            return items

        except Exception as e:
            logger.error(f"Error retrieving dataset items: {e}")
            return []

    def scrape_schools_for_postcode(
        self,
        postcode: str,
        school_type_key: str,
        school_type_name: str
    ) -> List[Dict]:
        """
        Scrape schools for a specific postcode and school type

        Args:
            postcode: Berlin postcode
            school_type_key: School type key (e.g., 'kitas')
            school_type_name: School type in German (e.g., 'Kita')

        Returns:
            List of school data
        """
        logger.info(f"Scraping {school_type_name} in postcode {postcode}...")

        # Create actor input
        actor_input = self.create_actor_input(
            school_type=school_type_name,
            postcode=postcode,
            max_results=self.max_results_per_search
        )

        # Run actor
        dataset_id = self.run_actor(actor_input)

        if not dataset_id:
            logger.warning(f"No dataset returned for {school_type_name} in {postcode}")
            return []

        # Get results
        results = self.get_dataset_items(dataset_id)

        # Add metadata to each result
        for result in results:
            result['_metadata'] = {
                'postcode': postcode,
                'school_type': school_type_key,
                'school_type_german': school_type_name,
                'scraped_at': datetime.now().isoformat(),
                'source': 'apify_google_maps'
            }

        return results

    def save_results(
        self,
        results: List[Dict],
        postcode: str,
        school_type: str
    ):
        """Save results locally and to GCS"""
        if not results:
            logger.info(f"No results to save for {school_type} in {postcode}")
            return

        filename = f"{school_type}_{postcode}_{self.run_timestamp}.json"

        # Save locally
        local_path = self.output_dir / filename
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(results)} results locally: {local_path}")

        # Upload to GCS
        gcs_path = f"school_data_apify/{school_type}/{postcode}/{filename}"
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(
                str(local_path),
                content_type='application/json'
            )
            logger.info(f"Uploaded to GCS: {gcs_path}")
        except Exception as e:
            logger.error(f"Error uploading to GCS: {e}")

    def run(self):
        """Main execution method"""
        logger.info("Starting School Master Table Builder - Apify")
        logger.info(f"Output directory: {self.output_dir}")

        # Initialize GCS
        self.initialize_gcs()

        # Load postcodes
        postcodes = self.load_berlin_postcodes()

        logger.info(f"Processing {len(postcodes)} postcodes x {len(self.SCHOOL_TYPES)} school types")

        all_results = {school_type: [] for school_type in self.SCHOOL_TYPES.keys()}
        stats = {
            'total_postcodes': len(postcodes),
            'total_school_types': len(self.SCHOOL_TYPES),
            'processed': 0,
            'failed': 0,
            'total_schools_found': 0
        }

        # Process each postcode and school type
        for postcode in postcodes:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing postcode: {postcode}")
            logger.info(f"{'='*60}")

            for school_type_key, school_type_name in self.SCHOOL_TYPES.items():
                try:
                    # Scrape schools
                    results = self.scrape_schools_for_postcode(
                        postcode=postcode,
                        school_type_key=school_type_key,
                        school_type_name=school_type_name
                    )

                    # Save results
                    self.save_results(results, postcode, school_type_key)

                    # Update stats
                    all_results[school_type_key].extend(results)
                    stats['processed'] += 1
                    stats['total_schools_found'] += len(results)

                    logger.info(f"Found {len(results)} {school_type_name} in {postcode}")

                    # Be respectful to the API - add delay between requests
                    time.sleep(2)

                except Exception as e:
                    logger.error(f"Error processing {school_type_name} in {postcode}: {e}")
                    stats['failed'] += 1
                    continue

        # Create summary report
        logger.info("\n" + "="*60)
        logger.info("SCRAPING SUMMARY")
        logger.info("="*60)

        summary = {
            'run_timestamp': self.run_timestamp,
            'statistics': stats,
            'schools_by_type': {
                school_type: len(results)
                for school_type, results in all_results.items()
            }
        }

        logger.info(f"Total postcodes: {stats['total_postcodes']}")
        logger.info(f"Total searches: {stats['processed']}")
        logger.info(f"Failed searches: {stats['failed']}")
        logger.info(f"Total schools found: {stats['total_schools_found']}")

        for school_type, count in summary['schools_by_type'].items():
            logger.info(f"  {school_type}: {count} schools")

        # Save summary
        summary_path = self.output_dir / f"summary_{self.run_timestamp}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # Upload summary to GCS
        try:
            blob = self.bucket.blob(f"school_data_apify/summary_{self.run_timestamp}.json")
            blob.upload_from_filename(str(summary_path), content_type='application/json')
            logger.info("Summary uploaded to GCS")
        except Exception as e:
            logger.error(f"Error uploading summary: {e}")

        logger.info("\nSchool scraping completed!")


def main():
    """Main entry point"""
    # Configuration
    APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')
    GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'schoolnossa-berlin')
    GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID', 'schoolnossa')
    MAX_RESULTS_PER_SEARCH = int(os.getenv('MAX_RESULTS_PER_SEARCH', '100'))

    if not APIFY_API_TOKEN:
        logger.error("APIFY_API_TOKEN environment variable not set")
        logger.info("Please set it in your .env file or environment")
        return

    scraper = SchoolScraperApify(
        apify_api_token=APIFY_API_TOKEN,
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        max_results_per_search=MAX_RESULTS_PER_SEARCH
    )

    scraper.run()


if __name__ == "__main__":
    main()
