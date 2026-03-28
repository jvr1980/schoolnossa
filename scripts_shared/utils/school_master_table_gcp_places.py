#!/usr/bin/env python3
"""
School Master Table Builder - Google Places API
Scrapes school data from Google Places API for each Berlin postcode
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


class SchoolScraperGooglePlaces:
    """Scrapes school data using Google Places API"""

    # Google Places API endpoints
    PLACES_API_BASE = "https://maps.googleapis.com/maps/api/place"
    TEXT_SEARCH_ENDPOINT = f"{PLACES_API_BASE}/textsearch/json"
    PLACE_DETAILS_ENDPOINT = f"{PLACES_API_BASE}/details/json"

    # School types to search for
    SCHOOL_TYPES = {
        'kitas': ['Kita', 'Kindertagesstätte', 'Kindergarten'],
        'grundschulen': ['Grundschule'],
        'gymnasien': ['Gymnasium'],
        'sekundarschulen': ['Integrierte Sekundarschule', 'Sekundarschule'],
        'private_schools': ['Privatschule', 'Private Schule']
    }

    def __init__(
        self,
        google_api_key: str,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        output_dir: str = "school_data_gcp_places",
        fetch_details: bool = True
    ):
        self.google_api_key = google_api_key
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.output_dir = Path(output_dir)
        self.fetch_details = fetch_details
        self.storage_client = None
        self.bucket = None
        self.session = requests.Session()

        # Create output directory
        self.output_dir.mkdir(exist_ok=True)

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Track API usage
        self.api_calls = {
            'text_search': 0,
            'place_details': 0,
            'total_cost_usd': 0.0
        }

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

    def text_search(
        self,
        query: str,
        location: Optional[str] = None
    ) -> List[Dict]:
        """
        Perform text search using Google Places API

        Args:
            query: Search query
            location: Location string (e.g., "10115 Berlin Germany")

        Returns:
            List of place results
        """
        all_results = []
        next_page_token = None

        while True:
            params = {
                'key': self.google_api_key,
                'query': query
            }

            if location:
                params['query'] = f"{query} {location}"

            if next_page_token:
                params['pagetoken'] = next_page_token

            try:
                response = self.session.get(
                    self.TEXT_SEARCH_ENDPOINT,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()
                self.api_calls['text_search'] += 1
                self.api_calls['total_cost_usd'] += 0.032  # $32 per 1000 requests

                if data['status'] == 'OK':
                    results = data.get('results', [])
                    all_results.extend(results)
                    logger.info(f"Found {len(results)} results (total: {len(all_results)})")

                    # Check for next page
                    next_page_token = data.get('next_page_token')
                    if next_page_token:
                        # Google requires a short delay before requesting next page
                        time.sleep(2)
                    else:
                        break

                elif data['status'] == 'ZERO_RESULTS':
                    logger.info("No results found")
                    break

                else:
                    logger.warning(f"API returned status: {data['status']}")
                    break

            except Exception as e:
                logger.error(f"Error in text search: {e}")
                break

        return all_results

    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """
        Get detailed information for a place

        Args:
            place_id: Google Place ID

        Returns:
            Place details dictionary or None
        """
        params = {
            'key': self.google_api_key,
            'place_id': place_id,
            'fields': (
                'name,formatted_address,geometry,place_id,'
                'types,rating,user_ratings_total,opening_hours,'
                'formatted_phone_number,website,url,reviews,'
                'photos,price_level,business_status'
            )
        }

        try:
            response = self.session.get(
                self.PLACE_DETAILS_ENDPOINT,
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            self.api_calls['place_details'] += 1
            self.api_calls['total_cost_usd'] += 0.017  # $17 per 1000 requests

            if data['status'] == 'OK':
                return data.get('result')
            else:
                logger.warning(f"Place details API returned status: {data['status']}")
                return None

        except Exception as e:
            logger.error(f"Error fetching place details: {e}")
            return None

    def scrape_schools_for_postcode(
        self,
        postcode: str,
        school_type_key: str,
        school_type_queries: List[str]
    ) -> List[Dict]:
        """
        Scrape schools for a specific postcode and school type

        Args:
            postcode: Berlin postcode
            school_type_key: School type key (e.g., 'kitas')
            school_type_queries: List of query terms to search

        Returns:
            List of school data
        """
        logger.info(f"Scraping {school_type_key} in postcode {postcode}...")

        all_schools = []
        seen_place_ids = set()

        # Try each query variation
        for query_term in school_type_queries:
            location_query = f"{postcode} Berlin Germany"
            search_query = f"{query_term} in {location_query}"

            logger.info(f"Searching: {search_query}")

            # Perform text search
            results = self.text_search(
                query=search_query,
                location=None  # Location is included in query
            )

            # Process results
            for result in results:
                place_id = result.get('place_id')

                # Skip duplicates
                if place_id in seen_place_ids:
                    continue

                seen_place_ids.add(place_id)

                # Optionally fetch detailed information
                if self.fetch_details:
                    details = self.get_place_details(place_id)
                    if details:
                        # Merge basic result with details
                        result.update(details)
                    time.sleep(0.1)  # Small delay between detail requests

                # Add metadata
                result['_metadata'] = {
                    'postcode': postcode,
                    'school_type': school_type_key,
                    'search_query': query_term,
                    'scraped_at': datetime.now().isoformat(),
                    'source': 'google_places_api'
                }

                all_schools.append(result)

            # Delay between different queries
            time.sleep(1)

        logger.info(f"Found {len(all_schools)} unique schools for {school_type_key} in {postcode}")
        return all_schools

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
        gcs_path = f"school_data_gcp_places/{school_type}/{postcode}/{filename}"
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
        logger.info("Starting School Master Table Builder - Google Places API")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Fetch details: {self.fetch_details}")

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

            for school_type_key, school_type_queries in self.SCHOOL_TYPES.items():
                try:
                    # Scrape schools
                    results = self.scrape_schools_for_postcode(
                        postcode=postcode,
                        school_type_key=school_type_key,
                        school_type_queries=school_type_queries
                    )

                    # Save results
                    self.save_results(results, postcode, school_type_key)

                    # Update stats
                    all_results[school_type_key].extend(results)
                    stats['processed'] += 1
                    stats['total_schools_found'] += len(results)

                    logger.info(f"Found {len(results)} {school_type_key} in {postcode}")

                    # Rate limiting - be respectful to the API
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing {school_type_key} in {postcode}: {e}")
                    stats['failed'] += 1
                    continue

        # Create summary report
        logger.info("\n" + "="*60)
        logger.info("SCRAPING SUMMARY")
        logger.info("="*60)

        summary = {
            'run_timestamp': self.run_timestamp,
            'statistics': stats,
            'api_usage': self.api_calls,
            'schools_by_type': {
                school_type: len(results)
                for school_type, results in all_results.items()
            }
        }

        logger.info(f"Total postcodes: {stats['total_postcodes']}")
        logger.info(f"Total searches: {stats['processed']}")
        logger.info(f"Failed searches: {stats['failed']}")
        logger.info(f"Total schools found: {stats['total_schools_found']}")
        logger.info(f"\nAPI Usage:")
        logger.info(f"  Text searches: {self.api_calls['text_search']}")
        logger.info(f"  Place details: {self.api_calls['place_details']}")
        logger.info(f"  Estimated cost: ${self.api_calls['total_cost_usd']:.2f}")

        for school_type, count in summary['schools_by_type'].items():
            logger.info(f"  {school_type}: {count} schools")

        # Save summary
        summary_path = self.output_dir / f"summary_{self.run_timestamp}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # Upload summary to GCS
        try:
            blob = self.bucket.blob(f"school_data_gcp_places/summary_{self.run_timestamp}.json")
            blob.upload_from_filename(str(summary_path), content_type='application/json')
            logger.info("Summary uploaded to GCS")
        except Exception as e:
            logger.error(f"Error uploading summary: {e}")

        logger.info("\nSchool scraping completed!")


def main():
    """Main entry point"""
    # Configuration
    GOOGLE_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
    GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'schoolnossa-berlin')
    GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID', 'schoolnossa')
    FETCH_DETAILS = os.getenv('FETCH_PLACE_DETAILS', 'true').lower() == 'true'

    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_PLACES_API_KEY environment variable not set")
        logger.info("Please set it in your .env file or environment")
        return

    scraper = SchoolScraperGooglePlaces(
        google_api_key=GOOGLE_API_KEY,
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        fetch_details=FETCH_DETAILS
    )

    scraper.run()


if __name__ == "__main__":
    main()
