#!/usr/bin/env python3
"""
Vector Database Input Preparation Pipeline
Processes JSON data from GCS and prepares it for ChromaDB indexing
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
import os

from google.cloud import storage
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VectorDBPreparer:
    """Prepares data from various sources for vector database ingestion"""

    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_project_id: Optional[str] = None,
        local_output: bool = True
    ):
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_project_id = gcs_project_id
        self.local_output = local_output
        self.storage_client = None
        self.bucket = None

        # Generate timestamp for this run
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create local output directory if needed
        if self.local_output:
            self.output_dir = Path("vector_db_input")
            self.output_dir.mkdir(exist_ok=True)
            (self.output_dir / "kitas").mkdir(exist_ok=True)
            (self.output_dir / "schools").mkdir(exist_ok=True)
            (self.output_dir / "metadata").mkdir(exist_ok=True)

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

    def clean_html_tags(self, text: str) -> str:
        """Remove HTML tags and clean text"""
        if not text:
            return ""

        # Remove script and style elements
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        return text

    def extract_vector_content_from_kita_detail(self, data: Dict) -> Optional[Dict]:
        """Extract clean content from kita detail JSON for vector DB"""
        try:
            # Build semantic content for vector search
            content_parts = []

            # Add structured metadata
            kita_name = data.get('kita_name', '')
            if kita_name:
                content_parts.append(f"Name: {kita_name}")

            title = data.get('title', '')
            if title and title != 'Kita-Navigator - Detailansicht':
                content_parts.append(f"Title: {title}")

            description = data.get('description', '')
            if description:
                content_parts.append(f"Description: {description}")

            keywords = data.get('keywords', '')
            if keywords:
                content_parts.append(f"Keywords: {keywords}")

            address = data.get('address', '')
            if address:
                content_parts.append(f"Address: {address}")

            contact = data.get('contact', {})
            if contact and isinstance(contact, dict):
                contact_text = contact.get('text', '')
                if contact_text:
                    content_parts.append(f"Contact: {contact_text}")

            opening_hours = data.get('opening_hours', '')
            if opening_hours:
                content_parts.append(f"Opening Hours: {opening_hours}")

            # Add markdown content (preferred over HTML)
            markdown = data.get('markdown', '')
            if markdown:
                # Clean markdown but keep structure
                cleaned_markdown = markdown.strip()
                if cleaned_markdown:
                    content_parts.append(f"\nContent:\n{cleaned_markdown}")
            else:
                # Fallback to cleaned_text if markdown not available
                cleaned_text = data.get('cleaned_text', '')
                if cleaned_text:
                    content_parts.append(f"\nContent:\n{cleaned_text}")

            # Add table data as structured text
            tables = data.get('tables', [])
            if tables:
                for idx, table in enumerate(tables):
                    table_text = self.format_table_as_text(table)
                    if table_text:
                        content_parts.append(f"\nTable {idx+1}:\n{table_text}")

            # Combine all parts
            full_content = "\n\n".join(content_parts)

            # Build the vector DB document
            vector_doc = {
                'id': data.get('kita_id', 'unknown'),
                'type': 'kita',
                'name': kita_name,
                'content': full_content,
                'metadata': {
                    'url': data.get('url', ''),
                    'address': address,
                    'source': 'kita-navigator.berlin.de',
                    'crawled_at': data.get('metadata', {}).get('crawled_at', ''),
                    'data_type': 'childcare_facility'
                }
            }

            return vector_doc

        except Exception as e:
            logger.error(f"Error extracting kita content: {e}")
            return None

    def extract_vector_content_from_school(self, data: Dict) -> Optional[Dict]:
        """Extract clean content from school JSON for vector DB"""
        try:
            # Build semantic content for vector search
            content_parts = []

            # Add structured metadata
            school_name = data.get('school_name', '')
            if school_name:
                content_parts.append(f"Name: {school_name}")

            title = data.get('title', '')
            if title and title != 'Schulportrait':
                content_parts.append(f"Title: {title}")

            description = data.get('description', '')
            if description:
                content_parts.append(f"Description: {description}")

            keywords = data.get('keywords', '')
            if keywords:
                content_parts.append(f"Keywords: {keywords}")

            # Add markdown content (preferred over HTML)
            markdown = data.get('markdown', '')
            if markdown:
                # Clean markdown but keep structure
                cleaned_markdown = markdown.strip()
                if cleaned_markdown:
                    content_parts.append(f"\nContent:\n{cleaned_markdown}")
            else:
                # Fallback to cleaned_text if markdown not available
                cleaned_text = data.get('cleaned_text', '')
                if cleaned_text:
                    content_parts.append(f"\nContent:\n{cleaned_text}")

            # Add table data as structured text
            tables = data.get('tables', [])
            if tables:
                for idx, table in enumerate(tables):
                    table_text = self.format_table_as_text(table)
                    if table_text:
                        content_parts.append(f"\nTable {idx+1}:\n{table_text}")

            # Combine all parts
            full_content = "\n\n".join(content_parts)

            # Build the vector DB document
            vector_doc = {
                'id': data.get('school_id', 'unknown'),
                'type': 'school',
                'name': school_name,
                'content': full_content,
                'metadata': {
                    'url': data.get('url', ''),
                    'source': 'www.bildung.berlin.de',
                    'crawled_at': data.get('metadata', {}).get('crawled_at', ''),
                    'data_type': 'school'
                }
            }

            return vector_doc

        except Exception as e:
            logger.error(f"Error extracting school content: {e}")
            return None

    def extract_vector_content_from_kita_page(self, data: Dict) -> List[Dict]:
        """Extract clean content from kita page JSON for vector DB"""
        try:
            vector_docs = []
            kitas = data.get('kitas', [])

            for kita in kitas:
                name = kita.get('name', '')
                detail_url = kita.get('detail_url', '')
                full_text = kita.get('full_text', '')

                if not name:
                    continue

                # Extract kita ID from URL
                kita_id = 'unknown'
                if detail_url:
                    if match := re.search(r'/einrichtungen/(\d+)', detail_url):
                        kita_id = match.group(1)

                content_parts = [f"Name: {name}"]
                if full_text:
                    cleaned_text = self.clean_html_tags(full_text)
                    if cleaned_text:
                        content_parts.append(f"\nDescription:\n{cleaned_text}")

                full_content = "\n\n".join(content_parts)

                vector_doc = {
                    'id': f"kita_page_{kita_id}",
                    'type': 'kita_summary',
                    'name': name,
                    'content': full_content,
                    'metadata': {
                        'detail_url': detail_url,
                        'source': 'kita-navigator.berlin.de',
                        'crawled_at': data.get('crawled_at', ''),
                        'data_type': 'childcare_facility_summary'
                    }
                }

                vector_docs.append(vector_doc)

            return vector_docs

        except Exception as e:
            logger.error(f"Error extracting kita page content: {e}")
            return []

    def format_table_as_text(self, table: List[List[str]]) -> str:
        """Format table data as readable text"""
        if not table:
            return ""

        lines = []
        for row in table:
            if row:
                line = " | ".join(str(cell) for cell in row)
                lines.append(line)

        return "\n".join(lines)

    def process_kita_details(self) -> int:
        """Process all kita detail files"""
        logger.info("Processing kita detail files...")

        try:
            # List all kita detail files
            blobs = list(self.bucket.list_blobs(prefix='kita_navigator/details/'))
            json_blobs = [b for b in blobs if b.name.endswith('.json')]

            logger.info(f"Found {len(json_blobs)} kita detail files")

            processed = 0
            output_docs = []

            for blob in tqdm(json_blobs, desc="Processing kita details"):
                try:
                    # Download and parse JSON
                    json_data = blob.download_as_string()
                    data = json.loads(json_data)

                    # Extract vector content
                    vector_doc = self.extract_vector_content_from_kita_detail(data)

                    if vector_doc:
                        output_docs.append(vector_doc)
                        processed += 1

                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {e}")

            # Save all documents to a single file
            if output_docs:
                output_file = self.output_dir / "kitas" / "kita_details_vector_input.jsonl"
                with open(output_file, 'w', encoding='utf-8') as f:
                    for doc in output_docs:
                        f.write(json.dumps(doc, ensure_ascii=False) + '\n')

                logger.info(f"Saved {len(output_docs)} kita detail documents to {output_file}")

                # Also upload to GCS
                self.upload_to_gcs_vector_input(
                    "kitas/kita_details_vector_input.jsonl",
                    output_file.read_text(encoding='utf-8')
                )

            return processed

        except Exception as e:
            logger.error(f"Error processing kita details: {e}")
            return 0

    def process_kita_pages(self) -> int:
        """Process all kita page files"""
        logger.info("Processing kita page files...")

        try:
            # List all kita page files
            blobs = list(self.bucket.list_blobs(prefix='kita_navigator/pages/'))
            json_blobs = [b for b in blobs if b.name.endswith('.json')]

            logger.info(f"Found {len(json_blobs)} kita page files")

            processed = 0
            output_docs = []

            for blob in tqdm(json_blobs, desc="Processing kita pages"):
                try:
                    # Download and parse JSON
                    json_data = blob.download_as_string()
                    data = json.loads(json_data)

                    # Extract vector content
                    vector_docs = self.extract_vector_content_from_kita_page(data)

                    if vector_docs:
                        output_docs.extend(vector_docs)
                        processed += len(vector_docs)

                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {e}")

            # Save all documents to a single file
            if output_docs:
                output_file = self.output_dir / "kitas" / "kita_pages_vector_input.jsonl"
                with open(output_file, 'w', encoding='utf-8') as f:
                    for doc in output_docs:
                        f.write(json.dumps(doc, ensure_ascii=False) + '\n')

                logger.info(f"Saved {len(output_docs)} kita page documents to {output_file}")

                # Also upload to GCS
                self.upload_to_gcs_vector_input(
                    "kitas/kita_pages_vector_input.jsonl",
                    output_file.read_text(encoding='utf-8')
                )

            return processed

        except Exception as e:
            logger.error(f"Error processing kita pages: {e}")
            return 0

    def process_schools(self) -> int:
        """Process all school files"""
        logger.info("Processing school files...")

        try:
            # List all school files
            blobs = list(self.bucket.list_blobs(prefix='bildung_berlin/schools/'))
            json_blobs = [b for b in blobs if b.name.endswith('.json')]

            logger.info(f"Found {len(json_blobs)} school files")

            processed = 0
            output_docs = []

            for blob in tqdm(json_blobs, desc="Processing schools"):
                try:
                    # Download and parse JSON
                    json_data = blob.download_as_string()
                    data = json.loads(json_data)

                    # Extract vector content
                    vector_doc = self.extract_vector_content_from_school(data)

                    if vector_doc:
                        output_docs.append(vector_doc)
                        processed += 1

                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {e}")

            # Save all documents to a single file
            if output_docs:
                output_file = self.output_dir / "schools" / "schools_vector_input.jsonl"
                with open(output_file, 'w', encoding='utf-8') as f:
                    for doc in output_docs:
                        f.write(json.dumps(doc, ensure_ascii=False) + '\n')

                logger.info(f"Saved {len(output_docs)} school documents to {output_file}")

                # Also upload to GCS
                self.upload_to_gcs_vector_input(
                    "schools/schools_vector_input.jsonl",
                    output_file.read_text(encoding='utf-8')
                )

            return processed

        except Exception as e:
            logger.error(f"Error processing schools: {e}")
            return 0

    def process_sekundarschule_schools(self) -> int:
        """Process sekundarschule school files from schools/ folder"""
        logger.info("Processing sekundarschule school files...")

        try:
            # List all school files from the schools/ folder (not bildung_berlin)
            blobs = list(self.bucket.list_blobs(prefix='schools/'))
            json_blobs = [b for b in blobs if b.name.endswith('.json') and 'manifest.json' not in b.name]

            logger.info(f"Found {len(json_blobs)} sekundarschule school files")

            processed = 0
            output_docs = []

            for blob in tqdm(json_blobs, desc="Processing sekundarschule schools"):
                try:
                    # Download and parse JSON
                    json_data = blob.download_as_string()
                    data = json.loads(json_data)

                    # Extract vector content (use same method as bildung_berlin schools)
                    vector_doc = self.extract_vector_content_from_school(data)

                    if vector_doc:
                        # Update source to indicate it's from sekundarschule
                        vector_doc['metadata']['source'] = 'sekundarschule-berlin.de'
                        output_docs.append(vector_doc)
                        processed += 1

                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {e}")

            # Save all documents to a single file
            if output_docs:
                output_file = self.output_dir / "schools" / "sekundarschule_schools_vector_input.jsonl"
                with open(output_file, 'w', encoding='utf-8') as f:
                    for doc in output_docs:
                        f.write(json.dumps(doc, ensure_ascii=False) + '\n')

                logger.info(f"Saved {len(output_docs)} sekundarschule school documents to {output_file}")

                # Also upload to GCS
                self.upload_to_gcs_vector_input(
                    "schools/sekundarschule_schools_vector_input.jsonl",
                    output_file.read_text(encoding='utf-8')
                )

            return processed

        except Exception as e:
            logger.error(f"Error processing sekundarschule schools: {e}")
            return 0

    def upload_to_gcs_vector_input(self, path: str, content: str):
        """Upload processed data to GCS vector_database_input folder"""
        try:
            blob_name = f"vector_database_input/{path}"
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(content, content_type='application/x-ndjson')
            logger.info(f"Uploaded to GCS: {blob_name}")
        except Exception as e:
            logger.error(f"Failed to upload {path}: {e}")

    def run(self):
        """Main pipeline execution"""
        logger.info("Starting Vector Database Input Preparation Pipeline")

        # Initialize GCS
        self.initialize_gcs()

        # Process all data sources
        stats = {
            'kita_details': 0,
            'kita_pages': 0,
            'bildung_schools': 0,
            'sekundarschule_schools': 0
        }

        # Process kita details
        stats['kita_details'] = self.process_kita_details()

        # Process kita pages (optional - may be redundant with details)
        # Uncomment if you want to include page summaries
        # stats['kita_pages'] = self.process_kita_pages()

        # Process bildung_berlin schools
        stats['bildung_schools'] = self.process_schools()

        # Process sekundarschule schools
        stats['sekundarschule_schools'] = self.process_sekundarschule_schools()

        # Create summary manifest
        manifest = {
            'preparation_date': datetime.now().isoformat(),
            'run_timestamp': self.run_timestamp,
            'total_documents': sum(stats.values()),
            'documents_by_source': stats,
            'output_format': 'jsonl (JSON Lines)',
            'schema': {
                'id': 'Unique identifier for the document',
                'type': 'Document type (kita, school, etc.)',
                'name': 'Name of the entity',
                'content': 'Clean text content for vector embedding',
                'metadata': 'Additional metadata (URL, source, dates, etc.)'
            }
        }

        # Save manifest locally
        manifest_path = self.output_dir / "metadata" / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        logger.info(f"Manifest saved: {manifest_path}")

        # Upload manifest to GCS
        self.upload_to_gcs_vector_input(
            "metadata/manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2)
        )

        logger.info(f"\n=== Pipeline Completed ===")
        logger.info(f"Total documents prepared: {sum(stats.values())}")
        logger.info(f"  - Kita details: {stats['kita_details']}")
        logger.info(f"  - Kita pages: {stats['kita_pages']}")
        logger.info(f"  - Bildung schools: {stats['bildung_schools']}")
        logger.info(f"  - Sekundarschule schools: {stats['sekundarschule_schools']}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"GCS folder: gs://{self.gcs_bucket_name}/vector_database_input/")


def main():
    """Main entry point"""
    # Configuration
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    LOCAL_OUTPUT = True

    preparer = VectorDBPreparer(
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        local_output=LOCAL_OUTPUT
    )

    preparer.run()


if __name__ == "__main__":
    main()
