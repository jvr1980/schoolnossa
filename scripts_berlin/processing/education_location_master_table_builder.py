#!/usr/bin/env python3
"""
Education Location Master Table Builder
Combines kitas and schools into a unified master parquet file
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
from tqdm import tqdm
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EducationLocationMasterTableBuilder:
    """Builds master table of education locations from kitas and schools"""

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

        # Create output directory
        if self.local_output:
            self.output_dir = Path("master_list")
            self.output_dir.mkdir(parents=True, exist_ok=True)

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
            logger.warning(f"GCS initialization failed: {e}. Will use local files only.")
            self.bucket = None

    def load_jsonl_from_gcs(self, blob_path: str) -> List[Dict]:
        """Load JSONL file from GCS"""
        if not self.bucket:
            return []

        try:
            blob = self.bucket.blob(blob_path)
            if not blob.exists():
                logger.warning(f"Blob does not exist: {blob_path}")
                return []

            content = blob.download_as_text()
            documents = []
            for line in content.strip().split('\n'):
                if line.strip():
                    documents.append(json.loads(line))

            logger.info(f"Loaded {len(documents)} documents from {blob_path}")
            return documents
        except Exception as e:
            logger.error(f"Error loading {blob_path}: {e}")
            return []

    def load_json_files_from_gcs_folder(self, folder_prefix: str, max_files: Optional[int] = None) -> List[Dict]:
        """Load all JSON files from a GCS folder"""
        if not self.bucket:
            return []

        try:
            blobs = self.bucket.list_blobs(prefix=folder_prefix)
            documents = []
            count = 0

            for blob in blobs:
                if blob.name.endswith('.json') and 'manifest' not in blob.name:
                    try:
                        content = blob.download_as_text()
                        doc = json.loads(content)
                        documents.append(doc)
                        count += 1

                        if max_files and count >= max_files:
                            break
                    except Exception as e:
                        logger.debug(f"Error loading {blob.name}: {e}")

            logger.info(f"Loaded {len(documents)} JSON files from {folder_prefix}")
            return documents
        except Exception as e:
            logger.error(f"Error loading from folder {folder_prefix}: {e}")
            return []

    def load_jsonl_from_local(self, file_path: str) -> List[Dict]:
        """Load JSONL file from local filesystem"""
        try:
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"File does not exist: {file_path}")
                return []

            documents = []
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        documents.append(json.loads(line))

            logger.info(f"Loaded {len(documents)} documents from {file_path}")
            return documents
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []

    def extract_kita_records(self, kita_docs: List[Dict]) -> List[Dict]:
        """Extract standardized records from kita detail documents"""
        records = []

        for doc in tqdm(kita_docs, desc="Processing kitas"):
            try:
                # Extract address and homepage from HTML
                address = ''
                kita_homepage = ''
                html = doc.get('html', '')

                if html:
                    soup = BeautifulSoup(html, 'html.parser')

                    # Extract homepage from anchor tag
                    # Look for pattern: Homepage: <a href="...">
                    homepage_links = soup.find_all('a', href=True)
                    for link in homepage_links:
                        href = link.get('href', '')
                        # Skip kita-navigator links and email links
                        if href and not 'kita-navigator' in href and not href.startswith('mailto:'):
                            # Check if this is preceded by "Homepage:" text
                            prev_text = link.find_previous(string=True)
                            if prev_text and 'Homepage' in prev_text:
                                kita_homepage = href
                                # Ensure it has http(s) prefix
                                if not kita_homepage.startswith('http'):
                                    kita_homepage = f"http://{kita_homepage}"
                                break

                    # Extract address from text
                    text = soup.get_text()
                    lines = [l.strip() for l in text.split('\n') if l.strip()]

                    # Look for address pattern: Street Number, Postal Berlin
                    # Address is typically formatted like: "Lobeckstr. 11, 10969 Berlin"
                    # Use a pattern that captures the full address including potential prefix text
                    addr_pattern = r'([\wäöüß\s-]*(?:straße|str\.|platz|weg|allee|damm|chaussee|ring|ufer))\s+(\d+[-\d]*)\s*,\s*(\d{5})\s+(Berlin)'

                    match = re.search(addr_pattern, text, re.IGNORECASE)
                    if match:
                        # Clean the street part - find the street name by looking for last capital letter
                        street_part = match.group(1).strip()
                        # Find the position of the last capital letter that starts a word
                        # This handles cases like "errorLobeckstr." -> "Lobeckstr."
                        # Look for pattern where first char is uppercase, followed by lowercase, then street suffix
                        cap_match = None
                        for m in re.finditer(r'[A-ZÄÖÜ][a-zäöüß-]*(?:straße|str\.|platz|weg|allee|damm|chaussee|ring|ufer)', street_part):
                            # Only accept if the match actually starts with an uppercase letter
                            if m.group(0)[0].isupper():
                                cap_match = m

                        if cap_match:
                            street_clean = cap_match.group(0)
                            address = f"{street_clean} {match.group(2)}, {match.group(3)} {match.group(4)}"

                    # If pattern matching failed, look for any line containing Berlin postal code
                    if not address:
                        for line in lines:
                            if re.search(r'\d{5}\s+Berlin', line):
                                # Check previous line for street
                                idx = lines.index(line)
                                if idx > 0:
                                    prev_line = lines[idx-1]
                                    if re.search(r'\d+[-\d]*', prev_line):  # Has a number (likely street number)
                                        address = f"{prev_line}, {line}"
                                        break

                # Fallback: Try to extract from doc.get('address') if HTML parsing failed
                if not address:
                    address = doc.get('address', '')

                    # If still empty, try to find address in tables
                    if not address:
                        for table in doc.get('tables', []):
                            for row in table:
                                if any('adresse' in str(cell).lower() for cell in row):
                                    if len(row) > 1:
                                        address = row[1]
                                        break
                            if address:
                                break

                # Use the actual kita homepage if found, otherwise fall back to kita-navigator URL
                website = kita_homepage if kita_homepage else doc.get('url', '')

                record = {
                    'edu_location_id': doc.get('kita_id', ''),
                    'edu_location_name': doc.get('kita_name', ''),
                    'edu_location_type': 'kita',
                    'edu_location_address': address,
                    'edu_location_website': website,
                    'source': 'kita-navigator.berlin.de',
                    'crawled_at': doc.get('metadata', {}).get('crawled_at', '')
                }

                # Only add if we have at least a name or ID
                if record['edu_location_name'] or record['edu_location_id']:
                    records.append(record)
            except Exception as e:
                logger.error(f"Error processing kita document: {e}")

        return records

    def extract_school_records(self, school_docs: List[Dict], source: str = '') -> List[Dict]:
        """Extract standardized records from school documents"""
        records = []

        for doc in tqdm(school_docs, desc=f"Processing schools ({source})"):
            try:
                # Extract address and website from HTML
                address = ''
                school_website = ''
                html = doc.get('html', '')

                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    text = soup.get_text()
                    lines = [l.strip() for l in text.split('\n') if l.strip()]

                    # Look for address pattern near "Stadtplan"
                    # Address format: Street Number on one line, Postcode City (District) on next line
                    for i, line in enumerate(lines):
                        if 'Stadtplan' in line and i > 0 and i < len(lines) - 1:
                            street_line = lines[i-1].strip()
                            postal_line = lines[i+1].strip()

                            # Check if this looks like a valid address
                            # Street should have a number, postal should have 5-digit code
                            if re.search(r'\d+[-\d]*', street_line) and re.search(r'\d{5}\s+Berlin', postal_line):
                                # Combine street and postal
                                address = f"{street_line}, {postal_line}"
                                break

                    # Look for "Web" field to extract actual school website
                    for i, line in enumerate(lines):
                        if line == 'Web' and i < len(lines) - 1:
                            # Next line should be the website
                            potential_website = lines[i+1].strip()
                            # Check if it looks like a URL or domain
                            if '.' in potential_website and not potential_website.startswith('http'):
                                # Add http:// if not present
                                school_website = f"http://{potential_website}"
                            elif potential_website.startswith('http'):
                                school_website = potential_website
                            break

                # Fallback: Try to extract from markdown if HTML extraction failed
                if not address:
                    markdown = doc.get('markdown', '')
                    # Look for pattern: Street Number, Postcode City
                    addr_pattern = r'([A-ZÄÖÜ][a-zäöüß\s-]+(?:straße|str\.|platz|weg|allee|damm|chaussee|ring))\s+(\d+[-\d]*)[,\s]+(\d{5})\s+(Berlin(?:\s+\([^)]+\))?)'
                    matches = re.findall(addr_pattern, markdown, re.IGNORECASE)
                    if matches:
                        match = matches[0]
                        address = f"{match[0]} {match[1]}, {match[2]} {match[3]}"

                # Use the actual school website if found, otherwise fall back to the bildung.berlin profile URL
                website = school_website if school_website else doc.get('url', '')

                record = {
                    'edu_location_id': doc.get('school_id', doc.get('id', '')),
                    'edu_location_name': doc.get('school_name', doc.get('name', '')),
                    'edu_location_type': 'school',
                    'edu_location_address': address,
                    'edu_location_website': website,
                    'source': source if source else doc.get('metadata', {}).get('source', ''),
                    'crawled_at': doc.get('metadata', {}).get('crawled_at', '')
                }

                # Only add if we have at least a name or ID
                if record['edu_location_name'] or record['edu_location_id']:
                    records.append(record)
            except Exception as e:
                logger.error(f"Error processing school document: {e}")

        return records

    def deduplicate_records(self, records: List[Dict]) -> List[Dict]:
        """Remove duplicate records based on location ID"""
        seen_ids = set()
        unique_records = []
        duplicates = 0

        for record in records:
            location_id = record.get('edu_location_id', '')

            # Skip if no ID
            if not location_id:
                unique_records.append(record)
                continue

            # Skip if duplicate
            if location_id in seen_ids:
                duplicates += 1
                continue

            seen_ids.add(location_id)
            unique_records.append(record)

        logger.info(f"Removed {duplicates} duplicate records")
        return unique_records

    def build_master_table(self) -> pd.DataFrame:
        """Build master table from all sources"""
        logger.info("Building education location master table")

        all_records = []

        # Load kita data from original detail files
        logger.info("Loading kita detail data...")
        kita_docs = []

        # Try GCS first - load from original kita detail JSON files
        if self.bucket:
            kita_docs = self.load_json_files_from_gcs_folder('kita_navigator/details/')

        if kita_docs:
            kita_records = self.extract_kita_records(kita_docs)
            all_records.extend(kita_records)
            logger.info(f"Added {len(kita_records)} kita records")

        # Load school data (bildung.berlin.de) - from original JSON files
        logger.info("Loading bildung.berlin.de school data...")
        school_docs = []

        # Try GCS first - load from original school JSON files
        if self.bucket:
            school_docs = self.load_json_files_from_gcs_folder('bildung_berlin/schools/')

        if school_docs:
            school_records = self.extract_school_records(school_docs, source='www.bildung.berlin.de')
            all_records.extend(school_records)
            logger.info(f"Added {len(school_records)} bildung.berlin.de school records")

        # Load school data (sekundarschule-berlin.de) - from original JSON files
        logger.info("Loading sekundarschule-berlin.de school data...")
        sekundarschule_docs = []

        # Try GCS first - load from original school JSON files
        if self.bucket:
            sekundarschule_docs = self.load_json_files_from_gcs_folder('schools/')

        if sekundarschule_docs:
            sekundarschule_records = self.extract_school_records(sekundarschule_docs, source='sekundarschule-berlin.de')
            all_records.extend(sekundarschule_records)
            logger.info(f"Added {len(sekundarschule_records)} sekundarschule-berlin.de school records")

        # Deduplicate
        logger.info(f"Total records before deduplication: {len(all_records)}")
        unique_records = self.deduplicate_records(all_records)
        logger.info(f"Unique records after deduplication: {len(unique_records)}")

        # Create DataFrame
        df = pd.DataFrame(unique_records)

        # Ensure correct column order
        column_order = [
            'edu_location_id',
            'edu_location_name',
            'edu_location_type',
            'edu_location_address',
            'edu_location_website',
            'source',
            'crawled_at'
        ]

        df = df[column_order]

        return df

    def save_to_parquet(self, df: pd.DataFrame, filename: str) -> bool:
        """Save DataFrame to parquet file locally"""
        try:
            filepath = self.output_dir / filename
            df.to_parquet(filepath, index=False, engine='pyarrow')
            logger.info(f"Saved parquet file locally: {filepath}")
            logger.info(f"File size: {filepath.stat().st_size / 1024 / 1024:.2f} MB")
            return True
        except Exception as e:
            logger.error(f"Failed to save parquet file {filename}: {e}")
            return False

    def upload_parquet_to_gcs(self, local_path: Path, blob_name: str) -> bool:
        """Upload parquet file to GCS"""
        if not self.bucket:
            logger.warning("GCS bucket not initialized, skipping upload")
            return False

        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(str(local_path), content_type='application/octet-stream')
            logger.info(f"Uploaded parquet to GCS: gs://{self.gcs_bucket_name}/{blob_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {e}")
            return False

    def create_manifest(self, df: pd.DataFrame) -> Dict:
        """Create manifest with processing metadata"""
        return {
            'creation_date': datetime.now().isoformat(),
            'run_timestamp': self.run_timestamp,
            'total_locations': len(df),
            'locations_by_type': df['edu_location_type'].value_counts().to_dict(),
            'locations_by_source': df['source'].value_counts().to_dict(),
            'columns': list(df.columns),
            'records_with_address': int(df['edu_location_address'].notna().sum()),
            'records_with_website': int(df['edu_location_website'].notna().sum()),
            'schema': {
                'edu_location_id': 'Unique identifier for the education location',
                'edu_location_name': 'Name of the kita or school',
                'edu_location_type': 'Type: kita or school',
                'edu_location_address': 'Physical address',
                'edu_location_website': 'Website URL',
                'source': 'Original data source',
                'crawled_at': 'Timestamp when data was crawled'
            }
        }

    def run(self):
        """Main execution"""
        logger.info("Starting Education Location Master Table Builder")

        # Initialize GCS
        self.initialize_gcs()

        # Build master table
        df = self.build_master_table()

        if df.empty:
            logger.error("No data to process!")
            return

        # Display summary
        logger.info(f"\n{'='*60}")
        logger.info(f"Master Table Summary:")
        logger.info(f"{'='*60}")
        logger.info(f"Total locations: {len(df)}")
        logger.info(f"\nBy type:")
        for loc_type, count in df['edu_location_type'].value_counts().items():
            logger.info(f"  {loc_type}: {count}")
        logger.info(f"\nBy source:")
        for source, count in df['source'].value_counts().items():
            logger.info(f"  {source}: {count}")
        logger.info(f"\nData completeness:")
        logger.info(f"  With address: {df['edu_location_address'].notna().sum()} ({df['edu_location_address'].notna().sum() / len(df) * 100:.1f}%)")
        logger.info(f"  With website: {df['edu_location_website'].notna().sum()} ({df['edu_location_website'].notna().sum() / len(df) * 100:.1f}%)")
        logger.info(f"{'='*60}\n")

        # Save to parquet
        filename = f"education_locations_berlin_{self.run_timestamp}.parquet"

        if self.local_output:
            if self.save_to_parquet(df, filename):
                local_path = self.output_dir / filename

                # Upload to GCS
                if self.bucket:
                    blob_name = f"master_list/{filename}"
                    self.upload_parquet_to_gcs(local_path, blob_name)

        # Create and save manifest
        manifest = self.create_manifest(df)

        # Save manifest locally
        if self.local_output:
            manifest_path = self.output_dir / f"manifest_{self.run_timestamp}.json"
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            logger.info(f"Manifest saved locally: {manifest_path}")

        # Upload manifest to GCS
        if self.bucket:
            manifest_blob = self.bucket.blob(f"master_list/manifest_{self.run_timestamp}.json")
            manifest_blob.upload_from_string(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            logger.info(f"Manifest uploaded to GCS")

        logger.info("\n✅ Master table creation completed successfully!")


def main():
    """Main entry point"""
    # Configuration
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    LOCAL_OUTPUT = True

    builder = EducationLocationMasterTableBuilder(
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        local_output=LOCAL_OUTPUT
    )

    builder.run()


if __name__ == "__main__":
    main()
