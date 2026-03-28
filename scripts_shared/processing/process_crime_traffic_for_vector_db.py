#!/usr/bin/env python3
"""
Crime and Traffic Data Processor for Vector Database
Processes Excel crime data and CSV traffic data into vector-searchable documents
Focus: Recent values (2024/2023) with year-over-year changes at geographic granularity
"""

import asyncio
import gzip
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
import pandas as pd

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


class CrimeTrafficVectorProcessor:
    """Processes crime and traffic data for ChromaDB"""

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
            (self.output_dir / "safety_traffic").mkdir(exist_ok=True)
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

    def process_crime_data(self) -> List[Dict]:
        """Process crime statistics Excel file"""
        logger.info("Processing crime data...")

        try:
            # Download Excel file
            crime_blobs = list(self.bucket.list_blobs(prefix='crime_data/'))
            xlsx_blobs = [b for b in crime_blobs if b.name.endswith('.xlsx')]

            if not xlsx_blobs:
                logger.error("No crime Excel file found")
                return []

            # Download the Excel file
            excel_blob = xlsx_blobs[0]
            temp_path = Path("/tmp/crime_data.xlsx")
            excel_blob.download_to_filename(temp_path)
            logger.info(f"Downloaded: {excel_blob.name}")

            # Load 2024 and 2023 data
            df_2024 = pd.read_excel(temp_path, sheet_name='Fallzahlen_2024', header=None, skiprows=4)
            df_2024.columns = df_2024.iloc[0]
            df_2024 = df_2024[1:].reset_index(drop=True)

            df_2023 = pd.read_excel(temp_path, sheet_name='Fallzahlen_2023', header=None, skiprows=4)
            df_2023.columns = df_2023.iloc[0]
            df_2023 = df_2023[1:].reset_index(drop=True)

            # Clean column names
            lor_col = 'LOR-Schlüssel (Bezirksregion)'
            name_col = 'Bezeichnung (Bezirksregion)'
            total_col = 'Straftaten \n-insgesamt-'

            # Create vector documents for each region
            vector_docs = []

            for idx, row_2024 in df_2024.iterrows():
                try:
                    lor_code = str(row_2024[lor_col]).strip()
                    region_name = str(row_2024[name_col]).strip()

                    # Skip if LOR code is not valid (summaries, headers)
                    if not lor_code or lor_code == 'nan' or len(lor_code) != 6:
                        continue

                    # Get corresponding 2023 data
                    row_2023 = df_2023[df_2023[lor_col] == lor_code]

                    if row_2023.empty:
                        continue

                    row_2023 = row_2023.iloc[0]

                    # Extract key crime metrics
                    crime_metrics = {}
                    yoy_changes = {}

                    # Key columns to extract
                    key_columns = {
                        'Straftaten \n-insgesamt-': 'Total Crimes',
                        'Raub': 'Robbery',
                        'Straßenraub,\nHandtaschen-raub': 'Street Robbery',
                        'Körper-verletzungen \n-insgesamt-': 'Bodily Injury',
                        'Gefährl. und schwere Körper-verletzung': 'Serious Bodily Injury',
                        'Diebstahl \n-insgesamt-': 'Theft Total',
                        'Wohnraum-\neinbruch': 'Burglary',
                        'Fahrrad-\ndiebstahl': 'Bicycle Theft',
                        'Kieztaten': 'Neighborhood Crimes'
                    }

                    for col_name, metric_name in key_columns.items():
                        if col_name in df_2024.columns:
                            val_2024 = pd.to_numeric(row_2024[col_name], errors='coerce')
                            val_2023 = pd.to_numeric(row_2023[col_name], errors='coerce')

                            if pd.notna(val_2024):
                                crime_metrics[metric_name] = int(val_2024)

                                # Calculate year-over-year change
                                if pd.notna(val_2023) and val_2023 > 0:
                                    pct_change = ((val_2024 - val_2023) / val_2023) * 100
                                    yoy_changes[metric_name] = round(pct_change, 1)

                    # Build content for vector search
                    content_parts = [
                        f"Crime Statistics for {region_name}",
                        f"LOR Code: {lor_code}",
                        f"\n2024 Crime Data (with year-over-year change from 2023):\n"
                    ]

                    for metric, value in crime_metrics.items():
                        change_str = ""
                        if metric in yoy_changes:
                            change = yoy_changes[metric]
                            direction = "increase" if change > 0 else "decrease"
                            change_str = f" ({abs(change):.1f}% {direction} from 2023)"
                        content_parts.append(f"- {metric}: {value:,}{change_str}")

                    # Add context
                    total_crimes = crime_metrics.get('Total Crimes', 0)
                    total_change = yoy_changes.get('Total Crimes', 0)

                    if total_change > 10:
                        content_parts.append(f"\nSafety Trend: Crime increased significantly by {total_change:.1f}% in {region_name}.")
                    elif total_change < -10:
                        content_parts.append(f"\nSafety Trend: Crime decreased significantly by {abs(total_change):.1f}% in {region_name}, indicating improved safety.")
                    else:
                        content_parts.append(f"\nSafety Trend: Crime rates remained relatively stable in {region_name}.")

                    full_content = "\n".join(content_parts)

                    # Determine district from LOR code (first 2 digits)
                    district_map = {
                        '01': 'Mitte', '02': 'Friedrichshain-Kreuzberg', '03': 'Pankow',
                        '04': 'Charlottenburg-Wilmersdorf', '05': 'Spandau', '06': 'Steglitz-Zehlendorf',
                        '07': 'Tempelhof-Schöneberg', '08': 'Neukölln', '09': 'Treptow-Köpenick',
                        '10': 'Marzahn-Hellersdorf', '11': 'Lichtenberg', '12': 'Reinickendorf'
                    }
                    district = district_map.get(lor_code[:2], 'Unknown')

                    # Create vector document
                    vector_doc = {
                        'id': f"crime_{lor_code}",
                        'type': 'crime_statistics',
                        'name': f"Crime Statistics - {region_name}",
                        'content': full_content,
                        'metadata': {
                            'lor_code': lor_code,
                            'region_name': region_name,
                            'district': district,
                            'year': 2024,
                            'total_crimes_2024': crime_metrics.get('Total Crimes', 0),
                            'total_crimes_change_pct': yoy_changes.get('Total Crimes', 0),
                            'source': 'kriminalitaetsatlas.berlin.de',
                            'data_type': 'safety_crime_statistics',
                            'geo_level': 'bezirksregion'  # Matches schools/kitas
                        },
                        'crime_metrics_2024': crime_metrics,
                        'yoy_changes': yoy_changes
                    }

                    vector_docs.append(vector_doc)

                except Exception as e:
                    logger.error(f"Error processing crime row {idx}: {e}")
                    continue

            logger.info(f"Processed {len(vector_docs)} crime statistics documents")
            return vector_docs

        except Exception as e:
            logger.error(f"Error processing crime data: {e}")
            return []

    def process_traffic_data(self) -> List[Dict]:
        """Process traffic count data (bicycle and car)"""
        logger.info("Processing traffic data...")

        try:
            # Download GeoJSON metadata for locations
            metadata_blobs = list(self.bucket.list_blobs(prefix='traffic_data/metadata/'))

            # Process bicycle traffic (Ecocounter)
            ecocounter_geojson = None
            telraam_geojson = None

            for blob in metadata_blobs:
                if 'ecocounter_segments.geojson' in blob.name:
                    temp_eco = Path("/tmp/ecocounter_segments.geojson")
                    blob.download_to_filename(temp_eco)
                    with open(temp_eco) as f:
                        ecocounter_geojson = json.load(f)
                elif 'telraam_segments.geojson' in blob.name:
                    temp_tel = Path("/tmp/telraam_segments.geojson")
                    blob.download_to_filename(temp_tel)
                    with open(temp_tel) as f:
                        telraam_geojson = json.load(f)

            vector_docs = []

            # Process Ecocounter (bicycle) data
            if ecocounter_geojson:
                docs = self._process_ecocounter_data(ecocounter_geojson)
                vector_docs.extend(docs)

            # Process Telraam (car) data
            if telraam_geojson:
                docs = self._process_telraam_data(telraam_geojson)
                vector_docs.extend(docs)

            logger.info(f"Processed {len(vector_docs)} traffic monitoring documents")
            return vector_docs

        except Exception as e:
            logger.error(f"Error processing traffic data: {e}")
            return []

    def _process_ecocounter_data(self, geojson: Dict) -> List[Dict]:
        """Process bicycle traffic counting stations"""
        vector_docs = []

        # Get list of CSV files
        csv_blobs = list(self.bucket.list_blobs(prefix='traffic_data/ecocounter/'))
        csv_files = [b for b in csv_blobs if b.name.endswith('.csv.gz')]

        if not csv_files:
            logger.warning("No ecocounter CSV files found")
            return []

        # Sort by date, get most recent 2 years worth
        csv_files.sort(key=lambda x: x.name, reverse=True)
        recent_files = csv_files[:24]  # ~2 years of monthly data

        # Download and aggregate recent data
        logger.info(f"Processing {len(recent_files)} recent ecocounter files...")

        for feature in geojson.get('features', []):
            try:
                props = feature.get('properties', {})
                segment_id = props.get('segment_id')
                counter_info = props.get('counter', [{}])[0]

                name = counter_info.get('name', props.get('eco-counter', {}).get('nom', 'Unknown'))
                description = counter_info.get('description', '')
                lat = counter_info.get('lat', props.get('eco-counter', {}).get('lat'))
                lon = counter_info.get('lon', props.get('eco-counter', {}).get('lon'))

                # Build content
                content = f"Bicycle Traffic Counter: {name}\n"
                if description:
                    content += f"Location: {description}\n"
                content += f"Coordinates: {lat}, {lon}\n"
                content += f"Segment ID: {segment_id}\n\n"

                content += "This is a permanent bicycle traffic counting station monitoring cyclist activity.\n"
                content += f"Station Name: {name}\n"

                if description:
                    content += f"Located at: {description}\n"

                # Create document
                vector_doc = {
                    'id': f"bicycle_traffic_{segment_id}",
                    'type': 'bicycle_traffic',
                    'name': f"Bicycle Counter - {name}",
                    'content': content,
                    'metadata': {
                        'segment_id': str(segment_id),
                        'location_name': name,
                        'description': description,
                        'lat': lat,
                        'lon': lon,
                        'source': 'berlin-zaehlt.de',
                        'data_type': 'traffic_bicycle_monitoring',
                        'counter_type': 'ecocounter'
                    }
                }

                vector_docs.append(vector_doc)

            except Exception as e:
                logger.error(f"Error processing ecocounter feature: {e}")

        return vector_docs

    def _process_telraam_data(self, geojson: Dict) -> List[Dict]:
        """Process car traffic counting stations"""
        vector_docs = []

        for feature in geojson.get('features', []):
            try:
                props = feature.get('properties', {})
                segment_id = props.get('segment_id')

                # Get location info
                telraam_info = props.get('telraam', {})
                name = telraam_info.get('street', 'Unknown Street')
                lat = telraam_info.get('lat')
                lon = telraam_info.get('lon')

                # Build content
                content = f"Vehicle Traffic Counter: {name}\n"
                content += f"Coordinates: {lat}, {lon}\n"
                content += f"Segment ID: {segment_id}\n\n"

                content += "This is a vehicle traffic counting station monitoring car, truck, and other motorized vehicle activity.\n"
                content += f"Street: {name}\n"

                # Create document
                vector_doc = {
                    'id': f"vehicle_traffic_{segment_id}",
                    'type': 'vehicle_traffic',
                    'name': f"Vehicle Counter - {name}",
                    'content': content,
                    'metadata': {
                        'segment_id': str(segment_id),
                        'street_name': name,
                        'lat': lat,
                        'lon': lon,
                        'source': 'berlin-zaehlt.de',
                        'data_type': 'traffic_vehicle_monitoring',
                        'counter_type': 'telraam'
                    }
                }

                vector_docs.append(vector_doc)

            except Exception as e:
                logger.error(f"Error processing telraam feature: {e}")

        return vector_docs

    def save_documents(self, documents: List[Dict], filename: str):
        """Save documents to JSONL file"""
        if not documents:
            logger.warning(f"No documents to save for {filename}")
            return

        # Save locally
        output_file = self.output_dir / "safety_traffic" / filename
        with open(output_file, 'w', encoding='utf-8') as f:
            for doc in documents:
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')

        logger.info(f"Saved {len(documents)} documents to {output_file}")

        # Upload to GCS
        self.upload_to_gcs_vector_input(
            f"safety_traffic/{filename}",
            output_file.read_text(encoding='utf-8')
        )

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
        """Main processor execution"""
        logger.info("Starting Crime and Traffic Data Processing for Vector DB")

        # Initialize GCS
        self.initialize_gcs()

        # Process crime data
        crime_docs = self.process_crime_data()
        if crime_docs:
            self.save_documents(crime_docs, 'crime_statistics_vector_input.jsonl')

        # Process traffic data
        traffic_docs = self.process_traffic_data()
        if traffic_docs:
            self.save_documents(traffic_docs, 'traffic_monitoring_vector_input.jsonl')

        # Create summary manifest
        manifest = {
            'processing_date': datetime.now().isoformat(),
            'run_timestamp': self.run_timestamp,
            'total_documents': len(crime_docs) + len(traffic_docs),
            'documents_by_type': {
                'crime_statistics': len(crime_docs),
                'traffic_monitoring': len(traffic_docs)
            },
            'crime_data': {
                'years': '2024 vs 2023',
                'metrics': 'Total crimes, robbery, theft, burglary, bodily injury, etc.',
                'geographic_level': 'LOR Bezirksregion (matches schools)',
                'includes_yoy_changes': True
            },
            'traffic_data': {
                'types': 'Bicycle counters (ecocounter) and vehicle counters (telraam)',
                'geographic_level': 'Point locations (lat/lon)',
                'note': 'Monitoring station locations, not aggregated counts'
            }
        }

        # Save manifest
        manifest_path = self.output_dir / "safety_traffic" / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        logger.info(f"Manifest saved: {manifest_path}")

        # Upload manifest
        self.upload_to_gcs_vector_input(
            "safety_traffic/manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2)
        )

        logger.info(f"\n=== Processing Completed ===")
        logger.info(f"Crime statistics documents: {len(crime_docs)}")
        logger.info(f"Traffic monitoring documents: {len(traffic_docs)}")
        logger.info(f"Total documents: {len(crime_docs) + len(traffic_docs)}")
        logger.info(f"Output directory: {self.output_dir / 'safety_traffic'}")
        logger.info(f"GCS folder: gs://{self.gcs_bucket_name}/vector_database_input/safety_traffic/")


def main():
    """Main entry point"""
    # Configuration
    GCS_BUCKET_NAME = "schoolnossa-berlin"
    GCS_PROJECT_ID = "schoolnossa"
    LOCAL_OUTPUT = True

    processor = CrimeTrafficVectorProcessor(
        gcs_bucket_name=GCS_BUCKET_NAME,
        gcs_project_id=GCS_PROJECT_ID,
        local_output=LOCAL_OUTPUT
    )

    processor.run()


if __name__ == "__main__":
    main()
