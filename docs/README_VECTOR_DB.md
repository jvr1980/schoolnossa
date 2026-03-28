# Vector Database Input Preparation Pipeline

This pipeline prepares crawled data from various Berlin open data sources for ingestion into ChromaDB vector database.

## Overview

The pipeline extracts clean, semantic content from raw JSON files stored in Google Cloud Storage and prepares it for vector embedding and semantic search. It removes HTML noise while preserving meaningful content and metadata.

## Data Sources Processed

### 1. Kita Navigator Data
- **Source**: kita-navigator.berlin.de
- **Location in GCS**: `gs://schoolnossa-berlin/kita_navigator/`
- **Content**:
  - **Detail pages** (`details/`): Rich information about individual kitas including addresses, contact info, opening hours, descriptions
  - **Page summaries** (`pages/`): Brief summaries from listing pages (optional, may be redundant with details)
- **Total documents**: ~2,168 kita detail pages

### 2. School Data (Bildung Berlin)
- **Source**: www.bildung.berlin.de
- **Location in GCS**: `gs://schoolnossa-berlin/bildung_berlin/schools/`
- **Content**: Detailed school information including descriptions, programs, facilities
- **Total documents**: Multiple school JSON files

### 3. School Data (Sekundarschule Berlin)
- **Source**: sekundarschule-berlin.de
- **Location in GCS**: `gs://schoolnossa-berlin/schools/`
- **Content**: Secondary school information
- **Total documents**: Multiple school JSON files

### 4. Crime Statistics (Not yet processed)
- **Source**: daten.berlin.de
- **Location in GCS**: `gs://schoolnossa-berlin/crime_data/`
- **Content**: Excel files with crime statistics by district (2015-2024)
- **Note**: Binary Excel format, would need separate processing for vector DB

### 5. Traffic Data (Not yet processed)
- **Source**: berlin-zaehlt.de
- **Location in GCS**: `gs://schoolnossa-berlin/traffic_data/`
- **Content**: CSV.GZ files with bicycle and car traffic counts
- **Note**: Tabular time-series data, would need separate processing for vector DB

## Pipeline Architecture

### Input Format
- **Raw data**: JSON files with HTML, markdown, cleaned text, and structured fields
- **Example raw kita detail structure**:
  ```json
  {
    "kita_id": "100017",
    "kita_name": "Kita Example",
    "url": "https://...",
    "title": "...",
    "address": "Street 123, Berlin",
    "contact": {"text": "..."},
    "opening_hours": "Mon-Fri 7-17",
    "markdown": "...",
    "html": "...",
    "cleaned_text": "...",
    "tables": [[...]]
  }
  ```

### Processing Steps

1. **HTML Removal**: Strip all HTML tags from content
2. **Markdown Extraction**: Prefer markdown over HTML for cleaner text
3. **Structured Data Extraction**:
   - Name, address, contact info
   - Metadata (URLs, sources, dates)
   - Table data converted to readable text
4. **Content Consolidation**: Combine all relevant text into single content field
5. **Metadata Preservation**: Keep references to original sources and IDs

### Output Format

**JSONL (JSON Lines)** - One JSON object per line for efficient streaming

Example output document:
```json
{
  "id": "100017",
  "type": "kita",
  "name": "Kita Example",
  "content": "Name: Kita Example\n\nAddress: Street 123, Berlin\n\nContact: ...\n\nOpening Hours: Mon-Fri 7-17\n\nContent:\n[Clean markdown content here...]",
  "metadata": {
    "url": "https://kita-navigator.berlin.de/einrichtungen/100017",
    "address": "Street 123, Berlin",
    "source": "kita-navigator.berlin.de",
    "crawled_at": "2025-11-16T...",
    "data_type": "childcare_facility"
  }
}
```

## Output Structure

### Local Output
```
vector_db_input/
├── kitas/
│   ├── kita_details_vector_input.jsonl      # ~2,168 kita documents
│   └── kita_pages_vector_input.jsonl        # (optional) page summaries
├── schools/
│   ├── schools_vector_input.jsonl           # Bildung Berlin schools
│   └── sekundarschule_schools_vector_input.jsonl  # Sekundarschule schools
└── metadata/
    └── manifest.json                         # Pipeline metadata
```

### GCS Output
```
gs://schoolnossa-berlin/vector_database_input/
├── kitas/
│   ├── kita_details_vector_input.jsonl
│   └── kita_pages_vector_input.jsonl
├── schools/
│   ├── schools_vector_input.jsonl
│   └── sekundarschule_schools_vector_input.jsonl
└── metadata/
    └── manifest.json
```

## Running the Pipeline

### Prerequisites
```bash
# Install dependencies (already in requirements)
pip install google-cloud-storage tqdm python-dotenv

# Set up GCS credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/gcs-credentials.json"

# Or create .env file with credentials
```

### Execute Pipeline
```bash
# Using virtual environment
venv/bin/python prepare_vector_db_input.py

# Or direct python
python3 prepare_vector_db_input.py
```

### Configuration
Edit the `main()` function in `prepare_vector_db_input.py`:
```python
GCS_BUCKET_NAME = "schoolnossa-berlin"
GCS_PROJECT_ID = "schoolnossa"
LOCAL_OUTPUT = True  # Save local copy
```

## What Gets Removed

The pipeline removes HTML-specific elements to keep only semantic content:
- ❌ HTML tags (`<div>`, `<span>`, `<p>`, etc.)
- ❌ JavaScript and CSS
- ❌ Navigation elements
- ❌ Boilerplate/footer content
- ❌ Excess whitespace

## What Gets Kept

The pipeline preserves meaningful content:
- ✅ Markdown-formatted content
- ✅ Cleaned text content
- ✅ Structured data (names, addresses, contacts)
- ✅ Table data (converted to readable text format)
- ✅ Metadata (URLs, sources, timestamps)
- ✅ Document IDs for linking back to original data

## Original HTML Storage

The raw HTML is **preserved** in the original JSON files:
- **GCS location**: `gs://schoolnossa-berlin/kita_navigator/`, `bildung_berlin/`, etc.
- **Local archives**: `data_archive/kita_navigator_*/`, `kita_details_*/`
- **Purpose**: Backup for future re-processing or structured database extraction

## Next Steps: ChromaDB Integration

After the pipeline completes, the cleaned data can be loaded into ChromaDB:

```python
import chromadb
import json

# Initialize ChromaDB
client = chromadb.Client()
collection = client.create_collection("berlin_education_childcare")

# Load JSONL file
with open("vector_db_input/kitas/kita_details_vector_input.jsonl") as f:
    for line in f:
        doc = json.loads(line)
        collection.add(
            documents=[doc["content"]],
            metadatas=[doc["metadata"]],
            ids=[doc["id"]]
        )
```

## Pipeline Manifest

The manifest file (`vector_db_input/metadata/manifest.json`) contains:
```json
{
  "preparation_date": "2025-11-16T...",
  "run_timestamp": "20251116_200743",
  "total_documents": 2500,
  "documents_by_source": {
    "kita_details": 2168,
    "kita_pages": 0,
    "bildung_schools": 200,
    "sekundarschule_schools": 132
  },
  "output_format": "jsonl (JSON Lines)",
  "schema": {
    "id": "Unique identifier for the document",
    "type": "Document type (kita, school, etc.)",
    "name": "Name of the entity",
    "content": "Clean text content for vector embedding",
    "metadata": "Additional metadata (URL, source, dates, etc.)"
  }
}
```

## Performance

- **Processing speed**: ~2-3 documents per second
- **Kita details** (2,168 files): ~12-15 minutes
- **Schools**: ~5-10 minutes
- **Total pipeline**: ~20-30 minutes

## Troubleshooting

### GCS Authentication Errors
```bash
# Check credentials
gcloud auth application-default login

# Or set service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
```

### Missing Dependencies
```bash
pip install -r requirements.txt
# or
pip install google-cloud-storage tqdm python-dotenv
```

### Memory Issues
The pipeline processes files one at a time and writes incrementally, so memory usage should be minimal. If issues occur, reduce batch sizes in the code.

## Data Quality

### Content Cleaning Quality
- **Markdown**: Preferred when available (highest quality)
- **Cleaned text**: Fallback when markdown missing
- **HTML**: Last resort (gets stripped)

### Metadata Integrity
- All documents retain original URLs for traceability
- Timestamps preserve crawl dates
- IDs enable linking to structured database

## Future Enhancements

1. **Crime data processing**: Convert Excel to searchable text summaries
2. **Traffic data processing**: Aggregate and summarize time-series data
3. **Multilingual support**: Detect and tag content language
4. **Entity extraction**: Extract and tag locations, dates, names
5. **Quality scoring**: Add relevance/quality scores to documents
6. **Incremental updates**: Process only new/changed files

## Related Files

- **Pipeline script**: `prepare_vector_db_input.py`
- **Original crawlers**:
  - `crawl_kita_navigator.py`
  - `crawl_kita_details_from_archive.py`
  - `crawl_school_bildungberlin.py`
  - `crawl_schools_sekundarschule-berlin.py`
  - `crawl_crime_daten_berlin.py`
  - `crawl_traffic_berlin_zaehlt.py`

## Questions?

For questions or issues with the pipeline:
1. Check the manifest file for processing statistics
2. Review error logs in terminal output
3. Verify GCS bucket access and permissions
4. Check sample output files to validate format
