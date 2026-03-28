# Berlin School Directory Crawler

This script crawls school information from the Berlin education website (bildung.berlin.de) using crawl4ai and stores the content in Google Cloud Storage for RAG (Retrieval-Augmented Generation) applications.

## Features

- Crawls all schools from the Berlin education directory
- Extracts structured data including school details, tables, and metadata
- Stores data in Google Cloud Storage as JSON files
- Local backup option for all crawled data
- Rate limiting to be respectful to the server
- Comprehensive error handling and logging
- Generates a manifest file with crawl metadata

## Prerequisites

- Python 3.8+
- Google Cloud Platform account (for GCS storage)
- GCP credentials configured

## Installation

1. Install dependencies:
```bash
pip install crawl4ai google-cloud-storage beautifulsoup4 lxml aiohttp tqdm
```

Or use the requirements from the existing project:
```bash
pip install -r requirements.txt
```

2. Set up Google Cloud credentials:
```bash
# Option 1: Use gcloud CLI
gcloud auth application-default login

# Option 2: Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

## Configuration

Edit the `main()` function in [crawl_school_bildungberlin.py](crawl_school_bildungberlin.py) to configure:

```python
# Configuration
BASE_URL = "https://www.bildung.berlin.de/schulverzeichnis/SchulListe.aspx"
GCS_BUCKET_NAME = "your-bucket-name"  # Update this!
GCS_PROJECT_ID = None  # Optional: specify if needed
MAX_CONCURRENT = 3  # Adjust based on your needs (lower = more respectful to server)
LOCAL_BACKUP = True  # Set to False to disable local backups
```

## Usage

Run the crawler:
```bash
python crawl_school_bildungberlin.py
```

The script will:
1. Fetch the main school list page
2. Extract all school detail page URLs
3. Crawl each school's detail page with a progress bar
4. Save data both locally (in `crawled_schools/` directory) and to GCS
5. Generate a manifest file with crawl statistics

You'll see a progress bar showing:
- Number of schools crawled
- Percentage complete
- Estimated time remaining
- Schools per second

## Output Format

Each school is saved as a JSON file with the following structure:

```json
{
  "school_id": "12345",
  "school_name": "Example School",
  "url": "https://www.bildung.berlin.de/...",
  "title": "Page title",
  "description": "Meta description",
  "markdown": "Markdown-formatted content",
  "html": "Raw HTML content",
  "cleaned_text": "Cleaned text content",
  "tables": [
    [["Header1", "Header2"], ["Value1", "Value2"]]
  ],
  "links": {
    "internal": [],
    "external": []
  },
  "metadata": {
    "crawled_at": "2025-11-15T...",
    "success": true,
    "source": "bildung.berlin.de"
  }
}
```

## GCS Storage Structure

Files are stored in GCS with the following structure:
```
bildung_berlin/
├── schools/
│   ├── 12345_school_name_20251115.json
│   ├── 12346_another_school_20251115.json
│   └── ...
└── manifest.json
```

## Local Backup

If `LOCAL_BACKUP = True`, files are also saved to:
```
crawled_schools/
├── bildung_berlin_schools_12345_school_name_20251115.json
├── bildung_berlin_schools_12346_another_school_20251115.json
└── manifest.json
```

## Rate Limiting

The script includes:
- Semaphore-based concurrency control (default: 3 concurrent requests)
- 1-second delay between requests
- Configurable `MAX_CONCURRENT` parameter

Adjust these values based on your needs and to be respectful to the server.

## Error Handling

- Failed page loads are logged but don't stop the crawler
- GCS upload failures fall back to local storage
- Duplicate URLs are automatically skipped
- All errors are logged with detailed messages

## Logging

The script uses Python's logging module with INFO level by default. Logs include:
- Progress updates
- Successful crawls and uploads
- Errors and warnings
- Final statistics

## Use with RAG Systems

The crawled data is structured for easy use with RAG systems:
- Markdown format for clean text representation
- HTML for preserving structure
- Extracted tables for structured data
- Metadata for filtering and sorting
- Links for relationship mapping

## Troubleshooting

### GCS Authentication Issues
If you get authentication errors:
```bash
gcloud auth application-default login
```

### Rate Limiting / Blocking
If you get blocked or rate limited:
- Reduce `MAX_CONCURRENT` to 1 or 2
- Increase the delay in `crawl_and_store()` method
- Add random delays between requests

### Missing Data
If some schools are missing data:
- Check the logs for error messages
- Increase `page_timeout` in `CrawlerRunConfig`
- Adjust the `wait_for_selector` to match the actual page structure

## License

This script is for educational and research purposes. Please respect the website's terms of service and robots.txt when crawling.
