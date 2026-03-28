# School Data Crawler for RAG System

This script crawls school information from sekundarschulen-berlin.de using their sitemap and stores the content in Google Cloud Storage for use in a RAG (Retrieval-Augmented Generation) system.

## Features

- Fetches all URLs from the sitemap
- Filters for relevant school-related content
- Extracts structured data including:
  - Page title, description, and keywords
  - Full content in markdown and HTML formats
  - Internal links
  - Metadata (last modified, change frequency, etc.)
- Stores data in Google Cloud Storage as JSON files
- Concurrent crawling with rate limiting
- Comprehensive logging

## Prerequisites

1. Python 3.8 or higher
2. Google Cloud account with a storage bucket
3. Google Cloud credentials configured

## Installation

1. Install dependencies:
```bash
pip install -r requirements_crawler.txt
```

2. Set up Google Cloud credentials:
   - Create a service account in Google Cloud Console
   - Download the service account key JSON file
   - Set the environment variable:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
     ```

3. Create a Google Cloud Storage bucket:
```bash
gsutil mb gs://your-bucket-name
```

4. Configure the script:
   - Copy `.env.example` to `.env`
   - Update the values in `.env` or directly in the script

## Configuration

Edit the following variables in `crawl_schools_sekundarschule-berlin.py`:

```python
SITEMAP_URL = "https://www.sekundarschulen-berlin.de/sitemap.xml"
GCS_BUCKET_NAME = "your-bucket-name"  # Update this
GCS_PROJECT_ID = None  # Optional: specify if needed
MAX_CONCURRENT = 5  # Adjust based on your needs
```

## Usage

Run the crawler:

```bash
python crawl_schools_sekundarschule-berlin.py
```

The script will:
1. Fetch the sitemap
2. Filter for school-related URLs (excludes news, events, etc.)
3. Crawl each URL concurrently
4. Store the extracted data in GCS at `gs://your-bucket-name/schools/`
5. Create a manifest file at `gs://your-bucket-name/schools/manifest.json`

## Output Structure

Each crawled page is stored as a JSON file:

```
schools/
├── homepage/20251115_abc12345.json
├── spandau/20251115_def67890.json
├── albrecht-haushofer-schule/20251115_ghi11121.json
└── manifest.json
```

Each JSON file contains:
```json
{
  "url": "https://...",
  "title": "School Name",
  "description": "Description from meta tags",
  "keywords": "Keywords from meta tags",
  "markdown": "Full content in markdown format",
  "html": "Full HTML content",
  "links": ["internal", "links", "found"],
  "metadata": {
    "lastmod": "2024-11-15",
    "changefreq": "yearly",
    "priority": "0.5",
    "crawled_at": "2025-11-15T10:30:00",
    "success": true
  },
  "extracted_content": {
    "text": "Cleaned text content"
  }
}
```

## Filtering

The script excludes the following URL patterns:
- `/news/` - News articles
- `/tag-der-offenen-tuer/` - Open house events
- `/abitur/` - Exam data
- `/impressum`, `/datenschutz`, `/kontakt` - Legal/contact pages

To modify filtering, edit the `filter_school_urls()` method.

## Rate Limiting

The script uses a semaphore to limit concurrent requests. Adjust `MAX_CONCURRENT` based on:
- Your network capacity
- Server load tolerance
- GCS upload limits

Default is 5 concurrent requests.

## For RAG Integration

The stored JSON files are optimized for RAG systems:

1. **Markdown content**: Use the `markdown` field for clean, structured text
2. **Metadata**: Use `title`, `description`, and `keywords` for enhanced search
3. **Chunking**: The markdown content can be split into chunks for vector embeddings
4. **Provenance**: Each chunk can be linked back to the original URL

Example RAG pipeline:
```python
# 1. Download JSON files from GCS
# 2. Extract markdown content
# 3. Split into chunks (e.g., 512 tokens)
# 4. Generate embeddings
# 5. Store in vector database with metadata
```

## Troubleshooting

**Import errors for crawl4ai:**
```bash
pip install --upgrade crawl4ai
```

**GCS authentication errors:**
- Verify `GOOGLE_APPLICATION_CREDENTIALS` is set correctly
- Check that the service account has Storage Object Creator role

**Rate limiting errors:**
- Reduce `MAX_CONCURRENT` value
- Add delays in the `crawl_url` method

## License

This script is provided as-is for educational and research purposes.

## Notes

- The script respects the sitemap's changefreq and priority metadata
- Crawled content includes German language text
- Total URLs: ~400+ from the sitemap (after filtering ~200-300 school-related pages)
- Estimated runtime: 10-20 minutes depending on network and concurrency settings
