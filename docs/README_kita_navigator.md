# Kita Navigator Berlin Crawler

A comprehensive web crawler for extracting kindergarten (Kita) information from the Kita-Navigator Berlin website using Crawl4AI and storing the data in Google Cloud Storage.

## Features

- **Automated pagination**: Automatically crawls through all pages of search results
- **Dual data extraction**: Extracts both list-level data and detailed individual Kita pages
- **Google Cloud Storage integration**: Automatically uploads crawled data to GCS
- **Local backup**: Optionally saves all data locally as backup
- **Structured data extraction**: Extracts contact information, addresses, opening hours, and more
- **Rate limiting**: Respects server resources with configurable concurrent request limits
- **Comprehensive logging**: Detailed logging for monitoring and debugging
- **Error handling**: Robust error handling and retry logic

## Prerequisites

1. **Python 3.8+**
2. **Google Cloud Project** with Cloud Storage enabled
3. **Service Account** with Storage Admin permissions (or appropriate read/write permissions)

## Installation

1. Install dependencies:
```bash
pip install -r requirements_crawler.txt
```

2. Set up Google Cloud authentication:

   **Option A: Service Account Key File**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

   **Option B: gcloud CLI**
   ```bash
   gcloud auth application-default login
   ```

3. Create a `.env` file (copy from `.env.example`):
```bash
cp .env.example .env
```

4. Edit `.env` and configure your settings:
```bash
GCS_BUCKET_NAME=your-kita-bucket-name
GCS_PROJECT_ID=your-project-id
```

## Usage

### Basic Usage

Run the crawler with default settings:

```bash
python crawl_kita_navigator.py
```

### Configuration

Edit the configuration variables in the `main()` function of [crawl_kita_navigator.py](crawl_kita_navigator.py):

```python
# Configuration
BASE_URL = "https://kita-navigator.berlin.de/einrichtungen?input=&betb=10-2025&einfacheSuche=true&entfernung=50&seite=1&index=0"
GCS_BUCKET_NAME = "your-kita-bucket-name"  # Your GCS bucket name
GCS_PROJECT_ID = None  # Optional: specify if needed
MAX_CONCURRENT = 3  # Number of concurrent requests
LOCAL_BACKUP = True  # Save files locally as backup
MAX_PAGES = None  # Limit pages (None = all pages, or set a number)
```

### Advanced Configuration

**Limit number of pages:**
```python
MAX_PAGES = 10  # Only crawl first 10 pages
```

**Increase/decrease crawling speed:**
```python
MAX_CONCURRENT = 5  # More concurrent requests (be careful!)
```

**Disable local backup:**
```python
LOCAL_BACKUP = False
```

## Data Structure

### Storage Organization

Data is stored in Google Cloud Storage with the following structure:

```
your-bucket/
├── kita_navigator/
│   ├── pages/
│   │   ├── page_1_20250115.json
│   │   ├── page_2_20250115.json
│   │   └── ...
│   ├── kitas/
│   │   ├── 12345_Kita_Sonnenschein_20250115.json
│   │   ├── 12346_Kita_Regenbogen_20250115.json
│   │   └── ...
│   └── manifest.json
```

### Data Formats

**Page Data** (`pages/page_N_YYYYMMDD.json`):
```json
{
  "page_number": 1,
  "kitas": [
    {
      "name": "Kita Sonnenschein",
      "address": "Musterstraße 123, 10115 Berlin",
      "kita_id": "12345",
      "detail_url": "https://...",
      "full_text": "..."
    }
  ],
  "kita_urls": [...],
  "crawled_at": "2025-01-15T10:30:00"
}
```

**Kita Detail Data** (`kitas/ID_Name_YYYYMMDD.json`):
```json
{
  "kita_id": "12345",
  "kita_name": "Kita Sonnenschein",
  "url": "https://...",
  "title": "...",
  "description": "...",
  "address": "Musterstraße 123, 10115 Berlin",
  "contact": {
    "phone": "030 123456",
    "email": "info@kita-sonnenschein.de"
  },
  "opening_hours": "Mo-Fr 7:00-17:00",
  "markdown": "...",
  "html": "...",
  "tables": [...],
  "metadata": {
    "crawled_at": "2025-01-15T10:30:00",
    "success": true,
    "source": "kita-navigator.berlin.de"
  }
}
```

**Manifest** (`manifest.json`):
```json
{
  "crawl_date": "2025-01-15T10:30:00",
  "total_kitas": 1500,
  "total_pages": 75,
  "successfully_crawled_details": 1450,
  "base_url": "https://...",
  "statistics": {
    "pages_processed": 75,
    "kitas_found": 1500,
    "detail_pages_crawled": 1450
  }
}
```

## Local Backup

When `LOCAL_BACKUP = True`, all data is also saved locally in the `crawled_kitas/` directory with the same structure as GCS.

## Building a RAG System

The crawled data is optimized for building a Retrieval-Augmented Generation (RAG) system:

1. **Structured metadata**: Each Kita has structured fields (name, address, contact, etc.)
2. **Rich text content**: Markdown and cleaned HTML for semantic search
3. **Tables extracted**: Structured table data for specific information retrieval
4. **Comprehensive manifest**: Easy to track and manage all crawled data

### Recommended RAG Pipeline

1. **Load data** from GCS or local backup
2. **Chunk the markdown content** for embedding
3. **Create embeddings** using OpenAI, Cohere, or open-source models
4. **Store in vector database** (Pinecone, Weaviate, Chroma, etc.)
5. **Add metadata filters** for structured search (location, opening hours, etc.)
6. **Build semantic search** over the embedded content

### Example RAG Use Cases

- "Find kindergartens in Mitte district with extended hours"
- "Which Kitas have bilingual programs?"
- "Show me Kitas near Alexanderplatz that accept children under 2"
- "What are the opening hours of Kita Sonnenschein?"

## Monitoring and Logging

The crawler provides detailed logging:

- `INFO`: Progress updates, successful operations
- `WARNING`: GCS initialization issues, non-critical errors
- `ERROR`: Failed crawls, upload errors
- `DEBUG`: Detailed extraction information

## Error Handling

The crawler handles various error scenarios:

- **Network failures**: Retries and continues with next items
- **GCS upload failures**: Falls back to local backup
- **Pagination detection failures**: Stops gracefully
- **HTML parsing errors**: Logs and continues

## Performance Considerations

- **Respectful crawling**: Built-in delays between requests
- **Rate limiting**: Configurable concurrent request limits
- **Resource efficient**: Streams data to storage, doesn't load everything in memory
- **Incremental backups**: Each item saved immediately after crawling

## Troubleshooting

### GCS Upload Fails

1. Check your credentials:
   ```bash
   gcloud auth application-default print-access-token
   ```

2. Verify bucket permissions:
   ```bash
   gsutil ls gs://your-bucket-name
   ```

3. Check the logs for specific error messages

### No Data Extracted

1. The website structure may have changed
2. Check the HTML selectors in the extraction methods
3. Use `verbose=True` in AsyncWebCrawler to see detailed logs
4. Inspect the saved HTML in local backup to understand the page structure

### Slow Crawling

1. Increase `MAX_CONCURRENT` (carefully!)
2. Reduce `js_code` wait times if content loads faster
3. Use `MAX_PAGES` to test with fewer pages first

## Future Enhancements

- [ ] Add support for different search filters
- [ ] Implement incremental crawling (only new/updated Kitas)
- [ ] Add data validation and quality checks
- [ ] Create embeddings directly during crawl
- [ ] Add support for other city Kita navigators
- [ ] Implement change detection and notifications

## License

This is an educational/research tool. Please respect the website's terms of service and robots.txt. Always crawl responsibly and ethically.

## Related Scripts

- [crawl_school_bildungberlin.py](crawl_school_bildungberlin.py) - Berlin school directory crawler
- [crawl_schools_sekundarschule-berlin.py](crawl_schools_sekundarschule-berlin.py) - Secondary school crawler

## Support

For issues or questions:
1. Check the logs for error messages
2. Review the website's current structure
3. Verify GCS configuration and permissions
4. Check network connectivity and firewall settings
