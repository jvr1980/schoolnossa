# Berlin Crime Data Downloader

A downloader script for Berlin crime statistics from the official open data portal (daten.berlin.de), automatically storing files in Google Cloud Storage.

## Overview

This script downloads crime statistics Excel files from the Berlin Crime Atlas (Kriminalitätsatlas Berlin) and stores them in Google Cloud Storage. The data includes:

- **Crime Statistics for Districts (2015-2024)**
  - Absolute case numbers and frequency rates
  - Covers Berlin's 12 districts and 138 district regions
  - 17 different crime categories
  - Time series data from 2015-2024

## Features

- **Automated download**: Downloads Excel files from official Berlin data portal
- **Progress tracking**: Shows download progress with tqdm progress bars
- **Google Cloud Storage integration**: Automatically uploads to GCS
- **Local backup**: Optionally saves files locally
- **Metadata preservation**: Saves metadata about each download
- **Error handling**: Robust error handling and logging
- **File sanitization**: Safely handles filenames with special characters

## Prerequisites

1. **Python 3.8+**
2. **Google Cloud Project** with Cloud Storage enabled
3. **Service Account** with Storage Admin permissions
4. **Dependencies** installed (see Installation)

## Installation

Dependencies are already listed in [requirements_crawler.txt](requirements_crawler.txt):

```bash
pip install -r requirements_crawler.txt
```

The script uses:
- `aiohttp` - Async HTTP client for downloads
- `google-cloud-storage` - GCS client
- `tqdm` - Progress bars
- `python-dotenv` - Environment variable management

## Configuration

The script uses the same credentials as the other crawlers. Make sure your `.env` file is set up:

```bash
# Google Cloud Storage Configuration
GCS_BUCKET_NAME=schoolnossa-berlin
GCS_PROJECT_ID=schoolnossa
GOOGLE_APPLICATION_CREDENTIALS=/path/to/schoolnossa-e1698305cfcb.json
```

## Usage

### Basic Usage

Run the downloader:

```bash
python crawl_crime_daten_berlin.py
```

### What It Does

1. **Initializes** Google Cloud Storage connection
2. **Downloads** the crime statistics Excel file with progress bar
3. **Saves locally** to `crime_data/` directory (if LOCAL_BACKUP=True)
4. **Uploads to GCS** at `gs://schoolnossa-berlin/crime_data/`
5. **Creates metadata** JSON file with download information
6. **Generates manifest** tracking all downloaded files

### Configuration Options

Edit the `main()` function in [crawl_crime_daten_berlin.py](crawl_crime_daten_berlin.py):

```python
# Configuration
GCS_BUCKET_NAME = "schoolnossa-berlin"  # Your GCS bucket
GCS_PROJECT_ID = "schoolnossa"  # Your GCP project
LOCAL_BACKUP = True  # Save files locally as backup
```

## Data Structure

### Storage Organization

Files are stored in Google Cloud Storage with this structure:

```
schoolnossa-berlin/
└── crime_data/
    ├── Crime_Statistics_Districts_2015-2024_20250115.xlsx
    ├── Crime_Statistics_Districts_2015-2024_20250115_metadata.json
    └── manifest.json
```

### Local Backup Structure

When `LOCAL_BACKUP = True`, files are saved to:

```
crime_data/
├── Crime_Statistics_Districts_2015-2024_20250115.xlsx
├── Crime_Statistics_Districts_2015-2024_20250115_metadata.json
└── manifest.json
```

### Metadata Format

Each downloaded file has an associated metadata JSON:

```json
{
  "original_url": "https://www.kriminalitaetsatlas.berlin.de/K-Atlas/bezirke/Fallzahlen&HZ 2015-2024.xlsx",
  "name": "Crime_Statistics_Districts_2015-2024",
  "description": "Absolute case numbers and frequency rates for Berlin districts (2015-2024)",
  "downloaded_at": "2025-01-15T10:30:00",
  "file_size": 2458624,
  "filename": "Crime_Statistics_Districts_2015-2024_20250115.xlsx"
}
```

### Manifest Format

The manifest tracks all downloads:

```json
{
  "download_date": "2025-01-15T10:30:00",
  "total_files": 1,
  "gcs_bucket": "schoolnossa-berlin",
  "files": [
    {
      "url": "https://www.kriminalitaetsatlas.berlin.de/K-Atlas/bezirke/Fallzahlen&HZ 2015-2024.xlsx",
      "name": "Crime_Statistics_Districts_2015-2024",
      "description": "Absolute case numbers and frequency rates for Berlin districts (2015-2024)"
    }
  ]
}
```

## Data Contents

### Crime Categories (17 types)

The Excel file includes statistics for various crime categories such as:
- Theft
- Violent crimes
- Property crimes
- Drug-related offenses
- And more...

### Geographic Coverage

- **12 Districts** (Bezirke): Main administrative divisions of Berlin
- **138 District Regions** (LOR): Smaller geographic units within districts

### Time Period

- **2015-2024**: 10 years of crime statistics
- Data includes both absolute numbers and rates per 100,000 residents

## Use Cases

### For RAG Systems

This data is perfect for building a RAG system that can answer questions like:
- "What are the crime trends in Mitte district?"
- "Which areas have the highest crime rates?"
- "How has crime changed over the past 10 years?"
- "Compare crime rates between different districts"

### For Analysis

The Excel files can be:
1. Loaded into pandas for analysis
2. Converted to other formats (CSV, Parquet)
3. Imported into databases
4. Visualized with tools like Tableau or PowerBI

## Adding More Data Sources

To download additional crime data files, edit the `crime_data_urls` list in the script:

```python
self.crime_data_urls = [
    {
        'url': 'https://example.com/data.xlsx',
        'name': 'Dataset_Name',
        'description': 'Description of the dataset'
    },
    # Add more URLs here
]
```

## Monitoring and Logging

The script provides detailed logging:
- `INFO`: Progress updates, successful operations
- `WARNING`: GCS initialization issues, non-critical errors
- `ERROR`: Download failures, upload errors

Example output:
```
2025-01-15 10:30:00 - INFO - Starting Berlin Crime Data Downloader
2025-01-15 10:30:01 - INFO - Initialized GCS bucket: schoolnossa-berlin
2025-01-15 10:30:02 - INFO - Processing: Crime_Statistics_Districts_2015-2024
2025-01-15 10:30:03 - INFO - Downloading: https://www.kriminalitaetsatlas.berlin.de/...
Downloading Crime_Statistics_Districts_2015-2024_20250115.xlsx: 100%|████████| 2.35M/2.35M [00:05<00:00, 450kB/s]
2025-01-15 10:30:08 - INFO - Successfully downloaded Crime_Statistics_Districts_2015-2024_20250115.xlsx (2458624 bytes)
2025-01-15 10:30:09 - INFO - Saved local backup: crime_data/Crime_Statistics_Districts_2015-2024_20250115.xlsx
2025-01-15 10:30:10 - INFO - Uploaded to GCS: crime_data/Crime_Statistics_Districts_2015-2024_20250115.xlsx
2025-01-15 10:30:11 - INFO - Download completed!
```

## Error Handling

The script handles various error scenarios:
- **Network failures**: Logs error and continues
- **GCS upload failures**: Falls back to local backup
- **Invalid URLs**: Logs error and skips
- **File write errors**: Logs error and continues with GCS upload

## Performance

- **Download speed**: Limited by network bandwidth and server speed
- **Progress tracking**: Real-time progress bars for large files
- **Async operations**: Efficient async HTTP client
- **Memory efficient**: Streams large files in chunks

## Scheduled Updates

To automatically download updated crime data on a schedule:

### Using cron (Linux/Mac)

```bash
# Add to crontab (monthly on the 1st at 2am)
0 2 1 * * cd /Users/josevonroth/Documents/schoolnossa && python crawl_crime_daten_berlin.py
```

### Using Cloud Scheduler (GCP)

Set up a Cloud Function or Cloud Run job triggered by Cloud Scheduler to run the script periodically.

## Troubleshooting

### Download Fails

1. Check if the URL is still valid
2. Test the URL in a browser
3. Check network connectivity
4. Review logs for specific error messages

### GCS Upload Fails

1. Verify credentials are set correctly
2. Check bucket permissions
3. Ensure bucket exists
4. Review GCS quotas

### File Too Large

The script handles large files by:
- Streaming downloads in chunks
- Using async I/O for efficiency
- Providing progress feedback

## Related Scripts

- [crawl_kita_navigator.py](crawl_kita_navigator.py) - Kita data crawler
- [crawl_school_bildungberlin.py](crawl_school_bildungberlin.py) - School data crawler

## Data Source

- **Source**: Berlin Open Data Portal (daten.berlin.de)
- **Dataset**: Kriminalitätsatlas Berlin
- **URL**: https://daten.berlin.de/datensaetze/kriminalitatsatlas-berlin
- **License**: Check the data portal for licensing information
- **Updates**: Data is updated periodically by Berlin authorities

## License

This is an educational/research tool. Please respect the data source's terms of service and licensing. Always use data responsibly and ethically.
