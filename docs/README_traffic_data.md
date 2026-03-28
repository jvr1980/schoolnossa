# Berlin Traffic Data Downloader (Berlin Zählt Mobilität)

A comprehensive downloader for Berlin's traffic counting data, including car and bicycle traffic statistics from the "Berlin Zählt Mobilität" open data initiative.

## Overview

This script downloads traffic counting data from [berlin-zaehlt.de](https://berlin-zaehlt.de/csv/) and stores it in Google Cloud Storage. The data includes:

### 1. **Telraam Data (Car & Vehicle Traffic)**
- **Period**: February 2021 - November 2025
- **Format**: Monthly CSV files (gzipped)
- **Content**: Hourly vehicle counts from Telraam sensors across Berlin
- **Categories**: Cars, larger vehicles, pedestrians, bicycles
- **Files**: ~58 monthly files

### 2. **Ecocounter Data (Bicycle & Pedestrian)**
- **Period**: March 2012 - December 2022
- **Format**: Monthly CSV files (gzipped)
- **Content**: Bicycle and pedestrian counts from permanent counting stations
- **Files**: ~131 monthly files

### 3. **Metadata Files**
- Geographic location data (GeoJSON format)
- Segment information for both sensor types
- README documentation

## Features

- **Bulk download**: Downloads all available traffic data files
- **Organized storage**: Separates car traffic, bicycle traffic, and metadata
- **Progress tracking**: Shows download progress with tqdm
- **Concurrent downloads**: Configurable parallel downloads
- **Google Cloud Storage integration**: Automatic upload to GCS
- **Local backup**: Optional local file storage
- **Comprehensive metadata**: Tracks all downloads in manifest file
- **Error handling**: Robust error handling and retry logic

## Prerequisites

1. **Python 3.8+**
2. **Google Cloud Project** with Cloud Storage enabled
3. **Service Account** credentials (same as other crawlers)
4. **Dependencies** installed

## Installation

Dependencies are in [requirements_crawler.txt](requirements_crawler.txt):

```bash
pip install -r requirements_crawler.txt
```

Additional dependency (already included):
- `beautifulsoup4` - For HTML parsing (included with `crawl4ai`)

## Configuration

Uses the same `.env` configuration as other crawlers:

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
python crawl_traffic_berlin_zaehlt.py
```

### What It Does

1. **Fetches** the directory listing from berlin-zaehlt.de
2. **Identifies** all traffic data files:
   - Telraam files (car traffic): `bzm_telraam_YYYY_MM.csv.gz`
   - Ecocounter files (bicycle): `bzm_ecocounter_YYYY_MM.csv.gz`
   - Metadata files: `*.geojson`
3. **Downloads** all files with progress tracking
4. **Saves locally** to organized folders (if LOCAL_BACKUP=True)
5. **Uploads** to GCS in structured format
6. **Creates manifest** with complete download inventory

### Configuration Options

Edit the `main()` function in [crawl_traffic_berlin_zaehlt.py](crawl_traffic_berlin_zaehlt.py):

```python
# Configuration
GCS_BUCKET_NAME = "schoolnossa-berlin"  # Your GCS bucket
GCS_PROJECT_ID = "schoolnossa"  # Your GCP project
LOCAL_BACKUP = True  # Save files locally as backup
MAX_CONCURRENT = 5  # Number of parallel downloads
```

## Data Structure

### Google Cloud Storage Organization

```
gs://schoolnossa-berlin/
└── traffic_data/
    ├── telraam/                                    # Car & vehicle traffic
    │   ├── bzm_telraam_2021_02.csv.gz
    │   ├── bzm_telraam_2021_03.csv.gz
    │   ├── ...
    │   └── bzm_telraam_2025_11.csv.gz             (~58 files)
    ├── ecocounter/                                 # Bicycle & pedestrian
    │   ├── bzm_ecocounter_2012_03.csv.gz
    │   ├── bzm_ecocounter_2012_04.csv.gz
    │   ├── ...
    │   └── bzm_ecocounter_2022_12.csv.gz          (~131 files)
    ├── metadata/
    │   ├── bzm_telraam_segments.geojson           # Telraam sensor locations
    │   ├── bzm_ecocounter_segments.geojson        # Ecocounter sensor locations
    │   └── READ_ME                                # Documentation
    └── manifest.json                               # Download inventory
```

### Local Backup Structure

```
traffic_data/
├── telraam/
│   └── bzm_telraam_*.csv.gz
├── ecocounter/
│   └── bzm_ecocounter_*.csv.gz
├── metadata/
│   ├── bzm_telraam_segments.geojson
│   └── bzm_ecocounter_segments.geojson
└── manifest.json
```

### Manifest Format

```json
{
  "download_date": "2025-01-15T14:30:00",
  "total_files": 192,
  "successful_downloads": 192,
  "gcs_bucket": "schoolnossa-berlin",
  "data_source": "https://berlin-zaehlt.de/csv/",
  "files_by_category": {
    "telraam_car_traffic": 58,
    "ecocounter_bicycle": 131,
    "metadata": 3
  },
  "files": [
    {
      "filename": "bzm_telraam_2025_11.csv.gz",
      "url": "https://berlin-zaehlt.de/csv/bzm_telraam_2025_11.csv.gz",
      "type": "telraam",
      "category": "car_traffic",
      "description": "Car and vehicle traffic counts (Telraam sensors)"
    }
  ]
}
```

## Data Details

### Telraam Data (Car Traffic)

**What it contains:**
- Hourly traffic counts
- Vehicle categories: cars, heavy vehicles, pedestrians, bicycles
- Data from citizen-installed Telraam sensors
- Growing coverage across Berlin neighborhoods

**File size growth:**
- Early 2021: ~2.6K (few sensors)
- Mid 2021: ~100K (network expansion)
- 2024-2025: 1.4M+ (extensive coverage)

**Use cases:**
- Traffic pattern analysis
- Urban mobility planning
- Environmental impact studies
- Congestion monitoring

### Ecocounter Data (Bicycle Traffic)

**What it contains:**
- Bicycle and pedestrian counts
- Data from permanent counting stations
- Long-term historical data (2012-2022)
- Key cycling routes across Berlin

**Coverage:**
- Major bike paths
- Bridge crossings
- Popular cycling corridors
- Multi-use paths

**Use cases:**
- Cycling infrastructure planning
- Seasonal pattern analysis
- Long-term trend identification
- Policy impact assessment

### Geographic Metadata (GeoJSON)

**Telraam segments** (903K):
- Sensor locations
- Street segment information
- Geographic coordinates
- Installation details

**Ecocounter segments** (38K):
- Counting station locations
- Route information
- Geographic data

## Data Format

### CSV Files (gzipped)

Each CSV file contains time-series data with columns typically including:
- Timestamp/Date
- Location/Segment ID
- Count values by category
- Additional metadata

**To decompress:**
```python
import gzip
import pandas as pd

with gzip.open('bzm_telraam_2025_11.csv.gz', 'rt') as f:
    df = pd.read_csv(f)
```

### GeoJSON Files

Standard GeoJSON format with:
- Geometry (points, lines)
- Properties (sensor info, names)
- Coordinate reference system

## Use Cases for RAG Systems

Perfect for building RAG systems to answer:
- "What are the busiest cycling routes in Berlin?"
- "How has car traffic changed in Kreuzberg since 2021?"
- "Which areas have the highest bicycle traffic?"
- "Show traffic patterns during rush hours"
- "Compare traffic before and after COVID-19"
- "Where should new bike lanes be installed?"

## Processing the Data

### Example: Load and Analyze

```python
import gzip
import pandas as pd
from pathlib import Path

# Load Telraam data
data_dir = Path('traffic_data/telraam')
all_data = []

for file in data_dir.glob('bzm_telraam_*.csv.gz'):
    with gzip.open(file, 'rt') as f:
        df = pd.read_csv(f)
        all_data.append(df)

# Combine all months
combined_df = pd.concat(all_data, ignore_index=True)

# Analyze
print(f"Total records: {len(combined_df)}")
print(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
```

### Example: Load GeoJSON

```python
import json
import geopandas as gpd

# Load sensor locations
with open('traffic_data/metadata/bzm_telraam_segments.geojson') as f:
    sensors = gpd.read_file(f)

print(f"Number of sensors: {len(sensors)}")
sensors.plot()  # Visualize sensor locations
```

## Monitoring and Logging

Example output:
```
2025-01-15 14:30:00 - INFO - Starting Berlin Traffic Data Downloader
2025-01-15 14:30:01 - INFO - Initialized GCS bucket: schoolnossa-berlin
2025-01-15 14:30:02 - INFO - Fetching file list from https://berlin-zaehlt.de/csv/
2025-01-15 14:30:03 - INFO - Found 192 files to download
2025-01-15 14:30:03 - INFO -   - Telraam (car traffic): 58 files
2025-01-15 14:30:03 - INFO -   - Ecocounter (bicycle): 131 files
2025-01-15 14:30:03 - INFO -   - Metadata files: 3 files
2025-01-15 14:30:04 - INFO - Starting download of 192 files...
Downloading traffic data: 100%|████████████| 192/192 [05:23<00:00, 1.68s/file]
2025-01-15 14:35:27 - INFO - Downloaded 192/192 files successfully
2025-01-15 14:35:28 - INFO - Manifest uploaded to GCS
2025-01-15 14:35:28 - INFO - Download completed!
```

## Performance

- **Total files**: ~192 files (as of Jan 2025)
- **Total size**: ~50-60 MB (compressed)
- **Download time**: 5-10 minutes (depends on network)
- **Concurrent downloads**: 5 by default (configurable)
- **Memory efficient**: Streams large files

## Scheduled Updates

The data is updated monthly with new traffic counts.

### Schedule with cron (monthly updates)

```bash
# Run on the 2nd of each month at 3am
0 3 2 * * cd /Users/josevonroth/Documents/schoolnossa && python crawl_traffic_berlin_zaehlt.py
```

## Error Handling

The script handles:
- **Network failures**: Logs and continues
- **Missing files**: Skips gracefully
- **GCS upload errors**: Falls back to local backup
- **Partial downloads**: Retries failed files

## Data Source Information

- **Source**: Berlin Zählt Mobilität
- **Website**: https://berlin-zaehlt.de/
- **Data Portal**: https://daten.berlin.de/datensaetze/berlin-zaehlt-mobilitaet
- **Update Frequency**: Monthly
- **Coverage Period**:
  - Telraam: 2021 - Present
  - Ecocounter: 2012 - 2022
- **License**: Open Data (check source for specific license)

## About the Sensors

### Telraam
- Citizen science project
- Low-cost traffic counting devices
- Installed in windows facing streets
- Counts cars, bikes, pedestrians, heavy vehicles
- Growing network across Berlin

### Ecocounter
- Professional permanent counting stations
- Installed on major cycling routes
- High accuracy bicycle counters
- Some stations display live counts

## Related Scripts

- [crawl_kita_navigator.py](crawl_kita_navigator.py) - Kita data crawler
- [crawl_crime_daten_berlin.py](crawl_crime_daten_berlin.py) - Crime statistics downloader
- [crawl_school_bildungberlin.py](crawl_school_bildungberlin.py) - School data crawler

## Troubleshooting

### Download Fails
1. Check network connectivity
2. Verify the source URL is accessible
3. Check available disk space
4. Review logs for specific errors

### Files Not Found
The directory listing may change. The script automatically adapts to available files.

### GCS Upload Issues
1. Verify credentials
2. Check bucket permissions
3. Ensure bucket exists
4. Review GCS quotas

## License

This is an educational/research tool. Please respect the data source's terms of service and licensing. The Berlin Open Data is generally freely available, but always verify current licensing terms.
