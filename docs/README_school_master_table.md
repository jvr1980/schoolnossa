# School Master Table Builder Scripts

This directory contains scripts to build a comprehensive master table of schools in Berlin using multiple data sources.

## Scripts Overview

### 1. `school_master_table_apify_maps.py`
Uses the Apify Google Maps Scraper actor to collect school data.

**Features:**
- Scrapes schools using Apify's `compass/crawler-google-places` actor
- Processes all Berlin postcodes from GCS
- Searches for 5 school categories
- Stores results in GCS and locally
- Includes comprehensive error handling and retry logic

**School Categories:**
- Kitas (Kindergartens)
- Grundschulen (Primary Schools)
- Gymnasien (Grammar Schools)
- Integrierte Sekundarschulen (Integrated Secondary Schools)
- Privatschulen (Private Schools)

**Cost:** Apify offers a free tier with limited usage. Paid plans start at $49/month.

### 2. `school_master_table_gcp_places.py`
Uses Google Places API directly to collect school data.

**Features:**
- Direct integration with Google Places API
- Text search for schools by postcode and type
- Optional detailed place information fetching
- API usage tracking and cost estimation
- Multiple query variations per school type

**API Pricing:**
- Text Search: $32 per 1,000 requests
- Place Details: $17 per 1,000 requests
- Estimated cost for full Berlin scrape: ~$50-150 (depending on results)

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements_crawler.txt
pip install requests
```

### 2. Configure API Keys

Copy `.env.example` to `.env` and add your API keys:

```bash
cp .env.example .env
```

Edit `.env` and add:

#### For Apify Script:
```bash
APIFY_API_TOKEN=your_apify_token_here
```
Get your token from: https://console.apify.com/account/integrations

#### For Google Places Script:
```bash
GOOGLE_PLACES_API_KEY=your_google_api_key_here
```
Get your key from: https://console.cloud.google.com/apis/credentials

### 3. Ensure GCS Access

Make sure you have:
- Google Cloud SDK installed and authenticated
- Access to the `schoolnossa-berlin` bucket
- The postcode data uploaded (see `geo_data_collection_berlin.py`)

## Usage

### Run Apify Scraper

```bash
python3 school_master_table_apify_maps.py
```

**Output:**
- Local: `school_data_apify/`
- GCS: `gs://schoolnossa-berlin/school_data_apify/`

**Structure:**
```
school_data_apify/
├── kitas/
│   ├── 10115/
│   │   └── kitas_10115_20250117_120000.json
│   └── 10117/
├── grundschulen/
├── gymnasien/
├── sekundarschulen/
├── private_schools/
└── summary_20250117_120000.json
```

### Run Google Places Scraper

```bash
python3 school_master_table_gcp_places.py
```

**Output:**
- Local: `school_data_gcp_places/`
- GCS: `gs://schoolnossa-berlin/school_data_gcp_places/`

**Optional Configuration:**
```bash
# Disable detailed place fetching to save API calls
FETCH_PLACE_DETAILS=false python3 school_master_table_gcp_places.py

# Limit results per search
MAX_RESULTS_PER_SEARCH=50 python3 school_master_table_gcp_places.py
```

## Data Structure

### Apify Results Format
Each result includes:
- Basic place information from Google Maps
- Address, location coordinates
- Rating, reviews count
- Opening hours
- Contact information
- Metadata (postcode, school type, scrape timestamp)

### Google Places Results Format
Each result includes:
- Place ID, name, formatted address
- Geometry (lat/lng)
- Types, rating, user ratings total
- Opening hours (if available)
- Phone number, website, Google Maps URL
- Reviews (if details fetched)
- Photos references
- Metadata (postcode, school type, search query, timestamp)

## Processing Pipeline

1. **Load Postcodes:** Reads Berlin postcodes from GCS (`geo_data/germany/berlin/postcodes/plz_csv.csv`)

2. **Iterate:** For each postcode and school type combination:
   - Apify: Creates actor run with search configuration
   - Google Places: Performs text search with multiple query variations

3. **Collect Results:**
   - Apify: Waits for actor completion and fetches dataset
   - Google Places: Paginates through all results

4. **Enrich:** Optionally fetches detailed place information

5. **Store:** Saves to local filesystem and uploads to GCS

6. **Summarize:** Creates summary report with statistics

## Rate Limiting & Best Practices

### Apify
- Actor runs can take 30 seconds to several minutes
- Built-in rate limiting by Apify platform
- Scripts includes 2-second delays between searches

### Google Places API
- 1-second delay between different searches
- 0.1-second delay between detail requests
- 2-second delay before fetching next page
- Consider setting `FETCH_PLACE_DETAILS=false` to reduce API calls by ~70%

## Monitoring Progress

Both scripts provide detailed logging:

```bash
# Watch progress in real-time
python3 school_master_table_apify_maps.py 2>&1 | tee scrape.log

# Check summary after completion
cat school_data_apify/summary_*.json | jq .
```

## Comparison: Apify vs Google Places API

| Feature | Apify | Google Places API |
|---------|-------|-------------------|
| **Setup Complexity** | Medium (actor configuration) | Low (direct API) |
| **Cost** | ~$49/month + usage | Pay per request (~$50-150 total) |
| **Data Quality** | High (fresh Google Maps data) | High (direct from Google) |
| **Rate Limits** | Platform managed | Self-managed |
| **Maintenance** | Actor updates handled by Compass | Direct control |
| **Best For** | Recurring scrapes, bulk data | One-time scrapes, custom queries |

## Troubleshooting

### "APIFY_API_TOKEN not set"
- Ensure `.env` file exists and contains your token
- Run `source .env` before running script

### "GCS bucket not initialized"
- Check Google Cloud authentication: `gcloud auth list`
- Verify bucket access: `gsutil ls gs://schoolnossa-berlin/`

### "No postcodes found"
- First run `geo_data_collection_berlin.py` to download postcode data
- Verify data exists: `gsutil cat gs://schoolnossa-berlin/geo_data/germany/berlin/postcodes/plz_csv.csv`

### High API costs
- Set `FETCH_PLACE_DETAILS=false` to reduce Google Places API calls
- Reduce `MAX_RESULTS_PER_SEARCH` to limit results per query
- Test with a single postcode first

## Next Steps

After collecting the data, you can:

1. **Deduplicate:** Remove duplicate schools across postcodes
2. **Geocode:** Enrich with precise coordinates
3. **Merge:** Combine Apify and Google Places results
4. **Validate:** Check data quality and completeness
5. **Build Master Table:** Create unified school database

See `education_location_master_table_builder.py` for the next stage of processing.
