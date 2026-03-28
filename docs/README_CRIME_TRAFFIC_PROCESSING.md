# Crime and Traffic Data Processing for Vector Database

This document explains how crime statistics and traffic monitoring data are processed for integration into the ChromaDB vector database, with focus on geographic matching to schools and kitas.

## Overview

The processor extracts **recent values only** (2024 with year-over-year comparison to 2023) and calculates **percentage changes** to provide context about safety trends and traffic patterns at a geographic level that matches schools.

## Crime Data Processing

### Source Data
- **File**: Excel workbook with crime statistics by district and sub-district (Bezirksregion)
- **Coverage**: 2015-2024, organized by year in separate sheets
- **Geographic Level**: LOR (Lebensweltlich orientierte Räume) - Bezirksregion codes
- **Total regions**: 169 Berlin geographic areas

### What Gets Extracted

For each geographic region (Bezirksregion), we extract:

#### 1. 2024 Absolute Values
- Total Crimes
- Robbery (Raub)
- Street Robbery (Straßenraub)
- Bodily Injury Total (Körperverletzungen)
- Serious Bodily Injury (Gefährliche und schwere Körperverletzung)
- Theft Total (Diebstahl)
- Burglary (Wohnraumeinbruch)
- Bicycle Theft (Fahrraddiebstahl)
- Neighborhood Crimes (Kieztaten)

#### 2. Year-over-Year Changes (2024 vs 2023)
For each metric above, we calculate:
```
Percentage Change = ((Value_2024 - Value_2023) / Value_2023) × 100
```

#### 3. Safety Trend Assessment
Automated interpretation based on total crime change:
- **Significant increase**: >10% increase → "Crime increased significantly"
- **Significant decrease**: >10% decrease → "Crime decreased significantly, indicating improved safety"
- **Stable**: -10% to +10% → "Crime rates remained relatively stable"

### Example Output Document

```json
{
  "id": "crime_010000",
  "type": "crime_statistics",
  "name": "Crime Statistics - Mitte",
  "content": "Crime Statistics for Mitte\nLOR Code: 010000\n\n2024 Crime Data (with year-over-year change from 2023):\n\n- Total Crimes: 84,145 (0.6% increase from 2023)\n- Robbery: 927 (4.9% increase from 2023)\n- Street Robbery: 546 (4.8% increase from 2023)\n- Bodily Injury: 8,885 (9.1% increase from 2023)\n- Serious Bodily Injury: 2,646 (14.9% increase from 2023)\n- Theft Total: 39,391 (3.5% increase from 2023)\n- Burglary: 780 (3.6% decrease from 2023)\n- Bicycle Theft: 4,372 (1.6% decrease from 2023)\n- Neighborhood Crimes: 20,743 (8.0% increase from 2023)\n\nSafety Trend: Crime rates remained relatively stable in Mitte.",
  "metadata": {
    "lor_code": "010000",
    "region_name": "Mitte",
    "district": "Mitte",
    "year": 2024,
    "total_crimes_2024": 84145,
    "total_crimes_change_pct": 0.6,
    "source": "kriminalitaetsatlas.berlin.de",
    "data_type": "safety_crime_statistics",
    "geo_level": "bezirksregion"
  },
  "crime_metrics_2024": {
    "Total Crimes": 84145,
    "Robbery": 927,
    "Street Robbery": 546,
    "Bodily Injury": 8885,
    "Serious Bodily Injury": 2646,
    "Theft Total": 39391,
    "Burglary": 780,
    "Bicycle Theft": 4372,
    "Neighborhood Crimes": 20743
  },
  "yoy_changes": {
    "Total Crimes": 0.6,
    "Robbery": 4.9,
    "Street Robbery": 4.8,
    "Bodily Injury": 9.1,
    "Serious Bodily Injury": 14.9,
    "Theft Total": 3.5,
    "Burglary": -3.6,
    "Bicycle Theft": -1.6,
    "Neighborhood Crimes": 8.0
  }
}
```

### Geographic Matching to Schools

**LOR Bezirksregion codes** (e.g., `010000`, `011001`) provide the geographic granularity:
- Each school and kita can be matched to a LOR code based on their address
- This enables queries like: "Find schools in areas with decreasing crime rates"
- District mapping is also included (Mitte, Pankow, etc.) for broader geographic queries

## Traffic Data Processing

### Source Data
- **Type**: CSV.GZ files with time-series traffic counts
- **Coverage**: Monthly data from 2012-2022
- **Metadata**: GeoJSON files with counter locations
- **Counter Types**:
  - **Ecocounter**: Bicycle and pedestrian traffic (20 stations)
  - **Telraam**: Vehicle traffic (266 stations)

### What Gets Extracted

Instead of processing massive time-series data, we extract **monitoring station metadata**:

#### Bicycle Traffic Counters (Ecocounter)
- **Count**: 20 permanent stations
- **Data**: Location name, description, coordinates
- **Purpose**: Identify areas with bicycle traffic monitoring

Example locations:
- Karl-Marx-Allee
- Oberbaumbrücke
- Maybachufer

#### Vehicle Traffic Counters (Telraam)
- **Count**: 266 monitoring locations
- **Data**: Street name, coordinates
- **Purpose**: Identify areas with vehicle traffic monitoring

### Why Not Aggregate Counts?

The traffic data is **time-series** (hourly/daily counts over years), which is:
1. **Too granular** for school matching (exact timestamps don't correlate to school quality)
2. **Better suited** for a time-series database, not a vector database
3. **Location-based** monitoring is more useful: "Is there a traffic counter near this school?"

### Example Traffic Document

```json
{
  "id": "bicycle_traffic_300021646",
  "type": "bicycle_traffic",
  "name": "Bicycle Counter - Karl-Marx-Allee",
  "content": "Bicycle Traffic Counter: Karl-Marx-Allee\nLocation: Karl-Marx-Allee\nCoordinates: 52.52173, 13.41776\nSegment ID: 300021646\n\nThis is a permanent bicycle traffic counting station monitoring cyclist activity.\nStation Name: Karl-Marx-Allee\nLocated at: Karl-Marx-Allee",
  "metadata": {
    "segment_id": "300021646",
    "location_name": "Karl-Marx-Allee",
    "description": "Karl-Marx-Allee",
    "lat": 52.52173,
    "lon": 13.41776,
    "source": "berlin-zaehlt.de",
    "data_type": "traffic_bicycle_monitoring",
    "counter_type": "ecocounter"
  }
}
```

### Geographic Matching to Schools

Traffic counters can be matched to schools via:
- **Proximity search**: Find counters within X meters of a school (using lat/lon)
- **Street name**: Match if school is on the same street as a counter
- **Use case**: "Find schools near high bicycle traffic areas"

## Output Structure

```
vector_db_input/
└── safety_traffic/
    ├── crime_statistics_vector_input.jsonl       # 169 crime documents
    ├── traffic_monitoring_vector_input.jsonl     # 286 traffic documents
    └── manifest.json                              # Processing metadata
```

### GCS Output
```
gs://schoolnossa-berlin/vector_database_input/
└── safety_traffic/
    ├── crime_statistics_vector_input.jsonl
    ├── traffic_monitoring_vector_input.jsonl
    └── manifest.json
```

## Running the Processor

### Prerequisites
```bash
# Install pandas and openpyxl for Excel processing
venv/bin/pip install pandas openpyxl

# Ensure GCS credentials are set
export GOOGLE_APPLICATION_CREDENTIALS="path/to/gcs-credentials.json"
```

### Execute
```bash
venv/bin/python process_crime_traffic_for_vector_db.py
```

### Configuration
Edit the `main()` function:
```python
GCS_BUCKET_NAME = "schoolnossa-berlin"
GCS_PROJECT_ID = "schoolnossa"
LOCAL_OUTPUT = True  # Save local copy
```

## Processing Summary

### Crime Statistics
- ✅ **169 geographic regions** (Bezirksregion level)
- ✅ **2024 values** with **year-over-year % changes** from 2023
- ✅ **9 key crime metrics** per region
- ✅ **Automated trend analysis** (increasing/decreasing/stable)
- ✅ **Geographic matching** via LOR codes (same as schools)

### Traffic Monitoring
- ✅ **286 monitoring stations** (20 bicycle, 266 vehicle)
- ✅ **Location metadata** (lat/lon, street names)
- ✅ **Geographic matching** via proximity/street name
- ❌ **No aggregated counts** (time-series data not included)

## Matching to Schools/Kitas

### Crime Data Matching
Schools and kitas can be matched to crime statistics by:

1. **Extracting LOR code** from school/kita address
2. **Looking up** corresponding crime document by LOR code
3. **Joining data** for queries like:
   - "Schools in areas with >10% crime decrease"
   - "Kitas in low-crime neighborhoods (bottom 25th percentile)"
   - "Schools where serious bodily injury is increasing"

### Traffic Data Matching
Schools and kitas can be matched to traffic counters by:

1. **Distance calculation** from school lat/lon to counter lat/lon
2. **Street name matching** if school is on same street
3. **Use cases**:
   - "Schools near bicycle traffic monitoring (bike-friendly areas)"
   - "Kitas on streets with vehicle traffic counters (busy roads)"

## ChromaDB Integration Example

```python
import chromadb
import json

# Initialize ChromaDB
client = chromadb.Client()
collection = client.create_collection("berlin_safety_traffic")

# Load crime statistics
with open("vector_db_input/safety_traffic/crime_statistics_vector_input.jsonl") as f:
    for line in f:
        doc = json.loads(line)
        collection.add(
            documents=[doc["content"]],
            metadatas=[doc["metadata"]],
            ids=[doc["id"]]
        )

# Load traffic monitoring
with open("vector_db_input/safety_traffic/traffic_monitoring_vector_input.jsonl") as f:
    for line in f:
        doc = json.loads(line)
        collection.add(
            documents=[doc["content"]],
            metadatas=[doc["metadata"]],
            ids=[doc["id"]]
        )

# Query example
results = collection.query(
    query_texts=["areas with decreasing crime rates"],
    n_results=10,
    where={"data_type": "safety_crime_statistics"}
)
```

## Example Queries Enabled

### Crime-based Queries
1. "Find schools in neighborhoods where crime decreased by more than 10%"
2. "Show kitas in areas with low burglary rates"
3. "Which districts had the biggest increase in serious bodily injury?"
4. "Schools in the safest Bezirksregionen (bottom 10% total crime)"

### Traffic-based Queries
1. "Schools near bicycle traffic monitoring stations"
2. "Kitas on streets with vehicle traffic counters" (potentially busy roads)
3. "Find schools within 500m of Karl-Marx-Allee counter"

### Combined Queries
1. "Safe neighborhoods with good bicycle infrastructure"
   - Low crime rates + bicycle counter nearby
2. "Family-friendly areas"
   - Decreasing crime + kitas nearby + not on busy traffic streets

## Key Benefits

### For Crime Data
✅ **Recent values only** (2024) - no historical bloat
✅ **Year-over-year % changes** - clear trend direction
✅ **Geographic granularity** - Bezirksregion level matches schools
✅ **Structured metadata** - Easy filtering and querying
✅ **Trend interpretation** - Automated safety assessment

### For Traffic Data
✅ **Lightweight** - Station locations, not massive time-series
✅ **Geographic context** - Proximity to schools/kitas
✅ **Practical queries** - "Is there a bike counter nearby?"
✅ **Avoids complexity** - No time-series aggregation needed

## Manifest File

The manifest provides processing metadata:

```json
{
  "processing_date": "2025-11-16T20:38:22.551",
  "run_timestamp": "20251116_203817",
  "total_documents": 455,
  "documents_by_type": {
    "crime_statistics": 169,
    "traffic_monitoring": 286
  },
  "crime_data": {
    "years": "2024 vs 2023",
    "metrics": "Total crimes, robbery, theft, burglary, bodily injury, etc.",
    "geographic_level": "LOR Bezirksregion (matches schools)",
    "includes_yoy_changes": true
  },
  "traffic_data": {
    "types": "Bicycle counters (ecocounter) and vehicle counters (telraam)",
    "geographic_level": "Point locations (lat/lon)",
    "note": "Monitoring station locations, not aggregated counts"
  }
}
```

## Future Enhancements

1. **LOR Code Extraction**: Auto-extract LOR codes from school/kita addresses
2. **Proximity Calculations**: Pre-calculate distances from schools to traffic counters
3. **Time-series Aggregation**: If needed, aggregate recent traffic counts (e.g., 2024 average)
4. **Additional Metrics**: Add crime rate per capita (requires population data)
5. **Trend Scoring**: Create composite safety scores for neighborhoods

## Related Files

- **Processor**: `process_crime_traffic_for_vector_db.py`
- **Crime downloader**: `crawl_crime_daten_berlin.py`
- **Traffic downloader**: `crawl_traffic_berlin_zaehlt.py`
- **Main vector DB pipeline**: `prepare_vector_db_input.py`
