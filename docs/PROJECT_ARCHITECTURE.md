# SchoolNossa - German School Data Platform

## Project Vision

SchoolNossa is a comprehensive data platform that aggregates, enriches, and standardizes information about secondary schools across German cities. The platform enables parents, educators, and researchers to compare schools using consistent metrics including academic performance, demographics, accessibility, and neighborhood context.

---

## Table of Contents

1. [Project Goals](#1-project-goals)
2. [Architecture Overview](#2-architecture-overview)
3. [Data Pipeline](#3-data-pipeline)
4. [City-Specific vs Shared Components](#4-city-specific-vs-shared-components)
5. [Supported Cities](#5-supported-cities)
6. [Data Schema](#6-data-schema)
7. [Directory Structure](#7-directory-structure)
8. [Script Reference](#8-script-reference)
9. [Adding a New City](#9-adding-a-new-city)
10. [Technology Stack](#10-technology-stack)

---

## 1. Project Goals

### Primary Objectives

1. **Comprehensive School Database**: Create a unified database of secondary schools (Gymnasium, Integrierte Sekundarschule/Stadtteilschule, Gemeinschaftsschule) for major German cities.

2. **Standardized Metrics**: Normalize data across cities to enable meaningful comparisons:
   - Academic performance (Abitur averages, MSA results)
   - Demographics (student/teacher counts, migration background)
   - Accessibility (transit, traffic, neighborhood)
   - Programs (languages, special focuses)

3. **AI-Enhanced Descriptions**: Generate rich, searchable school descriptions using LLM technology.

4. **Semantic Search**: Enable similarity-based school discovery using vector embeddings.

### Target Users

- Parents choosing schools for their children
- Educators researching best practices
- Researchers studying educational outcomes
- Policy makers analyzing educational equity

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SchoolNossa Architecture                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐          │
│  │  City-Specific   │  │  City-Specific   │  │  City-Specific   │   ...    │
│  │  Data Sources    │  │  Data Sources    │  │  Data Sources    │          │
│  │     (Berlin)     │  │    (Hamburg)     │  │    (Munich)      │          │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘          │
│           │                     │                     │                     │
│           ▼                     ▼                     ▼                     │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    CITY-SPECIFIC SCRAPERS                            │  │
│  │  • School portals (bildung.berlin.de, schulinfosystem.hamburg.de)   │  │
│  │  • Crime statistics (city-specific police data)                      │  │
│  │  • Traffic sensors (city-specific sensor networks)                   │  │
│  │  • Transit data (BVG, HVV, MVV)                                     │  │
│  └────────────────────────────────┬─────────────────────────────────────┘  │
│                                   │                                         │
│                                   ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    STANDARDIZED INTERMEDIATE FORMAT                  │  │
│  │  • Unified schema for all cities                                     │  │
│  │  • Common field names and data types                                │  │
│  │  • Normalized coordinates (WGS84)                                   │  │
│  └────────────────────────────────┬─────────────────────────────────────┘  │
│                                   │                                         │
│                                   ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                       SHARED ENRICHMENT LAYER                        │  │
│  │  • Google Places API (POIs, parks, amenities)                       │  │
│  │  • LLM descriptions (OpenAI, Gemini, Perplexity)                    │  │
│  │  • Vector embeddings (OpenAI text-embedding-3-large)                │  │
│  │  • Similarity computation                                           │  │
│  └────────────────────────────────┬─────────────────────────────────────┘  │
│                                   │                                         │
│                                   ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         FINAL OUTPUT                                 │  │
│  │  • Master Parquet file per city                                     │  │
│  │  • Vector database (ChromaDB)                                       │  │
│  │  • API-ready JSON exports                                           │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Pipeline

### Pipeline Phases (8 Stages)

```
Phase 1: Initial Data Scraping
    ├── Scrape school lists from official portals
    ├── Extract: name, address, type, contact info
    └── Output: raw_schools_[city].csv

Phase 2: Data Combination & Deduplication
    ├── Merge multiple data sources
    ├── Handle schools with multiple programs (ISS + Gymnasium)
    ├── Normalize naming and remove duplicates
    └── Output: combined_schools_[city].csv

Phase 3: Academic Statistics Enrichment
    ├── Add Abitur averages and pass rates
    ├── Add MSA/exam statistics (often district-level)
    └── Output: schools_with_academics_[city].csv

Phase 4: Metadata Enrichment
    ├── Scrape school websites for additional data
    ├── Extract: languages, programs, founding year
    ├── LLM-assisted extraction for complex pages
    └── Output: schools_with_metadata_[city].csv

Phase 5: Geospatial & Context Enrichment
    ├── Add traffic data (cars, bikes, pedestrians)
    ├── Add crime statistics by district
    ├── Add transit accessibility scores
    ├── Add nearby POIs (parks, libraries, etc.)
    └── Output: schools_enriched_[city].csv

Phase 6: School Descriptions Generation
    ├── Crawl school websites
    ├── Extract PDFs (brochures, reports)
    ├── Generate descriptions using LLM
    ├── Extract tuition data for private schools
    └── Output: school_descriptions_[city]/

Phase 7: Vision-Based Data Validation
    ├── Take screenshots of official portals
    ├── Use GPT Vision to extract table data
    ├── Fill missing student/teacher counts
    └── Output: schools_validated_[city].csv

Phase 8: Embeddings & Similarity
    ├── Generate text embeddings (3072-dim)
    ├── Compute cosine similarity matrix
    ├── Identify top 3 most similar schools
    └── Output: school_master_table_[city]_with_embeddings.parquet
```

---

## 4. City-Specific vs Shared Components

### City-Specific Components (Must be created for each city)

| Component Type | Description | Example (Berlin) |
|---------------|-------------|------------------|
| **School Portal Scraper** | Scrapes official school directory | `bildung_berlin_gymnasien_scraper.py` |
| **Academic Stats Scraper** | Extracts Abitur/exam results | `add_msa_to_combined_master.py` |
| **Crime Data Processor** | Parses city police statistics | `crawl_crime_daten_berlin.py` |
| **Traffic Data Collector** | Fetches sensor data | `crawl_traffic_berlin_zaehlt.py` |
| **Transit Enrichment** | Uses city transit API | `enrich_schools_with_transit.py` |
| **Orchestrator** | Coordinates city pipeline | `Berlin_secondary_school_data_asset_builder_orchestrator.py` |

### Shared Components (Reusable across all cities)

| Component Type | Description | Script |
|---------------|-------------|--------|
| **POI Enrichment** | Google Places for nearby amenities | `enrich_schools_with_pois.py` |
| **Description Generator** | LLM-based description creation | `generate_school_descriptions_v4.py` |
| **Metadata Scraper** | Website scraping with LLM | `scrape_missing_school_metadata_llm.py` |
| **Embedding Generator** | OpenAI embeddings | (embedded in orchestrator) |
| **Vector DB Prep** | ChromaDB preparation | `prepare_vector_db_input.py` |
| **Tuition Parser** | Extract fees from text | `format_income_based_tuition.py` |

### Decision Matrix: City-Specific vs Shared

| Data Type | Reason for Classification |
|-----------|--------------------------|
| **School lists** | City-specific: Each city has unique portal |
| **Abitur scores** | City-specific: Published by state education ministry |
| **Crime stats** | City-specific: Police publish in different formats |
| **Traffic data** | City-specific: Different sensor networks |
| **Transit access** | City-specific: Different transit APIs (BVG/HVV/MVV) |
| **POIs** | Shared: Google Places works everywhere |
| **Descriptions** | Shared: LLM framework is city-agnostic |
| **Embeddings** | Shared: OpenAI API works on any text |

---

## 5. Supported Cities

### Currently Implemented

| City | Status | Schools | Data Sources |
|------|--------|---------|--------------|
| **Berlin** | Complete | 259 secondary | bildung.berlin.de, sekundarschulen-berlin.de |

### Planned

| City | Status | Notes |
|------|--------|-------|
| **Hamburg** | Planned | See `HAMBURG_IMPLEMENTATION_PLAN.md` |
| **Munich** | Backlog | Bayern has different school system |
| **Cologne** | Backlog | NRW data sources TBD |
| **Frankfurt** | Backlog | Hessen data sources TBD |

---

## 6. Data Schema

### Core Fields (All Cities)

```yaml
# Identification
schulnummer: string        # Official school ID
schulname: string          # Full school name
school_type: string        # Gymnasium, Stadtteilschule, ISS, etc.

# Location
strasse: string            # Street address
plz: string                # Postal code
ortsteil: string           # Neighborhood/Stadtteil
bezirk: string             # District
latitude: float            # WGS84 latitude
longitude: float           # WGS84 longitude

# Contact
telefon: string
email: string
website: string
leitung: string            # Principal name

# Academic Performance
abitur_durchschnitt_2024: float      # Latest Abitur average
abitur_durchschnitt_2023: float
abitur_erfolgsquote_2024: float      # Pass rate percentage

# Demographics
schueler_2024_25: int      # Current student count
lehrer_2024_25: int        # Current teacher count
migration_2024_25: float   # % with migration background

# Programs
sprachen: string           # Languages offered (comma-separated)
besonderheiten: string     # Special programs

# Enrichment - Traffic
plz_avg_cars_per_hour: float
plz_avg_bikes_per_hour: float
plz_pedestrian_ratio: float

# Enrichment - Crime
bezirk_crime_index: float
bezirk_crime_rate: float

# Enrichment - Transit
nearest_ubahn_distance_m: float
nearest_sbahn_distance_m: float
transit_stops_500m: int

# Enrichment - POIs
poi_park_count_500m: int
poi_library_count_500m: int
poi_primary_school_count_500m: int

# Generated Content
description: string        # LLM-generated description (500-1000 chars)
tuition_monthly_eur: float # For private schools
tuition_notes: string

# Embeddings & Similarity
embedding: array[float]    # 3072-dimensional vector
most_similar_school_no_01: string
most_similar_school_no_02: string
most_similar_school_no_03: string
```

### City-Specific Fields

**Berlin:**
- `nachfrage_plaetze_2025_26`: Application demand
- `belastungsstufe`: Workload classification
- `msa_notendurchschnitt_bezirk_2024`: District MSA average

**Hamburg (planned):**
- `stadtteil_crime_rate`: Stadtteil-level crime
- `hvv_stops_within_500m`: HVV transit stops

---

## 7. Directory Structure

```
schoolnossa/
│
├── scripts_shared/                    # Reusable across all cities
│   ├── enrichment/
│   │   ├── enrich_schools_with_pois.py
│   │   └── format_income_based_tuition.py
│   ├── generation/
│   │   ├── generate_school_descriptions_v4.py
│   │   └── scrape_missing_school_metadata_llm.py
│   ├── processing/
│   │   ├── prepare_vector_db_input.py
│   │   └── process_crime_traffic_for_vector_db.py
│   └── utils/
│       └── google_places_master_table_builder.py
│
├── scripts_berlin/                    # Berlin-specific scripts
│   ├── scrapers/
│   │   ├── ISS_data_scraper.py
│   │   ├── bildung_berlin_gymnasien_scraper.py
│   │   ├── bildung_berlin_iss_scraper.py
│   │   ├── scrape_bildung_berlin_v2.py
│   │   ├── crawl_crime_daten_berlin.py
│   │   └── crawl_traffic_berlin_zaehlt.py
│   ├── enrichment/
│   │   ├── add_msa_to_combined_master.py
│   │   ├── enrich_schools_with_traffic.py
│   │   └── enrich_schools_with_transit.py
│   ├── processing/
│   │   ├── combine_gymnasium_iss_bildung_berlin_and_metadata.py
│   │   └── merge_scraped_metadata.py
│   └── Berlin_secondary_school_data_asset_builder_orchestrator.py
│
├── scripts_hamburg/                   # Hamburg-specific scripts (planned)
│   ├── scrapers/
│   ├── enrichment/
│   ├── processing/
│   └── Hamburg_school_data_asset_builder_orchestrator.py
│
├── data_berlin/                       # Berlin generated data
│   ├── raw/                           # Initial scraped data
│   ├── intermediate/                  # Processing stages
│   ├── final/                         # Final outputs
│   │   ├── school_master_table_final_with_embeddings.parquet
│   │   └── school_master_table_final.csv
│   ├── descriptions/                  # Generated descriptions
│   └── screenshots/                   # Vision scraping screenshots
│
├── data_hamburg/                      # Hamburg generated data (planned)
│   ├── raw/
│   ├── intermediate/
│   ├── final/
│   └── descriptions/
│
├── data_shared/                       # Shared reference data
│   └── (common lookup tables if any)
│
├── docs/                              # Documentation
│   ├── PROJECT_ARCHITECTURE.md        # This file
│   ├── HAMBURG_IMPLEMENTATION_PLAN.md
│   └── README_*.md                    # Component-specific docs
│
├── archive/                           # Deprecated scripts and data
│   ├── scripts/                       # Old/unused scripts
│   └── data/                          # Old/unused data files
│
├── config.yaml                        # API keys and settings
├── requirements.txt                   # Python dependencies
└── .env                               # Environment variables
```

---

## 8. Script Reference

### Berlin Scripts

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `ISS_data_scraper.py` | Scrape ISS schools from sekundarschulen-berlin.de | Web | `ISS_master_table.csv` |
| `bildung_berlin_gymnasien_scraper.py` | Scrape Gymnasien from bildung.berlin.de | Web | `bildung_berlin_gymnasien.csv` |
| `bildung_berlin_iss_scraper.py` | Scrape ISS from bildung.berlin.de | Web | `bildung_berlin_iss.csv` |
| `combine_gymnasium_iss_bildung_berlin_and_metadata.py` | Merge all sources | CSVs | `combined_schools_with_metadata.csv` |
| `add_msa_to_combined_master.py` | Add MSA statistics | CSV | `combined_schools_with_metadata_msa.csv` |
| `scrape_bildung_berlin_v2.py` | Vision-based data extraction | Web screenshots | Updated CSV |
| `crawl_traffic_berlin_zaehlt.py` | Collect traffic sensor data | API | `postcode_traffic_averages.csv` |
| `crawl_crime_daten_berlin.py` | Collect crime statistics | Web | `bezirk_crime_statistics.csv` |
| `enrich_schools_with_traffic.py` | Add traffic to schools | CSV + traffic | Enriched CSV |
| `enrich_schools_with_transit.py` | Add BVG transit data | CSV + API | Enriched CSV |

### Shared Scripts

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `enrich_schools_with_pois.py` | Add nearby POIs | CSV with coords | Enriched CSV |
| `generate_school_descriptions_v4.py` | Generate LLM descriptions | CSV + websites | Descriptions JSON |
| `scrape_missing_school_metadata_llm.py` | Extract metadata with LLM | Websites | Metadata CSV |
| `prepare_vector_db_input.py` | Prepare for ChromaDB | Parquet | Vector DB files |
| `format_income_based_tuition.py` | Parse tuition text | Text | Structured fees |

### Archive Scripts

| Script | Reason for Archive |
|--------|-------------------|
| `generate_school_descriptions.py` | Superseded by v4 |
| `generate_school_descriptions_v2.py` | Superseded by v4 |
| `generate_school_descriptions_v3.py` | Superseded by v4 |
| `scrape_bildung_berlin_portal.py` | Superseded by v2 |
| `scrape_bildung_berlin_simple.py` | Superseded by v2 |
| `school_master_table_apify_maps.py` | Replaced by Google Places |
| `crawl_kita_navigator.py` | Kita project paused |
| `crawl_kita_details_from_archive.py` | Kita project paused |

---

## 9. Adding a New City

### Step-by-Step Guide

1. **Research Data Sources** (1-2 days)
   - Find official school portal
   - Locate Abitur/exam statistics
   - Identify crime data source
   - Find traffic sensor data
   - Check transit API availability

2. **Create City Folder Structure**
   ```bash
   mkdir -p scripts_[city]/{scrapers,enrichment,processing}
   mkdir -p data_[city]/{raw,intermediate,final,descriptions}
   ```

3. **Implement City-Specific Scrapers** (5-10 days)
   - School portal scraper
   - Academic statistics scraper
   - Crime data processor
   - Traffic data collector
   - Transit enrichment

4. **Adapt Shared Scripts** (1-2 days)
   - Update city boundaries for POI search
   - Configure LLM prompts for local context

5. **Create City Orchestrator** (1 day)
   - Copy Berlin orchestrator as template
   - Update script references
   - Adjust phase configurations

6. **Test and Validate** (2-3 days)
   - Run pipeline on subset
   - Verify data quality
   - Compare with known school info

### Checklist for New City

- [ ] School portal scraper implemented
- [ ] Academic statistics integrated
- [ ] Crime data available and processed
- [ ] Traffic data integrated
- [ ] Transit accessibility calculated
- [ ] POI enrichment working
- [ ] Descriptions generated
- [ ] Embeddings computed
- [ ] Similarity calculated
- [ ] Orchestrator tested end-to-end

---

## 10. Technology Stack

### Languages & Frameworks

- **Python 3.9+**: Primary language
- **Pandas**: Data manipulation
- **GeoPandas**: Geospatial operations
- **Selenium**: Browser automation
- **BeautifulSoup4**: HTML parsing
- **Rich**: Progress bars and console output

### APIs & Services

| Service | Purpose | Key Required |
|---------|---------|--------------|
| OpenAI | Embeddings, GPT Vision, descriptions | Yes |
| Google Places | POI enrichment | Yes |
| Gemini | Alternative LLM | Yes |
| Perplexity | Web search for descriptions | Yes |

### Data Storage

- **CSV/XLSX**: Intermediate processing
- **Parquet**: Final output (efficient, typed)
- **ChromaDB**: Vector similarity search
- **JSON**: School descriptions

### Infrastructure

- **Local execution**: All scripts run locally
- **No cloud dependencies**: Except API calls
- **Git**: Version control

---

## Appendix: Glossary

| Term | German | English | Notes |
|------|--------|---------|-------|
| Abitur | Abitur | A-levels equivalent | Final secondary exam |
| MSA | Mittlerer Schulabschluss | Intermediate diploma | After grade 10 |
| ISS | Integrierte Sekundarschule | Comprehensive school | Berlin term |
| STS | Stadtteilschule | District school | Hamburg term |
| ndH | nichtdeutsche Herkunftssprache | Non-German native language | Berlin metric |
| Bezirk | Bezirk | District | Administrative division |
| Ortsteil | Ortsteil | Neighborhood | Sub-district |
| Stadtteil | Stadtteil | City quarter | Hamburg term |

---

*Document Version: 1.0*
*Last Updated: 2026-01-25*
*Maintainer: SchoolNossa Team*
