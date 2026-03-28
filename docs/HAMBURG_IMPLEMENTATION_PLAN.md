# Hamburg Secondary School Data Asset Builder - Implementation Plan

## Executive Summary

This document outlines the plan to create a Hamburg secondary school database comparable to our Berlin implementation. The goal is to build a comprehensive master data table for Hamburg's **Stadtteilschulen** (comparable to Berlin's Integrierte Sekundarschulen) and **Gymnasien**.

---

## 1. Data Source Comparison: Berlin vs Hamburg

### Berlin Sources (Current Implementation)

| Data Type | Source | Script |
|-----------|--------|--------|
| ISS Schools | sekundarschulen-berlin.de | `ISS_data_scraper.py` |
| Gymnasien | bildung.berlin.de | `bildung_berlin_gymnasien_scraper.py` |
| ISS (official) | bildung.berlin.de | `bildung_berlin_iss_scraper.py` |
| Student/Teacher counts | bildung.berlin.de (vision) | `scrape_bildung_berlin_v2.py` |
| Migration % (ndH) | bildung.berlin.de | `scrape_bildung_berlin_v2.py` |
| MSA Statistics | Senatsverwaltung PDFs | `add_msa_to_combined_master.py` |
| Traffic Data | berlin-zaehlt.de | `crawl_traffic_berlin_zaehlt.py` |
| Crime Statistics | kriminalitaetsatlas-berlin.de | `crawl_crime_daten_berlin.py` |

### Hamburg Equivalent Sources (Identified)

| Data Type | Hamburg Source | Availability | Format |
|-----------|---------------|--------------|--------|
| **School Master Data** | [Transparenzportal Hamburg](https://suche.transparenz.hamburg.de/dataset/schulstammdaten-und-schuelerzahlen-der-hamburger-schulen16) | **Open Data** | CSV, GeoJSON |
| **School Portal (detailed)** | [Schulinfosystem Hamburg](https://geoportal-hamburg.de/schulinfosystem/) | Web portal | WFS API |
| **Abitur Statistics** | [hamburg.de Abitur PDFs](https://www.hamburg.de/resource/blob/941494/d96604f49abdcfd67f72d2c11e2a68b1/ergebnisse-der-vorlaeufigen-abiturabfrage-2024-data.pdf) | PDF | Requires parsing |
| **Abitur by School** | [gymnasium-hamburg.net](https://gymnasium-hamburg.net/abiturnoten) | Web scraping | HTML |
| **Student/Teacher counts** | Transparenzportal + IfBQ | **Open Data** | CSV |
| **Migration Statistics** | [IfBQ Hamburg](https://ifbq.hamburg.de/) | Reports | PDF/Web |
| **Traffic Data** | [iot.hamburg.de](https://iot.hamburg.de/v1.1/) | **API** | SensorThings API (JSON) |
| **Crime Statistics** | [Stadtteilatlas PKS](https://suche.transparenz.hamburg.de/dataset/stadtteilatlas-der-polizeilichen-kriminalstatistik-pks-hamburgs-fuer-das-jahr-2024) | **Open Data** | PDF, CSV |
| **HVV Transit Data** | [HVV Haltestellen](https://suche.transparenz.hamburg.de/dataset/einzugsbereiche-von-hvv-haltestellen-hamburg4) | **Open Data** | GeoJSON, CSV |
| **Pre-scraped Data** | [JedeSchule.de](https://jedeschule.de/daten/) | **Open Data** | CSV, JSON |

---

## 2. Key Differences: Berlin vs Hamburg School Systems

### School Types

| Berlin | Hamburg | Notes |
|--------|---------|-------|
| Integrierte Sekundarschule (ISS) | **Stadtteilschule (STS)** | Both lead to MSA and Abitur (9 years) |
| Gymnasium | **Gymnasium** | G8 in Hamburg (8 years to Abitur) |
| Gemeinschaftsschule | (integrated in STS) | Hamburg combined these |

### Administrative Structure

| Aspect | Berlin | Hamburg |
|--------|--------|---------|
| Districts | 12 Bezirke | 7 Bezirke |
| Sub-districts | Ortsteile | Stadtteile (104 total) |
| School Authority | Senatsverwaltung für Bildung | Behörde für Schule und Berufsbildung |
| School Numbers | Format: 01K01, 01Y02, 03P05 | Format: Different numbering |

### Statistics Terminology

| Berlin | Hamburg |
|--------|---------|
| ndH (nichtdeutsche Herkunftssprache) | Migrationshintergrund |
| MSA (Mittlerer Schulabschluss) | MSA (same) |
| Abitur Durchschnitt | Abitur Durchschnitt (same) |

---

## 3. Data Fields Mapping

### Core Fields (Berlin → Hamburg)

| Berlin Field | Hamburg Equivalent | Source |
|--------------|-------------------|--------|
| `schulnummer` | `official_id` / School code | Transparenzportal |
| `schulname` | `name` | Transparenzportal |
| `school_type` | `school_type` (Gymnasium/Stadtteilschule) | Transparenzportal |
| `strasse` | `address` | Transparenzportal |
| `plz` | From address parsing | Transparenzportal |
| `bezirk` | From Stadtteil mapping | Geodata |
| `ortsteil` | `stadtteil` | Geodata |
| `telefon` | `phone` | Transparenzportal |
| `email` | `email` | School websites |
| `website` | `website` | School websites |
| `latitude` | `lat` | Transparenzportal GeoJSON |
| `longitude` | `lon` | Transparenzportal GeoJSON |

### Statistics Fields

| Berlin Field | Hamburg Source | Notes |
|--------------|---------------|-------|
| `schueler_2024_25` | Schulinfosystem API | Available per school |
| `lehrer_2024_25` | Schulinfosystem API | May need scraping |
| `migration_2024_25` | IfBQ reports | District-level, not per school |
| `abitur_durchschnitt_2024` | hamburg.de PDF / gymnasium-hamburg.net | Available per school |
| `msa_notendurchschnitt` | Need to research | May be district-level only |

### Enrichment Fields

| Field Category | Hamburg Source | Implementation |
|---------------|---------------|----------------|
| Traffic data | iot.hamburg.de SensorThings API | New scraper needed |
| Crime statistics | PKS Stadtteilatlas | PDF parsing or CSV |
| Transit accessibility | HVV Haltestellen GeoJSON | Distance calculation |
| POIs (Parks, etc.) | OpenStreetMap / Google Places | Same as Berlin |

---

## 4. Implementation Phases

### Phase 1: Core Data Collection (New Scripts)

```
hamburg_school_master_scraper.py
```
- Download CSV/GeoJSON from Transparenzportal
- Parse and normalize data
- Filter for Stadtteilschulen and Gymnasien only
- Extract: name, address, PLZ, coordinates, school type, contact info

**Input**: https://suche.transparenz.hamburg.de/dataset/schulstammdaten-und-schuelerzahlen-der-hamburger-schulen16
**Output**: `hamburg_schools_raw.csv`

### Phase 2: Abitur Statistics Enrichment

```
hamburg_abitur_scraper.py
```
- Scrape gymnasium-hamburg.net for historical Abitur averages
- Parse hamburg.de PDF for official 2024 data
- Match by school name to master table

**Input**: gymnasium-hamburg.net/abiturnoten, hamburg.de Abitur PDFs
**Output**: `hamburg_abitur_statistics.csv`

### Phase 3: Student/Teacher Data

```
hamburg_schulinfosystem_scraper.py
```
- Query WFS API at geoportal-hamburg.de
- Or use JedeSchule.de pre-scraped data as baseline
- Vision-based scraping from Schulinfosystem if needed

**Input**: WFS API / Schulinfosystem web portal
**Output**: Student counts, teacher counts per school

### Phase 4: Traffic Data Integration

```
hamburg_traffic_data_collector.py
```
- Connect to SensorThings API at iot.hamburg.de
- Query Kfz and Rad infrared detector data
- Aggregate by PLZ/Stadtteil

**Input**: https://iot.hamburg.de/v1.1/
**Output**: `hamburg_plz_traffic_averages.csv`

### Phase 5: Crime Statistics Integration

```
hamburg_crime_data_processor.py
```
- Download PKS Stadtteilatlas from Transparenzportal
- Parse PDF or use CSV export
- Map crime rates to Stadtteile

**Input**: PKS Stadtteilatlas PDF/CSV
**Output**: `hamburg_stadtteil_crime_statistics.csv`

### Phase 6: Transit Accessibility

```
hamburg_hvv_transit_enrichment.py
```
- Download HVV Haltestellen GeoJSON
- Calculate distance from each school to nearest stops
- Compute transit accessibility score

**Input**: HVV Haltestellen GeoJSON
**Output**: Transit columns in master table

### Phase 7: School Descriptions Generation

```
hamburg_school_descriptions_generator.py
```
- Crawl school websites
- Extract programs, languages, special features
- Generate descriptions using LLM (same approach as Berlin)

**Output**: `hamburg_school_descriptions/`

### Phase 8: Embeddings & Similarity

```
hamburg_embeddings_generator.py
```
- Generate OpenAI text-embedding-3-large embeddings
- Compute similarity matrix
- Add top 3 similar schools

**Output**: `hamburg_school_master_table_final_with_embeddings.parquet`

---

## 5. Script Adaptation Strategy

### Scripts to Reuse (with modifications)

| Berlin Script | Hamburg Adaptation | Changes Needed |
|--------------|-------------------|----------------|
| `enrich_schools_with_pois.py` | Minor changes | Update city bounds |
| `generate_school_descriptions_v4.py` | Mostly reusable | Update field names |
| `prepare_vector_db_input.py` | Mostly reusable | Update schema |

### Scripts Requiring Complete Rewrite

| Berlin Script | Hamburg Equivalent | Reason |
|--------------|-------------------|--------|
| `ISS_data_scraper.py` | `hamburg_stadtteilschule_scraper.py` | Different source |
| `bildung_berlin_gymnasien_scraper.py` | `hamburg_gymnasium_scraper.py` | Different portal |
| `scrape_bildung_berlin_v2.py` | `hamburg_schulinfosystem_scraper.py` | Different API |
| `crawl_traffic_berlin_zaehlt.py` | `hamburg_traffic_sensor_collector.py` | Different API (SensorThings) |
| `crawl_crime_daten_berlin.py` | `hamburg_pks_crime_processor.py` | Different format |

### Scripts with Minor Modifications

| Script | Changes |
|--------|---------|
| `enrich_schools_with_transit.py` | Change from BVG to HVV data source |
| `google_places_master_table_builder.py` | Update city parameter |

---

## 6. Data Quality Considerations

### Advantages of Hamburg Data

1. **Better Open Data**: Hamburg's Transparenzportal provides more structured data
2. **Real-time Traffic API**: SensorThings API is more modern than Berlin's approach
3. **Pre-scraped baseline**: JedeSchule.de has Hamburg data ready

### Challenges

1. **Migration statistics**: Only available at district level, not per school
2. **Abitur data**: Need to parse PDFs for complete historical data
3. **Schulinfosystem**: May require web scraping (no direct API for all data)
4. **Less granular crime data**: Stadtteil level (104) vs Berlin's finer grain

---

## 7. Technical Requirements

### New Dependencies

```python
# requirements_hamburg.txt
requests>=2.28.0
pandas>=1.5.0
geopandas>=0.12.0  # For GeoJSON handling
beautifulsoup4>=4.11.0
selenium>=4.8.0
openai>=1.0.0
PyPDF2>=3.0.0  # For PDF parsing
pdfplumber>=0.9.0  # Better PDF tables
geopy>=2.3.0  # Distance calculations
```

### API Keys Needed

- OpenAI API key (embeddings, descriptions)
- Google Places API key (POIs) - same as Berlin
- No special key needed for Hamburg Open Data

---

## 8. Estimated Timeline

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Phase 1: Core Data | 2-3 days | None |
| Phase 2: Abitur Stats | 2-3 days | Phase 1 |
| Phase 3: Student/Teacher | 3-5 days | Phase 1 |
| Phase 4: Traffic Data | 2-3 days | Phase 1 |
| Phase 5: Crime Data | 1-2 days | Phase 1 |
| Phase 6: Transit | 1-2 days | Phase 1 |
| Phase 7: Descriptions | 3-5 days | Phases 1-6 |
| Phase 8: Embeddings | 1 day | Phase 7 |

**Total Estimated Time**: 2-3 weeks

---

## 9. Output Schema

The final Hamburg master table will have these columns:

```
# Core identification
schulnummer, schulname, school_type, schulart, traegerschaft

# Location
strasse, plz, stadtteil, bezirk, latitude, longitude

# Contact
telefon, email, website, leitung

# Statistics
schueler_2024_25, lehrer_2024_25
migration_bezirk_2024_25  # Note: district-level only
abitur_durchschnitt_2024, abitur_durchschnitt_2023
abitur_erfolgsquote_2024

# Traffic (by PLZ/Stadtteil)
plz_avg_cars_per_hour, plz_avg_bikes_per_hour, ...

# Crime (by Stadtteil)
stadtteil_crime_rate_2024, stadtteil_crime_index, ...

# Transit
hvv_stops_within_500m, nearest_ubahn_distance_m, nearest_sbahn_distance_m

# POIs
poi_park_count_500m, poi_primary_school_count_500m, ...

# Generated content
description, tuition_* (for private schools)

# Embeddings & Similarity
embedding, most_similar_school_no_01, most_similar_school_no_02, most_similar_school_no_03
```

---

## 10. File Structure

```
schoolnossa/
├── hamburg/
│   ├── scrapers/
│   │   ├── hamburg_school_master_scraper.py
│   │   ├── hamburg_abitur_scraper.py
│   │   ├── hamburg_schulinfosystem_scraper.py
│   │   ├── hamburg_traffic_sensor_collector.py
│   │   └── hamburg_pks_crime_processor.py
│   ├── enrichment/
│   │   ├── hamburg_transit_enrichment.py
│   │   ├── hamburg_poi_enrichment.py
│   │   └── hamburg_description_generator.py
│   ├── processing/
│   │   ├── hamburg_data_combiner.py
│   │   └── hamburg_embeddings_generator.py
│   ├── data/
│   │   ├── raw/
│   │   ├── processed/
│   │   └── final/
│   ├── Hamburg_school_data_asset_builder_orchestrator.py
│   └── README_HAMBURG.md
```

---

## 11. Key URLs Reference

### Official Data Sources

- **School Master Data**: https://suche.transparenz.hamburg.de/dataset/schulstammdaten-und-schuelerzahlen-der-hamburger-schulen16
- **Schulinfosystem**: https://geoportal-hamburg.de/schulinfosystem/
- **Abitur Results PDF**: https://www.hamburg.de/resource/blob/941494/d96604f49abdcfd67f72d2c11e2a68b1/ergebnisse-der-vorlaeufigen-abiturabfrage-2024-data.pdf
- **Traffic Sensors API**: https://iot.hamburg.de/v1.1/
- **Crime Statistics**: https://suche.transparenz.hamburg.de/dataset/stadtteilatlas-der-polizeilichen-kriminalstatistik-pks-hamburgs-fuer-das-jahr-2024
- **HVV Transit Stops**: https://suche.transparenz.hamburg.de/dataset/einzugsbereiche-von-hvv-haltestellen-hamburg4

### Third-Party Sources

- **gymnasium-hamburg.net**: https://gymnasium-hamburg.net/abiturnoten
- **JedeSchule.de**: https://jedeschule.de/daten/
- **GitHub Scraper**: https://github.com/Datenschule/jedeschule-scraper

---

## 12. Next Steps

1. **Create `hamburg/` directory structure**
2. **Start with Phase 1**: Download and parse Transparenzportal data
3. **Validate data quality** against JedeSchule.de baseline
4. **Iterate through phases** building the complete pipeline
5. **Create Hamburg orchestrator** similar to Berlin version

---

*Document created: 2026-01-24*
*Based on research of Hamburg Open Data sources and comparison with Berlin implementation*
