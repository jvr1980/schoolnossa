# Dresden Pipeline Status

## Data Sources
- [x] School master data: Sächsische Schuldatenbank API (CSV, WGS84, free)
- [x] Traffic data: Unfallatlas accident counts (ULAND=14, EPSG:25832)
- [x] Transit data: Overpass API (OSM, free, no key)
- [x] Crime data: Dresden Open Data Portal (Stadtteil-level CSV, 2002–2024)
- [x] POI data: Google Places API (shared across all cities)
- [x] Demographics: GISD proxy by PLZ (no per-school Sozialindex in Sachsen)
- [x] Academic performance: Aggregate only (no per-school data published)
- [x] Website metadata: School URLs from Schuldatenbank API homepage field

## Phase Implementation Status
- [ ] Phase 1: School Master Data — STUB (scrapers/dresden_school_master_scraper.py)
- [ ] Phase 2: Traffic Enrichment — STUB (enrichment/dresden_traffic_enrichment.py)
- [ ] Phase 3: Transit Enrichment — STUB (enrichment/dresden_transit_enrichment.py)
- [ ] Phase 4: Crime Enrichment — STUB (enrichment/dresden_crime_enrichment.py)
- [ ] Phase 5: POI Enrichment — STUB (enrichment/dresden_poi_enrichment.py)
- [ ] Phase 6: Website Metadata — STUB (enrichment/dresden_website_metadata_enrichment.py)
- [ ] Phase 7: Data Combination — STUB (processing/dresden_data_combiner.py)
- [ ] Phase 8: Embeddings — STUB (processing/dresden_embeddings_generator.py)
- [ ] Phase 9: Schema Transformer — STUB (dresden_to_berlin_schema.py)

## Expected School Counts (2024/25)
- Grundschulen: 72 (20,295 students)
- Oberschulen: 27 (11,851 students)
- Gymnasien: 21 (17,833 students)
- Förderschulen: 14 (2,085 students)
- Other: 4 (Gemeinschaftsschulen, Abendschulen)
- Total public: 148 schools
- Private operators: ~41 (serving ~16,800 additional students)

## Template City
NRW pipeline (closest match — same Unfallatlas traffic source, Overpass transit, CSV-based schools)

## Key Differences from NRW
- School data: API with WGS84 coords (no UTM conversion for schools)
- Crime: Stadtteil-level (much better than NRW's city-wide estimates)
- Demographics: GISD proxy only (no Schulsozialindex in Sachsen)
- No Anmeldezahlen phase needed
