# Leipzig Pipeline Status

## Data Sources (from research)
- [x] School master data: Sächsische Schuldatenbank API (CSV/JSON, WGS84, no auth)
- [x] Traffic data: Unfallatlas accident atlas (ULAND=14 for Sachsen)
- [x] Transit data: LVB GTFS feed (tram + bus) + MDV GTFS (S-Bahn) + Overpass fallback
- [x] Crime data: Leipzig Open Data API, Ortsteil-level (63 districts), CC BY 4.0
- [x] POI data: Google Places API (shared)
- [x] Demographics: Ortsteilkatalog + Zensus 2022 grid (no Sozialindex published)
- [ ] Academic performance: NOT AVAILABLE per school (state averages only)
- [x] Website metadata: School homepages in Schuldatenbank API `homepage` field

## Phase Implementation Status
- [x] Phase 1: School Master Data (Schuldatenbank API) — IMPLEMENTED
- [x] Phase 2: Traffic Enrichment (Unfallatlas ULAND=14) — IMPLEMENTED
- [x] Phase 3: Transit Enrichment (LVB GTFS + Overpass) — IMPLEMENTED
- [x] Phase 4: Crime Enrichment (Ortsteil-Level API) — IMPLEMENTED
- [x] Phase 5: POI Enrichment (Google Places) — IMPLEMENTED
- [x] Phase 6: Website Metadata & Descriptions (Gemini) — IMPLEMENTED
- [x] Phase 7: Data Combination — IMPLEMENTED
- [x] Phase 8: Embeddings & Final Output — IMPLEMENTED
- [x] Phase 9: Berlin Schema Enforcement — IMPLEMENTED

## Expected School Counts (allgemeinbildende Schulen)
- Grundschule: ~80
- Oberschule: ~30
- Gymnasium: ~15
- Förderschule: ~15
- Total (general education): ~140

## Key Differences from Other Cities
- API-based school data retrieval (REST API, not static CSV or scraper)
- GTFS-based transit (cleaner than Overpass, includes route/frequency data)
- Excellent Ortsteil-level crime data (63 districts, 20 years of history)
- No per-school Abitur averages (Saxony doesn't publish)
- No published Sozialindex (use Ortsteil demographics as proxy)
- Student enrollment data available per school since 2021 (Leipzig Open Data)
