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
- [x] Phase 1: School Master Data — DONE (159 schools: 88 primary, 69 secondary, 2 Förderschulen)
- [x] Phase 2: Traffic Enrichment — DONE (Unfallatlas ULAND=14)
- [x] Phase 3: Transit Enrichment — DONE (Overpass API, Berlin-compatible output)
- [x] Phase 4: Crime Enrichment — DONE (Stadtteil-level from Open Data Portal)
- [x] Phase 5: POI Enrichment — DONE (159/159, avg 4.2 supermarkets, 7.9 restaurants within 500m)
- [x] Phase 6: Description Pipeline — DONE (159/159, 96% EN descriptions, 91% besonderheiten)
- [x] Phase 7: Data Combination — DONE (192 columns)
- [x] Phase 8: Embeddings — SKIPPED (no OPENAI_API_KEY in worktree)
- [x] Phase 9: Schema Transformer — DONE (Berlin reference schema enforcement)
- [x] Phase 10: Tuition Pass 1 — DONE (35/35 private schools: 33 medium, 1 ultra, 1 low)
- [x] Phase 11: Tuition Pass 2 — DONE (35/35 income matrices)
- [x] Phase 12: Tuition Pass 3 — DONE (16 income-based, 13 flat-fee confirmed)

## Expected School Counts (2024/25)
- Grundschulen: 88 (incl. private)
- Oberschulen: 39 (incl. private)
- Gymnasien: 30 (incl. private)
- Förderschulen: 2
- Total from API: 159 schools (124 public + 35 private)
- 100% coordinate coverage, 97% website coverage

## Template City
NRW pipeline (closest match — same Unfallatlas traffic source, Overpass transit, CSV-based schools)

## Key Differences from NRW
- School data: API with WGS84 coords (no UTM conversion for schools)
- Crime: Stadtteil-level (much better than NRW's city-wide estimates)
- Demographics: GISD proxy only (no Schulsozialindex in Sachsen)
- No Anmeldezahlen phase needed
