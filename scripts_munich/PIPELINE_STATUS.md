# Munich Pipeline Status

## Data Sources
- [x] School master data: km.bayern.de Schulsuche CSV (ISO-8859-15, semicolon, needs geocoding)
- [x] Traffic data: Unfallatlas (ULAND=09, accident-based like NRW)
- [x] Transit data: MVV GTFS (U-Bahn, S-Bahn, Tram, Bus) + Overpass fallback
- [x] Crime data: PP München Sicherheitsreport PDF (per-Stadtbezirk, 25 districts)
- [x] POI data: Google Places API (shared across all cities)
- [x] Demographics: Indikatorenatlas München (per-Stadtbezirk CSV)
- [x] Academic performance: NOT AVAILABLE in Bavaria (VERA password-protected)
- [x] Website metadata: School websites via Schulsuche links

## Phase Implementation Status
- [x] Phase 1: School Master Data — IMPLEMENTED (Schulsuche CSV + jedeschule coords + Nominatim geocoding)
- [x] Phase 2: Traffic Enrichment — IMPLEMENTED (Unfallatlas, ULAND=09, Munich bbox)
- [x] Phase 3: Transit Enrichment — IMPLEMENTED (Overpass API: U-Bahn/S-Bahn/Tram/Bus)
- [x] Phase 4: Crime Enrichment — IMPLEMENTED (city-level PKS, HZ=7684/100k)
- [x] Phase 5: POI Enrichment — IMPLEMENTED (Google Places API)
- [x] Phase 6: Website Metadata & Descriptions — IMPLEMENTED (Gemini API)
- [x] Phase 7: Data Combination — IMPLEMENTED (auto-detect most-enriched)
- [x] Phase 8: Embeddings — IMPLEMENTED (OpenAI/Gemini)
- [x] Phase 9: Schema Transformer — IMPLEMENTED (Berlin schema enforcement)

## Expected School Counts
- Secondary (Gymnasium + Realschule + Mittelschule + other): ~120-150 schools
- Primary: not in scope for this pipeline

## Notes
- Template city: NRW (closest data source pattern)
- Geocoding required: Schulsuche CSV has no coordinates
- Crime data: PDF tables need extraction or city-wide fallback
- No per-school academic data available in Bayern
