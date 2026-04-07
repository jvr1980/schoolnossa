# Munich Pipeline Status

## Data Sources
- [x] School master data: jedeschule.codefor.de (CC0, WKB coords) + Nominatim geocoding fallback
- [x] Traffic data: Unfallatlas (ULAND=09, accident-based like NRW)
- [x] Transit data: Overpass API (U-Bahn, S-Bahn, Tram, Bus)
- [x] Crime data: BKA PKS city-level (HZ=7684/100k, safest major city)
- [x] POI data: Google Places API (shared across all cities)
- [ ] Demographics: Indikatorenatlas München (per-Stadtbezirk CSV) — not yet implemented
- [x] Academic performance: NOT AVAILABLE in Bavaria (no per-school data published)
- [x] Website metadata: School websites + Gemini API descriptions

## Phase Implementation Status

### Secondary Schools (108 schools)
- [x] Phase 1: School Master Data — COMPLETE
- [x] Phase 2: Traffic Enrichment — COMPLETE
- [x] Phase 3: Transit Enrichment — COMPLETE
- [x] Phase 4: Crime Enrichment — COMPLETE
- [x] Phase 5: POI Enrichment — COMPLETE
- [x] Phase 6: Website Metadata & Descriptions — COMPLETE
- [x] Phase 7: Data Combination — COMPLETE
- [x] Phase 8: Embeddings — COMPLETE
- [x] Phase 9: Schema Transformer — COMPLETE

### Primary Schools (Grundschulen)
- [x] Phase 1: School Master Data — IMPLEMENTED (added 2026-04-07)
- [x] Phase 2: Traffic Enrichment — IMPLEMENTED
- [x] Phase 3: Transit Enrichment — IMPLEMENTED
- [x] Phase 4: Crime Enrichment — IMPLEMENTED
- [x] Phase 5: POI Enrichment — IMPLEMENTED
- [x] Phase 6: Website Metadata & Descriptions — IMPLEMENTED
- [x] Phase 7: Data Combination — IMPLEMENTED
- [x] Phase 8: Embeddings — IMPLEMENTED
- [x] Phase 9: Schema Transformer — IMPLEMENTED

## Expected School Counts
- Secondary (Gymnasium + Realschule + Mittelschule + Förderzentrum + other): ~108 schools
- Primary (Grundschule + Volksschule): ~130-150 schools (estimated)

## Notes
- All scripts parameterized with `school_type` ('primary' or 'secondary')
- Orchestrator runs both types by default: `--school-types primary,secondary`
- jedeschule.codefor.de provides coordinates (no Schulsuche CSV geocoding needed)
- Crime data: city-wide aggregate (PDF district data not yet parsed)
- No per-school academic data available in Bayern
