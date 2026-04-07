# Bremen Pipeline Status

## Data Sources
- [x] School master data: Schulwegweiser Excel (bildung.bremen.de) + GeoBremen Shapefile (EPSG:25832)
- [x] Traffic data: Unfallatlas accident data (ULAND=04)
- [x] Transit data: Overpass API (same as NRW)
- [x] Crime data: PKS Stadtteil-level from parliamentary PDFs (22 Beiratsbereiche, 7 categories)
- [x] POI data: Google Places API (shared across all cities)
- [x] Demographics: Kleinraumig Infosystem + Ortsteilatlas (Ortsteil-level)
- [x] Academic performance: Per-school Abitur averages (2015-2025, HTML scraping)
- [x] Website metadata: School websites accessible via Schulwegweiser

## Phase Implementation Status
- [ ] Phase 1: School Master Data — STUB
- [ ] Phase 2: Traffic Enrichment — STUB
- [ ] Phase 3: Transit Enrichment — STUB
- [ ] Phase 4: Crime Enrichment — STUB
- [ ] Phase 5: POI Enrichment — STUB
- [ ] Phase 6: Website Metadata & Descriptions — STUB
- [ ] Phase 7: Data Combination — STUB
- [ ] Phase 8: Embeddings — STUB
- [ ] Phase 9: Schema Transformer — STUB

## Expected School Counts
- Primary (Grundschule): ~75
- Secondary (Oberschule + Gymnasium): ~50
- Other (Werkschule, Beratungszentren, etc.): ~75
- Total: ~200 (Bremen + Bremerhaven)

## Key Notes
- Combined pipeline (all school types together, like Hamburg)
- Template city: NRW (same EPSG:25832 coords, Unfallatlas traffic, Overpass transit)
- Crime data from PDF tables (tabula-py extraction from Kleine Anfragen)
- No public per-school Sozialindex — use Ortsteil-level proxy from Kleinraumig
- Decide: include Bremerhaven or filter to Stadt Bremen only?
