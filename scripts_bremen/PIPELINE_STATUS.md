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
- [x] Phase 1: School Master Data — IMPLEMENTED (Schulwegweiser Excel + GeoBremen Shapefile + Nominatim geocoding)
- [x] Phase 2: Traffic Enrichment — IMPLEMENTED (Unfallatlas ULAND=04, 500m radius)
- [x] Phase 3: Transit Enrichment — IMPLEMENTED (Overpass API, Berlin-compatible columns)
- [x] Phase 4: Crime Enrichment — IMPLEMENTED (22 Beiratsbereiche, hardcoded + PDF parsing)
- [x] Phase 5: POI Enrichment — IMPLEMENTED (Google Places API, 8 categories, threaded)
- [x] Phase 6: Website Metadata & Descriptions — IMPLEMENTED (Gemini + Google Search grounding)
- [x] Phase 7: Data Combination — IMPLEMENTED (sequential merge by schulnummer)
- [x] Phase 8: Embeddings — IMPLEMENTED (OpenAI text-embedding-3-large + Gemini fallback)
- [x] Phase 9: Schema Transformer — IMPLEMENTED (Berlin-compatible column mapping)

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
