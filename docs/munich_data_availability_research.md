# Munich (München) Data Availability Research

**Date:** 2026-04-01
**State:** Bayern (Bavaria)
**Researcher:** Claude + user
**School types requested:** Secondary schools (weiterführende Schulen)

## Summary

| Category | Source | Granularity | Format | Access | Status |
|----------|--------|-------------|--------|--------|--------|
| School Master | km.bayern.de Schulsuche CSV | per-school | CSV (semicolon, ISO-8859-15) | free download | Ready |
| Traffic | Unfallatlas (national) | per-accident GPS | CSV | free download | Ready |
| Crime | PP München Sicherheitsreport + stadt.muenchen.de | per-Stadtbezirk | PDF (needs parsing) | free download | Moderate effort |
| Transit | MVV GTFS + Overpass API | per-stop | GTFS ZIP / Overpass JSON | free / no key | Ready |
| POI | Google Places API (New) | 500m radius | API JSON | API key needed | Ready |
| Demographics | stadt.muenchen.de Indikatorenatlas + Open Data | per-Stadtbezirk (25 districts) | CSV/XLSX | free download | Ready |
| Academic | Not publicly available per-school | state-level only | N/A | N/A | Not available |
| Websites | School websites via Schulsuche links | per-school | HTML | scraping | Ready |

## Detailed Findings

### 1. School Master Data

#### 1A. Bayerisches Staatsministerium — Schulsuche CSV (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://www.km.bayern.de/schulsuche |
| **Download** | CSV export available via the search interface ("als CSV exportieren") |
| **Format** | CSV, semicolon-separated |
| **Encoding** | ISO-8859-15 |
| **Records** | ~6,100 schools across all of Bavaria |
| **Munich filter** | Filter by PLZ prefix "80"/"81" or location "München" |
| **Cost** | Free |
| **API key** | Not needed |

**Available fields (7 columns):**
- `Schulnummer` — unique school ID
- `Schulart` — school type (Gymnasium, Realschule, Mittelschule, etc.)
- `Name` — school name
- `Straße` — street address
- `PLZ` — postal code
- `Ort` — city/location
- `Link` — relative URL to school detail page (e.g., `/schule/7180.html`)

**Important notes:**
- No coordinates included — will need geocoding via Google Geocoding API or Nominatim
- No student counts in this export — only basic directory info
- School detail pages at km.bayern.de may contain additional info (website URL, phone, etc.)
- Secondary school types to filter: Gymnasium, Realschule, Mittelschule, Gesamtschule, Förderschule

**Estimated Munich secondary schools:** ~120-150 schools (Gymnasien, Realschulen, Mittelschulen)

#### 1B. Open Data Portal München — Schulstandorte

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de (search "Schulen") |
| **Format** | CSV |
| **Updated** | April 2025 (school year 2024/25) |
| **Coverage** | Munich city schools with student numbers |

The Munich Open Data portal has school datasets with student counts per school. This may supplement the Schulsuche CSV with enrollment data. However, coordinates may still need geocoding.

#### 1C. geoportal.muenchen.de — Open Geodata

| Property | Value |
|----------|-------|
| **URL** | https://geoportal.muenchen.de/portal/opendata/ |
| **Format** | Potentially GeoJSON/WFS |
| **Coverage** | Munich city geographic features |

Munich's GeoPortal may contain school location layers with coordinates. Worth checking for a Schulstandorte layer.

### 2. Traffic Data

#### 2A. Unfallatlas — National Accident Data (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://unfallatlas.statistikportal.de/ |
| **Download** | https://www.destatis.de (bulk CSV download) |
| **Format** | CSV |
| **Encoding** | UTF-8 |
| **Coordinate system** | WGS84 (EPSG:4326) — NOTE: German comma decimal separator |
| **Filter** | `ULAND = 09` for Bayern; then filter by Munich coordinates bounding box |
| **Coverage** | Individual accident locations with severity, year, type |
| **Years available** | 2016–2023 |
| **Cost** | Free |

**Same approach as NRW pipeline** — filter accidents within configurable radius of each school, count by severity category.

**Munich bounding box (approximate):**
- Lat: 48.06 – 48.25
- Lon: 11.36 – 11.72

#### 2B. opendata.muenchen.de — Traffic counts

Munich's Open Data portal may have traffic counting station data, but accident data from Unfallatlas is more directly comparable to the NRW approach.

### 3. Crime Data

#### 3A. Polizeipräsidium München — Sicherheitsreport (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://www.polizei.bayern.de/kriminalitaet/statistik/006991/index.html |
| **2024 PDF** | https://www.polizei.bayern.de/mam/kriminalitaet/sicherheitsreport_2024.pdf |
| **Format** | PDF (tables need parsing/extraction) |
| **Granularity** | Per-Stadtbezirk (25 Bezirke in Munich) |
| **Coverage** | Crime categories, trends, district breakdown |
| **Cost** | Free download |

**Processing approach:** Extract district-level crime data from PDF tables, then map each school to its Stadtbezirk using coordinates. Similar to Hamburg's district-level approach but requires PDF parsing.

#### 3B. stadt.muenchen.de — Statistik Sicherheit

| Property | Value |
|----------|-------|
| **URL** | https://stadt.muenchen.de/infos/statistik-sicherheit.html |
| **Format** | PDF downloads by year |
| **Coverage** | Munich safety statistics with district breakdown |

Additional source for crime statistics at district level.

#### 3C. Bayern PKS (State-Level)

| Property | Value |
|----------|-------|
| **URL** | https://www.polizei.bayern.de/kriminalitaet/statistik/index.html |
| **Format** | PDF |
| **Coverage** | State-wide, city-wide aggregates |

State-level PKS can provide Munich city-wide crime rates as fallback if district parsing is too difficult.

**Crime approach recommendation:** Start with city-wide crime rate (easy), then attempt PDF parsing for per-Stadtbezirk data (moderate effort). This is similar to NRW's estimation approach but with better granularity (25 districts vs. city-wide).

### 4. Transit Data

#### 4A. MVV GTFS — Scheduled Transit Data (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://www.mvv-muenchen.de/fahrplanauskunft/fuer-entwickler/opendata/index.html |
| **Complete feed** | `gesamt_gtfs.zip` (~14 MB) |
| **Regional bus only** | `mvv_gtfs.zip` (~6 MB) |
| **Format** | GTFS (ZIP containing CSV files) |
| **Coordinate system** | WGS84 (EPSG:4326) |
| **License** | CC-BY (attribution: MVV GmbH + date) |
| **Coverage** | U-Bahn, S-Bahn, Tram, Stadt-/Metro-/Regionalbus |
| **Update frequency** | Every 4–8 weeks |
| **Cost** | Free, no API key |

**Also available from Open Data Portal:**
- https://opendata.muenchen.de/dataset/soll-fahrplandaten-mvv-gtfs

**Supplementary CSV files (updated January 2026):**
- Stop list with coordinates and IDs (742 KB)
- Line inventory (23 KB)
- Stop-to-tariff zone mapping (654 KB)

**Processing approach:** Use GTFS `stops.txt` for stop locations, calculate distance from each school to nearest stops by type (U-Bahn, S-Bahn, Tram, Bus). This is richer than the Overpass API approach used by NRW/Hamburg.

#### 4B. Overpass API (Fallback)

Always available as fallback for OSM transit stop data. No API key needed. Same approach as NRW and Hamburg pipelines.

### 5. POI Data

#### 5A. Google Places API (New) — Standard Approach

| Property | Value |
|----------|-------|
| **API** | Google Places API (New) |
| **Format** | JSON |
| **Access** | Requires `GOOGLE_PLACES_API_KEY` |
| **Coverage** | 500m radius search around each school |

Same approach as all other cities. POI categories: supermarkets, parks, playgrounds, pharmacies, libraries, sports facilities, etc.

No Munich-specific POI categories needed beyond the standard set.

### 6. Demographics & Social Index

#### 6A. Indikatorenatlas München (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://stadt.muenchen.de/infos/statistik-stadtteilinformationen.html |
| **Format** | CSV (machine-readable, Open Data optimized) |
| **Granularity** | Per-Stadtbezirk (25 districts) |
| **Indicators** | Population, labor market, vehicles, migration, social structure |
| **Years** | 2000–present |
| **Cost** | Free download |

**Available indicators:**
- Population by age, gender, nationality
- Migration background rates
- Unemployment rates
- Social welfare recipients (SGB II)
- Household income indicators
- Education levels

#### 6B. Open Data Portal München — Bevölkerung

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de (search "Bevölkerung" or "Stadtbezirk") |
| **Format** | CSV |
| **Coverage** | District-level demographics |

#### 6C. Bayern Sozialindex

Bavaria does **not** publish a per-school Sozialindex like NRW does. Demographics must be mapped at the Stadtbezirk level (25 districts), similar to Hamburg's approach.

**Processing approach:** Download Stadtbezirk-level indicators, geocode each school to its Stadtbezirk, assign district demographics to the school. This mirrors Hamburg's per-Stadtteil approach.

### 7. Academic Performance

| Property | Value |
|----------|-------|
| **Status** | **NOT PUBLICLY AVAILABLE** per school |
| **VERA/MSA results** | Password-protected, school-access only (las.bayern.de) |
| **Abitur results** | Not published per school in Bavaria |
| **Anmeldezahlen** | Not systematically published |
| **State averages** | Available from statistik.bayern.de but not per-school |

**Critical limitation:** Bavaria is one of the most restrictive German states for school performance data. Unlike Berlin (which publishes MSA results and demand data per school), Bavaria does not make any per-school academic data public. This is a known gap that cannot be filled.

**Fallback:** State-wide averages from `statistik.bayern.de/statistik/bildung_soziales/schulen/` can provide context but not school-level differentiation.

### 8. Website & Metadata

| Property | Value |
|----------|-------|
| **Source** | School websites linked from km.bayern.de Schulsuche |
| **Format** | HTML (scraping) |
| **Coverage** | Most schools have websites |
| **Access** | Public, no login required |
| **robots.txt** | Varies by school; most allow crawling |

The `Link` field in the Schulsuche CSV provides a relative URL to each school's detail page on km.bayern.de, which typically contains the school's own website URL. A two-step scrape: (1) get school detail page, (2) visit actual school website for content extraction.

## Comparison with Existing Cities

| Aspect | Berlin | Hamburg | NRW | Frankfurt | Munich |
|--------|--------|---------|-----|-----------|--------|
| School source | scrapers | WFS GeoJSON | Open Data CSV | Hessen XLSX | Schulsuche CSV |
| Coordinates | in data | in GeoJSON | UTM (convert) | geocoded | **needs geocoding** |
| Traffic type | sensor volumes | sensor volumes | accident counts | accident counts | accident counts |
| Crime granularity | Bezirk (12) | Stadtteil (107) | city-wide est. | Stadtteil | Stadtbezirk (25) |
| Crime format | structured | structured | structured | structured | **PDF (parse)** |
| Transit source | Overpass API | HVV GeoJSON | Overpass API | Overpass API | **MVV GTFS** |
| Demographics | per-school | per-Stadtteil | per-school | per-Stadtteil | per-Stadtbezirk |
| Academic data | MSA + demand | limited | Anmeldezahlen | limited | **none** |
| Encoding | UTF-8 | UTF-8 | UTF-8/cp850 | UTF-8 | ISO-8859-15 |

## Recommendations

### Template City: NRW (closest match)
Munich's pipeline should primarily adapt from the **NRW pipeline** because:
- CSV-based school data (semicolon separator, similar structure)
- Accident-based traffic enrichment (Unfallatlas, same national source)
- District-level crime estimation (rather than per-school)
- Similar demographic mapping approach (district-level, not per-school)

### Munich-Specific Adaptations Needed
1. **Geocoding step** — Munich school CSV has no coordinates; need to geocode addresses via Google Geocoding API or Nominatim before any spatial enrichment can run
2. **Crime PDF parsing** — The Sicherheitsreport PDF tables need extraction (PyPDF2/pdfplumber), or fall back to city-wide crime rate
3. **GTFS transit** — Can use MVV GTFS data instead of Overpass API for richer transit analysis (stop types, frequencies)
4. **ISO-8859-15 encoding** — School CSV uses ISO-8859-15, not UTF-8
5. **No academic data** — This column will be empty/N/A in the final output

### Potential Blockers
- **Geocoding dependency:** All spatial enrichments (traffic, transit, crime, POI) require coordinates first. If geocoding fails for some addresses, those schools will miss all spatial enrichments.
- **Crime PDF quality:** Tables in the Sicherheitsreport may be hard to parse programmatically. May need manual extraction or fallback to city-wide averages.

### Effort Estimate
**Difficulty: 3/5** (moderate)
- School data acquisition: straightforward (CSV download + geocoding)
- Traffic: easy (reuse NRW Unfallatlas approach)
- Transit: easy (GTFS is well-structured)
- Crime: moderate (PDF parsing or fallback)
- Demographics: moderate (district-level mapping)
- Academic: N/A (not available)
- Overall: comparable to Frankfurt pipeline, simpler than NRW (single city, not multi-city)
