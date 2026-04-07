# Leipzig Data Availability Research

**Date:** 2026-04-07
**State:** Sachsen (Saxony)
**Researcher:** Claude + user

## Summary

| Category | Source | Granularity | Format | Access | Status |
|----------|--------|-------------|--------|--------|--------|
| School Master | Sächsische Schuldatenbank API | per-school | CSV/JSON via REST API | free, no auth | Ready |
| Traffic | Unfallatlas (federal) | point-level accidents | CSV (zipped) | free download | Ready |
| Crime | Leipzig Open Data API | Ortsteil (63 districts) | CSV/JSON | free API | Ready |
| Transit | LVB GTFS + MDV GTFS | per-stop with coords | GTFS ZIP | free download | Ready |
| POI | Google Places | 500m radius | API | key needed | Ready |
| Demographics | Ortsteilkatalog + Zensus 2022 | Ortsteil / 100m grid | CSV | free | Ready |
| Academic | Not published per-school | N/A | N/A | N/A | Gap |
| Websites | Schuldatenbank has homepage URLs | per-school | HTML | scraping | Ready |

## Detailed Findings

### 1. School Master Data

**Primary Source: Sächsische Schuldatenbank API** (RECOMMENDED)

- **URL:** https://schuldatenbank.sachsen.de/
- **API endpoint:** `https://schuldatenbank.sachsen.de/api/v1/schools?format=csv&address=Leipzig&limit=500&pre_registered=yes&only_schools=yes`
- **API docs:** https://schuldatenbank.sachsen.de/api/v1/ (OpenAPI spec)
- **Format:** CSV (comma-separated), JSON, XML — selectable via `?format=csv`
- **Encoding:** UTF-8
- **Coordinates:** YES — `latitude` and `longitude` fields in WGS84 decimal degrees (EPSG:4326)
- **Authentication:** None required for basic school data
- **Maintained by:** SMK (Sächsisches Staatsministerium für Kultus) + TU Dresden + Statistisches Landesamt
- **Fields (62 columns):** `institution_key`, `name`, `institution_number`, `legal_status_key`, `school_category_key`, `street`, `postcode`, `community`, `longitude`, `latitude`, `phone_code_1`, `phone_number_1`, `mail`, `homepage`, `school_type_keys`, `headmaster_firstname`, `headmaster_lastname`, plus building/accessibility fields
- **School types covered:**
  - `school_category_key=10` — Allgemeinbildende Schulen (general education)
  - `school_category_key=20` — Berufsbildende Schulen (vocational)
  - `school_category_key=40` — Schulen des zweiten Bildungsweges
- **School type keys for general education:**
  - 11 = Grundschule (~31 schools)
  - 12 = Oberschule (~27 schools)
  - 13 = Gymnasium (~4+ schools)
  - 14 = Förderschule
- **Estimated Leipzig total:** ~120–150 schools (all types), ~60–80 general education
- **Pagination:** `limit` + `offset` parameters, default limit=20, set limit=500 for bulk

**Supplementary: Leipzig Open Data Portal**

Dataset A — School directory with addresses (2024/25):
- **URL:** https://opendata.leipzig.de/dataset/b01f056d-d416-4a31-b351-a7ca58d1f9d9
- **Download:** `allgemeinbildende_schulen_leipzig_sj-2024_25_standorte_mit_adresse.csv`
- **Fields:** Schulart, Schulträger, Name, Adresse, Stadtbezirk, Ortsteil, Schul_ID, Stala_ID
- **Coordinates:** NO — addresses only
- **Useful for:** Mapping Schul_ID / Stala_ID + Stadtbezirk/Ortsteil assignment

Dataset B — Student numbers per school (since 2021/22):
- **URL:** https://opendata.leipzig.de/dataset/1bb1c349-2259-450f-a5cb-d769a94a0e75
- **Download:** `schuelerzahlen_allgemeinbildende-schulen_leipzig_seitsj2021_22.csv`
- **Fields:** Schulart, Schulträger, Name, Schul_ID, Stala_ID, student counts per year
- **Useful for:** Enrichment (enrollment figures per school per year)

**License:** DL-BY-DE 2.0 (attribution required)

### 2. Traffic Data

**Primary Source: Federal Unfallatlas (Accident Atlas)** (RECOMMENDED)

- **Portal:** https://unfallatlas.statistikportal.de/
- **Download:** https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/
- **Format:** CSV (zipped) and Shapefile (zipped)
- **Coordinate system:** EPSG:25832 (UTM Zone 32N) — requires conversion to WGS84
- **Encoding:** UTF-8
- **Years covered:** 2016–2024 (each year as separate file)
- **Spatial resolution:** Point-level (exact crash coordinates)
- **Filtering:** `ULAND=14` for Saxony, `UGEMEINDE=14713000` for Leipzig
- **Key fields:** ULAND, UREGBEZ, UKREIS, UGEMEINDE, UKATEGORIE (severity 1-3), UART (type), UTYP1 (cause), IstRad, IstPKW, IstFuss, IstKrad, IstGkfz, LINREFX/LINREFY (coordinates)
- **License:** Datenlizenz Deutschland Namensnennung 2.0
- **Pipeline compatibility:** Same source used for NRW, Hamburg, Munich — existing enrichment code reusable with ULAND filter change

**Volume data gap:** Leipzig has 24 permanent Kfz counting stations, but the data is only available via a dashboard (currently offline) and PDF traffic volume maps. No machine-readable download available. The pipeline should rely on accident data, consistent with the NRW approach.

**Supplementary: Bicycle counting stations**
- **URL:** https://opendata.leipzig.de/dataset/dauerzahlstellen-radverkehr-stationen-stadt-leipzig
- **Format:** CSV, GeoJSON, GeoPackage, WFS
- **Coverage:** 19 stations, bicycle-only, rolling 2 years
- **Note:** Supplementary only, not primary traffic metric

### 3. Crime Data

**Primary Source: Leipzig Open Data API — Ortsteil-level crime data** (EXCELLENT)

- **CSV endpoint:** `https://statistik.leipzig.de/opendata/api/kdvalues?kategorie_nr=12&rubrik_nr=1&periode=y&format=csv`
- **JSON endpoint:** `https://statistik.leipzig.de/opendata/api/kdvalues?kategorie_nr=12&rubrik_nr=1&periode=y&format=json`
- **Portal:** https://opendata.leipzig.de/de/dataset/straftaten-jahreszahlen-kleinraumig
- **Format:** CSV (semicolon-separated) and JSON
- **Granularity:** **Ortsteil-level (63 Ortsteile) + Stadtbezirk-level (10 Stadtbezirke)** — BEST tier
- **Years:** 2004–2023 (20 years of annual data)
- **License:** CC BY 4.0 / DL-BY-DE 2.0
- **Source authority:** Landeskriminalamt Sachsen
- **Crime categories (Sachmerkmal):**
  - Straftaten insgesamt (total crimes)
  - Diebstahl (theft)
  - Körperverletzung (assault)
  - Vermögensdelikte (property crimes, from 2010)
  - Straftaten je Einwohner (per-capita rate)
- **CSV structure:** `Gebiet` (area name), `Sachmerkmal` (category), then year columns (2004–2023)

**Supplementary: Saxony State PKS**
- **URL:** https://www.polizei.sachsen.de/de/113130.htm
- **Stadt Leipzig specific PDFs** available (14 crime categories)
- **Granularity:** City-wide only — not useful for per-school differentiation
- **Format:** PDF only

**Leipzig administrative structure:** 10 Stadtbezirke containing 63 Ortsteile. Stable since 2000.

### 4. Transit Data

**Primary Source: LVB GTFS Feed** (EXCELLENT)

- **Portal:** https://opendata.leipzig.de/dataset/lvb-fahrplandaten
- **Download:** `https://opendata.leipzig.de/dataset/8803f612-2ce1-4643-82d1-213434889200/resource/b38955c4-431c-4e8b-a4ef-9964a3a2c95d/download/gtfsmdvlvb.zip`
- **Format:** Standard GTFS ZIP (stops.txt, routes.txt, trips.txt, stop_times.txt)
- **Coverage:** All LVB tram and bus lines in Leipzig (13 tram lines, 522+ tram stops, plus buses)
- **License:** Open data, free use
- **Stop types:** Tram (Straßenbahn), Bus
- **Line/route info:** YES — full GTFS with routes, trips, schedules

**Supplementary: MDV GTFS Feed** (regional, includes S-Bahn)

- **Source:** https://www.mdv.de/downloads/
- **File:** `mdv_mastscharf_gtfs.zip` (11.7 MB)
- **Format:** GTFS, updated weekly
- **License:** CC BY 4.0 DE
- **Coverage:** All MDV operators including S-Bahn Mitteldeutschland, regional rail

**Fallback: OpenStreetMap Overpass API**
- Leipzig bbox: ~51.24–51.45 lat, 12.20–12.55 lon
- Query bus_stop, tram_stop, station nodes
- Free, no API key, real-time data

**Recommendation:** Use LVB GTFS `stops.txt` as primary for transit stop locations. The GTFS approach is cleaner than Overpass and provides route/frequency data. Supplement with MDV GTFS for S-Bahn if needed.

### 5. POI Data

**Source:** Google Places API (New) — shared across all cities
- **Access:** Requires `GOOGLE_PLACES_API_KEY` in config.yaml
- **Method:** Nearby Search within 500m radius of each school
- **Categories:** restaurants, parks, libraries, sports facilities, supermarkets
- **No city-specific POI categories needed** for Leipzig

### 6. Demographics & Social Index

**Primary: Leipzig Ortsteilkatalog + Open Data API**

- **Portal:** https://statistik.leipzig.de/statdist/table.aspx
- **Open Data:** https://opendata.leipzig.de/group/soci
- **Granularity:** 63 Ortsteile + 10 Stadtbezirke
- **Published:** Biennially since 1993
- **Indicators available:**
  - Population, age structure, gender, nationality
  - Migration background
  - SGB II recipients (welfare/unemployment)
  - Housing stock, vacancy rates
  - Daycare, schools, physicians per district
- **Key datasets on opendata.leipzig.de:**
  - Einwohner (Jahreszahlen) — population by year, district-level CSV
  - Geodaten der Ortsteile — GIS boundaries (ETRS89) for spatial joins
  - Allgemeinbildende Schulen (Jahreszahlen) — school/student counts per district

**Secondary: Zensus 2022 Grid Data**

- **Source:** https://www.zensus2022.de/DE/Ergebnisse-des-Zensus/gitterzellen.html
- **Format:** CSV in ZIP, 100m resolution INSPIRE grid
- **Indicators:** Population count, average age, foreign resident share, vacancy rate, average net cold rent
- **Already in project:** `data_shared/zensus/` pipeline — same processing applies to Leipzig

**Sozialindex:** Saxony introduced a school Sozialindex in 2022, but it is **NOT published as open data**. Internal to the Landesamt für Schule und Bildung. Workaround: compute proxy from Ortsteilkatalog SGB-II rates + Zensus grid data + GISD quintile, consistent with approach used for cities with corrupted PLZ data.

### 7. Academic Performance

**Abitur results:** Saxony does **NOT** publish per-school Abitur averages. Only state-level aggregates (2024 avg: 2.18, pass rate: 96.4%). This is a data gap — cannot replicate Berlin/Hamburg Abitur spider chart dimension.

**Anmeldezahlen (enrollment demand):**
- The KreisElternRat Leipzig publishes Gymnasium/Oberschule 5th-grade registration numbers annually
- **Source:** https://ker-leipzig.de/docs/akgs/anmeldezahlen-stadt-leipzig-gymnasien-und-oberschulen-2023/
- **Format:** HTML blog posts with tables — requires scraping
- **Coverage:** Gymnasium and Oberschule only, 5th-grade transitions
- **Useful for:** Demand/oversubscription metric for secondary schools

**Student enrollment per school:**
- Available via opendata.leipzig.de Dataset B (see Section 1)
- Annual student counts per school since 2021/22

### 8. Websites & Metadata

- **School websites:** Available in Schuldatenbank API `homepage` field
- **Central portal:** https://schuldatenbank.sachsen.de/ allows browsing individual school pages
- **No known robots.txt restrictions** on school websites
- **Description pipeline:** `scripts_shared/generation/school_description_pipeline.py` will use Perplexity for web research on each school's homepage

## Comparison with Existing Cities

| Aspect | Berlin | Hamburg | NRW | Leipzig |
|--------|--------|---------|-----|---------|
| School source | scrapers (bildung.berlin.de) | WFS GeoJSON | Open Data CSV (static) | REST API (Schuldatenbank) |
| Coordinates | scraped | GeoJSON native | UTM EPSG:25832 | WGS84 in API |
| Traffic type | sensor volumes | sensor volumes | accident counts (Unfallatlas) | accident counts (Unfallatlas) |
| Crime granularity | Bezirk (12) | Stadtteil (107) | city-wide estimate | **Ortsteil (63)** |
| Crime format | scraped tables | CSV download | manual entry | **REST API CSV/JSON** |
| Transit source | Overpass API | HVV GeoJSON | Overpass API | **LVB GTFS download** |
| Demographics | per-school ndH | per-Stadtteil | per-school Sozialindex | per-Ortsteil + Zensus grid |
| Academic data | MSA + demand | Abitur per school | Anmeldezahlen | **Gap** (state avg only) |
| Enrollment data | limited | limited | Anmeldezahlen | per-school since 2021 |

## Recommendations

### Template City: NRW (with Hamburg crime pattern)

Leipzig's pipeline should be modeled primarily on the **NRW pipeline** because:
1. **Traffic:** Both use Unfallatlas accident data (same code, different ULAND filter)
2. **School data:** API-based retrieval (NRW is static CSV, but Leipzig API returns CSV too — similar downstream processing)
3. **No per-school Sozialindex:** Both need proxy computation from district-level data

For **crime enrichment**, adapt the **Hamburg pattern** because:
- Leipzig has excellent Ortsteil-level crime data via API (similar to Hamburg's Stadtteil granularity)
- Both use district→school spatial joins rather than city-wide estimation

For **transit enrichment**, Leipzig's GTFS data is a cleaner starting point than Overpass API — adapt the Hamburg HVV pattern or write a GTFS parser.

### City-Specific Phases

1. **Schuldatenbank API scraper** — new, API-based (unlike any existing city's static download)
2. **Leipzig Open Data student counts enrichment** — join enrollment data from Dataset B
3. **Ortsteil demographics enrichment** — join SGB-II/migration rates from Ortsteilkatalog API
4. **GTFS transit enrichment** — parse LVB GTFS stops.txt instead of Overpass API

### Data Gaps

- **No per-school Abitur averages** — spider chart will lack this dimension
- **No published Sozialindex** — use Ortsteil-level SGB-II + Zensus proxy
- **No machine-readable traffic volume data** — rely on accident counts only

### Estimated Effort Level: 3/5

Rationale: Leipzig has excellent open data infrastructure (REST APIs, GTFS, Ortsteil-level crime). The main complexity is the API-based school data retrieval (new pattern) and GTFS transit parsing. No major scraping challenges. Fewer data sources than NRW (no Sozialindex, no Abitur) actually reduces scope.
