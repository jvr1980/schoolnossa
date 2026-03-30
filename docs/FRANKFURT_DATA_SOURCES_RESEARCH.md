# Frankfurt am Main - Data Source Research

## Overview

Frankfurt am Main is a kreisfreie Stadt in Hessen, Germany. It has approximately 178 general education schools according to the Hessisches Statistisches Landesamt (2025 directory). This document catalogs all identified data sources for building a SchoolNossa pipeline for Frankfurt.

**Administrative codes:**
- AGS (full): 06412000
- Bundesland: 06 (Hessen)
- Regierungsbezirk: 4 (Darmstadt)
- Kreisfreie Stadt: 12 (Frankfurt am Main)
- Landkreis key in Hessen stats: 412

---

## 1. School Master Data

### 1A. Hessisches Statistisches Landesamt - Schulverzeichnis (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://statistik.hessen.de/publikationen/verzeichnisse |
| **Direct download** | https://statistik.hessen.de/sites/statistik.hessen.de/files/2025-09/verz-6_25_0.xlsx |
| **Format** | Excel (.xlsx), ~2 MB |
| **Cost** | Free (kostenloses Angebot) |
| **Coverage** | All 1,864 general education schools in Hessen |
| **Frankfurt schools** | ~178 schools (Landkreis code 412) |
| **API key** | Not needed |
| **Last updated** | September 2025 |

**Available fields (columns):**
- `Schulnummer` - Unique school number
- `Landkreis` - District code (412 = Frankfurt)
- `Gemeinde` - Municipality code
- `Rechtsform` - Legal form (1=public, 2=private)
- `Gesamtschule` - Comprehensive school type (1=KGS, 2=IGS)
- `Name der Schule` - School name
- `PLZ` - Postal code
- `Schulort` - City
- `Straße, Hausnummer` - Street address
- `Telefonvorwahl` / `Telefonnummer` - Phone
- `Fax` - Fax number
- `Email Adresse` - Email
- Student counts by school type:
  - `Vorklassen` - Pre-classes
  - `Grundschulen` - Primary (with Eingangsstufe/Grundschule breakdown)
  - `Förderstufe` - Orientation stage
  - `Hauptschule` - Lower secondary
  - `Mittelstufenschule` - Middle school
  - `Realschule` - Secondary (Realschule)
  - `Integrierte Gesamtschule` - Comprehensive
  - `Gymnasien` (Mittelstufe/Oberstufe) - Grammar school
  - `Förderschulen` - Special education (with 8 subcategories)
  - `Schulen für Erwachsene` - Adult education
- `Schülerinnen und Schüler insgesamt` - Total students (with/without pre-classes)
- `Nichtdeutscher Herkunftssprache` - Non-German native language count

**Format quirks:**
- Row 1 = column headers, Row 2 = sub-headers for multi-level columns
- Data starts at Row 3
- Sheet name: "Schulverzeichnis"
- Other sheets: Titelblatt, Impressum, Inhalt, Begriffliche Erläuterungen, Gemeindeschlüssel, Anleitung_Filter
- Filter Frankfurt by: column "Landkreis" == "412" OR column "Schulort" contains "Frankfurt am Main"

**Also available: Vocational schools (Verzeichnis 7):**
- URL: https://statistik.hessen.de/sites/statistik.hessen.de/files/2025-09/verz-7_25.xlsx
- Same format, covers berufliche Schulen

### 1B. Hessische Schul-Datenbank (Web interface)

| Property | Value |
|----------|-------|
| **URL** | https://schul-db.bildung.hessen.de/schul_db.html |
| **Format** | Web-based search only (no bulk export) |
| **Coverage** | All schools in Hessen |
| **API key** | N/A |

**Search filters available:**
- School name, municipality, postal code, school number
- School type (Gymnasium, Realschule, Grundschule, etc.)
- Full-day programs, languages offered
- Region/school authority district

**Assessment:** Useful for verification but not practical for bulk data extraction. No API or CSV export. Would require web scraping.

### 1C. JedeSchule.de / jedeschule.codefor.de

| Property | Value |
|----------|-------|
| **CSV download** | https://jedeschule.codefor.de/csv-data/jedeschule-data-2026-03-28.csv |
| **API docs** | https://jedeschule.codefor.de/docs |
| **Format** | CSV (~52 MB for all Germany) |
| **Update frequency** | Weekly (Saturdays) |
| **Cost** | Free (CC0 license) |

**CSV columns:**
- `id` - State-prefixed school ID
- `name` - School name
- `address`, `address2`, `zip`, `city` - Address
- `website`, `email`, `phone`, `fax` - Contact info
- `school_type` - Type category
- `legal_status` - Public/private
- `provider` - Operating authority
- `director` - Principal name
- `latitude`, `longitude` - Coordinates (WGS84)
- `raw` - JSON blob with source-specific raw fields
- `update_timestamp` - Last update

**CRITICAL NOTE on Hessen:** Historically, Hessen data was restricted (ministry did not grant permission). The jedeschule scraper for Hessen uses the `schul-db.bildung.hessen.de` database. The data appears to now be included, but coverage and completeness for Hessen should be verified. The `raw` field may contain additional Hessen-specific data from the scraper.

**Assessment:** Good secondary/validation source. The key advantage is pre-geocoded coordinates (lat/lon). However, the Hessen school directory Excel from statistik.hessen.de has much richer data (student counts by type, ndH data). Best strategy: use Hessen directory as primary, supplement coordinates from jedeschule if needed.

### 1D. SchulListe.eu

| Property | Value |
|----------|-------|
| **URL** | https://www.schulliste.eu/type/?bundesland=hessen&kreis=frankfurt-am-main |
| **Format** | Web only, no download |

**Assessment:** Not useful for pipeline. Web-only presentation, no API or download.

---

## 2. School Coordinates (Geocoding)

### 2A. JedeSchule.de coordinates (PREFERRED)

The jedeschule CSV includes `latitude` and `longitude` fields in WGS84. These are likely geocoded from addresses. Join on school number or name+city match.

### 2B. Geocoding from addresses

The Hessen school directory includes full street addresses. These can be geocoded using:
- **Nominatim/OpenStreetMap** (free, no key needed, rate-limited to 1 req/sec)
- **Google Geocoding API** (requires key, free tier of 40k/month)
- Frankfurt addresses are well-covered by OSM

### 2C. Hessen Geoportal WFS (Schulstandorte)

| Property | Value |
|----------|-------|
| **Base URL** | https://www.geoportal.hessen.de/spatial-objects/ |
| **Format** | OGC API Features / WFS, returns GeoJSON |
| **Coordinates** | WGS84 (EPSG:4326) |
| **API key** | Not needed |

**IMPORTANT LIMITATION:** The Schulstandorte WFS services are organized by individual Landkreis, NOT statewide. There is NO WFS service for Frankfurt am Main (Stadt). Available districts with school WFS:
- Wetteraukreis (ID 781), Hochtaunuskreis (ID 775), Main-Taunus-Kreis (ID 908), Bergstraße (ID 733), Main-Kinzig-Kreis (ID 777), Darmstadt-Dieburg (ID 737), Groß-Gerau (ID 739), Odenwaldkreis (ID 929), Vogelsbergkreis (ID 888)

**Fields per school (from WFS):** DSTNR, NAME, TYP, SCHULFORM, STRASSE, HAUSNR, PLZ, ORT, TELEFON, FAX, EMAIL, WEBSEITE, TRAEGER, KREIS, GEMEINDE, KLASSEN, SCHUELER, KAPAZITAET, plus boolean flags for school types (GRUNDSCHUL, GYMNASIUM, REALSCHULE, etc.)

**Assessment:** Not directly usable for Frankfurt, but useful reference. Geocoding from addresses is the recommended approach for Frankfurt.

---

## 3. Traffic / Accident Data

### 3A. Unfallatlas (Destatis) - CONFIRMED AVAILABLE

| Property | Value |
|----------|-------|
| **Portal** | https://opendata.hessen.de/dataset/unfallatlas |
| **CSV download (2024)** | https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2024_EPSG25832_CSV.zip |
| **Shapefile download (2024)** | https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2024_EPSG25832_Shape.zip |
| **Interactive map** | https://unfallatlas.statistikportal.de/?BL=HE |
| **Format** | CSV (zipped, ~7-13 MB) or Shapefile (zipped, ~16-32 MB) |
| **Coverage** | All of Germany, years 2016-2024 |
| **Coordinates** | EPSG:25832 (UTM zone 32N) |
| **Cost** | Free (Datenlizenz Deutschland Namensnennung 2.0) |
| **API key** | Not needed |

**Key fields:**
- `ULAND` - State code (06 = Hessen)
- `UREGBEZ` - Government district (4 = Darmstadt)
- `UKREIS` - District/city code (12 = Frankfurt am Main)
- `UGEMEINDE` - Municipality code
- `UKATEGORIE` - Severity (1=fatality, 2=serious injury, 3=minor injury)
- `UART` - Accident type (1-9)
- `UTYP1` - Accident category
- `IstRad`, `IstPKW`, `IstFuss`, `IstKrad`, `IstGkfz`, `IstSonstige` - Vehicle involvement (0/1)
- `LINREFX`, `LINREFY` - Coordinates in EPSG:25832
- `XGCSWGS84`, `YGCSWGS84` - WGS84 coordinates (check if present in 2024 version)
- `UJAHR`, `UMONAT`, `USTUNDE`, `UWOCHENTAG` - Temporal info

**Frankfurt filter:** `ULAND == '06' AND UKREIS == '12'`

**Format quirks:**
- Same format as used in NRW pipeline - coordinates in EPSG:25832, need pyproj conversion to WGS84
- German decimal separators may appear in some fields
- CSV delimiter: semicolon (`;`)

**Assessment:** Directly reusable. Same data source and format as the NRW pipeline. The `scripts_nrw/` traffic enrichment script can be adapted with minimal changes (just update the ULAND/UKREIS filter).

---

## 4. Transit / Public Transport Data

### 4A. OpenStreetMap / Overpass API (RECOMMENDED)

| Property | Value |
|----------|-------|
| **API** | https://overpass-api.de/api/interpreter |
| **Format** | JSON / XML |
| **Coverage** | Global, excellent for Frankfurt |
| **Cost** | Free |
| **API key** | Not needed |

**Overpass query for Frankfurt transit stops:**
```
[out:json][timeout:90];
area["name"="Frankfurt am Main"]["admin_level"="6"]->.searchArea;
(
  node["railway"="station"](area.searchArea);
  node["railway"="halt"](area.searchArea);
  node["railway"="tram_stop"](area.searchArea);
  node["highway"="bus_stop"](area.searchArea);
  node["public_transport"="stop_position"]["subway"="yes"](area.searchArea);
  node["station"="subway"](area.searchArea);
);
out body;
```

**Assessment:** Same approach as Hamburg and NRW pipelines. Frankfurt has U-Bahn, S-Bahn, Straßenbahn, and bus stops all well-mapped in OSM. This is the proven, no-key-required approach.

### 4B. RMV Open Data Portal

| Property | Value |
|----------|-------|
| **URL** | https://www.rmv.de/s/de/rmv-open-data |
| **Stop list download** | ZIP file (~2 MB), 2025-2026 tariff period |
| **API** | https://www.rmv.de/hapi/ (requires registration) |
| **Cost** | Free but requires registration for API |

**Assessment:** The stop list download is useful as a supplementary/validation source but requires registration for API access. The Overpass API approach is simpler and consistent with other city pipelines.

### 4C. traffiQ Frankfurt Open Data

| Property | Value |
|----------|-------|
| **URL** | https://www.traffiq.de/traffiq/service/open-data.html |
| **Format** | Excel (.xlsx) |
| **Content** | Passenger load data (Zähldaten Linienbelastung 2010-2025) |

**Assessment:** Contains line-level ridership data, not stop locations. Not directly useful for the transit enrichment (we need stop proximity, not ridership). Could be interesting for future features.

### 4D. GTFS.de (Germany-wide GTFS)

| Property | Value |
|----------|-------|
| **URL** | https://gtfs.de/en/ |
| **Format** | GTFS (zip with stops.txt, routes.txt, trips.txt, etc.) |
| **Coverage** | All Germany: 20,000+ lines, 500,000+ stops |
| **Cost** | Free |
| **Source** | Generated from DELFI NeTEx dataset |

**Assessment:** Contains comprehensive stop data with coordinates. Would need filtering for Frankfurt area. More complex than Overpass API for our use case but useful if we need route/frequency data in the future.

---

## 5. Crime Data

### 5A. BKA PKS Stadt-Falltabellen (City-level)

| Property | Value |
|----------|-------|
| **URL** | https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/PKS2024/PKSTabellen/StadtFalltabellen/stadtfalltabellen.html |
| **Format** | Excel (.xlsx, ~385 KB) |
| **Coverage** | All German cities with 100,000+ inhabitants |
| **Content** | Cases, attempts, offense breakdown, suspects, frequency rates |
| **Cost** | Free |
| **API key** | Not needed |

**Assessment:** Provides city-level aggregate crime statistics for Frankfurt. Useful for city-wide comparison but NOT for neighborhood-level crime enrichment around individual schools.

### 5B. Polizeipräsidium Frankfurt PKS (City-specific)

| Property | Value |
|----------|-------|
| **URL** | https://www.polizei.hessen.de/die-polizei/statistik/polizeiliche-kriminalstatistik-pks/polizeipraesidium-frankfurt |
| **Format** | PDF only |
| **Available files (2025):** |
| | Grundtabelle Straftaten (434 KB PDF) |
| | Prozenttabelle Fälle (451 KB PDF) |
| | Aufgliederung TV gesamt (619 KB PDF) |
| | Aufgliederung Opfer (333 KB PDF) |
| | Präsentation (10.8 MB PDF) |

**Assessment:** City-aggregate level only, PDF format. No Stadtteil-level breakdown available in publicly downloadable data. Would require PDF parsing for limited value.

### 5C. Approach: Use city-level aggregate (RECOMMENDED)

Unlike Berlin (which has Kriminalitätsatlas with neighborhood-level data) or Hamburg (which has Stadtteilatlas PKS), Frankfurt does NOT appear to publish granular, spatially detailed crime data as open data.

**Recommended strategy:**
1. Use BKA PKS city-level data for Frankfurt's overall crime rate
2. Assign uniform city-level crime metrics to all Frankfurt schools
3. Alternatively: scrape the PKS Grundtabelle PDF for category breakdowns
4. Document this as a known limitation vs. Berlin/Hamburg

---

## 6. Points of Interest (POI)

### 6A. OpenStreetMap / Overpass API (STANDARD APPROACH)

| Property | Value |
|----------|-------|
| **API** | https://overpass-api.de/api/interpreter |
| **Format** | JSON |
| **Cost** | Free |
| **API key** | Not needed |

Same approach as all other city pipelines. Query for relevant POI categories around each school:
- Parks and green spaces (`leisure=park`)
- Sports facilities (`leisure=sports_centre`, `leisure=pitch`)
- Libraries (`amenity=library`)
- Cultural venues (`amenity=theatre`, `amenity=cinema`, `tourism=museum`)
- Fast food (`amenity=fast_food`)
- Playgrounds (`leisure=playground`)

**Assessment:** Proven approach, directly reusable from existing pipelines.

---

## 7. Demographics / Sozialindex

### 7A. Frankfurt Strukturdatenatlas (Stadtteil profiles)

| Property | Value |
|----------|-------|
| **Interactive atlas** | https://statistik.stadt-frankfurt.de/strukturdatenatlas/stadtteilprofile/html/atlas.html |
| **Stadtteile atlas** | https://statistik.stadt-frankfurt.de/strukturdatenatlas/stadtteile/html/atlas.html |
| **Format** | Interactive InstantAtlas (web-based visualization) |

**Assessment:** Rich district-level demographic data visualized interactively, but no obvious CSV/API download from the atlas itself. Data would need to be extracted from the open data portal (see 7B).

### 7B. Offenedaten Frankfurt - Bevölkerung/Strukturdaten

| Property | Value |
|----------|-------|
| **Portal** | https://offenedaten.frankfurt.de/ |
| **Bevölkerung Stadtteile** | https://www.offenedaten.frankfurt.de/dataset/stadtteilprofile-bevoelkerung |
| **Bevölkerungsstruktur** | https://www.offenedaten.frankfurt.de/dataset/bevoelkerung |
| **Bürgeramt CSV datasets** | https://offenedaten.frankfurt.de/organization/buergeramt-statistik-und-wahlen?res_format=CSV |
| **Format** | CSV |
| **Cost** | Free |

**Known available datasets:**
- Population by Stadtteil (age, families, foreigners, migration background)
- Household composition by Stadtteil
- Employment/business data by Stadtteil

**NOTE:** The portal has SSL certificate issues (www. vs non-www). Data may be somewhat dated (some datasets from 2012). Check for more recent updates.

**Assessment:** Useful for district-level demographic context. Can be joined to schools via Stadtteil assignment. Need to verify what recent data is available and in what format.

### 7C. Hessen Sozialindex (School-level)

| Property | Value |
|----------|-------|
| **Reference** | https://kultus.hessen.de/presse/zehn-jahre-sozialindex-in-hessen-eine-erfolgsgeschichte |
| **Status** | EXISTS but NOT publicly available as downloadable data |

Hessen has had a Sozialindex for schools for 10+ years, based on:
- Proportion of students with non-German family language
- Number of books in household (from learning assessments in grades 3 and 8)

This data is used internally for teacher allocation and the Startchancen-Programm but is NOT published as open data per individual school.

**Assessment:** Not available for direct use. The Hessen school directory DOES include `Nichtdeutscher Herkunftssprache` (non-German native language) counts per school, which serves as a partial proxy for social disadvantage.

### 7D. Frankfurt Statistisches Jahrbuch

| Property | Value |
|----------|-------|
| **URL** | https://statistikportal.frankfurt.de/ |
| **Format** | PDF (large annual publication) |

**Assessment:** Comprehensive but PDF-only. Contains Stadtteil-level data on population, education, employment, housing, etc. Would require extensive PDF parsing. Low priority given other sources.

---

## 8. Academic Performance Data

### 8A. Hessen Abitur Results (State-level only)

| Property | Value |
|----------|-------|
| **Source** | https://kultus.hessen.de/ (press releases) |
| **Format** | Press releases, no machine-readable data |
| **Granularity** | State-wide averages only |

Hessen publishes only state-level Abitur statistics (e.g., average grade 2.26-2.27, pass rate ~94.5%). Individual school results are NOT published.

**Assessment:** No per-school Abitur data available. This is a known limitation compared to Hamburg (where gymnasium-hamburg.net publishes school-level Abitur averages).

### 8B. Hessische Lehrkräfteakademie - Learning Assessments (VERA/ZLSE)

| Property | Value |
|----------|-------|
| **URL** | https://lehrkraefteakademie.hessen.de/zentrale-lernstandserhebungen |
| **Format** | Not publicly downloadable per school |

Hessen administers centralized learning assessments (ZLSE/VERA) in grades 3 and 8, developed with the IQB. Results are given to individual schools but NOT published publicly.

**Assessment:** Not available for pipeline use.

---

## Data Source Summary Matrix

| Data Type | Source | Format | Granularity | Availability | Priority |
|-----------|--------|--------|-------------|-------------|----------|
| **School master data** | Hessen Statistik Verz.6 | Excel | Per school | FREE, confirmed | PRIMARY |
| **School names/contacts** | jedeschule.codefor.de | CSV | Per school | FREE, weekly | SECONDARY |
| **School coordinates** | jedeschule.codefor.de + geocoding | CSV | Per school | FREE | PRIMARY |
| **Student counts** | Hessen Statistik Verz.6 | Excel | Per school, by type | FREE, confirmed | PRIMARY |
| **ndH / migration** | Hessen Statistik Verz.6 | Excel | Per school (count) | FREE, confirmed | PRIMARY |
| **Traffic/accidents** | Unfallatlas (Destatis) | CSV | Per accident (geocoded) | FREE, confirmed | PRIMARY |
| **Transit stops** | Overpass API (OSM) | JSON | Per stop (geocoded) | FREE, no key | PRIMARY |
| **Crime data** | BKA PKS Stadt-Tabellen | Excel | City-level only | FREE | LIMITED |
| **Crime (Frankfurt)** | PP Frankfurt PKS | PDF | City-level | FREE (PDF only) | LOW |
| **POI** | Overpass API (OSM) | JSON | Per POI (geocoded) | FREE, no key | PRIMARY |
| **Demographics** | offenedaten.frankfurt.de | CSV | Per Stadtteil | FREE, check dates | MEDIUM |
| **Sozialindex** | Not published | N/A | N/A | NOT AVAILABLE | N/A |
| **Abitur results** | Not published per school | N/A | N/A | NOT AVAILABLE | N/A |

---

## Recommended Pipeline Architecture

### Phase 1: Master Data
1. Download Hessen Verzeichnis 6 Excel
2. Filter for Landkreis 412 (Frankfurt am Main)
3. Parse school types from column structure (Grundschule, Gymnasium, Realschule, IGS, etc.)
4. Split into primary (Grundschulen) and secondary (Gymnasien, Realschulen, Gesamtschulen, Hauptschulen, Mittelstufenschulen)

### Phase 2: Geocoding
1. Download jedeschule.codefor.de CSV, filter for Frankfurt schools
2. Match on Schulnummer or name+city
3. For unmatched schools: geocode from addresses via Nominatim
4. Cache all coordinates

### Phase 3: Enrichments (reuse existing patterns)
1. **Traffic** - Adapt NRW Unfallatlas script (change filter to ULAND=06, UKREIS=12)
2. **Transit** - Reuse Overpass API script (change bounding box to Frankfurt)
3. **Crime** - City-level only from BKA PKS (uniform per school, document limitation)
4. **POI** - Reuse Overpass API script (change bounding box to Frankfurt)
5. **Demographics** - Download Frankfurt Stadtteil CSV, assign to schools by postal code or Stadtteil

### Phase 4: Schema Transform
- Map Frankfurt-specific columns to Berlin schema
- Frankfurt school types -> Berlin-compatible categories
- Handle missing columns (no Sozialindex, no per-school Abitur)

---

## Key Differences from Other Cities

| Feature | Berlin | Hamburg | NRW | Frankfurt |
|---------|--------|--------|-----|-----------|
| School master data | Web scraping | Open Data CSV | Open Data CSV | Excel download |
| Coordinates | Geocoding | GeoJSON | UTM conversion | jedeschule + geocoding |
| Student counts | Web scraping | Open Data | Open Data | Excel (by type) |
| ndH / migration | Web scraping | Open Data | Sozialindex | Excel (count only) |
| Crime (granular) | Kriminalitätsatlas | Stadtteilatlas PKS | BKA Kreis-level | City-level only |
| Abitur per school | MSA+Abitur data | gymnasium-hamburg.net | Not available | Not available |
| Sozialindex | By school | KESS factor | Schulsozialindex | Not published |
| Transit | BVG GTFS | HVV GeoJSON | Overpass API | Overpass API |
| Traffic accidents | berlin-zaehlt.de | Unfallatlas | Unfallatlas | Unfallatlas |

---

## Data Gaps and Limitations

1. **No neighborhood-level crime data** - Frankfurt's PKS is only published at city aggregate level (PDF). Unlike Berlin's Kriminalitätsatlas or Hamburg's Stadtteilatlas, there is no machine-readable, spatially granular crime dataset. Workaround: use city-level average or explore whether the Frankfurter Statistisches Jahrbuch contains Stadtteil crime data.

2. **No per-school Abitur/performance data** - Hessen does not publish school-level exam results. The Sozialindex exists internally but is not open data. Workaround: use ndH count from school directory as a proxy indicator.

3. **No school-specific Sozialindex** - Unlike NRW (Schulsozialindex 1-9) or Hamburg (KESS factor), Hessen's Sozialindex is not publicly available. The ndH column in the school directory is the closest available proxy.

4. **Demographics data may be dated** - The offenedaten.frankfurt.de Stadtteil profiles may use 2012 data. Need to verify if more recent releases exist.

5. **No WFS for Frankfurt school locations** - The Hessen Geoportal WFS for Schulstandorte exists for surrounding Kreise but not for Frankfurt Stadt itself. Must rely on geocoding.

---

## URLs Quick Reference

```
# School master data (PRIMARY)
https://statistik.hessen.de/sites/statistik.hessen.de/files/2025-09/verz-6_25_0.xlsx

# School master data (SECONDARY - with coordinates)
https://jedeschule.codefor.de/csv-data/jedeschule-data-2026-03-28.csv

# Unfallatlas 2024 CSV
https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2024_EPSG25832_CSV.zip

# Overpass API (transit & POI)
https://overpass-api.de/api/interpreter

# BKA PKS Stadt tables
https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/PKS2024/PKSTabellen/StadtFalltabellen/stadtfalltabellen.html

# Frankfurt open data portal
https://offenedaten.frankfurt.de/

# Hessen school database (web search)
https://schul-db.bildung.hessen.de/schul_db.html

# Frankfurt PKS (PDF)
https://www.polizei.hessen.de/die-polizei/statistik/polizeiliche-kriminalstatistik-pks/polizeipraesidium-frankfurt
```
