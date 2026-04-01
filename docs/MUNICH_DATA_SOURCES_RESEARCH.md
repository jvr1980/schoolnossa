# Munich (Bayern/Bavaria) - Data Source Research

## Overview

Munich (Muenchen) is the capital of Bavaria (Bayern), Germany's largest federal state by area. It has approximately 400+ general education schools. This document catalogs all identified data sources for building a SchoolNossa pipeline for Munich.

**Administrative codes:**
- AGS (full): 09162000
- Bundesland: 09 (Bayern / Bavaria)
- Regierungsbezirk: 1 (Oberbayern)
- Kreisfreie Stadt: 62 (Muenchen)
- ULAND (for Unfallatlas): 09

---

## 1. School Master Data

### 1A. Bayerisches Kultusministerium - Schulsuche CSV Export (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://www.km.bayern.de/ministerium/schule-und-ausbildung/schulsuche.html |
| **Format** | CSV (semicolon-delimited) |
| **Encoding** | ISO-8859-15 |
| **Cost** | Free |
| **Coverage** | All ~6,100 schools in Bavaria |
| **Munich filter** | Filter by PLZ (80xxx-81xxx) or Ort contains "Muenchen" |
| **API key** | Not needed |

**How to access:**
The Schulsuche provides a web form with search filters. After running a search, results can be exported as CSV via the "als CSV exportieren" button. To get all Munich schools, search with Ort="Muenchen" or by Munich postal codes.

**Available fields (7 columns):**
- `Schulnummer` - Unique school ID
- `Schultyp` - School type (Grundschule, Gymnasium, Realschule, Mittelschule, etc.)
- `Name` - School name
- `Strasse` - Street address
- `PLZ` - Postal code
- `Ort` - City
- `Link` - Relative URL to school detail page (e.g., `/schule/7180.html`)

**Format quirks:**
- Semicolon delimiter (`;`)
- ISO-8859-15 encoding (NOT UTF-8)
- Values are quoted with double quotes
- School detail pages at `km.bayern.de/schule/{Schulnummer}.html` contain additional info (phone, email, website, etc.)

**Missing vs. NRW/Frankfurt:**
- No coordinate data (must be geocoded from address)
- No student counts in the CSV export
- No email/phone/website in the CSV (only on individual school detail pages)
- No Sozialindex equivalent in Bavaria

### 1B. JedeSchule.de / Code for Germany (SUPPLEMENTARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://jedeschule.de/daten/ |
| **Direct CSV** | https://jedeschule.codefor.de/csv-data/jedeschule-data-2023-07-22.csv |
| **Format** | CSV |
| **License** | CC0 (public domain) |
| **Coverage** | All schools in Germany (~33,700+) |
| **Bayern filter** | Filter by Bundesland = "Bayern" |

**Available fields:**
- id, name, address, zip, city
- school_type, phone, fax, email, website
- Bundesland, provider/operator (Traeger)

**Notes:**
- Data sourced from Bayerischer Lehrer- und Lehrerinnenverband e.V.
- Last update: July 2023 (may be somewhat stale)
- Could supplement km.bayern.de CSV with email/phone/website fields
- Join on Schulnummer or name+address matching

### 1C. SchulListe.eu (SUPPLEMENTARY)

| Property | Value |
|----------|-------|
| **URL** | https://www.schulliste.eu/type/?bundesland=bayern |
| **Format** | Web-only (no bulk download) |
| **Coverage** | Bavaria schools with type filtering |

**Notes:**
- Web scraping possible but no official export
- Contains school type categorization useful for cross-referencing

### 1D. Bayerisches Landesamt fuer Statistik - Amtliche Schuldaten

| Property | Value |
|----------|-------|
| **URL** | https://www.statistik.bayern.de/service/erhebungen/bildung_soziales/schuldaten/index.html |
| **Format** | PDF reports, statistical tables |
| **Coverage** | All schools in Bavaria (aggregate statistics) |

**Notes:**
- Contains Merkmalskataloge (attribute catalogs) and Datensatzbeschreibungen (dataset descriptions) for the 2025/2026 school year
- This is the official statistical collection system (ASV - Anwendung fuer Schuldatenverwaltung)
- Data is aggregated, not per-school downloadable
- Publication "Bayerns Schulen in Zahlen 2024/2025" at https://www.km.bayern.de/ministerium/statistik-und-forschung/bayerns-schulen-in-zahlen contains tabular overviews

**RECOMMENDED APPROACH FOR SCHOOL DATA:**
1. Export all Munich schools from km.bayern.de Schulsuche as CSV (primary list)
2. Supplement with JedeSchule.de data for email/phone/website
3. Geocode addresses using Google Geocoding API (no coordinates in source data)
4. Scrape individual school detail pages at km.bayern.de/schule/{id}.html for additional metadata if needed

---

## 2. Traffic / Accident Data

### 2A. Unfallatlas - Statistisches Bundesamt (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://unfallatlas.statistikportal.de/ |
| **Data portal** | https://data.mfdz.de/destatis_Unfalldaten/index.html |
| **Format** | CSV (yearly files) and Parquet |
| **Coverage** | All accidents with personal injury, Germany-wide |
| **Years** | 2016-2023 |
| **Coordinate system** | WGS84 (lat/lon) |
| **Cost** | Free / Open Data |
| **Bayern filter** | `ULAND = "09"` |

**Key columns (same as NRW pipeline):**
- `ULAND` - Bundesland code (09 = Bayern)
- `UREGBEZ` - Regierungsbezirk
- `UKREIS` - Kreis/kreisfreie Stadt
- `UGEMEINDE` - Gemeinde
- `UKATEGORIE` - Accident severity (1=fatal, 2=serious, 3=minor)
- `UART` - Accident type
- `UTYP1` - Accident cause type
- `USTRZUSTAND` - Road condition
- `ULICHTVERH` - Light conditions
- `LINREFX` / `LINREFY` - Coordinates (longitude/latitude)
- `UJAHR` - Year
- `UMONAT` - Month
- `USTUNDE` - Hour
- `UWOCHENTAG` - Day of week

**Notes:**
- German comma decimal separator in coordinates (same issue as NRW)
- Filter for Munich: `ULAND="09"` AND `UKREIS="62"` (kreisfreie Stadt Muenchen)
- Proven approach: reuse NRW pipeline's Unfallatlas processing code
- Accidents are point data with exact coordinates - can calculate nearest-school distances

### 2B. opendata.muenchen.de - Monatszahlen Verkehrsunfaelle

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de/dataset/monatszahlen-verkehrsunfaelle |
| **Format** | CSV (also TSV, JSON, XML) |
| **Size** | ~170 KB |
| **Cost** | Free / Open Data |
| **Coverage** | Munich city-wide, aggregated monthly |

**Columns:**
- `MONATSZAHL` - Category identifier
- `AUSPRAEGUNG` - Sub-category
- `JAHR` - Year
- `MONAT` - Month
- `WERT` - Value
- `VORJAHRESWERT` - Previous year value
- `VERAEND_VORMONAT_PROZENT` - Month-over-month change %
- `VERAEND_VORJAHRESMONAT_PROZENT` - Year-over-year change %
- `ZWOELF_MONATE_MITTELWERT` - 12-month rolling average

**Notes:**
- Aggregated data only (no per-location coordinates)
- Useful for city-level context but NOT for per-school proximity analysis
- Categories: traffic accidents, hit-and-run accidents, alcohol-related accidents

### 2C. Verkehrsdaten der Stadt Muenchen

| Property | Value |
|----------|-------|
| **URL** | https://stadt.muenchen.de/infos/verkehrsdaten.html |
| **Format** | Web / PDF reports |
| **Coverage** | Traffic volume counts at specific counting points |

**Notes:**
- DTVw (durchschnittlicher Tagesverkehr an Werktagen) - average workday traffic
- Surveys on Tue/Wed/Thu outside vacations
- May contain useful traffic volume data but availability as structured download unclear

**RECOMMENDED APPROACH FOR TRAFFIC:**
Use Unfallatlas (2A) as the primary source - same proven approach as NRW pipeline. Filter ULAND=09, UKREIS=62. Reuse existing accident processing code with Munich-specific parameters.

---

## 3. Crime Data (PKS)

### 3A. Polizeipraesidium Muenchen - Sicherheitsreport (PRIMARY for Munich)

| Property | Value |
|----------|-------|
| **URL** | https://www.polizei.bayern.de/kriminalitaet/statistik/006991/index.html |
| **Format** | PDF reports |
| **Coverage** | Munich police district |
| **Years available** | Since 1996 |
| **Cost** | Free |
| **Geographic granularity** | City-level aggregate (not per-district) |

**Notes:**
- Munich is consistently ranked Germany's safest major city
- PDF-only format - no structured data download
- Would need PDF parsing or manual extraction for pipeline use
- Limited geographic granularity (no per-Stadtbezirk breakdown in accessible format)

### 3B. Bayerische Polizei - Polizeiliche Kriminalstatistik (STATE-LEVEL)

| Property | Value |
|----------|-------|
| **URL** | https://www.polizei.bayern.de/kriminalitaet/statistik/index.html |
| **Format** | PDF (press reports) |
| **Direct PDFs** | |
| - 2024 | https://www.polizei.bayern.de/mam/kriminalitaet/250321_pks_pressebericht_2024.pdf |
| - 2023 | https://www.polizei.bayern.de/mam/kriminalitaet/240318_pks_pressebericht_2023.pdf |
| - 2022 | https://www.polizei.bayern.de/mam/kriminalitaet/230315_pks_pressebericht_2022.pdf |
| **Coverage** | All of Bavaria |
| **Cost** | Free |

**Notes:**
- State-level aggregate statistics
- Individual police presidia (Polizeipraesidien) publish their own regional reports
- PDF format only - no CSV/Excel download available

### 3C. BKA - Polizeiliche Kriminalstatistik (FEDERAL, FALLBACK)

| Property | Value |
|----------|-------|
| **URL** | https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/pks_node.html |
| **Format** | Excel/CSV at federal level, interactive tables |
| **Coverage** | Per-Bundesland and per-Landkreis breakdowns available |
| **Cost** | Free |

**Notes:**
- Can extract Munich-specific data (Landkreis 09162)
- More structured than Bavarian state PDFs
- Same approach as used in existing SchoolNossa pipelines

### 3D. Stadt Muenchen - Statistik Sicherheit

| Property | Value |
|----------|-------|
| **URL** | https://stadt.muenchen.de/infos/statistik-sicherheit.html |
| **Format** | Web / PDF |
| **Coverage** | Munich city, some historical per-district data |

**Notes:**
- Statistical overview of safety in Munich
- May contain per-Stadtbezirk breakdowns in archived publications
- Not available as structured open data on opendata.muenchen.de

**RECOMMENDED APPROACH FOR CRIME:**
1. Use BKA PKS data for structured per-Landkreis crime rates (same as other pipelines)
2. Supplement with Munich-specific Sicherheitsreport data if per-district granularity can be extracted from PDFs
3. Consider using a city-wide crime rate as a uniform score for all Munich schools (Munich is very safe overall)

---

## 4. Transit Data

### 4A. MVV - GTFS Gesamtfeed (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://www.mvv-muenchen.de/fahrplanauskunft/fuer-entwickler/opendata/index.html |
| **Direct download** | https://www.mvv-muenchen.de/fileadmin/mediapool/02-Fahrplanauskunft/03-Downloads/openData/gesamt_gtfs.zip |
| **Format** | GTFS (ZIP containing CSV files) |
| **Size** | ~14 MB |
| **License** | CC-BY (attribution: Muenchner Verkehrs- und Tarifverbund GmbH (MVV)) |
| **Coverage** | Entire MVV area (S-Bahn, U-Bahn, Tram, Bus - regional and city) |
| **Coordinate system** | WGS84 (standard GTFS) |
| **Update frequency** | Every 4-8 weeks |
| **Cost** | Free |

**GTFS standard files included:**
- `stops.txt` - All stops with coordinates (stop_lat, stop_lon)
- `routes.txt` - All routes/lines
- `trips.txt` - All trips
- `stop_times.txt` - All stop times
- `calendar.txt` / `calendar_dates.txt` - Service patterns
- `agency.txt` - Transit agencies

### 4B. MVV - Haltestellenliste CSV

| Property | Value |
|----------|-------|
| **URL** | https://www.mvv-muenchen.de/fileadmin/mediapool/02-Fahrplanauskunft/03-Downloads/openData/MVV_Haltestellen_Report_s26.csv |
| **Format** | CSV |
| **Size** | ~742 KB |
| **License** | CC-BY |
| **Coverage** | All MVV stops with main attributes and coordinates |
| **Update frequency** | Annually |

**Notes:**
- Simpler than full GTFS - just stop locations with IDs and coordinates
- Sufficient for nearest-stop-to-school distance calculations
- Updated January 2026

### 4C. MVV - Linienliste CSV

| Property | Value |
|----------|-------|
| **URL** | https://www.mvv-muenchen.de/fileadmin/mediapool/02-Fahrplanauskunft/03-Downloads/openData/MVV_Linien_s26.csv |
| **Format** | CSV |
| **Size** | ~23 KB |
| **License** | CC-BY |

### 4D. MVV - Tarifzonen-Zuordnung

| Property | Value |
|----------|-------|
| **URL** | https://www.mvv-muenchen.de/fileadmin/mediapool/02-Fahrplanauskunft/03-Downloads/openData/Haltestellen_Tarifzonen_s26.csv |
| **Format** | CSV |
| **Size** | ~654 KB |

### 4E. opendata.muenchen.de - GTFS Mirror

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de/dataset/soll-fahrplandaten-mvv-gtfs |
| **Format** | GTFS (ZIP) |
| **Notes** | Same MVV GTFS data, mirrored on Munich's Open Data Portal |

### 4F. OpenStreetMap / Overpass API (FALLBACK)

| Property | Value |
|----------|-------|
| **URL** | https://overpass-api.de/api/interpreter |
| **Format** | JSON/XML |
| **Coverage** | All OSM-mapped transit stops |
| **Cost** | Free, no key needed |

**Overpass query for Munich transit stops:**
```
[out:json][timeout:60];
area["name"="Muenchen"]["admin_level"="6"]->.searchArea;
(
  node["highway"="bus_stop"](area.searchArea);
  node["railway"="station"](area.searchArea);
  node["railway"="halt"](area.searchArea);
  node["railway"="tram_stop"](area.searchArea);
  node["public_transport"="stop_position"](area.searchArea);
  node["public_transport"="platform"](area.searchArea);
);
out body;
```

**RECOMMENDED APPROACH FOR TRANSIT:**
1. Use MVV Haltestellenliste CSV (4B) for simple stop-to-school distances - easiest option
2. Alternatively, use MVV GTFS stops.txt (4A) for the same with more metadata
3. Overpass API (4F) as fallback/supplement, consistent with Hamburg/NRW approach
4. Calculate nearest stops by type (U-Bahn, S-Bahn, Tram, Bus) for each school

---

## 5. Demographics / Social Structure

### 5A. Indikatorenatlas Muenchen (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de/pages/indikatorenatlas |
| **Format** | CSV (machine-optimized for Open Data) |
| **Coverage** | 25 Stadtbezirke (city districts) |
| **Cost** | Free / Open Data |
| **License** | Datenlizenz Deutschland Namensnennung 2.0 |

**Available indicator areas (~60 indicators from 7 themes):**
- Bevoelkerung (Population)
- Arbeitsmarkt (Labor market)
- KFZ-Bestand (Vehicle fleet)
- Gesundheit (Health)
- Soziales (Social welfare)
- Bildung (Education)
- Wohnen (Housing)

**CSV structure (per indicator file):**
- Indicator type
- Characteristics
- Year (data from 2000 onwards)
- Raumbezug (spatial reference = Stadtbezirk)
- Indicator value
- Base values

**Notes:**
- Interactive web application with data export
- Data at Stadtbezirk level (25 districts) - can assign to schools based on geographic location
- Excellent source for socioeconomic context per school neighborhood
- Multiple CSV files available, one per indicator topic

### 5B. Bevoelkerung in den Stadtbezirken

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de/dataset/bevoelkerung-stadtbezirken |
| **Format** | CSV |
| **Coverage** | 25 Stadtbezirke |
| **License** | Datenlizenz Deutschland Namensnennung 2.0 |
| **Last updated** | Data as of 31.12.2024 |

**Columns:**
- `stadtbezirk` - District name/number
- `einwohner` - Population (at registered residence)
- `flaeche` - Area in km2

**Also available via API:** JSON, RDF/XML, Turtle, N3, JSON-LD

### 5C. Stadtbezirke Geodata

| Property | Value |
|----------|-------|
| **URL** | https://opendata.muenchen.de/dataset/vablock_stadtbezirke_opendata |
| **Format** | GeoJSON / Shapefile |
| **Coverage** | Administrative boundaries of 25 Stadtbezirke |

**Notes:**
- Needed to assign schools to Stadtbezirke via point-in-polygon spatial join
- Combine with Indikatorenatlas indicators for per-school demographic scores

### 5D. Statistisches Taschenbuch Muenchen

| Property | Value |
|----------|-------|
| **URL** | https://stadt.muenchen.de/infos/statistik-stadtteilinformationen.html |
| **Format** | PDF / interactive web application |
| **Coverage** | All 25 Stadtbezirke |

**Available per Stadtbezirk:**
- Bevoelkerung (population)
- Flaeche nach Bodennutzungsarten (land use)
- Gesundheits- und Sozialwesen (health/social)
- Bildungswesen und Kultur (education/culture)
- Arbeitslosigkeit (unemployment)
- KFZ-Bestand (vehicle fleet)
- Wahlergebnisse (election results)

### 5E. Bayern Sozialindex - NOT AVAILABLE

| Property | Value |
|----------|-------|
| **Status** | Does NOT exist for Bavaria |
| **Comparison** | NRW has Schulsozialindex, Bavaria does not |

**Notes:**
- Unlike NRW which publishes a per-school Sozialindex, Bavaria has no equivalent publicly available index
- Must construct a proxy using Indikatorenatlas district-level indicators (unemployment, migration background, social welfare recipients)
- This is a significant gap compared to the NRW pipeline

**RECOMMENDED APPROACH FOR DEMOGRAPHICS:**
1. Download Stadtbezirke GeoJSON boundaries (5C)
2. Assign each school to a Stadtbezirk via spatial join
3. Download Indikatorenatlas CSV indicators (5A) for each Stadtbezirk
4. Construct composite socioeconomic scores from available indicators (unemployment rate, migration share, welfare recipients, education level)
5. Use population data (5B) for context

---

## 6. Academic Performance

### 6A. Bayerisches Landesamt fuer Schule - Uebertrittszahlen

| Property | Value |
|----------|-------|
| **URL** | https://www.las.bayern.de/qualitaetsagentur/bildungsberichterstattung/portal/themenseiten/uebertritte.html |
| **Format** | Web / PDF / interactive portal |
| **Coverage** | Bavaria-wide aggregate |
| **Granularity** | State-level (NOT per-school) |

**Available data:**
- Transition rates from primary to secondary (Mittelschule, Realschule, Gymnasium)
- 2021/22: 28% Mittelschule, 28% Realschule, 41% Gymnasium
- Historical trends available

**Notes:**
- Not available at per-school level
- Can use as city-wide context information

### 6B. Bayerisches Staatsministerium - Statistik und Forschung

| Property | Value |
|----------|-------|
| **URL** | https://www.km.bayern.de/ministerium/statistik-und-forschung |
| **Publication** | "Bayerns Schulen in Zahlen 2024/2025" |
| **Format** | PDF |
| **Coverage** | State-wide statistical overview |

**Notes:**
- Contains tabular overviews and charts on Bavarian school statistics
- Historical editions available in the Bavarian government's online order portal
- PDF format only - no per-school machine-readable data

### 6C. Abitur Results

| Property | Value |
|----------|-------|
| **Status** | NOT publicly available per school |
| **URL** | https://www.km.bayern.de/meldung/abiturpruefungen-2025-an-den-gymnasien-in-bayern |

**Notes:**
- Bavaria does NOT publish per-school Abitur results
- Only state-wide aggregate pass rates are published
- Press releases about Abitur season exist but contain no school-level data
- This is a significant gap - cannot create academic performance scores per school

**RECOMMENDED APPROACH FOR ACADEMIC DATA:**
1. Use state-wide transition rates as context (not per-school scoring)
2. Consider omitting academic performance column or using placeholder values
3. If needed, use Indikatorenatlas education indicators at Stadtbezirk level as a proxy

---

## 7. Points of Interest (POI) - Standard Approach

### 7A. OpenStreetMap / Overpass API

| Property | Value |
|----------|-------|
| **URL** | https://overpass-api.de/api/interpreter |
| **Format** | JSON |
| **Coverage** | All OSM-mapped POIs in Munich |
| **Cost** | Free, no key needed |

**Relevant POI categories (same as other pipelines):**
- `amenity=playground` - Playgrounds
- `leisure=park` - Parks
- `amenity=library` - Libraries
- `leisure=sports_centre` / `leisure=swimming_pool` - Sports facilities
- `amenity=theatre` / `amenity=cinema` - Cultural venues
- `amenity=doctors` / `amenity=pharmacy` - Medical facilities

### 7B. Google Places API (SUPPLEMENT)

Standard approach for POI enrichment, consistent with other city pipelines.

---

## 8. Summary: Data Availability Comparison

| Category | NRW | Hamburg | Frankfurt | Munich |
|----------|-----|---------|-----------|--------|
| School list (CSV) | Excellent (full CSV with details) | Good (web scrape) | Excellent (XLSX) | Fair (CSV but limited fields) |
| Coordinates | In source (UTM) | In source | Geocoding needed | Geocoding needed |
| Student counts | In source | In source | In source | NOT in CSV export |
| Sozialindex | Per-school | Not available | Not available | NOT available |
| Traffic/Accidents | Unfallatlas | Unfallatlas | Unfallatlas | Unfallatlas |
| Crime (PKS) | BKA + NRW state | BKA + HH state | BKA + HE state | BKA + BY state (PDF only) |
| Transit | Overpass API | Overpass API | Overpass API | MVV GTFS + Overpass |
| Demographics | Zensus grid | Stadtteilprofile | Stadtteilprofile | Indikatorenatlas (excellent) |
| Academic perf. | Limited | Limited | Limited | Not available per school |

## 9. Key Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| No coordinates in school CSV | Must geocode all schools | Use Google Geocoding API; budget for ~400 API calls |
| Limited school CSV fields | No student counts, no email/phone | Supplement with JedeSchule.de; scrape km.bayern.de detail pages |
| No Sozialindex | Cannot do per-school social scoring | Build proxy from Indikatorenatlas district-level indicators |
| Crime data PDF-only | Cannot easily extract structured data | Use BKA federal PKS data; apply city-wide rate uniformly |
| No per-school academic data | Cannot score academic performance | Use district-level education indicators as proxy |
| ISO-8859-15 encoding | Encoding mismatch risk | Explicit encoding parameter in pandas read_csv |

## 10. opendata.muenchen.de Portal Summary

The Munich Open Data Portal (https://opendata.muenchen.de/) is a rich source with datasets organized by provider. Key providers relevant to SchoolNossa:

- **Statistisches Amt** - Demographics, population, social indicators
- **Muenchner Verkehrs- und Tarifverbund (MVV)** - Transit GTFS and stop data
- **Referat fuer Stadtplanung** - Geographic/spatial data including Stadtbezirke boundaries

All datasets use the "Datenlizenz Deutschland Namensnennung 2.0" license (attribution required, free use).
