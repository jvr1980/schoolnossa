# Dresden Data Availability Research

**Date:** 2026-04-07
**State:** Sachsen (Saxony)
**Researcher:** Claude + user

## Summary

| Category | Source | Granularity | Format | Access | Status |
|----------|--------|-------------|--------|--------|--------|
| School Master | Sächsische Schuldatenbank API | per-school | CSV (API) | free, no auth | Ready |
| Traffic | Unfallatlas (destatis) | per-accident GPS | CSV | free download | Ready |
| Crime | Dresden Open Data Portal | per-Stadtteil | CSV | free download | Ready |
| Transit | Overpass API (OSM) | per-stop | API/JSON | free, no key | Ready |
| POI | Google Places API | 500m radius | API | key needed | Ready |
| Demographics | Dresden Stadtteilkatalog | per-Stadtteil | PDF/interactive | free | Needs scraping |
| Academic | Sachsen Statistik (aggregate) | state/city level | Excel/PDF | free | Limited |
| Websites | Per-school URLs from API | per-school | HTML | scraping | Ready |

## Detailed Findings

### 1. School Master Data

**Source:** Sächsische Schuldatenbank — `schuldatenbank.sachsen.de`
**API Endpoint:** `https://schuldatenbank.sachsen.de/api/v1/schools?format=csv&address=Dresden`
**Format:** CSV, comma-separated, UTF-8
**Coordinate System:** WGS84 (EPSG:4326) — latitude/longitude fields included directly
**Access:** Free, public API, no authentication required for school listing endpoints
**Documentation:** Swagger UI at `https://schuldatenbank.sachsen.de/docs/api.html` (loads `api_v1.yaml`)

**Available Fields (45 columns):**
- `institution_key`, `name`, `id`
- `street`, `postcode`, `community` (address)
- `longitude`, `latitude` (WGS84 coordinates)
- `phone`, `fax`, `email`, `homepage` (contact)
- `headmaster`, `headmaster_salutation`
- `school_category_key`, `school_type_key`, `legal_status_key`
- Building and property information
- Educational configuration codes

**Filter Parameters:**
- `address=Dresden` — filter by city
- `school_category_key=10` — Allgemeinbildende Schulen (general education)
- `school_type_key` values: `11` (Grundschule), `13` (Gymnasium), `12` (Oberschule), etc.
- `legal_status_key`: `01` (public), `02` (private/independent)

**School Counts (2024/25):**
- 72 Grundschulen (primary) — 20,295 students
- 27 Oberschulen (secondary) — 11,851 students
- 21 Gymnasien — 17,833 students
- 14 Förderschulen — 2,085 students
- 2 Gemeinschaftsschulen, 1 Abendgymnasium, 1 Abendoberschule
- Total: 148 public schools, ~41 private school operators with ~16,800 additional students

**Assessment:** Excellent — structured API with CSV export, coordinates in WGS84, no scraping needed. Much simpler than Berlin (web scraping) and similar convenience to NRW (Open Data CSV). Covers both primary and secondary schools in a single API.

### 2. Traffic Data

**Source:** Unfallatlas (Statistisches Bundesamt / destatis)
**Download:** `https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/`
**Format:** CSV in ZIP files, pattern: `Unfallorte{YEAR}_EPSG25832_CSV.zip`
**Coordinate System:** UTM EPSG:25832 (same as NRW — conversion to WGS84 needed via pyproj)
**Years Available:** 2016–2024
**Access:** Free download, Datenlizenz Deutschland Namensnennung 2.0
**Filter:** `ULAND=14` for Sachsen (same approach as NRW where `ULAND=05`)

**Data Fields:**
- Accident location (coordinates), year, month
- Accident type, severity (killed/seriously injured/slightly injured)
- Vehicle categories involved (car, bicycle, motorcycle, pedestrian)

**Assessment:** Identical approach to NRW pipeline. Same Unfallatlas data, same coordinate system, same filter pattern — just change `ULAND=05` to `ULAND=14`. Can reuse NRW's traffic enrichment code with minimal changes.

### 3. Crime Data

**Source:** Dresden Open Data Portal — Landeskriminalamt Sachsen data
**URL:** `https://opendata.dresden.de/` — dataset "Kriminalität ab Stadtteile 2002ff."
**Format:** CSV
**Granularity:** **Per-Stadtteil** (district-level) — this is excellent, comparable to Hamburg
**Years:** 2002–2024 (23 years of data)
**Access:** Free, CC BY 3.0 DE license
**Contact:** Thomas Gründemann (opendata@dresden.de)

**Data Fields:**
- Jahr (year)
- Stadtteil (district identifier)
- Fälle erfasst (recorded cases, by crime scene principle)
- Fälle aufgeklärt (solved cases)
- Tatverdächtige insgesamt (total suspects)

**Additional Context:**
- Dresden 2024: 47,305 total crimes, 52.7% clearance rate
- Dresden 2025: 42,735 crimes (lowest in 10 years except 2021 COVID)
- Per-Stadtteil data allows direct mapping of crime rates to school locations

**Assessment:** Excellent — Stadtteil-level crime data is the best granularity after Hamburg's 107-district breakdown. Much better than NRW's city-wide estimates. Can map each school to its Stadtteil and use actual crime counts.

### 4. Transit Data

**Primary Source:** Overpass API (OpenStreetMap) — same as Berlin and NRW
**Backup Source:** DVB (Dresdner Verkehrsbetriebe) Geobasisdaten at `https://www.dvb.de/de-de/liniennetz/geobasisdaten`
**Additional:** VVO stops available as `VVO_STOPS.JSON` (daily updated)

**DVB Geobasisdaten:**
- Network geodata available for download
- License: private use only
- Formats not fully documented on public page (may require accepting terms)

**Overpass API (recommended):**
- Free, no key needed, consistent with other city pipelines
- Query for `highway=bus_stop`, `railway=tram_stop`, `railway=station`, `railway=halt`
- Dresden bounding box: approximately `lat 50.96–51.14`, `lon 13.57–13.90`
- Covers: Bus, Tram (Straßenbahn), S-Bahn, regional rail

**Assessment:** Use Overpass API for consistency with Berlin/NRW pipelines. Dresden has extensive tram network (12 lines) plus buses and S-Bahn, so transit coverage will be rich.

### 5. POI Data

**Source:** Google Places API (New) — shared across all cities
**Access:** Requires `GOOGLE_PLACES_API_KEY` in config.yaml
**Method:** 500m radius search around each school location
**Categories:** Standard set (parks, restaurants, supermarkets, sports facilities, libraries, etc.)

**Assessment:** No city-specific work needed. Shared script handles all cities.

### 6. Demographics & Social Index

**Source 1:** Dresden Stadtteilkatalog
**URL:** `https://www.dresden.de/de/leben/stadtportrait/statistik/publikationen/interaktive-anwendungen/stadtteilkatalog.php`
**Granularity:** Per-Stadtteil and per-Ortsteil (finest subdivision)
**Format:** Interactive web application + PDF publications
**Updated:** 2024 data available
**Indicators:** Population, social structure, construction/housing, infrastructure, economy, voting behavior
**Access:** Free, but primarily interactive/PDF — no direct CSV download found

**Source 2:** Zensus / GISD (German Index of Socioeconomic Deprivation)
**Access:** Available at PLZ level — can use as proxy (same approach as Munich/Stuttgart)
**Format:** CSV download

**Sachsen Sozialindex Status:**
- Sachsen does NOT yet have a per-school Sozialindex (unlike NRW which has Schulsozialindex)
- SPD Dresden has campaigned for introducing one, but not implemented at state level
- The city applies a social index for KITAs but not systematically for schools

**Assessment:** Use GISD quintile as demographic proxy (same as Munich/Stuttgart pipeline). Stadtteilkatalog data could supplement but would require scraping. No per-school Sozialindex available — this is a gap compared to NRW.

### 7. Academic Performance

**Source:** Sachsen Statistisches Landesamt
**URL:** `https://www.statistik.sachsen.de/html/allgemeinbildende-schulen.html`
**Granularity:** State and district aggregate only — no per-school Abitur results published
**Format:** Excel workbooks (Statistical Report B I 1), PDF publications

**Available Data:**
- Total student enrollment by school type (state/district level)
- Graduate counts by qualification type
- Teacher employment figures
- Regional breakdowns by Landkreis and municipality

**Per-School Data:** Not publicly available for Sachsen. Abitur results are not published at school level.

**Enrollment Numbers:** The city publishes aggregate school counts and total enrollment (148 schools, 66,208 students for 2024/25), but not per-school breakdowns via download.

**Schulnetzplan:** Dresden publishes a Schulnetzplan (school network plan) PDF with detailed per-school information including capacity and enrollment, but it's in PDF format requiring extraction.

**Assessment:** Limited compared to other cities. No per-school academic data available for download. Can potentially extract from Schulnetzplan PDF, but this is labor-intensive. Mark as "city-wide aggregate only" for now.

### 8. Website & Metadata

**Source:** School websites from Schuldatenbank API `homepage` field
**Access:** Direct scraping of individual school websites
**Coverage:** Most schools have websites listed in the API data

**Central Portal:** The Sächsische Schuldatenbank itself serves as a central school information portal with school profiles.

**Robots.txt:** Standard school websites — no known blanket restrictions. Individual sites may vary.

**Assessment:** Ready. The API provides homepage URLs, and the description pipeline can scrape and generate descriptions from these.

## Comparison with Existing Cities

| Aspect | Berlin | Hamburg | NRW | Dresden |
|--------|--------|---------|-----|---------|
| School source | Web scrapers | WFS GeoJSON | Open Data CSV | **API CSV** (best) |
| Coordinates | WGS84 | WGS84 | UTM EPSG:25832 | **WGS84** |
| Traffic type | Sensor volumes | Sensor volumes | Accident counts | **Accident counts** |
| Traffic source | Berlin API | Hamburg API | Unfallatlas | **Unfallatlas** |
| Crime granularity | Bezirk (12) | Stadtteil (107) | City-wide est. | **Stadtteil** |
| Crime source | PKS Berlin | PKS Hamburg | PKS NRW | **Open Data Portal** |
| Transit source | Overpass API | HVV GeoJSON | Overpass API | **Overpass API** |
| Demographics | Per-school | Per-Stadtteil | Per-school (Sozialindex) | **GISD by PLZ** |
| Academic data | MSA + demand | Abitur | Anmeldezahlen | **Aggregate only** |
| School types | Primary + Secondary | Both combined | Both combined | **Both in one API** |

## Recommendations

### Template City: **NRW pipeline** (closest match)

Dresden most closely resembles the NRW pipeline because:
1. **Traffic:** Same Unfallatlas source, same coordinate conversion (EPSG:25832 → WGS84)
2. **Transit:** Same Overpass API approach
3. **Crime:** Stadtteil-level data (actually better than NRW's city-wide estimation)
4. **School data:** CSV-based (API vs file, but same output format)

Key differences from NRW:
- **School data is easier:** Direct API with WGS84 coordinates vs NRW's UTM conversion
- **Crime data is better:** Per-Stadtteil vs NRW's city-wide estimates
- **No Sozialindex:** NRW has per-school Schulsozialindex; Dresden will use GISD proxy
- **No Anmeldezahlen:** NRW has per-school demand data; Dresden doesn't publish this

### City-Specific Phases

1. **Scraper phase will be simpler** — API call instead of web scraping or file parsing
2. **Crime enrichment will be richer** — Stadtteil-level mapping similar to Hamburg approach
3. **Demographics will use GISD proxy** — same as Munich/Stuttgart
4. **No city-specific academic enrichment** — skip Anmeldezahlen phase

### Potential Blockers

- **Stadtteilkatalog demographics:** Only available as interactive app/PDF — may need to use GISD as sole demographic proxy
- **Academic data gap:** No per-school performance data publicly available
- **DVB transit data license:** "Private use only" — use Overpass API instead to avoid licensing issues

### Estimated Effort: **3 out of 5**

Easier than NRW (effort 4) because:
- School data comes from a clean API (no file parsing, no coordinate conversion for schools)
- Crime data is already at Stadtteil level (no population-weighted estimation needed)
- Can reuse most NRW enrichment code with minor adaptations

Harder than a minimal pipeline because:
- Need Stadtteil→school mapping for crime data
- Need Unfallatlas coordinate conversion (same as NRW)
- GISD demographic proxy requires PLZ matching
