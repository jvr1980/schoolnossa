# Bremen Data Availability Research

**Date:** 2026-04-07
**State:** Bremen (Freie Hansestadt Bremen)
**Researcher:** Claude + user
**School types requested:** Both primary (Grundschule) and secondary (Oberschule, Gymnasium)

## Summary

| Category | Source | Granularity | Format | Access | Status |
|----------|--------|-------------|--------|--------|--------|
| School Master | bildung.bremen.de Schulwegweiser + GeoBremen Shapefile | per-school | Excel + SHP (EPSG:25832) | free download | Ready |
| Traffic | Unfallatlas (national) | per-accident GPS | CSV | free download | Ready |
| Crime | Kleine Anfragen PDFs (Stadtteil-level) | per-Beiratsbereich (22 districts) | PDF (needs parsing) | free download | Moderate effort |
| Transit | Overpass API (fallback); VBN GTFS available | per-stop | Overpass JSON / GTFS | free / registration | Ready |
| POI | Google Places API (New) | 500m radius | API JSON | API key needed | Ready |
| Demographics | Kleinraumig Infosystem + Ortsteilatlas | per-Ortsteil | CSV/XLS | free download | Ready |
| Academic | bildung.bremen.de Abiturnoten (2015-2025) | per-school | HTML (needs scraping) | free | Moderate effort |
| Websites | School websites via Schulwegweiser | per-school | HTML | scraping | Ready |

## Detailed Findings

### 1. School Master Data

#### 1A. Schulwegweiser Excel (PRIMARY — school details)

| Property | Value |
|----------|-------|
| **URL** | https://www.bildung.bremen.de/schulwegweiser-3714 |
| **Format** | Excel (.xls/.xlsx) download |
| **Encoding** | Likely Windows-1252 or UTF-8 (verify on download) |
| **Coverage** | All schools in Bremen and Bremerhaven, public and private |
| **Authority** | Der Senator fur Kinder und Bildung |

**Available fields:**
- School name, school number (Schulnummer)
- Address, phone, fax, email
- School principal
- School type (Schulform): Grundschule, Oberschule, Gymnasium, Werkschule, etc.
- District (Stadtteil)
- All-day care status (Ganztagsschule)
- Foreign language offerings
- Student enrollment numbers

**Note:** No coordinates included — must be joined with the geodata below.

#### 1B. GeoBremen Schulstandorte Shapefile (PRIMARY — coordinates)

| Property | Value |
|----------|-------|
| **Download URL** | https://gdi2.geo.bremen.de/inspire/download/Schulstandorte/data/Schulstandorte_HB_BHV.zip |
| **MetaVer** | https://metaver.de/trefferanzeige?docuuid=8A3117FE-FF51-41ED-9BC4-C20633C91ACF |
| **GovData** | https://www.govdata.de/daten/-/details/schulstandorte-land-bremen |
| **Format** | Shapefile (point geometries, ~202 features) |
| **Coordinate system** | EPSG:25832 (ETRS89 / UTM Zone 32N) — same as NRW |
| **License** | CC-BY (Freie Hansestadt Bremen, Die Senatorin fur Kinder und Bildung) |
| **Updated** | December 2025 |
| **WMS** | http://geodienste.bremen.de/wms_schulstandorte |

**Fields:** School address, school type (Schulform), all-day school status, ISCED-2011 level.

**Join strategy:** Match schools between the Schulwegweiser Excel and the Shapefile using school name + address or Schulnummer. Fallback: geocode via Nominatim for any missing matches.

#### 1C. JedeSchule.de (Cross-reference)

| Property | Value |
|----------|-------|
| **URL** | https://jedeschule.codefor.de/csv-data/schools.csv |
| **Bremen page** | https://jedeschule.de/laender/bremen/ |
| **Format** | CSV, JSON |
| **Note** | Community-maintained scrape, data from 2023. Useful as cross-reference only. |

### 2. Traffic Data

#### 2A. Unfallatlas — National Accident Data (PRIMARY SOURCE)

| Property | Value |
|----------|-------|
| **URL** | https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/ |
| **ULAND code** | `04` (Bremen) |
| **Format** | CSV (semicolon-separated, UTF-8-sig, German comma decimal) + Shapefile |
| **Coordinate system** | WGS84 (EPSG:4326), German comma decimal separator in coords |
| **Temporal coverage** | 2016-2024 (9 years) |
| **Spatial resolution** | Point-level (individual accident GPS coordinates) |
| **License** | Datenlizenz Deutschland Namensnennung 2.0 |

**Filter:** `ULAND == '04'`. Bremen is a city-state so no sub-filtering needed (though a bounding box can exclude Bremerhaven if desired).

**Key columns:** `ULAND`, `UJAHR`, `UMONAT`, `UKATEGORIE`, `UART`, `UTYP1`, `IstRad`, `IstPKW`, `IstFuss`, `IstKrad`, `LINREFX`, `LINREFY`

Same approach as NRW/Munich/Stuttgart/Frankfurt pipelines — directly reusable.

#### 2B. Bremen Traffic Volume Data

**Not available as structured open data.** Bremen has:
- Verkehrsmengenkarten (static PDF maps, 2005/2010 only)
- VMZ Bremen real-time sensors (no historical CSV downloads)
- 12 bicycle counting stations (website display only, no bulk download)

**Recommendation:** Use Unfallatlas accident-based approach.

### 3. Crime Data

#### 3A. Stadtteil-Level Crime Data via Parliamentary Inquiries (PRIMARY)

| Property | Value |
|----------|-------|
| **2023-2024 data** | https://www.rathaus.bremen.de/sixcms/media.php/13/20250617_top%2011_Kriminalitaet_in_den_Stadtteilen.pdf |
| **2022-2023 data** | https://www.rathaus.bremen.de/sixcms/media.php/13/20240709_Verteilung_Kriminalitaet_auf_die_Stadtteil.pdf |
| **Granularity** | 22 Beiratsbereiche (council districts) |
| **Format** | PDF tables (structured, parseable with tabula/camelot) |
| **Access** | Free download |

**Crime categories per district (7):**
| PKS Key | Category | Pipeline Column |
|---------|----------|-----------------|
| (total) | Straftaten insgesamt | `crime_total` |
| 100000 | Sexualstraftaten | `crime_sexual` |
| 210000 | Raub, rauberische Erpressung | `crime_robbery` |
| 220000 | Korperverletzung | `crime_assault` |
| 435*00 | Wohnungseinbruchdiebstahl | `crime_burglary` |
| ****00 | Diebstahl insgesamt | `crime_theft` |
| 730000 | Rauschgiftdelikte | `crime_drugs` |

**Additional tables:** Clearance rates and non-German suspects per Beiratsbereich.

**22 Beiratsbereiche:** Blockland, Blumenthal, Borgfeld, Burglesum, Findorff, Gropelingen, Hemelingen, Horn-Lehe, Huchting, Mitte, Neustadt, Oberneuland, Obervieland, Osterholz, Ostliche Vorstadt, Schwachhausen, Seehausen, Strom, Vahr, Vegesack, Walle, Woltmershausen.

#### 3B. City-Wide PKS (Supplementary)

| Property | Value |
|----------|-------|
| **URL** | https://www.inneres.bremen.de/dokumente/pks-2496 |
| **Format** | PDF (Grundtabellen) |
| **Granularity** | Land Bremen / Stadt Bremen / Stadt Bremerhaven (no Stadtteil) |
| **Years** | 2006-2024 |

### 4. Transit Data

#### 4A. Overpass API (RECOMMENDED — matches existing pattern)

| Property | Value |
|----------|-------|
| **Access** | Free, no registration, no API key |
| **Query** | `highway=bus_stop`, `railway=tram_stop`, `public_transport=stop_position` within Bremen bounding box |
| **Format** | JSON |
| **Coverage** | All OSM-mapped stops (bus, tram, S-Bahn) |

Same approach as Hamburg and NRW pipelines — directly reusable.

#### 4B. VBN GTFS Feed (Alternative — richer data)

| Property | Value |
|----------|-------|
| **Provider** | Connect Fahrplanauskunft GmbH (for VBN) |
| **Access** | Registration required at https://connect-fahrplanauskunft.de/datenbereitstellung/ |
| **Format** | GTFS ZIP (stops, routes, schedules) |
| **License** | CC BY-SA 4.0 |
| **Coverage** | All Bremen + Lower Saxony public transit (BSAG tram/bus included) |
| **Update** | Daily |
| **Also on** | GovData, Mobilithek |

The GTFS feed provides schedule frequency data but requires registration. For proximity-only enrichment, Overpass API suffices.

### 5. POI Data

**Google Places API (New)** — shared pattern across all cities. No city-specific research needed.

Confirm `GOOGLE_PLACES_API_KEY` is available in `config.yaml`. Standard POI categories apply (parks, playgrounds, libraries, sports facilities, supermarkets, etc.).

### 6. Demographics & Social Index

#### 6A. Schulsozialindex

Bremen **has** a per-school social index (in use since 2010), but it is **NOT publicly released** as a downloadable dataset. It is used internally for resource allocation (class sizes, staffing).

- Methodology described in: https://www.bremische-buergerschaft.de/drs_abo/2017-12-13_Drs-19-1446_991be.pdf
- Factsheet: https://www.bildung.bremen.de/sixcms/media.php/13/sosiehtsaus_schulsozialindex.pdf
- Variables: SGB-II quota (weighted by students' residential Ortsteil), non-German family language share, special education needs share

**Pipeline implication:** Must use district-level proxy (like the GISD approach for other cities).

#### 6B. Kleinraumig Infosystem (PRIMARY — downloadable tables)

| Property | Value |
|----------|-------|
| **URL** | https://www.statistik-bremen.de/soev/aktuelle_tabellen.cfm |
| **Granularity** | Ortsteil level (finest available) |
| **Format** | HTML, Excel (XLS), CSV |
| **Key tables** | SGB-II data (table 22811), population by migration (tables 12400+), unemployment (table 13211) |

#### 6C. Bremer Ortsteilatlas

| Property | Value |
|----------|-------|
| **URL** | https://www.statistik.bremen.de/datenangebote/bremer-ortsteilatlas-15228 |
| **Interactive** | https://www.statistik-bremen.de/tabellen/kleinraum/ortsteilatlas/atlas.html |
| **Granularity** | Ortsteil, Stadtteil, Stadtbezirk |
| **Indicators** | 180+ metrics across 20 topic areas |

#### 6D. Stadtteil- und Ortsteiltabellen

| Property | Value |
|----------|-------|
| **URL** | https://www.statistik.bremen.de/datenangebote/stadtteil-und-ortsteiltabellen-4529 |
| **Format** | HTML, Excel, CSV |
| **Granularity** | 21 Stadtteile + 3 independent Ortsteile |
| **Topics** | Population, social benefits, labor market, education, housing |

**Recommended pipeline strategy:** Join schools to their Ortsteil, attach SGB-II rate, migration share, and unemployment rate from the Kleinraumig tables as a composite deprivation proxy.

### 7. Academic Performance

#### 7A. Abitur Results (Per-School)

| Property | Value |
|----------|-------|
| **URL** | https://www.bildung.bremen.de/abiturnoten-216729 |
| **Coverage** | ~40 schools with Abitur across Bremen |
| **Years** | 2015-2025 (11 years) |
| **Format** | HTML tables (needs scraping, no CSV/Excel download) |
| **Data** | Per-school average Abitur grade |

#### 7B. Student Enrollment Numbers

| Property | Value |
|----------|-------|
| **URL** | https://www.bildung.bremen.de/schuler-innenzahlen-4372 |
| **Format** | PDF only |
| **Coverage** | Annual counts from 2008/2009 through 2025/2026 |
| **Note** | Per-school breakdowns inside PDFs, would require tabula extraction |

### 8. Website & Metadata

- School websites are accessible via the Schulwegweiser links
- No central robots.txt restrictions noted
- bildung.bremen.de serves as the central school information portal
- Individual school websites are generally accessible for scraping

## Bremen School System

Bremen uses a **two-track (zweigliedrig) system** after primary:

| School Type | Grades | Notes |
|-------------|--------|-------|
| **Grundschule** | 1-4 | Catchment-area based |
| **Oberschule** | 5-12/13 | Comprehensive, can lead to Abitur in 13 years |
| **Gymnasium** | 5-12 | Abitur after 12 years (G8) |
| **Werkschule** | 9-10 | Vocational-oriented |
| **Bildungs-/Beratungszentren** | various | Special-needs schools |

Total: ~200 schools across Bremen and Bremerhaven.

## Comparison with Existing Cities

| Aspect | Berlin | Hamburg | NRW | Bremen |
|--------|--------|---------|-----|--------|
| School source | scrapers | WFS GeoJSON | Open Data CSV | Excel + Shapefile |
| Coordinates | scraped | in GeoJSON | UTM EPSG:25832 | UTM EPSG:25832 (same as NRW) |
| Traffic type | sensor volumes | sensor volumes | accident counts (Unfallatlas) | accident counts (Unfallatlas) |
| Crime granularity | Bezirk (12) | Stadtteil (107) | city-wide est. | Beiratsbereich (22) |
| Crime format | CSV | CSV | PDF parsing | PDF parsing |
| Transit source | Overpass API | HVV GeoJSON | Overpass API | Overpass API |
| Demographics | per-school | per-Stadtteil | per-school (Sozialindex) | per-Ortsteil (no public Sozialindex) |
| Academic data | MSA + demand | Abitur per-school | Anmeldezahlen | Abitur per-school (HTML scrape) |
| School count | ~300 secondary | ~400 all | ~400 (Cologne+Dusseldorf) | ~200 all |

## Recommendations

### Template City: NRW (closest match)

Bremen's data landscape is closest to NRW:
- Same coordinate system (EPSG:25832) requiring pyproj conversion
- Same traffic data source (Unfallatlas, just different ULAND filter)
- Same transit approach (Overpass API)
- Similar crime data situation (PDF parsing required, district-level)
- Demographics via district-level tables rather than per-school Sozialindex

### City-Specific Considerations

1. **Excel + Shapefile join** — Unlike NRW's single CSV, Bremen requires joining two data sources (Schulwegweiser Excel for details, Shapefile for coordinates). Match on school name/address or Schulnummer.
2. **Crime PDF extraction** — Parliamentary inquiry PDFs have well-structured tables (22 districts x 7 crime categories). Use `tabula-py` for extraction. Only 2 years of Stadtteil data available.
3. **Abitur scraping** — Per-school Abitur averages exist on HTML pages (2015-2025). Worth scraping as a bonus enrichment not available for all cities.
4. **Small city-state** — Only ~200 schools total. Pipeline will be fast to run.
5. **Bremerhaven** — The school data includes Bremerhaven. Decide whether to include it (it's technically part of Land Bremen) or filter to Stadt Bremen only.

### Potential Blockers

- Crime PDFs may change format between years — need robust extraction
- Schulwegweiser Excel encoding needs verification on download
- No public per-school Sozialindex — must rely on Ortsteil-level proxy

### Estimated Effort: 3/5

Slightly easier than NRW (fewer schools, simpler geography) but requires PDF parsing for crime data and HTML scraping for Abitur results. The dual-source school data (Excel + Shapefile) adds a join step not present in NRW.
