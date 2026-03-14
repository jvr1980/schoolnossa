# Data Availability Research

Research into expanding SchoolNossa beyond Berlin — covering other German states, European countries, and German demographics at PLZ (postal code) level.

---

## A) German States: Per-School Abitur & MSA Data

### Easily Available (like Berlin)

| State | Per-School Abitur Data | Format | Source |
|-------|----------------------|--------|--------|
| **Berlin** | Yes — grades + success rates | CSV | [daten.berlin.de](https://daten.berlin.de/datensaetze/abitur-ergebnisse-2025-1612167) |
| **Hamburg** | Yes — grades + graduate counts | PDF | [hamburg.de](https://www.hamburg.de), [gymnasium-hamburg.net](https://gymnasium-hamburg.net/abiturnoten) |
| **Brandenburg** | Yes — shared statistics office with Berlin | Tables/Reports | [statistik-berlin-brandenburg.de](https://www.statistik-berlin-brandenburg.de/b-i-9-j/) |

### Partially Available (more effort required)

| State | Per-School Abitur Data | Format | Source |
|-------|----------------------|--------|--------|
| **Niedersachsen** | Yes — but school admin login for detail | Web/PDF (GOSIN) | [mk.niedersachsen.de](https://www.mk.niedersachsen.de/startseite/schule/unsere_schulen/allgemein_bildende_schulen/gymnasium/abiturprufung/abiturdaten-2013-online-117422.html) |
| **Thüringen** | State-level + top schools named | Web portal | [schulstatistik-thueringen.de](https://www.schulstatistik-thueringen.de/html/themen/schullaufbahn/abiturientenstatistik-thueringen.html) |
| **Sachsen** | Partially — requires database navigation | Statistics DB | [statistik.sachsen.de](https://www.statistik.sachsen.de/html/allgemeinbildende-schulen.html) |

### NOT Publicly Available Per School

Bayern, NRW, Baden-Württemberg, Hessen, Rheinland-Pfalz, Saarland, Sachsen-Anhalt, Schleswig-Holstein, Mecklenburg-Vorpommern, Bremen — these only publish **state-level aggregates**. Some (Hessen, Baden-Württemberg) explicitly restrict per-school data access.

### MSA (Mittlerer Schulabschluss) Data

MSA per-school data is substantially less available than Abitur data across all states. Berlin itself abolished MSA at Gymnasiums starting 2023/24 school year.

### Cross-State Resource

- **JedeSchule.de** ([jedeschule.codefor.de](https://jedeschule.de/daten/)): ~30,000 schools, CC0 license. However, data is frozen at 2017 and Abitur performance integration is unclear.

---

## B) European Countries: School Performance Data

### Tier 1 — Excellent: UK (England) *[IMPLEMENTED]*

| Aspect | Details |
|--------|---------|
| **Exams** | GCSE (age 15) + A-Levels (age 18) |
| **Portal** | [compare-school-performance.service.gov.uk](https://www.compare-school-performance.service.gov.uk) |
| **API** | [Explore Education Statistics API](https://explore-education-statistics.service.gov.uk) with CSV downloads |
| **Granularity** | Per-school, per-subject, progress measures |
| **School directory** | [GIAS](https://get-information-schools.service.gov.uk/) — all schools with URN, type, location |
| **Integration difficulty** | **LOW** — well-documented REST API, standardized CSV |
| **Mapping** | Abitur → A-Levels, MSA → GCSE, Gymnasium → Grammar School, Bezirk → Local Authority |

### Tier 2 — Good: France

| Aspect | Details |
|--------|---------|
| **Exam** | Baccalauréat (age 18) |
| **Portal** | [data.education.gouv.fr](https://data.education.gouv.fr) |
| **Data** | IVAL — per-lycée success rates, value-added indicators, mention rates |
| **Format** | API + CSV downloads |
| **Integration difficulty** | **LOW-MEDIUM** |
| **Key URL** | [data.gouv.fr IVAL dataset](https://www.data.gouv.fr/en/datasets/indicateurs-de-resultat-des-lycees-denseignement-general-et-technologique/) |

### Tier 2 — Good: Portugal

| Aspect | Details |
|--------|---------|
| **Portal** | InfoEscolas — 100+ indicators per school, ~5,000 establishments |
| **Format** | Web portal (limited API) |
| **Integration difficulty** | **MEDIUM** |
| **Key URL** | [DGEEC](https://www.dgeec.medu.pt/) |

### Tier 3 — Fragmented: Spain

| Aspect | Details |
|--------|---------|
| **Exam** | EBAU/EvAU (formerly Selectividad, age 18) |
| **Issue** | Each of 17 autonomous communities publishes separately |
| **Aggregators** | [buscocolegio.com](https://www.buscocolegio.com/notas-de-selectividad-evau-ebau.jsp) has per-school rankings |
| **Format** | Mostly PDFs, no unified API |
| **Integration difficulty** | **HIGH** (region-by-region scraping) |

### Tier 3 — Moderate: Scotland

| Aspect | Details |
|--------|---------|
| **Exams** | Highers / Advanced Highers |
| **Source** | [SQA statistics](https://www.sqa.org.uk/sqa/105123.html) + [opendata.scot](https://opendata.scot/) |
| **Integration difficulty** | **MEDIUM** |

### Tier 4 — Aggregate Only

- **Netherlands** — DUO administrative data, mostly aggregate
- **Sweden, Denmark, Finland** — PISA-based, no per-school public data
- **Austria, Czech Republic** — Matura/Maturita, minimal per-school data

### Recommendation

**UK (England) is the clear winner** for European expansion. Best API, most granular data, easiest integration. France (Baccalauréat/IVAL) is second-best.

---

## C) German Demographics at PLZ (Postal Code) Level

### Critical Finding

**Official census data (Zensus 2022) is NOT published at PLZ level.** It uses administrative boundaries (Gemeinde/Kreis) and grid cells (100m/1km/10km). PLZ-level demographics require either commercial purchase or DIY spatial aggregation.

### PLZ Boundary Shapefiles — Available Free

| Source | Format | License | URL |
|--------|--------|---------|-----|
| Geofabrik | Shapefile, GeoJSON, GeoPackage | ODbL | [geofabrik.de](https://www.geofabrik.de/de/data/postalcodes.html) |
| GovData | GeoJSON | ODbL | [govdata.de](https://www.govdata.de/) (search "Postleitzahlen") |
| suche-postleitzahl.org | KML, SHP, CSV | Free | [suche-postleitzahl.org](https://www.suche-postleitzahl.org/) |
| BKG (official) | Vector | Restricted | [gdz.bkg.bund.de](https://gdz.bkg.bund.de/) |
| OpenPLZ API | JSON/GeoJSON | Open | [openplzapi.org](https://www.openplzapi.org/) |

### Demographic Data Availability by Type

| Demographic | At PLZ Level? | Best Free Alternative | Commercial at PLZ? |
|-------------|--------------|----------------------|-------------------|
| **Population count** | Indirect only | Zensus 2022 grid (100m) → aggregate to PLZ | Yes (RFS-Data, p17-data) |
| **Gender split** | No | Zensus at Gemeinde level | Possibly (WIGeoGIS) |
| **Age groups** | No | Zensus at Gemeinde level | Yes (RFS-Data, p17-data) |
| **Ethnicity/Migration** | No | Zensus at Kreis level only | Unlikely |
| **Income/Purchasing power** | No official | None free | Yes (GfK, RFS-Data, WIGeoGIS) |
| **Education level** | No | Zensus at Bundesland level only | No |

### DIY Approach (Free but Effort-Intensive)

1. Download Zensus 2022 100m grid data from [ergebnisse.zensus2022.de](https://ergebnisse.zensus2022.de/datenbank/online/)
2. Download PLZ boundary shapefiles from [Geofabrik](https://www.geofabrik.de/de/data/postalcodes.html) (free, ODbL)
3. Use PostGIS/QGIS to spatially join grid cells to PLZ polygons
4. Aggregate population, age, gender from grid cells within each PLZ
5. **Limitation:** Migration background and income are NOT in grid data

### Key Data Sources

| Source | URL | What It Provides |
|--------|-----|-----------------|
| Zensus 2022 Database | [ergebnisse.zensus2022.de](https://ergebnisse.zensus2022.de/datenbank/online/) | Census data at grid/municipality level |
| Zensus Atlas | [atlas.zensus2022.de](https://atlas.zensus2022.de/) | Interactive grid-based visualization |
| Destatis | [destatis.de](https://www.destatis.de/) | Federal Statistical Office |
| Geofabrik PLZ Shapefiles | [geofabrik.de](https://www.geofabrik.de/de/data/postalcodes.html) | Free PLZ boundaries |
| WIGeoGIS | [wigeogis.com](https://www.wigeogis.com/en/demographics_germany_data) | 600+ demographic variables (commercial, Munich sample free) |
| RFS-Data | [rfs-data.de](https://www.rfs-data.de/) | Purchasing power + demographics by PLZ (commercial) |

---

## Data Attribution

- **Berlin Open Data**: CC BY 3.0 DE — Senatsverwaltung für Bildung, Jugend und Familie
- **UK DfE**: Open Government Licence v3.0 — Department for Education
- **GIAS**: Open Government Licence v3.0 — Department for Education
- **OpenStreetMap/Geofabrik**: Open Database License (ODbL)
