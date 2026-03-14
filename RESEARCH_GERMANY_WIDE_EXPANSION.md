# Research: Expanding School Nossa Beyond Berlin

## Executive Summary

This document captures research findings on three questions:
1. Which German states publish school-level performance data (Abitur, MSA)?
2. What demographic data is publicly available at PLZ/fine-grained level for Germany?
3. Can we estimate school performance for other cities using Berlin data + area demographics?

**Key finding:** Only Berlin and Hamburg publish per-school Abitur data. This makes an ML-based estimation approach not just useful — it's the **only viable strategy** for covering other German cities.

---

## Part A: School Performance Data Availability by State

### States that DO publish school-level Abitur data

| State | Data Available | Format | Source |
|-------|---------------|--------|--------|
| **Berlin** | Abitur average per school, pass rates, subject scores | Open Data CSV (CC-BY 4.0) | [daten.berlin.de](https://daten.berlin.de/datensaetze/abitur-ergebnisse-2025-1612167) |
| **Hamburg** | Abitur average per school, graduate count | PDF lists | [hamburg.de](https://www.hamburg.de/resource/blob/941494/d96604f49abdcfd67f72d2c11e2a68b1/ergebnisse-der-vorlaeufigen-abiturabfrage-2024-data.pdf), [gymnasium-hamburg.net](https://gymnasium-hamburg.net/abiturnoten) |

### States that do NOT publish school-level data

| State | Notes |
|-------|-------|
| Bayern | Statewide averages only |
| Baden-Württemberg | Statewide averages only (e.g., 2.23 in 2024) via statistik-bw.de |
| NRW | Schools get own results via protected login; not public |
| Hessen | Statewide averages only |
| Sachsen | No school-level publication |
| Niedersachsen | No school-level data |

**Why?** Germany has a strong cultural aversion to public "Schulrankings". Most states deliberately withhold school-level results.

### MSA (Mittlerer Schulabschluss) per school

- **Only Berlin** publishes MSA results per school via [bildung.berlin.de/Schulverzeichnis](https://www.bildung.berlin.de/Schulverzeichnis/PruefErgebnisse.aspx)
- No other state publishes school-level MSA data

### Pan-German databases

**None exist at school level.** The closest resources are:
- **KMK Abiturnotenstatistik** — grade distributions by state (not school)
- **IQB-Bildungstrend** — student competencies by state (sample-based)
- **Bildungsmonitor (INSM)** — ranks states, not schools
- **Nationaler Bildungsbericht** ([bildungsbericht.de](https://www.bildungsbericht.de)) — aggregate level

---

## Part B: Demographic Data for Germany

### Critical insight: PLZ is NOT an official administrative unit

Official German statistics use Gemeinden (municipalities) and Kreise (districts), not PLZ. To get PLZ-level demographics, you must **spatially join** grid or municipality data to PLZ polygons.

### Available Free Data Sources

#### 1. Zensus 2022 (Best source for fine-grained demographics)

| Data | Granularity | URL |
|------|-------------|-----|
| Population by age, gender | 100m grid, 1km grid, Gemeinde | [atlas.zensus2022.de](https://atlas.zensus2022.de) |
| Migration background / nationality (EU vs non-EU) | 100m grid, Gemeinde | [ergebnisse.zensus2022.de](https://ergebnisse.zensus2022.de) |
| Education level | Gemeinde | Zensus 2022 database |
| Housing/rent levels | 100m grid | Zensus 2022 grid downloads |

- R package `z22` provides programmatic access: [jslth.github.io/z22](https://jslth.github.io/z22/)
- 100m grid data is ~35M cells — requires GIS tools, not Excel

#### 2. Regionalstatistik.de

| Data | Granularity | URL |
|------|-------------|-----|
| Population, demographics | Gemeinde, Kreis | [regionalstatistik.de](https://www.regionalstatistik.de) |
| Taxable income (Einkommensteuerstatistik) | Kreis, some Gemeinde | Same portal |
| Education levels | Kreis | Same portal |
| Employment | Gemeinde | Same portal |

- Has a RESTful API (free registration required)
- R package: [restatis](https://github.com/CorrelAid/restatis)

#### 3. INKAR (Indicators for Spatial/Urban Development)

- ~600 indicators covering demographics, labor market, income, education, transport, housing
- Granularity: Kreise and Gemeindeverbände
- Free CSV/Excel/PDF export
- URL: [inkar.de](https://www.inkar.de)

#### 4. PLZ Population (basic)

- `plz_einwohner.csv` from [suche-postleitzahl.org](https://www.suche-postleitzahl.org/downloads)
- Total population per PLZ only (no age/gender breakdown)
- Derived from Zensus 2011 grid data

### PLZ Boundary Shapefiles (Free)

| Source | Format | License | URL |
|--------|--------|---------|-----|
| suche-postleitzahl.org | Shapefile (.shp) | ODbL | [downloads](https://www.suche-postleitzahl.org/downloads) |
| GitHub: yetzt/postleitzahlen | GeoJSON, TopoJSON | ODbL | [github.com/yetzt/postleitzahlen](https://github.com/yetzt/postleitzahlen) |
| Kaggle | Shapefile | ODbL | [kaggle.com/datasets/jonaslneri/germany-plz](https://www.kaggle.com/datasets/jonaslneri/germany-plz) |

### Administrative Boundary Shapefiles

- **BKG VG250**: Official boundaries for Länder → Regierungsbezirke → Kreise → Gemeinden
- Download: [gdz.bkg.bund.de](https://gdz.bkg.bund.de/index.php/default/digitale-geodaten/verwaltungsgebiete.html)
- License: Datenlizenz Deutschland 2.0 (free with attribution)
- VG250-EW variant includes population figures

### Income Data (the gap)

- **Not freely available at PLZ level from official sources**
- Closest free option: taxable income at Kreis/Gemeinde level from Regionalstatistik
- Commercial providers for PLZ-level income:
  - GfK/NIQ Purchasing Power ([shop.gfk-geomarketing.de](https://shop.gfk-geomarketing.de))
  - Nexiga ([nexiga.com](https://nexiga.com/en/daten/kaufkraftkarte/))
  - Michael Bauer International ([mbi-geodata.com](https://www.mbi-geodata.com))
  - WIGeoGIS ([wigeogis.com](https://www.wigeogis.com/en/demographics_germany_data))

### Recommended Strategy for PLZ-Level Demographics

1. Download **Zensus 2022 100m grid data** (population, demographics, migration, housing)
2. Download **PLZ boundary polygons** from suche-postleitzahl.org
3. **Spatial join** grid cells → PLZ polygons using PostGIS or geopandas
4. **Aggregate** grid-level indicators to PLZ level (weighted by population)

This gives you: population by age/gender, migration background %, housing density/type — all at PLZ level, entirely from free open data.

---

## Part C: Estimation Strategy for School Performance

### The Problem

We have detailed school performance data (Abitur averages, MSA scores, completion rates) for Berlin's ~300 secondary schools, but this data is unavailable for the ~4,000+ secondary schools across the rest of Germany.

### The Hypothesis

School performance is strongly correlated with **catchment area demographics** and **surrounding environment**. If we can model this relationship using Berlin data (where we have both performance AND demographics), we can estimate performance for schools in other cities where we only have demographics.

### Proposed Approach: Transfer Learning via Catchment Area Features

#### Step 1: Build a Feature Set for Berlin Schools (Training Data)

For each Berlin secondary school, compute features from its catchment area:

**Demographic features** (from Zensus 2022 grid data within catchment):
- Population density
- Age distribution (% children, % working age, % elderly)
- Migration background %
- Average household size

**Socioeconomic features** (from Regionalstatistik/INKAR at Gemeinde/Kreis level):
- Average taxable income (or proxy from housing data)
- Education level of adult population (% with Abitur, % with university degree)
- Employment rate / unemployment rate
- Average rent levels (from Zensus grid — proxy for affluence)

**POI features** (from OpenStreetMap / project's existing collection):
- Density of libraries, bookstores, cultural institutions within radius
- Density of parks/green spaces
- Public transport accessibility (stops within radius)
- Proximity to universities
- Density of social services / Jugendamt offices
- Commercial environment (restaurants, shops — economic activity proxy)

**School-specific features** (from official school directories):
- School type (Gymnasium, ISS/Gesamtschule, etc.)
- Public vs. private
- School size (student count)
- Student-teacher ratio
- Whether it has a specific profile (MINT, language, arts)
- Whether it has a Ganztagsangebot (full-day program)

#### Step 2: Train a Model on Berlin Data

- Target variable: Abitur average, Abitur pass rate, MSA score
- Features: All of the above
- Model options: Gradient boosted trees (XGBoost/LightGBM), Random Forest, or even linear regression
- Validation: k-fold cross-validation within Berlin to measure accuracy
- Hamburg data can serve as an **out-of-sample validation set** (since Hamburg also publishes per-school Abitur data)

#### Step 3: Apply to Other Cities

For each secondary school in Germany:
1. Get school location from official school directories (most states publish school lists with addresses)
2. Compute the same feature set using Zensus + OSM + available data
3. Apply the trained model to estimate Abitur average / MSA score

#### Step 4: Confidence & Transparency

- Report predictions with **confidence intervals**, not point estimates
- Show users which features contributed most (SHAP values)
- Clearly label estimated vs. actual performance data
- Flag schools where the model might be less reliable (e.g., very different from Berlin training data)

### Key Advantages

1. **Hamburg as validation**: We can validate the model on Hamburg data before applying it elsewhere
2. **Zensus 2022 is nationwide**: The same demographic data is available everywhere in Germany
3. **OSM POIs are nationwide**: OpenStreetMap coverage is excellent across Germany
4. **School directories exist everywhere**: Every state publishes at least basic school lists with locations

### Key Risks

1. **Berlin may not be representative**: Berlin's school system (ISS + Gymnasium) differs from other states
2. **Catchment areas differ**: Berlin has formal enrollment areas; other cities may not
3. **Income data gap**: Without PLZ-level income, socioeconomic proxies (rent, housing) must substitute
4. **Model accuracy**: Even with good features, prediction error could be significant

### Alternative: Crowdsourced / Survey-Based Augmentation

As a complement to the ML approach:
- Allow parents to self-report known school performance
- Partner with school review platforms
- Use IQB-Bildungstrend state-level data as calibration anchors

---

## Part D: Adapting GreenspaceFinder's Architecture for SchoolNossa

### The Core Insight

GreenspaceFinder and SchoolNossa's expansion solve structurally identical problems:

| Concept | GreenspaceFinder | SchoolNossa |
|---------|-----------------|-------------|
| **Entity** | Retail store | School |
| **Question** | "How good is this location for a new store?" | "How good is this school likely to perform?" |
| **Method** | Profile catchment area with multiple dimensions | Profile catchment area with multiple dimensions |
| **Scoring** | Rules-based + optional regression | Same approach applies |
| **Training data** | Existing stores with known performance | Berlin schools with known Abitur/MSA |
| **Prediction targets** | Candidate locations without performance data | Schools in other states without published data |

### What We Can Reuse Directly

#### 1. Catchment Profiling Engine (GSF Steps 5-6 → SchoolNossa)

GreenspaceFinder's `step5_profile.py` builds a `CatchmentProfile` for each location by querying multiple data sources within a radius. SchoolNossa needs the exact same pattern:

**GSF dimensions we keep as-is:**
- `transit_count` / `transit_nearest_m` — public transport accessibility matters for school choice
- `population_density` / `catchment_population` — enrollment pool
- `crime_index` — neighborhood safety (parents care deeply about this)

**GSF dimensions we adapt:**
- `competitor_count` → `other_schools_count` (schools of same type within catchment)
- `competitor_nearest_m` → `nearest_same_type_m` (distance to nearest competing school)
- `retail_count` → `cultural_pois` (libraries, museums, bookstores — educational environment proxy)

**New dimensions specific to schools:**
- `adult_education_level` — % of adults with Abitur/university degree in catchment (from Zensus 2022)
- `migration_background_pct` — % with migration background (from Zensus 2022 grid)
- `avg_rent_level` — housing cost as socioeconomic proxy (from Zensus 2022 grid)
- `youth_population_pct` — % aged 6-18 in catchment (from Zensus 2022 grid)
- `unemployment_rate` — from Regionalstatistik at Gemeinde/Kreis level
- `green_space_density` — parks/playgrounds within catchment (from OSM)
- `university_proximity` — distance to nearest university (higher education aspiration proxy)

#### 2. Scoring Pipeline (GSF Step 7 → SchoolNossa)

GreenspaceFinder's `step7_score.py` has two scoring modes that map perfectly:

**Rules-based scoring** (works immediately, no training data needed):
```
school_score = Σ(weight[d] × normalized[d]) / Σ(weights)
```
- Parents set weights based on what they value (safety vs. academics vs. accessibility)
- Dimensions marked "lower_better" (crime, distance to transit) are inverted
- This gives a **parent-preference-weighted comparison** — useful even without performance estimation

**Regression scoring** (the ML estimation):
- Training data: Berlin schools where we have both catchment profiles AND actual Abitur/MSA scores
- Features: All catchment dimensions above
- Target: `abitur_average_grade`, `abitur_success_rate`, or `msa_score`
- GSF's greedy feature selection approach (add features that improve R² by ≥ 0.01) is directly applicable
- Hamburg schools serve as out-of-sample validation (GSF equivalent: holding out some stores)

#### 3. Cannibalization Filter → Enrollment Competition Filter (GSF Step 8)

GSF's `step8_filter.py` prevents recommending clustered locations. We adapt this concept:
- Instead of filtering candidates, we use it to **detect enrollment competition zones**
- Schools within overlapping catchment areas compete for the same students
- This affects demand metrics and helps parents understand which schools are realistic options based on their address

#### 4. Rate-Limited API Client (GSF `google_places.py`)

GreenspaceFinder's async httpx client with semaphore-based rate limiting, retry logic, and caching is directly reusable for:
- Google Nearby Search (POI dimensions)
- Zensus API calls (if using the API instead of bulk downloads)
- State school directory APIs

#### 5. Local Data Services Pattern (GSF `crime.py`, `traffic.py`, `population.py`)

GSF bundles Berlin-specific JSON files with lookup services. SchoolNossa already has a similar pattern planned. We extend it:

| GSF Service | SchoolNossa Equivalent | Data Source |
|-------------|----------------------|-------------|
| `crime.py` (Berlin Bezirke) | `crime.py` (expand to all cities) | BKA Polizeiliche Kriminalstatistik + city open data |
| `population.py` (Berlin PLZ) | `demographics.py` (Zensus 2022 grid nationwide) | Zensus 2022 100m grid |
| `traffic.py` (Berlin sensors) | Drop or replace with `accessibility.py` | GTFS transit data / OSM |

### What We Build Differently

#### 1. No H3 Grid Generation Needed

GSF generates candidate locations on a hex grid because it's finding *new* locations. SchoolNossa already knows where every school is — we just need to profile each school's catchment area. This eliminates GSF Steps 1, 3, 6 (grid generation, candidate filtering).

#### 2. Nationwide School Directory as Input (replaces GSF Step 2-3)

Instead of discovering entities via Google Places, SchoolNossa ingests school locations from official state directories:

| State | School Directory Source |
|-------|----------------------|
| Berlin | WFS endpoint (already implemented) |
| Hamburg | [geoportal-hamburg.de](https://geoportal-hamburg.de) school data |
| Bayern | [km.bayern.de Schulsuche](https://www.km.bayern.de/ministerium/schule-und-ausbildung/schulsuche.html) |
| NRW | [schulministerium.nrw](https://www.schulministerium.nrw/BiPo/SVS/schulsuche) |
| All states | KMK Schulverzeichnis or individual state portals |

Each state directory provides: school name, type, address, coordinates (or geocodable addresses), and often additional metadata (profiles, programs, student counts).

#### 3. Two-Phase User Experience

**Phase 1 — Rules-based comparison** (no ML, works for any city):
- Parent enters a city or PLZ
- System shows all secondary schools in the area
- Each school has a catchment profile (transit, safety, demographics, POIs)
- Parent adjusts dimension weights → schools re-rank in real time
- This is valuable even without performance estimation

**Phase 2 — Performance estimation** (ML, Berlin-trained):
- Same schools now also show an "estimated Abitur average" with confidence interval
- Clearly labeled as "estimated based on catchment area analysis"
- Feature importance shown (what drives the estimate)
- Berlin/Hamburg schools show actual data instead of estimates

### Proposed SchoolNossa Dimensions (Full List)

| Key | Label | Direction | Data Source | GSF Equivalent |
|-----|-------|-----------|-------------|----------------|
| `school_type` | School type | Categorical | State directory | — |
| `public_private` | Public/Private | Categorical | State directory | — |
| `student_count` | Student count | Informational | State directory | — |
| `transit_count` | Transit stops nearby | Higher is better | Google / GTFS | `transit_count` |
| `transit_nearest_m` | Nearest transit stop | Lower is better | Google / GTFS | `transit_nearest_m` |
| `crime_index` | Area crime rate | Lower is better | BKA / city data | `crime_index` |
| `catchment_population` | Catchment population | Informational | Zensus 2022 | `catchment_population` |
| `population_density` | Population density | Informational | Zensus 2022 | `population_density` |
| `youth_pct` | Youth population % | Higher is better | Zensus 2022 grid | — (new) |
| `migration_pct` | Migration background % | Informational | Zensus 2022 grid | — (new) |
| `adult_abitur_pct` | Adults with Abitur % | Higher is better | Zensus 2022 | — (new) |
| `avg_rent` | Average rent level | Informational | Zensus 2022 grid | — (new) |
| `other_schools_count` | Competing schools | Informational | State directory | `competitor_count` |
| `nearest_school_m` | Nearest same-type school | Informational | State directory | `competitor_nearest_m` |
| `library_count` | Libraries nearby | Higher is better | OSM / Google | — (new) |
| `park_count` | Parks/playgrounds nearby | Higher is better | OSM / Google | — (new) |
| `university_nearest_m` | Nearest university | Informational | OSM / Google | — (new) |
| `green_space_m2` | Green space area | Higher is better | OSM | — (new) |

### Implementation Phases

**Phase 0 (current):** Berlin school directory + basic metrics via REST API ✅

**Phase 1: Catchment Profiling Engine**
- Port GSF's profiling pattern to SchoolNossa
- Implement demographic dimensions using Zensus 2022 bulk data
- Implement POI dimensions using Google Nearby Search (reuse GSF client)
- Build catchment profiles for all Berlin schools
- Store profiles in `school_catchment_profile` table

**Phase 2: Rules-Based Scoring + Dashboard**
- Port GSF's normalization and weighted scoring
- Build parent-facing dashboard with dimension weights
- Add map view with school locations + catchment overlays
- Enable real-time re-ranking as parents adjust weights

**Phase 3: ML Performance Estimation**
- Collect Berlin Abitur/MSA data (sekundarschulen-berlin.de scraper)
- Train regression model: catchment features → Abitur average
- Validate on Hamburg data
- Add "estimated performance" to school profiles with confidence intervals

**Phase 4: Germany-Wide Expansion**
- Ingest school directories from additional states (start with Hamburg, Bayern, NRW)
- Compute catchment profiles for all ingested schools
- Apply trained model to estimate performance
- Scale Zensus data processing (100m grid → PostGIS spatial joins)

---

## Data Portal Quick Reference

| Portal | URL | Key Data |
|--------|-----|----------|
| Zensus 2022 Atlas | [atlas.zensus2022.de](https://atlas.zensus2022.de) | 100m grid demographics |
| Zensus 2022 Database | [ergebnisse.zensus2022.de](https://ergebnisse.zensus2022.de) | Gemeinde-level tables |
| Regionalstatistik | [regionalstatistik.de](https://www.regionalstatistik.de) | Income, education, employment by Kreis/Gemeinde |
| INKAR | [inkar.de](https://www.inkar.de) | 600+ spatial indicators |
| GENESIS-Online | [www-genesis.destatis.de](https://www-genesis.destatis.de) | Destatis full database |
| GovData | [govdata.de](https://www.govdata.de) | Central open data portal |
| BKG Geodaten | [gdz.bkg.bund.de](https://gdz.bkg.bund.de) | Administrative boundary shapefiles |
| suche-postleitzahl.org | [suche-postleitzahl.org](https://www.suche-postleitzahl.org/downloads) | PLZ shapefiles + population |
| Berlin Open Data | [daten.berlin.de](https://daten.berlin.de) | School performance, city data |
