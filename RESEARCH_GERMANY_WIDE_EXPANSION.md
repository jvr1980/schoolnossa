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
