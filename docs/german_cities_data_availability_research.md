# German Cities School Data Availability Research

**Research Date:** February 2026
**Purpose:** Identify cities where we can expand our school data asset with minimal effort

---

## Executive Summary

After thorough research of 8 major German cities, the findings reveal significant variation in public school data accessibility across German federal states (Bundesländer). The key insight is that **data availability is primarily determined at the state level**, not the city level.

### Quick Recommendation

| Priority | City | State | Effort Level | Key Advantage |
|----------|------|-------|--------------|---------------|
| 🥇 **1st** | **Cologne/Düsseldorf** | NRW | **LOW** | Best open data infrastructure in Germany; daily-updated CSV with coordinates |
| 🥈 **2nd** | **Leipzig/Dresden** | Saxony | **LOW-MEDIUM** | Excellent API (JSON) with coordinates; strong open data culture |
| 🥉 **3rd** | **Frankfurt** | Hessen | **MEDIUM-HIGH** | School database exists but no API; PDF-heavy |
| 4th | **Munich** | Bavaria | **HIGH** | No public school-level performance data; PDF crime data |
| 5th | **Stuttgart** | Baden-Württemberg | **HIGH** | No bulk download for schools; limited open data |

---

## Detailed City-by-City Analysis

### 1. COLOGNE & DÜSSELDORF (Nordrhein-Westfalen) ⭐ RECOMMENDED

**Why NRW is the best expansion target:**

| Data Category | Availability | Format | Source |
|---------------|-------------|--------|--------|
| School Directory | ✅ Excellent | CSV, XML (daily updates) | schulministerium.nrw/open-data |
| School Coordinates | ✅ Excellent | Gauss-Krüger coords in CSV | Same source |
| Student Numbers | ✅ Excellent | CSV per school | NRW Open Data |
| Schulsozialindex | ✅ Excellent | CSV (school-level social index) | ckan.open.nrw.de |
| Traffic Accidents | ✅ Excellent | CSV, Shapefile | opengeodata.nrw.de/unfallatlas |
| Crime by District | ⚠️ Moderate | PDF (district-level) | polizei.nrw |
| Teacher Statistics | ⚠️ Limited | Aggregate only (PDF/Excel) | schulministerium.nrw |
| MSA/Performance | ❌ Not available | Per-school not public | - |

**Key URLs:**
- School data CSV: `https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/schuldaten.csv`
- CKAN API: `https://ckan.open.nrw.de/`
- Cologne Open Data: `https://www.offenedaten-koeln.de/` (488+ datasets)
- Düsseldorf Open Data: `https://opendata.duesseldorf.de/`

**Unique advantage:** NRW publishes a **Schulsozialindex** - a school-level social challenge indicator that could substitute for some performance metrics.

**Effort estimate:** 1-2 weeks to replicate Berlin-level dataset (minus MSA data)

---

### 2. LEIPZIG & DRESDEN (Sachsen/Saxony) ⭐ RECOMMENDED

**Why Saxony is excellent:**

| Data Category | Availability | Format | Source |
|---------------|-------------|--------|--------|
| School Directory | ✅ Excellent | **JSON/XML API** | schuldatenbank.sachsen.de |
| School Coordinates | ✅ Excellent | Lat/Long in API | Same source |
| Student Numbers | ✅ Excellent | CSV, JSON | opendata.leipzig.de |
| Traffic Accidents | ✅ Excellent | CSV, Shapefile | unfallatlas.statistikportal.de |
| Crime by District | ⚠️ Moderate | PDF only | polizei.sachsen.de |
| Teacher Statistics | ⚠️ Moderate | Excel aggregate | statistik.sachsen.de |
| Performance Data | ❌ Not available | Per-school not public | - |

**Key URLs:**
- **School API**: `https://schuldatenbank.sachsen.de/` (request credentials via support@schuldatenbank.sachsen.de)
- API endpoints include `/schools`, `/schools/students/{year}`, `/schools/map`
- Leipzig Open Data: `https://opendata.leipzig.de/` (CSV/JSON)
- Leipzig Statistics API: `https://statistik.leipzig.de/opendata/api/values`
- Dresden Open Data: `https://opendata.dresden.de/` (1,117 datasets - 3rd most in Germany)

**Unique advantage:** The Sächsische Schuldatenbank has a **proper REST API with JSON export** including coordinates - superior to most German states.

**Effort estimate:** 2-3 weeks (API integration + crime PDF parsing)

---

### 3. FRANKFURT (Hessen)

**Assessment: Medium difficulty**

| Data Category | Availability | Format | Source |
|---------------|-------------|--------|--------|
| School Directory | ⚠️ Exists | Web only (no API) | schul-db.bildung.hessen.de |
| School Coordinates | ❌ Limited | Would need geocoding | - |
| Student Numbers | ⚠️ Moderate | PDF primarily | statistik.hessen.de |
| Traffic Accidents | ✅ Good | Via national Unfallatlas | destatis.de |
| Crime by District | ⚠️ Limited | PDF only, city-wide | polizei.hessen.de |
| Teacher Statistics | ⚠️ Moderate | PDF/Excel aggregate | statistik.hessen.de |
| Performance Data | ❌ Not available | Per-school not public | - |

**Key URLs:**
- School database: `https://schul-db.bildung.hessen.de/` (searchable but NO export)
- Frankfurt Open Data: `https://offenedaten.frankfurt.de/` (good portal, but NO school datasets)
- Hessen Open Data: `https://opendata.hessen.de/`

**Major limitation:** The Hessian school database has no API and no bulk download - would require scraping.

**Effort estimate:** 4-6 weeks (scraping + PDF extraction + geocoding)

---

### 4. MUNICH (Bayern/Bavaria)

**Assessment: High difficulty**

| Data Category | Availability | Format | Source |
|---------------|-------------|--------|--------|
| School Directory | ⚠️ Limited | CSV (basic) | opendata.muenchen.de |
| School Coordinates | ❌ Limited | Not in school dataset | Would need geocoding |
| Student Numbers | ✅ Good | CSV | opendata.muenchen.de |
| Traffic Accidents | ✅ Good | Via national Unfallatlas | destatis.de |
| Crime by District | ⚠️ PDF only | By Stadtbezirk | stadt.muenchen.de |
| Teacher Statistics | ⚠️ Moderate | Excel/GENESIS | statistik.bayern.de |
| **MSA/VERA Results** | ❌ **NOT PUBLIC** | Password-protected | las.bayern.de |

**Key URLs:**
- Munich Open Data: `https://opendata.muenchen.de/`
- Bavaria GENESIS: `https://www.statistikdaten.bayern.de/`
- School statistics: `https://www.statistik.bayern.de/statistik/bildung_soziales/schulen/`

**Critical limitation:** Bavaria does **NOT publish individual school performance data** publicly. VERA test results are password-protected for schools only. This is a fundamental difference from Berlin.

**Effort estimate:** 4-6 weeks, but **cannot match Berlin data richness** due to missing performance data

---

### 5. STUTTGART (Baden-Württemberg)

**Assessment: High difficulty**

| Data Category | Availability | Format | Source |
|---------------|-------------|--------|--------|
| School Directory | ⚠️ Web only | No bulk download | bewo.kultus-bw.de/schulfinder |
| School Coordinates | ⚠️ Via OSM | Shapefile/GeoJSON | geofabrik.de |
| Student Numbers | ✅ Good | CSV/Excel via GENESIS | daten.statistik-bw.de |
| Traffic Accidents | ⚠️ Limited | PDF reports | ppstuttgart.polizei-bw.de |
| Crime by District | ⚠️ PDF only | District-level | ppstuttgart.polizei-bw.de |
| Teacher Statistics | ⚠️ Aggregate | Excel/GENESIS | statistik-bw.de |
| Performance Data | ✅ Available | Aggregate only | ibbw-bw.de |

**Key URLs:**
- School finder (web only): `https://bewo.kultus-bw.de/,Lde/Startseite/schulfinder`
- BW GENESIS: `https://daten.statistik-bw.de/genesisonline/online`
- Stuttgart Open Data: `https://opendata.stuttgart.de/` (110 datasets, **NO education data**)
- BW Open Data: `https://www.daten-bw.de/`

**Critical limitation:** No official bulk download for school data. Would need to either:
1. Scrape the Schulfinder website, OR
2. Use OpenStreetMap data (less authoritative)

**Effort estimate:** 5-7 weeks (scraping required, PDF parsing for crime/traffic)

---

### 6. HAMBURG (Reference - Already Attempted)

**What we learned from Hamburg:**

| Data Category | Berlin | Hamburg | Gap |
|---------------|--------|---------|-----|
| School Directory | ✅ | ✅ | Same |
| Coordinates | ✅ | ✅ (GeoJSON) | Same |
| MSA Data | ✅ District-level | ❌ Not available | **Major gap** |
| Traffic | ~50% coverage | 5.9% coverage | **Major gap** |
| Crime | ✅ CSV by Bezirk | ~73% by Stadtteil | Moderate gap |
| Teacher Numbers | ✅ | ❌ Not collected | **Major gap** |

**Hamburg's limitations were due to:**
- Extremely sparse traffic sensor network (only 12 schools matched)
- No equivalent to Berlin's MSA district-level data
- Teacher data not reliably available via web scraping

---

## Comparison Matrix: All Cities vs. Berlin

| Data Type | Berlin | Cologne/Düsseldorf | Leipzig/Dresden | Frankfurt | Munich | Stuttgart |
|-----------|--------|-------------------|-----------------|-----------|--------|-----------|
| **School List** | ✅ CSV | ✅ CSV (daily) | ✅ JSON API | ⚠️ Web only | ⚠️ Basic CSV | ⚠️ Web only |
| **Coordinates** | ✅ | ✅ Built-in | ✅ Built-in | ❌ Geocode | ❌ Geocode | ⚠️ OSM |
| **MSA/Performance** | ✅ District | ❌ No | ❌ No | ❌ No | ❌ No | ❌ No |
| **Traffic** | ~50% | ✅ Unfallatlas | ✅ Unfallatlas | ✅ Unfallatlas | ✅ Unfallatlas | ⚠️ PDF |
| **Crime** | ✅ CSV | ⚠️ PDF | ⚠️ PDF | ⚠️ PDF | ⚠️ PDF | ⚠️ PDF |
| **Teacher Data** | ✅ | ⚠️ Aggregate | ⚠️ Aggregate | ⚠️ PDF | ⚠️ GENESIS | ⚠️ GENESIS |
| **Student Numbers** | ✅ | ✅ CSV | ✅ CSV/JSON | ⚠️ PDF | ✅ CSV | ✅ GENESIS |
| **Open Data Portal** | Excellent | Excellent | Very Good | Good | Moderate | Limited |

**Legend:** ✅ Easy/Available | ⚠️ Requires effort | ❌ Not available/Very difficult

---

## Key Insight: The MSA Problem

**Berlin is unique** in publishing Mittlerer Schulabschluss (MSA) exam results at the district level. No other German state publishes equivalent data publicly:

- **Bavaria:** VERA results password-protected
- **NRW:** No MSA equivalent published
- **Saxony:** No public performance data per school/district
- **Hessen:** Internal LUSD system, not public
- **Baden-Württemberg:** Aggregate state-level only

**Implication:** For any city outside Berlin, we cannot replicate the MSA performance metrics. We would need to either:
1. Accept this limitation and focus on other metrics
2. Use proxy indicators like the Schulsozialindex (NRW) or demographic data
3. Contact education ministries directly to request data sharing agreements

---

## Recommended Expansion Strategy

### Phase 1: Quick Wins (NRW - Cologne/Düsseldorf)
**Timeline:** 2-3 weeks
**Rationale:** Best infrastructure, CSV with coordinates, Schulsozialindex as performance proxy

**Data achievable:**
- ✅ Complete school directory with coordinates
- ✅ Student enrollment numbers
- ✅ School social index (proxy for challenges)
- ✅ Traffic accident analysis (Unfallatlas)
- ⚠️ Crime by police district (PDF parsing)
- ⚠️ Transit accessibility (similar methods)
- ⚠️ POI data (Google Places)

### Phase 2: API-First Approach (Saxony - Leipzig/Dresden)
**Timeline:** 3-4 weeks
**Rationale:** JSON API with coordinates; strong open data ecosystem

**Data achievable:**
- ✅ Complete school directory via API
- ✅ Geographic coordinates built-in
- ✅ Student/teacher aggregate statistics
- ✅ Traffic accident analysis
- ⚠️ Crime statistics (PDF parsing)
- ⚠️ Transit/POI enrichment

### Phase 3: Consider Carefully (Frankfurt, Munich, Stuttgart)
**Timeline:** 5-7 weeks each
**Rationale:** Higher effort, more manual work, still missing key metrics

**Only pursue if:**
- Specific business need for these cities
- Acceptance that dataset will be less rich than Berlin
- Resources available for scraping/geocoding

---

## Technical Notes

### Reusable Components from Berlin/Hamburg

| Component | Reusability | Notes |
|-----------|-------------|-------|
| Transit enrichment | High | Adapt for local transit APIs (DVB, VRS, MVV, etc.) |
| POI enrichment | High | Google Places API works anywhere |
| Crime enrichment | Medium | PDF parsing logic reusable; formats vary |
| Traffic (Unfallatlas) | High | National dataset, just filter by coordinates |
| School scraping | Low | Each state has different systems |

### National Data Sources (Apply to All Cities)

1. **Unfallatlas** (Traffic Accidents)
   - URL: `https://unfallatlas.statistikportal.de/`
   - Format: CSV, Shapefile
   - Coverage: All of Germany, 2016-2024

2. **Bildungsmonitoring.de** (Education Statistics)
   - URL: `https://www.bildungsmonitoring.de/`
   - District-level education data for all cities

3. **INKAR** (Regional Indicators)
   - URL: `https://www.inkar.de/`
   - Socioeconomic indicators by region

---

## Conclusion

**Best ROI for expansion:** NRW cities (Cologne, Düsseldorf) due to superior open data infrastructure and the Schulsozialindex as a unique value-add.

**Second best:** Saxony cities (Leipzig, Dresden) due to the excellent school database API.

**Avoid for now:** Munich, Stuttgart, Frankfurt - high effort with less data richness achievable compared to Berlin.

The fundamental limitation across all cities is the **lack of public school-level performance data** comparable to Berlin's MSA statistics. This is a policy difference, not a technical one, and would require direct engagement with state education ministries to overcome.
