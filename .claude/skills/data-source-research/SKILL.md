---
name: data-source-research
description: "SchoolNossa project only. Research and document available open data sources for a new German city's school pipeline. Use whenever the user wants to explore what data exists for a city before coding, asks 'what data is available for Munich?', 'can we get crime data for Frankfurt?', 'research sources for Stuttgart', or any question about whether a specific data type (traffic, crime, transit, schools, demographics) is available as open data in a German city. Also trigger when the user asks which existing pipeline to base a new city on. Only applies when working in the schoolnossa repository."
allowed-tools: Bash, Read, Write, Glob, Grep, Agent, WebSearch, WebFetch
---

# Data Source Research for New City Pipeline

When adding a new German city, systematically research what open data is available before writing any code. This skill produces a structured assessment document.

## Required Input

Ask the user for:
1. **City name** (e.g. `Munich`, `Frankfurt`)
2. **State** (Bundesland — e.g. `Bayern`, `Hessen`)
3. **Whether primary, secondary, or both school types** are needed

## Research Procedure

For each data category below, search for and document:
- **Source URL** (direct download link or API endpoint)
- **Format** (CSV, JSON, GeoJSON, XLSX, PDF, API)
- **Encoding** (UTF-8, cp850, Latin-1)
- **Separator** for CSVs (comma, semicolon, tab)
- **Coordinate system** (WGS84/EPSG:4326, UTM EPSG:25832, etc.)
- **Update frequency** (annual, quarterly, real-time)
- **Access method** (free download, API key needed, scraping required)
- **Coverage** (city-level, district-level, school-level)

### Category 1: School Master Data

Research in this order:
1. **State education ministry open data portal** — most German states publish school lists via their Schulministerium or Kultusministerium. Search for `{state} Schuldaten Open Data` or `{state} Schulverzeichnis download`.
2. **City education authority** (Schulamt / Schulbehörde) — may have supplementary data.
3. **Fallback: Bildungsserver** — `bildungsserver.de` aggregates school directories.

Document: school name, address, coordinates, school type (Schulform), public/private status, website URL, contact info.

Check known patterns from existing cities:
- NRW: `schulministerium.nrw.de/BiPo/OpenData/` — semicolon-sep CSV, UTF-8, UTM EPSG:25832
- Hamburg: WFS GeoJSON from `geodienste.hamburg.de`
- Berlin: Multiple scrapers from `sekundarschulen-berlin.de` and `bildung.berlin.de`

### Category 2: Traffic Data

Two possible approaches:
1. **Traffic volume sensors** — search `{city} Verkehrszählung Open Data` or `{city} traffic count API`. Berlin and Hamburg have sensor APIs.
2. **Accident data (Unfallatlas)** — `unfallatlas.statistikportal.de` covers all of Germany. NRW uses this. Filter by ULAND code for the state.

Document: whether volume-based or accident-based, spatial resolution, temporal coverage (years available).

### Category 3: Crime Data (PKS — Polizeiliche Kriminalstatistik)

Research in this order:
1. **State-level PKS** — search `{state} PKS {year} download` or `Polizeiliche Kriminalstatistik {state}`.
2. **City police PKS** — search `{city} Polizei Kriminalstatistik` for city-level breakdowns.
3. **District-level (Bezirk/Stadtteil)** — the finest granularity available.

Classify the data:
- **Stadtteil-level** (like Hamburg: 107 districts) — BEST
- **Bezirk-level** (like Berlin: 12 districts) — GOOD
- **City-wide only** (like NRW: must estimate) — ACCEPTABLE, needs population weighting

Document: granularity, crime categories available, years covered, format.

### Category 4: Transit Data

Research in this order:
1. **Local transit authority GTFS feed** — search `{city} GTFS download` or `{transport_authority} Open Data`.
2. **Local GeoJSON / API** — some cities publish stop locations separately.
3. **Fallback: OpenStreetMap Overpass API** — always available, free, no key needed. Query `highway=bus_stop`, `railway=tram_stop`, etc. within city bounding box.

Document: data format, whether line/route info is included, stop types covered (bus, tram, U-Bahn, S-Bahn).

### Category 5: POI Data

**Google Places API (New)** is used for all cities — shared pattern. No city-specific research needed.

Document: confirm that `GOOGLE_PLACES_API_KEY` is available, note any city-specific POI categories to add.

### Category 6: Demographics & Social Index

Research:
1. **State Sozialindex** — e.g. NRW has Schulsozialindex per school. Search `{state} Sozialindex Schulen`.
2. **City Stadtteil-Profile** — search `{city} Stadtteilprofil Sozialstruktur`. Hamburg has excellent district profiles from statistik-nord.de.
3. **Zensus / Mikrozensus** — federal data from destatis.de, available at grid or district level.
4. **Migration statistics** — `{city} Migrationshintergrund Statistik`.

Document: granularity (school-level, district-level, city-level), indicators available (income, migration, welfare, unemployment).

### Category 7: Academic Performance

Research:
1. **Abitur results** — search `{city} Abitur Ergebnisse` or `{state} Abiturschnitt Schulen`.
2. **MSA / ZP10 results** — Mittlerer Schulabschluss or Zentrale Prüfungen.
3. **Demand/enrollment** — Anmeldezahlen, Einschulungszahlen. Search `{city} Anmeldezahlen Schulen {year}`.

Document: what level of detail is public (per-school, per-district, state average only).

### Category 8: Website & Metadata

- Confirm school websites are accessible (not behind login walls).
- Check if the city has a central school information portal.
- Note any robots.txt restrictions on school websites.

## Output Document

Write the research results to `docs/{city}_data_availability_research.md` with this structure:

```markdown
# {City} Data Availability Research

**Date:** {today}
**State:** {Bundesland}
**Researcher:** Claude + user

## Summary

| Category | Source | Granularity | Format | Access | Status |
|----------|--------|-------------|--------|--------|--------|
| School Master | ... | per-school | CSV | free | Ready |
| Traffic | ... | ... | ... | ... | ... |
| Crime | ... | ... | ... | ... | ... |
| Transit | ... | ... | ... | ... | ... |
| POI | Google Places | 500m radius | API | key needed | Ready |
| Demographics | ... | ... | ... | ... | ... |
| Academic | ... | ... | ... | ... | ... |
| Websites | ... | per-school | HTML | scraping | Ready |

## Detailed Findings

### 1. School Master Data
[Detailed findings with URLs, format notes, encoding, coordinate system]

### 2. Traffic Data
[...]

... (one section per category)

## Comparison with Existing Cities

| Aspect | Berlin | Hamburg | NRW | {City} |
|--------|--------|---------|-----|--------|
| School source | scrapers | WFS GeoJSON | Open Data CSV | ... |
| Traffic type | sensor volumes | sensor volumes | accident counts | ... |
| Crime granularity | Bezirk | Stadtteil | city-wide est. | ... |
| Transit source | Overpass API | HVV GeoJSON | Overpass API | ... |
| Demographics | per-school | per-Stadtteil | per-school | ... |
| Academic data | MSA + demand | Abitur | Anmeldezahlen | ... |

## Recommendations

- Which approach is closest to an existing city (determines which code to adapt)
- Any city-specific phases needed (like NRW's Anmeldezahlen)
- Potential blockers or data gaps
- Estimated effort level (1-5, where NRW was ~4)
```

## After Research

Once the document is complete, recommend which existing city's pipeline to use as the code template:
- **Copy from Berlin** if: school-level data available, sensor-based traffic, good crime granularity
- **Copy from Hamburg** if: GeoJSON-based school data, district-level demographics
- **Copy from NRW** if: state ministry Open Data CSV, accident-based traffic, city-wide crime needing estimation

---

## Evaluations

After the research document is complete, run these checks. ALL must pass.

### EVAL-1: Document Exists and Has All 8 Categories
```bash
test -f "docs/{city}_data_availability_research.md" && echo "PASS: File exists"
for cat in "School Master" "Traffic" "Crime" "Transit" "POI" "Demographics" "Academic" "Website"; do
  grep -q "$cat" "docs/{city}_data_availability_research.md" || echo "FAIL: Missing category $cat"
done
```
**Pass criteria:** File exists and all 8 categories are documented.

### EVAL-2: Summary Table Has All Rows
```bash
# The summary table should have at least 8 data rows (one per category)
grep -c "|" "docs/{city}_data_availability_research.md" | head -1
```
**Pass criteria:** Summary comparison table has entries for all 8 categories.

### EVAL-3: Every Source Has a URL or Explicit "N/A"
```bash
python3 -c "
with open('docs/{city}_data_availability_research.md') as f:
    content = f.read()
sections = content.split('### ')
for section in sections[1:]:  # Skip header
    name = section.split('\n')[0].strip()
    has_url = 'http' in section or '.de' in section or 'N/A' in section or 'not available' in section.lower()
    if not has_url:
        print(f'WARNING: Section \"{name}\" has no URL or explicit N/A')
print('EVAL-3 complete')
"
```
**Pass criteria:** Every detailed section contains either a URL or an explicit statement that data is not available.

### EVAL-4: Coordinate System Documented
```bash
grep -iE "(EPSG|WGS84|UTM|coordinate|coord)" "docs/{city}_data_availability_research.md" || echo "FAIL: No coordinate system documented"
```
**Pass criteria:** At least one mention of coordinate system (EPSG, WGS84, UTM).

### EVAL-5: Encoding Documented for CSV Sources
```bash
grep -iE "(UTF-8|utf8|cp850|latin|ISO-8859|encoding)" "docs/{city}_data_availability_research.md" || echo "FAIL: No encoding documented"
```
**Pass criteria:** Encoding is mentioned for at least one data source.

### EVAL-6: Comparison Table Includes All Existing Cities
```bash
for city in "Berlin" "Hamburg" "NRW"; do
  grep -q "$city" "docs/{city}_data_availability_research.md" || echo "FAIL: Missing comparison with $city"
done
```
**Pass criteria:** The comparison table references Berlin, Hamburg, and NRW.

### EVAL-7: Recommendation Section Exists and Names a Template City
```bash
grep -iE "(recommend|template|copy from|adapt from|closest)" "docs/{city}_data_availability_research.md" || echo "FAIL: No recommendation section"
```
**Pass criteria:** Document contains a recommendation for which existing city pipeline to use as template.

### EVAL-8: Format and Access Method Documented Per Source
```bash
python3 -c "
with open('docs/{city}_data_availability_research.md') as f:
    content = f.read()
formats_found = sum(1 for fmt in ['CSV', 'JSON', 'GeoJSON', 'XLSX', 'PDF', 'API', 'XML'] if fmt in content)
access_found = sum(1 for acc in ['free', 'API key', 'scraping', 'download', 'open data'] if acc.lower() in content.lower())
print(f'Formats mentioned: {formats_found}, Access methods mentioned: {access_found}')
assert formats_found >= 3, 'FAIL: Fewer than 3 data formats documented'
assert access_found >= 2, 'FAIL: Fewer than 2 access methods documented'
print('PASS')
"
```
**Pass criteria:** At least 3 data formats and 2 access methods documented.

### EVAL-9: No Stale or Placeholder Content
```bash
grep -iE "(TODO|TBD|FIXME|placeholder|lorem|xxx)" "docs/{city}_data_availability_research.md" && echo "FAIL: Contains placeholder content" || echo "PASS: No placeholders"
```
**Pass criteria:** No TODO/TBD/placeholder text remains in the final document.

### EVAL-10: Effort Estimate Included
```bash
grep -iE "(effort|estimate|complexity|difficulty|level)" "docs/{city}_data_availability_research.md" || echo "FAIL: No effort estimate"
```
**Pass criteria:** Document includes an effort or complexity estimate for the implementation.
