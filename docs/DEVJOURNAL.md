# SchoolNossa Development Journal

## 2026-04-05 — Hamburg Primary School Pipeline Built

**What:** Created a complete primary school (Grundschule) pipeline for Hamburg, following the Berlin pattern of separate `scripts_hamburg_primary/` and `data_hamburg_primary/` directories.

**Why:** The existing Hamburg pipeline only covered secondary schools (Stadtteilschulen + Gymnasien). The same WFS data source contains standalone Grundschulen that were filtered out.

**Key results:**
- **259 Grundschulen** extracted (230 state + 29 private), 100% coordinate coverage
- Data source: same Transparenzportal WFS (`HH_WFS_Schulen`) — the `schulform` column distinguishes primary from secondary
- Filter: `schulform` contains "Grundschule" AND does NOT contain "Stadtteilschule"/"Gymnasium"
- 8-phase pipeline: Scraper → Traffic → Crime → Transit → POI → Combiner → Embeddings → Berlin Schema
- No Abitur or website scraping phases (not applicable to Grundschulen)
- Scripts in `scripts_hamburg_primary/` with orchestrator
- Distribution by Bezirk: Wandsbek (58), Altona (46), Eimsbüttel (39), Hamburg-Nord (38), Hamburg-Mitte (35), Harburg (23), Bergedorf (20)

**Branch:** `feature/munich-pipeline` (continuation of existing branch)

## 2026-04-01 — Munich Secondary School Pipeline Scaffolded and Implemented

**What:** Built the complete Munich (Bayern) secondary school data pipeline — all 9 phases from research through Berlin schema enforcement. This is the 5th city in the SchoolNossa platform.

**Why:** Expanding SchoolNossa coverage to Munich, Germany's third-largest city.

**Key results:**
- Research document: `docs/munich_data_availability_research.md` (8 data categories assessed)
- Orchestrator + 9 phase scripts in `scripts_munich/`
- Data sources: Schulsuche CSV (ISO-8859-15), Unfallatlas (ULAND=09), Overpass API transit, city-level PKS crime, Google Places POI
- Munich-specific: no coordinates in school CSV (needs geocoding via jedeschule.codefor.de + Nominatim)
- Notable limitation: Bavaria does NOT publish per-school academic performance data (VERA password-protected)
- Crime: Munich is Germany's safest major city for 50 consecutive years (HZ 7,684/100k vs Frankfurt 14,840)
- Template city: NRW pipeline pattern (CSV-based schools, Unfallatlas traffic, district-level crime estimation)
- Pipeline ready to run but requires: geocoding step execution, Google Places API key, Gemini/OpenAI API keys for descriptions and embeddings

**Branch:** `feature/munich-pipeline`

<!-- NEW ENTRIES GO ABOVE THIS LINE -->

## 2026-03-30 — Frankfurt Pipeline Built End-to-End

**What:** Built the complete Frankfurt am Main school data pipeline from scratch — all 8 phases from data source research through Berlin schema enforcement.

**Why:** Expanding SchoolNossa coverage to Frankfurt (Hessen), the fifth largest city in Germany.

**Results:**
- **85 primary schools** + **73 secondary schools** = **158 total**
- Data source: Hessen Statistisches Landesamt Verzeichnis 6 (Excel with multi-level headers)
- Coordinates: jedeschule.codefor.de + Nominatim geocoding fallback → 100% coverage
- All enrichments at 100% coverage: transit (Overpass), traffic (Unfallatlas ULAND=06/UKREIS=12), crime (city-level PKS), POI (Google Places), embeddings (Gemini 3072d)
- Berlin schema alignment: PASS for both primary and secondary
- QA reports generated: both PASS

**Key decisions:**
- Hessen doesn't publish Stadtteil-level crime data — used city-level aggregate (documented limitation)
- Hessen doesn't publish school-level Abitur/MSA data — academic performance columns are NULL
- Used ndH (non-German native language) count from Verzeichnis 6 as belastungsstufe proxy
- 38 secondary schools classified as "Weiterführende Schule" (Förderschulen, Abendschulen, etc.) rather than forcing them into Gymnasium/Realschule/Gesamtschule categories
- Fixed `Schul-nummer` → `schulnummer` column rename in data combiner (Hessen-specific hyphenation)

**Files created:**
- `scripts_frankfurt/` — Full pipeline (scraper, 4 enrichments, combiner, embeddings, schema transformer, orchestrator)
- `data_frankfurt/` — Raw, intermediate, final, cache directories with all output files
- `docs/FRANKFURT_DATA_SOURCES_RESEARCH.md` — Data availability research
- `data_frankfurt/final/QA_REPORT_*.md` — QA validation reports
