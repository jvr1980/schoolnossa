# SchoolNossa Development Journal

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
