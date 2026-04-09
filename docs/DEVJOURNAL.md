# SchoolNossa Development Journal

## 2026-04-09 — Bremen Pipeline: Full Run + Descriptions + Tuition

**What:** Ran the complete Bremen pipeline end-to-end, including description generation (Perplexity+OpenAI) for secondary schools and tuition extraction for private schools. Generated QA report and schema drift report.

**Why:** All enrichment phases needed to be executed to produce a production-ready dataset for the frontend.

**Results:**
| Metric | Value |
|--------|-------|
| Total schools | 253 (113 primary, 65 secondary, 75 other) |
| Coordinates | 252/253 (99.6%) |
| Traffic (Unfallatlas) | 250/252 schools, 7,809 accidents across 3 years |
| Transit (Overpass) | 7,194 stops, avg 14.1 within 500m |
| Crime (PKS) | 206/253 matched (81%), 22 Beiratsbereiche |
| POI (Google Places) | 252 schools, 83 columns, 8 categories, 2,751 API calls |
| Descriptions (secondary) | 59/65 with bilingual DE+EN (Perplexity+OpenAI) |
| Descriptions (primary) | In progress via shared pipeline |
| Tuition (secondary) | 3 private: FEBB €150, Mentor €355, FGS €75-450 |
| Tuition (primary) | 3 private: 2 high tier, 1 low tier |
| Embeddings | 253/253 (text-embedding-3-large, 3072-dim) |
| Berlin schema | 265 columns match exactly |
| QA | 9/10 checks OK |

**Key fixes during pipeline run:**
- Scraper column mapping: `Name1`→`schulname`, `Planbezirk`→`stadtteil`, `Region`→`bezirk`, `Internet`→`website`
- Crime enrichment dtype: added `pd.to_numeric()` before `.rank()/.round()` to fix object-type errors
- Website metadata: added config.yaml key loading for Gemini API key

## 2026-04-08 — Bremen Pipeline: Implementation (All 9 Phases)

**What:** Added Bremen as a new city to SchoolNossa. Completed all workflow phases: research, scaffold, and full implementation of all 9 pipeline scripts.

**Why:** Bremen is a city-state (~200 schools) with good open data availability. Expands SchoolNossa coverage to 5 cities (Berlin, Hamburg, NRW, Munich, Bremen).

**Technical approach:**
- **Phase 1 (Research):** Documented 8 data categories. Key sources: Schulwegweiser Excel (bildung.bremen.de) + GeoBremen Shapefile (EPSG:25832) for school master data, Unfallatlas for traffic, Overpass API for transit, parliamentary PDFs for crime (22 Beiratsbereiche).
- **Phase 2 (Scaffold):** Combined pipeline (all school types together, like Hamburg). NRW as template city.
- **Phase 3 (Implementation):** All 9 phases implemented:
  - Scraper: Downloads Excel + Shapefile, converts EPSG:25832→WGS84, joins sources, geocodes missing via Nominatim
  - Traffic: Unfallatlas ULAND=04 (identical to NRW pattern)
  - Transit: Overpass API with bbox splitting (identical to NRW pattern)
  - Crime: Hardcoded 22 Beiratsbereiche data with tabula-py PDF parsing fallback, Stadtteil→Beirat mapping
  - POI: Google Places API (New), 8 categories, threaded
  - Website: Gemini + Google Search grounding for metadata extraction
  - Combiner, Embeddings (OpenAI+Gemini fallback), Schema Transformer

**Key files:**
- `docs/bremen_data_availability_research.md`
- `scripts_bremen/Bremen_school_data_asset_builder_orchestrator.py`
- `scripts_bremen/{scrapers,enrichment,processing}/` (10 Python scripts)
- `scripts_bremen/bremen_to_berlin_schema.py`

## 2026-04-07 — Munich Primary School Pipeline: 148 Grundschulen

**What:** Built the complete Munich primary school (Grundschule) pipeline by refactoring all 11 existing secondary-only scripts to support a `school_type` parameter. Ran all 9 phases producing 148 fully enriched Grundschulen.

**Why:** Munich only had a secondary school pipeline (108 schools). Primary schools were out of scope. Adding them brings Munich to 256 total schools.

**Technical approach:**
- Refactored all scripts (scraper, 5 enrichment scripts, combiner, embeddings, schema transformer, orchestrator) to accept `school_type='primary'|'secondary'`
- jedeschule.codefor.de used as data source — filters for `Grundschulen` type patterns
- Orchestrator now runs both types by default: `--school-types primary,secondary`
- All file paths parameterized: `munich_{school_type}_schools_with_{enrichment}.csv`
- Added OSM Overpass-based private school detection (code written, not yet wired in)

**Results (primary):**
| Field | Coverage |
|---|---|
| Schools | 148 (147 Grundschulen + 1 Grundschule) |
| Coordinates | 148/148 (100%) |
| Traffic (Unfallatlas) | 148/148, avg 61.8 accidents/500m |
| Transit (Overpass) | 148/148, avg score 93.7/100 |
| Crime (PKS city-level) | 148/148, HZ=7684/100k |
| POI (Google Places) | 148/148, 1197 API calls |
| Descriptions (Gemini) | 148/148 bilingual DE+EN |
| Embeddings (OpenAI) | 148/148, 3072-dim |
| Berlin Schema | PASS, 153/265 columns |

**Known gap:** jedeschule.codefor.de has zero private schools for Munich. Private school detection via OSM is implemented but not yet integrated. Tuition pipeline pending traegerschaft tagging.

## 2026-04-07 — Stuttgart Pipeline: Full City Build (95 Primary + 80 Secondary)

**What:** Built the complete Stuttgart school data pipeline from scratch — scraper, 4 enrichments, descriptions, tuition, embeddings, and Berlin schema alignment.

**Data source selection:**
- **Primary source:** `stuttgart.de/organigramm/adressen` — official city directory with 258 school entries. Scraped via RSS feed (790 URLs) → detail page scraping (JSON metadata + HTML fields). Provides: name, coordinates (WKT), phone, email, Schulart, Stadtbezirk, website.
- **Supplementary:** LOBW Dienststellensuche (`lobw.kultus-bw.de/didsuche/`) — scraped via ASMX web service API for student/teacher/class counts. jedeschule.codefor.de for principal names.
- **Rejected:** City PDFs (poster layout, unparseable), jedeschule as primary (inflated BW data with duplicates), Statistisches Landesamt (€101 paywall).

**Pipeline phases (13 total):**
1. School data scrape (stuttgart.de directory) → 95 primary + 80 secondary
2. Traffic (Unfallatlas BW, ULAND=08) → 100%
3. Transit (Overpass API, 2068 stops) → 100%
4. Crime (PKS Stuttgart 2023, bezirk-level estimates) → 100%
5. POI (Google Places API) → 100%
6. Data combiner → 178 columns
7. Embeddings (Gemini gemini-embedding-001, 768d) → 100%
8. Berlin schema enforcement → PASS
10. Descriptions (Perplexity Pass 0 + OpenAI Pass 1+2) → 100% EN/DE
11-13. Tuition (Gemini Pass 1+2, GPT-5.2 Pass 3) → 17/17 private schools

**Final coverage:**
| Field | Primary (95) | Secondary (80) |
|---|---|---|
| Coordinates | 100% | 100% |
| Phone | 99% | 100% |
| Email | 92% | 95% |
| Website | 100% | 100% |
| Schulleitung | 54% | 60% |
| Schülerzahl | 68% | 84% |
| Lehrerzahl | 63% | 69% |
| Description EN/DE | 100% | 100% |
| Besonderheiten | 91% | 96% |
| Transit/Traffic/Crime/POI | 100% | 100% |
| Tuition (private) | 5/5 | 12/12 |
| Embeddings | 100% | 100% |
| Berlin schema | PASS | PASS |

**Key technical decisions:**
- Used LOBW ASMX web service API (`SearchDienststellen` + `GetDienststelle`) — undocumented but stable, returns SCHUELER/KLASSEN/LEHRER per school
- Stuttgart crime: bezirk-level estimates using PKS city totals × district crime indices × district population
- Tuition Pass 3 results: 8/12 secondary private schools confirmed income-based (Waldorf + Evangelische), 4/12 flat-fee

**Files added:**
- `scripts_stuttgart/` — 13 pipeline scripts (scraper, 4 enrichments, combiner, embeddings, schema, orchestrator)
- `data_stuttgart/final/` — 6 parquet + 6 CSV final outputs
- `data_stuttgart/intermediate/` — enrichment chain CSVs
- `data_stuttgart/cache/` — LOBW, jedeschule, Unfallatlas, transit stops, description/tuition caches

---

## 2026-04-07 — Frankfurt POI Gap Fixed: 49% → 99% + Full Clean Pipeline Run

**What:** Fixed a persistent secondary school POI coverage gap (49/99 → 98/99 schools), updated Berlin schema canonical backfills for school stats, and ran the full Frankfurt pipeline to produce clean final output.

**Root cause of POI gap:** `frankfurt_poi_enrichment.py` always read from `with_crime.csv` (no POI data) regardless of whether a `with_pois.csv` already existed. On a checkpoint-based re-run, only newly processed schools were written to the output — losing all previously enriched data. The same 26 alphabetically-first schools (indices 0–25) were consistently skipped because they'd been in a stale checkpoint from a partial prior run, and the next run overwrote the output with only the new batch.

**Fixes:**
1. **POI enrichment input fallback** — added `with_pois.csv` as highest-priority input source (before `with_crime.csv`), so partial results are preserved across re-runs
2. **Already-enriched skip logic** — schools with non-null `poi_supermarket_count_500m` are now excluded from `to_process`, preventing duplicate API calls
3. **No-checkpoint POI-file guard** — don't drop existing POI columns when reading from `with_pois.csv` (only drop when starting truly fresh from `with_crime.csv`)
4. **Better error logging** — exception handler now logs `idx`, `type(e).__name__`, and message for easier diagnosis
5. **POI→final merge** — after POI fix, merged 81 updated POI columns from `master_table.csv` into `_final.csv` and parquet before Phase 9, so the Berlin schema output carries full POI coverage

**Final output (clean):**
| Field | Secondary | Primary |
|---|---|---|
| schulnummer | 100% | 100% |
| website | 100% | 99% |
| email | 96% | 99% |
| schulleitung | 100% | 99% |
| schueler_2024_25 | 100% | 97% |
| poi_supermarket_count_500m | 99% | 99% |
| transit_accessibility_score | 100% | 100% |
| crime data | 100% | 100% |
| description | 100% | 100% |
| tuition_display | 97% | 99% |
| embedding | 100% | 100% |

**Files changed:**
- `scripts_frankfurt/enrichment/frankfurt_poi_enrichment.py` — input fallback + already-enriched skip + no-drop guard + better error logging

<!-- NEW ENTRIES GO ABOVE THIS LINE -->

## 2026-04-06 — Frankfurt Pipeline Rebuilt: Schulwegweiser as Primary Source

**What:** Completely rebuilt the Frankfurt data pipeline to use frankfurt.de/schulwegweiser as the PRIMARY data source, replacing Hessen Verzeichnis 6 as Phase 1.

**Why:** The Schulwegweiser is the city's own school directory — more authoritative, more current, and far richer than the statistical Verzeichnis 6. It covers all 4 school categories (Grundschulen, Weiterführende allgemein, Förderschulen, Weiterführende beruflich) with ~279 schools vs 158 from Verzeichnis 6. Most importantly it provides official website URLs, contact info, Schulleitung, Schulprofile, Fremdsprachen, Ganztagsform, Besondere Angebote, and Auszeichnungen directly — no web research needed.

**Coverage map confirmed by browser inspection:**

| Field | Primary | Sec. allgemein | Beruflich | Förderschulen |
|---|---|---|---|---|
| Schulleitung | ✓ | ✓ | ✓ | ✓ |
| Schülerzahl, Klassenzahl | ✓ | ✓ | ✓ | ✓ |
| Official website | ✓ | ✓ | ✓ | mostly |
| Email + Telefon | ✓ | ✓ | ✓ | ✓ |
| Schulform (typed) | ✓ | ✓ | ✓ | ✓ |
| Profile / Schwerpunkte | ✓ | ✓ | - | ✓ |
| Förderschwerpunkt | - | - | - | ✓ |
| Frühe Fremdsprache | ✓ | - | - | - |
| 1./2./3. Fremdsprache | - | ✓ | ✓ | - |
| Ganztagsform (Einrichtungsart) | ✓ | ✓ | - | - |
| Besondere Angebote | ✓ | ✓ | ✓ | - |
| Auszeichnungen | - | ✓ | - | - |
| Berufsbereiche + Ausbildungsberufe | - | - | ✓ | - |
| Stadtteil | ✓ | ✓ | ✓ | ✓ |

**Technical implementation:**
- Phase 1: `frankfurt_schulwegweiser_scraper.py` (rewritten as full primary scraper)
  - Scrapes all 4 categories via Playwright + page-text line parser
  - Text parser locates content start by finding school name→first known label
  - All multi-value fields (Fremdsprachen, Schulform, Besondere Angebote etc.) handled
  - Nominatim geocoding of addresses → lat/lon
  - JSON cache keyed by category; partial-resumption support
  - Outputs: `raw/frankfurt_primary_schools.csv`, `secondary_schools.csv`, `vocational_schools.csv`
- Phase 2: `frankfurt_verz6_enrichment.py` (new optional phase)
  - Downloads Verzeichnis 6 Excel, joins by fuzzy name (≥0.75) + PLZ
  - Adds `schulnummer` + `ndh_count`; generates SW-{slug} IDs for non-matches
- Data combiner: rewritten with new column order reflecting Schulwegweiser-first schema
- Berlin schema transformer: updated mappings (school_type, sprachen, ganztagsform, Trägerschaft → tuition_display, leistungsprofil, betreuungsangebot); metadata_source updated

**School counts:**
- Grundschulen: 6 pages × 20 = ~120
- Weiterführende allgemein: 5 pages × 20 = ~100
- Förderschulen: 1 page = 19
- Weiterführende beruflich: 2 pages × 20 = ~40
- Total: ~279 (vs 158 from Verzeichnis 6)

**Parser validation:** 14/14 field extraction checks passed against Adorno-Gymnasium sample.

**Files changed:**
- `scripts_frankfurt/scrapers/frankfurt_schulwegweiser_scraper.py` — full rewrite as primary scraper
- `scripts_frankfurt/scrapers/frankfurt_verz6_enrichment.py` — new
- `scripts_frankfurt/processing/frankfurt_data_combiner.py` — new column schema, removed old SW overlay
- `scripts_frankfurt/frankfurt_to_berlin_schema.py` — updated field mappings
- `scripts_frankfurt/Frankfurt_school_data_asset_builder_orchestrator.py` — Phase 1 = SW primary, Phase 2 = Verz6 join

## 2026-04-06 — Frankfurt Schulwegweiser Scraper (Phase 2 — Official Websites & Profiles)

**What:** Built a Playwright-based scraper for the Frankfurt city school portal (frankfurt.de/schulwegweiser) as a new Phase 2 in the Frankfurt pipeline.

**Why:** The Verzeichnis 6 source (Hessen Statistik) provides no official school website URLs, email addresses, Schulprofile, or Ganztagsform data. The Schulwegweiser portal has all of this, and it's the canonical city-maintained school directory. This gives us deterministic, authoritative data rather than relying entirely on Perplexity-based web research.

**Data extracted per school:**
- `website` — official school URL (external link on detail page)
- `sw_email`, `sw_telefon` — contact details
- `sw_schueler` — student count from portal
- `sw_schulleitung` — principal name
- `sw_profile` — Schwerpunkte / school profiles (comma-joined)
- `sw_sprachen` — Frühe Fremdsprache
- `sw_ganztagsform` — Einrichtungsart (all-day school type)
- `sw_besonderheiten` — Besondere Angebote

**Technical details:**
- Playwright headless Chromium with anti-bot headers to bypass Cloudflare
- Crawls list pages: Grundschulen (6 pages × 20) + Weiterführende (5 pages × 20)
- Detail page extraction: 2-child div pattern for label/value pairs + external link detection
- All scraped data cached to `data_frankfurt/cache/schulwegweiser_cache.json`
- Fuzzy name matching (SequenceMatcher, threshold ≥ 0.65) to join portal data to Verzeichnis 6 schools
- Outputs: `data_frankfurt/intermediate/frankfurt_{type}_schools_with_schulwegweiser.csv`
- Data combiner updated: `merge_schulwegweiser()` overlays portal data even when later enrichments (traffic/transit/crime/POI) are the loaded source

**Phase renumbering:**
- Phase 2 is now Schulwegweiser (was Traffic). Traffic/Transit/Crime/POI/Combiner/Embeddings/Schema shifted to 3-9. Description pipeline is now Phase 10. Tuition phases are 11-13.

**Files changed:**
- `scripts_frankfurt/scrapers/frankfurt_schulwegweiser_scraper.py` — new scraper
- `scripts_frankfurt/processing/frankfurt_data_combiner.py` — `merge_schulwegweiser()` + fallback chain update
- `scripts_frankfurt/Frankfurt_school_data_asset_builder_orchestrator.py` — new phase 2, renumbered 3-13

## 2026-04-06 — Description Pipeline: Website Coverage 53% → 99% + Primary Schools

**What:** Improved the shared description pipeline to find school websites for nearly all schools, and ran description + tuition pipelines on Frankfurt primary schools.

**Why:** The original pipeline found websites for only 53% of Frankfurt secondary schools (39/73). Every school has a website — this was a data quality gap. Also fixed a corrupted primary `_final.csv` that had 2 schools instead of 85.

**Root cause of missing websites:** Perplexity's Sonar API returns source citation URLs alongside its text response, but we were only capturing the text — discarding the citation array that often contains the school's official website URL.

**Fixes:**
1. **Capture Perplexity citations** in Pass 0 — stored as `pass0_citations` in cache
2. **Pass citations to Pass 2** — structured extraction prompt now includes source URLs as hints
3. **Citation domain filter** with scoring: school portals (+5), school keywords in domain (+3), city name in domain (+2), name match (+4); blocks directories, FOIA portals, city portals
4. **Targeted website fallback**: if Pass 2 + citation analysis fail, fires targeted Perplexity query *"What is the official website of [school] in [city]?"*
5. **URL normalization**: strips `[1][2]` citation markers, normalizes to base homepage
6. **In-place parquet update**: `save_results` now updates `_final_with_embeddings.parquet` preserving embeddings — prevents schema transformer from overwriting enriched data

**Results:**
- Frankfurt secondary: **39/73 (53%) → 72/73 (99%)** websites found
- Frankfurt primary description pipeline: running (descriptions + structured extraction for 85 schools)
- All POI coverage confirmed 100% (primary CSV was corrupted with 2 rows; regenerated from parquet)

**Files changed:** `scripts_shared/generation/school_description_pipeline.py`

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
