# SchoolNossa Development Journal

## 2026-08-23 — Stuttgart PLZ fix (plz == schulnummer digits since April)

**What:** Every Stuttgart row (81 secondary / 95 primary, locally and in Supabase) carried the stuttgart.de directory entry id as `plz` (STG-17965 -> plz 17965; 6-digit ids truncated, e.g. STG-310163 -> 31016). Root cause: `scripts_stuttgart/scrapers/stuttgart_school_scraper.py::scrape_detail_page` took the first 5-digit number on the detail page (`re.search(r'(\d{5})\s*(Stuttgart)?', html)`) — that is the id in the `data-content` JSON, not the PLZ. The wrong PLZ was baked into the templated `description` ("Adresse: ..., 19965 Stuttgart") and 36 LLM `description_de` texts.

**Fix:** new `extract_address_from_html()` reads the PLZ from the page's schema.org JSON-LD (`CityovSerializer-<id>` -> `address.postalCode`), falls back to the "Anschrift" box, then to the first `7[01]xxx Stuttgart` before the footer (footer = Rathaus 70173); `is_valid_plz()` rejects anything that is not `^7[01]\d{3}$` or equals the id. `repair_cached_plz()` re-fetches only the PLZ for cached entries with an invalid plz, so the 7-day cache self-heals (258/258 repaired). Side fix in `stuttgart_crime_enrichment.py`: the scraped Stadtbezirk (`ortsteil`) is now preferred over the approximate PLZ->Bezirk table (with real PLZ the table would have moved 14 secondary schools into the wrong Bezirk; crime columns are unchanged vs. before).

**Regeneration (free chain only):** scraper (cache) -> traffic -> transit -> crime -> combiner (merge-back kept all paid columns) -> `scripts_stuttgart/processing/stuttgart_plz_repair.py` (exact per-row `'<old> Stuttgart' -> '<new> Stuttgart'` replacement in description/description_de, writes master_table.*, *_final_with_embeddings.parquet, *_final.csv; no re-embedding, no LLM) -> `stuttgart_to_berlin_schema.py`. Backup of the previous finals/raw/intermediates/cache in `data_stuttgart/final/backup_2026-08-23/`. Result: 176/176 rows corrected, all plz 70xxx (70173–70619), no column lost coverage; the *_final/berlin_schema column set changed only in all-NULL placeholder columns because the Berlin reference parquets were rebuilt after the Wave A Stuttgart run.

**Supabase:** `data_shared/supabase_sql/stuttgart_plz_fix_2026-08-23.sql` — 176 idempotent UPDATEs (schools 81, primary_schools 95), guarded on the old plz (verified read-only: 176/176 guards match), fixing `plz` and `replace()`-ing the old PLZ in description / description_de / description_en. Not executed (Lovable MCP auth). Note `insert_new_schools_2026-08-23.sql` still carries plz '19965' for STG-19965 (already present in Supabase, so the INSERT is a no-op).

## 2026-08-23 (afternoon) — Munich cleanup, Berlin transit restore, Supabase handoff applied

**Munich:** (1) `munich_school_master_scraper.filter_munich_schools` matched 'München' as a *substring* of city/name → 21 non-Munich rows reached Supabase (Waldmünchen ×3, Schwabmünchen ×4, Ostermünchen ×2, Münchenbernsdorf/Thüringen, Ingolstadt 'Münchener Straße', Grafing ×2, plus the Landkreis suburbs Garching ×4 and Kirchheim ×4). Now exact municipality + PLZ 80/81, never by name; `munich_data_combiner._drop_non_munich_rows` guards the intermediates. Finals re-run (phases 7–9, embeddings skipped): secondary 139→129, primary 165→154, paid columns intact. (2) The 48 researched private schools (`MUCPRIV_*`) were verified **not** present in Supabase under any other id (name / street+PLZ / <120 m match against all 256 Munich rows — all of which are public `BY-SCHUL_*` entries). `phase4_ingest_to_pipeline.school_type_label` now emits Munich's own vocabulary (Gymnasien 15 / Realschulen 8 / Waldorfschule 2 / Internationale Schule 3 / Montessorischule 2 / Grundschulen 17; only Christophorus-Schule stays 'Sekundarschule') instead of the generic placeholder. SQL: `data_shared/supabase_sql/insert_munich_private_2026-08-23.sql` (48) and `delete_non_munich_2026-08-23.sql` (13 clearly non-Munich + 8 Landkreis suburbs as a separate block). Side note: Supabase Munich private rows carry no descriptions/POI/embeddings yet.

**Stuttgart PLZ** (separate entry above): root cause in the scraper's first-5-digit regex; 176 guarded UPDATEs in `stuttgart_plz_fix_2026-08-23.sql`; `insert_new_schools_2026-08-23.sql` regenerated so STG-19965 carries 70173.

**Berlin transit:** BVG API still refusing connections. Both enrichers (`scripts_berlin/enrichment/enrich_schools_with_transit.py`, `scripts_berlin_primary/enrichment/enrich_grundschulen_with_transit.py`) now retry, trip a circuit breaker after 10 consecutive failures, keep previous values per schulnummer (new per-school caches `data_berlin*/cache/bvg_transit_cache.csv`, seeded from the last good April run, git f685437), and never write a 0/0 summary; `rebuild_final_table.blank_failed_transit_summary()` keeps an existing 0/0 from shipping or gap-filling. Real run today: secondary 252/258 and primary 489/490 rows got their April transit summaries back (identical to Supabase), everything else byte-identical. When the API is back, the same three commands refresh live.

**Supabase (applied by the interactive session with the Lovable MCP, observed live):** stable-fields DDL ran (9 + 6 columns), the 8 new schools are inserted, the stable fill was in progress at 16:40 (done: Berlin, Bremen, Dresden, Düsseldorf, Frankfurt, Hamburg, Köln `schools` + Berlin/Bremen/Dresden/Düsseldorf `primary_schools`; `stable_2026-08-23/` regenerated to the 11 remaining files). Still to run: Munich private INSERTs, non-Munich DELETEs, Stuttgart PLZ fix.

**Lovable "data admin" processing — now studied from the code** (Lovable project synced to GitHub as `jvr1980/schoolnossa-2d5310de` on 2026-08-23; full write-up in `docs/LOVABLE_DATA_ADMIN_PROCESSING.md`: import = full replace per city with a column whitelist + post-import SQL, year config in `app_settings.year_config_*`, seven enrichment jobs, origin of every "extra" Supabase field, data contract, and the Lovable-side change set needed to adopt the stable fields). Earlier data-side inference for the record: Columns/values that exist in Supabase but were never produced by any local file, with their run timestamps: English translations `description_en` (Berlin 259, Dresden, Düsseldorf, Frankfurt, Hamburg, Köln, Stuttgart; primaries too), `admission_*_en` (345–890 rows), `open_days_en` (151); German descriptions where the pipeline had none (`description_de` Hamburg 170/257, Frankfurt 73/100) with `description_researched_at` (Feb–Apr 2026); granular tuition research (`tuition_granular_*`, `tuition_income_matrix`, `tuition_sibling_discounts`, `tuition_tier`+`_reasoning`, `income_based_tuition`, `scholarship_available`, fee columns; `tuition_granular_generated_at` Feb–Apr, ~250 rows); environment fetches `env_aqi_*` (142 schools / 726 primary, `env_aqi_fetched_at`) and `env_pollen_*` (491 primary, Feb); metadata/contact research (Munich `telefon` 91, `website` 88, `traegerschaft` 79 — jedeschule has none; Hamburg `besonderheiten` 161/238, `gruendungsjahr` 124/125, `lehrer_2024_25` 95/104; Frankfurt primary `gruendungsjahr`, `lehrer`, `nachfrage_*`); type normalisation `school_type`/`schulart` for Dresden (75/90) and Bremen; and the abitur predictions (`abitur_*_estimated*`, from this repo's `apply_abitur_predictions.sql`). Berlin also keeps `crime_*_2023` and the Telraam `plz_*` traffic columns (200 rows) that the Wave A Berlin rebuild no longer produces locally. Proper study of the code needs the Lovable MCP (file reading) or a GitHub sync of the app.

## 2026-08-23 — Stable-Fields Upload Prep + New-School INSERTs (blocked on Lovable auth)

**What:** Prepared everything for the Supabase side of the Wave A refresh so it is a handful of Lovable MCP SQL-tool calls: the stable-fields DDL, the stable fill as generated SQL, and the INSERTs for the schools the refresh found. Found and fixed three real bugs on the way. The Lovable MCP server was in "Needs authentication" state in this (non-interactive) session and no service-role key exists locally, so **nothing was written to Supabase yet**.

**Why:** Lovable Cloud owns the DB: the anon key is read-only, DDL and writes have to go through Lovable (its MCP SQL tool). The April backfill needed temporary RLS UPDATE policies; emitting the fill as idempotent SQL avoids any policy window entirely.

**Bugs found & fixed:**
- `scripts_shared/schema/stable_fields.py` built the vintage stamps with `pd.NA`; `upload_to_supabase._is_empty()` only recognised float NaN, so `_coerce()` str()'d it to `'<NA>'` — **950 cells would have been written as the literal text `'<NA>'`** (REST and SQL paths alike). Root cause fixed (plain None) + `_is_empty()` now treats any scalar NA as empty. After the fix `abitur_year` (+196) = `abitur_durchschnitt_current`, `crime_data_year` (+2131) = `crime_total_crimes_current`.
- `scripts_bremen/bremen_to_berlin_schema.py` never mapped `schulform` → `schulart`/`school_type`, so `_bucket()` saw NaN, all 254 rows went to the "secondary" split and the primary split file was stale from April 19. Now `schulart = schulform`, `school_type` bucketed like the live Bremen rows (Grundschule / Gymnasium / else Oberschule) → 114 primary + 140 secondary. Also: Berlin's reference schema moved to `crime_*_2024/2025`, so Bremen's 2023-vintage `crime_*_2023` columns were silently dropped from the output (and `crime_total_crimes_current` could not be derived) — now kept as extras (93 primary / 114 secondary rows restored).
- Berlin transit: **every** Berlin row in the local finals has `transit_accessibility_score = 0` and `transit_stop_count_1000m = 0` (258/258 secondary, 489/490 primary) — the BVG API was down during Wave A and the summary was recomputed to zero while the nearest-stop columns were carried. Supabase is untouched (fill-gaps), but do **not** run `--groups transit_summary` for Berlin until the transit re-run; the insert script drops a 0/0 transit summary as a failure marker.

**New tooling:**
- `upload_to_supabase.py --emit-sql DIR` — same planning as `--dry-run`, but writes chunked `UPDATE t AS s SET col = COALESCE(s.col, v.col) FROM (VALUES ...)` statements with explicit casts (types inferred from live rows, DDL types for the still-missing stable columns). Output: `data_shared/supabase_sql/stable_2026-08-23/` — 24 files, 353 KB, 2,694 rows (`schueler_current` +2211, `lehrer_current` +1417, `crime_total_crimes_current` +2131, `migration_current` +244, `nachfrage_prozent_current` +231, `abitur_durchschnitt_current` +196, `data_school_year` +2174, `abitur_year` +196, `crime_data_year` +2131). Structurally validated (tuple/field counts, casts, quoting) and spot-checked against local data + Supabase ids.
- `scripts_shared/insert_new_schools_to_supabase.py` — local rows whose (city, schulnummer) is absent from Supabase; list / `--emit-sql` (idempotent `INSERT ... SELECT ... WHERE NOT EXISTS`) / `--apply` (REST POST). Hamburg reads the Berlin-schema output (the `_final.csv` has no plz/strasse/email). `--assume-stable-cols` for SQL meant to run right after the DDL.

**New schools found by the refresh (8 rows / 7 schools) → `data_shared/supabase_sql/insert_new_schools_2026-08-23.sql`:** Berlin `04A44` Willkommensschule TXL (ISS); Hamburg `5605-0` Schule Leuschnerstraße (Stadtteilschule), `7747-0` Academy of LIFE (schools + primary_schools, like the 30 other Hamburg Grund- und Stadtteilschulen), `5485-0` Grundschule am Schilfufer (opens 2027); Stuttgart `STG-19965` Kolping-Kolleg (adult Gymnasium — comparable Abendgymnasien/Kollegs already exist in Supabase); Bremen `449` Neue Oberschule Osterholz (schools), `138` Schule an der Luxemburger Straße (primary_schools; Bildungsgang `G`).

**Pre-existing local↔Supabase drift surfaced by the same diff (NOT inserted — decide separately):** Munich 31 + 17 `MUCPRIV_*` researched private schools (commit 301fb50; synthetic ids, never uploaded); Frankfurt 34 local-only (Förderschulen/Berufsschulen/Grundschulen in the secondary file — April curation left them out) + 8 Supabase-only ids; Düsseldorf 3 (Waldorf + 2 international schools, ids 187410/990001/990002); Berlin `09S06`, `12A44`, `04P41` and Bremen `513` exist only in Supabase (closed locally). `python3 scripts_shared/insert_new_schools_to_supabase.py` lists all of them.

**Also noticed:** Stuttgart `plz` equals the digits of `schulnummer` for 71/81 secondary rows (e.g. `STG-17965` → plz `17965`; Stuttgart is 70xxx) — already in Supabase and baked into LLM descriptions; needs a separate fix.

**Next session (needs Lovable MCP re-auth: `/mcp` in an interactive `claude`, or `claude mcp add --transport http lovable https://mcp.lovable.dev`):**
1. Run `scripts_shared/schema/supabase_stable_fields.sql` via the Lovable SQL tool.
2. `python3 scripts_shared/upload_to_supabase.py --groups stable --dry-run` (now passes the ghost-column guard; expect the numbers above).
3. Run `data_shared/supabase_sql/insert_new_schools_2026-08-23.sql` (8 statements), then the 24 files in `data_shared/supabase_sql/stable_2026-08-23/` via the SQL tool — or, if a service-role key / RLS policy is available instead, `upload_to_supabase.py --groups stable` and `insert_new_schools_to_supabase.py --ids ... --apply`.
4. Re-run `--groups stable --dry-run` to confirm 0 remaining fills.

## 2026-08-22 — Wave A Free Data Refresh + Stable Fields Migration

**What:** Executed the zero-API-cost half of the August source audit (`docs/DATA_REFRESH_PLAN_2026.md`): refreshed base data for Berlin (SJ 2026/27), Munich, Hamburg, Dresden, Stuttgart, Bremen; Unfallatlas 2025 everywhere; crime 2025 (Kriminalitätsatlas 2016-2025, Hamburg Stadtteilatlas 2025, BKA PKS 2025 T01 for Munich/Stuttgart); introduced stable year-agnostic fields so the Lovable app stops breaking on yearly refreshes. Zero paid API calls (verified by log grep + env-stripped runs).

**Why:** Asset was uniformly on SJ 2024/25; Munich's scraper was pinned to a Jan-2025 jedeschule snapshot (19 months stale); Berlin's portal already serves 2026/27. NRW/Hessen/Leipzig deliberately deferred to Wave B (their annual releases land ~Sept).

**Refresh-safety infrastructure (the bulk of the work):**
- `scripts_shared/processing/merge_enriched_columns.py` — merge-back of paid columns (POI/descriptions/embeddings/tuition/env/admission + LLM-scraped year stats) from the previous final parquet on `schulnummer`; wired into Munich/Stuttgart/Dresden combiners. Handles mixed-dtype normalization (pyarrow write failures).
- Embedding generators no longer null paid vectors on keyless runs (Hamburg restore-from-parquet, Munich parquet-input preference, Stuttgart preserve-carried).
- Berlin secondary path unification (scrapers wrote to CWD, combiner read scripts_berlin/processing/, manifest said data_berlin/raw/ — none existed); both Berlin orchestrators shell-quoted (space in volume name broke every subprocess with exit 2); new `convert_crime_xlsx_to_csv.py` (the missing Kriminalitätsatlas step — semantics reverse-engineered to byte-match the old parquet: HZ sheets, violent = Raub+Straßenraub+KV+gef.KV, rank tertiles safe/moderate/elevated).
- Munich orchestrator phase 7 re-ingests the 48 researched private schools (a base refresh silently dropped 31 secondary rows — caught and restored same-day).
- Berlin scrapers resolve the newest Schuljahr from the live dropdown (was pinned to value 16 = 2025/26; now picks 17 = 2026/27) and ISS scraper relabels year columns from the site's own headers (anti-vintage-laundering).
- `scripts_shared/processing/refresh_traffic_columns.py` — surgical Unfallatlas update for unchanged-base cities (Frankfurt/Leipzig/NRW), overwrites traffic_* in finals on schulnummer.

**Stable fields (additive):** `scripts_shared/schema/stable_fields.py` derives `schueler_current`, `lehrer_current`, `migration_current`, `nachfrage_prozent_current`, `abitur_durchschnitt_current`, `crime_total_crimes_current` + vintage stamps (`data_school_year`, `abitur_year`, `crime_data_year`), newest-first fallback chains. Wired into all 9 schema mappers + `upload_to_supabase.py` (new `stable` field group, ghost-column guard that aborts instead of silently skipping). Supabase DDL prepared in `scripts_shared/schema/supabase_stable_fields.sql` — **pending execution via Lovable** (Cloud-managed), then `--groups stable` upload.

**Results (before → after, POI/embedding preservation verified per table):**
- Berlin primary: 257 → **490 rows** on SJ 2026/27 base; local parquet had drifted to a stale 257-row subset while Supabase served 492 — recovered descriptions/embeddings (490/490) from a Supabase snapshot after the old parquet's numeric-ID keyspace matched 0 rows.
- Berlin secondary: 259 → 258 (one school closed, one opened), crime 2024+2025 (Lichtenberg → safe, Tempelhof → moderate), traffic re-attached, transit partially fresh (BVG API refused connections mid-run; rest carried).
- Munich: fresh jedeschule 2026-08-15 base, PKS 2025 (HZ 7684→6061), 139/165 rows, POI 108/148 + embeddings preserved exactly.
- Hamburg 172/259 (+2/+2 new schools), Stuttgart 81/95 (+1), Dresden 159, Bremen 254 (+1) — all with Unfallatlas 2025 and preserved paid columns; Dresden split finals gained embeddings (0 → 75/90).
- Frankfurt/Leipzig/NRW: traffic 2025 in all finals (100% row match), no base change.

**Still open:** Supabase DDL + stable upload (needs Lovable MCP OAuth — `claude mcp add --transport http lovable "https://mcp.lovable.dev"` from an interactive terminal); ~6 new schools need Supabase INSERTs (uploader is PATCH-only); regression scripts + LLM prompt contracts still read year-suffixed names; Berlin transit re-run when BVG API recovers; Wave B triggers ~Sept 9 (Hessen) / Sept 23 (NRW).

## 2026-04-20 — NL POI Enrichment + GB Pipeline Chain Fix

**What:** Ran NL Phase 6 POI enrichment (0% → 100% POI coverage across 1,626 schools) and repaired the GB pipeline so Phase 3 (traffic) actually produces data and Phases 4/5/9 flow it into the final table.

**Why:** Status audit showed both pipelines well short of Berlin parity. NL had a POI script but had never been run. GB had multiple silent failures that left traffic/transit empty in the final table despite the phases reporting OK.

**NL fixes:**
- Rewrote `nl_poi_enrichment.py` to use `scripts_shared.enrichment.enrich_schools_with_pois.enrich_school` with a 5-worker thread pool, per-school JSON cache (keyed by `vestiging_code`/`brin_code`), and preferred the most-enriched input (`nl_schools_with_demographics.csv`) so POI output carries all prior enrichments.
- `nl_combine_and_finalize.py`: `find_best_input` now picks the intermediate with the highest column count rather than a fixed priority list — ordering-agnostic to handle POI-after-demographics.
- Result: 100% POI coverage (81 POI cols), 1,626 rows, ~7,800 Google Places API calls (~$250).

**GB fixes (four bugs, not one):**
- `gb_traffic_enrichment.py`: STATS19 raw now ships as `collision_severity`; script expected `accident_severity`. Added a rename before the slim-cache write. Without this, Phase 3 errored and silently stayed un-run for weeks.
- `gb_transit_enrichment.py`: NaPTAN `Status` filter was `== "act"` but real values are `active`/`inactive`. The whole 101 MB NaPTAN file was being parsed to 0 stops, so transit enrichment was a no-op. Also deleted the empty `naptan_stops_clean.csv` cache to force a re-parse.
- `gb_to_core_schema.py` — column-name mismatch: the OSM-based base scraper produces DfE-native names (`establishment_type_group`, `la_name`, `attainment8_average`, `progress8_average`) but the finalizer read GIAS names (`establishment_type`, `local_authority`, `ks4_*`). Added an alias rename at load time so the scraper stays idiomatic and the finalizer speaks one vocabulary.
- `gb_to_core_schema.py` — `compute_academic_score` crashed on DfE suppression codes (`'z'`, `'x'`). Wrapped the float coercion in try/except returning NaN.

**GB results (before → after):**
- Traffic: 0% → **100%** (9 cols)
- Transit rail: 0% → 50.7%, tram 0% → 12.7%, bus 0% → 88.9%, stop_count 100%
- `gb_establishment_type`: 0% → 90.2%
- `gb_local_authority`: 0% → 92.6%
- `gb_ks4_attainment8`/`progress8`: populated at 80.1% (via column-alias fix)

**Still-open GB gaps** (not in this change): POI script missing entirely, IMD decile / FSM% join (raw exists at `data_gb/raw/gb_imd.csv` but needs postcodes.io LSOA lookup), Ofsted scraping, remaining KS4 detail (progress8 CI bounds, ebacc, grade5 %), embeddings + similar schools, tuition.

**Still-open NL gaps:** embeddings + similar schools, Inspectorate ratings, school_track/board, buurt/wijk codes, tuition, enrollment pressure.

## 2026-04-19 — Schema Mapper Gaps + Supabase Backfill (1,512 rows)

**What:** Audited a Supabase coverage report that flagged many 0%-populated columns across cities, tracked each gap to either a mapper bug or a genuine source gap, fixed the mappers, extended the Supabase uploader, and filled the resulting cells.

**Why:** The audit showed columns like Dresden `email` and `bezirk` at 0/75, Frankfurt `leitung` at 0/99, Stuttgart `schueler_2024_25` at 0/80, Bremen `description_en` at 0/140, Leipzig `ortsteil` at 0/94. Most were mapping bugs — the data existed locally under a different column name or was being silently dropped by a combiner/schema-mapper save-order issue. Only the truly un-fixable gaps turned out to be external source limits (Munich jedeschule only returns addresses, Leipzig Saxon source lacks Stadtbezirk, Hamburg crime ships categories instead of counts).

**Pipeline fixes (permanent, committed):**
- **Dresden `dresden_to_berlin_schema.py`:** new `_fill_from_saxon_source` helper populates Berlin-canonical columns from raw Saxon names — `mail`→`email`, `phone_code_1+phone_number_1`→`telefon`, `headmaster_firstname+lastname`→`leitung`, `community_part`→`ortsteil`, `crime_stadtbezirk`→`bezirk` (with "StB N " prefix stripped).
- **Leipzig `leipzig_to_berlin_schema.py`:** same Saxon-source helper adapted. Also added primary/secondary split output so the stale April-13 berlin_schema artefacts the Supabase uploader consumed actually get refreshed.
- **Frankfurt `frankfurt_to_berlin_schema.py`:** `schulleitung`→`leitung` rename that was missing. Moved the `scripts_shared.schema.core_schema` import into a try/except since it fails outside the orchestrator and was silently truncating the save.
- **Bremen `bremen_to_berlin_schema.py`:** `summary_en`→`description_en` fill-gaps (Bremen's description pipeline wrote summary_* not description_*). Added primary/secondary split outputs; preserved `description_en` as a Bremen-specific extra since Berlin schema doesn't include it.
- **Stuttgart `processing/stuttgart_data_combiner.py`:** `find_most_enriched_file` now prefers `_with_metadata` over `_with_pois`. The metadata enrichment (leitung, schueler, lehrer, sprachen, gruendungsjahr, besonderheiten) layers on top of pois but was being silently skipped, so the master table carried only the pois file and downstream outputs lost all student/teacher/principal data. Added defensive `school_type`-from-`schulart` normalization so generic `'primary'/'secondary'` buckets never leak past the combiner again (the scraper was fixed in d6819cd but raw CSVs on disk predate that commit).
- **Stuttgart `stuttgart_to_berlin_schema.py`:** reordered save-before-import, same pattern as Frankfurt.

**Supabase uploader rewrite (`scripts_shared/upload_to_supabase.py`):**
- Replaces the old `UPDATE_FIELDS = [6 items]` with organized `FIELD_GROUPS` (contact / location / school_attr / descriptions / crime / transit_summary). `--groups` flag selects which categories upload.
- `COL_ALIASES` maps local `transit_bus_01_name`→Supabase `transit_bus_name` (Supabase uses unprefixed names for nearest stop, _02/_03 for the rest).
- Per-field PATCH with `?<col>=is.null` URL guard so the server-side filter matches the Python NULL-only check — a race between our SELECT and PATCH cannot overwrite a non-NULL value.
- Service-role key from env (`SUPABASE_SERVICE_ROLE_KEY`) with fallback to embedded anon key when Lovable installs a scoped RLS policy.
- Per-table schema probe; columns missing from a target table (e.g. `transit_all_lines_1000m` on `primary_schools`) are logged as skipped instead of crashing.
- Dry-run prints per-city per-field fill counts plus a grand total.

**Lovable coordination:** Anon key was RLS-blocked from UPDATE. Went with temporary per-column `IS NULL`-gated UPDATE policies on `schools` and `primary_schools` (21 columns), covering contact, location, school_attr, descriptions, crime, transit_summary. Lovable dropped them after the fill. Verified drop by PATCH-then-reread on a real Dresden row.

**Supabase results:**
- 1,357 rows filled in the main run (before Stuttgart), 155 more in the Stuttgart re-run, plus 175 Stuttgart `school_type` values corrected via Lovable SQL. 1,512 touched total.
- Top per-field fills: `besonderheiten` +783, `schulart` +483, `leitung` +443, `lehrer_2024_25` +425, `gruendungsjahr` +408, `ortsteil` +351, `schueler_2024_25` +321, `description_en` +203.
- Biggest city-level wins: Dresden `schools` email/telefon/leitung/ortsteil/bezirk 0/75 → 75/75; Frankfurt `primary_schools` leitung 0/100 → 99/100; Hamburg `primary_schools` besonderheiten 0/257 → 238/257; Munich `primary_schools` besonderheiten 0/148 → 146/148, lehrer/schueler 0 → ~138.
- Zero overwrites, zero HTTP errors across ~6,800 PATCH calls.

**Remaining genuine source gaps (require new data source, out of scope):**
- Munich `schools` (secondary) contact info: jedeschule.codefor.de only returns name/address/coords. All 108 rows are 0% for email/telefon/website/leitung.
- Leipzig `bezirk`: Saxon source has `community_part` (→Ortsteil) only; no reliable Stadtbezirk mapping.
- Frankfurt `bezirk` / `lehrer` / `migration_*`: Schulwegweiser (the primary source) doesn't expose them.
- Hamburg `crime_total_crimes_2023`: Hamburg's crime feed ships categories not raw counts.
- `transit_all_lines_1000m` on `primary_schools`: table schema gap Lovable offered to add.

**Commits (all on `main`):** 9c30ebb, c1a06ad, ad8fea4, 54392c0, 0a0f1fc, f16dfac, merged as 9eb8e34.

## 2026-04-17 — Cross-City Admission Criteria + Open Day Enrichment

**What:** Built and ran a new cross-city Gemini enrichment that visits every German school's website to extract structured admission criteria and upcoming open day dates. Two scripts:
1. `scripts_shared/enrichment/enrich_german_schools_with_admission_and_open_days.py` — main enrichment, feeds homepage URL to Gemini 2.5 Flash with URL-context + Google-Search grounding
2. `scripts_shared/enrichment/reenrich_admission_open_days_via_sitemap.py` — fix pass that discovers the site's sitemap.xml, picks admission/events subpage URLs by keyword scoring, and re-calls Gemini with those URLs for schools where the homepage-only pass failed

**Why:** SchoolNossa had rich enrichments (traffic, transit, crime, POIs, descriptions) but no structured fields for the two things parents care about most at decision time: how to apply and when to visit. Both exist on every school website in unstructured form.

**Scope:** 2,578 rows across 15 city-tables (9 German cities). 2,229 schools had a usable website URL; 349 (mostly Munich) had none.

**Results after both passes:**
- 1,932 success (83% of URL-having schools)
- 1,851 schools with admission bullets (83%)
- 729 schools with application windows (33%)
- 420 schools with upcoming open days (19% — expected for mid-April, most are Oct–Feb)
- 1,452 schools with past open day dates (65% — useful for next-cycle predictions)
- 36 remaining parse_error (1.6%), 261 no_admission_info (12%)

**Sitemap re-enrichment impact:** Recovered 107 schools (from 114 parse_error down to 36, a 68% reduction). Discovery methods: sitemap (117), homepage anchors (14), canonical probing (2).

**Key design decisions:**
- Shared URL-keyed cache (`data_shared/cache/admission_open_days/cache.json`, sha1 of normalized URL) so duplicate URLs across primary/secondary tables hit Gemini once. Hamburg Primary got 257 cache hits and 0 API calls.
- 60-day TTL because open-day calendars are seasonal
- German prompt; structured JSON output with ISO dates, bullet lists
- Scripts are standalone (not wired into per-city orchestrators) — run after all city pipelines produce final master tables

## 2026-04-17 — Fix school_type Mapping Bug (Stuttgart + Frankfurt)

**What:** Fixed school_type containing generic placeholders ("secondary", "Weiterführende Schule") instead of specific German school types. Added validation guard to prevent recurrence.

**Why:** Lovable frontend filter UI relies on school_type being a canonical German type (Gymnasium, Realschule, etc.). Stuttgart had `school_type = 'secondary'` for all 80 rows despite having correct `schulart` values. Frankfurt had a fallback path that could produce "Weiterführende Schule" for unmatched categories.

**Fixes:**
- `scripts_stuttgart/scrapers/stuttgart_school_scraper.py` — changed `school_type: classification` → `school_type: schulart` so schools get their actual German type (Gymnasium, Realschule, Gemeinschaftsschule, etc.) instead of generic "secondary"/"primary"
- `scripts_frankfurt/scrapers/frankfurt_schulwegweiser_scraper.py` — added `weiterfuehrende-allgemeinbildende-schulen` → `Gesamtschule` to category fallback mapping, preventing "Weiterführende Schule" placeholder
- `scripts_shared/schema/core_schema.py` — added `CANONICAL_DE_SCHOOL_TYPES`, `GENERIC_SCHOOL_TYPE_PLACEHOLDERS`, and `validate_school_types()` guard function
- `scripts_stuttgart/stuttgart_to_berlin_schema.py` — wired in `validate_school_types(strict=True)` as CI guard
- `scripts_frankfurt/frankfurt_to_berlin_schema.py` — wired in `validate_school_types(strict=True)` as CI guard

**Audit results:** All other cities (Berlin, Hamburg, NRW, Munich, Bremen, Dresden, Leipzig) already use specific German types correctly. The regression script (`run_regression.py:391-398`) already had a band-aid workaround preferring `schulart` when `school_type` is generic — this is now redundant but harmless.

**Convention documented:** `school_type` must always be one of the canonical German types used by the filter UI. Never the generic "secondary"/"primary" placeholders.

## 2026-04-12 — Fix Student/Teacher Data Gaps Across All German Cities

**What:** Investigated and fixed missing student/teacher counts (`schueler_2024_25`, `lehrer_2024_25`) across all German city pipelines. Previously only Berlin had good coverage; most other cities were at 0%.

**Why:** Lovable.Dev dashboard showed that slider filters for student/teacher counts only worked for Berlin. Root cause analysis revealed two issues: (1) website enrichment scripts for most cities only generated descriptions, not student/teacher counts; (2) schema transformers didn't map available source data to the canonical Berlin fields.

**Phase 1 — Schema transformer fixes (zero API cost):**
- `scripts_frankfurt/frankfurt_to_berlin_schema.py` — added `schueler_gesamt` → `schueler_2024_25` mapping (data existed but was only used for ndH ratio calculation)
- `scripts_munich/munich_to_berlin_schema.py` — added student/teacher passthrough mappings
- `scripts_dresden/dresden_to_berlin_schema.py` — added student/teacher passthrough mappings
- `scripts_stuttgart/stuttgart_to_berlin_schema.py` — added student/teacher passthrough mappings
- All mappings use fill-gaps-only guards (`pd.isna()` check before writing)

**Phase 2 — Website enrichment scripts (Gemini 2.5 Flash + URL context + Google Search grounding):**
- `scripts_munich/enrichment/munich_website_metadata_enrichment.py` — upgraded from description-only to full NRW-style metadata extraction. Added Google Search fallback for schools without website URLs (Bayern API doesn't provide them)
- `scripts_dresden/enrichment/dresden_website_metadata_enrichment.py` — fully implemented (was a `NotImplementedError` stub)
- `scripts_hamburg/enrichment/hamburg_website_metadata_enrichment.py` — new script, targeting teacher data gap. Fixed `schul_homepage` column mapping and separate `data_hamburg_primary/` path handling
- `scripts_stuttgart/enrichment/stuttgart_website_metadata_enrichment.py` — new script
- `scripts_frankfurt/enrichment/frankfurt_website_metadata_enrichment.py` — new script, prioritizes final files over intermediate (intermediate lacked website URLs)
- `scripts_shared/verify_coverage.py` — new coverage reporting script

**Phase 3 — Retry logic added to all `_call_gemini` functions (all 7 city scripts + NRW):**
- Empty responses: retry up to 2x with 3s delay
- JSON parse errors: retry up to 2x with 3s delay
- 500 INTERNAL server errors: retry up to 2x with 5s delay
- Improved coverage by ~10-20% on second pass

**Results after 2 passes:**

| City | Schools | Schüler (before → after) | Lehrer (before → after) |
|------|---------|------------------------|----------------------|
| Munich | 148 pri | 0% → **93%** | 0% → **94%** |
| Hamburg sec | 170 | 97% (kept) | 0% → **56%** |
| Hamburg pri | 257 | 98% (kept) | 0% → **40%** |
| Dresden | 75+90 | 0% → **65%** | 0% → **50%** |
| Stuttgart sec | 80 | 0% → **66%** | 0% → **56%** |
| Stuttgart pri | 95 | 0% → **54%** | 0% → **39%** |
| Frankfurt sec | 99 | 0% → **52%** | 0% → **33%** |
| Frankfurt pri | 100 | 97% (kept) | 8% → **32%** |
| Leipzig | 186 | 0% → **45%** | 0% → **32%** |
| Bremen | 253 | 0% → **49%** | 0% → **30%** |
| NRW (D'dorf+Köln) | 407 | 60% (kept) | 31-46% (kept) |

**Bonus coverage gained:** 80-98% `description_de` across all cities (was 0% for most), plus `schulleitung`, `besonderheiten`, `sprachen` filled.

**Key technical decisions:**
- All enrichment follows NRW template: `google.genai` with `UrlContext` + `GoogleSearch` grounding
- Fill-gaps-only semantics everywhere — never overwrite existing source data
- JSON caching per schulnummer avoids repeated API calls on re-runs
- Munich uses Google Search grounding alone (no URL context) since Bayern API provides almost no website URLs — still achieved 93% coverage

**Files changed:** 14 scripts modified/created across 8 city pipelines + shared utils

## 2026-04-12 — NRW Pipeline Retrigger: Köln & Düsseldorf Data Regenerated

**What:** Full NRW pipeline re-execution for Köln (155 primary + 103 secondary) and Düsseldorf (91 primary + 58 secondary). Data had been lost and needed regeneration from scratch. All 11 phases completed for both school types.

**Why:** Previously generated NRW data files were missing from `data_nrw/`. Pipeline scripts and some cache files (transit, website metadata, anmeldezahlen matches) were intact, allowing partial reuse.

**Pipeline phases executed:**
1. School Master Data (NRW Schulministerium Open Data → filter Köln+Düsseldorf)
2. Traffic Accidents (Unfallatlas 2022–2024, 500m radius)
3. Transit Accessibility (Overpass API, fresh query — old cache expired)
4. Crime Statistics (PKS NRW, Bezirk-level)
5. POI Enrichment (Google Places API)
5b. Anmeldezahlen (Düsseldorf only — re-downloaded CSV + 4 PDFs from city open data)
5c. Website Metadata & Descriptions (Gemini 2.5 Flash, ~380 school websites)
6. Data Combination
7. Embeddings (Gemini fallback, 768-dim — no OPENAI_API_KEY)
8. Berlin Schema Enforcement

**Düsseldorf Anmeldezahlen recovery:**
- Primary CSV: `opendata.duesseldorf.de` — 85/91 schools matched (2026/27 data)
- Secondary PDFs: 4 school-type PDFs from `duesseldorf.de/fileadmin/` — 46/58 schools matched
- 13 primary + 20 secondary schools oversubscribed (>100% demand/capacity)

**Firecrawl tuition extraction (new):**
- Built `scripts_nrw/enrichment/nrw_tuition_firecrawl_enrichment.py`
- Deep-crawls ~30 private school websites via Firecrawl map+scrape
- Extracts structured tuition data (11 schema columns) via Gemini
- Result: Most German private schools don't publish fees online — 5/35 had any info (free schools identified, income-based models noted, "auf Anfrage" captured in tuition_notes)
- Added `FIRECRAWL_API_KEY` to `.env`

**Output files** (`data_nrw/final/`):
- `nrw_{primary,secondary}_school_master_table_final_with_embeddings.parquet`
- `nrw_{primary,secondary}_school_master_table_final.csv`
- `{koeln,duesseldorf}_{primary,secondary}_school_master_table_final_with_embeddings.parquet`
- Berlin schema parquets for frontend compatibility

**Key fix:** Berlin primary reference parquet was missing from `data_berlin_primary/final/` — copied from worktree `.claude/worktrees/condescending-haslett/` to unblock Phase 8.

## 2026-04-09 — Abitur Predictions: Frankfurt, Munich, Stuttgart

**What:** Extended the Ridge regression Abitur prediction pipeline to three new cities. Generated rebased predictions for all Abitur-eligible schools and merged results into master table parquets.

**Why:** Frankfurt, Munich, and Stuttgart pipelines were complete but had no Abitur quality estimates. Model was trained on Berlin (128) + Hamburg (73) labeled schools; CV R²=0.417, MAE=0.179.

**Key technical changes (scripts_shared/regression/):**
- Removed Model B (Abitur erfolgsquote) — CV R²=−0.24, no improvement from Y-standardization or binary classification
- Added FRANKFURT_COLUMN_MAP for different column naming in Frankfurt/Munich/Stuttgart
- Expanded ABITUR_ELIGIBLE_TYPES to include Frankfurt types (IGS, KGS, Gemeinschaftsschule, Berufsoberschulen, etc.)
- Added schulart fallback when school_type is generic "secondary" (Stuttgart/Munich pattern)
- Implemented _impute_gisd_from_zensus(): OLS proxy for GISD quintile from Zensus features with R²<0.10 guard
- Downloaded Zensus 100m grid: Frankfurt (16,380 cells), Munich (25,566), Stuttgart (13,546)

**Stuttgart known issue:** PLZ column contains the numeric part of the Schulnummer instead of real postal codes — GISD imputation skipped. Needs fix in Stuttgart scraper.

**Results:**
| City | Predicted | Total | Rebase shift | State avg |
|---|---|---|---|---|
| Frankfurt | 56 | 99 | +0.030 | 2.38 (Hessen) |
| Munich | 30 | 108 | −0.221 | 2.29 (Bayern) |
| Stuttgart | 45 | 80 | −0.051 | 2.32 (BaWü) |

**Output:** Prediction columns merged into {city}_{type}_school_master_table_final_with_embeddings.parquet for all three cities. Standalone prediction parquets also in data_{city}/final/.

## 2026-04-09 — Leipzig Pipeline: 186 Schools Complete

**What:** Built the complete Leipzig school data pipeline (all school types combined: 92 Grundschulen, 37 Oberschulen, 31 Gymnasien, 24 Sonstige, 2 Förderschulen). All 9 phases + tuition + description pipelines.

**Data source:** Sachsen Schuldatenbank API (`schuldatenbank.sachsen.de`) — provides school metadata, coordinates, legal_status_key (1=public, 2=private). Crime data from Leipzig Kriminalstatistik (Ortsteil-level).

**Key technical decisions:**
- `legal_status_key` → `traegerschaft` mapping added to combiner's `clean_data()` — scraper returns "unbekannt" but has the key (1=oeffentlich, 2=frei/privat)
- Berlin reference parquet not available in worktree — schema transformer made resilient with fallback to other cities or partial-only enforcement
- Description and tuition pipelines write to cache files, then merged back into main CSV before embeddings/schema phases
- Symlinked `config.yaml` to `scripts_shared/generation/` for API key access

**Results:**
| Field | Coverage |
|---|---|
| Schools | 186 (156 public, 30 private) |
| Coordinates | 186/186 (100%) |
| Traffic (Unfallatlas) | 186/186 (100%) |
| Transit (Overpass) | 186/186, all scored |
| Crime (Leipzig PKS) | 186/186, Ortsteil-level |
| POI (Google Places) | 186/186 (100%) |
| Descriptions (Perplexity+Gemini) | 178/186 (96%) |
| Sprachen | 113/186 (61%) |
| Besonderheiten | 185/186 (99%) |
| Website | 185/186 (99%) |
| Tuition (30 private) | 30/30 classified (22 medium, 4 low, 2 high, 2 ultra) |
| Embeddings (OpenAI) | 186/186, 3072-dim |
| Berlin Schema | PASS, 265 Berlin + 92 Leipzig extras |

**Notable:** Leipzig International School correctly classified as "ultra" tier (€1,010-1,070/month). 7 of 30 private schools have income-based tuition.
## 2026-04-07 — Dresden Pipeline: 159 Schools from Sächsische Schuldatenbank API

**What:** Added Dresden (Sachsen) as a new city to SchoolNossa. Complete pipeline with 9 phases — from data source research through enrichment implementation.

**Key findings:**
- Sächsische Schuldatenbank has a free CSV API with WGS84 coords (easiest school data source so far)
- 159 schools: 88 Grundschulen, 39 Oberschulen, 30 Gymnasien, 2 Förderschulen (124 public + 35 private)
- 100% coordinate coverage, 97% website coverage
- Dresden Open Data Portal has per-Stadtteil crime data (best granularity after Hamburg)
- Traffic: Unfallatlas with ULAND=14 (same pattern as NRW)
- No per-school Sozialindex in Sachsen — using GISD proxy

**Template:** NRW pipeline (closest match for traffic/transit approach)

**Files:** `scripts_dresden/` (10 scripts), `data_dresden/` (5 dirs), `docs/dresden_data_availability_research.md`

**Status:** Full pipeline executed. All phases complete including descriptions (159/159) and tuition (35/35 private schools). Embeddings skipped (no OPENAI_API_KEY in worktree).

**Tuition results (35 private schools):**
- 33 medium tier (€120-200/month), 1 ultra (Dresden International School: €1,300/month), 1 low (€65/month)
- 16 income-based tuition, 13 confirmed flat-fee
- Income matrices generated for all 35 schools

**Final output (split for frontend):**
- `dresden_primary_school_master_table_final.csv` — 90 schools (88 GS + 6 cross-level)
- `dresden_secondary_school_master_table_final.csv` — 75 schools (69 OS/Gym + 6 cross-level)
- `dresden_school_master_table_final.csv` — 159 combined
- Secondary file: 265/265 Berlin schema columns matched

**Cross-level schools** (Waldorf, DIS, Gemeinschaftsschulen) duplicated into both files with appropriate schultyp

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

## 2026-04-08 — Hamburg Full Rebuild: Primary Pipeline + Deduplication Fix

**What:** Created a complete Hamburg primary school (Grundschule) pipeline and rebuilt both Hamburg pipelines from scratch with data quality fixes.

**Why:** Hamburg only had secondary schools. Also discovered Zweigstellen (branch campuses) and duplicate WFS rows were inflating school counts in both pipelines.

**Key fixes:**
- Removed Zweigstellen from both scrapers — these are satellite campuses, not separate schools
- Deduplicated on `schulnummer` — WFS returned multiple rows per school from GeoJSON entrance points
- Combined schools (Grund-/Stadtteilschule) now appear in **both** pipelines since they serve both levels
- Secondary went from 286 → **170** schools; primary has **257** schools; 30 overlap

**Results:**
| | Secondary | Primary |
|---|---|---|
| Schools | 170 (84 STS + 80 Gym + 6 combined) | 257 Grundschulen |
| Columns | 187 | 182 |
| Coordinates | 100% | 100% |
| Transit | 100% | 100% |
| Traffic | 68% | 68% |
| Crime | 100% | 100% |
| POI | 100% (81 cols) | 100% (81 cols) |
| Embeddings | 170/170 | 257/257 |
| Berlin Schema | 265 cols | 265 cols |

**Structure:** `scripts_hamburg_primary/` + `data_hamburg_primary/` (separate from secondary)

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
