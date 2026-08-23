# Lovable app — data admin processing (reference for the Python pipeline)

*Studied 2026-08-23 from the Lovable project's GitHub sync `jvr1980/schoolnossa-2d5310de`
(single snapshot commit `af17cf8`, pushed 2026-08-23 15:00 UTC). 86 SQL migrations, ~30 edge
functions, `src/pages/AdminImport.tsx` (4,047 lines), `AdminYearConfig.tsx`,
`components/admin/YearSelectionPanel.tsx`, `hooks/useYearConfig.ts`, `hooks/useSchools.ts`.
File:line references below point into that repo.*

Why this exists: the app's **Admin → Data Import** tab does real processing on top of what we
upload (full-replace imports with a column whitelist, post-import SQL, and seven LLM/API
enrichment jobs), and **Admin → Year Config** decides which year-suffixed columns the UI reads.
Every data refresh on the Python side has to respect these rules or the app silently loses or
ignores fields.

---

## 1. Architecture in one paragraph

Supabase (Lovable Cloud, project `whzvzoumldeqgyrqlilt`) holds `schools` (275 cols + 9 stable
cols added 2026-08-23) and `primary_schools` (93 + 6). RLS: **public SELECT only**; every write
goes through the service role — edge functions, the Lovable MCP SQL tool, or (April 2026 only)
temporary anon UPDATE policies that have all been dropped. There are **no triggers** that derive
data (only `schools.updated_at`; `primary_schools` has none), no views, no generated columns.
All derivations happen in (a) the import edge functions + two RPCs, (b) seven admin-triggered
enrichment edge functions, (c) one-off SQL migrations written through Lovable chat, and (d) our
own Python fill-gaps uploads. `schulnummer` is **UNIQUE per table across all cities** (no
(city, schulnummer) key) and is the upsert key.

Two write paths exist today:

| Path | Semantics | Use for |
|---|---|---|
| **Lovable import** (Admin → Data Import → `.parquet` upload) | full replace per city: archive → `DELETE WHERE city` → upsert whitelist; everything not in the whitelist becomes NULL; post-import SQL; nothing re-enriched automatically | brand-new city / category, or a deliberate full reload followed by re-running all enrichment jobs |
| **Python fill-gaps** (`scripts_shared/upload_to_supabase.py` PATCH or `--emit-sql`, `insert_new_schools_to_supabase.py`, hand-written SQL via the Lovable MCP SQL tool) | touches only the columns/rows named; never NULLs anything | yearly refreshes, new columns (stable fields), new schools, corrections |

**Recommendation:** keep refreshes on the fill-gaps path. Use the Lovable import only when the
parquet is a verified superset of what the app already holds for that city (see §6 — today it
is not, for Hamburg, Munich and Dresden).

---

## 2. Import path (`Admin → Data Import`)

### 2.1 UI flow (`src/pages/AdminImport.tsx`)
1. Select **Stadt** (`germanCities` ids: `berlin, hamburg, duesseldorf, koeln, frankfurt, muenchen, dresden, stuttgart, bremen, leipzig`) and **Schulkategorie** (`secondary` → `schools`, `primary` → `primary_schools`).
2. "Prüfe vorhandene Daten" — counts existing rows.
3. File picker accepts **`.parquet` only** (CSV/XLSX/JSON rejected, `:1030`). Parsed **in the browser** with `hyparquet` (int64 → Number, `NaN` → null, nested values stay JSON). The `city` column in the file is **ignored** — city comes from the dropdown.
4. If the city already has `tuition_income_matrix` values → dialog "Gebührendaten beibehalten?" (`preserveGranularFees`).
5. **YearSelectionPanel** ("Jahreskonfiguration") — analyses the uploaded column names for year suffixes and offers to update `app_settings.year_config` (the *legacy* key — the live UI reads `year_config_secondary/primary`, so this step is effectively a no-op; see §3). It only *shows* `ALTER TABLE … ADD COLUMN` SQL for unknown year columns, never runs it.
6. Rows are stripped of `embedding` and POSTed in **25-row chunks** to `import-schools` / `import-primary-schools` with `{schools, filename, filePath, preserveGranularFees, city, schoolCategory, isFirstBatch, isLastBatch}`. The raw parquet is also uploaded to storage bucket `school-imports` (policy is service-role only since 2026-07-10, so this client-side upload now fails silently; `file_path` ends up NULL).
7. Post-import tools on the same page (all manual, nothing runs automatically): research descriptions, generate DE/EN descriptions, regenerate embeddings, fetch environment data, tuition pass 1/2/3, data-completeness and column-analysis views, archive list with Re-Import / Wiederherstellen.

### 2.2 What the edge function does (`supabase/functions/import-schools/index.ts`, primary variant `import-primary-schools`)
- `isFirstBatch`: snapshot all rows of the city (`select *`) into `school_data_archives` (JSONB, `version_number`, `city`, `school_category`), then **`DELETE FROM schools WHERE city = <city>`**.
- Every chunk: map each row through an **explicit whitelist** (≈246 keys secondary, 66 primary) and `upsert(..., { onConflict: 'schulnummer' })` in 50-row batches.
- `isLastBatch` (secondary only): `rpc('reclassify_iss_with_abitur')` (**all cities**: `ISS` → `ISS-Gymnasium` when any `abitur_durchschnitt_2023/2024/2025` or `abitur_erfolgsquote_2024/2025` is non-null) and `rpc('cleanup_school_types', p_city)` (`school_type ILIKE '%gymnasium%'` and not already `Gymnasium`/`ISS-Gymnasium` → `Gymnasium`; **for Berlin only: DELETE every row whose school_type is not `Gymnasium`/`ISS`/`ISS-Gymnasium`**).
- Granular tuition "preserve": backs up `tuition_income_matrix, tuition_sibling_discounts, tuition_granular_reasoning, tuition_granular_generated_at, tuition_tier, tuition_tier_reasoning` per schulnummer — but the backup is a per-request local, so **only schools in the first 25-row chunk are actually restored**.
- No transaction: a failing chunk (NULL schulnummer, duplicate schulnummer within a 50-row batch, DECIMAL overflow) leaves the city half-imported; the archive exists but the restore tools are unsafe (§8).

### 2.3 Column mapping rules
- **Renames:** `summary_en || description_en` → `description_en`; `summary_de || description_de` → `description_de`; `transit_{rail,tram,bus}_01_{distance_m,name,lines}` → un-suffixed `transit_{rail,tram,bus}_{distance_m,name,lines}` (the `_01_latitude/_longitude` and all `_02/_03` columns keep their names); `city` := UI selection; primary `school_type := row.school_type || 'Grundschule'`; primary `tuition_tier` is taken from the file (secondary never is).
- **Coercion:** `parseInt_` (numbers → `Math.round`, strings → `parseInt`, so `"12.7"` → 12) for counts, ranks, distances, scores, `gruendungsjahr`, `schueler_*`, `lehrer_*`, `nachfrage_plaetze/wuensche_*`, `crime_*_2023/2024`, `poi_*_count_500m`, `poi_*_0N_distance_m`; `parseNum` (German comma accepted, first comma only) for `abitur_*`, `migration_*`, `nachfrage_prozent_2025_26`, lat/lng, `crime_*_avg/_yoy_pct`, `plz_avg_*`, tuition amounts, env numerics; `parseBool` (`true/false/1/0/yes/no/ja/nein`) for `plz_interpolated`, `scholarship_available`, `income_based_tuition`; everything else **passthrough** — so the strings `'nan'`, `'None'`, `''` land verbatim in text columns (emit real nulls).
- **Hard-coded year columns** in the mapping: `schueler_/lehrer_{2022_23,2023_24,2024_25}`, `abitur_durchschnitt_{2023,2024,2025}`, `abitur_erfolgsquote_{2024,2025}`, `nachfrage_plaetze/wuensche_{2024_25,2025_26}`, `nachfrage_prozent_2025_26` (no `_2024_25`), `migration_{2023_24,2024_25}`, `crime_*_{2023,2024}`. **Any other year (`schueler_2025_26`, `crime_total_crimes_2025`, …) and all `*_current` / `*_year` columns are silently dropped** until both the DB column and the mapping exist.
- **Never written by the import → NULL after any import:** `abitur_*_estimated*`, `abitur_prediction_*`, `admission_*`, `open_days*`, `last_open_day_seen`, `admission_fetched_at`, `description_researched_at`, `embedding`, `tuition_tier*` (secondary), `tuition_income_matrix`, `tuition_sibling_discounts`, `tuition_granular_*` (unless "preserve", first chunk only), and the new stable columns.
- DB precision limits that reject a batch: `abitur_durchschnitt_2023-2025 DECIMAL(3,2)` (max 9.99), `abitur_erfolgsquote_2024/2025`, `migration_2024_25`, `nachfrage_prozent_2025_26 DECIMAL(5,2)`. NOT NULL: only `schulnummer`, `schulname`.

---

## 3. Year configuration (`Admin → Year Config`) and the stable fields

- Stored in `app_settings` under **`year_config_secondary`** and **`year_config_primary`** (JSONB): per metric `{current, previous, available[], columnPattern, labelFormat}` for `studentCount (schueler_{year}), teacherCount (lehrer_{year}), abiturAverage (abitur_durchschnitt_{year}), abiturSuccessRate, demand (nachfrage_plaetze_{year}), migration, crime (crime_total_crimes_{year})` (`hooks/useYearConfig.ts:6-132`). Defaults: students/teachers current `2024_25`; abitur `2025`; demand `2025_26`; migration `2024_25`; crime `2024`.
- `hooks/useSchools.ts:51-65,125-252` resolves each metric with a fallback chain current → previous → rest of `available` (newest first) and records which year was used for labels. Exceptions that bypass the config: demand % is literally `nachfrage_prozent_<demand.current>` (no fallback); `crime_total_crimes_2023/2024`, `nachfrage_*_2025_26`, `schueler_2024_25`/`lehrer_2024_25`/`abitur_*_2024` are hard-coded in `SchoolDetailsModal.tsx`, `PinnedSchoolsContext.tsx` (uses compiled defaults, not the setting), `DataSnapshot.tsx`, `DataCoverage.tsx`, `DataQualitySection.tsx`, `lib/mcp/*`, and in the `generate-school-descriptions`, `research-school-descriptions`, `semantic-search`, `mcp` edge functions and the `reclassify_iss_with_abitur` SQL.
- **Stable fields (`schueler_current`, `lehrer_current`, `migration_current`, `nachfrage_prozent_current`, `abitur_durchschnitt_current`, `crime_total_crimes_current`, `data_school_year`, `abitur_year`, `crime_data_year`):** the DB columns exist since 2026-08-23 and are filled, but **nothing in the app reads them yet** — harmless, ignored. Adopting them (a Lovable-side change set, in order):
  1. `import-schools` / `import-primary-schools`: add the nine/six keys to the whitelist (`parseInt_` for schueler/lehrer, `parseNum` for the rest, passthrough for the three `*_year` strings) so a future full import doesn't NULL them.
  2. `useYearConfig` / `useSchools`: either set each metric's `columnPattern` to the constant stable name (a pattern without `{year}` already works — `.replace` is a no-op) and derive the displayed year from `data_school_year` / `abitur_year` / `crime_data_year`, or replace the fallback chains with direct reads.
  3. Fix the hard-coded references listed above (modal Nachfrage/Crime blocks, PinnedSchoolsContext, DataSnapshot, DataCoverage, DataQuality, MCP tools, the three edge functions, `reclassify_iss_with_abitur`).
  4. Retire or repurpose YearSelectionPanel / `analyze-year-columns` / AdminYearConfig (labels only).
  Until 1–2 are done, keep shipping the year-suffixed columns the mapping knows.

---

## 4. Enrichment jobs (Admin → Data Import, manual buttons; edge functions under `supabase/functions/`)

All admin-gated, driven from `AdminImport.tsx` via `invokeLongRunningFunction()` (10-min timeout, loops until `remaining === 0`). Each receives `city` (slug or `all`) and `schoolCategory`. Nothing is scheduled; **every job must be re-run by hand after a full import**.

| Job (button) | Reads | Writes | Overwrite? | Model / API |
|---|---|---|---|---|
| `research-school-descriptions` ("Rohdaten-Beschreibungen recherchieren": Fehlende / Fehlende & Ältere / Alle neu) | schulname, school_type, bezirk, ortsteil, traegerschaft, website, gruendungsjahr, besonderheiten, sprachen, schueler_2024_25, lehrer_2024_25, city | `description` (raw **English** 400–800 words), sets `description_de/_en = NULL`, `description_researched_at`; on failure writes sentinel `[RESEARCH_FAILED]` / `[RESEARCH_FAILED:QUOTA_EXHAUSTED]` | fill-gaps (`description` null/''; optional older-than-N-days); "Alle neu" bulk-clears the city first; 1 row/call | Gemini `gemini-3-pro-preview` → `gemini-2.5-pro` + Google Search grounding |
| `generate-school-descriptions` ("KI-Beschreibungen generieren": Fehlende / Alle neu) | `description` (raw) + structured fields (type, bezirk, ortsteil, besonderheiten, sprachen, traegerschaft, gruendungsjahr, counts, transit score, crime cat, poi_kita; secondary: abitur_2025, poi_primary) | `description_en`, `description_de` (150–300 words each; no timestamp) | fill-gaps (DE or EN null); "Alle neu" has no paging → only the first 25 rows | Lovable AI gateway `google/gemini-3-flash-preview`; prompts hard-code "in Berlin" |
| `regenerate-embeddings` ("Embeddings regenerieren") | `description` (raw EN) | `embedding vector(768)` | fill-gaps or overwrite-all (paged); sentinel descriptions are embedded too | Google `gemini-embedding-001`, 768-d |
| `semantic-search` (public, dashboard/KI-Suche) | `embedding` + 15 display cols | — | — | translate query (`gemini-2.5-flash-lite`) → embed → cosine in JS over all rows; the pgvector RPCs `match_schools_by_embedding*` exist but are unused |
| `compute-school-similarities` (**no UI button**; manual call) | `schools.embedding` (768-d only) | `school_similarities` (delete-all + top-N pairs `similarity_score, rank`) | full rebuild; `schools` only | — (used by the knowledge graph) |
| `fetch-environment-data` ("Umweltdaten abrufen": Beide / Luft / Pollen; Fehlende / Alle neu) | latitude, longitude, `env_*_fetched_at` | `env_aqi_annual_avg` (actually *current* AQI), `env_aqi_category` (Google string verbatim), `env_aqi_dominant_pollutant`, `env_aqi_fetched_at`; `env_pollen_{tree,grass,weed,overall}_avg` (0–5), `env_pollen_fetched_at`; stamps written even on "no coverage" | fill-gaps by `*_fetched_at`; "Alle neu" has no cursor (re-fetches the same 10 rows) | Google Air Quality `currentConditions:lookup`, Pollen `forecast:lookup?days=1` |
| `generate-tuition-tiers` ("Schulgebühren-Kategorien", pass 1) | private rows (`traegerschaft ILIKE %privat%/%frei%`): tuition amounts, fees, notes, website, type | `tuition_tier` (`low ≤100 / medium ≤300 / high ≤500 / premium ≤750 / ultra >750` €/month), **`tuition_monthly_eur` overwritten** (rounded / annual÷12 / AI estimate / fallback 200), `tuition_tier_reasoning` (schools only) | fill-gaps (`tuition_tier IS NULL`); "Alle neu" bulk-clears; on total AI failure persists `medium / 200 / "requires manual verification"` | deterministic if an amount exists, else Gemini `gemini-3-pro-preview` + search, JSON schema |
| `generate-tuition-granular` (pass 2) | private rows with `tuition_tier` and no matrix | `tuition_income_matrix` (12 brackets `under20 … over250` → €/month), `tuition_sibling_discounts` (`child_2_pct, child_3_pct, child_4_plus_pct`), `tuition_granular_reasoning`, `tuition_granular_generated_at`; synthetic fallback matrix on API error | fill-gaps; 1 row/call | Gemini `gemini-3-pro-preview` + search |
| `generate-tuition-pass3` ("Gebühren-Verifikation – GPT-5.2") | private rows whose matrix is flat and `income_based_tuition` null/true | matrix, discounts, reasoning `[GPT-5.2 Pass3]`, `tuition_granular_generated_at`, **`income_based_tuition` true/false** | overwrites selected rows | OpenAI Responses `gpt-5.2` + web_search |

Notes: the display modal shows `description_de` **or** `description_en` by UI language with no cross-fallback, so both must be filled; semantic search needs `embedding` regenerated after any description change (no trigger does it); the knowledge graph reads `school_similarities` which is deleted by cascade when `import-schools` deletes a city.

---

## 5. Where the "extra" Supabase fields actually came from

Cross-checked against the live DB on 2026-08-23 (columns/values present in Supabase but absent from the current local finals):

| Field(s) | Origin | Implication |
|---|---|---|
| `description_en` (all cities), `description_de` where the pipeline had none (Hamburg 170/257, Frankfurt 73/100), `description` raw, `description_researched_at` | app jobs §4 (research + generate), Feb–Apr 2026 | Lovable-owned; pipeline must not overwrite; a full import NULLs `description_researched_at` and drops `embedding` |
| `tuition_tier*`, `tuition_income_matrix`, `tuition_sibling_discounts`, `tuition_granular_*`, `income_based_tuition` (pass 3), `tuition_monthly_eur` (rewritten by pass 1) | app jobs §4 | Lovable-owned |
| `env_aqi_*`, `env_pollen_*` | app job §4 | Lovable-owned |
| `admission_criteria_bullets_en`, `admission_application_window_en`, `admission_notes_en`, `open_days_en` | written 2026-04-18 through the temporary anon "full access" policy window (`migrations/20260418092435` → `20260418095412`) by an external script (not in either repo) | not reproducible from this repo; a full import NULLs them |
| Munich `telefon` (91), `website` (88), `traegerschaft` (79) | **our** Places-contact enrichment (`data_munich/intermediate/munich_secondary_schools_with_places_contact.csv`, branch `feature/munich-places-contact`) uploaded 2026-04-19 via the fill-gaps uploader during the `temp_anon_fill_telefon/website` windows | **local regression**: `_with_places_contact` is not in `munich_data_combiner.find_most_enriched_file` and the contact columns are outside the merge-back carry list → current Munich finals have 0/129 |
| Hamburg `besonderheiten` (161/238), `gruendungsjahr` (124/125), `lehrer_2024_25` (95/104), `description_de/_en` | our Hamburg website-metadata enrichment uploaded 2026-04-19/20 (`temp_anon_fill_*` windows) | **local regression**: the Wave A Hamburg rebuild no longer carries them (0 in `hamburg_school_master_table_berlin_schema.csv`) |
| Dresden `school_type`/`schulart` (75/90), Bremen `school_type` | SQL migrations in Lovable chat: `20260409182026` (Dresden types), Stuttgart `school_type := schulart` (`20260417074330`); Bremen via the April pipeline split | **local regression** for Dresden (0/75, 0/90 locally); Bremen fixed in the mapper 2026-08-23 |
| `abitur_*_estimated*`, `abitur_prediction_*` | our `data_shared/apply_abitur_predictions.sql` | pipeline-owned but outside the finals; never in the import whitelist |
| Berlin `crime_*_2023`, Telraam `plz_*` traffic (200 rows) | older Berlin pipeline outputs | Berlin finals moved to crime 2024/2025 and no longer produce `plz_*`; the app **never reads `plz_*`** |
| `crime_safety_rank` 1..5 + German categories | one-off SQL `20260124184523` (quintiles) | app displays rank/category verbatim; pipeline ships its own |

**Before any full Lovable import of Hamburg, Munich or Dresden, the local finals must regain the rows marked "local regression" (or those fields will be wiped).** Until then, use fill-gaps uploads only.

---

## 6. Data contract the pipeline must respect

- `city` ∈ `berlin|hamburg|duesseldorf|koeln|frankfurt|muenchen|dresden|stuttgart|bremen|leipzig`; category `primary|secondary`.
- `schulnummer`: non-null **string**, unique within the file and **globally unique per table across cities** (Bremen 1–3-digit numbers, Frankfurt 4-digit, NRW 6-digit, Hamburg `nnnn-0`, Berlin `01Y01`, Stuttgart `STG-`, Munich `BY-SCHUL_…`/`MUCPRIV_…` — currently no collisions, checked 2026-08-23). `schulname` non-null.
- `plz` TEXT (keep leading zeros); `gruendungsjahr`, counts, ranks, scores INTEGER; `transit_accessibility_score` INTEGER (UI defaults to 50 when null — never ship 0 as a failure marker); `crime_safety_rank` 1..5; `crime_safety_category` German text shown verbatim (`Sehr sicher / Sicher / Durchschnittlich / Erhöht / Hoch`; the UI also recognises sicher/safe/erhöht/elevated substrings).
- `traegerschaft`: private iff it contains `privat` or `frei` (case-insensitive) — drives tuition passes and the public/private filter.
- `school_type`: free text, but Berlin secondary must be `Gymnasium|ISS|ISS-Gymnasium` (others are deleted on import); `%gymnasium%` variants are collapsed to `Gymnasium`; `ISS` + any abitur value → `ISS-Gymnasium`; Munich uses `Gymnasien/Realschulen/Mittelschulen/Förderzentren/Grundschulen`; the filter list is built dynamically from the data; NULL renders as `ISS`. Keep `schulart` populated too (MCP `search_schools` filters on it).
- `sprachen`: `,` or `;` separated (pinned view splits on `,` only) — **`|`-separated values render as one language**.
- `description_de` **and** `description_en` both needed (no cross-fallback); `description` (raw EN) feeds research coverage and embeddings.
- Tuition: `tuition_tier ∈ low|medium|high|premium|ultra`; `tuition_income_matrix` keys exactly `under20, 21-30, 31-40, 41-50, 51-75, 76-100, 101-125, 126-150, 151-175, 176-200, 201-250, over250`; `tuition_display` free text shown raw.
- Admission JSON (schools only): `admission_criteria_bullets[_en]` string[]; `admission_application_window[_en]` `{opens, closes, notes}` ISO dates; `open_days[_en]` `[{date, start_time, end_time, event_type, audience, notes}]`; `last_open_day_seen` DATE.
- Transit: only stop #1 is read (`transit_{rail,tram,bus}_{name,distance_m,lines}`); `_02/_03`, lat/lng, `transit_stop_count_1000m`, `transit_all_lines_1000m` are stored but unused. Proximity filters use `≤ 500 m`.
- Precision: `abitur_durchschnitt_*` ≤ 9.99; `abitur_erfolgsquote_*`, `migration_2024_25`, `nachfrage_prozent_2025_26` ≤ 999.99.
- `ortsteil`/`bezirk` must be filled for every city (UI falls back to the string "Berlin"); crime rate per 1,000 students is computed client-side per `bezirk`.
- Columns the UI never reads (safe to stop shipping): `plz_*`, per-category `crime_*_{2023,2024,_avg,_yoy_pct}`, `crime_violent_crime_avg`, transit `_02/_03` and all transit/POI lat/lng, `poi_{supermarket,restaurant,bakery_cafe}_0x_*`, `poi_secondary_school_count_500m`, `nachfrage_*_2024_25`, `metadata_source`, `env_*_fetched_at`, `env_aqi_dominant_pollutant`.

---

## 7. Operating model going forward

1. **Yearly refresh**: regenerate finals → `upload_to_supabase.py --groups … --dry-run` → apply via `--emit-sql` + Lovable MCP SQL tool (or PATCH with a service-role key if one ever exists). New schools: `insert_new_schools_to_supabase.py --emit-sql`. New year columns: DDL via the SQL tool **and** update `app_settings.year_config_secondary/primary` at Admin → Year Config (the import-time panel writes the wrong key).
2. **Never run the Lovable import on a refreshed city** unless the parquet is a superset and you are prepared to re-run research → descriptions → embeddings → similarities → environment → tuition passes and to lose `admission_*_en`/`open_days_en` (not reproducible).
3. **Stable fields**: they are live in the DB; the Lovable change set in §3 is what makes the app use them. Until then keep the year-suffixed columns flowing.
4. After any description change on our side, ask the admin to run "Embeddings regenerieren" (and `compute-school-similarities` by hand) — no trigger does it.

---

## 8. App-side issues worth fixing in Lovable (found during the study)

- `generate-school-descriptions` "Alle neu generieren" only regenerates the first 25 rows (no paging; `onlyMissing=false` refetches the same batch); prompts hard-code "in Berlin" for every city.
- `fetch-environment-data` "Alle neu abrufen" has no cursor (re-fetches the same 10 rows; `remaining` never drops); `env_aqi_annual_avg` is a current-conditions snapshot.
- `generate-tuition-tiers` persists `medium / 200 €` on total AI failure and overwrites `tuition_monthly_eur`; UI badges know only four tiers (`ultra` renders none).
- `compute-school-similarities` has no UI trigger and handles `schools` only; `school_similarities` is cascade-deleted by every city import.
- `semantic-search`: `webmcp/tools.ts` reads `data.results` but the function returns `{schools}` (always 0 results); `ki-search/SemanticSearch.tsx` omits `schoolCategory` (always secondary).
- Import: `preserveGranularFees` restores only the first 25-row chunk; `schulnummer` global uniqueness lets a cross-city collision re-city a row; `restore-school-archive` deletes **all** `schools` rows regardless of city/category and re-inserts with new ids (orphans pins); "Re-Import" from the archive list calls `import-schools` without `city` (defaults to Berlin) and without batching; the storage bucket policy (service-role only since 07-10) silently breaks the raw-parquet upload and signed URLs.
- `PinnedSchoolsContext` uses compiled default years, `,`-only `sprachen` split and exact-match safety categories; `SchoolDetailsModal` hard-codes "Berlin-{ortsteil}" and crime years 2023/2024.
- `primary_schools` has no `updated_at` trigger; no vector index exists on either table (dropped with the embedding recreation, never recreated).
- YearSelectionPanel and `analyze-year-columns` write/read the legacy `year_config` key the UI no longer uses.

---

*Study artefacts: three read-only sub-agent reports (import/year-config, enrichment jobs, DB layer & field consumption) synthesised here; the cloned app repo lives only in the session scratchpad — re-clone `jvr1980/schoolnossa-2d5310de` for details.*
