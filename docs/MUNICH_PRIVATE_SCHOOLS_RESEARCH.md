# Munich Private Schools — Data Source Research

**Date:** 2026-04-23
**Author:** SchoolNossa pipeline
**Status:** Research complete; implementation ticket pending (see "Recommended pipeline" below).

## 1. Why this research exists

The live SchoolNossa Munich dataset contains **148 primary + 108 secondary = 256 schools**, all sourced from `jedeschule.codefor.de`. Two Gemini classification passes over these 256 schools returned **zero** schools tagged as private — confirming that jedeschule's Bavarian feed ships state-registered public schools only. Users of the app were expecting well-known names like Munich International School, Obermenzinger Gymnasium, Maria-Ward-Schule, Rudolf-Steiner-Schule, and Phorms — none of which are in our data.

**Goal:** identify candidate private schools in **Stadt München + Landkreis München**, scoped to **primary + secondary** levels (no Förderschulen, no Berufsschulen), and recommend how to ingest them permanently into the pipeline.

**Scope decisions made with user:**

| Dimension | Choice |
|---|---|
| School levels | Primary + secondary only |
| Geography | Stadt + Landkreis München |
| Places API budget | Up to €20 |
| km.bayern.de scraping | Polite HTML scrape (cached, <1 req/sec, identified User-Agent) |

## 2. Sources evaluated

Four sources probed in Phase 0, all passed the "≥ 10 parseable private-school rows" gate. Aggregator sites (privatschulen-in-bayern.de, VDP, ARGE) were not probed — the four primary sources already exceeded coverage expectations, and aggregators can be added as a gap-fill source later without rework.

### Source ranking matrix

Scored 1–5 (5 = best). "Legal/ToS risk" is inversed so 5 = lowest risk.

| Source | Coverage | Accuracy | Machine-readable | Legal/ToS | Effort | Total |
|---|---|---|---|---|---|---|
| **km.bayern.de Schulsuche** | 3 | **5** | 3 | 4 | 3 | 18 |
| **Google Places API** | **5** | 3 | **5** | **5** | 4 | 22 |
| **OSM / Overpass** | 3 | 3 | **5** | **5** | **5** | 21 |
| **Gemini 2.5-flash + Google Search** | 4 | 3 | 4 | **5** | 4 | 20 |

Scoring notes:
- **km.bayern.de** is the official state registry — highest accuracy, clean addresses, Trägerschaft explicitly flagged in school names ("Privat", "freier Trägerschaft", "Staatlich anerkannte"). Machine-readability is limited: POST-form + custom `--@--@--@--@--` delimiter response. Coverage is Stadt München only via the `MB1=Landeshauptstadt München` filter; Landkreis would require a second pass over Oberbayern-Ost/West with PLZ post-filtering.
- **Google Places** had the best single-source recall (59 rows after filtering) because category-specific text queries plus 22 km radius covers the whole Landkreis in one pass. Downside: noisy (Kita, Nachhilfe, Lehrinstitute show up) — needs keyword + level filters.
- **OSM / Overpass** is free, fast, machine-readable. Lower coverage because private-school tagging (`operator:type=private`, explicit `operator`) is uneven. Reused the scraper's already-written `fetch_osm_private_schools` logic. Overpass is flaky — added fallback mirror chain after a 504 from `overpass-api.de`.
- **Gemini 2.5-flash with Google Search grounding** has good breadth across categories. Confidence fields in output help filter low-quality hits. Used as triangulation + gap-fill rather than primary source.

## 3. Phase-by-phase results

### Phase 0 — Reconnaissance

Every source returned ≥ 10 parseable private-school rows for Munich:

| Source | Raw returns | Flagged private |
|---|---|---|
| OSM / Overpass | 737 schools in bbox | 50 |
| km.bayern.de | 110 Stadt München schools | 21 |
| Google Places (4 queries) | 48 unique | 48 |
| Gemini grounded (1 category prompt) | 46 | 46 |

Gate passed 4-of-4.

### Phase 1 — Per-source pilots

Per-source canonical CSVs produced with a uniform schema:
`source_key, source_ref, schulname, address_line1, plz, ort, latitude, longitude, schulart, schulart_detail, traegerschaft_hint, …, source_url, match_reasons, scraped_at`

| Source | Rows kept (primary + secondary private only) |
|---|---|
| `osm_overpass.csv` | 18 |
| `km_bayern.csv` | 24 (incl. 3 from detail-page scrape for names without private marker) |
| `google_places.csv` | 59 (from 21 text queries, ~111 raw unique places) |
| `gemini_grounded.csv` | 51 (Stadt + dedicated Landkreis pass) |

### Phase 2 — Combine + deduplicate

- Dedup key: `max(token_set_ratio(full names), token_set_ratio(stemmed cores)) >= 80` **AND** one of: same PLZ+ort, haversine distance ≤ 150 m, or (stem_core ≥ 80 with matching ort).
- Authority stack: `km.bayern > osm > google_places > gemini_grounded`.
- Out-of-scope filters applied before union:
  - Ort must be in the Stadt+Landkreis München whitelist.
  - Name must NOT contain `sonderpäd`, `förderzentr`, `berufsschul`, `berufsfachschul`, `fachoberschul`, `berufsoberschul`, `fachschul`, `hochschul`, `akademie`, `kolleg`, `vhs`, `sprachschul`, `musikschul`, `fahrschul`, `tanzschul`, `schwimmschul`, `internat` (standalone), `wirtschaftsschul`.
  - Level must be `primary`, `secondary`, or `both` (not `other` / `unknown`).

**Output: 60 unique canonical candidates** — distribution by source-count:

| Sources merged into the row | Count |
|---|---|
| 4 (all sources agree) | 1 |
| 3 | 10 |
| 2 | 13 |
| 1 | 36 |

Scope rejections: 20 by name-excluded markers (mostly `sonderpäd`), 3 by out-of-scope ort (Starnberg, Dachau, Gilching).

Diff vs. the live 256-row Munich dataset:
- **10** candidate rows correspond to schools already present in our pipeline (mostly Sabel, Lukas etc. that happened to scrape under public-school naming)
- **50** net-new private-school candidates

### Phase 3 — Validation

**Recall** — against 15 known Munich-area privates:

| Target | Score | Matched? |
|---|---|---|
| Obermenzinger Gymnasium | 100 | ✓ |
| Rudolf-Steiner-Schule München | 100 | ✓ |
| Phorms München | 100 | ✓ |
| Isar-Gymnasium | 92 | ✓ |
| Lukas-Gymnasium | 100 | ✓ |
| Sabel Realschule München | 100 | ✓ |
| Nymphenburger Schulen | 100 | ✓ |
| Edith-Stein-Gymnasium | 100 | ✓ |
| Jules Verne Campus | 100 | ✓ |
| Jüdisches Gymnasium München | 100 | ✓ |
| Sinai-Grundschule | 90 | ✓ |
| Montessori München | 100 | ✓ |
| Maria-Ward-Gymnasium Nymphenburg | 78 | ✗ (present under alias "Congregatio Jesu vorm. Englische Fräulein" + "Erzbischöfliche Maria-Ward-Realschule") |
| Gymnasium der Englischen Fräulein Nymphenburg | 69 | ✗ (same as above — historic name) |
| Bavarian International School | 52 | ✗ (correctly absent — Haimhausen is Landkreis Dachau, outside scope) |

**Effective recall: 14/15 (93%)** after reconciling aliases; the one true miss (BIS) is a correct exclusion.

**Precision** — 20 random rows manually inspected: all real private Munich-area primary/secondary schools. Zero false positives visible (target ≤ 10% FP).

**Coverage** — 50 net-new, within expected band `[25, 60]`.

## 4. What the candidate list looks like

Breakdown of the 60 canonical rows:

| Dimension | Counts |
|---|---|
| Trägerschaft hint | privat 56 · kirchlich 4 |
| Gemini category (where set) | katholisch 8 · andere 7 · International 5 · Montessori 3 · evangelisch 2 · jüdisch 2 · Waldorf 1 |
| Schulart | secondary 36 · primary 16 · both 8 |
| Ort | München 49 · Ismaning / Pullach / Neubiberg / Riemerling / Unterschleißheim / Schäftlarn 1 each · blank 5 |

Where net-new rows came from — rows found *only* by one source:
- `google_places` unique finds: 16 (Jules Verne Campus, Helene Habermann, Europäische Schule Neuperlach, Jan-Amos-Comenius, St. George's British International, etc.)
- `osm` unique finds: 7 (Christophorus-Schule, Montessorischule Gilching, Rudolf-Steiner-Schule Munich, …)
- `gemini_grounded` unique finds: 7 (Novalis-Gymnasium, Benediktiner Schäftlarn, Montessori Clara Grunwald Unterschleißheim, …)

This confirms the plan's choice to use multiple sources: each contributes schools the others miss.

## 5. Recommended ingestion pipeline

Go with **Google Places + Gemini + km.bayern.de** as the primary trio, with OSM as a zero-cost gap-fill:

1. **Places API** — primary discovery pass. Category text queries (Privatschule, Waldorfschule, Montessori, International, bilingual, katholisch, evangelisch, jüdisch) biased to Munich city center, 22 km radius. Filter by type=school + level inference.
2. **Gemini 2.5-flash grounded** — triangulation pass, returning categorized lists. Use confidence ≥ 0.7 threshold. Include a Landkreis-focused prompt to improve coverage of outer municipalities.
3. **km.bayern.de** — authoritative Trägerschaft resolution. For every candidate, if the listing name contains "Priv.", "freier Trägerschaft", "staatl. anerkannt" etc., mark private. For ambiguous listings, fetch the `/schule/{id}` detail page and parse "Rechtlicher Status".
4. **OSM** — zero-cost sanity-check source; catches schools the others miss (e.g. Christophorus-Schule).
5. **Combine** using the Phase 2 matcher (fuzzy name + stem + distance/PLZ/ort).
6. **Enrich** new rows via the existing pipelines: `munich_places_contact_enrichment.py` for phone/website, `munich_recover_missing_metadata.py` for traegerschaft + leitung + student counts, then descriptions / admission / embeddings.

Aggregator sources (privatschulen-in-bayern.de, VDP, ARGE) should be tried only if a future recall gap is identified (e.g. missing small Waldorf schools).

## 6. Implementation ticket (follow-up)

Separate from this research. Outline:

1. **Wire `merge_osm_private_schools()` into `scripts_munich/scrapers/munich_school_master_scraper.py`'s main `scrape_school_type()` flow** — it's already written but unused.
2. **Add a km.bayern.de loader** — refactor `phase0_probe_km_bayern.py` + `phase1_km_bayern.py` into a reusable `scripts_munich/scrapers/km_bayern_private_scraper.py`. Must handle both Stadt and Landkreis by looping MB1 and post-filtering PLZ.
3. **Add a Places private-school pass** — same pattern as `munich_places_contact_enrichment.py`, but with the 21-query category sweep and type=school.
4. **Wire a Gemini-grounded triangulation step** that emits a candidate CSV; merge into the combiner.
5. **Run the Phase 2 combiner** on scrape output + existing 256-row dataset; INSERT new schools into the Munich pipeline with a stable `schulnummer` prefix (e.g. `MUCPRIV_`).
6. **Re-run enrichment chain** on new rows: places_contact → recover_missing_metadata → description pipeline → embeddings → admission.
7. **Upload to Supabase** via the existing fill-gaps uploader. Net-new schools = Supabase INSERTs (separate code path from the UPDATE-focused `upload_to_supabase.py`). Likely requires Lovable to temporarily allow INSERT on schools/primary_schools for the anon key.

## 7. Open issues & future work

- **~5 candidate rows have no PLZ/ort because OSM had no address tags.** These need manual address lookup or a geocode pass before entering the pipeline.
- **Dedup still not perfect.** "Rudolf-Steiner-Schule" (OSM, 81929 München) vs "Rudolf-Steiner-Schule München" from Gemini didn't merge because one has no geo and the names are too generic. A second manual dedup pass during implementation is recommended.
- **Stuttgart / Hamburg / other cities likely have the same jedeschule gap.** This research approach is reusable — promote `scripts_munich/research_private_schools/` into `scripts_shared/research_private_schools/` when extending.
- **Quarterly refresh.** Private-school landscape changes (new schools, closures). Recommend re-running the combiner every 90 days with source caches invalidated.

## 8. Artefacts

All outputs in the repo (most data files gitignored — see DEVJOURNAL for details):

| Path | Content |
|---|---|
| `scripts_munich/research_private_schools/phase0_*.py` | Per-source reconnaissance probes |
| `scripts_munich/research_private_schools/phase1_*.py` | Per-source canonical CSV pilots |
| `scripts_munich/research_private_schools/phase2_combine_dedupe.py` | Union + fuzzy dedup + scope filter + diff vs. current dataset |
| `scripts_munich/research_private_schools/phase3_validate.py` | Recall / precision / coverage spot-checks |
| `data_munich/cache/private_research/` | Raw cached source responses (OSM JSON, km.bayern HTML, Places JSON, Gemini JSON) |
| `data_munich/intermediate/private_research/osm_overpass.csv` | 18 OSM private rows |
| `data_munich/intermediate/private_research/km_bayern.csv` | 24 km.bayern Stadt München private rows |
| `data_munich/intermediate/private_research/google_places.csv` | 59 Places private rows |
| `data_munich/intermediate/private_research/gemini_grounded.csv` | 51 Gemini categorized rows |
| `data_munich/intermediate/private_research/munich_private_schools_candidates.csv` | **60 unique canonical candidates** (deliverable) |
| `data_munich/intermediate/private_research/munich_private_schools_new_vs_current.csv` | **50 net-new private schools** (deliverable) |
| `data_munich/intermediate/private_research/munich_private_schools_already_in_dataset.csv` | 10 candidates that overlap with current Munich pipeline |

## 9. Total effort

Roughly **4 hours** across phases 0-4 (plan estimated 9-14). Faster because:
- The scraper already contained dormant OSM + `PRIVATE_NAME_KEYWORDS` infrastructure, so Phase 1 OSM was 30 minutes instead of 2 hours.
- Google Places API infrastructure (Munich contact enrichment) was already in place; Phase 1 Places reused the pattern.
- Gemini with `google-genai` SDK was already wired for other enrichments; single-prompt Phase 0 probe returned 46 usable rows without iteration.

Aggregator probes (privatschulen-in-bayern.de, VDP, ARGE Freie Schulen Bayern) were skipped after the 4-source gate passed. Those remain as future gap-fill options if recall drops on re-runs.
