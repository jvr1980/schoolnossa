# Data Source Refresh Audit & Update Plan — August 2026

*Audit date: 2026-08-22. Covers all German city pipelines (Berlin, Hamburg, Munich, Frankfurt, Stuttgart, Leipzig, Dresden, Bremen, NRW/Düsseldorf+Köln).*

---

## TL;DR

**Yes, but not all of it, and not in one go.**

The German asset is uniformly on **school year 2024/25** student/teacher figures (~2,600 rows). Meanwhile:

- **Berlin's own portal now serves SJ 2026/27** — we are two school years behind on our flagship city.
- **Munich's scraper is pinned to a January 2025 snapshot** despite the upstream publishing fresh snapshots roughly weekly (latest: 2026-08-15). That's a one-line fix worth ~19 months of drift.
- **NRW and Hessen — the two biggest annual drops — have not published 2026/27 yet.** Their historical cadence is early-to-late September. Refreshing them today gets us nothing.

So: run a **Wave A** refresh now on the live/rolling sources (cheap, high value), then a **Wave B** in late September/October once the annual statistical releases land. A single "refresh everything" run today would have to be repeated in five weeks.

---

## 1. Source inventory & update status

### 1.1 Base school registries (one per city)

| City | Source | Cadence | In our asset | Available now | Verdict |
|---|---|---|---|---|---|
| **Berlin** (sec + prim) | `bildung.berlin.de/Schulverzeichnis` | Rolling; SJ flips ~Aug | SJ 2024/25 | **SJ 2026/27** (portal selector default) | 🔴 **Update now** — 2 years stale |
| **Berlin** statistics | `bildungsstatistik.berlin.de` SVZ_Fakt5 / *Blickpunkt Schule* | Annual, ~Nov–Dec | 2023/24 file + 2024/25 | *Blickpunkt Schule* **2025/26** published | 🟡 Update to 25/26 now, or fold into Wave B |
| **Hamburg** (sec + prim) | `geodienste.hamburg.de` `HH_WFS_Schulen` (live WFS) | Rolling; CKAN meta modified **2026-07-03** | Pulled Apr 2026 | Current | 🟢 **Update now** — cheap re-pull |
| **Munich** | `jedeschule.codefor.de` CSV snapshots | Rolling, ~weekly | Pinned **`2025-01-04`** | **`2026-08-15`** | 🔴 **Update now** — 19 months stale, one-line change |
| **NRW** (Düsseldorf, Köln) | `schulministerium.nrw` `schulliste_sj_25_26_open_data.csv` | Annual, **late Sept** | SJ 25/26 | SJ 25/26 (last-mod **2025-09-23**); no 26/27 file yet (404) | ⏸️ **Wait** — 26/27 due ~late Sept 2026 |
| **Frankfurt** | `statistik.hessen.de` `verz-6_25_0.xlsx` | Annual, **early Sept** | verz-6_25 | verz-6_25 (last-mod **2025-09-09**); no verz-6_26 yet (404) | ⏸️ **Wait** — verz-6_26 due ~early Sept 2026 |
| **Stuttgart** | `stuttgart.de/organigramm/adressen` (live RSS+HTML scrape) | Rolling | Scraped Apr 2026 | Current | 🟢 Update now — cheap re-scrape |
| **Leipzig** | `opendata.leipzig.de` | Annual | SJ 2024/25 | Still SJ 2024/25 | ⏸️ **Wait** |
| **Dresden** | `schuldatenbank.sachsen.de/api/v1/schools` (live API) | Rolling | Pulled Apr 2026 | Current, API verified live | 🟢 Update now — cheap re-pull |
| **Bremen** | `gdi2.geo.bremen.de` INSPIRE shapefile + Schulwegweiser XLSX | Annual-ish, undated | Pulled Apr 2026 | Undated — needs a fetch-and-diff | 🟡 Re-pull & diff |

### 1.2 Cross-city enrichment layers

| Layer | Source | Cadence | In our asset | Available now | Verdict |
|---|---|---|---|---|---|
| **Traffic accidents** | Unfallatlas (`unfallatlas.statistikportal.de`) | Annual, **July** | through 2024 | **2025 released July 2026** | 🔴 **Update now** — all cities, one shared job |
| **Crime (Berlin)** | Kriminalitätsatlas Berlin | Annual, ~Apr | through 2024 | **through 2025**, updated **2026-04-30** | 🔴 **Update now** |
| **Crime (other cities)** | BKA PKS | Annual, ~Apr | PKS 2024 | **PKS 2025**, published **2026-04-20** | 🟡 Update in Wave A |
| **Transit** | VBB / HVV / regional GTFS, `v6.bvg.transport.rest` | Rolling | Apr 2026 | Current | ⚪ Low value — stop locations barely move |
| **POI** | Google Places API | Rolling | Apr 2026 | Current | ⚪ **Skip** — ~$250 per 1,600 schools, POIs are near-static |
| **Berlin exams** | ISQ Berlin (Abitur/MSA/VERA) | Annual | 2024 | *Not verified in this audit* | ❓ Verify before Wave B |
| **Traffic counts** | Telraam / berlin-zaehlt.de | Rolling | Apr 2026 | Current | ⛔ **CC-BY-NC — do not refresh for commercial use** (see `DATA_SOURCES_LICENSING.md`) |

---

## 2. The case for splitting into two waves

The German annual statistics calendar is the whole argument:

```
Aug 2026   ← we are here
             Berlin Schulverzeichnis already on 26/27 (rolling)
             Munich jedeschule snapshots weekly
             Unfallatlas 2025 landed in July

early Sept  Hessen verz-6_26   (Frankfurt)
late Sept   NRW schulliste_sj_26_27  (Düsseldorf, Köln)
Oct–Nov     Leipzig / Saxony annual, Bremen Schulwegweiser
Nov–Dec     Berlin Blickpunkt Schule 2026/27
```

Refreshing NRW or Frankfurt today re-downloads the identical September-2025 file. Refreshing Berlin today closes a two-school-year gap. Those are different decisions and should not be bundled.

---

## 3. Update plan

### Wave A — now (late August)

Scope: sources already ahead of us. No waiting, no wasted work.

| # | Task | Change | Effort |
|---|---|---|---|
| A1 | **Munich snapshot bump** | `JEDESCHULE_CSV_URL` → `jedeschule-data-2026-08-15.csv` in `scripts_munich/scrapers/munich_school_master_scraper.py:53`. De-hardcode: resolve latest snapshot at runtime. | 30 min |
| A2 | **Berlin re-scrape (sec + prim)** | Re-run `bildung_berlin_*_scraper.py` against SJ 2026/27. Expect column/format drift after two school years — budget for it. | 0.5–1 day |
| A3 | **Hamburg / Dresden / Stuttgart re-pull** | Re-run the three live-source scrapers. Mechanical. | 2 h |
| A4 | **Unfallatlas 2025** | Refresh shared traffic enrichment, re-run for all cities. | 3 h |
| A5 | **Crime 2025** | Kriminalitätsatlas 2025 (Berlin) + BKA PKS 2025 (other cities). | 3 h |
| A6 | **Bremen fetch-and-diff** | Pull INSPIRE + Schulwegweiser, diff against April copy; only rebuild if changed. | 1 h |
| A7 | **Schema drift check + QA** | `/schema-drift-check` and `/pipeline-qa` across touched cities. | 0.5 day |
| A8 | **Supabase upload** | Reuse `scripts_shared/upload_to_supabase.py` field-group flow. | 2 h |

**Explicitly out of scope for Wave A:** Google Places POI re-enrichment (cost, near-zero change), embeddings regeneration (only if descriptions change), Telraam (licensing).

### Wave B — late September / October

Trigger: the September annual releases actually appearing.

| # | Task | Trigger |
|---|---|---|
| B1 | Frankfurt → `verz-6_26_0.xlsx` | Hessen publishes, ~early Sept |
| B2 | NRW → `schulliste_sj_26_27_open_data.csv` | Schulministerium publishes, ~late Sept |
| B3 | Leipzig SJ 2025/26 | `opendata.leipzig.de` refresh |
| B4 | Berlin Bildungsstatistik → 2025/26 (or 2026/27 if out) | *Blickpunkt Schule* |
| B5 | ISQ exam data verification + refresh | Verify vintage first |
| B6 | Full cross-city QA, schema drift, Supabase upload | After B1–B5 |

### Wave C — deferred / optional

- POI re-enrichment (only if Google Places ToS compliance work happens anyway — we currently cache more than Place IDs, which `DATA_SOURCES_LICENSING.md` flags as a ToS problem).
- Embeddings + similar-schools regeneration.
- Transit GTFS refresh.

---

## 4. Recommended automation

Two of this audit's findings are avoidable-by-automation:

1. **Pinned snapshot URLs rot silently.** Munich sat 19 months behind because a URL was hardcoded. Any rolling source should resolve "latest" at runtime.
2. **Annual releases are predictable.** Hessen ≈ Sept 9, NRW ≈ Sept 23, Unfallatlas ≈ July, PKS ≈ April, Kriminalitätsatlas ≈ April.

Proposal: a `scripts_shared/check_source_freshness.py` that HEAD-probes every versioned source URL, compares `Last-Modified` / probes next-year filename patterns, and writes a status table. Run it monthly (or as a scheduled task) so Wave B triggers itself instead of relying on someone remembering.

---

## 5. Risk notes

- **Berlin two-year jump is the main technical risk.** Scrapers written against SJ 2024/25 markup may break against 2026/27. Do Berlin first in Wave A so there's time to react.
- **Column vintage churn.** `schueler_2024_25` → `schueler_2026_27` renames ripple through schema mappers, the Supabase schema, and the frontend. Decide now whether to (a) keep adding year-suffixed columns, or (b) introduce a stable `schueler_current` + `data_school_year` pair. **(b) is strongly recommended** — it stops this being a schema migration every single year.
- **Supabase/Lovable coordination.** The April backfill needed temporary RLS policies. Same coordination will be needed again; plan it into Wave A rather than discovering it at upload time.

---

*Author: Claude Opus 5 · Audit method: live HEAD/GET probes of every source URL in `scripts_*/scrapers/` and `scripts_*/enrichment/`, cross-checked against publisher landing pages.*
