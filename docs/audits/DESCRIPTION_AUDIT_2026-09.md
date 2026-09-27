# School description audit — September 2026

**Question:** how often do the AI-researched school descriptions (Gemini + Google Search, via the `research-school-descriptions` / `generate-school-descriptions` jobs) contain specific claims we cannot back up?

## Method
- **Sample:** 30 German schools, stratified: 2 secondary + 1 primary per city × 10 cities, reproducible random order (`md5(id || 'audit-2026-09-27')`). Luisen-Gymnasium was excluded because it had just been rewritten.
- **Claims:** every specific, checkable claim in the texts users see (`description_de`, `description_en`) — numbers, dates, programmes, partners, awards, facilities, staff, location, languages, care. Generic phrases were skipped.
- **Sources:** the school's own website (homepage + up to 4 relevant subpages), our register fields (address, type, languages, `besonderheiten`) and official school portals. Five AI agents did the checking; every verdict records a URL and a short quote.
- **Verdicts:**
  - `verified` — a source states the claim.
  - `not_found` — none of the checked pages mention it (unsourced, not necessarily false).
  - `contradicted` — a source says otherwise.
- **Double-check:**
  - All 18 `contradicted` verdicts were re-confirmed against the raw page or PDF text (18/18 hold).
  - A random 40 of the `verified` verdicts were re-fetched. 36 of the 37 fetchable pages confirm the quoted evidence. One JavaScript-rendered page was inconclusive, and 3 pages block scripted access.
- **Limits:** "not found" means the few pages checked do not state it; the claim may appear elsewhere. School websites can also be outdated. Claim-level data: `description_audit_2026-09_claims.json`.

## Results (485 claims, 30 schools)
| | claims | share |
|---|---|---|
| verified | 397 | 82% |
| not found (unsourced) | 70 | 14% |
| contradicted — minor (rounding ≤~10%, near-miss place names) | 8 | 2% |
| contradicted — material (a parent would be misled) | 10 | 2% |

- **Per school:** only 2 of 30 schools had every claim verified; 15 of 30 had at least one contradicted claim; **10 of 30 (one in three; 95% CI ≈ 17–53%) had at least one materially wrong claim**.
- **By category** (unsourced + wrong):
  - worst: numbers 47% (student/teacher counts), awards 50% (n=8), facilities 24%, location 21%, programmes 19%
  - best: staff 0%, languages 4%, partners 9%, care 11%
- **Error pattern:** mostly **outdated** facts rather than inventions — ended programmes (TAFF pilot, England trips since 2019, a lost British partner school), stale student counts (735 vs 854, ~1,000 vs 1,261) and a wrong move date. Then **wrong locations** (Zuffenhausen instead of Stammheim) and a few embellishments: "school-owned goats and sheep" (only planned), an art studio described as "traditional craftsmanship", "DGNB Platinum awarded" (only "angestrebt"). One description repeated our own implausible `schueler_current` (1,882 for a second-year Leipzig school).

### Material errors found (all corrected in Supabase 2026-09-27; see DEVJOURNAL)
| School | Claim | Source says |
|---|---|---|
| KGS Heßhofstraße, Köln | school-owned goats and sheep | sheep left with a staff member in 2024; goats/sheep only planned |
| Mittelschule Blumenau, München | TAFF = current transition programme | talent pilot from 2015/16, 4 years, ended |
| Kant-Gymnasium, Berlin | ~735 students | 854 (homepage) |
| 76. Oberschule, Dresden | annual Great Britain trip | "Derzeit finden keine Fahrten nach England statt" |
| Matthias-Claudius-Schule, Düsseldorf | Generationenwerkstatt = traditional craftsmanship | "wie ein Künstleratelier eingerichtet" |
| Stadtteilschule Helmuth Hübener, Hamburg | ~1,000 students | 1,261 in 2024/25 (portal) |
| Park-Realschule, Stuttgart | "im Herzen von Zuffenhausen" | Park-Realschule **Stammheim** |
| Gesamtschule Wasseramselweg, Köln | moved in May 2024 | building handed over May 2024, move after the summer holidays |
| Georg-Droste-Schule, Bremen | 15 teachers | staff page lists ~24 |
| Max-Ernst-Gesamtschule, Köln | exchange with Great Britain | "derzeit wird eine neue Schule gesucht" |

## Side findings (data scope, not descriptions)
- **Schools outside their city:**
  - Leipzig tables hold 8 schools outside Leipzig (3 in Dresden, plus Meißen, Zwochau, Dommitzsch, Zwickau, Frankenhausen).
  - "Bremen" holds 47 Bremerhaven schools.
  - Munich holds 8 private schools in suburbs.
- **Bad postcodes:** 4 Frankfurt primaries have `plz = '0None'`; 1 Berlin primary has none.
- **Websites:** stale URLs (KGS Heßhofstraße's stored domain is dead; Tagore-Gymnasium's `www.` host has a certificate error).
- **Student counts:** ours differ from official portals for some schools (Helmuth Hübener: 996 vs 1,261).

## Already changed
- Since 2026-09-27 the research job stores its Google Search sources in `description_grounding`, so future claims are traceable (see DEVJOURNAL).
