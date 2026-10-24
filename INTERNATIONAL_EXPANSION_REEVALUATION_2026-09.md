# International Expansion Re-evaluation — September 2026

Re-evaluation of NL / UK / FR / IT / ES expansion priority for the SchoolNossa data asset,
measured against the Berlin benchmark profile (262 columns: contact, students/teachers,
languages, performance + estimates, enrollment pressure, tuition, descriptions, crime/
traffic/transit/POI, similar schools, embeddings).

**Previous ranking (April 2026):** NL > UK > FR > IT > ES
**New ranking (data richness):** **FR ≈ UK > NL > IT > ES**
**Recommended build order (richness × sunk investment):** **1. NL (finish) → 2. GB (close gaps) → 3. FR (first new build) → 4. IT → 5. ES (Madrid-first, optional)**

---

## 1. What is already done

| Country | State | Detail |
|---|---|---|
| NL | **~80% built** | 1,626 schools × 268 cols, final master table (2026-04-20). Students/teachers ~92–93%, exams ~70%, crime ~100%, languages 88%, descriptions 84%, phone 88%, website 76%. Missing: tuition, Inspectorate rating, email, enrollment pressure, embeddings/similar schools, school track. |
| GB | **~60% built** | 5,303 schools × 268 cols (England). Attainment 8 at 80%, crime/traffic 100%, descriptions 77%. Big holes: students 25%, teachers 3%, Ofsted 0%, IMD/FSM 0%, POI 0%, no tuition, languages 34% (LLM-derived only). |
| FR / IT / ES | **Scaffolding only** | Empty `scripts_international/{fr,it,es}` + `data_{fr,it,es}` dirs. No code. |
| Scoping | Done (April 2026) | `scripts_international/country_config.py` — full source registry, 8 categories × 5 countries. `orchestrator_template.py`, `international_to_berlin_schema.py`, shared description pipeline exist and are proven on NL/GB. |

Note: `sprachen` / `besonderheiten` in NL/GB come from the Gemini description pipeline, not
registries — language/offering coverage is achievable in any country we run descriptions for.

---

## 2. Per-country scorecards (September 2026 re-research)

Scores: ✅ open per-school data · 🟡 partial / scrape / fragmented · ❌ absent

### France — richest data of any country incl. Germany. NEW #1 on pure data
| Dimension | Status |
|---|---|
| Consolidation | ✅ Single national spine, UAI code, Licence Ouverte, Opendatasoft API |
| Contact | ✅ Annuaire: email **99.3%**, phone 98.7%, web 73% (14,819 collèges+lycées) |
| Students | ✅ Effectifs per school by grade/sex/LV, public **and** privé sous contrat, 2019–2025 |
| Teachers | ❌ No per-UAI staffing dataset (aggregate only) |
| Performance | ✅ IVAL/IVAC per school: bac/DNB rates, mentions, **value-added**, 2025 ed. publ. 2026-04 |
| Languages | ✅ `fr-en-offre-langues-2d`: every language per school LV1/2/3; AbiBac/Bachibac/Esabac; sections internationales; lycée spécialités with enrolments |
| Special offerings | ✅ Registry flags (arts/cinema/theatre/sport/internat/resto) + new sports-sections datasets |
| Tuition | 🟡 Scrape-only. Privé sous contrat ~96% of private pupils, €650–1,200/yr; hors contrat ~2,600 schools invisible in stats |
| Admission | 🟡 Affelnet pressure: per-académie PDFs only (Paris semi-structured). **Open days: ✅ ONISEP Idéo feed has JPO dates per UAI** |
| Social context | ✅ IPS per school (public since CADA 2022, updated Sept 2026) |
| Extras | ONISR accidents, SSMSI crime, national GTFS aggregator, INSEE FILOSOFI — all confirmed |

Verdict: ~85–90% of a Berlin-level profile from open national CSV/API, zero scraping for the core.

### England — best admission + inspection + workforce data. #2
| Dimension | Status |
|---|---|
| Consolidation | ✅ GIAS daily bulk CSV (~65k establishments, URN) |
| Contact | 🟡 Phone/website/head name ✅; **email restricted** (masked in public CSV) |
| Students | ✅ School census: enrollment, capacity, FSM% |
| Teachers | ✅ School Workforce Census per school: headcount/FTE, PTR (Nov 2025 publ. 2026-06) |
| Performance | ✅ KS4 (Attainment/Progress 8) + KS5 incl. per-school subject entries |
| Languages | 🟡 Derivable from KS5 subject files + EBacc language %; GCSE subject offerings not open |
| Special offerings | ✅ GIAS: faith, selective, boarding, sixth form; specialisms defunct since 2011 |
| Tuition | 🟡 Scrape ~1,100–1,300 independent secondaries; ISC census = averages only |
| Admission | ✅ **Applications & offers per school, preferences by rank, 2014–date** — best oversubscription signal anywhere |
| Inspection | ✅ Ofsted monthly MI per URN survived reform: report cards with 8 graded areas (since Nov 2025) |
| New | EES public API (beta); IMD 2025 on 2021 LSOAs (migration needed) |

Scotland/Wales/NI: separate mini-pipelines, ~40–60% of England effort each for thinner
profiles — only after England is exhausted.

### Netherlands — strong open core, gated qualitative layer. #3
| Dimension | Status |
|---|---|
| Consolidation | ✅ DUO national, monthly refresh, BRIN+vestiging |
| Contact | 🟡 Phone + website in DUO adressen; **email exists nowhere centrally** |
| Students/Teachers | ✅ DUO enrollment + staff FTE (already at 92–93% in our table) |
| Performance | ✅ DUO exams (in table at ~70%) + **Inspectorate oordelen open ODS incl. standards** (not yet ingested) |
| Languages/profiles | 🟡 Four small stable association scrapes: Nuffic TTO (~130), Technasium (~100), Cultuurprofiel (40), Topsport (30) |
| Tuition | 🟡 Only ~dozens of B3 particulier VO schools; per-site scrape (avg €24k/yr) |
| Admission | 🟡 Amsterdam only: Schoolwijzer open API + OSVO loting PDFs. Open days: VO Gids scrape |
| Rich layer | ❌/🟡 Scholen op de kaart (satisfaction, class sizes, support profiles) is formally **not open data** — VO-raad approval or hostile scrape |
| New | **CBS achterstandsscore per VO-vestiging** (SES indicator Berlin lacks) |

### Italy — data exists, half locked behind Scuola in Chiaro. #4
| Dimension | Status |
|---|---|
| Consolidation | ✅ dati.istruzione.it national CSVs (statali + paritarie, IODL 2.0) |
| Contact | 🟡 Email + PEC + website in anagrafe CSV; **no phone** (SiC scrape); **no coordinates** (geocode ~4k+ schools) |
| Students | ✅ Per school per indirizzo (= offerings + size per track) |
| Teachers | ✅ Personale CSVs |
| Performance | 🟡 Per-school maturità distributions, dropout, university credits + employment outcomes exist **only inside Scuola in Chiaro** (WAF-protected scrape / txt export); INVALSI per school non-public; open RAV self-evaluation CSVs as proxy |
| Languages | ✅ Via indirizzi (liceo linguistico etc.); ESABAC list fragmented |
| Tuition | ❌ Nothing structured; scrape paritarie sites |
| Admission | ❌ No per-school application data at all |
| Infra | 🟡 GTFS city-by-city (Milan/Rome/Turin fine, no aggregator); crime municipal for >250k comuni, else provincial |

### Spain — better than April verdict, but only cities-first. #5
| Dimension | Status |
|---|---|
| Consolidation | ❌ RECD national registry is search-only (no bulk); practical sources = 17 regional portals |
| Contact | ✅ (regions) Madrid CSV: 4×phone, 2×email, web, UTM coords; Catalonia SODA API 63 cols; Valencia CSV/JSON |
| Students | 🟡 Catalonia per-centre enrollment API; Madrid via ficha scrape; nationally provincial only |
| Performance | 🟡 **Madrid is the only region with per-school results** (EvAU since 2016-17, graduation rates) — official but **scrape-only** buscador ficha. Catalonia moved competències tests to 69-school samples in 2025-26 → per-school performance now structurally impossible there |
| Languages | 🟡 Bilingual-program flags semi-structured per region |
| Tuition | ❌ Nothing official; CICAE/OCU studies (Madrid concertado avg €136/mo) + international-school directories |
| Admission | 🟡 Catalonia standout: per-centre places + first-choice assignments as open API (demand pressure). Madrid: ephemeral PDFs |
| Verdict | Madrid ≈ Berlin-comparable but scrape-heavy; Barcelona structural-only; 17-community rollout impractical |

---

## 3. What changed vs April 2026

1. **France jumps from #3 to #1 on data richness.** April scoping missed: per-school language
   offerings dataset, IPS social index, ONISEP structured open-days feed, spécialité
   enrolments, sections binationales — all open. France now looks richer than Berlin itself.
2. **UK confirmed and upgraded**: Ofsted per-URN data survived the reform (multi-dimensional
   report cards since Nov 2025), workforce per school confirmed, applications-vs-offers
   per school is the best admission-pressure dataset in any country. New EES API + IMD 2025.
3. **NL still excellent but capped**: the richest qualitative layer (Scholen op de kaart) is
   licensing-gated; admission pressure structured only in Amsterdam. New: CBS per-school SES.
4. **Italy better than remembered**: per-school outcomes (incl. university/employment) exist
   via Scuola in Chiaro, city-level crime for big comuni — but scrape-gated, no admission data.
5. **Spain less hopeless than "weakest" if Madrid-first**, but per-school performance outside
   Madrid got *worse* (Catalonia sample-based testing from 2025-26).

## 4. Recommended sequence

1. **Finish NL** (small effort, ~80% done): ingest Inspectorate oordelen ODS + CBS
   achterstandsscore; 4 association scrapes for TTO/profiles; tuition scrape (~dozens);
   embeddings + similar schools; Amsterdam Schoolwijzer for admission pressure.
2. **Close GB gaps** (medium effort, sources are simple URN joins): school census enrollment,
   Workforce Census, Ofsted MI report cards, IMD 2025 + postcodes.io LSOA join,
   applications-and-offers, POI run, KS5 subject-derived languages.
3. **Build FR** (first new country, largest payoff per effort): Annuaire spine → effectifs →
   IVAL/IVAC → languages/sections/spécialités → IPS → ONISEP open days → standard
   enrichments (ONISR/SSMSI/GTFS/FILOSOFI). Core requires no scraping.
4. **Build IT** (after FR): anagrafe CSVs + geocoding + SiC scraper/txt ingester for the
   performance layer; city GTFS for Milan/Rome/Turin.
5. **ES Madrid-first, optional**: Madrid CSV + buscador ficha scrape; Barcelona structural
   profiles via Generalitat APIs; defer national rollout indefinitely.

---

## Appendix — key source URLs (verified Sept 2026)

**FR:** Annuaire `https://data.education.gouv.fr/explore/dataset/fr-en-annuaire-education/` ·
languages `fr-en-offre-langues-2d` · IVAC `fr-en-indicateurs-valeur-ajoutee-colleges` ·
IPS `fr-en-ips-colleges-ap2023` · spécialités `fr-en-effectifs-specialites-doublettes-terminale-generale` ·
effectifs collèges `fr-en-college-effectifs-niveau-sexe-lv` (all on data.education.gouv.fr) ·
open days: ONISEP Idéo-Structures `https://opendata.onisep.fr/data/5fa5816ac6a6e/2-ideo-structures-d-enseignement-secondaire.htm` ·
binational sections `https://www.data.gouv.fr/datasets/etablissements-avec-sections-binationales-abibac-bachibac-et-esabac`

**GB:** GIAS bulk `https://get-information-schools.service.gov.uk/` (daily CSV) ·
Ofsted MI `https://www.gov.uk/government/statistical-data-sets/monthly-management-information-ofsteds-school-inspections-outcomes` ·
workforce `https://explore-education-statistics.service.gov.uk/find-statistics/school-workforce-in-england/2025` ·
applications & offers `https://explore-education-statistics.service.gov.uk/find-statistics/primary-and-secondary-school-applications-and-offers/2025-26/data-guidance` ·
performance downloads `https://www.compare-school-performance.service.gov.uk/download-data` ·
EES API `https://www.api.gov.uk/dfe/explore-education-statistics-api/` ·
IMD 2025 `https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025`

**NL:** DUO adressen `https://onderwijsdata.duo.nl/datasets/adressen_vo` ·
Inspectorate oordelen `https://www.onderwijsinspectie.nl/trends-en-ontwikkelingen/onderwijsdata/oordelen` ·
TTO list `https://www.nuffic.nl/onderwijssectoren/voortgezet-onderwijs/tweetalig-onderwijs/alle-tto-scholen-in-nederland` ·
CBS achterstandsscores VO: via VO-raad `https://www.vo-raad.nl/nieuws/geactualiseerde-achterstandscores-cbs-bekend` ·
Amsterdam Schoolwijzer API `https://schoolwijzer.amsterdam.nl/en/api-documentation/` ·
B3 particulier list `https://www.onderwijsinspectie.nl/onderwijssectoren/particulier-onderwijs/rapporten-particuliere-scholen-vo` ·
Scholen op de kaart (licensing-gated) `https://scholenopdekaart.nl`

**IT:** open data catalog `https://dati.istruzione.it/opendata/opendata/catalogo/elements1/?area=Scuole`
(anagrafe DS0400/0410 w/ email+PEC+web; students per indirizzo DS0070; RAV DS0500–0530) ·
Scuola in Chiaro (per-school esiti/outcomes; scrape/txt export) `https://unica.istruzione.gov.it/cercalatuascuola/` ·
Eduscopio (display-only) `https://eduscopio.it` ·
Lombardia anagrafe w/ coords `https://www.dati.lombardia.it/Istruzione/Anagrafe-Scuole/fm99-kxtn`

**ES:** Madrid centros CSV `https://datos.comunidad.madrid/catalogo/dataset/centros_educativos` ·
Madrid buscador ficha (EvAU per school, scrape) `https://gestiona.comunidad.madrid/wpad_pub` ·
Catalonia directory API `https://analisi.transparenciacatalunya.cat/Educaci-/Directori-de-centres-docents-anual-Base-2020/kvmv-ahh4` ·
Catalonia enrollment `xvme-26kg` / places `vaht-2sjk` / preinscripció assignments `99md-r3rq` (same portal) ·
Valencia guía de centros `https://dadesobertes.gva.es/es/dataset/edu-centros` ·
RECD (search-only) `https://www.educacion.gob.es/centros/`
