# Frankfurt Secondary Schools — QA Report

**Generated:** 2026-03-30
**Source:** `frankfurt_secondary_school_master_table_final_with_embeddings.parquet`
**Schools:** 73
**Columns:** 337 (265 Berlin schema + 72 Frankfurt extras)

## Schema Alignment

| Check | Result |
|-------|--------|
| Berlin column order | PASS (265/265 columns in exact order) |
| Frankfurt extras appended | PASS (72 additional columns) |

## Core Fields (100% required)

| Field | Coverage | Status |
|-------|----------|--------|
| schulnummer | 73/73 (100%) | PASS |
| schulname | 73/73 (100%) | PASS |
| school_type | 73/73 (100%) | PASS |
| latitude | 73/73 (100%) | PASS |
| longitude | 73/73 (100%) | PASS |
| strasse | 73/73 (100%) | PASS |
| plz | 73/73 (100%) | PASS |
| stadt | 73/73 (100%) | PASS |
| bundesland | 73/73 (100%) | PASS |

## School Type Distribution

| Type | Count |
|------|-------|
| Weiterführende Schule | 38 |
| Gesamtschule | 20 |
| Gymnasium | 14 |
| Realschule | 1 |

## Enrichment Coverage

| Enrichment | Coverage | Status |
|------------|----------|--------|
| Transit accessibility score | 73/73 (100%) | PASS |
| Transit detail (45 cols) | avg 80% fill | PASS |
| Traffic accidents total | 73/73 (100%) | PASS |
| Traffic accidents/year | 73/73 (100%) | PASS |
| Crime total 2023 | 73/73 (100%) | PASS |
| Crime safety category | 73/73 (100%) | PASS |
| POI (6 categories) | avg 100% fill | PASS |
| Description | 73/73 (100%) | PASS |
| Embeddings (3072d Gemini) | 73/73 (100%) | PASS |
| Similar schools | 73/73 (100%) | PASS |
| Tuition display | 73/73 (100%) | PASS |

## Coordinate Validation

| Check | Result |
|-------|--------|
| All in Frankfurt bbox (50.0-50.25, 8.5-8.85) | 73/73 PASS |

## Data Integrity

| Check | Result |
|-------|--------|
| No duplicate schulnummer | PASS |
| Unique schulname | 73/73 PASS |

## Known Limitations

- **Crime data is city-level only** — Frankfurt does not publish Stadtteil-level PKS data, so all schools share the same crime statistics. The `crime_bezirk_index` is 1.0 for all schools.
- **38 schools classified as "Weiterführende Schule"** — These are schools whose specific type (Förderschule, Abendschule, etc.) doesn't map cleanly to the standard Gymnasium/Realschule/Gesamtschule categories. They include special education schools (Förderschulen), adult education (Schulen für Erwachsene), and other specialized types.
- **No academic performance data** — Hessen does not publish Abitur/MSA pass rates at school level. The `abitur_*` and `msa_*` columns are NULL.

## Overall: PASS
