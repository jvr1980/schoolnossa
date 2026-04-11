# Frankfurt Primary Schools — QA Report

**Generated:** 2026-03-30
**Source:** `frankfurt_primary_school_master_table_final_with_embeddings.parquet`
**Schools:** 85
**Columns:** 330 (227 Berlin schema + 103 Frankfurt extras)

## Schema Alignment

| Check | Result |
|-------|--------|
| Berlin column order | PASS (227/227 columns in exact order) |
| Frankfurt extras appended | PASS (103 additional columns) |

## Core Fields (100% required)

| Field | Coverage | Status |
|-------|----------|--------|
| schulnummer | 85/85 (100%) | PASS |
| schulname | 85/85 (100%) | PASS |
| school_type | 85/85 (100%) | PASS |
| latitude | 85/85 (100%) | PASS |
| longitude | 85/85 (100%) | PASS |
| strasse | 85/85 (100%) | PASS |
| plz | 85/85 (100%) | PASS |
| stadt | 85/85 (100%) | PASS |
| bundesland | 85/85 (100%) | PASS |

## School Type Distribution

| Type | Count |
|------|-------|
| Grundschule | 85 |

## Enrichment Coverage

| Enrichment | Coverage | Status |
|------------|----------|--------|
| Transit accessibility score | 85/85 (100%) | PASS |
| Transit detail (45 cols) | avg 80% fill | PASS |
| Traffic accidents total | 85/85 (100%) | PASS |
| Traffic accidents/year | 85/85 (100%) | PASS |
| Crime total 2023 | 85/85 (100%) | PASS |
| Crime safety category | 85/85 (100%) | PASS |
| POI (7 categories) | avg 86% fill | PASS |
| Description | 85/85 (100%) | PASS |
| Embeddings (3072d Gemini) | 85/85 (100%) | PASS |
| Similar schools | 85/85 (100%) | PASS |
| Tuition display | 85/85 (100%) | PASS |

## Coordinate Validation

| Check | Result |
|-------|--------|
| All in Frankfurt bbox | 84/85 PASS |
| Outlier | Käthe-Kollwitz-Schule (lon=8.4956) — western Frankfurt, legitimate |

## Data Integrity

| Check | Result |
|-------|--------|
| No duplicate schulnummer | PASS |
| Unique schulname | 85/85 PASS |

## Known Limitations

- **Crime data is city-level only** — Frankfurt does not publish Stadtteil-level PKS data. All schools share the same aggregate statistics.
- **No academic performance data** — Hessen does not publish school-level Grundschule test results. Performance columns are NULL.
- **ndH (non-German native language) used as belastungsstufe proxy** — Hessen reports ndH counts in Verzeichnis 6, mapped to a 1-9 scale as an approximation of Berlin's Belastungsstufe.

## Overall: PASS
