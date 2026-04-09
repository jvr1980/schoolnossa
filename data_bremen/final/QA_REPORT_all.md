# QA Report: Bremen Schools

**Date:** 2026-04-09
**Pipeline version:** b36c586 Add config.yaml key loading to Bremen website metadata enrichment
**Input:** bremen_school_master_table_final.csv

## Summary

| Metric | Value | Status |
|--------|-------|--------|
| Total schools | 253 | OK |
| Coordinate coverage | 99.6% | OK |
| Outside Germany bounds | 0 | OK |
| Duplicate IDs | 0 | OK |
| Embedding coverage | 100.0% | OK |
| Embedding dimension | 3072 | OK |
| Similar schools | 100.0% | OK |
| All-NULL columns | 7 | OK |
| Berlin schema match | False | FAIL |
| Berlin cols populated | 119/265 | OK |

## Enrichment Coverage

| Enrichment | Key Column | Coverage | Status |
|------------|-----------|----------|--------|
| Traffic | traffic_accidents_total | 99.6% | OK |
| Traffic | traffic_accidents_per_year | 99.6% | OK |
| Transit | transit_accessibility_score | 100.0% | OK |
| Transit | transit_stop_count_1000m | 100.0% | OK |
| Crime | crime_total | 81.4% | OK |
| Crime | crime_safety_score | 81.4% | OK |
| Crime | crime_safety_category | 81.4% | OK |
| POI | poi_supermarket_count_500m | 99.6% | OK |
| POI | poi_restaurant_count_500m | 99.6% | OK |
| POI | poi_park_count_500m | 99.6% | OK |
| Description (auto) | description | 100.0% | OK |
| Description (DE) | description_de | 23.3% | LOW |
| Description (EN) | description_en | 23.3% | LOW |
| Tuition | tuition_tier | 1.2% | WARN |
| Tuition | income_based_tuition | 1.2% | WARN |

## School Breakdown

| Type | Count |
|------|-------|
| Grundschule | 113 |
| Oberschule | 54 |
| Berufsbildende Schule | 53 |
| Sonstige | 18 |
| Gymnasium | 11 |
| Waldorfschule | 3 |
| Förderzentrum | 1 |

## Description Quality

| Metric | Value |
|--------|-------|
| Auto descriptions | 253/253 (100%) |
| Avg auto length | 232 chars |
| Rich DE descriptions | 59/253 (23%) |
| Avg rich DE length | 1297 chars |
| Besonderheiten | 65/253 |
| Gruendungsjahr | 38/253 |

## Tuition (Private Schools)

| School | Tier | Monthly | Income-based |
|--------|------|---------|-------------|
| Freie Evangelische Bekenntnisschule Bremen | medium | EUR150.0 | True |
| Privatschule Mentor gGmbH | medium | EUR200.0 | False |
| Freie Gemeinschaftsschule Bremen | medium | EUR150.0 | True |

## Embedding & Similar Schools

- Dimension: 3072
- Coverage: 100.0%
- Similar schools: 100.0%

## Berlin Schema Alignment

- Bremen final: 235 columns
- Berlin-schema file: 264 columns, match=False
- Populated in Berlin schema: 119/265

## NULL Columns (7)

- Schulart10
- Schulart11
- transit_rail_01_lines
- transit_rail_02_lines
- transit_rail_03_lines
- transit_tram_01_lines
- transit_tram_03_lines

## Recommendations

- Run description pipeline for primary schools (113 Grundschulen) to increase DE/EN coverage from 23% to ~70%
- Run tuition pipeline for 3 primary private schools
- Consider demographics enrichment (Kleinraumig Infosystem) for Ortsteil-level social indicators
- Coordinate coverage at 99.6% — 1 school missing (acceptable)
