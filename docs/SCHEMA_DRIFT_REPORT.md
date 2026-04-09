# Schema Drift Report

**Date:** 2026-04-09
**Reference:** Berlin secondary (265 columns)
**Cities compared:** berlin_secondary, bremen, bremen_primary, bremen_secondary

## Summary

| City | Rows | Total Cols | Common w/ Berlin | Missing | Extra | Type Mismatches | Order OK | Score |
|------|------|-----------|-----------------|---------|-------|-----------------|----------|-------|
| berlin_secondary | 259 | 265 | — (reference) | — | — | — | — | 100% |
| bremen | 253 | 225 | 113 | 152 | 112 | 5 | No | 32.6% |
| bremen_primary | 113 | 141 | 64 | 201 | 77 | 6 | No | 12.2% |
| bremen_secondary | 65 | 152 | 72 | 193 | 80 | 7 | No | 13.2% |

## Missing Columns by City

### bremen (152 missing)

**crime** (37): crime_aggravated_assault_2023, crime_aggravated_assault_2024, crime_aggravated_assault_avg, crime_aggravated_assault_yoy_pct, crime_assault_2023, crime_assault_2024, crime_assault_avg, crime_assault_yoy_pct, crime_bike_theft_2023, crime_bike_theft_2024 ... +27 more

**poi** (33): poi_bakery_cafe_01_address, poi_bakery_cafe_01_distance_m, poi_bakery_cafe_01_latitude, poi_bakery_cafe_01_longitude, poi_bakery_cafe_01_name, poi_bakery_cafe_02_address, poi_bakery_cafe_02_distance_m, poi_bakery_cafe_02_latitude, poi_bakery_cafe_02_longitude, poi_bakery_cafe_02_name ... +23 more

**description** (3): description_de, summary_de, summary_en

**demographic** (3): belastungsstufe, migration_2023_24, migration_2024_25

**metadata** (76): abitur_durchschnitt_2023, abitur_durchschnitt_2024, abitur_durchschnitt_2025, abitur_durchschnitt_estimated, abitur_durchschnitt_estimated_lower, abitur_durchschnitt_estimated_rebased, abitur_durchschnitt_estimated_rebased_lower, abitur_durchschnitt_estimated_rebased_upper, abitur_durchschnitt_estimated_upper, abitur_erfolgsquote_2024 ... +66 more


### bremen_primary (201 missing)

**crime** (37): crime_aggravated_assault_2023, crime_aggravated_assault_2024, crime_aggravated_assault_avg, crime_aggravated_assault_yoy_pct, crime_assault_2023, crime_assault_2024, crime_assault_avg, crime_assault_yoy_pct, crime_bike_theft_2023, crime_bike_theft_2024 ... +27 more

**poi** (81): poi_bakery_cafe_01_address, poi_bakery_cafe_01_distance_m, poi_bakery_cafe_01_latitude, poi_bakery_cafe_01_longitude, poi_bakery_cafe_01_name, poi_bakery_cafe_02_address, poi_bakery_cafe_02_distance_m, poi_bakery_cafe_02_latitude, poi_bakery_cafe_02_longitude, poi_bakery_cafe_02_name ... +71 more

**description** (3): description_de, summary_de, summary_en

**demographic** (3): belastungsstufe, migration_2023_24, migration_2024_25

**metadata** (77): abitur_durchschnitt_2023, abitur_durchschnitt_2024, abitur_durchschnitt_2025, abitur_durchschnitt_estimated, abitur_durchschnitt_estimated_lower, abitur_durchschnitt_estimated_rebased, abitur_durchschnitt_estimated_rebased_lower, abitur_durchschnitt_estimated_rebased_upper, abitur_durchschnitt_estimated_upper, abitur_erfolgsquote_2024 ... +67 more


### bremen_secondary (193 missing)

**crime** (37): crime_aggravated_assault_2023, crime_aggravated_assault_2024, crime_aggravated_assault_avg, crime_aggravated_assault_yoy_pct, crime_assault_2023, crime_assault_2024, crime_assault_avg, crime_assault_yoy_pct, crime_bike_theft_2023, crime_bike_theft_2024 ... +27 more

**poi** (81): poi_bakery_cafe_01_address, poi_bakery_cafe_01_distance_m, poi_bakery_cafe_01_latitude, poi_bakery_cafe_01_longitude, poi_bakery_cafe_01_name, poi_bakery_cafe_02_address, poi_bakery_cafe_02_distance_m, poi_bakery_cafe_02_latitude, poi_bakery_cafe_02_longitude, poi_bakery_cafe_02_name ... +71 more

**description** (3): description_de, summary_de, summary_en

**demographic** (2): belastungsstufe, migration_2023_24

**metadata** (70): abitur_durchschnitt_2023, abitur_durchschnitt_2024, abitur_durchschnitt_2025, abitur_durchschnitt_estimated, abitur_durchschnitt_estimated_lower, abitur_durchschnitt_estimated_rebased, abitur_durchschnitt_estimated_rebased_lower, abitur_durchschnitt_estimated_rebased_upper, abitur_durchschnitt_estimated_upper, abitur_erfolgsquote_2024 ... +60 more


## Type Mismatches


### bremen

| Column | Berlin Type | City Type | Severity |
|--------|-----------|-----------|----------|
| transit_rail_03_lines | object | float64 | LOW |
| transit_tram_03_lines | object | float64 | LOW |
| transit_tram_01_lines | object | float64 | LOW |
| transit_rail_01_lines | object | float64 | LOW |
| transit_rail_02_lines | object | float64 | LOW |

### bremen_primary

| Column | Berlin Type | City Type | Severity |
|--------|-----------|-----------|----------|
| transit_rail_03_lines | object | float64 | LOW |
| transit_tram_01_lines | object | float64 | LOW |
| transit_rail_01_lines | object | float64 | LOW |
| transit_rail_02_lines | object | float64 | LOW |
| schulnummer | object | int64 | LOW |
| transit_tram_03_lines | object | float64 | LOW |

### bremen_secondary

| Column | Berlin Type | City Type | Severity |
|--------|-----------|-----------|----------|
| transit_rail_03_lines | object | float64 | LOW |
| transit_tram_01_lines | object | float64 | LOW |
| transit_tram_02_lines | object | float64 | LOW |
| transit_rail_01_lines | object | float64 | LOW |
| transit_rail_02_lines | object | float64 | LOW |
| schulnummer | object | int64 | LOW |
| transit_tram_03_lines | object | float64 | LOW |

## Naming Convention Issues

Columns not following `lowercase_with_underscores`:

- **bremen**: Cafeteria, Fax, GanztagsForm, Ganztagsschule_ab, Internet, Kiosk, LKW_Anfahrt, Liegenschaftsbetreuer, LiegenschaftsbetreuerEmail, Mensa
- **bremen_primary**: Cafeteria, Fax, GanztagsForm, Ganztagsschule_ab, Internet, Kiosk, LKW_Anfahrt, Liegenschaftsbetreuer, LiegenschaftsbetreuerEmail, Mensa
- **bremen_secondary**: Cafeteria, Fax, GanztagsForm, Ganztagsschule_ab, Internet, Kiosk, LKW_Anfahrt, Liegenschaftsbetreuer, LiegenschaftsbetreuerEmail, Mensa

## Shared Extra Columns (candidates for schema promotion)

Columns appearing in 2+ non-Berlin cities:

| Column | Count | Cities |
|--------|-------|--------|
| Cafeteria | 3 | bremen, bremen_primary, bremen_secondary |
| Fax | 3 | bremen, bremen_primary, bremen_secondary |
| GanztagsForm | 3 | bremen, bremen_primary, bremen_secondary |
| Ganztagsschule_ab | 3 | bremen, bremen_primary, bremen_secondary |
| Internet | 3 | bremen, bremen_primary, bremen_secondary |
| Kiosk | 3 | bremen, bremen_primary, bremen_secondary |
| LKW_Anfahrt | 3 | bremen, bremen_primary, bremen_secondary |
| Liegenschaftsbetreuer | 3 | bremen, bremen_primary, bremen_secondary |
| LiegenschaftsbetreuerEmail | 3 | bremen, bremen_primary, bremen_secondary |
| Mensa | 3 | bremen, bremen_primary, bremen_secondary |
| Mittagessen | 3 | bremen, bremen_primary, bremen_secondary |
| Name1 | 3 | bremen, bremen_primary, bremen_secondary |
| Name2 | 3 | bremen, bremen_primary, bremen_secondary |
| PaedMittagessen | 3 | bremen, bremen_primary, bremen_secondary |
| Planbezirk | 3 | bremen, bremen_primary, bremen_secondary |
| Region | 3 | bremen, bremen_primary, bremen_secondary |
| Schulart1 | 3 | bremen, bremen_primary, bremen_secondary |
| Schulart10 | 3 | bremen, bremen_primary, bremen_secondary |
| Schulart11 | 3 | bremen, bremen_primary, bremen_secondary |
| Schulart2 | 3 | bremen, bremen_primary, bremen_secondary |

## All-NULL Berlin Columns

Berlin columns that exist but are entirely NULL in other cities:

| Column | Cities where all-NULL |
|--------|----------------------|
| transit_rail_03_lines | bremen, bremen_primary, bremen_secondary |
| transit_tram_03_lines | bremen, bremen_primary, bremen_secondary |
| transit_tram_01_lines | bremen, bremen_primary, bremen_secondary |
| transit_rail_01_lines | bremen, bremen_primary, bremen_secondary |
| transit_rail_02_lines | bremen, bremen_primary, bremen_secondary |
| transit_tram_02_lines | bremen_secondary |

## Recommendations

1. **Bremen**: Run description pipeline for primary schools (113 Grundschulen) to fill description_de/description_en columns
2. **Bremen**: Consider adding demographics enrichment using Kleinraumig Infosystem data for Ortsteil-level social indicators
3. **Schema transformers**: Ensure all cities produce the berlin-schema parquet with exact column match (265 columns)
4. **Naming conventions**: Fix any uppercase column names inherited from raw data sources before the combiner stage
5. **Shared extras**: Evaluate whether columns appearing in 3+ cities should be promoted to the Berlin reference schema
