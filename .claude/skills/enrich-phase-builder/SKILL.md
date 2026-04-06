---
name: enrich-phase-builder
description: "SchoolNossa project only. Generate a production-ready enrichment script for a specific city and enrichment type (traffic, transit, crime, POI, demographics, website metadata). Use when the user asks to 'build the traffic enrichment for Munich', 'add crime data to the Frankfurt pipeline', 'implement transit phase', 'write the POI enrichment script', or any request to create or implement a specific enrichment phase for any city pipeline. Also use when adapting an enrichment from one city to another. Follows established pipeline patterns with fallback chain, Berlin-compatible column naming, caching, and rate limiting. Only applies when working in the schoolnossa repository."
allowed-tools: Bash, Read, Edit, Write, Glob, Grep, Agent
---

# Enrichment Phase Builder

Generate a complete, production-ready enrichment script for a city pipeline phase. This skill produces a working Python script that follows the exact patterns established across Berlin, Hamburg, and NRW.

## Required Input

Ask the user for:
1. **City** (e.g. `munich`, `nrw`, `hamburg`)
2. **Enrichment type** — one of: `traffic`, `transit`, `crime`, `poi`, `demographics`, `website_metadata`, `academic`, `anmeldezahlen`
3. **Data source details** — URL, format, API, or approach (from the data-source-research output)
4. **School type** — `primary`, `secondary`, or `both`

## Why These Patterns Matter

The SchoolNossa frontend expects all cities to produce identically-structured data. When an enrichment script deviates from the established patterns — different column names, missing fallback logic, no caching — it creates downstream problems: the schema transformer can't map columns correctly, re-running a partial pipeline overwrites good data, and API costs balloon without caching. These patterns evolved from real problems encountered during Berlin, Hamburg, and NRW development.

## Script Structure

Every enrichment script follows this structure. Read existing scripts from the closest city for reference before generating — adapting proven code is faster and more reliable than writing from scratch.

### Pattern 1: File Header and Imports

```python
"""
Phase N: {City} {Enrichment} Enrichment
Source: {data_source_url_or_description}
Input:  data_{city}/intermediate/{city}_{type}_schools_with_{previous}.csv
Output: data_{city}/intermediate/{city}_{type}_schools_with_{enrichment}.csv
"""
import pandas as pd
import numpy as np
import logging
import time
import json
import requests
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data_{city}"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"
```

### Pattern 2: Fallback Input Chain

The fallback chain is what makes the pipeline resilient to partial runs. If someone runs only phases 1, 3, and 5, phase 5 needs to find the output of phase 3 (not phase 4, which didn't run). Without this, re-running individual phases would fail or silently lose enrichment data from earlier phases:

```python
def find_input_file(school_type: str) -> Path:
    """Find the most-enriched input file, falling back through the chain."""
    # Order: most recent enrichment first, raw last
    fallback_chain = [
        # Add all enrichments that come BEFORE this one in pipeline order
        ("with_{previous_enrichment}", INTERMEDIATE_DIR),
        ("with_{earlier_enrichment}", INTERMEDIATE_DIR),
        ("schools", RAW_DIR),  # raw data as last resort
    ]
    for suffix, directory in fallback_chain:
        path = directory / f"{city}_{school_type}_{suffix}.csv"
        if path.exists():
            logger.info(f"Using input: {path.name}")
            return path
    raise FileNotFoundError(f"No input file found for {school_type}")
```

### Pattern 3: Caching for API Calls

For any enrichment that calls external APIs (Google Places, Overpass, traffic sensors):

```python
CACHE_FILE = CACHE_DIR / f"{city}_{enrichment}_cache.json"

def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        logger.info(f"Loaded cache with {len(cache)} entries")
        return cache
    return {}

def save_cache(cache: dict):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved cache with {len(cache)} entries")
```

### Pattern 4: Rate Limiting for APIs

```python
def rate_limited_request(url: str, params: dict = None, delay: float = 0.5) -> requests.Response:
    """Make a rate-limited HTTP request with retry logic."""
    for attempt in range(3):
        try:
            time.sleep(delay)
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                raise
            logger.warning(f"Request failed (attempt {attempt+1}): {e}")
            time.sleep(delay * (attempt + 1))
```

### Pattern 5: Berlin-Compatible Column Naming

ALL enrichment columns MUST use Berlin-compatible names. Reference the naming patterns:

**Traffic columns:**
- `traffic_volume_total`, `traffic_volume_cars`, `traffic_volume_trucks`
- `traffic_accidents_total`, `traffic_accidents_fatal`, `traffic_accidents_serious`
- `traffic_score`, `traffic_data_source`

**Transit columns:**
- `transit_{mode}_{rank}_name` — e.g. `transit_bus_1_name`
- `transit_{mode}_{rank}_distance_m`
- `transit_{mode}_{rank}_lat`, `transit_{mode}_{rank}_lon`
- `transit_{mode}_{rank}_lines`
- Modes: `bus`, `tram`, `ubahn`, `sbahn`, `bahn`
- Ranks: `1`, `2`, `3` (top 3 nearest)
- `transit_score`

**Crime columns:**
- `crime_total`, `crime_rate_per_1000`
- `crime_{category}` — e.g. `crime_theft`, `crime_robbery`, `crime_assault`
- `crime_data_source`, `crime_year`, `crime_area_name`

**POI columns:**
- `poi_{category}_count` — e.g. `poi_supermarket_count`
- `poi_{category}_{rank}_name`, `poi_{category}_{rank}_distance_m`
- `poi_{category}_{rank}_rating`
- Categories: `supermarket`, `restaurant`, `bakery`, `kita`, `primary_school`, `secondary_school`, `park`

**Demographics columns:**
- `sozialindex`, `belastungsstufe`
- `migration_percentage`, `unemployment_rate`
- `median_income`, `welfare_rate`

### Pattern 6: Main Function Structure

```python
def enrich_schools(df: pd.DataFrame, school_type: str) -> pd.DataFrame:
    """Core enrichment logic. Takes a DataFrame, returns enriched DataFrame."""
    cache = load_cache()
    total = len(df)
    enriched_count = 0

    for idx, row in df.iterrows():
        school_id = str(row.get('schulnummer', row.get('bsn', idx)))
        lat = row.get('latitude', row.get('lat'))
        lon = row.get('longitude', row.get('lon'))

        if pd.isna(lat) or pd.isna(lon):
            logger.warning(f"No coordinates for {row.get('schulname', school_id)}")
            continue

        # Check cache first
        cache_key = f"{school_id}_{lat:.4f}_{lon:.4f}"
        if cache_key in cache:
            for col, val in cache[cache_key].items():
                df.at[idx, col] = val
            enriched_count += 1
            continue

        # TODO: Implement enrichment API call here
        enrichment_data = {}

        # Store results
        for col, val in enrichment_data.items():
            df.at[idx, col] = val

        cache[cache_key] = enrichment_data
        enriched_count += 1

        if enriched_count % 50 == 0:
            save_cache(cache)
            logger.info(f"Progress: {enriched_count}/{total} schools enriched")

    save_cache(cache)
    logger.info(f"Enriched {enriched_count}/{total} schools ({enriched_count/total*100:.1f}%)")
    return df


def main(school_type: str = "secondary") -> str:
    """Main entry point called by orchestrator."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    input_file = find_input_file(school_type)
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} {school_type} schools from {input_file.name}")

    df = enrich_schools(df, school_type)

    output_path = INTERMEDIATE_DIR / f"{city}_{school_type}_schools_with_{enrichment}.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} schools to {output_path.name}")
    return str(output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-type", default="secondary", choices=["primary", "secondary"])
    args = parser.parse_args()
    main(args.school_type)
```

### Pattern 7: Enrichment-Specific Logic

Before generating, read the scripts from all three cities for the chosen enrichment type. Each city solved the same problem differently — understanding the variations helps you pick the right approach for the new city.

### Traffic Enrichment References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_traffic_enrichment.py` | Sensor API (PLZ-level volume counts) |
| Hamburg | `scripts_hamburg/enrichment/hamburg_traffic_enrichment.py` | OGC SensorThings API (Kfz + Rad counting stations, concurrent requests) |
| NRW | `scripts_nrw/enrichment/nrw_traffic_enrichment.py` | Unfallatlas accident data (national dataset, works for any German city) |

### Transit Enrichment References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_transit_enrichment.py` | BVG + Overpass API |
| Hamburg | `scripts_hamburg/enrichment/hamburg_hvv_transit_enrichment.py` | Local HVV GeoJSON files with line/route extraction |
| NRW | `scripts_nrw/enrichment/nrw_transit_enrichment.py` | Overpass API only (free, universal fallback, no API key) |

### Crime Enrichment References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_crime_enrichment.py` | Bezirk-level CSV from police statistics (12 districts) |
| Hamburg | `scripts_hamburg/enrichment/hamburg_crime_enrichment.py` | PKS PDF parsing, 16 crime categories, Stadtteil-level (107 districts) |
| NRW | `scripts_nrw/enrichment/nrw_crime_enrichment.py` | Hardcoded city-wide PKS with population-weighted Bezirk estimation |

### POI Enrichment References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_poi_enrichment.py` | Google Places API |
| Hamburg | `scripts_hamburg/enrichment/hamburg_poi_enrichment.py` | Google Places API with threading + rate limiting (most mature) |
| NRW | `scripts_nrw/enrichment/nrw_poi_enrichment.py` | Google Places API (near-identical to Hamburg) |

### Website Metadata & Descriptions References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_shared/enrichment/enrich_berlin_schools_with_website_metadata.py` | Separate metadata extraction script |
| Berlin | `scripts_shared/generation/generate_school_descriptions_v4.py` | Multi-model LLM description generation (~3 hours) |
| Hamburg | `scripts_hamburg/enrichment/hamburg_tuition_enrichment.py` | Gemini REST API for tuition flags |
| NRW | `scripts_nrw/enrichment/nrw_website_metadata_enrichment.py` | Gemini + Google Search grounding (metadata + bilingual descriptions in one pass) |
| **All cities** | `scripts_shared/generation/school_description_pipeline.py` | **Preferred for new cities.** 3-pass pipeline: Pass 0 = Perplexity web research, Pass 1 = GPT-4o rich descriptions (DE+EN), Pass 2 = structured JSON extraction to fill empty columns (lehrer, website, schueler by year, sprachen, besonderheiten, nachfrage, migration). Resumable via JSON cache. Run via orchestrator `--with-descriptions` flag or directly: `python school_description_pipeline.py --city {city} --school-type {type}`. Prompts stored in `prompts/pass0_web_research.md`, `prompts/pass1_description_generation.md`, `prompts/pass2_structured_extraction.md`. |

### Tuition Fee References
| City | Script | Approach |
|------|--------|----------|
| **All cities** | `scripts_shared/generation/tuition_pipeline.py` | **3-pass tuition pipeline for private schools only** (traegerschaft contains 'privat'/'frei'). Pass 1 (Phase 10): Gemini `gemini-3-pro-preview` + Google Search → `tuition_tier` (low/medium/high/premium/ultra) + `tuition_monthly_eur`. Pass 2 (Phase 11): Gemini + Google Search → `tuition_income_matrix` (12 income brackets €<20k–€>250k as JSON) + `tuition_sibling_discounts`. Pass 3 (Phase 12): GPT-5.2 via OpenAI Responses API (`/v1/responses`) + `web_search` + `report_tuition_fees` function calling → verifies flat matrices, sets `income_based_tuition` bool. **Pass 3 is skipped when Pass 2 already returns a non-flat matrix — in that case `income_based_tuition=True` is set automatically.** Idempotent: each pass skips already-processed schools. Resumable via JSON cache in `data_{city}/cache/tuition_pipeline_{type}.json`. Run via orchestrator `--with-tuition` flag or: `python tuition_pipeline.py --city {city} --school-type {type} --passes 1,2,3`. New columns: `tuition_tier`, `tuition_tier_reasoning`, `tuition_income_matrix`, `tuition_sibling_discounts`, `tuition_granular_reasoning`, `tuition_granular_generated_at`, `income_based_tuition`. Each school takes ~5 min total across all 3 passes. |

### Demographics References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_demographics_enrichment.py` | School-level migration % from bildung.berlin + Belastungsstufe |
| Hamburg | `scripts_hamburg/enrichment/hamburg_demographics_enrichment.py` | Stadtteil-Profile XLSX (26 indicators from statistik-nord.de) |
| NRW | (integrated in crime script) | Schulsozialindex from ministry CSV (per-school, joined by schulnummer) |
| Shared | `scripts_shared/enrichment/download_zensus_grid.py` | Federal Zensus 2022 grid data (works for any German city) |

### Academic / Anmeldezahlen References
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_academic_enrichment.py` | MSA district stats + vision scraping (GPT for screenshots) |
| Hamburg | `scripts_hamburg/scrapers/hamburg_abitur_scraper.py` | Abitur averages scraped from gymnasium-hamburg.net |
| NRW | `scripts_nrw/enrichment/nrw_anmeldezahlen_enrichment.py` | Düsseldorf Open Data + PDF extraction, LLM name matching |

Read all available references for the chosen enrichment type, then adapt the approach that best fits the new city's data sources. Don't reinvent patterns — adapt proven code.

## Workflow

1. Read the reference script for the chosen enrichment type
2. Read the city's data-source-research document if it exists
3. Generate the enrichment script with all 7 patterns applied
4. Wire it into the orchestrator's `run_phase_N()` function
5. Run syntax check
6. Run the evaluations

---

## Evaluations

After script generation, run ALL of these. Every check must pass.

### EVAL-1: Syntax Valid
```bash
python3 -c "import py_compile; py_compile.compile('scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py', doraise=True)"
```
**Pass criteria:** No syntax errors.

### EVAL-2: Has Fallback Input Chain
```bash
grep -c "find_input_file\|fallback" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py"
```
**Pass criteria:** Count >= 1. The script must have a `find_input_file` function or equivalent fallback logic.

### EVAL-3: Has Cache Logic (for API-based enrichments)
```bash
# Only required for: traffic, transit, poi, website_metadata, academic
grep -c "load_cache\|save_cache\|cache" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py"
```
**Pass criteria:** Count >= 3 for API-based enrichments. At least load_cache, save_cache, and one cache usage.

### EVAL-4: Has Rate Limiting (for API-based enrichments)
```bash
grep -c "time.sleep\|rate_limit\|delay" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py"
```
**Pass criteria:** Count >= 1 for any script that makes HTTP requests.

### EVAL-5: Berlin-Compatible Column Names
```bash
python3 -c "
import ast
with open('scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py') as f:
    content = f.read()

# Check for the enrichment-specific column prefix
enrichment_type = '{enrichment}'
prefix_map = {
    'traffic': 'traffic_',
    'transit': 'transit_',
    'crime': 'crime_',
    'poi': 'poi_',
    'demographics': ['sozialindex', 'migration', 'belastung'],
}
expected = prefix_map.get(enrichment_type, [enrichment_type + '_'])
if isinstance(expected, str):
    expected = [expected]

found = any(e in content for e in expected)
assert found, f'FAIL: No Berlin-compatible column names with prefix {expected}'
print('PASS: Berlin-compatible column naming found')
"
```
**Pass criteria:** Script uses the correct column name prefix for the enrichment type.

### EVAL-6: Has main() Function with Correct Signature
```bash
python3 -c "
import ast
with open('scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py') as f:
    tree = ast.parse(f.read())
funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
assert 'main' in funcs, 'FAIL: No main() function'
print('PASS: main() function exists')
"
```
**Pass criteria:** `main()` function exists and is importable by the orchestrator.

### EVAL-7: Output File Follows Naming Convention
```bash
grep -E "{city}.*schools_with_{enrichment}" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py" || echo "FAIL: Output file does not follow naming convention"
```
**Pass criteria:** Output filename matches `{city}_{type}_schools_with_{enrichment}.csv`.

### EVAL-8: No Hardcoded API Keys
```bash
grep -iE "(api_key|secret|token)\s*=\s*['\"]" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py" && echo "FAIL: Hardcoded API key found" || echo "PASS: No hardcoded keys"
```
**Pass criteria:** Zero hardcoded API keys. All keys must come from environment variables.

### EVAL-9: Progress Logging Included
```bash
grep -c "logger\.\(info\|warning\|error\)" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py"
```
**Pass criteria:** Count >= 5. Script must have meaningful progress logging.

### EVAL-10: Orchestrator Integration
```bash
# Verify the orchestrator references this script
ORCH=$(find scripts_{city}/ -name "*orchestrator*.py" | head -1)
grep -q "{enrichment}" "$ORCH" && echo "PASS: Orchestrator references enrichment" || echo "FAIL: Orchestrator not wired up"
```
**Pass criteria:** The orchestrator has a phase function that imports this enrichment script.

### EVAL-11: Handles Missing Coordinates Gracefully
```bash
grep -c "pd.isna\|pd.isnull\|np.isnan\|is None\|missing\|skip" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py"
```
**Pass criteria:** Count >= 1. Script must handle schools with missing lat/lon.

### EVAL-12: No Absolute Paths
```bash
grep -n "/Users/" "scripts_{city}/enrichment/{city}_{enrichment}_enrichment.py" && echo "FAIL" || echo "PASS: No absolute paths"
```
**Pass criteria:** Zero absolute paths. All paths use `Path(__file__)` relative resolution.
