---
name: new-city-pipeline
description: "SchoolNossa project only. Scaffold a complete data pipeline for a new German city — directories, orchestrator, stub scripts, and schema transformer. Use whenever the user mentions adding a new city to SchoolNossa, expanding to another German city, or says things like 'let's do Munich next', 'set up Frankfurt', 'scaffold a pipeline for Stuttgart', 'add a new city', or 'create the pipeline structure for [city]'. Also use when the user wants to understand or replicate the pipeline structure pattern. Only applies when working in the schoolnossa repository."
allowed-tools: Bash, Read, Edit, Write, Glob, Grep, Agent
---

# New City Pipeline Scaffolding

When the user asks to add a new city (e.g. Munich, Frankfurt, Stuttgart), follow this skill to scaffold the full pipeline.

## Required Input

Ask the user for:
1. **City name** (e.g. `munich`) — used as the directory/file prefix
2. **German name** (e.g. `München`) — used in display strings and comments
3. **State** (e.g. `Bayern`) — affects data source selection
4. **School types** — `combined` (one orchestrator for both, like Hamburg) or `split` (separate primary/secondary, like Berlin/NRW)

## Phase 1: Create Directory Structure

```
data_{city}/
  raw/
  intermediate/
  final/
  cache/
  descriptions/
scripts_{city}/
  scrapers/
  enrichment/
  processing/
```

Create all directories using `mkdir -p`.

## Workflow Context — Where This Skill Fits

This skill is step 2 in the new-city workflow. The full sequence is:
1. `/data-source-research` — discover what open data exists for the city
2. **`/new-city-pipeline`** (this skill) — scaffold the directories and code structure
3. `/enrich-phase-builder` (repeat) — implement each enrichment phase
4. `/pipeline-qa` — validate output quality after running the pipeline
5. `/schema-drift-check` — verify cross-city compatibility

If the data-source-research hasn't been done yet, suggest it first — knowing what data sources exist determines which existing city to use as the template.

## Phase 2: Generate the Orchestrator

Create `scripts_{city}/{City}_school_data_asset_builder_orchestrator.py` following the **NRW/Hamburg pattern** (simple dict-based). The Berlin orchestrator uses a more complex dataclass-based approach that added engineering overhead without proportional benefit for the Hamburg and NRW pipelines — the simpler dict pattern has proven easier to extend and debug.

**Reference file — read this before generating:**
→ `scripts_hamburg/Hamburg_school_data_asset_builder_orchestrator.py` (cleanest example, 281 lines)

Read the Hamburg orchestrator and adapt it for the new city. Key elements to preserve:
- `available_phases` dict mapping integer keys to `(name, callable)` tuples
- `run_phase_N()` functions that lazy-import from script modules
- `run_full_pipeline()` with per-phase try/except, Phase 1 critical stop, and summary report
- `argparse` with `--phases` (comma-separated) and `--skip-embeddings` flags
- Logging to both file (`orchestrator.log`) and console

Standard phases for every city (use integer keys, insert city-specific phases at intermediate keys like `15`, `55`, `56`):

| Key | Phase | Notes |
|-----|-------|-------|
| 1 | School Master Data | Critical — pipeline stops if this fails |
| 2 | Traffic Enrichment | |
| 3 | Transit Enrichment | |
| 4 | Crime Enrichment | |
| 5 | POI Enrichment | Google Places API (shared pattern) |
| 6 | Website Metadata & Descriptions | |
| 7 | Data Combination | |
| 8 | Embeddings & Final Output | Supports `--skip-embeddings` |
| 9 | Berlin Schema Enforcement | |

For `split` mode (separate primary/secondary), create TWO orchestrators with a `SCHOOL_TYPE` variable at the top. See NRW for this pattern:
→ `scripts_nrw/NRW_primary_school_data_asset_builder_orchestrator.py`
→ `scripts_nrw/NRW_secondary_school_data_asset_builder_orchestrator.py`

## Phase 3: Generate Stub Scripts

Create empty stub scripts for every phase with the standard signature:

For each stub, read the reference files from all three cities to understand the different approaches, then adapt the best fit for the new city. Replace city-specific logic, data source URLs, and column names, but keep the structural patterns (fallback chain, caching, logging, main() signature).

### Reference Scripts by Phase

**Scraper** — `scrapers/{city}_school_master_scraper.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/scrapers/ISS_data_scraper.py` | Web scraping (requests + BeautifulSoup) |
| Berlin | `scripts_berlin/scrapers/bildung_berlin_gymnasien_scraper.py` | Web scraping with vision/GPT for PDFs |
| Hamburg | `scripts_hamburg/scrapers/hamburg_school_master_scraper.py` | WFS GeoJSON from geodienste API |
| NRW | `scripts_nrw/scrapers/nrw_school_master_scraper.py` | Open Data CSV download + UTM coord conversion |

**Traffic** — `enrichment/{city}_traffic_enrichment.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_traffic_enrichment.py` | Sensor API (PLZ-level volume) |
| Hamburg | `scripts_hamburg/enrichment/hamburg_traffic_enrichment.py` | OGC SensorThings API (Kfz + Rad counting stations) |
| NRW | `scripts_nrw/enrichment/nrw_traffic_enrichment.py` | Unfallatlas accident data (national, works for any German city) |

**Transit** — `enrichment/{city}_transit_enrichment.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_transit_enrichment.py` | BVG + Overpass API |
| Hamburg | `scripts_hamburg/enrichment/hamburg_hvv_transit_enrichment.py` | Local HVV GeoJSON files + line extraction |
| NRW | `scripts_nrw/enrichment/nrw_transit_enrichment.py` | Overpass API only (free, no API key, universal fallback) |

**Crime** — `enrichment/{city}_crime_enrichment.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_crime_enrichment.py` | Bezirk-level CSV from police statistics |
| Hamburg | `scripts_hamburg/enrichment/hamburg_crime_enrichment.py` | PKS PDF parsing, 16 categories, Stadtteil-level |
| NRW | `scripts_nrw/enrichment/nrw_crime_enrichment.py` | Hardcoded city-wide PKS with population-weighted Bezirk estimation |

**POI** — `enrichment/{city}_poi_enrichment.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/enrichment/berlin_poi_enrichment.py` | Google Places API |
| Hamburg | `scripts_hamburg/enrichment/hamburg_poi_enrichment.py` | Google Places API with threading + rate limiting |
| NRW | `scripts_nrw/enrichment/nrw_poi_enrichment.py` | Google Places API (near-identical to Hamburg) |

**Website Metadata & Descriptions** — `enrichment/{city}_website_metadata_enrichment.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_shared/enrichment/enrich_berlin_schools_with_website_metadata.py` | Separate metadata + description scripts |
| Berlin | `scripts_shared/generation/generate_school_descriptions_v4.py` | Multi-model LLM description generation |
| Hamburg | `scripts_hamburg/enrichment/hamburg_tuition_enrichment.py` | Gemini REST API for tuition extraction |
| NRW | `scripts_nrw/enrichment/nrw_website_metadata_enrichment.py` | Gemini + Google Search grounding (metadata + bilingual descriptions in one pass) |

**Data Combiner** — `processing/{city}_data_combiner.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/processing/berlin_data_combiner.py` | Sequential file chain |
| Hamburg | `scripts_hamburg/processing/hamburg_data_combiner.py` | Sequential merge by schulnummer (cleanest) |
| NRW | `scripts_nrw/processing/nrw_data_combiner.py` | Auto-detect most-enriched intermediate file |

**Embeddings** — `processing/{city}_embeddings_generator.py`
| City | Script | Approach |
|------|--------|----------|
| Berlin | `scripts_berlin/processing/berlin_embeddings_generator.py` | OpenAI text-embedding-3-large (3072-dim) |
| Hamburg | `scripts_hamburg/processing/hamburg_embeddings_generator.py` | Feature-based cosine similarity (no API needed) |
| NRW | `scripts_nrw/processing/nrw_embeddings_generator.py` | OpenAI with Gemini fallback (768-dim) |

**Schema Transformer** — `{city}_to_berlin_schema.py`
| City | Script | Approach |
|------|--------|----------|
| Hamburg | `scripts_hamburg/hamburg_to_berlin_schema.py` | Column rename dict + reorder to Berlin column list |
| NRW | `scripts_nrw/nrw_to_berlin_schema.py` | Same pattern + per-city splits (Cologne/Düsseldorf) |

Each stub should be a minimal working skeleton with:
- Correct docstring (phase number, source TODO, input/output file paths)
- Correct directory constants (`BASE_DIR`, `DATA_DIR`, `RAW_DIR`, `INTERMEDIATE_DIR`, `CACHE_DIR`)
- A `main()` function that raises `NotImplementedError` with a clear message
- The fallback input chain pattern (for enrichment scripts)
- A `if __name__ == "__main__"` block with logging setup

## Phase 4: Generate a City-Specific TODO Checklist

Write `scripts_{city}/PIPELINE_STATUS.md` with:

```markdown
# {City} Pipeline Status

## Data Sources (research needed)
- [ ] School master data: source URL, format, encoding, coordinate system
- [ ] Traffic data: sensor API or accident atlas?
- [ ] Transit data: local GTFS, GeoJSON, or fallback to Overpass API?
- [ ] Crime data: PKS granularity (city-level, Bezirk, Stadtteil?)
- [ ] POI data: Google Places API (shared across all cities)
- [ ] Demographics: Sozialindex equivalent? Stadtteil-Profile?
- [ ] Academic performance: Abitur data? Demand/enrollment numbers?
- [ ] Website metadata: school websites accessible?

## Phase Implementation Status
- [ ] Phase 1: School Master Data — STUB
- [ ] Phase 2: Traffic Enrichment — STUB
- [ ] Phase 3: Transit Enrichment — STUB
- [ ] Phase 4: Crime Enrichment — STUB
- [ ] Phase 5: POI Enrichment — STUB
- [ ] Phase 6: Website Metadata & Descriptions — STUB
- [ ] Phase 7: Data Combination — STUB
- [ ] Phase 8: Embeddings — STUB
- [ ] Phase 9: Schema Transformer — STUB

## Expected School Counts
- Primary: ???
- Secondary: ???
- Total: ???
```

## Phase 5: Wire Up Imports

Ensure the orchestrator's `run_phase_N()` functions have correct import paths pointing to the stub scripts.

---

## Evaluations

After scaffolding is complete, run these checks. ALL must pass.

### EVAL-1: Directory Structure Exists
```bash
# Verify all required directories exist
for dir in raw intermediate final cache descriptions; do
  test -d "data_{city}/$dir" || echo "FAIL: data_{city}/$dir missing"
done
for dir in scrapers enrichment processing; do
  test -d "scripts_{city}/$dir" || echo "FAIL: scripts_{city}/$dir missing"
done
```
**Pass criteria:** All 8 directories exist.

### EVAL-2: Orchestrator Syntax Valid
```bash
python3 -c "import py_compile; py_compile.compile('scripts_{city}/{City}_school_data_asset_builder_orchestrator.py', doraise=True)"
```
**Pass criteria:** No syntax errors.

### EVAL-3: Orchestrator Has All Required Phases
```bash
python3 -c "
import ast, sys
with open('scripts_{city}/{City}_school_data_asset_builder_orchestrator.py') as f:
    tree = ast.parse(f.read())
funcs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
required = ['run_phase_1', 'run_full_pipeline']
for r in required:
    assert r in funcs, f'Missing function: {r}'
print('PASS: All required functions present')
"
```
**Pass criteria:** `run_phase_1` and `run_full_pipeline` exist.

### EVAL-4: All Stub Scripts Are Syntactically Valid Python
```bash
find scripts_{city}/ -name "*.py" -exec python3 -c "import py_compile; py_compile.compile('{}', doraise=True)" \;
```
**Pass criteria:** Zero syntax errors across all generated .py files.

### EVAL-5: File Naming Convention Consistent
```bash
# All enrichment stubs must follow {city}_{type}_enrichment.py pattern
ls scripts_{city}/enrichment/*.py | grep -v "__" | while read f; do
  basename "$f" | grep -qE "^{city}_.*_enrichment\.py$" || echo "FAIL: $f does not follow naming convention"
done
```
**Pass criteria:** All enrichment scripts match `{city}_*_enrichment.py`.

### EVAL-6: Orchestrator --help Works
```bash
python3 scripts_{city}/{City}_school_data_asset_builder_orchestrator.py --help
```
**Pass criteria:** Shows usage with `--phases` and `--skip-embeddings` flags.

### EVAL-7: PIPELINE_STATUS.md Exists and Has All Phases
```bash
test -f scripts_{city}/PIPELINE_STATUS.md && grep -c "\- \[ \]" scripts_{city}/PIPELINE_STATUS.md
```
**Pass criteria:** File exists with at least 15 checklist items (9 phases + 8 data sources).

### EVAL-8: Schema Transformer References Berlin Parquet
```bash
grep -l "data_berlin" scripts_{city}/{city}_to_berlin_schema.py
```
**Pass criteria:** The schema transformer file contains a reference to `data_berlin/` as the schema source.

### EVAL-9: No Hardcoded Absolute Paths
```bash
grep -rn "/Users/" scripts_{city}/ || echo "PASS: No absolute paths"
```
**Pass criteria:** Zero matches — all paths must be relative using `Path(__file__)`.

### EVAL-10: Orchestrator Import Paths Match Stub Locations
```bash
python3 -c "
import re
with open('scripts_{city}/{City}_school_data_asset_builder_orchestrator.py') as f:
    content = f.read()
imports = re.findall(r'from\s+([\w.]+)\s+import', content)
print(f'Found {len(imports)} imports')
for imp in imports:
    path = imp.replace('.', '/') + '.py'
    # Verify the referenced module exists (relative to scripts_{city}/)
    print(f'  {imp} -> {path}')
"
```
**Pass criteria:** All import paths resolve to existing stub files.
