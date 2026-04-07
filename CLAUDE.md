# SchoolNossa Project

## Pipeline Skills Workflow

When working on SchoolNossa city data pipelines, always use the `/schoolnossa-workflow` skill as the entry point. It coordinates the 5-phase process:

1. `/data-source-research` — Research available open data
2. `/new-city-pipeline` — Scaffold directories and scripts
3. `/enrich-phase-builder` — Implement each enrichment (repeat)
4. `/pipeline-qa` — Validate final output
5. `/schema-drift-check` — Cross-city schema alignment

**After completing any phase or enrichment script**, re-check the pipeline state and tell the user what the next step is. Do not wait for the user to ask "what's next" — proactively show the updated status and suggest the next action.

**When writing enrichment scripts**, always read the reference implementations from all three existing cities (Berlin, Hamburg, NRW) before generating code. The reference tables are in the `/enrich-phase-builder` and `/new-city-pipeline` skills.

## Project Structure

- `scripts_{city}/` — Pipeline scripts per city (scrapers, enrichment, processing)
- `data_{city}/` — Data per city (raw, intermediate, final, cache)
- `scripts_shared/` — Shared enrichment/generation scripts
- Berlin schema is the reference — all cities must align to it
- Each city has an orchestrator: `{City}_school_data_asset_builder_orchestrator.py`

## Conventions

- File naming: `{city}_{school_type}_schools_with_{enrichment}.csv`
- Column naming: Berlin-compatible prefixes (`traffic_`, `transit_`, `crime_`, `poi_`)
- All paths relative via `Path(__file__)` — never hardcode absolute paths
- Cache API responses to `data_{city}/cache/` to avoid repeated calls
- Fallback input chain in every enrichment script (most-enriched first, raw last)

## Git & Journaling

Follow the global git discipline from `~/.claude/CLAUDE.md`. Additionally for SchoolNossa:
- Use the `/branch-workflow` skill when starting implementation work
- Update `docs/DEVJOURNAL.md` after completing each pipeline phase or enrichment
- Commit after each enrichment script is implemented — don't batch multiple enrichments into one commit
- Feature branch naming: `feature/{city}-{phase}` (e.g., `feature/munich-traffic-enrichment`)
