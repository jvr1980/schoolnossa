# SchoolNossa Project

## MANDATORY: Commit, Journal, and Push After Every Unit of Work

These are non-negotiable behaviors. Do them automatically — never wait for the user to ask.

**After EVERY completed unit of work** (a script, a fix, a config change, an enrichment phase):
1. `git add` the changed files and `git commit` with a concise message ending with `Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>`
2. Update `docs/DEVJOURNAL.md` — add entry ABOVE the `<!-- NEW ENTRIES GO ABOVE THIS LINE -->` marker. Format: `## YYYY-MM-DD — Short Title` with What/Why/Results.
3. `git push -u origin <branch>` after every 2-3 commits for backup.

**Before starting any code change**: check `git branch --show-current`. If on `main`, create a feature branch first: `git checkout -b feature/<short-description>`. Branch naming: `feature/{city}-{phase}` (e.g., `feature/munich-traffic-enrichment`).

**When work on a branch is complete**: tell the user the branch is ready for testing. Only merge to main after user confirms. Use `--no-ff` merges.

**Self-check cadence**: after 3+ file edits without a commit, stop and commit before continuing.

## Pipeline Skills Workflow

When working on city data pipelines, use `/schoolnossa-workflow` as the entry point. It coordinates:

1. `/data-source-research` — Research available open data
2. `/new-city-pipeline` — Scaffold directories and scripts
3. `/enrich-phase-builder` — Implement each enrichment (repeat)
4. `/pipeline-qa` — Validate final output
5. `/schema-drift-check` — Cross-city schema alignment

After completing any phase, proactively show the updated status and suggest the next action.

When writing enrichment scripts, read the reference implementations from Berlin, Hamburg, and NRW first.

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
