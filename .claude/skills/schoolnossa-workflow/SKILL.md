---
name: schoolnossa-workflow
description: "SchoolNossa project only. Master workflow coordinator for expanding SchoolNossa to new cities. This is the FIRST skill to trigger for any SchoolNossa pipeline work — it determines where you are in the process and routes to the right sub-skill. Trigger on ANY of: 'expand to [city]', 'add [city]', 'let's do [city] next', 'new city', 'what's next for [city]', 'where are we with [city]', 'schoolnossa workflow', 'pipeline status', 'continue with [city]', or any mention of expanding SchoolNossa to a new German city. Also trigger when resuming work on an in-progress city pipeline. Only applies when working in the schoolnossa repository."
allowed-tools: Bash, Read, Edit, Write, Glob, Grep, Agent, Skill
---

# SchoolNossa Workflow Coordinator

This skill is the entry point for all SchoolNossa city expansion work. It determines where you are in the process, shows the user their options, and routes to the correct sub-skill.

## How This Works

When the user mentions anything about expanding SchoolNossa or working on a city pipeline, this skill activates first. It:
1. Figures out which city the user is talking about
2. Checks what already exists for that city (directories, scripts, data, reports)
3. Determines which workflow phase to enter
4. Invokes the appropriate sub-skill

## The 5-Phase Workflow

Every new city follows this sequence. Each phase has a dedicated skill:

```
Phase 1: RESEARCH          →  /data-source-research
          What data exists for this city?
          Output: docs/{city}_data_availability_research.md

Phase 2: SCAFFOLD           →  /new-city-pipeline
          Create directories, orchestrator, stub scripts
          Output: scripts_{city}/, data_{city}/

Phase 3: IMPLEMENT          →  /enrich-phase-builder (repeat per enrichment)
          Build each enrichment phase one at a time
          Output: working scripts in scripts_{city}/enrichment/

Phase 4: VALIDATE           →  /pipeline-qa
          Run QA checks on the final output
          Output: data_{city}/final/QA_REPORT_{type}.md

Phase 5: ALIGN              →  /schema-drift-check
          Verify cross-city schema compatibility
          Output: docs/SCHEMA_DRIFT_REPORT.md
```

## Step 1: Detect the City

If the user hasn't named a city, ask:
> "Which city do you want to work on? (e.g., Munich, Frankfurt, Stuttgart, Leipzig...)"

## Step 2: Assess Current State

Run these checks to determine where the city is in the workflow:

```bash
CITY="the_city_name_lowercase"

# Phase 1 done?
test -f "docs/${CITY}_data_availability_research.md" && echo "RESEARCH: DONE" || echo "RESEARCH: NOT STARTED"

# Phase 2 done?
test -d "scripts_${CITY}" && echo "SCAFFOLD: DONE" || echo "SCAFFOLD: NOT STARTED"

# Phase 3 progress?
if [ -d "scripts_${CITY}/enrichment" ]; then
  TOTAL=$(ls scripts_${CITY}/enrichment/*.py 2>/dev/null | grep -v __pycache__ | wc -l)
  IMPLEMENTED=$(grep -rL "NotImplementedError" scripts_${CITY}/enrichment/*.py 2>/dev/null | wc -l)
  echo "IMPLEMENT: ${IMPLEMENTED}/${TOTAL} enrichments implemented"
else
  echo "IMPLEMENT: NOT STARTED"
fi

# Phase 4 done?
ls data_${CITY}/final/QA_REPORT_*.md 2>/dev/null && echo "VALIDATE: DONE" || echo "VALIDATE: NOT STARTED"

# Phase 5 done?
if [ -f "docs/SCHEMA_DRIFT_REPORT.md" ]; then
  grep -q "${CITY}" docs/SCHEMA_DRIFT_REPORT.md && echo "ALIGN: DONE" || echo "ALIGN: NOT STARTED"
else
  echo "ALIGN: NOT STARTED"
fi

# Final output exists?
ls data_${CITY}/final/*master_table_final* 2>/dev/null && echo "FINAL OUTPUT: EXISTS" || echo "FINAL OUTPUT: NOT YET"
```

## Step 3: Present Status and Route

Show the user a clear status dashboard, then route to the next action:

```
=== SchoolNossa: {City} Pipeline Status ===

  [x] Phase 1: Research          docs/{city}_data_availability_research.md
  [x] Phase 2: Scaffold          scripts_{city}/ created
  [~] Phase 3: Implement         4/9 enrichments done
      [ ] traffic enrichment
      [ ] crime enrichment
      [x] transit enrichment
      [x] poi enrichment
      [x] website metadata
      [x] data combiner
      [ ] embeddings
      [ ] schema transformer
  [ ] Phase 4: Validate          QA report not generated
  [ ] Phase 5: Align             Not in schema drift report

  Next step: Implement traffic enrichment (Phase 3)
  → Invoking /enrich-phase-builder
```

### Routing Rules

| Current State | Action |
|---------------|--------|
| Nothing exists for this city | Invoke `/data-source-research` |
| Research doc exists, no scripts dir | Invoke `/new-city-pipeline` |
| Scripts dir exists with stubs | Invoke `/enrich-phase-builder` for the next unimplemented phase |
| All enrichments implemented, no final output | Guide user to run the orchestrator |
| Final output exists, description pipeline not yet run | Suggest running `--with-descriptions` (Phase 9) to enrich descriptions and fill empty columns |
| Final output exists, no QA report | Invoke `/pipeline-qa` |
| QA report exists, city not in drift report | Invoke `/schema-drift-check` |
| Everything done | Report completion, suggest running the pipeline end-to-end |

## Step 4: After Each Sub-Skill Completes

After any sub-skill finishes its work, return to this workflow coordinator to:
1. Re-assess the state (re-run the checks from Step 2)
2. Show the updated status dashboard
3. Ask the user: "Ready to continue to the next phase, or do you want to stop here?"
4. If continuing, invoke the next sub-skill

This keeps the workflow moving forward without the user needing to remember which skill to call next.

## Enrichment Phase Order

When in Phase 3 (IMPLEMENT), work through enrichments in this order because each phase's output feeds the next via the fallback chain:

1. **School Master Data** (scraper) — must be first, everything depends on it
2. **Traffic Enrichment** — independent of other enrichments
3. **Transit Enrichment** — independent of other enrichments
4. **Crime Enrichment** — independent of other enrichments
5. **POI Enrichment** — independent of other enrichments
6. **Demographics** — if applicable for the city
7. **Academic / Anmeldezahlen** — if data was found in research phase
8. **Data Combiner** — merges all enrichment outputs
9. **Embeddings Generator** — initial pass (uses rule-based descriptions)
10. **Schema Transformer** — maps to Berlin schema
11. **Description Pipeline** — web research + LLM descriptions (DE+EN) + structured extraction to fill empty columns (lehrer, website, schueler by year, sprachen, besonderheiten, nachfrage, migration). Script: `scripts_shared/generation/school_description_pipeline.py`. Orchestrator flag: `--with-descriptions`. Requires Perplexity + OpenAI API keys. After this, re-run steps 9+10 so embeddings reflect the richer descriptions. **Frankfurt secondary reference coverage: description+description_en 100%, besonderheiten 89%, schueler_gesamt_web 100%, sprachen 60%, gruendungsjahr 58%, website 53%, lehrer_2024_25 12% (teacher counts are rarely published online).**
12. **Tuition Pass 1** — Gemini + Google Search classifies each **private school** (traegerschaft contains 'privat'/'frei') into a tuition tier (low/medium/high/premium/ultra) and estimates a single monthly fee. Script: `scripts_shared/generation/tuition_pipeline.py --passes 1`. Orchestrator: Phase 10 / `--with-tuition`.
13. **Tuition Pass 2** — Gemini + Google Search generates a 12-bracket income matrix (€<20k → €>250k) and sibling discount percentages. If the resulting matrix is non-flat (income-based variation found), `income_based_tuition=True` is set immediately and Pass 3 is skipped for that school. Script: `--passes 2`. Orchestrator: Phase 11.
14. **Tuition Pass 3** — GPT-5.2 via OpenAI Responses API verifies schools whose Pass 2 matrix is still flat (all brackets identical). Sets `income_based_tuition=False` for confirmed flat-fee schools to exclude from future runs. Script: `--passes 3`. Orchestrator: Phase 12 / `--phases 12`. Each school takes ~5 min. **Frankfurt reference: 12 private secondary schools; ~3 had income-based fees (Phorms, Waldorf, Karl Popper), ~9 confirmed flat-fee.**

## Quick Commands

The user can also jump to specific phases:
- "research {city}" → Phase 1
- "scaffold {city}" → Phase 2
- "build traffic for {city}" → Phase 3 (specific enrichment)
- "validate {city}" → Phase 4
- "check schema alignment" → Phase 5
- "what's next for {city}" → Status check + next phase
- "status" → Full dashboard for all cities

## Multi-City Status

If the user asks "status" without naming a city, show a summary for ALL cities:

```bash
for dir in data_*/; do
  CITY=$(basename "$dir" | sed 's/data_//')
  # Skip berlin_primary (it's a variant, not a separate city)
  [[ "$CITY" == "berlin_primary" ]] && continue
  HAS_FINAL=$(ls ${dir}final/*master_table_final* 2>/dev/null | wc -l)
  HAS_QA=$(ls ${dir}final/QA_REPORT_*.md 2>/dev/null | wc -l)
  echo "${CITY}: final_output=${HAS_FINAL} qa_report=${HAS_QA}"
done
```

---

## Evaluations

These check that the workflow coordinator is functioning correctly.

### EVAL-1: City Detection Works
When the user says "let's add Munich", the coordinator should identify "munich" as the city and check for `data_munich/` and `scripts_munich/`.
**Pass criteria:** City name is correctly extracted and lowercased for directory lookups.

### EVAL-2: State Assessment Runs All Checks
The coordinator should check for: research doc, scripts directory, enrichment stubs, final output, QA report, and schema drift report.
**Pass criteria:** All 5 phases are assessed and reported.

### EVAL-3: Correct Phase Routing
| State | Expected Route |
|-------|---------------|
| Brand new city | `/data-source-research` |
| Research done, no scaffold | `/new-city-pipeline` |
| Scaffold done, stubs exist | `/enrich-phase-builder` |
| Final output exists, no QA | `/pipeline-qa` |
| QA done, not in drift report | `/schema-drift-check` |
**Pass criteria:** The coordinator invokes the correct sub-skill for each state.

### EVAL-4: Status Dashboard Is Accurate
Compare the dashboard output against actual filesystem state.
**Pass criteria:** Every checkbox ([x], [~], [ ]) accurately reflects what exists on disk.

### EVAL-5: Sub-Skill Invocation
After routing, the coordinator should actually invoke the sub-skill (via `/data-source-research`, `/new-city-pipeline`, etc.), not just describe what to do.
**Pass criteria:** The Skill tool is called with the correct skill name.

### EVAL-6: Post-Phase Return
After a sub-skill completes, the coordinator should re-check state and offer the next phase.
**Pass criteria:** Updated status is shown and next action is suggested.

### EVAL-7: Multi-City Status Works
When no city is named, the coordinator should scan all `data_*/` directories and show a summary.
**Pass criteria:** All existing cities appear in the summary with correct final_output and qa_report counts.

### EVAL-8: Enrichment Order Is Correct
In Phase 3, the coordinator should suggest enrichments in the correct dependency order (scraper first, schema transformer last).
**Pass criteria:** Enrichments are presented in the order listed in "Enrichment Phase Order" section.
