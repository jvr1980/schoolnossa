---
name: schema-drift-check
description: "SchoolNossa project only. Compare final parquet/CSV outputs across all cities to detect schema divergence — missing columns, type mismatches, naming inconsistencies, and undocumented city-specific columns. Use when the user asks 'do all cities match?', 'check schema alignment', 'compare columns across cities', 'is Hamburg compatible with Berlin?', or any question about cross-city data consistency. Also use after completing a pipeline for a new city, after modifying the Berlin reference schema, or when the frontend shows unexpected missing data for certain cities. Only applies when working in the schoolnossa repository."
allowed-tools: Bash, Read, Write, Glob, Grep, Agent
---

# Schema Drift Check — Cross-City Schema Comparison

Compare the final output schemas of all city pipelines to detect divergence from the Berlin reference schema. Produces a drift report with specific fix recommendations.

## Required Input

No input needed — this skill automatically discovers all cities with final output files.

## Step 1: Discover All City Outputs

Find all final parquet/CSV files across cities:

```python
from pathlib import Path
import pandas as pd

BASE = Path(".")
cities = {}

# Berlin secondary (reference schema)
for pattern in [
    "data_berlin/final/school_master_table_final_with_embeddings.parquet",
    "data_berlin/final/school_master_table_final.csv",
]:
    p = BASE / pattern
    if p.exists():
        cities.setdefault("berlin_secondary", {})["path"] = str(p)

# Berlin primary
for pattern in [
    "data_berlin_primary/final/primary_school_master_table_final_with_embeddings.parquet",
    "data_berlin_primary/final/primary_school_master_table_final.csv",
]:
    p = BASE / pattern
    if p.exists():
        cities.setdefault("berlin_primary", {})["path"] = str(p)

# Hamburg, NRW, and any other cities
for data_dir in BASE.glob("data_*/final/*master_table_final*"):
    city = data_dir.parent.parent.name.replace("data_", "")
    if city not in ["berlin", "berlin_primary"]:
        school_type = "primary" if "primary" in data_dir.name else "secondary"
        key = f"{city}_{school_type}"
        cities.setdefault(key, {})["path"] = str(data_dir)
```

Report which cities and school types were found.

## Step 2: Load Berlin Reference Schema

Berlin secondary is the **canonical reference**. All other cities should have at least these columns (possibly NULL-filled).

```python
berlin_ref = pd.read_parquet("data_berlin/final/school_master_table_final_with_embeddings.parquet")
berlin_cols = list(berlin_ref.columns)
berlin_types = {col: str(berlin_ref[col].dtype) for col in berlin_cols}
```

## Step 3: Compare Each City Against Reference

For each city output, check:

### 3a: Column Presence

```python
for city_key, info in cities.items():
    if city_key == "berlin_secondary":
        continue  # Skip reference

    df = pd.read_parquet(info["path"]) if info["path"].endswith(".parquet") else pd.read_csv(info["path"])
    city_cols = set(df.columns)
    berlin_set = set(berlin_cols)

    missing = berlin_set - city_cols      # In Berlin but not in this city
    extra = city_cols - berlin_set        # In this city but not in Berlin
    common = berlin_set & city_cols       # Shared columns

    info["missing"] = sorted(missing)
    info["extra"] = sorted(extra)
    info["common_count"] = len(common)
    info["total_cols"] = len(city_cols)
```

### 3b: Type Mismatches

For columns present in both, compare dtypes:

```python
    type_mismatches = {}
    for col in common:
        berlin_type = berlin_types[col]
        city_type = str(df[col].dtype)
        if berlin_type != city_type:
            # Allow compatible types (int64 ~ float64 for nullable ints)
            compatible = {
                ('int64', 'float64'), ('float64', 'int64'),
                ('object', 'str'), ('str', 'object'),
            }
            if (berlin_type, city_type) not in compatible:
                type_mismatches[col] = {"berlin": berlin_type, "city": city_type}

    info["type_mismatches"] = type_mismatches
```

### 3c: Column Order Alignment

Check if Berlin columns appear in the same order:

```python
    city_berlin_cols = [c for c in df.columns if c in berlin_set]
    berlin_order = [c for c in berlin_cols if c in city_cols]
    order_matches = city_berlin_cols == berlin_order
    info["order_matches"] = order_matches
```

### 3d: Naming Convention Check

Flag columns that don't follow established patterns:

```python
    import re
    bad_names = []
    for col in extra:
        # Valid patterns: lowercase with underscores, or known Berlin patterns
        if not re.match(r'^[a-z][a-z0-9_]*$', col) and col != 'embedding':
            bad_names.append(col)
    info["bad_names"] = bad_names
```

### 3e: NULL-Fill Verification

For Berlin columns that exist in the city but are entirely NULL, flag them:

```python
    all_null_berlin_cols = [col for col in common if df[col].isna().all()]
    info["all_null_berlin_cols"] = all_null_berlin_cols
```

## Step 4: Cross-City Extra Column Comparison

Identify city-specific columns that appear in multiple non-Berlin cities (candidates for promotion to the shared schema):

```python
from collections import Counter
all_extras = Counter()
for city_key, info in cities.items():
    if city_key != "berlin_secondary":
        for col in info.get("extra", []):
            all_extras[col] += 1

shared_extras = {col: count for col, count in all_extras.items() if count >= 2}
```

## Step 5: Generate Drift Report

Write to `docs/SCHEMA_DRIFT_REPORT.md`:

```markdown
# Schema Drift Report

**Date:** {today}
**Reference:** Berlin secondary ({N} columns)
**Cities compared:** {list of cities}

## Summary

| City | Total Cols | Missing from Berlin | Extra | Type Mismatches | Order OK | Score |
|------|-----------|---------------------|-------|-----------------|----------|-------|
| berlin_secondary | {N} | — (reference) | — | — | — | 100% |
| berlin_primary | ... | ... | ... | ... | ... | ...% |
| hamburg | ... | ... | ... | ... | ... | ...% |
| nrw_secondary | ... | ... | ... | ... | ... | ...% |
| nrw_primary | ... | ... | ... | ... | ... | ...% |

**Score** = (common columns / Berlin columns) * 100, minus penalties for type mismatches.

## Missing Columns by City

### {city}
Columns in Berlin but missing from {city}:
- `column_name` — [likely category: traffic/transit/crime/poi/demographics/metadata]
...

## Extra Columns by City

### {city}
City-specific columns not in Berlin schema:
- `column_name` — [purpose if identifiable]
...

## Type Mismatches

| Column | Berlin Type | {City} Type | Severity |
|--------|------------|-------------|----------|
| ... | float64 | object | HIGH |
| ... | int64 | float64 | LOW (compatible) |

## Naming Convention Issues

Columns not following `lowercase_with_underscores`:
- {city}: `ColumnName` (should be `column_name`)
...

## Shared Extra Columns (candidates for schema promotion)

Columns that appear in 2+ non-Berlin cities:
| Column | Cities | Consider adding to Berlin? |
|--------|--------|---------------------------|
| ... | hamburg, nrw | Yes/No |

## All-NULL Berlin Columns

Berlin columns that exist but are entirely NULL in other cities:
| Column | Cities where all-NULL |
|--------|----------------------|
| ... | hamburg, nrw |

These may indicate enrichments that failed or are not applicable.

## Recommendations

1. [Specific actions to fix critical drift issues]
2. [Columns to add/remove/rename]
3. [Schema transformer updates needed]
```

---

## Evaluations

These checks verify the drift report is complete and accurate.

### EVAL-1: Report File Exists
```bash
test -f "docs/SCHEMA_DRIFT_REPORT.md" && echo "PASS" || echo "FAIL: Report not generated"
```
**Pass criteria:** Report file exists at `docs/SCHEMA_DRIFT_REPORT.md`.

### EVAL-2: All Discovered Cities Are In Report
```bash
python3 -c "
from pathlib import Path
# Find all cities with final data
cities_found = set()
for d in Path('.').glob('data_*/final/*master_table_final*'):
    city = d.parent.parent.name.replace('data_', '')
    cities_found.add(city)

with open('docs/SCHEMA_DRIFT_REPORT.md') as f:
    content = f.read()

for city in cities_found:
    if city not in content.lower():
        print(f'FAIL: City {city} has data but is not in report')
print(f'Checked {len(cities_found)} cities')
print('PASS' if all(c in content.lower() for c in cities_found) else 'FAIL')
"
```
**Pass criteria:** Every city with final output files appears in the report.

### EVAL-3: Summary Table Has Score Column
```bash
grep -c "Score\|score\|%" "docs/SCHEMA_DRIFT_REPORT.md"
```
**Pass criteria:** Count >= 3. Report contains alignment scores for each city.

### EVAL-4: Missing Columns Are Categorized
```bash
grep -iE "(traffic|transit|crime|poi|demographic|metadata|description)" "docs/SCHEMA_DRIFT_REPORT.md"
```
**Pass criteria:** Missing columns are tagged with their likely enrichment category.

### EVAL-5: Type Mismatches Section Exists
```bash
grep -i "type mismatch" "docs/SCHEMA_DRIFT_REPORT.md" || echo "FAIL: No type mismatch section"
```
**Pass criteria:** Report has a type mismatches section.

### EVAL-6: Recommendations Section Not Empty
```bash
python3 -c "
with open('docs/SCHEMA_DRIFT_REPORT.md') as f:
    content = f.read()
rec_idx = content.lower().find('## recommendation')
if rec_idx == -1:
    print('FAIL: No recommendations section')
else:
    rec_content = content[rec_idx:]
    lines = [l for l in rec_content.split('\n') if l.strip() and not l.startswith('#')]
    print(f'Recommendations section has {len(lines)} lines')
    assert len(lines) >= 2, 'FAIL: Recommendations section is too short'
    print('PASS')
"
```
**Pass criteria:** Recommendations section has at least 2 actionable items.

### EVAL-7: Report Contains Actual Column Counts
```bash
python3 -c "
import re
with open('docs/SCHEMA_DRIFT_REPORT.md') as f:
    content = f.read()
numbers = re.findall(r'\d+', content)
# Report should contain many numbers (column counts, percentages, etc.)
assert len(numbers) >= 20, f'FAIL: Only {len(numbers)} numbers in report — may lack actual data'
print(f'PASS: {len(numbers)} numeric values in report')
"
```
**Pass criteria:** Report contains at least 20 numeric values (column counts, scores, etc.).

### EVAL-8: Berlin Reference Column Count Is Plausible
```bash
python3 -c "
import re
with open('docs/SCHEMA_DRIFT_REPORT.md') as f:
    content = f.read()
# Berlin reference should have 100+ columns
match = re.search(r'berlin.*?(\d+)\s*columns', content, re.IGNORECASE)
if match:
    count = int(match.group(1))
    assert count >= 80, f'FAIL: Berlin reference has only {count} columns — seems too low'
    print(f'PASS: Berlin reference has {count} columns')
else:
    print('WARN: Could not find Berlin column count in report')
"
```
**Pass criteria:** Berlin reference column count is >= 80 (typical for a fully enriched pipeline).

### EVAL-9: Cross-City Extras Section Exists
```bash
grep -iE "(shared extra|candidate|promotion|multiple cities)" "docs/SCHEMA_DRIFT_REPORT.md" || echo "FAIL: No cross-city extras analysis"
```
**Pass criteria:** Report analyzes columns shared across multiple non-Berlin cities.

### EVAL-10: No Stale Data
```bash
python3 -c "
with open('docs/SCHEMA_DRIFT_REPORT.md') as f:
    content = f.read()
# Check date is recent (within last 7 days)
import re
from datetime import datetime, timedelta
date_match = re.search(r'(\d{4}-\d{2}-\d{2})', content)
if date_match:
    report_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
    age = (datetime.now() - report_date).days
    assert age <= 7, f'FAIL: Report date is {age} days old'
    print(f'PASS: Report is {age} days old')
else:
    print('WARN: No date found in report')
"
```
**Pass criteria:** Report date is within the last 7 days.

### EVAL-11: All-NULL Analysis Performed
```bash
grep -i "all.*null\|entirely.*null\|null.*column" "docs/SCHEMA_DRIFT_REPORT.md" || echo "FAIL: No all-NULL column analysis"
```
**Pass criteria:** Report identifies columns that are present but entirely NULL.

### EVAL-12: Naming Convention Check Performed
```bash
grep -iE "(naming.*convention\|convention.*issue\|lowercase\|underscore)" "docs/SCHEMA_DRIFT_REPORT.md" || echo "FAIL: No naming convention check"
```
**Pass criteria:** Report includes a naming convention analysis section.
