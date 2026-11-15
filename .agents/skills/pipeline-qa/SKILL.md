---
name: pipeline-qa
description: "SchoolNossa project only. Validate a completed city pipeline's output for data quality, schema alignment, coverage, and correctness. Use whenever the user asks to check, validate, or QA pipeline output — 'does the data look right?', 'check the NRW output', 'validate Hamburg pipeline', 'run QA on the final CSV', 'how's the coverage?', 'are there any issues with the data?'. Also trigger after any pipeline run completes, or when the user reports unexpected values or missing data in the frontend. Only applies when working in the schoolnossa repository."
allowed-tools: Bash, Read, Write, Glob, Grep, Agent
---

# Pipeline QA — Validate City Pipeline Output

Run comprehensive quality checks on a completed city pipeline. Produces a QA report and flags any issues that need fixing.

## Required Input

Ask the user for:
1. **City** (e.g. `berlin`, `hamburg`, `nrw`, `munich`)
2. **School type** — `primary`, `secondary`, or `both`
3. **Whether to check against Berlin schema** (default: yes for non-Berlin cities)

## Step 1: Locate Final Output Files

Find the pipeline outputs:
```python
data_{city}/final/{city}_{type}_school_master_table_final.csv
data_{city}/final/{city}_{type}_school_master_table_final_with_embeddings.parquet
```

For Berlin (no city prefix):
```python
data_berlin/final/school_master_table_final.csv
data_berlin/final/school_master_table_final_with_embeddings.parquet
```

If files don't exist, report which are missing and stop.

## Step 2: Run Quality Checks

Execute ALL of the following checks and collect results into a report.

### Check 2.1: Row Count Sanity

```python
import pandas as pd
df = pd.read_csv(final_csv_path)
print(f"Total schools: {len(df)}")

# Expected minimums by city (from known data)
expected_minimums = {
    'berlin_secondary': 200,
    'berlin_primary': 350,
    'hamburg': 300,
    'nrw_primary': 200,
    'nrw_secondary': 130,
}
key = f"{city}_{school_type}" if city != 'berlin' else f"berlin_{school_type}"
if key in expected_minimums:
    assert len(df) >= expected_minimums[key], f"WARN: Only {len(df)} schools, expected >= {expected_minimums[key]}"
```

### Check 2.2: Coordinate Coverage

```python
has_lat = df['latitude'].notna().sum()
has_lon = df['longitude'].notna().sum()
coord_pct = has_lat / len(df) * 100
print(f"Coordinate coverage: {coord_pct:.1f}% ({has_lat}/{len(df)})")
assert coord_pct >= 95, f"FAIL: Coordinate coverage {coord_pct:.1f}% below 95% threshold"

# Check for obviously wrong coordinates (outside Germany)
if 'latitude' in df.columns:
    bad_lat = df[(df['latitude'] < 47) | (df['latitude'] > 55.5)]['latitude']
    bad_lon = df[(df['longitude'] < 5.5) | (df['longitude'] > 15.5)]['longitude']
    assert len(bad_lat) == 0, f"FAIL: {len(bad_lat)} schools with latitude outside Germany"
    assert len(bad_lon) == 0, f"FAIL: {len(bad_lon)} schools with longitude outside Germany"
```

### Check 2.3: Enrichment Coverage

For each enrichment type, measure what percentage of schools have data:

```python
enrichment_checks = {
    'traffic': ['traffic_volume_total', 'traffic_accidents_total', 'traffic_score'],
    'transit': ['transit_bus_1_name', 'transit_score'],
    'crime': ['crime_total', 'crime_rate_per_1000'],
    'poi': ['poi_supermarket_count', 'poi_restaurant_count'],
    'demographics': ['sozialindex', 'belastungsstufe', 'migration_percentage'],
    'descriptions': ['description_en', 'description_de', 'summary_en'],
}

for enrichment, columns in enrichment_checks.items():
    present_cols = [c for c in columns if c in df.columns]
    if not present_cols:
        print(f"  {enrichment}: NO COLUMNS FOUND")
        continue
    for col in present_cols:
        coverage = df[col].notna().sum() / len(df) * 100
        status = "OK" if coverage > 50 else "LOW" if coverage > 10 else "FAIL"
        print(f"  {enrichment}/{col}: {coverage:.1f}% [{status}]")
```

### Check 2.4: Data Type Validation

```python
expected_types = {
    'latitude': 'float64',
    'longitude': 'float64',
    'schulnummer': ['int64', 'object', 'str'],  # varies by city
    'schulname': 'object',
    'crime_total': ['float64', 'int64'],
    'transit_score': 'float64',
}
for col, expected in expected_types.items():
    if col in df.columns:
        actual = str(df[col].dtype)
        if isinstance(expected, list):
            ok = actual in expected
        else:
            ok = actual == expected
        if not ok:
            print(f"  TYPE MISMATCH: {col} is {actual}, expected {expected}")
```

### Check 2.5: Duplicate Detection

```python
# Check for duplicate school IDs
id_col = 'schulnummer' if 'schulnummer' in df.columns else 'bsn'
if id_col in df.columns:
    dupes = df[df.duplicated(subset=[id_col], keep=False)]
    if len(dupes) > 0:
        print(f"  WARN: {len(dupes)} duplicate {id_col} values")
        print(dupes[[id_col, 'schulname']].to_string())

# Check for duplicate coordinates (different schools at exact same location)
coord_dupes = df[df.duplicated(subset=['latitude', 'longitude'], keep=False)]
if len(coord_dupes) > 5:
    print(f"  WARN: {len(coord_dupes)} schools share exact coordinates")
```

### Check 2.6: Embedding Verification (parquet only)

```python
import pyarrow.parquet as pq
pf = pq.read_table(parquet_path)
pdf = pf.to_pandas()

if 'embedding' in pdf.columns:
    # Check embedding dimensions
    sample = pdf['embedding'].dropna().iloc[0]
    if isinstance(sample, (list, np.ndarray)):
        dim = len(sample)
        print(f"  Embedding dimension: {dim}")
        assert dim in [768, 3072], f"FAIL: Unexpected embedding dimension {dim}"

    # Check embedding coverage
    embed_coverage = pdf['embedding'].notna().sum() / len(pdf) * 100
    print(f"  Embedding coverage: {embed_coverage:.1f}%")
    assert embed_coverage >= 90, f"FAIL: Embedding coverage {embed_coverage:.1f}% below 90%"
```

### Check 2.7: Similar Schools Populated

```python
similar_cols = [c for c in df.columns if 'similar_school' in c.lower()]
if similar_cols:
    for col in similar_cols[:3]:
        coverage = df[col].notna().sum() / len(df) * 100
        print(f"  {col}: {coverage:.1f}%")
else:
    print("  WARN: No similar_school columns found")
```

### Check 2.8: Description Quality Spot-Check

```python
desc_col = 'description_en' if 'description_en' in df.columns else 'school_profile_en'
if desc_col in df.columns:
    descs = df[desc_col].dropna()
    avg_len = descs.str.len().mean()
    short = (descs.str.len() < 100).sum()
    empty = df[desc_col].isna().sum()
    print(f"  Descriptions: {len(descs)}/{len(df)} populated, avg length {avg_len:.0f} chars")
    print(f"  Short (<100 chars): {short}, Empty: {empty}")
    if avg_len < 200:
        print("  WARN: Average description length seems low")
```

### Check 2.9: NULL Column Detection

```python
# Find columns that are entirely NULL (likely failed enrichment)
all_null = [col for col in df.columns if df[col].isna().all()]
if all_null:
    print(f"  WARN: {len(all_null)} columns are entirely NULL:")
    for col in all_null:
        print(f"    - {col}")
```

## Step 3: Berlin Schema Alignment (non-Berlin cities only)

```python
berlin_ref = pd.read_parquet('data_berlin/final/school_master_table_final_with_embeddings.parquet')
berlin_cols = set(berlin_ref.columns)
city_cols = set(df.columns)

missing_from_city = berlin_cols - city_cols
extra_in_city = city_cols - berlin_cols

print(f"\nSchema Alignment vs Berlin:")
print(f"  Berlin columns: {len(berlin_cols)}")
print(f"  {city} columns: {len(city_cols)}")
print(f"  Missing from {city}: {len(missing_from_city)}")
print(f"  Extra in {city}: {len(extra_in_city)}")

if missing_from_city:
    print(f"\n  Missing columns (should be NULL-filled):")
    for col in sorted(missing_from_city):
        print(f"    - {col}")
```

## Step 4: Generate QA Report

Write results to `data_{city}/final/QA_REPORT_{type}.md`:

```markdown
# QA Report: {City} {Type} Schools

**Date:** {today}
**Pipeline version:** {git_hash}
**Input file:** {final_csv_path}

## Summary

| Metric | Value | Status |
|--------|-------|--------|
| Total schools | N | OK/WARN |
| Coordinate coverage | N% | OK/FAIL |
| Embedding coverage | N% | OK/FAIL |
| Duplicate IDs | N | OK/WARN |
| All-NULL columns | N | OK/WARN |
| Berlin schema alignment | N missing | OK/WARN |

## Enrichment Coverage

| Enrichment | Key Column | Coverage | Status |
|------------|-----------|----------|--------|
| Traffic | ... | ...% | OK/LOW/FAIL |
| Transit | ... | ...% | OK/LOW/FAIL |
| Crime | ... | ...% | OK/LOW/FAIL |
| POI | ... | ...% | OK/LOW/FAIL |
| Demographics | ... | ...% | OK/LOW/FAIL |
| Descriptions | ... | ...% | OK/LOW/FAIL |

## Issues Found

1. [list of FAIL and WARN items]

## Recommendations

- [specific actions to fix issues]
```

---

## Evaluations

These evaluations check that the QA process itself ran correctly and produced a complete report.

### EVAL-1: QA Report File Exists
```bash
test -f "data_{city}/final/QA_REPORT_{type}.md" && echo "PASS" || echo "FAIL: QA report not generated"
```
**Pass criteria:** QA report file exists.

### EVAL-2: All 9 Check Categories Ran
```bash
python3 -c "
with open('data_{city}/final/QA_REPORT_{type}.md') as f:
    content = f.read()
checks = ['Row Count', 'Coordinate', 'Enrichment Coverage', 'Data Type', 'Duplicate', 'Embedding', 'Similar School', 'Description', 'NULL Column']
found = sum(1 for c in checks if c.lower() in content.lower())
print(f'{found}/9 checks documented')
assert found >= 7, f'FAIL: Only {found}/9 checks documented'
print('PASS')
"
```
**Pass criteria:** At least 7 of 9 check categories appear in the report.

### EVAL-3: Summary Table Has Status Column
```bash
grep -c "OK\|WARN\|FAIL\|LOW" "data_{city}/final/QA_REPORT_{type}.md"
```
**Pass criteria:** Count >= 5. Report contains explicit status indicators.

### EVAL-4: No Critical Failures Left Unaddressed
```bash
python3 -c "
with open('data_{city}/final/QA_REPORT_{type}.md') as f:
    content = f.read()
fails = content.lower().count('fail')
has_recommendations = 'recommend' in content.lower() or 'action' in content.lower() or 'fix' in content.lower()
if fails > 0 and not has_recommendations:
    print(f'FAIL: {fails} failures found but no recommendations section')
else:
    print(f'PASS: {fails} failures, recommendations present: {has_recommendations}')
"
```
**Pass criteria:** If any FAIL statuses exist, the report must include a recommendations section.

### EVAL-5: Coordinate Bounds Check Ran
```bash
grep -iE "(47|55|germany|bounds|outside)" "data_{city}/final/QA_REPORT_{type}.md" || echo "WARN: Coordinate bounds check may not have run"
```
**Pass criteria:** Report mentions coordinate boundary validation.

### EVAL-6: Schema Alignment Documented (non-Berlin only)
```bash
if [ "{city}" != "berlin" ]; then
  grep -i "berlin.*schema\|schema.*alignment\|missing.*column" "data_{city}/final/QA_REPORT_{type}.md" || echo "FAIL: No schema alignment check"
else
  echo "PASS: Berlin is the reference schema"
fi
```
**Pass criteria:** Non-Berlin cities have schema alignment results documented.

### EVAL-7: Enrichment Coverage Table Complete
```bash
python3 -c "
with open('data_{city}/final/QA_REPORT_{type}.md') as f:
    content = f.read()
enrichments = ['Traffic', 'Transit', 'Crime', 'POI']
found = sum(1 for e in enrichments if e.lower() in content.lower())
print(f'{found}/4 core enrichments in coverage table')
assert found >= 4, f'FAIL: Only {found}/4 enrichments covered'
print('PASS')
"
```
**Pass criteria:** All 4 core enrichments (traffic, transit, crime, POI) appear in coverage table.

### EVAL-8: Report Contains Actual Numbers
```bash
python3 -c "
import re
with open('data_{city}/final/QA_REPORT_{type}.md') as f:
    content = f.read()
numbers = re.findall(r'\d+\.?\d*%', content)
print(f'Found {len(numbers)} percentage values in report')
assert len(numbers) >= 5, f'FAIL: Only {len(numbers)} percentage values — report may lack actual data'
print('PASS')
"
```
**Pass criteria:** Report contains at least 5 percentage values (actual measurements, not placeholders).

### EVAL-9: Git Hash Recorded
```bash
grep -iE "(git|commit|hash|version)" "data_{city}/final/QA_REPORT_{type}.md" || echo "WARN: No git hash in report"
```
**Pass criteria:** Report includes pipeline version or git hash for reproducibility.

### EVAL-10: Report Is Not a Template
```bash
grep -iE "(TODO|TBD|FIXME|placeholder|N/A.*N/A.*N/A)" "data_{city}/final/QA_REPORT_{type}.md" && echo "FAIL: Report contains placeholder content" || echo "PASS: Report has real data"
```
**Pass criteria:** No placeholder or template text remains — all values are populated from actual data.
