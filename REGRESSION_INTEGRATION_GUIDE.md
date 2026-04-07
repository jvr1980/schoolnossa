# Regression Pipeline Integration Guide

## What was built (this repo) vs. what you have locally

This document maps the regression forecasting pipeline built here to your local
SchoolNossa project structure, so you know exactly which files to copy and where
they fit.

---

## 1. Components to Transfer

### Core regression engine (copy as-is)

| GitHub file | Purpose | Where to put locally |
|---|---|---|
| `src/pipeline/scorer.py` | OLS regression, forward stepwise selection, CV, diagnostics | `scripts_shared/regression/scorer.py` |
| `src/pipeline/types.py` | Data classes (CatchmentProfile, ModelDiagnostics, etc.) | `scripts_shared/regression/types.py` |
| `src/pipeline/dimensions.py` | Dimension metadata (18 dimensions with direction, source) | `scripts_shared/regression/dimensions.py` |

These three files are **self-contained** — they only depend on each other and
`numpy`. No database, no API, no external services. You can drop them into any
Python project.

### Diagnostic script (adapt paths)

| GitHub file | Purpose | Where to put locally |
|---|---|---|
| `scripts/run_diagnostics.py` | CLI to train model and print diagnostics | `scripts_shared/run_regression_diagnostics.py` |

Needs minimal changes — just update the import paths and the database loading
section to match your local data format.

### Profiler (reference only — you probably don't need it)

| GitHub file | Purpose | Notes |
|---|---|---|
| `src/pipeline/profiler.py` | Queries crime, population, Google Places | Your local project already does enrichment via `scripts_berlin/enrichment/` and `scripts_shared/enrichment/`. You won't need this file — instead, build CatchmentProfile objects from your existing enriched data. |

### Data services (reference only)

| GitHub file | Your local equivalent |
|---|---|
| `src/services/crime.py` | `scripts_berlin/enrichment/enrich_crime.py` |
| `src/services/population.py` | (Zensus data if you have it, or postcode-level data) |
| `src/services/google_places.py` | `scripts_shared/enrichment/enrich_pois.py` (if exists) |

---

## 2. How the Regression Pipeline Works

```
Your existing enriched school data (CSV/JSON)
        │
        ▼
Build CatchmentProfile objects  ← types.py
(one per school, with dimension values)
        │
        ▼
train_and_diagnose()  ← scorer.py
        │
        ├── normalize_profiles()     → min-max to 0-100
        ├── _greedy_feature_selection() → additive forward stepwise
        ├── _fit_ols()               → β = (X'X)⁻¹X'y
        ├── _kfold_cv()              → k-fold cross-validation
        ├── _decompose_contributions() → per-school feature breakdown
        │
        ▼
ModelDiagnostics object  ← types.py
(R², CV, feature importance, predictions, contributions)
```

---

## 3. Integration Steps

### Step 1: Copy the core files

```bash
# In your local project
mkdir -p scripts_shared/regression
cp scorer.py   scripts_shared/regression/
cp types.py    scripts_shared/regression/
cp dimensions.py scripts_shared/regression/
touch scripts_shared/regression/__init__.py
```

### Step 2: Fix imports in the copied files

In `scorer.py`, change:
```python
# FROM:
from .dimensions import DIMENSIONS, Direction
from .types import ...

# TO:
from scripts_shared.regression.dimensions import DIMENSIONS, Direction
from scripts_shared.regression.types import ...
```

Or if you prefer relative imports, keep them as-is — they'll work since all
three files are in the same package.

### Step 3: Create the bridge script

This is the key integration point. You need a script that:
1. Loads your existing Berlin school data (from your CSVs or database)
2. Loads Abitur grades (your scraped data)
3. Builds `CatchmentProfile` objects from your enriched data
4. Calls `train_and_diagnose()`

Here's a template:

```python
#!/usr/bin/env python3
"""
Train regression model on Berlin school data and print diagnostics.

Reads enriched school data from data_berlin/ and trains a model to
predict Abitur average grade from catchment area features.
"""

import json
import csv
import sys
import os
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts_shared.regression.types import CatchmentProfile
from scripts_shared.regression.scorer import train_and_diagnose
from scripts_shared.regression.dimensions import DIMENSIONS


def load_schools_and_profiles():
    """Load your enriched Berlin school data.

    ADAPT THIS to your actual data format. Below is a template
    assuming you have a JSON or CSV with enriched school data.
    """
    profiles = []
    labeled = {}
    school_info = {}

    # Option A: Load from your enriched JSON/CSV
    # Adjust the path and field names to match your data
    data_path = "data_berlin/schools_enriched.json"  # or .csv

    with open(data_path) as f:
        schools = json.load(f)  # or csv.DictReader(f)

    for school in schools:
        sid = school["school_id"]  # or whatever your ID field is
        lat = float(school["latitude"])
        lng = float(school["longitude"])

        p = CatchmentProfile(
            school_id=sid,
            latitude=lat,
            longitude=lng,
            radius_m=1000,
        )

        # Map your enriched fields to dimension keys
        # These are the fields that the regression will use as predictors
        if "crime_index" in school:
            p.set("crime_index", float(school["crime_index"]))
        if "transit_count" in school:
            p.set("transit_count", float(school["transit_count"]))
        if "population_density" in school:
            p.set("population_density", float(school["population_density"]))
        # ... add more dimensions as available in your data

        profiles.append(p)
        school_info[sid] = {
            "name": school.get("name", sid),
            "district": school.get("district", ""),
        }

        # Abitur grade = the target variable
        abitur = school.get("abitur_average_grade")
        if abitur is not None and abitur != "":
            labeled[sid] = float(abitur)

    return profiles, labeled, school_info


def main():
    profiles, labeled, school_info = load_schools_and_profiles()

    print(f"Loaded {len(profiles)} schools, {len(labeled)} with Abitur grades")

    if len(labeled) < 5:
        print("Need at least 5 labeled schools for regression.")
        sys.exit(1)

    diag = train_and_diagnose(
        profiles=profiles,
        labeled=labeled,
        cv_folds=5,
    )

    if diag is None:
        print("Model training failed.")
        sys.exit(1)

    # Use the same print functions from run_diagnostics.py
    # (copy print_diagnostics() and related functions)
    print(f"R² = {diag.r_squared:.4f}")
    print(f"Adjusted R² = {diag.adjusted_r_squared:.4f}")
    print(f"Features selected: {diag.n_features}")
    for f in diag.features:
        print(f"  {f.label}: std_coef={f.standardized_coef:+.4f}, "
              f"partial_R²={f.partial_r_squared:.4f}")


if __name__ == "__main__":
    main()
```

### Step 4: Adapt dimensions to your data

Your local project likely has enrichment fields that don't perfectly match the
dimension keys defined in `dimensions.py`. You have two options:

**Option A**: Rename your enrichment fields to match the dimension keys
(e.g., make sure your crime enrichment outputs a field called `crime_index`)

**Option B**: Add new dimensions to `dimensions.py` to match your field names.
Just add more `_register()` calls:

```python
_register(
    DimensionMeta(
        key="traffic_index",  # your field name
        label="Traffic volume",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="Traffic volume near school",
    ),
)
```

---

## 4. What Your Local Data Needs

For the regression to work, you need:

1. **Catchment features** (predictors) — at least 3-4 numeric dimensions per school
   - Your enrichment scripts already produce these (crime, transit, POIs, etc.)

2. **Abitur average grades** (target variable) — for at least 5 schools
   - Your Berlin scraper likely already has this in your data
   - Look in `data_berlin/` for scraped Abitur data

3. **School coordinates** — latitude/longitude for each school
   - Already in your school data from geocoding

### Minimum viable data for a first test:

| Field | Source in your project |
|---|---|
| `school_id` | Your school identifier |
| `latitude`, `longitude` | Geocoded coordinates |
| `abitur_average_grade` | Scraped from bildung.berlin or equivalent |
| `crime_index` | From `enrich_crime.py` output |
| Any other numeric enrichment | From your enrichment pipeline |

---

## 5. Key Design Decisions

### Why forward stepwise selection?
With potentially 15+ dimensions but only ~100 labeled schools, overfitting is a
real risk. Forward stepwise adds one feature at a time, only keeping those that
meaningfully improve R². The full candidate evaluation at each step lets you see
which features almost made the cut.

### Why min-max normalization?
Dimensions have wildly different scales (crime index 0-200 vs. transit count 0-20
vs. rent €7-15). Min-max to 0-100 makes coefficients comparable. The
standardized coefficients then show the relative importance of each feature.

### Why OLS instead of sklearn?
No external dependencies beyond numpy. The closed-form solution is fast,
interpretable, and gives exact results. For the sample sizes we're working with
(50-200 schools), there's no advantage to iterative solvers.

### Lower-is-better inversion
German Abitur: 1.0 is best, 4.0 is worst. When a dimension like `crime_index`
has direction `LOWER_BETTER`, normalization inverts it so that lower raw values
get higher normalized scores (0-100). This means a positive regression
coefficient always means "this feature is associated with better outcomes."

---

## 6. Extending to Hamburg and Beyond

The regression engine is city-agnostic. To use it for Hamburg:

1. Profile Hamburg schools with the same dimension keys
2. Train on Hamburg's labeled schools (Hamburg also publishes Abitur data)
3. Or: train on Berlin, predict Hamburg (transfer learning — inspect confidence scores)

For Germany-wide prediction:
1. Train on Berlin + Hamburg combined (both have Abitur data)
2. Profile target schools in other states using Zensus + Google Places
3. Predictions will have lower confidence (further from training distribution)

---

## 7. Files Summary

### Must copy (3 files, self-contained):
- `src/pipeline/scorer.py` → your regression engine
- `src/pipeline/types.py` → data classes
- `src/pipeline/dimensions.py` → dimension definitions

### Should copy (1 file, adapt paths):
- `scripts/run_diagnostics.py` → diagnostic CLI with demo mode

### Reference only (already have equivalents):
- `src/pipeline/profiler.py`
- `src/services/crime.py`
- `src/services/population.py`
- `src/services/google_places.py`

### Not needed locally:
- `src/api/` (FastAPI endpoints — only for web deployment)
- `src/models/` (PostgreSQL models — you use your own data format)
- `src/collectors/` (Berlin Open Data — you have your own scrapers)
