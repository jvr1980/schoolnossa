#!/usr/bin/env python3
"""
School Performance Regression — Real Data Bridge

Loads enriched school data from the SchoolNossa pipeline (Berlin, Hamburg, NRW)
and trains a regression model to predict Abitur average grades from catchment
area features. Produces full diagnostics.

Usage:
    # Berlin secondary schools (default):
    python scripts_shared/regression/run_regression.py

    # Hamburg:
    python scripts_shared/regression/run_regression.py --city hamburg

    # All cities combined:
    python scripts_shared/regression/run_regression.py --city all

    # Export predictions:
    python scripts_shared/regression/run_regression.py --export predictions.csv

    # Force specific features:
    python scripts_shared/regression/run_regression.py --features crime_index transit_accessibility belastungsstufe
"""

import argparse
import csv
import json
import sys
import os
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
from scripts_shared.regression.types import CatchmentProfile, ModelDiagnostics
from scripts_shared.regression.scorer import train_and_diagnose
from scripts_shared.regression.dimensions import DIMENSIONS


# ---------------------------------------------------------------------------
# GISD + Zensus data loaders
# ---------------------------------------------------------------------------

GISD_PATH = PROJECT_ROOT / "data_shared" / "gisd" / "GISD_Bund_PLZ5.csv"
ZENSUS_DIR = PROJECT_ROOT / "data_shared" / "zensus"

_gisd_cache = None
_zensus_cache = {}


def _load_gisd() -> dict:
    """Load GISD PLZ5 data, returning {plz5_str: {score, quintile, population}}."""
    global _gisd_cache
    if _gisd_cache is not None:
        return _gisd_cache

    if not GISD_PATH.exists():
        print(f"  WARNING: GISD data not found at {GISD_PATH}")
        _gisd_cache = {}
        return _gisd_cache

    df = pd.read_csv(GISD_PATH)
    latest_year = df["year"].max()
    df = df[df["year"] == latest_year].copy()
    df["PLZ5"] = df["PLZ5"].astype(str).str.zfill(5)

    _gisd_cache = {}
    for _, row in df.iterrows():
        _gisd_cache[row["PLZ5"]] = {
            "gisd_score": row["gisd_score"],
            "gisd_quintile": row["gisd_5"],
            "gisd_population": row["population"],
        }
    print(f"  GISD: loaded {len(_gisd_cache)} PLZ5 entries (year={latest_year})")
    return _gisd_cache


def _load_zensus(city: str) -> "pd.DataFrame | None":
    """Load Zensus 100m grid parquet for a city.

    For "nrw", loads and concatenates both Köln and Düsseldorf grids.
    """
    if city in _zensus_cache:
        return _zensus_cache[city]

    city_map = {"berlin": "berlin", "hamburg": "hamburg", "nrw_koeln": "koeln", "nrw_duesseldorf": "duesseldorf", "frankfurt": "frankfurt", "munich": "munich", "stuttgart": "stuttgart"}

    # NRW covers two cities — load and merge both
    if city == "nrw":
        frames = []
        for fname in ("koeln", "duesseldorf"):
            path = ZENSUS_DIR / f"{fname}_zensus_100m.parquet"
            if path.exists():
                frames.append(pd.read_parquet(path))
                print(f"  Zensus: loaded {len(frames[-1])} grid cells for {fname}")
        if frames:
            merged = pd.concat(frames, ignore_index=True)
            _zensus_cache[city] = merged
            return merged
        _zensus_cache[city] = None
        return None

    fname = city_map.get(city)
    if not fname:
        _zensus_cache[city] = None
        return None

    path = ZENSUS_DIR / f"{fname}_zensus_100m.parquet"
    if not path.exists():
        print(f"  WARNING: Zensus grid not found at {path}")
        _zensus_cache[city] = None
        return None

    df = pd.read_parquet(path)
    _zensus_cache[city] = df
    print(f"  Zensus: loaded {len(df)} grid cells for {fname}")
    return df


def _enrich_with_zensus(profile: CatchmentProfile, zensus_df: "pd.DataFrame", radius_m: float = 1000.0):
    """Aggregate Zensus grid cells within radius of the school into catchment dimensions."""
    if zensus_df is None or zensus_df.empty:
        return

    lat, lon = profile.latitude, profile.longitude

    # Approximate degree distances at German latitudes
    dlat = radius_m / 111320.0
    dlon = radius_m / (111320.0 * np.cos(np.radians(lat)))

    mask = (
        (zensus_df["lat"] >= lat - dlat) & (zensus_df["lat"] <= lat + dlat) &
        (zensus_df["lon"] >= lon - dlon) & (zensus_df["lon"] <= lon + dlon)
    )
    nearby = zensus_df[mask]

    if nearby.empty:
        return

    # Population-weighted aggregation
    pop = nearby["population"].fillna(0)
    total_pop = pop.sum()

    if total_pop > 0:
        profile.set("catchment_population", float(total_pop))
        # Area of the catchment circle in km²
        area_km2 = np.pi * (radius_m / 1000.0) ** 2
        profile.set("catchment_population_density", float(total_pop / area_km2))

        for col, dim_key in [
            ("avg_age", "catchment_avg_age"),
            ("foreigner_pct", "catchment_foreigner_pct"),
            ("avg_rent", "catchment_avg_rent"),
            ("vacancy_rate", "catchment_vacancy_rate"),
        ]:
            if col in nearby.columns:
                valid = nearby[[col, "population"]].dropna(subset=[col])
                if not valid.empty and valid["population"].sum() > 0:
                    weighted_avg = (valid[col] * valid["population"]).sum() / valid["population"].sum()
                    profile.set(dim_key, float(weighted_avg))


# ---------------------------------------------------------------------------
# Data loading — maps enriched CSV columns to CatchmentProfile dimensions
# ---------------------------------------------------------------------------

# Column mapping: CSV column name → dimension key
# Each city can have slightly different column names; we handle that here.

BERLIN_COLUMN_MAP = {
    # Crime
    "crime_total_crimes_avg": "crime_index",
    "crime_violent_crime_avg": "crime_violent",
    "crime_drug_offenses_avg": "crime_drug_offenses",
    "crime_safety_rank": "crime_safety_rank",
    # Transit
    "transit_stop_count_1000m": "transit_stop_count",
    "transit_all_lines_1000m": "transit_lines_count",
    "transit_accessibility_score": "transit_accessibility",
    "transit_rail_01_distance_m": "transit_nearest_m",
    # Traffic
    "plz_traffic_intensity": "traffic_intensity",
    "plz_avg_cars_per_hour": "traffic_cars_per_hour",
    "plz_avg_bikes_per_hour": "traffic_bikes_per_hour",
    "plz_avg_v85_speed": "traffic_speed_v85",
    # POIs
    "poi_supermarket_count_500m": "supermarket_count",
    "poi_restaurant_count_500m": "poi_restaurant_count",
    "poi_kita_count_500m": "poi_kita_count",
    "poi_primary_school_count_500m": "poi_school_count",
    # Demographics / school-level
    "migration_2024_25": "migration_pct_school",
    "belastungsstufe": "belastungsstufe",
    "nachfrage_prozent_2025_26": "nachfrage_prozent",
}

HAMBURG_COLUMN_MAP = {
    # Crime
    "crime_total_crimes_2024": "crime_index",
    "crime_violent_crime_avg": "crime_violent",
    "crime_drug_offenses_avg": "crime_drug_offenses",
    "crime_safety_rank": "crime_safety_rank",
    # Transit
    "hvv_stops_1000m": "transit_stop_count",
    "transit_accessibility_score": "transit_accessibility",
    "nearest_ubahn_distance_m": "transit_nearest_m",
    "transit_all_lines_1000m": "transit_lines_count",
    # Traffic
    "traffic_dtv_kfz": "traffic_cars_per_hour",
    # POIs
    "poi_supermarket_count_500m": "supermarket_count",
    "poi_restaurant_count_500m": "poi_restaurant_count",
    "poi_kita_count_500m": "poi_kita_count",
    # Demographics — sozialindex handled specially (text "Stufe X" → numeric)
}

NRW_COLUMN_MAP = {
    # Crime
    "crime_total_crimes_avg": "crime_index",
    "crime_violent_crime_avg": "crime_violent",
    "crime_safety_rank": "crime_safety_rank",
    # Transit
    "transit_stop_count_1000m": "transit_stop_count",
    "transit_all_lines_1000m": "transit_lines_count",
    "transit_accessibility_score": "transit_accessibility",
    # Traffic
    "traffic_avg_cars_per_hour": "traffic_cars_per_hour",
    "traffic_avg_bikes_per_hour": "traffic_bikes_per_hour",
    "traffic_v85_speed": "traffic_speed_v85",
    # POIs
    "poi_supermarket_count_500m": "supermarket_count",
    "poi_restaurant_count_500m": "poi_restaurant_count",
    "poi_kita_count_500m": "poi_kita_count",
    # Demographics
    "sozialindexstufe": "sozialindex",
}

# Frankfurt/Munich/Stuttgart use city-level crime (same value for all schools → not
# discriminative, but included for completeness; crime_safety_rank excluded as NON_PORTABLE).
FRANKFURT_COLUMN_MAP = {
    # Crime — city-level only; crime_safety_rank is excluded as NON_PORTABLE_DIM anyway
    "crime_total_crimes_2023": "crime_index",
    "crime_koerperverletzung_2023": "crime_violent",
    "crime_safety_rank": "crime_safety_rank",
    # Transit
    "transit_stop_count_1000m": "transit_stop_count",
    "transit_all_lines_1000m": "transit_lines_count",
    "transit_accessibility_score": "transit_accessibility",
    "transit_rail_01_distance_m": "transit_nearest_m",
    # POIs
    "poi_supermarket_count_500m": "supermarket_count",
    "poi_restaurant_count_500m": "poi_restaurant_count",
    "poi_kita_count_500m": "poi_kita_count",
    "poi_primary_school_count_500m": "poi_school_count",
    # Demographics
    "belastungsstufe": "belastungsstufe",
}

# Abitur column preference (try most recent first)
ABITUR_COLUMNS = [
    "abitur_durchschnitt_2024",
    "abitur_durchschnitt_2025",
    "abitur_durchschnitt_2023",
    "abitur_durchschnitt_2022",
]

ERFOLGSQUOTE_COLUMNS = [
    "abitur_erfolgsquote_2024",
    "abitur_erfolgsquote_2025",
]


def _safe_float(val: str) -> "float | None":
    """Convert string to float, returning None for empty/invalid values."""
    if val is None:
        return None
    val = str(val).strip().replace(",", ".")
    if not val or val.lower() in ("", "nan", "none", "n/a", "-"):
        return None
    try:
        return float(val)
    except ValueError:
        return None


def load_city_data(
    csv_path: str,
    column_map: dict,
    city_label: str,
    use_gisd: bool = True,
    use_zensus: bool = True,
):
    """Load enriched school CSV and build CatchmentProfiles.

    Returns:
        (profiles, labeled, school_info)
        labeled includes abitur grades; school_info includes erfolgsquote where available.
    """
    profiles = []
    labeled = {}
    school_info = {}

    gisd = _load_gisd() if use_gisd else {}
    zensus_df = _load_zensus(city_label) if use_zensus else None

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"  [{city_label}] Loaded {len(rows)} schools from {Path(csv_path).name}")

    gisd_hits = 0
    zensus_hits = 0

    for row in rows:
        sid = row.get("schulnummer", "").strip()
        if not sid:
            continue

        # Prefix with city to avoid ID collisions in multi-city mode
        full_sid = f"{city_label}_{sid}"

        lat = _safe_float(row.get("latitude"))
        lng = _safe_float(row.get("longitude"))
        if lat is None or lng is None:
            continue

        p = CatchmentProfile(
            school_id=full_sid,
            latitude=lat,
            longitude=lng,
            radius_m=1000,
        )

        # Map CSV columns to dimension keys
        for csv_col, dim_key in column_map.items():
            raw_val = row.get(csv_col, "")
            # Handle transit_lines_count: count comma-separated items
            if dim_key == "transit_lines_count" and raw_val and str(raw_val).strip():
                items = [x.strip() for x in str(raw_val).split(",") if x.strip()]
                p.set(dim_key, float(len(items)))
                continue
            val = _safe_float(raw_val)
            if val is not None:
                p.set(dim_key, val)

        # Handle text-format sozialindex ("Stufe 2" → 2.0)
        sozial_raw = row.get("sozialindex", "")
        if sozial_raw and "stufe" in str(sozial_raw).lower():
            import re
            m = re.search(r"(\d+)", str(sozial_raw))
            if m:
                p.set("sozialindex", float(m.group(1)))
        elif sozial_raw:
            sv = _safe_float(sozial_raw)
            if sv is not None:
                p.set("sozialindex", sv)

        # Handle NRW sozialindexstufe (already numeric)
        sozial_nrw = _safe_float(row.get("sozialindexstufe"))
        if sozial_nrw is not None:
            p.set("sozialindex", sozial_nrw)

        # --- School-internal features ---

        # Is Gymnasium (binary)
        # Prefer schulart when school_type is a generic placeholder (e.g. "secondary")
        stype_raw = row.get("school_type", "") or ""
        schulart_raw = row.get("schulart", "") or ""
        stype = (schulart_raw if stype_raw.lower() in ("secondary", "primary", "") else stype_raw).lower()
        if "gymnasium" in stype:
            p.set("is_gymnasium", 1.0)
        elif stype:  # only set 0 if we have type info
            p.set("is_gymnasium", 0.0)

        # Student count
        schueler_raw = _safe_float(row.get("schueler_2024_25") or row.get("schueler_2023_24") or row.get("schueler_gesamt"))
        if schueler_raw and schueler_raw > 0:
            p.set("student_count_total", schueler_raw)

        # Is private
        traeger = (row.get("traegerschaft") or row.get("rechtsform") or "").lower()
        if traeger:
            p.set("is_private", 0.0 if any(x in traeger for x in ("öffentlich", "staatlich")) else 1.0)

        # Number of foreign languages
        sprachen = row.get("sprachen") or row.get("fremdsprache") or ""
        if sprachen and str(sprachen).strip().lower() not in ("", "nan", "none"):
            langs = [x.strip() for x in str(sprachen).split(",") if x.strip()]
            if not langs:
                langs = [x.strip() for x in str(sprachen).split("|") if x.strip()]
            if langs:
                p.set("num_foreign_languages", float(len(langs)))

        # Compute student-teacher ratio if data available
        schueler = _safe_float(row.get("schueler_2024_25") or row.get("schueler_2023_24") or row.get("schueler_gesamt"))
        lehrer = _safe_float(row.get("lehrer_2024_25") or row.get("lehrer_2023_24"))
        if schueler and lehrer and lehrer > 0:
            p.set("student_teacher_ratio", schueler / lehrer)

        # --- GISD enrichment via PLZ ---
        plz_raw = row.get("plz", "")
        if not plz_raw or str(plz_raw).strip().lower() in ("", "nan", "none"):
            # Try to extract PLZ from adresse_ort (Hamburg: "22307 Hamburg")
            ort = row.get("adresse_ort", "")
            if ort:
                parts = str(ort).strip().split()
                if parts and parts[0].isdigit() and len(parts[0]) == 5:
                    plz_raw = parts[0]

        plz = str(plz_raw).strip().split(".")[0].zfill(5) if plz_raw else ""
        if plz and plz in gisd:
            g = gisd[plz]
            p.set("gisd_score", g["gisd_score"])
            p.set("gisd_quintile", float(g["gisd_quintile"]))
            gisd_hits += 1

        # --- Zensus catchment enrichment ---
        if zensus_df is not None:
            _enrich_with_zensus(p, zensus_df)
            if "catchment_population" in p.values:
                zensus_hits += 1

        profiles.append(p)

        # Store school info for display
        school_info[full_sid] = {
            "name": row.get("schulname", sid),
            "district": row.get("bezirk", row.get("stadtteil", "")),
            "school_type": row.get("school_type", row.get("schulart", "")),
            "city": city_label,
        }

        # Find Abitur grade (try columns in preference order)
        for abitur_col in ABITUR_COLUMNS:
            grade = _safe_float(row.get(abitur_col))
            if grade is not None and 1.0 <= grade <= 4.0:
                labeled[full_sid] = grade
                break

        # Find Abitur completion rate (erfolgsquote)
        for eq_col in ERFOLGSQUOTE_COLUMNS:
            eq = _safe_float(row.get(eq_col))
            if eq is not None and 0.0 <= eq <= 100.0:
                school_info[full_sid]["erfolgsquote"] = eq
                break

    n_dims = len(set().union(*(p.values.keys() for p in profiles))) if profiles else 0
    print(f"  [{city_label}] {len(profiles)} with coordinates, "
          f"{len(labeled)} with Abitur grades, {n_dims} dimensions populated")
    if use_gisd:
        print(f"  [{city_label}] GISD enriched: {gisd_hits}/{len(profiles)} schools")
    if use_zensus:
        print(f"  [{city_label}] Zensus catchment: {zensus_hits}/{len(profiles)} schools")

    return profiles, labeled, school_info


def load_all_data(
    city: str,
):
    """Load data for one or all cities."""
    all_profiles = []
    all_labeled = {}
    all_info = {}

    datasets = []

    if city in ("berlin", "all"):
        datasets.append((
            str(PROJECT_ROOT / "data_berlin" / "final" / "school_master_table_final.csv"),
            BERLIN_COLUMN_MAP,
            "berlin",
        ))

    if city in ("hamburg", "all"):
        datasets.append((
            str(PROJECT_ROOT / "data_hamburg" / "final" / "hamburg_school_master_table_final.csv"),
            HAMBURG_COLUMN_MAP,
            "hamburg",
        ))

    if city in ("nrw", "all"):
        datasets.append((
            str(PROJECT_ROOT / "data_nrw" / "final" / "nrw_secondary_school_master_table_final.csv"),
            NRW_COLUMN_MAP,
            "nrw",
        ))

    if city in ("frankfurt", "all"):
        datasets.append((
            str(PROJECT_ROOT / "data_frankfurt" / "final" / "frankfurt_secondary_school_master_table_final.csv"),
            FRANKFURT_COLUMN_MAP,
            "frankfurt",
        ))

    if city in ("munich", "all"):
        datasets.append((
            str(PROJECT_ROOT / "data_munich" / "final" / "munich_secondary_school_master_table_final.csv"),
            FRANKFURT_COLUMN_MAP,  # same column pattern as Frankfurt
            "munich",
        ))

    if city in ("stuttgart", "all"):
        datasets.append((
            str(PROJECT_ROOT / "data_stuttgart" / "final" / "stuttgart_secondary_school_master_table_final.csv"),
            FRANKFURT_COLUMN_MAP,  # same column pattern as Frankfurt
            "stuttgart",
        ))

    for path, col_map, label in datasets:
        if not os.path.exists(path):
            print(f"  [{label}] WARNING: {path} not found, skipping")
            continue
        profiles, labeled, info = load_city_data(path, col_map, label)
        all_profiles.extend(profiles)
        all_labeled.update(labeled)
        all_info.update(info)

    # Post-process: compute school competition density
    _compute_competition_density(all_profiles, all_info)

    # For schools missing gisd_quintile (e.g. Stuttgart with corrupted PLZ data),
    # impute a proxy from Zensus catchment features (rent, foreigner %, density)
    # calibrated on schools where both sources are available.
    _impute_gisd_from_zensus(all_profiles)

    # Regression-based imputation for features missing in some cities.
    # Uses population density (and GISD quintile) as auxiliary predictors,
    # trained on schools that have both the feature AND the auxiliary, then
    # applied to schools missing the feature.  This produces city-agnostic
    # imputations instead of injecting a single Berlin median.
    _regress_impute(all_profiles)

    return all_profiles, all_labeled, all_info


def _impute_gisd_from_zensus(profiles: list):
    """Impute gisd_quintile for schools that have Zensus catchment data but no GISD PLZ match.

    GISD is derived from the same census data underlying the Zensus grid. Where
    PLZ-based GISD lookup fails (e.g. Stuttgart with corrupted PLZ in the source data),
    we fit a local OLS: gisd_quintile ~ intercept + avg_rent + foreigner_pct + pop_density
    on schools with both sources, then apply to schools missing gisd_quintile.

    This is more principled than skipping GISD entirely, because:
    - Zensus avg_rent and foreigner_pct are the primary inputs to GISD
    - The fitted mapping is calibrated cross-city (using Berlin, Hamburg, NRW, etc.)
    - The result is stored as gisd_quintile so downstream imputation can use it
    """
    PREDICTORS = ["catchment_avg_rent", "catchment_foreigner_pct", "catchment_population_density"]

    vals = {k: np.array([p.get(k, np.nan) for p in profiles]) for k in PREDICTORS}
    gisd_q = np.array([p.get("gisd_quintile", np.nan) for p in profiles])

    # Schools with all predictors AND gisd_quintile
    complete = ~np.isnan(gisd_q)
    for k in PREDICTORS:
        complete &= ~np.isnan(vals[k])

    # Schools missing gisd_quintile but having all predictors
    missing = np.isnan(gisd_q)
    for k in PREDICTORS:
        missing &= ~np.isnan(vals[k])

    n_complete, n_missing = complete.sum(), missing.sum()
    if n_complete < 20 or n_missing == 0:
        return

    X_complete = np.column_stack([np.ones(n_complete)] + [vals[k][complete] for k in PREDICTORS])
    y_complete = gisd_q[complete]
    try:
        beta, _, _, _ = np.linalg.lstsq(X_complete, y_complete, rcond=None)
    except np.linalg.LinAlgError:
        return

    y_hat = X_complete @ beta
    r2 = float(1 - np.var(y_complete - y_hat) / (np.var(y_complete) + 1e-12))

    if r2 < 0.10:
        # Calibration too weak (likely homogeneous calibration cities); skip imputation.
        # Schools will fall back to mean-imputation in standardize_profiles().
        print(f"  GISD proxy: skipped (calibration R²={r2:.2f} < 0.10 — calibration set too homogeneous)")
        return

    X_missing = np.column_stack([np.ones(n_missing)] + [vals[k][missing] for k in PREDICTORS])
    y_pred = np.clip(X_missing @ beta, 1.0, 5.0)  # quintiles are 1–5

    missing_indices = np.where(missing)[0]
    for i, pred_q in zip(missing_indices, y_pred):
        profiles[i].set("gisd_quintile", float(round(pred_q, 2)))

    print(f"  GISD proxy (Zensus-derived): imputed gisd_quintile for {n_missing} schools "
          f"(calibrated on {n_complete} schools, R²={r2:.2f})")


def _regress_impute(profiles: list):
    """Fill in missing feature values using regression on available auxiliary predictors.

    For each feature with <100% coverage, fits a multivariate OLS:
        feature ~ intercept + β1*population_density + β2*gisd_quintile + β3*transit_accessibility
    on all schools that have the feature AND all predictors populated.  The fitted
    model is then applied to schools missing the feature.

    This avoids injecting a city-specific median into schools from other cities.
    Features with <10 complete cases are skipped (imputation not reliable).
    """
    AUX_KEYS = ["catchment_population_density", "gisd_quintile", "transit_accessibility"]

    # Features to attempt regression imputation (those that have partial coverage)
    IMPUTE_TARGETS = [
        "crime_index", "crime_violent", "crime_drug_offenses",
        "poi_school_count", "population_per_school",
    ]

    # Build auxiliary matrix (schools × aux features, raw values)
    n = len(profiles)
    aux_raw = {}
    for key in AUX_KEYS:
        aux_raw[key] = np.array([p.get(key, np.nan) for p in profiles])

    imputed_counts = {}
    for target in IMPUTE_TARGETS:
        target_vals = np.array([p.get(target, np.nan) for p in profiles])

        # Find schools with complete data (target + all aux)
        complete_mask = ~np.isnan(target_vals)
        for akey in AUX_KEYS:
            complete_mask &= ~np.isnan(aux_raw[akey])

        missing_mask = np.isnan(target_vals)
        # Only impute if aux is available for the school
        for akey in AUX_KEYS:
            missing_mask &= ~np.isnan(aux_raw[akey])

        n_complete = complete_mask.sum()
        n_missing = missing_mask.sum()

        if n_complete < 10 or n_missing == 0:
            continue

        # Fit OLS on complete cases
        X_complete = np.column_stack([np.ones(n_complete)] + [aux_raw[k][complete_mask] for k in AUX_KEYS])
        y_complete = target_vals[complete_mask]
        try:
            beta, _, _, _ = np.linalg.lstsq(X_complete, y_complete, rcond=None)
        except np.linalg.LinAlgError:
            continue

        # Predict for missing schools
        X_missing = np.column_stack([np.ones(n_missing)] + [aux_raw[k][missing_mask] for k in AUX_KEYS])
        y_imputed = X_missing @ beta

        # Write imputed values back into profiles
        missing_indices = np.where(missing_mask)[0]
        for i, pred_val in zip(missing_indices, y_imputed):
            profiles[i].set(target, float(pred_val))

        imputed_counts[target] = int(n_missing)

    if imputed_counts:
        print(f"  Regression imputation (density+GISD+transit): "
              + ", ".join(f"{k}={v}" for k, v in imputed_counts.items()))


def _compute_competition_density(profiles: list, school_info: dict, radius_m: float = 2000.0):
    """Compute same-type school density within 2x catchment radius.

    For each school:
      1. Count same school-type schools within radius_m
      2. population_per_school = catchment_pop / (same_type_count + 1)
      3. school_supply_ratio = (same_type_count + 1) / catchment_pop * 10000

    The hypothesis: more school supply per capita → less demand pressure
    → less overfill → tentatively better outcomes.
    """
    if not profiles:
        return

    # Build arrays for fast computation
    lats = np.array([p.latitude for p in profiles])
    lons = np.array([p.longitude for p in profiles])
    types = [school_info.get(p.school_id, {}).get("school_type", "unknown").lower() for p in profiles]

    # Normalize school types for matching
    def _normalize_type(t):
        t = t.lower().strip()
        # Group ISS variants together, Gymnasium together, Stadtteilschule with ISS
        if "gymnasium" in t:
            return "gymnasium"
        if any(x in t for x in ("sekundar", "iss", "stadtteil", "gesamtschule", "gemeinschafts")):
            return "secondary"
        return t

    norm_types = [_normalize_type(t) for t in types]

    # Approximate haversine distance using equirectangular projection (fast, good enough at city scale)
    lat_rad = np.radians(lats)
    cos_lat = np.cos(lat_rad)

    enriched = 0
    for i, p in enumerate(profiles):
        my_type = norm_types[i]

        # Distance in meters (equirectangular approximation)
        dlat = np.radians(lats - lats[i]) * 6371000
        dlon = np.radians(lons - lons[i]) * 6371000 * cos_lat[i]
        dist = np.sqrt(dlat**2 + dlon**2)

        # Same type within radius (excluding self)
        mask = (dist <= radius_m) & (dist > 0) & np.array([nt == my_type for nt in norm_types])
        same_type_count = int(mask.sum())

        p.set("same_type_schools_2km", float(same_type_count))

        # Nearest same-type school distance
        same_type_dists = dist[mask]
        if len(same_type_dists) > 0:
            p.set("nearest_same_type_m", float(same_type_dists.min()))

        # Competition density: divide by catchment population
        catchment_pop = p.get("catchment_population", 0)
        if catchment_pop > 0:
            total_schools = same_type_count + 1  # Include self
            p.set("population_per_school", catchment_pop / total_schools)
            p.set("school_supply_ratio", total_schools / catchment_pop * 10000)
            enriched += 1

    print(f"  Competition density: {enriched}/{len(profiles)} schools enriched "
          f"(2km radius, same school type)")


# ---------------------------------------------------------------------------
# Formatted output (adapted from run_diagnostics.py)
# ---------------------------------------------------------------------------

def print_header(title: str):
    width = 76
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def print_section(title: str):
    print()
    print(f"── {title} " + "─" * max(0, 72 - len(title)))


def print_diagnostics(diag: ModelDiagnostics, school_info: dict):
    """Print full model diagnostics."""

    print_header("MODEL PERFORMANCE DIAGNOSTICS")

    # 1. Overall fit
    print_section("1. Overall Model Fit")
    print(f"  R²              = {diag.r_squared:.4f}")
    print(f"  Adjusted R²     = {diag.adjusted_r_squared:.4f}")
    print(f"  RMSE            = {diag.rmse:.4f}")
    print(f"  MAE             = {diag.mae:.4f}")
    print(f"  N (labeled)     = {diag.n_samples}")
    print(f"  Features used   = {diag.n_features}")
    print(f"  Intercept       = {diag.intercept:.4f}")
    print()

    reliability = "✅ RELIABLE" if diag.is_reliable else "⚠️  USE WITH CAUTION"
    print(f"  Model status: {reliability}")
    if not diag.is_reliable:
        print(f"    (CV R² mean = {diag.cv_r_squared_mean:.3f}, "
              f"std = {diag.cv_r_squared_std:.3f})")

    # 2. Cross-validation
    print_section("2. Cross-Validation")
    print(f"  CV R² mean      = {diag.cv_r_squared_mean:.4f}")
    print(f"  CV R² std       = {diag.cv_r_squared_std:.4f}")
    print(f"  CV RMSE mean    = {diag.cv_rmse_mean:.4f}")
    print()
    print(f"  {'Fold':>4}  {'Train':>5}  {'Test':>4}  {'R²':>8}  {'RMSE':>8}  {'MAE':>8}")
    print(f"  {'----':>4}  {'-----':>5}  {'----':>4}  {'--------':>8}  {'--------':>8}  {'--------':>8}")
    for f in diag.cv_folds:
        print(f"  {f.fold:4d}  {f.train_size:5d}  {f.test_size:4d}  "
              f"{f.r_squared:8.4f}  {f.rmse:8.4f}  {f.mae:8.4f}")

    # 3. Feature ranking (Ridge uses all features, ranked by importance)
    print_section("3. Feature Ranking (Ridge — all features included)")
    print()
    print("  Ridge regression uses ALL features with L2 regularization (alpha=50).")
    print("  No feature selection is needed — regularization handles overfitting.")
    print("  Features ranked by absolute standardized coefficient:")
    print()
    print(f"  {'Rank':>4}  {'Feature':<35} {'Std Coef':>9}")
    print(f"  {'----':>4}  {'-'*35} {'-'*9}")
    for step in diag.feature_selection_path:
        rank = step["step"]
        label = step.get("feature_label", step["feature"])
        std_coef = step.get("std_coef", 0.0)
        bar = "#" * int(abs(std_coef) * 50) if abs(std_coef) > 0.01 else ""
        print(f"  {rank:4d}  {label:<35} {std_coef:+.4f}  {bar}")

    # 4. Feature importance
    print_section("4. Feature Importance (Selected Predictors)")
    print()
    print(f"  {'Feature':<30} {'Std Coef':>9} {'Dir':>10} {'Partial R²':>11} {'p-value':>9}")
    print(f"  {'-'*30} {'-'*9} {'-'*10} {'-'*11} {'-'*9}")
    for f in diag.features:
        pval_str = f"{f.p_value:.4f}" if f.p_value is not None else "    n/a"
        sig = ""
        if f.p_value is not None:
            if f.p_value < 0.001:
                sig = " ***"
            elif f.p_value < 0.01:
                sig = " **"
            elif f.p_value < 0.05:
                sig = " *"
        print(f"  {f.label:<30} {f.standardized_coef:>+9.4f} {f.direction:>10} "
              f"{f.partial_r_squared:>11.4f} {pval_str}{sig}")
    print()
    print("  Significance: *** p<0.001  ** p<0.01  * p<0.05")

    # 5. Interpretation
    print_section("5. Interpretation")
    print()
    print("  Note: German Abitur grading: 1.0 = best, 4.0 = worst.")
    print("  A positive std. coef means the feature is associated with WORSE grades.")
    print()
    for f in diag.features:
        target_effect = "worse" if f.standardized_coef > 0 else "better"
        print(f"  {f.label}: 1 SD increase → {abs(f.standardized_coef):.3f} SD "
              f"{target_effect} Abitur grade")

    # 6. Labeled schools: actual vs predicted (sorted by actual)
    print_section("6. Labeled Schools: Actual vs Predicted")
    print()
    labeled_preds = [p for p in diag.predictions if p.actual is not None]
    labeled_preds.sort(key=lambda p: p.actual)

    print(f"  {'School':<35} {'Actual':>7} {'Pred':>7} {'Resid':>7} {'Conf':>6}")
    print(f"  {'-'*35} {'-'*7} {'-'*7} {'-'*7} {'-'*6}")
    for p in labeled_preds[:30]:  # Top 30
        info = school_info.get(p.school_id, {})
        name = info.get("name", p.school_id)[:35]
        print(f"  {name:<35} {p.actual:>7.2f} {p.predicted:>7.3f} "
              f"{p.residual:>+7.3f} {p.confidence:>6.2f}")
    if len(labeled_preds) > 30:
        print(f"  ... and {len(labeled_preds) - 30} more")

    # 7. Feature contributions for top/bottom 3
    print_section("7. Feature Contributions (Best & Worst 3)")
    print()
    print(f"  Shows how each feature moves the prediction from the intercept ({diag.intercept:.3f})")

    showcase = labeled_preds[:3] + labeled_preds[-3:]
    for p in showcase:
        info = school_info.get(p.school_id, {})
        name = info.get("name", p.school_id)
        district = info.get("district", "")
        print(f"\n  {name} ({district}) — actual={p.actual:.2f}, predicted={p.predicted:.3f}:")

        contribs = sorted(p.feature_contributions.items(), key=lambda x: abs(x[1]), reverse=True)
        # Show top 8 contributors (with 29 features, showing all is too verbose)
        for key, val in contribs[:8]:
            dim = DIMENSIONS.get(key)
            label = dim.label if dim else key
            bar_len = int(abs(val) * 5)
            bar = ("+" if val > 0 else "-") * max(1, bar_len)
            print(f"    {label:<30} {val:>+8.3f}  {bar}")
        remaining = len(contribs) - 8
        if remaining > 0:
            remaining_sum = sum(v for _, v in contribs[8:])
            print(f"    {'('+str(remaining)+' more features)':<30} {remaining_sum:>+8.3f}")

    # 8. Unlabeled predictions
    unlabeled_preds = [p for p in diag.predictions if p.actual is None]
    if unlabeled_preds:
        print_section("8. Unlabeled Schools: Estimated Performance")
        print()
        unlabeled_preds.sort(key=lambda p: p.predicted)

        print(f"  {'School':<35} {'Pred':>7} {'Conf':>6}  {'City'}")
        print(f"  {'-'*35} {'-'*7} {'-'*6}  {'-'*8}")
        for p in unlabeled_preds[:20]:
            info = school_info.get(p.school_id, {})
            name = info.get("name", p.school_id)[:35]
            city = info.get("city", "")
            print(f"  {name:<35} {p.predicted:>7.3f} {p.confidence:>6.2f}  {city}")
        if len(unlabeled_preds) > 20:
            print(f"  ... and {len(unlabeled_preds) - 20} more")
        print()
        print("  Confidence: distance from training distribution (1.0 = within, 0.0 = far)")

    # 9. Model equation (top 10 features for readability)
    print_section("9. Model Equation (top 10 features)")
    print()
    print("  Ridge regression with alpha=50, all 29 features.")
    print("  Showing top 10 by importance (full model uses all):")
    print()
    terms = [f"{diag.intercept:.4f}"]
    for f in diag.features[:10]:
        sign = "+" if diag.coefficients[f.key] >= 0 else "-"
        terms.append(f"{sign} {abs(diag.coefficients[f.key]):.6f} × {f.key}")
    equation = f"  predicted_grade = {terms[0]}"
    for t in terms[1:]:
        equation += f"\n                    {t}"
    equation += f"\n                    + ({len(diag.features) - 10} more terms)"
    print(equation)
    print()


def export_predictions(diag: ModelDiagnostics, school_info: dict, path: str):
    """Export predictions + contributions to CSV."""
    feature_keys = [f.key for f in diag.features]
    fieldnames = [
        "school_id", "name", "district", "city", "school_type",
        "actual", "predicted", "residual", "confidence",
    ] + [f"contrib_{k}" for k in feature_keys]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for p in sorted(diag.predictions, key=lambda x: x.predicted):
            info = school_info.get(p.school_id, {})
            row = {
                "school_id": p.school_id,
                "name": info.get("name", ""),
                "district": info.get("district", ""),
                "city": info.get("city", ""),
                "school_type": info.get("school_type", ""),
                "actual": p.actual if p.actual is not None else "",
                "predicted": p.predicted,
                "residual": p.residual if p.residual is not None else "",
                "confidence": p.confidence,
            }
            for k in feature_keys:
                row[f"contrib_{k}"] = round(p.feature_contributions.get(k, 0), 4)
            writer.writerow(row)

    print(f"\n  Predictions exported to: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Train school performance regression on real enriched data"
    )
    parser.add_argument(
        "--city", type=str, default="berlin",
        choices=["berlin", "hamburg", "nrw", "all"],
        help="Which city's data to use (default: berlin)"
    )
    parser.add_argument(
        "--export", type=str, default=None,
        help="Export predictions to CSV file"
    )
    parser.add_argument(
        "--cv-folds", type=int, default=5,
        help="Number of cross-validation folds (default: 5)"
    )
    parser.add_argument(
        "--features", type=str, nargs="*", default=None,
        help="Force specific feature keys (default: auto-select)"
    )
    parser.add_argument(
        "--min-improvement", type=float, default=0.01,
        help="Minimum R² improvement for feature selection (default: 0.01)"
    )
    args = parser.parse_args()

    print(f"Loading enriched school data for: {args.city}")
    print()
    profiles, labeled, school_info = load_all_data(args.city)

    if not profiles:
        print("ERROR: No school data loaded. Check data paths.")
        sys.exit(1)

    if len(labeled) < 5:
        print(f"ERROR: Only {len(labeled)} schools with Abitur grades — need >= 5.")
        sys.exit(1)

    # Show dimension coverage
    all_dims = set()
    for p in profiles:
        all_dims.update(p.values.keys())
    print(f"\nTotal: {len(profiles)} schools, {len(labeled)} with Abitur grades")
    print(f"Dimensions populated: {sorted(all_dims)}")

    # Train
    print(f"\nTraining regression model...")
    print(f"  Labeled schools: {len(labeled)}")
    print(f"  Total schools:   {len(profiles)}")
    print(f"  CV folds:        {args.cv_folds}")
    if args.features:
        print(f"  Forced features: {args.features}")

    diag = train_and_diagnose(
        profiles=profiles,
        labeled=labeled,
        feature_keys=args.features,
        cv_folds=args.cv_folds,
    )

    if diag is None:
        print("\nModel training failed. Check data quality and feature availability.")
        sys.exit(1)

    print_diagnostics(diag, school_info)

    if args.export:
        export_predictions(diag, school_info, args.export)


if __name__ == "__main__":
    main()
