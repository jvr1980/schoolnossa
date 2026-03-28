"""Apply Ridge regression predictions to city parquet/CSV files.

Trains two Ridge models:
  Model A: Predicts average Abitur grade (trained on Berlin+Hamburg labeled schools)
  Model B: Predicts Abitur completion rate (trained on Berlin schools with erfolgsquote)

Writes prediction columns back into each city's final parquet and CSV files,
including 80% prediction intervals and per-school feature contribution drivers.

Usage:
    python scripts_shared/regression/apply_predictions_to_parquets.py
"""

import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from scripts_shared.regression.run_regression import load_all_data
from scripts_shared.regression.scorer import (
    DEFAULT_RIDGE_ALPHA,
    _fit_ridge,
    _predict,
    _r_squared,
    _rmse,
    compute_prediction_intervals,
    standardize_profiles,
)
from scripts_shared.regression.dimensions import DIMENSIONS

# School types that can lead to Abitur
ABITUR_ELIGIBLE_TYPES = {
    "Gymnasium", "ISS", "ISS-Gymnasium",            # Berlin
    "Stadtteilschule", "Stadtteilschule_Gymnasium",  # Hamburg
    "Gesamtschule", "Waldorfschule",                 # NRW
    "Internationale Schule",                          # NRW
}

# City-specific dims to exclude (not portable)
CITY_SPECIFIC_DIMS = {
    "nachfrage_prozent", "belastungsstufe", "migration_pct_school",
    "sozialindex", "traffic_cars_per_hour",
    "traffic_bikes_per_hour", "traffic_speed_v85",
}

# Features excluded from cross-city prediction due to incomparable scales
# or near-zero variance in the training set:
#   gisd_score       — all Berlin schools = 0.5, Hamburg = 0.3 (no within-city variance;
#                      std≈0 causes division-by-~0 in standardization)
#   crime_safety_rank — Berlin uses 1–12 district ranks, Hamburg 1–198 area ranks
#                      (different geographic unit → not comparable)
#   transit_stop_count — Berlin mean=16, NRW mean=70 (different counting methodology)
NON_PORTABLE_DIMS = {"gisd_score", "crime_safety_rank", "transit_stop_count"}

# Files to update
CITY_FILES = [
    {
        "city_prefix": "berlin",
        "parquet": "data_berlin/final/school_master_table_final_with_embeddings.parquet",
        "csv": "data_berlin/final/school_master_table_final.csv",
    },
    {
        "city_prefix": "hamburg",
        "parquet": "data_hamburg/final/hamburg_school_master_table_final_with_embeddings.parquet",
        "csv": "data_hamburg/final/hamburg_school_master_table_final.csv",
    },
    {
        "city_prefix": "nrw",
        "parquet": "data_nrw/final/nrw_secondary_school_master_table_final_with_embeddings.parquet",
        "csv": "data_nrw/final/nrw_secondary_school_master_table_final.csv",
    },
]

NEW_COLUMNS = [
    "abitur_durchschnitt_estimated",
    "abitur_durchschnitt_estimated_lower",
    "abitur_durchschnitt_estimated_upper",
    "abitur_erfolgsquote_estimated",
    "abitur_erfolgsquote_estimated_lower",
    "abitur_erfolgsquote_estimated_upper",
    "abitur_prediction_confidence",
    "abitur_prediction_drivers",
]


def _train_ridge_model(profiles, labeled, alpha=DEFAULT_RIDGE_ALPHA, fit_on_all=False):
    """Train a Ridge model and return everything needed for predictions.

    Args:
        fit_on_all: If True, compute z-score mean/std from ALL profiles (not just labeled).
                    Use this when labeled schools cover only one city (e.g. erfolgsquote
                    labels are Berlin-only) so that Hamburg/NRW schools don't get extreme
                    z-scores relative to a Berlin-only standardization distribution.

    Returns dict with: beta, feature_keys, X_train, y_train, X_all, school_ids,
                       stats (standardization params), mse
    """
    labeled_ids = set(labeled.keys())
    X_dict, stats, school_ids = standardize_profiles(profiles, labeled_ids, fit_on_all=fit_on_all)
    if not X_dict:
        return None

    labeled_mask = np.array([sid in labeled for sid in school_ids])
    y_labeled = np.array([labeled.get(sid, np.nan) for sid in school_ids])
    y_train = y_labeled[labeled_mask]

    feature_keys = sorted(k for k, arr in X_dict.items()
                         if np.std(arr[labeled_mask]) > 1e-6)

    X_all = np.column_stack([X_dict[k] for k in feature_keys])
    X_train = X_all[labeled_mask]

    beta = _fit_ridge(X_train, y_train, alpha)
    if beta is None:
        return None

    y_pred_train = _predict(X_train, beta)
    y_pred_all = _predict(X_all, beta)
    mse = np.sum((y_train - y_pred_train) ** 2) / max(len(y_train) - len(feature_keys), 1)

    # Confidence scores
    labeled_mean = np.mean(X_train, axis=0)
    labeled_std = np.std(X_train, axis=0) + 1e-6
    confidences = []
    for i in range(len(school_ids)):
        z_scores = np.abs((X_all[i] - labeled_mean) / labeled_std)
        avg_z = float(np.mean(z_scores))
        confidences.append(max(0.0, min(1.0, 1.0 - avg_z / 5.0)))

    # Feature contributions per school
    contributions = []
    for i in range(len(school_ids)):
        c = {}
        for j, key in enumerate(feature_keys):
            c[key] = float(beta[j + 1] * X_all[i, j])
        contributions.append(c)

    return {
        "beta": beta,
        "feature_keys": feature_keys,
        "X_train": X_train,
        "y_train": y_train,
        "X_all": X_all,
        "y_pred_all": y_pred_all,
        "school_ids": school_ids,
        "confidences": confidences,
        "contributions": contributions,
        "alpha": alpha,
        "r2": _r_squared(y_train, y_pred_train),
        "rmse": _rmse(y_train, y_pred_train),
        "n_labeled": len(y_train),
    }


def _build_drivers_json(contributions, top_n=8):
    """Build JSON string of top feature contributions for a school."""
    sorted_contribs = sorted(contributions.items(), key=lambda x: abs(x[1]), reverse=True)
    top = {}
    for key, val in sorted_contribs[:top_n]:
        dim = DIMENSIONS.get(key)
        label = dim.label if dim else key
        top[label] = round(val, 4)
    return json.dumps(top, ensure_ascii=False)


def main():
    print("=" * 70)
    print("  APPLYING RIDGE PREDICTIONS TO CITY PARQUETS")
    print("=" * 70)
    print()

    # --- Step 1: Load all data ---
    print("Step 1: Loading all city data...")
    profiles, labeled_grade, info = load_all_data("all")

    # Remove city-specific and non-portable dims from all profiles
    for p in profiles:
        for key in CITY_SPECIFIC_DIMS | NON_PORTABLE_DIMS:
            p.values.pop(key, None)

    # Build erfolgsquote labels from school_info
    labeled_erfolgsquote = {}
    for sid, meta in info.items():
        eq = meta.get("erfolgsquote")
        if eq is not None:
            labeled_erfolgsquote[sid] = eq

    print(f"\n  Abitur grade labels: {len(labeled_grade)} schools")
    print(f"  Erfolgsquote labels: {len(labeled_erfolgsquote)} schools")

    # --- Step 2: Train Model A (Abitur grade) ---
    # Labeled schools span Berlin + Hamburg → labeled-only standardization is fine
    print("\nStep 2: Training Model A (Abitur grade)...")
    model_a = _train_ridge_model(profiles, labeled_grade, fit_on_all=False)
    if model_a is None:
        print("  ERROR: Failed to train grade model")
        return
    print(f"  Grade model: R²={model_a['r2']:.4f}, RMSE={model_a['rmse']:.4f}, "
          f"n={model_a['n_labeled']}, features={len(model_a['feature_keys'])}")

    # Prediction intervals for grade model
    lower_a, upper_a = compute_prediction_intervals(
        model_a["X_train"], model_a["y_train"],
        model_a["X_all"], model_a["y_pred_all"],
        model_a["alpha"], ci_level=0.80,
    )

    # --- Step 3: Train Model B (completion rate) ---
    # Labeled schools are Berlin-only → use fit_on_all=True so that Hamburg/NRW
    # schools are standardized against the full cross-city distribution, not just
    # the 88 Berlin Gymnasien.  Without this, gisd_quintile alone causes z ≈ -15
    # for Hamburg (Berlin Gymnasien cluster at quintile 4, std=0.2; Hamburg=1).
    print("\nStep 3: Training Model B (Abitur completion rate)...")
    model_b = _train_ridge_model(profiles, labeled_erfolgsquote, fit_on_all=True)
    if model_b is None:
        print("  WARNING: Failed to train completion rate model — skipping")
    else:
        print(f"  Completion model: R²={model_b['r2']:.4f}, RMSE={model_b['rmse']:.4f}, "
              f"n={model_b['n_labeled']}, features={len(model_b['feature_keys'])}")
        lower_b, upper_b = compute_prediction_intervals(
            model_b["X_train"], model_b["y_train"],
            model_b["X_all"], model_b["y_pred_all"],
            model_b["alpha"], ci_level=0.80,
        )

    # --- Step 4: Build prediction lookup ---
    print("\nStep 4: Building prediction lookup...")
    # Index: school_id → predictions
    pred_lookup = {}
    for i, sid in enumerate(model_a["school_ids"]):
        entry = {
            "abitur_durchschnitt_estimated": round(float(model_a["y_pred_all"][i]), 3),
            "abitur_durchschnitt_estimated_lower": round(float(lower_a[i]), 3),
            "abitur_durchschnitt_estimated_upper": round(float(upper_a[i]), 3),
            "abitur_prediction_confidence": round(model_a["confidences"][i], 3),
            "abitur_prediction_drivers": _build_drivers_json(model_a["contributions"][i]),
        }
        if model_b is not None:
            idx_b = model_b["school_ids"].index(sid) if sid in model_b["school_ids"] else None
            if idx_b is not None:
                pred_eq = float(model_b["y_pred_all"][idx_b])
                entry["abitur_erfolgsquote_estimated"] = round(np.clip(pred_eq, 0, 100), 1)
                entry["abitur_erfolgsquote_estimated_lower"] = round(np.clip(float(lower_b[idx_b]), 0, 100), 1)
                entry["abitur_erfolgsquote_estimated_upper"] = round(np.clip(float(upper_b[idx_b]), 0, 100), 1)
        pred_lookup[sid] = entry

    print(f"  {len(pred_lookup)} school predictions ready")

    # --- Step 5: Write to parquets and CSVs ---
    print("\nStep 5: Writing predictions to city files...")

    for city_config in CITY_FILES:
        prefix = city_config["city_prefix"]
        parquet_path = city_config["parquet"]
        csv_path = city_config["csv"]

        if not os.path.exists(csv_path):
            print(f"\n  [{prefix}] CSV not found: {csv_path} — skipping")
            continue

        # Load CSV (always exists) and optionally parquet
        df_csv = pd.read_csv(csv_path)
        df_parquet = pd.read_parquet(parquet_path) if os.path.exists(parquet_path) else None

        # Initialize new columns as NaN
        for col in NEW_COLUMNS:
            dtype = "object" if col == "abitur_prediction_drivers" else float
            df_csv[col] = pd.Series(dtype=dtype)
            if df_parquet is not None:
                df_parquet[col] = pd.Series(dtype=dtype)

        matched = 0
        eligible = 0
        for idx, row in df_csv.iterrows():
            schulnummer = str(row.get("schulnummer", "")).strip()
            school_type = str(row.get("school_type", ""))
            full_sid = f"{prefix}_{schulnummer}"

            # Only write predictions for Abitur-eligible school types
            if school_type not in ABITUR_ELIGIBLE_TYPES:
                continue
            eligible += 1

            if full_sid not in pred_lookup:
                continue
            matched += 1

            preds = pred_lookup[full_sid]
            for col in NEW_COLUMNS:
                val = preds.get(col, np.nan)
                df_csv.at[idx, col] = val

                # Also update parquet if it exists and has matching row
                if df_parquet is not None and idx < len(df_parquet):
                    df_parquet.at[idx, col] = val

        # Save
        df_csv.to_csv(csv_path, index=False)
        print(f"\n  [{prefix}] CSV updated: {csv_path}")
        print(f"    {eligible} eligible schools, {matched} matched with predictions")

        if df_parquet is not None:
            df_parquet.to_parquet(parquet_path, index=False)
            print(f"  [{prefix}] Parquet updated: {parquet_path}")

        # Quick validation
        has_pred = df_csv["abitur_durchschnitt_estimated"].notna().sum()
        has_actual = df_csv[[c for c in df_csv.columns
                            if c.startswith("abitur_durchschnitt_20")]].notna().any(axis=1).sum()
        ci_width = (df_csv["abitur_durchschnitt_estimated_upper"] -
                   df_csv["abitur_durchschnitt_estimated_lower"]).dropna()
        print(f"    Predictions written: {has_pred}")
        print(f"    Schools with actual abitur: {has_actual}")
        if len(ci_width) > 0:
            print(f"    80% CI width: mean={ci_width.mean():.3f}, "
                  f"range=[{ci_width.min():.3f}, {ci_width.max():.3f}]")

        # Spot-check: compare predictions vs actuals where both exist
        both = df_csv[df_csv["abitur_durchschnitt_estimated"].notna() &
                     df_csv["abitur_durchschnitt_2024"].notna()].copy()
        if len(both) > 0:
            resids = both["abitur_durchschnitt_2024"] - both["abitur_durchschnitt_estimated"]
            print(f"    Pred vs actual ({len(both)} schools): "
                  f"MAE={resids.abs().mean():.3f}, "
                  f"mean residual={resids.mean():+.3f}")

    print("\n" + "=" * 70)
    print("  DONE — All city files updated with predictions")
    print("=" * 70)


if __name__ == "__main__":
    main()
