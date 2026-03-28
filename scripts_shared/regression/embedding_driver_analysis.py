"""Vector element driver analysis for school description embeddings.

Tests whether individual raw embedding dimensions (out of 3072) add predictive
power for Abitur grades beyond the existing structured features.

Methodology (leakage-free):
  1. Load labeled schools + their embeddings from parquets (Berlin + Hamburg)
  2. Load structured features via the normal regression pipeline
  3. For each CV fold:
     a. On the TRAINING split only: rank all 3072 dims by |Pearson r| with label
     b. Select top-K dims
     c. Train Ridge on (structured + selected embedding dims)
     d. Evaluate on TEST split
  4. Repeat for K ∈ {0, 5, 10, 20, 50, 100} — K=0 is the structured-only baseline
  5. Report CV R² per K; identify dims that are consistently selected across folds

Usage:
    python scripts_shared/regression/embedding_driver_analysis.py
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.regression.run_regression import load_all_data
from scripts_shared.regression.scorer import (
    DEFAULT_RIDGE_ALPHA,
    _fit_ridge,
    _predict,
    _r_squared,
    _rmse,
    standardize_profiles,
)

# City-specific dims excluded from portable model (same as apply_predictions_to_parquets.py)
CITY_SPECIFIC_DIMS = {
    "nachfrage_prozent", "belastungsstufe", "migration_pct_school",
    "sozialindex", "traffic_cars_per_hour",
    "traffic_bikes_per_hour", "traffic_speed_v85",
}

PARQUET_FILES = {
    "berlin": PROJECT_ROOT / "data_berlin/final/school_master_table_final_with_embeddings.parquet",
    "hamburg": PROJECT_ROOT / "data_hamburg/final/hamburg_school_master_table_final_with_embeddings.parquet",
}

K_VALUES = [0, 5, 10, 20, 50, 100]
CV_FOLDS = 5
ALPHA = DEFAULT_RIDGE_ALPHA


# ---------------------------------------------------------------------------
# Load embeddings from parquet files
# ---------------------------------------------------------------------------

def load_embeddings() -> dict:
    """Return {city_schulnummer: np.array(3072)} for schools that have embeddings."""
    embeddings = {}
    for city_prefix, path in PARQUET_FILES.items():
        if not path.exists():
            print(f"  WARNING: {path} not found — skipping {city_prefix}")
            continue
        df = pd.read_parquet(path)
        if "embedding" not in df.columns:
            print(f"  WARNING: no 'embedding' column in {path.name}")
            continue
        if "schulnummer" not in df.columns:
            print(f"  WARNING: no 'schulnummer' column in {path.name}")
            continue
        n_before = len(df)
        df = df.dropna(subset=["embedding", "schulnummer"])
        n_after = len(df)
        print(f"  [{city_prefix}] {n_after}/{n_before} rows have embeddings")
        for _, row in df.iterrows():
            sid = f"{city_prefix}_{str(row['schulnummer']).strip()}"
            emb = row["embedding"]
            if hasattr(emb, "__len__") and len(emb) == 3072:
                embeddings[sid] = np.array(emb, dtype=np.float32)
    print(f"  Total embeddings loaded: {len(embeddings)}")
    return embeddings


# ---------------------------------------------------------------------------
# Build structured feature matrix
# ---------------------------------------------------------------------------

def build_structured_matrix(profiles, labeled_ids):
    """Returns (X_struct, feature_keys, school_ids, labeled_mask)."""
    X_dict, stats, school_ids = standardize_profiles(profiles, labeled_ids)
    if not X_dict:
        return None, None, None, None

    labeled_mask = np.array([sid in labeled_ids for sid in school_ids])

    # Select features with variance in labeled subset
    feature_keys = sorted(
        k for k, arr in X_dict.items()
        if np.std(arr[labeled_mask]) > 1e-6
    )
    if not feature_keys:
        return None, None, None, None

    X_struct = np.column_stack([X_dict[k] for k in feature_keys])
    return X_struct, feature_keys, school_ids, labeled_mask


# ---------------------------------------------------------------------------
# Cross-validation with fold-internal dim selection
# ---------------------------------------------------------------------------

def cv_with_embedding_selection(
    X_struct: np.ndarray,
    E: np.ndarray,
    y: np.ndarray,
    k: int,
    top_k_dims: int,
    alpha: float = ALPHA,
) -> list:
    """K-fold CV with embedding dim selection done inside each training fold.

    Args:
        X_struct: (n, p_struct) structured features matrix (labeled schools only)
        E: (n, 3072) embedding matrix aligned to X_struct rows
        y: (n,) labels
        k: number of CV folds
        top_k_dims: how many embedding dims to select (0 = structured-only baseline)
        alpha: Ridge alpha

    Returns:
        list of fold R² values
    """
    n = len(y)
    indices = np.arange(n)
    rng = np.random.default_rng(42)
    rng.shuffle(indices)
    fold_sizes = np.full(k, n // k, dtype=int)
    fold_sizes[:n % k] += 1

    r2s = []
    current = 0
    for fold_i in range(k):
        start, stop = current, current + fold_sizes[fold_i]
        test_idx = indices[start:stop]
        train_idx = np.concatenate([indices[:start], indices[stop:]])
        current = stop

        X_struct_tr = X_struct[train_idx]
        X_struct_te = X_struct[test_idx]
        y_tr = y[train_idx]
        y_te = y[test_idx]

        if top_k_dims == 0:
            # Baseline: structured features only
            X_tr = X_struct_tr
            X_te = X_struct_te
        else:
            # Select top-K embedding dims by |correlation| on TRAINING set only
            E_tr = E[train_idx]
            E_te = E[test_idx]

            # Compute Pearson r for each of 3072 dims (on training fold)
            y_tr_c = y_tr - np.mean(y_tr)
            E_tr_c = E_tr - np.mean(E_tr, axis=0)
            numerators = E_tr_c.T @ y_tr_c  # shape (3072,)
            denom_E = np.sqrt(np.sum(E_tr_c ** 2, axis=0) + 1e-12)
            denom_y = np.sqrt(np.sum(y_tr_c ** 2) + 1e-12)
            correlations = numerators / (denom_E * denom_y)  # shape (3072,)

            top_dims = np.argsort(np.abs(correlations))[-top_k_dims:]

            # Standardize selected dims (fit on training fold)
            E_tr_sel = E_tr[:, top_dims]
            E_te_sel = E_te[:, top_dims]
            emb_mean = np.mean(E_tr_sel, axis=0)
            emb_std = np.std(E_tr_sel, axis=0) + 1e-9
            E_tr_z = (E_tr_sel - emb_mean) / emb_std
            E_te_z = (E_te_sel - emb_mean) / emb_std

            X_tr = np.column_stack([X_struct_tr, E_tr_z])
            X_te = np.column_stack([X_struct_te, E_te_z])

        beta = _fit_ridge(X_tr, y_tr, alpha)
        if beta is None:
            continue

        y_pred = _predict(X_te, beta)
        r2 = _r_squared(y_te, y_pred) if len(y_te) > 1 else 0.0
        r2s.append(r2)

    return r2s


# ---------------------------------------------------------------------------
# Identify consistently selected dimensions (cross-fold stability)
# ---------------------------------------------------------------------------

def find_stable_dims(
    X_struct: np.ndarray,
    E: np.ndarray,
    y: np.ndarray,
    k: int = 5,
    top_k_dims: int = 20,
) -> list:
    """Find embedding dims that are consistently top-K predictive across folds.

    Returns list of (dim_index, n_folds_selected, mean_abs_correlation, mean_correlation)
    sorted by n_folds_selected desc, then mean_abs_correlation desc.
    """
    n = len(y)
    indices = np.arange(n)
    rng = np.random.default_rng(42)
    rng.shuffle(indices)
    fold_sizes = np.full(k, n // k, dtype=int)
    fold_sizes[:n % k] += 1

    selection_counts = np.zeros(3072, dtype=int)
    correlation_sums = np.zeros(3072, dtype=float)

    current = 0
    for fold_i in range(k):
        start, stop = current, current + fold_sizes[fold_i]
        test_idx = indices[start:stop]
        train_idx = np.concatenate([indices[:start], indices[stop:]])
        current = stop

        E_tr = E[train_idx]
        y_tr = y[train_idx]

        y_tr_c = y_tr - np.mean(y_tr)
        E_tr_c = E_tr - np.mean(E_tr, axis=0)
        numerators = E_tr_c.T @ y_tr_c
        denom_E = np.sqrt(np.sum(E_tr_c ** 2, axis=0) + 1e-12)
        denom_y = np.sqrt(np.sum(y_tr_c ** 2) + 1e-12)
        correlations = numerators / (denom_E * denom_y)

        top_dims = np.argsort(np.abs(correlations))[-top_k_dims:]
        selection_counts[top_dims] += 1
        correlation_sums += correlations

    # Return dims selected in at least 2 folds
    stable = []
    for dim_idx in range(3072):
        if selection_counts[dim_idx] >= 2:
            stable.append((
                dim_idx,
                selection_counts[dim_idx],
                float(np.abs(correlation_sums[dim_idx]) / k),
                float(correlation_sums[dim_idx] / k),
            ))

    stable.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return stable


# ---------------------------------------------------------------------------
# Full-data correlation analysis (for visualization only — not used for model)
# ---------------------------------------------------------------------------

def full_correlation_analysis(E: np.ndarray, y: np.ndarray, top_n: int = 30) -> list:
    """Compute |Pearson r| between each embedding dim and labels on ALL labeled data.

    WARNING: This is purely informational — never use these correlations for
    feature selection in a model (that's leakage). Only use for exploratory analysis.
    """
    y_c = y - np.mean(y)
    E_c = E - np.mean(E, axis=0)
    numerators = E_c.T @ y_c
    denom_E = np.sqrt(np.sum(E_c ** 2, axis=0) + 1e-12)
    denom_y = np.sqrt(np.sum(y_c ** 2) + 1e-12)
    correlations = numerators / (denom_E * denom_y)
    top_dims = np.argsort(np.abs(correlations))[-top_n:][::-1]
    return [(int(d), float(correlations[d])) for d in top_dims]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  VECTOR ELEMENT DRIVER ANALYSIS — EMBEDDING DIMENSIONS")
    print("=" * 70)
    print()

    # Step 1: Load structured data
    print("Step 1: Loading structured features via regression pipeline...")
    profiles, labeled_grade, info = load_all_data("all")
    # Remove city-specific dims (same as apply_predictions_to_parquets.py)
    for p in profiles:
        for key in CITY_SPECIFIC_DIMS:
            p.values.pop(key, None)

    print(f"  Labeled schools (Abitur grade): {len(labeled_grade)}")

    # Step 2: Load embeddings
    print("\nStep 2: Loading school description embeddings...")
    embeddings = load_embeddings()

    # Step 3: Find intersection — schools with both structured features AND embeddings
    labeled_ids = set(labeled_grade.keys())
    X_struct, feature_keys, school_ids, labeled_mask = build_structured_matrix(profiles, labeled_ids)
    if X_struct is None:
        print("ERROR: Could not build structured feature matrix")
        return

    school_ids_labeled = [sid for sid, is_labeled in zip(school_ids, labeled_mask) if is_labeled]
    y_all = np.array([labeled_grade[sid] for sid in school_ids_labeled])

    # Filter to schools that also have embeddings
    has_embedding = [sid in embeddings for sid in school_ids_labeled]
    embed_mask = np.array(has_embedding)
    school_ids_with_emb = [sid for sid, has in zip(school_ids_labeled, has_embedding) if has]
    y_with_emb = y_all[embed_mask]
    X_struct_with_emb = X_struct[labeled_mask][embed_mask]

    print(f"\n  Schools with structured features (labeled): {len(school_ids_labeled)}")
    print(f"  Schools with embeddings + structured + label: {len(school_ids_with_emb)}")

    if len(school_ids_with_emb) < 20:
        print("\nERROR: Fewer than 20 schools have all three (features + embeddings + label).")
        print("       Cannot run reliable CV analysis.")
        return

    # Build embedding matrix aligned to school_ids_with_emb
    E = np.array([embeddings[sid] for sid in school_ids_with_emb], dtype=np.float64)
    print(f"  Embedding matrix: {E.shape}")
    print(f"  Structured feature matrix: {X_struct_with_emb.shape}")
    print(f"  Features: {feature_keys}")

    n_struct = X_struct_with_emb.shape[1]
    n_emb_dims = E.shape[1]
    n_labeled_with_emb = len(school_ids_with_emb)

    print(f"\n  Structured features: {n_struct}")
    print(f"  Embedding dimensions: {n_emb_dims}")
    print(f"  Ratio (dims/samples): {n_emb_dims}/{n_labeled_with_emb} = {n_emb_dims/n_labeled_with_emb:.1f}x")

    # Step 4: Full-data correlation analysis (informational only)
    print("\n" + "-" * 50)
    print("STEP 4: Top-30 most correlated raw embedding dims (full labeled set)")
    print("        [INFORMATIONAL ONLY — not used for model selection]")
    print("-" * 50)
    top_full = full_correlation_analysis(E, y_with_emb, top_n=30)
    print(f"\n  {'Rank':<5} {'Dim Index':<12} {'Pearson r':<12} {'Direction'}")
    print(f"  {'-'*4:<5} {'-'*9:<12} {'-'*9:<12} {'-'*12}")
    for rank_i, (dim_idx, r) in enumerate(top_full, 1):
        direction = "↑ higher grade" if r > 0 else "↓ lower grade"
        print(f"  {rank_i:<5} {dim_idx:<12} {r:+.4f}      {direction}")

    max_abs_r = max(abs(r) for _, r in top_full)
    print(f"\n  Max |r| on full data: {max_abs_r:.4f} (expected ~0.3-0.5 due to multiple testing inflation)")

    # Step 5: CV experiment — compare K values
    print("\n" + "-" * 50)
    print("STEP 5: Cross-validation R² for different top-K embedding dims")
    print("        [Leakage-free: dim selection inside each training fold]")
    print("-" * 50)
    print(f"\n  CV setup: {CV_FOLDS}-fold, Ridge α={ALPHA}")
    print(f"  {'K dims':<10} {'CV R²':<10} {'±std':<8} {'vs baseline':<15} {'fold R²s'}")
    print(f"  {'-'*9:<10} {'-'*7:<10} {'-'*6:<8} {'-'*12:<15} {'-'*30}")

    baseline_r2 = None
    results = {}
    for k_val in K_VALUES:
        r2s = cv_with_embedding_selection(
            X_struct_with_emb, E, y_with_emb,
            k=CV_FOLDS, top_k_dims=k_val, alpha=ALPHA,
        )
        mean_r2 = float(np.mean(r2s))
        std_r2 = float(np.std(r2s))
        results[k_val] = (mean_r2, std_r2, r2s)

        if k_val == 0:
            baseline_r2 = mean_r2
            delta_str = "  (baseline)"
        else:
            delta = mean_r2 - baseline_r2
            delta_str = f"  {delta:+.4f}"

        fold_str = "  ".join(f"{r:.3f}" for r in r2s)
        label = "structured only" if k_val == 0 else f"struct + top-{k_val} emb"
        print(f"  K={k_val:<7} {mean_r2:+.4f}    ±{std_r2:.4f}   {delta_str:<15}  [{fold_str}]")

    # Step 6: Stability analysis
    print("\n" + "-" * 50)
    print("STEP 6: Embedding dims consistently selected across CV folds")
    print("        (selected in ≥2/5 folds when picking top-20)")
    print("-" * 50)

    stable_dims = find_stable_dims(X_struct_with_emb, E, y_with_emb, k=CV_FOLDS, top_k_dims=20)
    if not stable_dims:
        print("\n  No dimensions were consistently selected across ≥2 folds.")
        print("  This suggests the embedding correlation signal is unstable — likely noise.")
    else:
        print(f"\n  {'Dim Index':<12} {'Folds Selected':<16} {'Mean |r|':<12} {'Mean r':<10} {'Direction'}")
        print(f"  {'-'*9:<12} {'-'*13:<16} {'-'*8:<12} {'-'*8:<10} {'-'*15}")
        for dim_idx, n_folds, mean_abs_r, mean_r in stable_dims[:20]:
            direction = "↑ higher grade" if mean_r > 0 else "↓ lower grade"
            print(f"  {dim_idx:<12} {n_folds}/{CV_FOLDS:<13}    {mean_abs_r:.4f}       {mean_r:+.4f}    {direction}")

    # Step 7: Summary verdict
    print("\n" + "=" * 70)
    print("  VERDICT")
    print("=" * 70)

    best_k = max(results.keys(), key=lambda k: results[k][0])
    best_r2 = results[best_k][0]
    baseline = results[0][0]
    improvement = best_r2 - baseline
    best_std = results[best_k][1]

    print(f"\n  Structured-only baseline CV R²: {baseline:+.4f}")
    print(f"  Best embedding-augmented CV R²: {best_r2:+.4f} (K={best_k})")
    print(f"  Net improvement: {improvement:+.4f}")

    if improvement > best_std * 0.5:
        verdict = "MARGINAL GAIN — embedding dims add some signal, but unstable"
    elif improvement > 0.01:
        verdict = "SMALL GAIN — embedding dims help slightly"
    elif improvement > -0.01:
        verdict = "NO GAIN — embedding dims add noise, not signal"
    else:
        verdict = "HURTS — adding embedding dims degrades CV R² (overfit)"

    print(f"\n  Assessment: {verdict}")
    n_stable = len(stable_dims)
    print(f"  Stable dims (selected in ≥2/5 folds): {n_stable}")
    if n_stable < 3:
        print("  → Very few stable dimensions: signal is likely noise across the 3072 dims.")
        print("    The 3072-dim / ~128-sample ratio means almost any dim could look predictive")
        print("    on full-data correlation, but not in proper hold-out evaluation.")
    else:
        print(f"  → {n_stable} dims show some fold-stability. Consider dim indices above as")
        print("    potential features — but verify they have interpretable meaning.")

    print(f"\n  RECOMMENDATION:")
    if improvement < 0.02:
        print("  Stick with structured features only. Description embeddings add noise,")
        print("  not signal, when evaluated properly. The high-dim/low-sample ratio")
        print("  makes dimension selection unreliable with this sample size.")
    else:
        print(f"  Adding top-{best_k} embedding dims may help slightly.")
        print("  Use these dims with caution and validate on a held-out city (e.g. NRW).")
    print()


if __name__ == "__main__":
    main()
