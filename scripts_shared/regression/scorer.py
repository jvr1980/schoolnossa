"""Regression scoring pipeline for school performance estimation.

Trains a Ridge regression on labeled schools (Abitur data from Berlin,
Hamburg, and other cities) and predicts performance for all profiled
schools. Uses L2 regularization instead of feature selection to handle
the high feature-to-sample ratio (29 features, ~200 samples).

Produces full diagnostics: model fit, feature importance,
cross-validation, per-school contributions.
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np

from .dimensions import DIMENSIONS, Direction
from .types import (
    CatchmentProfile,
    SchoolPrediction,
    FeatureDiagnostic,
    CrossValidationFold,
    ModelDiagnostics,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Standardization (z-score)
# ---------------------------------------------------------------------------

def standardize_profiles(
    profiles: List[CatchmentProfile],
    labeled_ids: set,
    fit_on_all: bool = False,
) -> Tuple[Dict[str, np.ndarray], Dict[str, Tuple[float, float]], List[str]]:
    """Z-score standardize dimensions.

    Args:
        profiles: All school profiles (labeled + unlabeled).
        labeled_ids: Set of school IDs with target labels.
        fit_on_all: If True, compute mean/std from ALL profiles (labeled + unlabeled).
                    Use this for cross-city models where labeled schools cover only one
                    city — fitting on labeled-only would give extreme z-scores for
                    out-of-distribution cities.
                    If False (default), fit mean/std on labeled schools only.

    NaN values are imputed with the reference-set median before standardizing.

    Returns:
        (feature_matrix_dict, stats, ordered_school_ids)
        feature_matrix_dict maps dim_key → np.array of standardized values.
        stats maps dim_key → (mean, std) from reference data.
    """
    if not profiles:
        return {}, {}, []

    all_keys = sorted(set().union(*(p.values.keys() for p in profiles)))
    school_ids = [p.school_id for p in profiles]
    labeled_mask = np.array([sid in labeled_ids for sid in school_ids])

    stats: Dict[str, Tuple[float, float]] = {}
    standardized: Dict[str, np.ndarray] = {}

    for key in all_keys:
        raw = np.array([p.get(key, np.nan) for p in profiles])

        # Choose reference population for mean/std
        if fit_on_all:
            ref_vals = raw[~np.isnan(raw)]
        else:
            labeled_vals = raw[labeled_mask]
            ref_vals = labeled_vals[~np.isnan(labeled_vals)]

        if len(ref_vals) < 5:
            continue

        # Impute NaN with reference median
        median_val = float(np.median(ref_vals))
        imputed = np.where(np.isnan(raw), median_val, raw)

        mean = float(np.mean(ref_vals))
        std = float(np.std(ref_vals))
        if std < 1e-9:
            continue

        # Guard against extreme z-scores: cap at ±10 sigma to prevent
        # wildly out-of-distribution values from dominating Ridge predictions.
        stats[key] = (mean, std)
        standardized[key] = np.clip((imputed - mean) / std, -10.0, 10.0)

    return standardized, stats, school_ids


# ---------------------------------------------------------------------------
# Ridge regression core
# ---------------------------------------------------------------------------

DEFAULT_RIDGE_ALPHA = 50.0

def _fit_ridge(
    X: np.ndarray,
    y: np.ndarray,
    alpha: float = DEFAULT_RIDGE_ALPHA,
) -> Optional[np.ndarray]:
    """Fit Ridge regression: returns [intercept, β1, β2, ...] or None.

    Closed-form: β = (X'X + αI)^(-1) X'y  (with intercept not penalized).
    """
    n, p = X.shape
    # Center y around its mean (intercept handled separately)
    y_mean = np.mean(y)
    y_c = y - y_mean
    x_mean = np.mean(X, axis=0)
    X_c = X - x_mean

    try:
        gram = X_c.T @ X_c + alpha * np.eye(p)
        beta = np.linalg.solve(gram, X_c.T @ y_c)
        intercept = y_mean - x_mean @ beta
        return np.concatenate([[intercept], beta])
    except np.linalg.LinAlgError:
        return None


def compute_prediction_intervals(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_new: np.ndarray,
    y_pred_new: np.ndarray,
    alpha: float = DEFAULT_RIDGE_ALPHA,
    ci_level: float = 0.80,
) -> Tuple[np.ndarray, np.ndarray]:
    """Compute prediction intervals for Ridge regression predictions.

    Uses: Var(ŷ_new) = σ² × (1 + x_new' W x_new)
    where W = (X'X + αI)^{-1}, σ² = RSS / (n - p_eff)

    Args:
        X_train: Training feature matrix (standardized)
        y_train: Training targets
        X_new: New observation feature matrix (standardized)
        y_pred_new: Point predictions for X_new
        alpha: Ridge regularization parameter
        ci_level: Confidence level (default 0.80 for 80% CI)

    Returns:
        (lower, upper) arrays of prediction interval bounds
    """
    n, p = X_train.shape

    # Center training data
    x_mean = np.mean(X_train, axis=0)
    X_c = X_train - x_mean
    X_new_c = X_new - x_mean

    # Residual variance
    beta = _fit_ridge(X_train, y_train, alpha)
    if beta is None:
        half_width = np.full(len(X_new), 0.3)  # fallback
        return y_pred_new - half_width, y_pred_new + half_width

    y_pred_train = _predict(X_train, beta)
    rss = np.sum((y_train - y_pred_train) ** 2)

    # Effective degrees of freedom for Ridge: tr(H) where H = X(X'X+αI)^{-1}X'
    gram = X_c.T @ X_c
    gram_reg_inv = np.linalg.inv(gram + alpha * np.eye(p))
    hat_matrix_trace = np.trace(gram @ gram_reg_inv)
    df_resid = max(n - hat_matrix_trace, 1.0)
    sigma2 = rss / df_resid

    # Per-observation prediction variance: σ²(1 + x'Wx)
    # W = (X'X + αI)^{-1}
    # For each new obs: leverage = x_new_c' @ gram_reg_inv @ x_new_c
    leverages = np.sum((X_new_c @ gram_reg_inv) * X_new_c, axis=1)
    pred_var = sigma2 * (1.0 + leverages)
    pred_se = np.sqrt(np.maximum(pred_var, 0.0))

    # z-value for desired CI level
    from math import erfc, sqrt
    # For 80% CI: z = 1.2816
    tail = (1.0 - ci_level) / 2.0
    # Inverse normal approximation (good enough for n > 30)
    # Using scipy-free approximation: z ≈ sqrt(2) * erfinv(1 - 2*tail)
    # For common levels:
    z_values = {0.80: 1.2816, 0.90: 1.6449, 0.95: 1.9600}
    z = z_values.get(ci_level, 1.2816)

    lower = y_pred_new - z * pred_se
    upper = y_pred_new + z * pred_se

    return lower, upper


def _r_squared(y: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0


def _adjusted_r_squared(r2: float, n: int, p: int) -> float:
    if n - p - 1 <= 0:
        return r2
    return 1.0 - (1.0 - r2) * (n - 1) / (n - p - 1)


def _rmse(y: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y - y_pred) ** 2)))


def _mae(y: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y - y_pred)))


def _predict(X: np.ndarray, beta: np.ndarray) -> np.ndarray:
    X_b = np.column_stack([np.ones(len(X)), X])
    return X_b @ beta


# ---------------------------------------------------------------------------
# Feature ranking (all features used, ranked by importance for diagnostics)
# ---------------------------------------------------------------------------

def _rank_features_by_importance(
    X: np.ndarray,
    y: np.ndarray,
    feature_keys: List[str],
    alpha: float = DEFAULT_RIDGE_ALPHA,
) -> List[Dict]:
    """Rank features by their contribution to the Ridge model.

    Unlike forward stepwise, all features are always included.
    This ranking is purely diagnostic — it shows which features
    carry the most weight in the regularized model.
    """
    beta = _fit_ridge(X, y, alpha)
    if beta is None:
        return []

    # Rank by absolute standardized coefficient
    x_std = np.std(X, axis=0)
    y_std = np.std(y)
    ranking = []
    for j, key in enumerate(feature_keys):
        coef = beta[j + 1]
        std_coef = coef * x_std[j] / y_std if y_std > 0 and x_std[j] > 0 else 0.0
        dim_meta = DIMENSIONS.get(key)
        ranking.append({
            "feature": key,
            "feature_label": dim_meta.label if dim_meta else key,
            "abs_std_coef": abs(std_coef),
            "std_coef": round(std_coef, 4),
        })

    ranking.sort(key=lambda r: r["abs_std_coef"], reverse=True)
    return ranking


# ---------------------------------------------------------------------------
# Cross-validation (Ridge)
# ---------------------------------------------------------------------------

def _kfold_cv(
    X: np.ndarray,
    y: np.ndarray,
    k: int = 5,
    alpha: float = DEFAULT_RIDGE_ALPHA,
) -> List[CrossValidationFold]:
    """K-fold cross-validation using Ridge regression."""
    n = len(y)
    if n < k:
        k = n  # Fall back to LOO

    indices = np.arange(n)
    np.random.seed(42)
    np.random.shuffle(indices)
    fold_sizes = np.full(k, n // k, dtype=int)
    fold_sizes[:n % k] += 1

    folds = []
    current = 0
    for fold_i in range(k):
        start, stop = current, current + fold_sizes[fold_i]
        test_idx = indices[start:stop]
        train_idx = np.concatenate([indices[:start], indices[stop:]])
        current = stop

        X_train, y_train = X[train_idx], y[train_idx]
        X_test, y_test = X[test_idx], y[test_idx]

        beta = _fit_ridge(X_train, y_train, alpha)
        if beta is None:
            continue

        y_pred = _predict(X_test, beta)
        folds.append(CrossValidationFold(
            fold=fold_i + 1,
            train_size=len(train_idx),
            test_size=len(test_idx),
            r_squared=_r_squared(y_test, y_pred) if len(y_test) > 1 else 0.0,
            rmse=_rmse(y_test, y_pred),
            mae=_mae(y_test, y_pred),
        ))

    return folds


# ---------------------------------------------------------------------------
# Per-school contribution decomposition
# ---------------------------------------------------------------------------

def _decompose_contributions(
    X: np.ndarray,
    beta: np.ndarray,
    feature_keys: List[str],
) -> List[Dict[str, float]]:
    """For each school, compute how much each feature moves the prediction
    away from the intercept. contribution_i = β_i × x_i."""
    contributions = []
    intercept = beta[0]
    for row in X:
        c = {}
        for j, key in enumerate(feature_keys):
            c[key] = float(beta[j + 1] * row[j])
        contributions.append(c)
    return contributions


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def train_and_diagnose(
    profiles: List[CatchmentProfile],
    labeled: Dict[str, float],
    feature_keys: Optional[List[str]] = None,
    cv_folds: int = 5,
    alpha: float = DEFAULT_RIDGE_ALPHA,
    target_label: str = "Abitur average",
) -> Optional[ModelDiagnostics]:
    """Train Ridge regression model and produce full diagnostics.

    Uses all available features with L2 regularization (Ridge) instead of
    feature selection. This approach is more stable with our sample size
    (~200 schools) and reduces the train/CV gap significantly.

    Args:
        profiles: Catchment profiles for ALL schools (labeled + unlabeled).
        labeled: school_id → target value (e.g. Abitur average grade).
        feature_keys: Explicit features to use. If None, uses all with variance.
        cv_folds: Number of cross-validation folds.
        alpha: Ridge regularization strength (higher = more regularization).
        target_label: Human-readable name of the target variable.

    Returns:
        ModelDiagnostics with full model info, or None if insufficient data.
    """
    if len(labeled) < 5:
        logger.warning(f"Only {len(labeled)} labeled schools — need ≥ 5 for regression.")
        return None

    # Standardize (z-score, fit on labeled data)
    X_dict, stats, school_ids = standardize_profiles(profiles, set(labeled.keys()))
    if not X_dict:
        return None

    # Build labeled mask
    labeled_mask = np.array([sid in labeled for sid in school_ids])
    if labeled_mask.sum() < 5:
        return None

    y_all = np.array([labeled.get(sid, np.nan) for sid in school_ids])
    y_labeled = y_all[labeled_mask]

    # Select features: use all with variance, or explicit list
    if feature_keys is None:
        selected = sorted(k for k, arr in X_dict.items()
                         if np.std(arr[labeled_mask]) > 1e-6)
    else:
        selected = [k for k in feature_keys if k in X_dict]

    if not selected:
        logger.warning("No features available — cannot build model.")
        return None

    # Feature ranking (diagnostic only — all features are used)
    X_labeled_full = np.column_stack([X_dict[k][labeled_mask] for k in selected])
    feature_ranking = _rank_features_by_importance(X_labeled_full, y_labeled, selected, alpha)

    selection_path = []
    for rank_i, r in enumerate(feature_ranking):
        selection_path.append({
            "step": rank_i + 1,
            "action": "INCLUDE",
            "feature": r["feature"],
            "feature_label": r["feature_label"],
            "std_coef": r["std_coef"],
            "abs_std_coef": round(r["abs_std_coef"], 4),
        })

    # Build feature matrices
    X_all = np.column_stack([X_dict[k] for k in selected])
    X_labeled = X_all[labeled_mask]

    # Fit final Ridge model
    beta = _fit_ridge(X_labeled, y_labeled, alpha)
    if beta is None:
        return None

    y_pred_labeled = _predict(X_labeled, beta)
    y_pred_all = _predict(X_all, beta)

    n = len(y_labeled)
    p = len(selected)
    r2 = _r_squared(y_labeled, y_pred_labeled)
    adj_r2 = _adjusted_r_squared(r2, n, p)
    rmse = _rmse(y_labeled, y_pred_labeled)
    mae = _mae(y_labeled, y_pred_labeled)

    # --- Feature-level diagnostics ---
    y_std = np.std(y_labeled)
    feature_diagnostics = []

    for j, key in enumerate(selected):
        x_col = X_labeled[:, j]
        x_std = np.std(x_col)
        coef = float(beta[j + 1])

        # Standardized coefficient
        std_coef = coef * x_std / y_std if y_std > 0 and x_std > 0 else 0.0

        # Partial R²: fit Ridge without this feature, measure R² drop
        other_keys = [k for k in selected if k != key]
        if other_keys:
            X_without = np.column_stack([X_dict[k][labeled_mask] for k in other_keys])
            beta_without = _fit_ridge(X_without, y_labeled, alpha)
            r2_without = _r_squared(y_labeled, _predict(X_without, beta_without)) if beta_without is not None else 0.0
        else:
            r2_without = 0.0
        partial_r2 = r2 - r2_without

        # Direction interpretation
        dim_meta = DIMENSIONS.get(key)
        label = dim_meta.label if dim_meta else key
        desc = dim_meta.description if dim_meta else ""
        direction = "positive" if std_coef >= 0 else "negative"

        # Approximate p-value via t-statistic (using effective degrees of freedom)
        p_value = None
        if n > p + 1:
            mse = np.sum((y_labeled - y_pred_labeled) ** 2) / (n - p - 1)
            # For Ridge, SE approximation using (X'X + αI)^-1 X'X (X'X + αI)^-1
            X_c = X_labeled - np.mean(X_labeled, axis=0)
            try:
                gram = X_c.T @ X_c
                gram_reg = gram + alpha * np.eye(p)
                gram_reg_inv = np.linalg.inv(gram_reg)
                cov = mse * gram_reg_inv @ gram @ gram_reg_inv
                se = np.sqrt(np.abs(cov[j, j]))
                if se > 0:
                    t_stat = coef / se
                    from math import erfc, sqrt
                    p_value = erfc(abs(t_stat) / sqrt(2))
            except np.linalg.LinAlgError:
                pass

        feature_diagnostics.append(FeatureDiagnostic(
            key=key,
            label=label,
            coefficient=coef,
            standardized_coef=round(std_coef, 4),
            direction=direction,
            p_value=round(p_value, 4) if p_value is not None else None,
            partial_r_squared=round(partial_r2, 4),
            description=desc,
        ))

    # Sort by absolute standardized coefficient (most important first)
    feature_diagnostics.sort(key=lambda f: abs(f.standardized_coef), reverse=True)

    # --- Cross-validation ---
    cv_results = _kfold_cv(X_labeled, y_labeled, k=cv_folds, alpha=alpha)
    cv_r2s = [f.r_squared for f in cv_results]
    cv_rmses = [f.rmse for f in cv_results]

    # --- Per-school predictions + contributions ---
    contributions = _decompose_contributions(X_all, beta, selected)

    # Confidence: based on distance from training distribution in standardized space
    labeled_mean = np.mean(X_labeled, axis=0)
    labeled_std = np.std(X_labeled, axis=0) + 1e-6

    predictions = []
    for i, sid in enumerate(school_ids):
        actual = labeled.get(sid)
        pred = float(y_pred_all[i])
        resid = actual - pred if actual is not None else None

        z_scores = np.abs((X_all[i] - labeled_mean) / labeled_std)
        avg_z = float(np.mean(z_scores))
        confidence = max(0.0, min(1.0, 1.0 - avg_z / 5.0))

        predictions.append(SchoolPrediction(
            school_id=sid,
            predicted=round(pred, 3),
            actual=round(actual, 3) if actual is not None else None,
            residual=round(resid, 3) if resid is not None else None,
            confidence=round(confidence, 3),
            feature_contributions=contributions[i],
        ))

    return ModelDiagnostics(
        r_squared=round(r2, 4),
        adjusted_r_squared=round(adj_r2, 4),
        rmse=round(rmse, 4),
        mae=round(mae, 4),
        n_samples=n,
        n_features=p,
        intercept=round(float(beta[0]), 4),
        features=feature_diagnostics,
        feature_selection_path=selection_path,
        cv_folds=cv_results,
        cv_r_squared_mean=round(float(np.mean(cv_r2s)), 4) if cv_r2s else 0.0,
        cv_r_squared_std=round(float(np.std(cv_r2s)), 4) if cv_r2s else 0.0,
        cv_rmse_mean=round(float(np.mean(cv_rmses)), 4) if cv_rmses else 0.0,
        predictions=predictions,
        coefficients={k: round(float(beta[i + 1]), 6) for i, k in enumerate(selected)},
    )
