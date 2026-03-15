"""Regression scoring pipeline for school performance estimation.

Trains a linear regression on labeled schools (Berlin Abitur data) and
predicts performance for all profiled schools. Produces full diagnostics:
model fit, feature importance, cross-validation, per-school contributions.
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
# Normalization
# ---------------------------------------------------------------------------

def normalize_profiles(
    profiles: List[CatchmentProfile],
) -> Tuple[Dict[str, np.ndarray], Dict[str, Tuple[float, float]], List[str]]:
    """Min-max normalize all numeric dimensions to 0-100.

    Returns:
        (feature_matrix_dict, ranges, ordered_school_ids)
        feature_matrix_dict maps dim_key → np.array of normalized values (one per school).
    """
    if not profiles:
        return {}, {}, []

    all_keys = sorted(set().union(*(p.values.keys() for p in profiles)))
    school_ids = [p.school_id for p in profiles]
    n = len(profiles)

    ranges: Dict[str, Tuple[float, float]] = {}
    normalized: Dict[str, np.ndarray] = {}

    for key in all_keys:
        raw = np.array([p.get(key, np.nan) for p in profiles])
        valid = raw[~np.isnan(raw)]
        if len(valid) == 0:
            continue

        mn, mx = float(valid.min()), float(valid.max())
        ranges[key] = (mn, mx)

        if mx == mn:
            norm = np.full(n, 50.0)
        else:
            norm = (raw - mn) / (mx - mn) * 100.0

        # Invert lower-is-better dimensions
        dim_meta = DIMENSIONS.get(key)
        if dim_meta and dim_meta.direction == Direction.LOWER_BETTER:
            norm = 100.0 - norm

        # Replace NaN with 50 (neutral)
        norm = np.where(np.isnan(norm), 50.0, norm)
        normalized[key] = norm

    return normalized, ranges, school_ids


# ---------------------------------------------------------------------------
# Core regression with diagnostics
# ---------------------------------------------------------------------------

def _fit_ols(X: np.ndarray, y: np.ndarray) -> Optional[np.ndarray]:
    """Fit OLS: returns coefficient vector [intercept, β1, β2, ...] or None."""
    X_b = np.column_stack([np.ones(len(X)), X])
    try:
        beta, residuals, rank, sv = np.linalg.lstsq(X_b, y, rcond=None)
        return beta
    except np.linalg.LinAlgError:
        return None


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
# Greedy feature selection with logged path
# ---------------------------------------------------------------------------

def _greedy_feature_selection(
    X_dict: Dict[str, np.ndarray],
    y: np.ndarray,
    labeled_mask: np.ndarray,
    candidates: List[str],
    min_improvement: float = 0.01,
) -> Tuple[List[str], List[Dict]]:
    """Forward stepwise selection. Returns (selected_keys, selection_path)."""
    selected: List[str] = []
    path: List[Dict] = []
    best_r2 = -1.0
    remaining = list(candidates)

    y_train = y[labeled_mask]

    while remaining:
        step_best_key = None
        step_best_r2 = best_r2

        for key in remaining:
            trial = selected + [key]
            X_trial = np.column_stack([X_dict[k][labeled_mask] for k in trial])
            beta = _fit_ols(X_trial, y_train)
            if beta is None:
                continue
            r2 = _r_squared(y_train, _predict(X_trial, beta))
            if r2 > step_best_r2:
                step_best_r2 = r2
                step_best_key = key

        improvement = step_best_r2 - best_r2
        if step_best_key is None or improvement < min_improvement:
            path.append({
                "step": len(selected) + 1,
                "action": "STOP",
                "reason": f"Best improvement {improvement:.4f} < threshold {min_improvement}",
                "r_squared": best_r2,
            })
            break

        selected.append(step_best_key)
        remaining.remove(step_best_key)
        best_r2 = step_best_r2

        label = DIMENSIONS[step_best_key].label if step_best_key in DIMENSIONS else step_best_key
        path.append({
            "step": len(selected),
            "action": "ADD",
            "feature": step_best_key,
            "feature_label": label,
            "r_squared": round(best_r2, 4),
            "improvement": round(improvement, 4),
        })

    return selected, path


# ---------------------------------------------------------------------------
# Cross-validation
# ---------------------------------------------------------------------------

def _leave_one_out_cv(
    X: np.ndarray,
    y: np.ndarray,
) -> List[CrossValidationFold]:
    """Leave-one-out cross-validation (appropriate for small N)."""
    n = len(y)
    folds = []

    for i in range(n):
        mask = np.ones(n, dtype=bool)
        mask[i] = False

        X_train, y_train = X[mask], y[mask]
        X_test, y_test = X[~mask], y[~mask]

        beta = _fit_ols(X_train, y_train)
        if beta is None:
            continue

        y_pred_test = _predict(X_test, beta)
        y_pred_train = _predict(X_train, y_train)

        folds.append(CrossValidationFold(
            fold=i + 1,
            train_size=int(mask.sum()),
            test_size=1,
            r_squared=_r_squared(y_train, _predict(X_train, beta)),
            rmse=float(np.abs(y_test[0] - y_pred_test[0])),  # Single sample → |error|
            mae=float(np.abs(y_test[0] - y_pred_test[0])),
        ))

    return folds


def _kfold_cv(
    X: np.ndarray,
    y: np.ndarray,
    k: int = 5,
) -> List[CrossValidationFold]:
    """K-fold cross-validation."""
    n = len(y)
    if n < k:
        return _leave_one_out_cv(X, y)

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

        beta = _fit_ols(X_train, y_train)
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
    target_label: str = "Abitur average",
) -> Optional[ModelDiagnostics]:
    """Train regression model and produce full diagnostics.

    Args:
        profiles: Catchment profiles for ALL schools (labeled + unlabeled).
        labeled: school_id → target value (e.g. Abitur average grade).
        feature_keys: Explicit features to use. If None, runs greedy selection.
        cv_folds: Number of cross-validation folds (uses LOO if N < cv_folds).
        target_label: Human-readable name of the target variable.

    Returns:
        ModelDiagnostics with full model info, or None if insufficient data.
    """
    if len(labeled) < 5:
        logger.warning(f"Only {len(labeled)} labeled schools — need ≥ 5 for regression.")
        return None

    # Normalize
    X_dict, ranges, school_ids = normalize_profiles(profiles)
    if not X_dict:
        return None

    # Build labeled mask
    labeled_mask = np.array([sid in labeled for sid in school_ids])
    if labeled_mask.sum() < 5:
        return None

    y_all = np.array([labeled.get(sid, np.nan) for sid in school_ids])
    y_labeled = y_all[labeled_mask]

    # Available candidate features (must have variance in labeled data)
    if feature_keys is None:
        candidates = []
        for key, arr in X_dict.items():
            vals = arr[labeled_mask]
            if np.std(vals) > 1e-6:
                candidates.append(key)
        selected, selection_path = _greedy_feature_selection(
            X_dict, y_all, labeled_mask, candidates
        )
    else:
        selected = [k for k in feature_keys if k in X_dict]
        selection_path = [{"step": i + 1, "action": "FORCED", "feature": k}
                          for i, k in enumerate(selected)]

    if not selected:
        logger.warning("No features selected — cannot build model.")
        return None

    # Build feature matrices
    X_all = np.column_stack([X_dict[k] for k in selected])
    X_labeled = X_all[labeled_mask]

    # Fit final model
    beta = _fit_ols(X_labeled, y_labeled)
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

        # Partial R²: fit model without this feature, measure R² drop
        other_keys = [k for k in selected if k != key]
        if other_keys:
            X_without = np.column_stack([X_dict[k][labeled_mask] for k in other_keys])
            beta_without = _fit_ols(X_without, y_labeled)
            r2_without = _r_squared(y_labeled, _predict(X_without, beta_without)) if beta_without is not None else 0.0
        else:
            r2_without = 0.0
        partial_r2 = r2 - r2_without

        # Direction interpretation
        dim_meta = DIMENSIONS.get(key)
        label = dim_meta.label if dim_meta else key
        desc = dim_meta.description if dim_meta else ""
        direction = "positive" if std_coef >= 0 else "negative"

        # Approximate p-value via t-statistic
        p_value = None
        if n > p + 1:
            mse = np.sum((y_labeled - y_pred_labeled) ** 2) / (n - p - 1)
            X_b = np.column_stack([np.ones(n), X_labeled])
            try:
                cov = mse * np.linalg.inv(X_b.T @ X_b)
                se = np.sqrt(np.abs(cov[j + 1, j + 1]))
                if se > 0:
                    t_stat = coef / se
                    # Two-tailed p-value approximation using normal (good enough for diagnostics)
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
    cv_results = _kfold_cv(X_labeled, y_labeled, k=cv_folds)
    cv_r2s = [f.r_squared for f in cv_results]
    cv_rmses = [f.rmse for f in cv_results]

    # --- Per-school predictions + contributions ---
    contributions = _decompose_contributions(X_all, beta, selected)

    # Confidence: based on Mahalanobis-like distance from training distribution
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
