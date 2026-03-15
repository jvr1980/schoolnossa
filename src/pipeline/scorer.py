"""Scoring pipeline for school catchment profiles.

Implements both rules-based weighted scoring and optional regression-based
performance estimation. Directly adapted from GreenspaceFinder's step7_score.py.
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np

from .dimensions import DIMENSIONS, Direction, get_scorable_dimensions, default_weights
from .types import CatchmentProfile, NormalizedProfile, ScoreMode, ScoringResult

logger = logging.getLogger(__name__)


def normalize_profiles(
    profiles: List[CatchmentProfile],
) -> Tuple[List[NormalizedProfile], Dict[str, Tuple[float, float]]]:
    """Min-max normalize all numeric dimension values to 0-100.

    Returns:
        (normalized_profiles, ranges) where ranges maps dim_key → (min_val, max_val).
    """
    if not profiles:
        return [], {}

    # Collect all dimension keys that have at least one value
    all_keys = set()
    for p in profiles:
        all_keys.update(p.values.keys())

    # Compute min/max per dimension
    ranges: Dict[str, Tuple[float, float]] = {}
    for key in all_keys:
        vals = [p.get(key) for p in profiles if key in p.values]
        if not vals:
            continue
        mn, mx = min(vals), max(vals)
        ranges[key] = (mn, mx)

    # Normalize
    result = []
    for p in profiles:
        norm = NormalizedProfile(school_id=p.school_id, raw=dict(p.values))
        for key in all_keys:
            if key not in p.values or key not in ranges:
                continue
            mn, mx = ranges[key]
            if mx == mn:
                norm.normalized[key] = 50.0  # All same → midpoint
            else:
                norm.normalized[key] = ((p.get(key) - mn) / (mx - mn)) * 100.0

            # Invert "lower_better" dimensions so higher normalized = better
            dim_meta = DIMENSIONS.get(key)
            if dim_meta and dim_meta.direction == Direction.LOWER_BETTER:
                norm.normalized[key] = 100.0 - norm.normalized[key]

        result.append(norm)

    return result, ranges


def rules_score(
    profiles: List[NormalizedProfile],
    weights: Optional[Dict[str, float]] = None,
) -> None:
    """Compute weighted rules-based score for each profile (in place).

    Formula: score = Σ(weight[d] × normalized[d]) / Σ(weights)
    Only scorable dimensions (not informational) are included.
    """
    if weights is None:
        weights = default_weights()

    scorable_keys = {d.key for d in get_scorable_dimensions()}

    for p in profiles:
        total_weight = 0.0
        weighted_sum = 0.0

        for key, w in weights.items():
            if key not in scorable_keys or key not in p.normalized:
                continue
            weighted_sum += w * p.normalized[key]
            total_weight += w

        p.rules_score = weighted_sum / total_weight if total_weight > 0 else 0.0


def regression_score(
    profiles: List[NormalizedProfile],
    labeled: Dict[str, float],
    feature_keys: Optional[List[str]] = None,
) -> Tuple[Optional[float], Optional[Dict[str, float]], Optional[List[str]]]:
    """Train a linear regression on labeled schools and score all profiles.

    Args:
        profiles: All normalized profiles (both labeled and unlabeled).
        labeled: Map of school_id → performance value (e.g. Abitur average).
        feature_keys: Optional list of feature keys to use. If None, uses
            greedy feature selection (GSF Step 7 approach).

    Returns:
        (r_squared, feature_importance, selected_features) or (None, None, None)
        if not enough labeled data.
    """
    if len(labeled) < 5:
        logger.info(f"Only {len(labeled)} labeled schools, need ≥5 for regression")
        return None, None, None

    # Separate labeled profiles
    labeled_profiles = [p for p in profiles if p.school_id in labeled]
    if len(labeled_profiles) < 5:
        return None, None, None

    # Determine candidate features
    candidate_keys = feature_keys
    if candidate_keys is None:
        # Use all numeric normalized dimensions that have values in labeled data
        candidate_keys = sorted(set().union(*(p.normalized.keys() for p in labeled_profiles)))

    if not candidate_keys:
        return None, None, None

    # Build target vector
    y = np.array([labeled[p.school_id] for p in labeled_profiles])
    y_std = np.std(y)
    if y_std == 0:
        return None, None, None

    # Greedy feature selection if no explicit features provided
    if feature_keys is None:
        selected = _greedy_select(labeled_profiles, y, candidate_keys)
    else:
        selected = list(feature_keys)

    if not selected:
        return None, None, None

    # Build feature matrix for labeled data
    X_labeled = np.array([
        [p.normalized.get(k, 50.0) for k in selected]
        for p in labeled_profiles
    ])

    # Train linear regression (closed-form: β = (X'X)^-1 X'y)
    X_b = np.column_stack([np.ones(len(X_labeled)), X_labeled])
    try:
        beta = np.linalg.lstsq(X_b, y, rcond=None)[0]
    except np.linalg.LinAlgError:
        return None, None, None

    # R² on training data
    y_pred_labeled = X_b @ beta
    ss_res = np.sum((y - y_pred_labeled) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Feature importance: standardized coefficients
    feature_importance = {}
    for i, key in enumerate(selected):
        x_std = np.std(X_labeled[:, i])
        if x_std > 0 and y_std > 0:
            feature_importance[key] = float(beta[i + 1] * x_std / y_std)
        else:
            feature_importance[key] = 0.0

    # Score ALL profiles
    X_all = np.array([
        [p.normalized.get(k, 50.0) for k in selected]
        for p in profiles
    ])
    X_all_b = np.column_stack([np.ones(len(X_all)), X_all])
    predictions = X_all_b @ beta

    # Normalize predictions to 0-100
    p_min, p_max = predictions.min(), predictions.max()
    if p_max > p_min:
        norm_pred = (predictions - p_min) / (p_max - p_min) * 100.0
    else:
        norm_pred = np.full_like(predictions, 50.0)

    for p, score in zip(profiles, norm_pred):
        p.model_score = float(score)

    # Confidence: lower for profiles far from training data distribution
    labeled_means = np.mean(X_labeled, axis=0)
    labeled_stds = np.std(X_labeled, axis=0) + 1e-6
    for i, p in enumerate(profiles):
        x_vec = X_all[i]
        z_scores = np.abs((x_vec - labeled_means) / labeled_stds)
        avg_z = float(np.mean(z_scores))
        # Confidence decays as profile diverges from training distribution
        p.model_confidence = max(0.0, min(1.0, 1.0 - avg_z / 5.0))

    return r_squared, feature_importance, selected


def _greedy_select(
    profiles: List[NormalizedProfile],
    y: np.ndarray,
    candidates: List[str],
    min_improvement: float = 0.01,
) -> List[str]:
    """Greedy forward feature selection (GSF Step 7 approach).

    Start with the single best feature, then add features one at a time
    only if they improve R² by at least min_improvement.
    """
    selected: List[str] = []
    best_r2 = -1.0

    remaining = list(candidates)

    while remaining:
        best_key = None
        best_new_r2 = best_r2

        for key in remaining:
            trial = selected + [key]
            X = np.array([
                [p.normalized.get(k, 50.0) for k in trial]
                for p in profiles
            ])
            X_b = np.column_stack([np.ones(len(X)), X])
            try:
                beta = np.linalg.lstsq(X_b, y, rcond=None)[0]
            except np.linalg.LinAlgError:
                continue

            y_pred = X_b @ beta
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            if r2 > best_new_r2:
                best_new_r2 = r2
                best_key = key

        if best_key is None or best_new_r2 - best_r2 < min_improvement:
            break

        selected.append(best_key)
        remaining.remove(best_key)
        best_r2 = best_new_r2

    return selected


def score_schools(
    profiles: List[CatchmentProfile],
    weights: Optional[Dict[str, float]] = None,
    labeled: Optional[Dict[str, float]] = None,
) -> ScoringResult:
    """Complete scoring pipeline: normalize → rules score → optional regression.

    Args:
        profiles: Raw catchment profiles for all schools.
        weights: Dimension weights for rules-based scoring. Defaults to equal weights.
        labeled: Optional map of school_id → performance value for regression.

    Returns:
        ScoringResult with ranked profiles.
    """
    normalized, _ = normalize_profiles(profiles)
    rules_score(normalized, weights)

    mode = ScoreMode.RULES
    r_squared = None
    importance = None
    selected_features = None

    if labeled and len(labeled) >= 5:
        r2, imp, sel = regression_score(normalized, labeled)
        if r2 is not None:
            mode = ScoreMode.REGRESSION
            r_squared = r2
            importance = imp
            selected_features = sel

    return ScoringResult(
        profiles=normalized,
        mode=mode,
        weights=weights or default_weights(),
        r_squared=r_squared,
        feature_importance=importance,
        selected_features=selected_features,
    )
