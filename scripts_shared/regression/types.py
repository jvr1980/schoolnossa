"""Data types for the catchment profiling and scoring pipeline."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class CatchmentProfile:
    """Catchment area profile for a single school.

    Contains raw dimension values computed from the school's surroundings.
    """
    school_id: str
    latitude: float
    longitude: float
    radius_m: float
    values: Dict[str, float] = field(default_factory=dict)

    def get(self, key: str, default: float = 0.0) -> float:
        return self.values.get(key, default)

    def set(self, key: str, value: float) -> None:
        self.values[key] = value


@dataclass
class SchoolPrediction:
    """Regression prediction for a single school."""
    school_id: str
    predicted: float                          # Raw predicted value (e.g. Abitur grade)
    actual: Optional[float] = None            # Actual value if labeled (for diagnostics)
    residual: Optional[float] = None          # actual - predicted
    confidence: float = 0.0                   # 0-1, based on distance from training distribution
    feature_contributions: Dict[str, float] = field(default_factory=dict)
    # Per-feature: how much this feature moved the prediction from the intercept


@dataclass
class FeatureDiagnostic:
    """Diagnostic info for a single selected feature."""
    key: str
    label: str
    coefficient: float              # Raw regression coefficient
    standardized_coef: float        # Standardized (beta) coefficient — comparable across features
    direction: str                  # "positive" or "negative" — how it affects the target
    p_value: Optional[float]        # Statistical significance (None if not computed)
    partial_r_squared: float        # How much R² this feature adds on its own
    description: str = ""


@dataclass
class CrossValidationFold:
    """Results from a single CV fold."""
    fold: int
    train_size: int
    test_size: int
    r_squared: float
    rmse: float
    mae: float


@dataclass
class ModelDiagnostics:
    """Complete regression model diagnostics."""
    # Overall fit
    r_squared: float
    adjusted_r_squared: float
    rmse: float
    mae: float
    n_samples: int
    n_features: int
    intercept: float

    # Feature-level
    features: List[FeatureDiagnostic]
    feature_selection_path: List[Dict]  # Step-by-step feature selection log

    # Cross-validation
    cv_folds: List[CrossValidationFold]
    cv_r_squared_mean: float
    cv_r_squared_std: float
    cv_rmse_mean: float

    # Per-school
    predictions: List[SchoolPrediction]

    # Model coefficients for reproduction
    coefficients: Dict[str, float]

    @property
    def is_reliable(self) -> bool:
        """Heuristic: model is usable if CV R² > 0.3 and std < 0.3."""
        return self.cv_r_squared_mean > 0.3 and self.cv_r_squared_std < 0.3
