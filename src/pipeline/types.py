"""Data types for the catchment profiling and scoring pipeline."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


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
class NormalizedProfile:
    """Profile with min-max normalized values (0-100 scale)."""
    school_id: str
    raw: Dict[str, float] = field(default_factory=dict)
    normalized: Dict[str, float] = field(default_factory=dict)
    rules_score: float = 0.0
    model_score: Optional[float] = None
    model_confidence: Optional[float] = None

    @property
    def active_score(self) -> float:
        return self.model_score if self.model_score is not None else self.rules_score


class ScoreMode(str, Enum):
    RULES = "rules"
    REGRESSION = "regression"


@dataclass
class ScoringResult:
    """Complete result of scoring a set of schools."""
    profiles: List[NormalizedProfile]
    mode: ScoreMode
    weights: Dict[str, float]
    r_squared: Optional[float] = None
    feature_importance: Optional[Dict[str, float]] = None
    selected_features: Optional[List[str]] = None

    def ranked(self) -> List[NormalizedProfile]:
        return sorted(self.profiles, key=lambda p: p.active_score, reverse=True)
