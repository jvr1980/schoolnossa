"""Regression scoring pipeline for school performance estimation."""

from .types import CatchmentProfile, SchoolPrediction, FeatureDiagnostic, CrossValidationFold, ModelDiagnostics
from .scorer import train_and_diagnose
from .dimensions import DIMENSIONS, Direction, DimensionSource, DimensionMeta

__all__ = [
    "CatchmentProfile",
    "SchoolPrediction",
    "FeatureDiagnostic",
    "CrossValidationFold",
    "ModelDiagnostics",
    "train_and_diagnose",
    "DIMENSIONS",
    "Direction",
    "DimensionSource",
    "DimensionMeta",
]
