"""Pydantic schemas for API requests and responses"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal


class SchoolResponse(BaseModel):
    """Basic school information response"""
    school_id: str
    name: str
    school_type: Optional[str] = None
    address: Optional[str] = None
    district: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    public_private: Optional[str] = None

    class Config:
        from_attributes = True


class SchoolDetailResponse(BaseModel):
    """Detailed school information response"""
    school_id: str
    name: str
    school_type: Optional[str] = None
    address: Optional[str] = None
    district: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    public_private: Optional[str] = None
    contact_info: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SchoolMetricsResponse(BaseModel):
    """School metrics annual data response"""
    year: int
    total_students: Optional[int] = None
    students_change_percent: Optional[Decimal] = None
    total_teachers: Optional[int] = None
    teachers_change_percent: Optional[Decimal] = None
    student_teacher_ratio: Optional[Decimal] = None
    ratio_change_percent: Optional[Decimal] = None
    abitur_success_rate: Optional[Decimal] = None
    abitur_success_change_percent: Optional[Decimal] = None
    abitur_average_grade: Optional[Decimal] = None
    abitur_grade_change: Optional[Decimal] = None
    demand_score: Optional[int] = None
    demand_change_percent: Optional[Decimal] = None
    migration_background_percent: Optional[Decimal] = None
    data_source: Optional[str] = None
    collected_at: datetime

    class Config:
        from_attributes = True


class ImportResultResponse(BaseModel):
    """Data import result response"""
    status: str
    created: int
    updated: int
    errors: int
    total_processed: int


class DistrictsResponse(BaseModel):
    """Districts and school types response"""
    districts: List[str]
    school_types: List[str]


# --- Catchment profiling & regression scoring schemas ---


class DimensionInfo(BaseModel):
    """Metadata for a single dimension."""
    key: str
    label: str
    direction: str
    source: str
    unit: str = ""
    description: str = ""


class ProfileRequest(BaseModel):
    """Request to profile schools in a catchment area."""
    district: Optional[str] = None
    school_type: Optional[str] = None
    radius_m: float = Field(1000.0, ge=200, le=5000, description="Catchment radius in meters")


class SchoolProfileResponse(BaseModel):
    """Catchment profile for a single school."""
    school_id: str
    name: str
    school_type: Optional[str] = None
    address: Optional[str] = None
    district: Optional[str] = None
    latitude: float
    longitude: float
    dimensions: Dict[str, float]


class FeatureDiagnosticResponse(BaseModel):
    """Diagnostic info for a single regression feature."""
    key: str
    label: str
    coefficient: float
    standardized_coef: float
    direction: str
    p_value: Optional[float] = None
    partial_r_squared: float


class SchoolPredictionResponse(BaseModel):
    """Regression prediction for a single school."""
    school_id: str
    name: str
    district: Optional[str] = None
    predicted: float
    actual: Optional[float] = None
    residual: Optional[float] = None
    confidence: float
    feature_contributions: Dict[str, float]


class RegressionRequest(BaseModel):
    """Request to train regression model and score schools."""
    labeled: Dict[str, float] = Field(
        ...,
        description="school_id → target value (e.g. Abitur average grade). Minimum 5 entries.",
        min_length=5,
    )
    features: Optional[List[str]] = Field(
        None,
        description="Feature keys to force. If omitted, uses greedy forward selection.",
    )
    district: Optional[str] = None
    school_type: Optional[str] = None
    radius_m: float = Field(1000.0, ge=200, le=5000)
    cv_folds: int = Field(5, ge=2, le=20)


class CVFoldResponse(BaseModel):
    """Single cross-validation fold result."""
    fold: int
    train_size: int
    test_size: int
    r_squared: float
    rmse: float
    mae: float


class RegressionResultResponse(BaseModel):
    """Complete regression model result with diagnostics."""
    # Overall model fit
    r_squared: float
    adjusted_r_squared: float
    rmse: float
    mae: float
    n_samples: int
    n_features: int
    intercept: float
    is_reliable: bool

    # Feature diagnostics
    features: List[FeatureDiagnosticResponse]
    feature_selection_path: List[Dict[str, Any]]
    coefficients: Dict[str, float]

    # Cross-validation
    cv_folds: List[CVFoldResponse]
    cv_r_squared_mean: float
    cv_r_squared_std: float
    cv_rmse_mean: float

    # Per-school predictions
    predictions: List[SchoolPredictionResponse]
