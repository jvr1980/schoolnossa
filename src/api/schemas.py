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


# --- Catchment profiling & scoring schemas ---


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
    rules_score: float = 0.0
    model_score: Optional[float] = None
    model_confidence: Optional[float] = None
    rank: int = 0


class ScoreRequest(BaseModel):
    """Request to score profiled schools with custom weights."""
    weights: Dict[str, float] = Field(
        default_factory=dict,
        description="Dimension key → weight (0-100). Missing dimensions get 0.",
    )
    labeled: Optional[Dict[str, float]] = Field(
        None,
        description="Optional school_id → performance value for regression scoring.",
    )


class ScoringResultResponse(BaseModel):
    """Complete scoring result."""
    mode: str
    weights: Dict[str, float]
    r_squared: Optional[float] = None
    feature_importance: Optional[Dict[str, float]] = None
    selected_features: Optional[List[str]] = None
    schools: List[SchoolProfileResponse]
