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
    country: Optional[str] = "DE"
    city: Optional[str] = "Berlin"
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
    country: Optional[str] = "DE"
    city: Optional[str] = "Berlin"
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


class CrimeStatsResponse(BaseModel):
    """Crime statistics for a school's area"""
    year: int
    month: Optional[int] = None
    total_crimes: Optional[int] = None
    violent_crimes: Optional[int] = None
    theft: Optional[int] = None
    burglary: Optional[int] = None
    robbery: Optional[int] = None
    vehicle_crime: Optional[int] = None
    drugs: Optional[int] = None
    antisocial_behaviour: Optional[int] = None
    criminal_damage: Optional[int] = None
    crime_rate_per_1000: Optional[Decimal] = None
    radius_meters: Optional[int] = None
    area_name: Optional[str] = None
    data_source: Optional[str] = None
    collected_at: datetime

    class Config:
        from_attributes = True


class DistrictsResponse(BaseModel):
    """Districts and school types response"""
    districts: List[str]
    school_types: List[str]
    countries: Optional[List[str]] = None
    cities: Optional[List[str]] = None
