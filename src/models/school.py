"""School data models"""

from sqlalchemy import Column, Integer, String, Text, DECIMAL, TIMESTAMP, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime

from .database import Base


class School(Base):
    """Core school information (slowly changing)"""
    __tablename__ = "schools"

    id = Column(Integer, primary_key=True, index=True)
    school_id = Column(String(255), unique=True, nullable=False, index=True)  # Official ID (Berlin schulnummer or UK URN)
    name = Column(String(500), nullable=False)
    school_type = Column(String(100))  # Gymnasium, Sekundarschule, Grammar School, Academy, etc.
    address = Column(Text)
    district = Column(String(100), index=True)  # Berlin Bezirk or UK Local Authority
    country = Column(String(10), default="DE", index=True)  # 'DE', 'UK'
    city = Column(String(100), default="Berlin", index=True)  # 'Berlin', 'London', 'Manchester', etc.
    latitude = Column(DECIMAL(10, 8))
    longitude = Column(DECIMAL(11, 8))
    public_private = Column(String(20))
    contact_info = Column(JSONB)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    metrics = relationship("SchoolMetricsAnnual", back_populates="school")
    enrollment_areas = relationship("EnrollmentArea", back_populates="school")
    crime_stats = relationship("CrimeStats", back_populates="school")

    def __repr__(self):
        return f"<School(id={self.school_id}, name={self.name}, type={self.school_type})>"


class SchoolMetricsAnnual(Base):
    """Annual snapshot data for year-over-year comparisons"""
    __tablename__ = "school_metrics_annual"

    id = Column(Integer, primary_key=True, index=True)
    school_id = Column(String(255), ForeignKey("schools.school_id"), nullable=False)
    year = Column(Integer, nullable=False)  # e.g., 2024, 2023

    # Student metrics
    total_students = Column(Integer)
    students_change_percent = Column(DECIMAL(5, 2))  # % change from previous year

    # Teacher metrics
    total_teachers = Column(Integer)
    teachers_change_percent = Column(DECIMAL(5, 2))

    # Ratio metrics
    student_teacher_ratio = Column(DECIMAL(5, 2))
    ratio_change_percent = Column(DECIMAL(5, 2))

    # Performance metrics (from sekundarschulen-berlin.de)
    abitur_success_rate = Column(DECIMAL(5, 2))  # % who pass Abitur
    abitur_success_change_percent = Column(DECIMAL(5, 2))
    abitur_average_grade = Column(DECIMAL(5, 2))  # DE: e.g., 2.5; UK: A-Level APS e.g., 35.2
    abitur_grade_change = Column(DECIMAL(5, 2))  # absolute change

    # Demand metrics
    demand_score = Column(Integer)  # Number of applications
    demand_change_percent = Column(DECIMAL(5, 2))

    # Demographics
    migration_background_percent = Column(DECIMAL(5, 2))

    # Raw data storage (for flexibility)
    raw_data = Column(JSONB)

    data_source = Column(String(100))  # 'berlin_open_data', 'sekundarschulen-berlin.de'
    collected_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    school = relationship("School", back_populates="metrics")

    __table_args__ = (
        UniqueConstraint('school_id', 'year', name='uq_school_year'),
        Index('idx_school_metrics_year', 'year', postgresql_ops={'year': 'DESC'}),
        Index('idx_school_metrics_school_year', 'school_id', 'year', postgresql_ops={'year': 'DESC'}),
    )

    def __repr__(self):
        return f"<SchoolMetrics(school_id={self.school_id}, year={self.year})>"


class CrimeStats(Base):
    """Crime statistics near a school, collected periodically.

    For UK: sourced from data.police.uk API (street-level, 1-mile radius).
    For Berlin: sourced from Kriminalitätsatlas (Bezirksregion/Planungsraum level).

    Stores aggregated crime counts by category for a given month around a school.
    """
    __tablename__ = "crime_stats"

    id = Column(Integer, primary_key=True, index=True)
    school_id = Column(String(255), ForeignKey("schools.school_id"), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=True)  # 1-12, NULL for annual aggregates

    # Aggregated crime counts (common categories across countries)
    total_crimes = Column(Integer)
    violent_crimes = Column(Integer)  # DE: Gewaltkriminalität, UK: violence-and-sexual-offences
    theft = Column(Integer)  # DE: Diebstahl, UK: theft-from-the-person + other-theft + shoplifting
    burglary = Column(Integer)  # DE: Wohnungseinbruch, UK: burglary
    robbery = Column(Integer)  # DE: Raub, UK: robbery
    vehicle_crime = Column(Integer)  # DE: Kfz-Diebstahl, UK: vehicle-crime
    drugs = Column(Integer)  # DE: Rauschgiftdelikte, UK: drugs
    antisocial_behaviour = Column(Integer)  # UK-specific, NULL for DE
    criminal_damage = Column(Integer)  # DE: Sachbeschädigung, UK: criminal-damage-arson

    # Rate per 1000 residents (normalized for comparison)
    crime_rate_per_1000 = Column(DECIMAL(8, 2))

    # Metadata
    radius_meters = Column(Integer)  # Search radius (UK: ~1609m/1mile, Berlin: area-based)
    area_name = Column(String(255))  # LOR Planungsraum or "1-mile radius"
    raw_data = Column(JSONB)  # Full breakdown by category
    data_source = Column(String(100))  # 'data_police_uk', 'berlin_kriminalitaetsatlas'
    collected_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    school = relationship("School", back_populates="crime_stats")

    __table_args__ = (
        UniqueConstraint('school_id', 'year', 'month', name='uq_crime_school_year_month'),
        Index('idx_crime_stats_school', 'school_id'),
        Index('idx_crime_stats_year_month', 'year', 'month'),
    )

    def __repr__(self):
        return f"<CrimeStats(school_id={self.school_id}, year={self.year}, month={self.month})>"


class EnrollmentArea(Base):
    """Enrollment areas (geospatial data)"""
    __tablename__ = "enrollment_areas"

    id = Column(Integer, primary_key=True, index=True)
    school_id = Column(String(255), ForeignKey("schools.school_id"), nullable=False)
    year = Column(Integer, nullable=False)
    # area_geom would use PostGIS GEOMETRY type, but for now we'll store as JSONB
    area_geom = Column(JSONB)  # Will store GeoJSON
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    # Relationships
    school = relationship("School", back_populates="enrollment_areas")

    def __repr__(self):
        return f"<EnrollmentArea(school_id={self.school_id}, year={self.year})>"


class CollectionLog(Base):
    """Data collection log for tracking updates"""
    __tablename__ = "collection_log"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(100))
    collection_date = Column(TIMESTAMP, default=datetime.utcnow)
    year_collected = Column(Integer)
    schools_updated = Column(Integer)
    status = Column(String(50))  # 'success', 'partial', 'failed'
    error_log = Column(Text)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    def __repr__(self):
        return f"<CollectionLog(source={self.source}, date={self.collection_date}, status={self.status})>"
