"""FastAPI application for School Nossa API"""

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from ..models.database import get_db
from ..services.school_service import SchoolService
from .schemas import (
    SchoolResponse,
    SchoolDetailResponse,
    SchoolMetricsResponse,
    ImportResultResponse,
    DistrictsResponse,
    DimensionInfo,
    ProfileRequest,
    SchoolProfileResponse,
    FeatureDiagnosticResponse,
    SchoolPredictionResponse,
    RegressionRequest,
    CVFoldResponse,
    RegressionResultResponse,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="School Nossa API",
    description="Berlin school selection dashboard API",
    version="0.1.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "School Nossa API",
        "version": "0.1.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/schools", response_model=List[SchoolResponse])
async def get_schools(
    district: Optional[str] = None,
    school_type: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get list of schools with optional filtering

    - **district**: Filter by district (e.g., "Mitte", "Charlottenburg-Wilmersdorf")
    - **school_type**: Filter by school type (e.g., "Gymnasium", "Sekundarschule")
    - **limit**: Number of results to return (max 500)
    - **offset**: Number of results to skip (for pagination)
    """
    service = SchoolService(db)

    if district:
        schools = service.get_schools_by_district(district)
        schools = schools[offset:offset+limit]
    else:
        schools = service.get_all_schools(limit=limit, offset=offset)

    # Filter by school type if specified
    if school_type:
        schools = [s for s in schools if s.school_type == school_type]

    return schools


@app.get("/schools/{school_id}", response_model=SchoolDetailResponse)
async def get_school_detail(
    school_id: str,
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a specific school

    - **school_id**: Official Berlin school ID
    """
    service = SchoolService(db)
    school = service.get_school_by_id(school_id)

    if not school:
        raise HTTPException(status_code=404, detail="School not found")

    return school


@app.get("/schools/{school_id}/metrics", response_model=List[SchoolMetricsResponse])
async def get_school_metrics(
    school_id: str,
    years: int = Query(3, ge=1, le=10),
    db: Session = Depends(get_db),
):
    """
    Get historical metrics for a school

    - **school_id**: Official Berlin school ID
    - **years**: Number of years of history to retrieve (default: 3)
    """
    service = SchoolService(db)

    # Check if school exists
    school = service.get_school_by_id(school_id)
    if not school:
        raise HTTPException(status_code=404, detail="School not found")

    metrics = service.get_school_metrics_history(school_id, years=years)

    if not metrics:
        raise HTTPException(
            status_code=404,
            detail="No metrics found for this school"
        )

    return metrics


@app.get("/districts", response_model=DistrictsResponse)
async def get_districts(db: Session = Depends(get_db)):
    """
    Get list of all Berlin districts with schools

    Returns a list of district names for filtering
    """
    service = SchoolService(db)
    districts = service.get_districts()
    school_types = service.get_school_types()

    return {
        "districts": districts,
        "school_types": school_types,
    }


@app.post("/admin/import", response_model=ImportResultResponse)
async def import_schools(db: Session = Depends(get_db)):
    """
    Import/update schools from Berlin Open Data Portal

    This endpoint triggers data collection from the official API.
    Use this to refresh school data.

    **Note**: This is an admin endpoint and should be protected in production.
    """
    service = SchoolService(db)

    try:
        result = service.import_schools_from_berlin_open_data()
        return result
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Import failed: {str(e)}"
        )


# --- Catchment Profiling & Regression Endpoints ---


@app.get("/dimensions", response_model=List[DimensionInfo])
async def get_dimensions():
    """
    Get metadata for all available catchment dimensions.

    Returns dimension keys, labels, directions, and data sources.
    """
    from ..pipeline.dimensions import DIMENSIONS

    return [
        DimensionInfo(
            key=d.key,
            label=d.label,
            direction=d.direction.value,
            source=d.source.value,
            unit=d.unit,
            description=d.description,
        )
        for d in DIMENSIONS.values()
    ]


@app.post("/schools/profile", response_model=List[SchoolProfileResponse])
async def profile_schools(
    request: ProfileRequest,
    db: Session = Depends(get_db),
):
    """
    Profile schools using catchment area analysis.

    Builds a catchment profile for each matching school by analyzing
    demographics, POIs, transit, crime, and competition within the
    specified radius. Returns raw dimension values per school.
    """
    from ..pipeline.profiler import SchoolProfiler

    service = SchoolService(db)
    schools_data = _get_schools_data(service, request.district, request.school_type)

    profiler = SchoolProfiler(radius_m=request.radius_m)
    profiles = await profiler.profile_schools(schools_data)

    school_lookup = {s["school_id"]: s for s in schools_data}
    return [
        SchoolProfileResponse(
            school_id=p.school_id,
            name=school_lookup[p.school_id].get("name", ""),
            school_type=school_lookup[p.school_id].get("school_type"),
            address=school_lookup[p.school_id].get("address"),
            district=school_lookup[p.school_id].get("district"),
            latitude=p.latitude,
            longitude=p.longitude,
            dimensions=p.values,
        )
        for p in profiles
        if p.school_id in school_lookup
    ]


@app.post("/schools/regression", response_model=RegressionResultResponse)
async def train_regression_model(
    request: RegressionRequest,
    db: Session = Depends(get_db),
):
    """
    Train a regression model to estimate school performance.

    Profiles all matching schools, trains a linear regression on the
    labeled subset, and returns full diagnostics: model fit, feature
    importance, cross-validation, per-school predictions with
    feature contribution breakdowns.

    - **labeled**: school_id → target value (e.g. Abitur average). Min 5 entries.
    - **features**: Optional feature keys to force. Omit for auto-selection.
    - **district/school_type**: Optional filters.
    - **radius_m**: Catchment radius in meters.
    - **cv_folds**: Number of cross-validation folds.
    """
    from ..pipeline.profiler import SchoolProfiler
    from ..pipeline.scorer import train_and_diagnose

    service = SchoolService(db)
    schools_data = _get_schools_data(service, request.district, request.school_type)

    # Profile
    profiler = SchoolProfiler(radius_m=request.radius_m)
    profiles = await profiler.profile_schools(schools_data)

    if not profiles:
        raise HTTPException(status_code=400, detail="No schools could be profiled")

    # Train and diagnose
    diag = train_and_diagnose(
        profiles=profiles,
        labeled=request.labeled,
        feature_keys=request.features,
        cv_folds=request.cv_folds,
    )

    if diag is None:
        raise HTTPException(
            status_code=400,
            detail="Model training failed. Need ≥5 labeled schools with matching IDs and feature variance.",
        )

    school_lookup = {s["school_id"]: s for s in schools_data}

    return RegressionResultResponse(
        r_squared=diag.r_squared,
        adjusted_r_squared=diag.adjusted_r_squared,
        rmse=diag.rmse,
        mae=diag.mae,
        n_samples=diag.n_samples,
        n_features=diag.n_features,
        intercept=diag.intercept,
        is_reliable=diag.is_reliable,
        features=[
            FeatureDiagnosticResponse(
                key=f.key,
                label=f.label,
                coefficient=f.coefficient,
                standardized_coef=f.standardized_coef,
                direction=f.direction,
                p_value=f.p_value,
                partial_r_squared=f.partial_r_squared,
            )
            for f in diag.features
        ],
        feature_selection_path=diag.feature_selection_path,
        coefficients=diag.coefficients,
        cv_folds=[
            CVFoldResponse(
                fold=f.fold,
                train_size=f.train_size,
                test_size=f.test_size,
                r_squared=f.r_squared,
                rmse=f.rmse,
                mae=f.mae,
            )
            for f in diag.cv_folds
        ],
        cv_r_squared_mean=diag.cv_r_squared_mean,
        cv_r_squared_std=diag.cv_r_squared_std,
        cv_rmse_mean=diag.cv_rmse_mean,
        predictions=[
            SchoolPredictionResponse(
                school_id=p.school_id,
                name=school_lookup.get(p.school_id, {}).get("name", ""),
                district=school_lookup.get(p.school_id, {}).get("district"),
                predicted=p.predicted,
                actual=p.actual,
                residual=p.residual,
                confidence=p.confidence,
                feature_contributions=p.feature_contributions,
            )
            for p in diag.predictions
        ],
    )


@app.get("/schools/{school_id}/profile", response_model=SchoolProfileResponse)
async def get_school_profile(
    school_id: str,
    radius_m: float = Query(1000.0, ge=200, le=5000),
    db: Session = Depends(get_db),
):
    """
    Get catchment profile for a single school.
    """
    from ..pipeline.profiler import SchoolProfiler

    service = SchoolService(db)
    school = service.get_school_by_id(school_id)

    if not school:
        raise HTTPException(status_code=404, detail="School not found")

    if not school.latitude or not school.longitude:
        raise HTTPException(status_code=400, detail="School has no coordinates")

    all_schools = service.get_all_schools(limit=500)
    all_data = [
        {
            "school_id": s.school_id,
            "latitude": float(s.latitude) if s.latitude else 0.0,
            "longitude": float(s.longitude) if s.longitude else 0.0,
            "school_type": s.school_type or "",
        }
        for s in all_schools
        if s.latitude and s.longitude
    ]

    profiler = SchoolProfiler(radius_m=radius_m)
    profile = await profiler.profile_school(
        school_id=school.school_id,
        lat=float(school.latitude),
        lng=float(school.longitude),
        address=school.address or "",
        school_type=school.school_type or "",
        all_schools=all_data,
    )

    return SchoolProfileResponse(
        school_id=school.school_id,
        name=school.name,
        school_type=school.school_type,
        address=school.address,
        district=school.district,
        latitude=float(school.latitude),
        longitude=float(school.longitude),
        dimensions=profile.values,
    )


def _get_schools_data(
    service: SchoolService,
    district: Optional[str] = None,
    school_type: Optional[str] = None,
) -> List[dict]:
    """Helper: load schools from DB and convert to dicts for the profiler."""
    if district:
        schools_orm = service.get_schools_by_district(district)
    else:
        schools_orm = service.get_all_schools(limit=500)

    if school_type:
        schools_orm = [s for s in schools_orm if s.school_type == school_type]

    if not schools_orm:
        raise HTTPException(status_code=404, detail="No schools found matching criteria")

    schools_data = [
        {
            "school_id": s.school_id,
            "name": s.name,
            "latitude": float(s.latitude) if s.latitude else 0.0,
            "longitude": float(s.longitude) if s.longitude else 0.0,
            "address": s.address or "",
            "school_type": s.school_type or "",
            "district": s.district or "",
        }
        for s in schools_orm
        if s.latitude and s.longitude
    ]

    if not schools_data:
        raise HTTPException(status_code=404, detail="No schools with coordinates found")

    return schools_data


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
