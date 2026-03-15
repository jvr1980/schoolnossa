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
    ScoreRequest,
    ScoringResultResponse,
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


# --- Catchment Profiling & Scoring Endpoints ---


@app.get("/dimensions", response_model=List[DimensionInfo])
async def get_dimensions():
    """
    Get metadata for all available catchment dimensions.

    Returns dimension keys, labels, directions, and data sources
    so the frontend can build weight sliders and display labels.
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


@app.post("/schools/profile", response_model=ScoringResultResponse)
async def profile_and_score_schools(
    request: ProfileRequest,
    db: Session = Depends(get_db),
):
    """
    Profile and score schools using catchment area analysis.

    Builds a catchment profile for each matching school by analyzing
    demographics, POIs, transit, crime, and competition within the
    specified radius. Returns rules-based scores with equal weights.

    - **district**: Optional filter by district
    - **school_type**: Optional filter by school type
    - **radius_m**: Catchment radius in meters (default: 1000)
    """
    from ..pipeline.profiler import SchoolProfiler
    from ..pipeline.scorer import score_schools
    from ..pipeline.dimensions import default_weights

    service = SchoolService(db)

    # Get schools to profile
    if request.district:
        schools_orm = service.get_schools_by_district(request.district)
    else:
        schools_orm = service.get_all_schools(limit=500)

    if request.school_type:
        schools_orm = [s for s in schools_orm if s.school_type == request.school_type]

    if not schools_orm:
        raise HTTPException(status_code=404, detail="No schools found matching criteria")

    # Convert ORM objects to dicts for the profiler
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

    # Profile all schools
    profiler = SchoolProfiler(radius_m=request.radius_m)
    profiles = await profiler.profile_schools(schools_data)

    # Score with equal weights
    result = score_schools(profiles)

    # Build response
    school_lookup = {s["school_id"]: s for s in schools_data}
    ranked = result.ranked()

    school_responses = []
    for rank, p in enumerate(ranked, start=1):
        s = school_lookup.get(p.school_id, {})
        school_responses.append(
            SchoolProfileResponse(
                school_id=p.school_id,
                name=s.get("name", ""),
                school_type=s.get("school_type"),
                address=s.get("address"),
                district=s.get("district"),
                latitude=s.get("latitude", 0.0),
                longitude=s.get("longitude", 0.0),
                dimensions=p.normalized,
                rules_score=round(p.rules_score, 2),
                model_score=round(p.model_score, 2) if p.model_score is not None else None,
                model_confidence=round(p.model_confidence, 3) if p.model_confidence is not None else None,
                rank=rank,
            )
        )

    return ScoringResultResponse(
        mode=result.mode.value,
        weights=result.weights,
        r_squared=result.r_squared,
        feature_importance=result.feature_importance,
        selected_features=result.selected_features,
        schools=school_responses,
    )


@app.post("/schools/score", response_model=ScoringResultResponse)
async def rescore_schools(
    request: ScoreRequest,
    district: Optional[str] = None,
    school_type: Optional[str] = None,
    radius_m: float = Query(1000.0, ge=200, le=5000),
    db: Session = Depends(get_db),
):
    """
    Re-score schools with custom weights (and optional regression).

    Use this after the initial profile to adjust dimension weights
    or to provide labeled performance data for ML-based estimation.

    - **weights**: Custom dimension weights (e.g. {"crime_index": 80, "transit_count": 60})
    - **labeled**: Optional map of school_id → Abitur average for regression
    - **district**: Optional district filter
    - **school_type**: Optional school type filter
    - **radius_m**: Catchment radius
    """
    from ..pipeline.profiler import SchoolProfiler
    from ..pipeline.scorer import score_schools

    service = SchoolService(db)

    if district:
        schools_orm = service.get_schools_by_district(district)
    else:
        schools_orm = service.get_all_schools(limit=500)

    if school_type:
        schools_orm = [s for s in schools_orm if s.school_type == school_type]

    if not schools_orm:
        raise HTTPException(status_code=404, detail="No schools found")

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

    profiler = SchoolProfiler(radius_m=radius_m)
    profiles = await profiler.profile_schools(schools_data)

    # Score with custom weights and optional labeled data
    result = score_schools(
        profiles,
        weights=request.weights if request.weights else None,
        labeled=request.labeled,
    )

    school_lookup = {s["school_id"]: s for s in schools_data}
    ranked = result.ranked()

    school_responses = []
    for rank, p in enumerate(ranked, start=1):
        s = school_lookup.get(p.school_id, {})
        school_responses.append(
            SchoolProfileResponse(
                school_id=p.school_id,
                name=s.get("name", ""),
                school_type=s.get("school_type"),
                address=s.get("address"),
                district=s.get("district"),
                latitude=s.get("latitude", 0.0),
                longitude=s.get("longitude", 0.0),
                dimensions=p.normalized,
                rules_score=round(p.rules_score, 2),
                model_score=round(p.model_score, 2) if p.model_score is not None else None,
                model_confidence=round(p.model_confidence, 3) if p.model_confidence is not None else None,
                rank=rank,
            )
        )

    return ScoringResultResponse(
        mode=result.mode.value,
        weights=result.weights,
        r_squared=result.r_squared,
        feature_importance=result.feature_importance,
        selected_features=result.selected_features,
        schools=school_responses,
    )


@app.get("/schools/{school_id}/profile", response_model=SchoolProfileResponse)
async def get_school_profile(
    school_id: str,
    radius_m: float = Query(1000.0, ge=200, le=5000),
    db: Session = Depends(get_db),
):
    """
    Get catchment profile for a single school.

    Returns all dimension values for the school's catchment area.
    """
    from ..pipeline.profiler import SchoolProfiler

    service = SchoolService(db)
    school = service.get_school_by_id(school_id)

    if not school:
        raise HTTPException(status_code=404, detail="School not found")

    if not school.latitude or not school.longitude:
        raise HTTPException(status_code=400, detail="School has no coordinates")

    # Get all schools for competition calculation
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
        rank=0,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
