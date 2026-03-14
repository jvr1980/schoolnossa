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
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="School Nossa API",
    description="School selection dashboard API — Berlin, London, and more",
    version="0.2.0",
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
    country: Optional[str] = None,
    city: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get list of schools with optional filtering

    - **district**: Filter by district/Local Authority (e.g., "Mitte", "Camden")
    - **school_type**: Filter by school type (e.g., "Gymnasium", "Grammar School")
    - **country**: Filter by country code (e.g., "DE", "UK")
    - **city**: Filter by city (e.g., "Berlin", "London", "Manchester")
    - **limit**: Number of results to return (max 500)
    - **offset**: Number of results to skip (for pagination)
    """
    service = SchoolService(db)

    if district:
        schools = service.get_schools_by_district(district, country=country, city=city)
        schools = schools[offset:offset+limit]
    else:
        schools = service.get_all_schools(limit=limit, offset=offset, country=country, city=city)

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
async def get_districts(
    country: Optional[str] = None,
    city: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Get filter options for schools

    - **country**: Filter by country code (e.g., "DE", "UK")
    - **city**: Filter by city (e.g., "Berlin", "London")

    Returns district names, school types, available countries, and cities
    """
    service = SchoolService(db)
    districts = service.get_districts(country=country, city=city)
    school_types = service.get_school_types(country=country, city=city)
    countries = service.get_countries()
    cities = service.get_cities(country=country)

    return {
        "districts": districts,
        "school_types": school_types,
        "countries": countries,
        "cities": cities,
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


@app.post("/admin/import/uk")
async def import_uk_schools(
    city: str = Query(..., description="UK city to import (e.g., 'London', 'Manchester')"),
    academic_year: str = Query("2023-24", description="Academic year (e.g., '2023-24')"),
    db: Session = Depends(get_db),
):
    """
    Import/update schools from UK Department for Education

    Fetches school data from GIAS and performance data from DfE
    for the specified city.

    **Note**: This is an admin endpoint and should be protected in production.
    """
    service = SchoolService(db)

    try:
        result = service.import_schools_from_uk_dfe(city, academic_year)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"UK import failed for {city}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Import failed: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
