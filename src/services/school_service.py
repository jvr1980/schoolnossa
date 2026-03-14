"""School service for business logic and data operations"""

from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from ..models.school import School, SchoolMetricsAnnual, CollectionLog
from ..collectors.germany.berlin_open_data import BerlinOpenDataCollectorSync
from ..collectors.united_kingdom.dfe_collector import UKDfECollectorSync

logger = logging.getLogger(__name__)


class SchoolService:
    """Service for school data operations"""

    def __init__(self, db: Session):
        self.db = db
        self.collector = BerlinOpenDataCollectorSync()
        self.uk_collector = UKDfECollectorSync()

    def import_schools_from_berlin_open_data(self) -> Dict[str, Any]:
        """
        Import schools from Berlin Open Data Portal

        Returns:
            Dictionary with import statistics
        """
        log_entry = CollectionLog(
            source="berlin_open_data",
            collection_date=datetime.utcnow(),
            status="in_progress",
        )
        self.db.add(log_entry)
        self.db.commit()

        try:
            # Collect schools from API
            logger.info("Starting school data collection from Berlin Open Data")
            schools_data = self.collector.collect_all_schools()

            created_count = 0
            updated_count = 0
            error_count = 0

            for school_data in schools_data:
                try:
                    school_id = school_data.get("school_id")
                    if not school_id:
                        logger.warning(f"Skipping school without ID: {school_data.get('name')}")
                        error_count += 1
                        continue

                    # Check if school exists
                    existing_school = self.db.query(School).filter(
                        School.school_id == str(school_id)
                    ).first()

                    if existing_school:
                        # Update existing school
                        for key, value in school_data.items():
                            if key != "raw_data" and hasattr(existing_school, key):
                                setattr(existing_school, key, value)
                        existing_school.updated_at = datetime.utcnow()
                        updated_count += 1
                    else:
                        # Create new school
                        new_school = School(
                            school_id=str(school_id),
                            name=school_data.get("name"),
                            school_type=school_data.get("school_type"),
                            address=school_data.get("address"),
                            district=school_data.get("district"),
                            latitude=school_data.get("latitude"),
                            longitude=school_data.get("longitude"),
                            public_private=school_data.get("public_private"),
                            contact_info=school_data.get("contact_info"),
                        )
                        self.db.add(new_school)
                        created_count += 1

                except Exception as e:
                    logger.error(f"Error importing school {school_data.get('name')}: {str(e)}")
                    error_count += 1
                    continue

            # Commit all changes
            self.db.commit()

            # Update log entry
            log_entry.status = "success" if error_count == 0 else "partial"
            log_entry.schools_updated = created_count + updated_count
            log_entry.error_log = f"Created: {created_count}, Updated: {updated_count}, Errors: {error_count}"
            self.db.commit()

            result = {
                "status": "success",
                "created": created_count,
                "updated": updated_count,
                "errors": error_count,
                "total_processed": len(schools_data),
            }

            logger.info(f"School import completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Error importing schools: {str(e)}")
            log_entry.status = "failed"
            log_entry.error_log = str(e)
            self.db.commit()
            raise

    def import_schools_from_uk_dfe(self, city: str, academic_year: str = "2023-24") -> Dict[str, Any]:
        """
        Import schools from UK Department for Education for a specific city.

        Args:
            city: Target city (e.g., 'London', 'Manchester')
            academic_year: Academic year for performance data (e.g., '2023-24')

        Returns:
            Dictionary with import statistics
        """
        log_entry = CollectionLog(
            source="uk_dfe",
            collection_date=datetime.utcnow(),
            status="in_progress",
        )
        self.db.add(log_entry)
        self.db.commit()

        try:
            logger.info(f"Starting UK school data collection for {city}")
            schools_data = self.uk_collector.collect_schools_for_city(city, academic_year)

            created_count = 0
            updated_count = 0
            error_count = 0

            for school_data in schools_data:
                try:
                    school_id = school_data.get("school_id")
                    if not school_id:
                        error_count += 1
                        continue

                    existing_school = self.db.query(School).filter(
                        School.school_id == str(school_id)
                    ).first()

                    if existing_school:
                        for key in ("name", "school_type", "address", "district",
                                    "country", "city", "latitude", "longitude",
                                    "public_private", "contact_info"):
                            value = school_data.get(key)
                            if value is not None:
                                setattr(existing_school, key, value)
                        existing_school.updated_at = datetime.utcnow()
                        updated_count += 1
                    else:
                        new_school = School(
                            school_id=str(school_id),
                            name=school_data.get("name"),
                            school_type=school_data.get("school_type"),
                            address=school_data.get("address"),
                            district=school_data.get("district"),
                            country=school_data.get("country", "UK"),
                            city=school_data.get("city", city),
                            latitude=school_data.get("latitude"),
                            longitude=school_data.get("longitude"),
                            public_private=school_data.get("public_private"),
                            contact_info=school_data.get("contact_info"),
                        )
                        self.db.add(new_school)
                        created_count += 1

                    # Import metrics if available
                    metrics_data = school_data.get("metrics")
                    if metrics_data:
                        year = metrics_data.get("year")
                        existing_metrics = self.db.query(SchoolMetricsAnnual).filter(
                            and_(
                                SchoolMetricsAnnual.school_id == str(school_id),
                                SchoolMetricsAnnual.year == year,
                            )
                        ).first()

                        if not existing_metrics:
                            new_metrics = SchoolMetricsAnnual(
                                school_id=str(school_id),
                                year=year,
                                abitur_success_rate=metrics_data.get("abitur_success_rate"),
                                abitur_average_grade=metrics_data.get("abitur_average_grade"),
                                migration_background_percent=metrics_data.get("migration_background_percent"),
                                raw_data=metrics_data.get("raw_data"),
                                data_source=metrics_data.get("data_source", "uk_dfe"),
                            )
                            self.db.add(new_metrics)

                except Exception as e:
                    logger.error(f"Error importing UK school {school_data.get('name')}: {str(e)}")
                    error_count += 1
                    continue

            self.db.commit()

            log_entry.status = "success" if error_count == 0 else "partial"
            log_entry.schools_updated = created_count + updated_count
            log_entry.error_log = f"Created: {created_count}, Updated: {updated_count}, Errors: {error_count}"
            self.db.commit()

            result = {
                "status": "success",
                "created": created_count,
                "updated": updated_count,
                "errors": error_count,
                "total_processed": len(schools_data),
                "city": city,
            }

            logger.info(f"UK school import completed for {city}: {result}")
            return result

        except Exception as e:
            logger.error(f"Error importing UK schools for {city}: {str(e)}")
            log_entry.status = "failed"
            log_entry.error_log = str(e)
            self.db.commit()
            raise

    def get_school_by_id(self, school_id: str) -> Optional[School]:
        """Get school by ID"""
        return self.db.query(School).filter(School.school_id == school_id).first()

    def get_schools_by_district(
        self, district: str, country: Optional[str] = None, city: Optional[str] = None
    ) -> List[School]:
        """Get all schools in a district, optionally filtered by country/city"""
        query = self.db.query(School).filter(School.district == district)
        if country:
            query = query.filter(School.country == country)
        if city:
            query = query.filter(School.city == city)
        return query.all()

    def get_all_schools(
        self,
        limit: int = 100,
        offset: int = 0,
        country: Optional[str] = None,
        city: Optional[str] = None,
    ) -> List[School]:
        """Get all schools with pagination, optionally filtered by country/city"""
        query = self.db.query(School)
        if country:
            query = query.filter(School.country == country)
        if city:
            query = query.filter(School.city == city)
        return query.offset(offset).limit(limit).all()

    def calculate_year_over_year_change(
        self,
        school_id: str,
        current_year: int,
        metric: str
    ) -> Optional[float]:
        """
        Calculate year-over-year change percentage for a metric

        Args:
            school_id: School identifier
            current_year: Current year
            metric: Metric name (e.g., 'total_students', 'abitur_success_rate')

        Returns:
            Percentage change or None if data not available
        """
        current_data = self.db.query(SchoolMetricsAnnual).filter(
            and_(
                SchoolMetricsAnnual.school_id == school_id,
                SchoolMetricsAnnual.year == current_year
            )
        ).first()

        previous_data = self.db.query(SchoolMetricsAnnual).filter(
            and_(
                SchoolMetricsAnnual.school_id == school_id,
                SchoolMetricsAnnual.year == current_year - 1
            )
        ).first()

        if not current_data or not previous_data:
            return None

        current_value = getattr(current_data, metric, None)
        previous_value = getattr(previous_data, metric, None)

        if current_value is None or previous_value is None or previous_value == 0:
            return None

        change_percent = ((current_value - previous_value) / previous_value) * 100
        return round(change_percent, 2)

    def get_school_metrics_history(
        self,
        school_id: str,
        years: int = 3
    ) -> List[SchoolMetricsAnnual]:
        """
        Get historical metrics for a school

        Args:
            school_id: School identifier
            years: Number of years to retrieve

        Returns:
            List of metrics ordered by year (descending)
        """
        return (
            self.db.query(SchoolMetricsAnnual)
            .filter(SchoolMetricsAnnual.school_id == school_id)
            .order_by(SchoolMetricsAnnual.year.desc())
            .limit(years)
            .all()
        )

    def get_districts(
        self, country: Optional[str] = None, city: Optional[str] = None
    ) -> List[str]:
        """Get list of all districts with schools, optionally filtered"""
        query = self.db.query(School.district).distinct().filter(
            School.district.isnot(None)
        )
        if country:
            query = query.filter(School.country == country)
        if city:
            query = query.filter(School.city == city)
        return sorted([r[0] for r in query.all()])

    def get_school_types(
        self, country: Optional[str] = None, city: Optional[str] = None
    ) -> List[str]:
        """Get list of all school types, optionally filtered"""
        query = self.db.query(School.school_type).distinct().filter(
            School.school_type.isnot(None)
        )
        if country:
            query = query.filter(School.country == country)
        if city:
            query = query.filter(School.city == city)
        return sorted([r[0] for r in query.all()])

    def get_countries(self) -> List[str]:
        """Get list of all countries with schools"""
        result = self.db.query(School.country).distinct().filter(
            School.country.isnot(None)
        ).all()
        return sorted([r[0] for r in result])

    def get_cities(self, country: Optional[str] = None) -> List[str]:
        """Get list of all cities with schools, optionally filtered by country"""
        query = self.db.query(School.city).distinct().filter(
            School.city.isnot(None)
        )
        if country:
            query = query.filter(School.country == country)
        return sorted([r[0] for r in query.all()])
