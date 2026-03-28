"""Dimension metadata for school catchment profiling.

Each dimension describes one measurable aspect of a school's catchment area.
Modeled after GreenspaceFinder's dimension system.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class Direction(str, Enum):
    HIGHER_BETTER = "higher_better"
    LOWER_BETTER = "lower_better"
    INFORMATIONAL = "informational"


class DimensionSource(str, Enum):
    SCHOOL_DIRECTORY = "school_directory"
    GOOGLE_NEARBY = "google_nearby"
    ZENSUS = "zensus"
    LOCAL_DATA = "local_data"
    COMPUTED = "computed"


@dataclass(frozen=True)
class DimensionMeta:
    key: str
    label: str
    direction: Direction
    source: DimensionSource
    unit: str = ""
    description: str = ""
    google_place_type: Optional[str] = None  # For Google Nearby Search dimensions


# All dimensions available for catchment profiling
DIMENSIONS: Dict[str, DimensionMeta] = {}


def _register(*dims: DimensionMeta) -> None:
    for d in dims:
        DIMENSIONS[d.key] = d


# --- School directory dimensions (categorical/informational) ---
_register(
    DimensionMeta(
        key="school_type",
        label="School type",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
        description="Gymnasium, Sekundarschule, Gesamtschule, etc.",
    ),
    DimensionMeta(
        key="public_private",
        label="Public / Private",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
    ),
    DimensionMeta(
        key="student_count",
        label="Student count",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
        unit="students",
    ),
)

# --- Transit dimensions (Google Nearby Search) ---
_register(
    DimensionMeta(
        key="transit_count",
        label="Transit stops nearby",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="stops",
        description="Number of transit/bus stations within catchment radius",
        google_place_type="transit_station",
    ),
    DimensionMeta(
        key="transit_nearest_m",
        label="Nearest transit stop",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="m",
        description="Distance in meters to the closest transit station",
        google_place_type="transit_station",
    ),
)

# --- Safety dimension (local data) ---
_register(
    DimensionMeta(
        key="crime_index",
        label="Area crime rate",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="District-level crime index (lower = safer)",
    ),
)

# --- Population / demographic dimensions (Zensus) ---
_register(
    DimensionMeta(
        key="catchment_population",
        label="Catchment population",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="people",
        description="Estimated population within catchment radius",
    ),
    DimensionMeta(
        key="population_density",
        label="Population density",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="per km²",
        description="People per square kilometer",
    ),
    DimensionMeta(
        key="youth_pct",
        label="Youth population %",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.ZENSUS,
        unit="%",
        description="Percentage aged 6-18 in catchment area",
    ),
    DimensionMeta(
        key="migration_pct",
        label="Migration background %",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="%",
        description="Percentage with migration background",
    ),
    DimensionMeta(
        key="adult_abitur_pct",
        label="Adults with Abitur %",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.ZENSUS,
        unit="%",
        description="Percentage of adults with Abitur or higher qualification",
    ),
    DimensionMeta(
        key="avg_rent",
        label="Average rent level",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="€/m²",
        description="Average rent per sqm as socioeconomic proxy",
    ),
)

# --- Competition dimensions (computed from school directory) ---
_register(
    DimensionMeta(
        key="other_schools_count",
        label="Competing schools",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.COMPUTED,
        unit="schools",
        description="Number of same-type schools within catchment radius",
    ),
    DimensionMeta(
        key="nearest_school_m",
        label="Nearest same-type school",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.COMPUTED,
        unit="m",
        description="Distance to the closest school of the same type",
    ),
)

# --- POI / environment dimensions (Google Nearby Search) ---
_register(
    DimensionMeta(
        key="library_count",
        label="Libraries nearby",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="libraries",
        description="Libraries within catchment radius",
        google_place_type="library",
    ),
    DimensionMeta(
        key="park_count",
        label="Parks / playgrounds nearby",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="parks",
        description="Parks and playgrounds within catchment radius",
        google_place_type="park",
    ),
    DimensionMeta(
        key="university_nearest_m",
        label="Nearest university",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="m",
        description="Distance to the nearest university",
        google_place_type="university",
    ),
    DimensionMeta(
        key="supermarket_count",
        label="Supermarkets nearby",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="supermarkets",
        description="Supermarkets within catchment radius (neighborhood vitality proxy)",
        google_place_type="supermarket",
    ),
)

# --- Traffic / road safety dimensions (local sensor data) ---
_register(
    DimensionMeta(
        key="traffic_intensity",
        label="Traffic intensity",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="Composite traffic intensity score near school (cars, speed, heavy vehicles)",
    ),
    DimensionMeta(
        key="traffic_cars_per_hour",
        label="Cars per hour",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        unit="cars/h",
        description="Average cars per hour at nearby sensors",
    ),
    DimensionMeta(
        key="traffic_bikes_per_hour",
        label="Bikes per hour",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        unit="bikes/h",
        description="Average bikes per hour (bike-friendliness proxy)",
    ),
    DimensionMeta(
        key="traffic_speed_v85",
        label="85th percentile speed",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        unit="km/h",
        description="85th percentile vehicle speed (road safety)",
    ),
)

# --- Crime sub-dimensions (local police data) ---
_register(
    DimensionMeta(
        key="crime_violent",
        label="Violent crime rate",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="Average violent crime (robbery + assault) in district",
    ),
    DimensionMeta(
        key="crime_drug_offenses",
        label="Drug offenses",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="Drug offense rate in district",
    ),
    DimensionMeta(
        key="crime_safety_rank",
        label="Safety rank",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="District safety rank (1 = safest)",
    ),
)

# --- School-level academic/demand dimensions ---
_register(
    DimensionMeta(
        key="nachfrage_prozent",
        label="Demand ratio",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.SCHOOL_DIRECTORY,
        unit="%",
        description="Application-to-places ratio (oversubscription)",
    ),
    DimensionMeta(
        key="belastungsstufe",
        label="Belastungsstufe",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.SCHOOL_DIRECTORY,
        description="Social burden index (Berlin: 1=low, 5=high)",
    ),
    DimensionMeta(
        key="migration_pct_school",
        label="School migration %",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
        unit="%",
        description="Percentage of students with migration background at school level",
    ),
    DimensionMeta(
        key="student_teacher_ratio",
        label="Student-teacher ratio",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.COMPUTED,
        unit="students/teacher",
        description="Students per teacher (lower = more individual attention)",
    ),
    DimensionMeta(
        key="is_gymnasium",
        label="Is Gymnasium",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
        description="1 if Gymnasium, 0 otherwise. Strong predictor of Abitur outcomes.",
    ),
    DimensionMeta(
        key="student_count_total",
        label="Total students",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
        unit="students",
        description="Total number of students enrolled",
    ),
    DimensionMeta(
        key="num_foreign_languages",
        label="Foreign languages offered",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.SCHOOL_DIRECTORY,
        unit="languages",
        description="Number of foreign languages offered (academic breadth proxy)",
    ),
    DimensionMeta(
        key="is_private",
        label="Is private school",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.SCHOOL_DIRECTORY,
        description="1 if private/independent, 0 if public/state",
    ),
    DimensionMeta(
        key="sozialindex",
        label="Sozialindex",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="Social index (Hamburg/NRW: 1=affluent, 6=disadvantaged)",
    ),
)

# --- POI counts from enrichment ---
_register(
    DimensionMeta(
        key="poi_restaurant_count",
        label="Restaurants nearby",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="restaurants",
        description="Restaurants within 500m (neighborhood vitality)",
    ),
    DimensionMeta(
        key="poi_kita_count",
        label="Kitas nearby",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.GOOGLE_NEARBY,
        unit="kitas",
        description="Kitas/daycares within 500m (family infrastructure)",
    ),
    DimensionMeta(
        key="poi_school_count",
        label="Schools nearby",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.COMPUTED,
        unit="schools",
        description="Other schools within 500m (competition/density)",
    ),
)

# --- GISD socioeconomic deprivation (Robert Koch Institut, PLZ-level) ---
_register(
    DimensionMeta(
        key="gisd_score",
        label="Socioeconomic deprivation (GISD)",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="German Index of Socioeconomic Deprivation (0=least deprived, 1=most deprived). Composite of education, income, employment.",
    ),
    DimensionMeta(
        key="gisd_quintile",
        label="GISD quintile",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        description="Socioeconomic deprivation quintile (1=least deprived, 5=most deprived)",
    ),
)

# --- School competition / supply dimensions ---
_register(
    DimensionMeta(
        key="same_type_schools_2km",
        label="Same-type schools (2km)",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.COMPUTED,
        unit="schools",
        description="Number of same school-type schools within 2km radius",
    ),
    DimensionMeta(
        key="population_per_school",
        label="Population per school",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.COMPUTED,
        unit="people/school",
        description="Catchment population divided by same-type schools in 2km. Higher = more demand pressure per school.",
    ),
    DimensionMeta(
        key="school_supply_ratio",
        label="School supply ratio",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.COMPUTED,
        unit="schools/10k people",
        description="Same-type schools per 10k catchment population. Higher = more choice, less overfill pressure.",
    ),
    DimensionMeta(
        key="nearest_same_type_m",
        label="Nearest same-type school",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.COMPUTED,
        unit="m",
        description="Distance in meters to the closest school of the same type",
    ),
)

# --- Zensus 2022 grid-based catchment demographics ---
_register(
    DimensionMeta(
        key="catchment_avg_age",
        label="Avg age in catchment",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="years",
        description="Average age of residents within 1km catchment (Zensus 2022 100m grid)",
    ),
    DimensionMeta(
        key="catchment_foreigner_pct",
        label="Foreigner % in catchment",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="%",
        description="Percentage of foreign nationals within 1km catchment (Zensus 2022 100m grid)",
    ),
    DimensionMeta(
        key="catchment_avg_rent",
        label="Avg rent in catchment",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="EUR/m²",
        description="Average rent per sqm within 1km catchment (Zensus 2022 100m grid)",
    ),
    DimensionMeta(
        key="catchment_vacancy_rate",
        label="Vacancy rate in catchment",
        direction=Direction.LOWER_BETTER,
        source=DimensionSource.ZENSUS,
        unit="%",
        description="Housing vacancy rate within 1km catchment (Zensus 2022 100m grid)",
    ),
    DimensionMeta(
        key="catchment_population",
        label="Catchment population",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="people",
        description="Estimated population within 1km catchment (Zensus 2022 100m grid)",
    ),
    DimensionMeta(
        key="catchment_population_density",
        label="Population density in catchment",
        direction=Direction.INFORMATIONAL,
        source=DimensionSource.ZENSUS,
        unit="per km²",
        description="Population density within 1km catchment (Zensus 2022 100m grid)",
    ),
)

# --- Transit enrichment fields from actual data ---
_register(
    DimensionMeta(
        key="transit_stop_count",
        label="Transit stops (1km)",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        unit="stops",
        description="Total transit stops within 1000m",
    ),
    DimensionMeta(
        key="transit_lines_count",
        label="Transit lines (1km)",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.LOCAL_DATA,
        unit="lines",
        description="Total unique transit lines within 1000m",
    ),
    DimensionMeta(
        key="transit_accessibility",
        label="Transit accessibility score",
        direction=Direction.HIGHER_BETTER,
        source=DimensionSource.COMPUTED,
        description="Composite transit accessibility score",
    ),
)


# --- Convenience accessors ---

def get_scorable_dimensions() -> List[DimensionMeta]:
    """Return dimensions that can be used in scoring (not informational)."""
    return [d for d in DIMENSIONS.values() if d.direction != Direction.INFORMATIONAL]


def get_numeric_dimensions() -> List[DimensionMeta]:
    """Return all numeric dimensions (excludes categorical like school_type)."""
    categorical = {"school_type", "public_private"}
    return [d for d in DIMENSIONS.values() if d.key not in categorical]


def default_weights() -> Dict[str, float]:
    """Return equal weights for all scorable dimensions."""
    scorable = get_scorable_dimensions()
    w = 1.0 / len(scorable) if scorable else 1.0
    return {d.key: w for d in scorable}
