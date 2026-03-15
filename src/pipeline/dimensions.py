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
