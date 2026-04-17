"""
SchoolNossa Core Schema — Universal columns shared across ALL countries.

This is the international superset schema. It contains:
1. CORE columns: universal across all countries (identity, location, enrichment)
2. NORMALIZED columns: cross-country comparable metrics (academic_performance_score, etc.)
3. COUNTRY METADATA: country_code, language, currency, etc.

German pipelines continue to use the Berlin 265-column schema directly.
International pipelines produce this core schema + country-specific extensions.

Usage:
    from scripts_shared.schema.core_schema import CORE_COLUMNS, get_core_dataframe
"""

from collections import OrderedDict

# =============================================================================
# COUNTRY METADATA (added to every international record)
# =============================================================================
COUNTRY_META_COLUMNS = [
    "country_code",          # ISO 3166-1 alpha-2 (NL, GB, FR, IT, ES, DE)
    "country_name",          # Full English name
    "city",                  # City name (or region for national datasets)
    "language",              # Primary language code (nl, en, fr, it, es, de)
    "currency",              # ISO 4217 (EUR, GBP)
    "data_source_version",   # Date of source data download (YYYY-MM-DD)
]

# =============================================================================
# SCHOOL IDENTITY — maps to Berlin cols 0-13
# =============================================================================
IDENTITY_COLUMNS = [
    "school_id",             # Canonical ID (maps to schulnummer for DE, URN for UK, BRIN for NL, etc.)
    "school_id_national",    # Original national ID code as-is
    "school_name",           # Maps to schulname
    "school_type",           # Normalized: primary, secondary, combined, special
    "school_type_national",  # Original national type (Gymnasium, Academy, Lycée, etc.)
    "school_subtype",        # Maps to schulart — finer categorization
    "ownership",             # Maps to traegerschaft — normalized: public, private, charter/concertado
    "ownership_national",    # Original national ownership category
    "founding_year",         # Maps to gruendungsjahr
    "street_address",        # Maps to strasse
    "postal_code",           # Maps to plz
    "district",              # Maps to ortsteil — local neighborhood/district
    "region",                # Maps to bezirk — larger administrative area
    "phone",                 # Maps to telefon
    "email",                 # Maps to email
    "website",               # Maps to website
    "principal",             # Maps to leitung — head of school
    "languages_offered",     # Maps to sprachen
    "special_features",      # Maps to besonderheiten
    "metadata_source",       # Maps to metadata_source
]

# =============================================================================
# GEOSPATIAL
# =============================================================================
GEO_COLUMNS = [
    "latitude",
    "longitude",
]

# =============================================================================
# STUDENT / TEACHER STATISTICS — maps to Berlin cols 14-19
# =============================================================================
STUDENT_TEACHER_COLUMNS = [
    "students_current",      # Maps to schueler_2024_25 (most recent year)
    "students_previous",     # Maps to schueler_2023_24
    "students_2yr_ago",      # Maps to schueler_2022_23
    "teachers_current",      # Maps to lehrer_2024_25
    "teachers_previous",     # Maps to lehrer_2023_24
    "teachers_2yr_ago",      # Maps to lehrer_2022_23
    "student_teacher_ratio", # Computed: students_current / teachers_current
]

# =============================================================================
# NORMALIZED ACADEMIC PERFORMANCE (cross-country comparable)
# =============================================================================
NORMALIZED_ACADEMIC_COLUMNS = [
    "academic_performance_score",       # 0-100 normalized score (from country exam data)
    "academic_performance_percentile",  # National percentile rank (0-100)
    "academic_performance_trend",       # YoY change in score
    "academic_value_added",             # How much school improves outcomes vs expected (if available)
    "academic_data_source",             # What exam/metric this was derived from
    "academic_data_year",               # Year of the academic data
    "school_quality_rating",            # Normalized: excellent/good/adequate/inadequate
    "school_quality_rating_national",   # Original rating (Ofsted Outstanding, Inspectorate voldoende, etc.)
]

# =============================================================================
# SOCIOECONOMIC / DEMOGRAPHICS (area-level)
# =============================================================================
SOCIOECONOMIC_COLUMNS = [
    "deprivation_index",         # Maps to belastungsstufe — normalized 1-10 scale
    "deprivation_index_national", # Original national index (IMD decile, CBS social index, etc.)
    "migration_background_pct",  # Maps to migration_2024_25
    "area_median_income",        # Mapped from census/statistics (EUR or local currency)
    "area_population_density",   # People per km²
    "area_unemployment_rate",    # Percent
]

# =============================================================================
# TRAFFIC / ROAD SAFETY — maps to Berlin cols 49-64
# =============================================================================
TRAFFIC_COLUMNS = [
    "traffic_accidents_500m",        # Accident count within 500m radius
    "traffic_accidents_1000m",       # Accident count within 1000m radius
    "traffic_accidents_fatal_1000m", # Fatal accidents within 1000m
    "traffic_accidents_year",        # Year of accident data
    "traffic_volume_index",          # Normalized 0-10 traffic intensity (from sensors or modeled)
    "traffic_speed_zone_kmh",        # Speed limit or measured speed near school
    "traffic_bike_friendly",         # Boolean or score — bike infrastructure nearby
    "traffic_pedestrian_safe",       # Boolean or score — pedestrian safety indicators
    "traffic_data_source",           # Source identifier
]

# =============================================================================
# CRIME — simplified from Berlin's 38-column breakdown
# =============================================================================
CRIME_COLUMNS = [
    "crime_total_per_1000",          # Total crime rate per 1000 residents in area
    "crime_violent_per_1000",        # Violent crime rate
    "crime_property_per_1000",       # Property crime rate (theft, burglary, etc.)
    "crime_drug_per_1000",           # Drug offense rate
    "crime_total_per_1000_prev",     # Previous year for trend
    "crime_yoy_change_pct",          # Year-over-year change
    "crime_safety_rank",             # Rank within dataset (1 = safest)
    "crime_safety_category",         # safe / moderate / high
    "crime_data_year",               # Year of crime data
    "crime_area_name",               # Name of the area the crime data covers
    "crime_data_source",             # Source identifier
]

# =============================================================================
# TRANSIT — maps to Berlin's 48-column transit block
# Same structure: 3 transport types × 3 nearest stops × 5 fields + aggregates
# =============================================================================

def _transit_columns():
    """Generate transit columns following Berlin pattern."""
    cols = []
    for mode in ["rail", "tram", "bus"]:
        for idx in ["01", "02", "03"]:
            for field in ["name", "distance_m", "latitude", "longitude", "lines"]:
                cols.append(f"transit_{mode}_{idx}_{field}")
    cols.extend([
        "transit_stop_count_1000m",
        "transit_all_lines_1000m",
        "transit_accessibility_score",
    ])
    return cols

TRANSIT_COLUMNS = _transit_columns()

# =============================================================================
# POI — maps to Berlin's 81-column POI block
# Same structure: 6 categories × (count + 3 nearest × 5 fields) + secondary count
# =============================================================================

def _poi_columns():
    """Generate POI columns following Berlin pattern."""
    cols = []
    categories = [
        "supermarket", "restaurant", "bakery_cafe",
        "kita", "primary_school", "secondary_school",
    ]
    for cat in categories[:-1]:  # All except secondary_school have full detail
        cols.append(f"poi_{cat}_count_500m")
        for idx in ["01", "02", "03"]:
            for field in ["name", "address", "distance_m", "latitude", "longitude"]:
                cols.append(f"poi_{cat}_{idx}_{field}")
    cols.append("poi_secondary_school_count_500m")
    return cols

POI_COLUMNS = _poi_columns()

# =============================================================================
# TUITION — maps to Berlin cols 234-244
# =============================================================================
TUITION_COLUMNS = [
    "tuition_monthly_eur",
    "tuition_annual_eur",
    "registration_fee_eur",
    "material_fee_annual_eur",
    "meal_plan_monthly_eur",
    "after_school_care_monthly_eur",
    "scholarship_available",
    "income_based_tuition",
    "tuition_notes",
    "tuition_source_url",
    "tuition_display",
]

# =============================================================================
# DESCRIPTIONS & EMBEDDINGS — maps to Berlin cols 233, 245-251
# =============================================================================
CONTENT_COLUMNS = [
    "description",           # English description (generated)
    "description_local",     # Description in local language (maps to description_de for DE)
    "summary_en",
    "summary_local",         # Maps to summary_de for DE
    "embedding",
    "most_similar_school_01",
    "most_similar_school_02",
    "most_similar_school_03",
]

# =============================================================================
# DEMAND / ENROLLMENT PRESSURE (optional — not all countries publish this)
# =============================================================================
DEMAND_COLUMNS = [
    "enrollment_capacity",       # Maps to nachfrage_plaetze_2025_26
    "enrollment_applications",   # Maps to nachfrage_wuensche_2025_26
    "enrollment_pressure_pct",   # Maps to nachfrage_prozent_2025_26
]

# =============================================================================
# FULL CORE SCHEMA
# =============================================================================
CORE_COLUMNS = (
    COUNTRY_META_COLUMNS
    + IDENTITY_COLUMNS
    + GEO_COLUMNS
    + STUDENT_TEACHER_COLUMNS
    + NORMALIZED_ACADEMIC_COLUMNS
    + SOCIOECONOMIC_COLUMNS
    + TRAFFIC_COLUMNS
    + CRIME_COLUMNS
    + TRANSIT_COLUMNS
    + POI_COLUMNS
    + TUITION_COLUMNS
    + CONTENT_COLUMNS
    + DEMAND_COLUMNS
)

# Column group boundaries for easy slicing
COLUMN_GROUPS = OrderedDict([
    ("country_meta", COUNTRY_META_COLUMNS),
    ("identity", IDENTITY_COLUMNS),
    ("geo", GEO_COLUMNS),
    ("student_teacher", STUDENT_TEACHER_COLUMNS),
    ("academic_normalized", NORMALIZED_ACADEMIC_COLUMNS),
    ("socioeconomic", SOCIOECONOMIC_COLUMNS),
    ("traffic", TRAFFIC_COLUMNS),
    ("crime", CRIME_COLUMNS),
    ("transit", TRANSIT_COLUMNS),
    ("poi", POI_COLUMNS),
    ("tuition", TUITION_COLUMNS),
    ("content", CONTENT_COLUMNS),
    ("demand", DEMAND_COLUMNS),
])


def get_core_dataframe():
    """Return an empty DataFrame with the full core schema."""
    import pandas as pd
    return pd.DataFrame(columns=CORE_COLUMNS)


def validate_core_schema(df):
    """
    Validate a DataFrame against the core schema.
    Returns (is_valid, missing_cols, extra_cols).
    """
    expected = set(CORE_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected
    return len(missing) == 0, missing, extra


# Canonical German school types accepted by the filter UI.
# school_type must always be one of these — never generic "secondary"/"primary".
CANONICAL_DE_SCHOOL_TYPES = {
    # Secondary
    "Gymnasium", "Gesamtschule", "Stadtteilschule", "Realschule",
    "Gemeinschaftsschule", "Waldorfschule", "Internationale Schule",
    "ISS-Gymnasium", "Grund- und Werkrealschule", "Werkrealschule",
    "Berufsoberschulen", "Fachoberschulen", "Wirtschaftsschulen",
    "Oberschule", "Hauptschule", "Mittelschulen",
    # Gesamtschule variants
    "Integrierte Gesamtschule (IGS)", "Kooperative Gesamtschule (KGS)",
    "Integrierte Sekundarschule",
    # Combined / special
    "Stadtteilschule_Gymnasium",
    # Primary
    "Grundschule",
    # Förderschule / SBBZ
    "Förderschule", "Förderzentrum", "Förderzentren",
    # Other
    "Berufliche Schule", "Berufsbildende Schule",
    "Sonstige",
}

# Generic placeholders that should NEVER appear as school_type.
GENERIC_SCHOOL_TYPE_PLACEHOLDERS = {"secondary", "primary", "Weiterführende Schule", ""}


def validate_school_types(df, city: str = "unknown", strict: bool = False):
    """
    Guard: fail-fast if any row has a generic/missing school_type.

    Args:
        df: DataFrame with a 'school_type' column
        city: city name for error messages
        strict: if True, raise AssertionError; if False, log warnings

    Returns:
        DataFrame of bad rows (empty if all valid)
    """
    import logging
    logger = logging.getLogger(__name__)

    if "school_type" not in df.columns:
        msg = f"[{city}] school_type column missing entirely"
        if strict:
            raise AssertionError(msg)
        logger.warning(msg)
        return df.head(0)

    bad = df[
        df["school_type"].isin(GENERIC_SCHOOL_TYPE_PLACEHOLDERS)
        | df["school_type"].isna()
    ]

    if not bad.empty:
        sample_vals = bad["school_type"].value_counts().to_dict()
        msg = (
            f"[{city}] {len(bad)} rows have generic/missing school_type: {sample_vals}. "
            f"school_type must be a specific German school type (Gymnasium, Realschule, etc.), "
            f"never 'secondary'/'primary'/''."
        )
        if strict:
            raise AssertionError(msg)
        logger.warning(msg)

    return bad


def schema_coverage_report(df):
    """Print a coverage report showing populated columns per group."""
    total = len(df)
    if total == 0:
        print("Empty DataFrame — no rows to report on.")
        return

    print(f"\nSchema Coverage Report ({total} schools)")
    print("=" * 60)

    for group_name, group_cols in COLUMN_GROUPS.items():
        present = [c for c in group_cols if c in df.columns]
        populated = sum(1 for c in present if df[c].notna().any())
        print(f"\n  {group_name} ({len(present)}/{len(group_cols)} columns present, {populated} with data):")
        for col in group_cols:
            if col in df.columns:
                non_null = df[col].notna().sum()
                pct = non_null / total * 100
                status = "+" if pct > 50 else "~" if pct > 0 else "-"
                print(f"    {status} {col}: {non_null}/{total} ({pct:.0f}%)")
            else:
                print(f"    X {col}: MISSING")


# =============================================================================
# BERLIN SCHEMA MAPPING — maps core columns to Berlin's 265-column schema
# This enables converting international data to Berlin format for the frontend
# =============================================================================
CORE_TO_BERLIN_MAP = {
    # Identity
    "school_id": "schulnummer",
    "school_name": "schulname",
    "school_type": "school_type",
    "school_subtype": "schulart",
    "ownership": "traegerschaft",
    "founding_year": "gruendungsjahr",
    "street_address": "strasse",
    "postal_code": "plz",
    "district": "ortsteil",
    "region": "bezirk",
    "phone": "telefon",
    "email": "email",
    "website": "website",
    "principal": "leitung",
    "languages_offered": "sprachen",
    "special_features": "besonderheiten",
    "metadata_source": "metadata_source",
    # Geo
    "latitude": "latitude",
    "longitude": "longitude",
    # Student/Teacher
    "students_current": "schueler_2024_25",
    "students_previous": "schueler_2023_24",
    "students_2yr_ago": "schueler_2022_23",
    "teachers_current": "lehrer_2024_25",
    "teachers_previous": "lehrer_2023_24",
    "teachers_2yr_ago": "lehrer_2022_23",
    # Socioeconomic
    "deprivation_index": "belastungsstufe",
    "migration_background_pct": "migration_2024_25",
    # Crime
    "crime_safety_rank": "crime_safety_rank",
    "crime_safety_category": "crime_safety_category",
    # Transit — direct 1:1 mapping (same column names)
    **{col: col for col in _transit_columns()},
    # POI — direct 1:1 mapping (same column names)
    **{col: col for col in _poi_columns()},
    # Tuition — direct 1:1 mapping
    "tuition_monthly_eur": "tuition_monthly_eur",
    "tuition_annual_eur": "tuition_annual_eur",
    "registration_fee_eur": "registration_fee_eur",
    "material_fee_annual_eur": "material_fee_annual_eur",
    "meal_plan_monthly_eur": "meal_plan_monthly_eur",
    "after_school_care_monthly_eur": "after_school_care_monthly_eur",
    "scholarship_available": "scholarship_available",
    "income_based_tuition": "income_based_tuition",
    "tuition_notes": "tuition_notes",
    "tuition_source_url": "tuition_source_url",
    "tuition_display": "tuition_display",
    # Content
    "description": "description",
    "description_local": "description_de",
    "summary_en": "summary_en",
    "summary_local": "summary_de",
    "embedding": "embedding",
    "most_similar_school_01": "most_similar_school_no_01",
    "most_similar_school_02": "most_similar_school_no_02",
    "most_similar_school_03": "most_similar_school_no_03",
    # Demand
    "enrollment_capacity": "nachfrage_plaetze_2025_26",
    "enrollment_applications": "nachfrage_wuensche_2025_26",
    "enrollment_pressure_pct": "nachfrage_prozent_2025_26",
}
