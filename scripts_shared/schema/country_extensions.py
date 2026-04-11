"""
Country-specific schema extensions — raw academic and regulatory columns
that are unique to each country's education system.

These columns are appended AFTER the core schema columns. Each country
gets its own set of raw exam/assessment columns alongside the normalized
academic_performance_score in the core schema.

Usage:
    from scripts_shared.schema.country_extensions import get_country_extension
    ext_cols = get_country_extension("NL")
"""

# =============================================================================
# GERMANY (DE) — Berlin reference columns that are Germany-specific
# These exist in the Berlin 265-col schema but have no international equivalent
# =============================================================================
DE_EXTENSION = [
    # Abitur (university entrance exam)
    "abitur_durchschnitt_2024",
    "abitur_durchschnitt_2023",
    "abitur_durchschnitt_2025",
    "abitur_erfolgsquote_2024",
    "abitur_erfolgsquote_2025",
    # MSA (Mittlerer Schulabschluss — year 10 exit exam)
    "msa_notendurchschnitt_bezirk_2024",
    "msa_bestehensquote_bezirk_2024",
    "msa_plus_quote_bezirk_2024",
    "msa_teilnehmer_bezirk_2024",
    "msa_notendurchschnitt_bezirk_2023",
    "msa_bestehensquote_bezirk_2023",
    "msa_plus_quote_bezirk_2023",
    "msa_teilnehmer_bezirk_2023",
    "leistungsdaten_quelle",
    "notendurchschnitt_unified_2024",
    "notendurchschnitt_unified_2023",
    # Abitur predictions (regression model)
    "abitur_durchschnitt_estimated",
    "abitur_durchschnitt_estimated_lower",
    "abitur_durchschnitt_estimated_upper",
    "abitur_erfolgsquote_estimated",
    "abitur_erfolgsquote_estimated_lower",
    "abitur_erfolgsquote_estimated_upper",
    "abitur_prediction_confidence",
    "abitur_prediction_drivers",
    "abitur_durchschnitt_estimated_rebased",
    "abitur_durchschnitt_estimated_rebased_lower",
    "abitur_durchschnitt_estimated_rebased_upper",
    "abitur_rebase_shift",
    "abitur_state_avg_official",
    # Berlin-specific traffic (PLZ-level sensor data)
    "plz_avg_cars_per_hour",
    "plz_std_cars_per_hour",
    "plz_observation_count",
    "plz_avg_bikes_per_hour",
    "plz_avg_pedestrians_per_hour",
    "plz_avg_heavy_vehicles_per_hour",
    "plz_avg_v85_speed",
    "plz_sensor_count",
    "plz_bike_friendliness",
    "plz_pedestrian_ratio",
    "plz_heavy_vehicle_ratio",
    "plz_speed_safe_zone",
    "plz_traffic_intensity",
    "plz_interpolated",
    "plz_neighbor_count",
    "plz_avg_neighbor_distance_km",
    # Berlin-specific crime breakdown (38 detailed columns)
    "crime_total_crimes_2023",
    "crime_robbery_2023",
    "crime_street_robbery_2023",
    "crime_assault_2023",
    "crime_aggravated_assault_2023",
    "crime_threats_coercion_2023",
    "crime_bike_theft_2023",
    "crime_drug_offenses_2023",
    "crime_neighborhood_crimes_2023",
    "crime_total_crimes_2024",
    "crime_robbery_2024",
    "crime_street_robbery_2024",
    "crime_assault_2024",
    "crime_aggravated_assault_2024",
    "crime_threats_coercion_2024",
    "crime_bike_theft_2024",
    "crime_drug_offenses_2024",
    "crime_neighborhood_crimes_2024",
    "crime_total_crimes_avg",
    "crime_robbery_avg",
    "crime_street_robbery_avg",
    "crime_assault_avg",
    "crime_aggravated_assault_avg",
    "crime_threats_coercion_avg",
    "crime_bike_theft_avg",
    "crime_drug_offenses_avg",
    "crime_neighborhood_crimes_avg",
    "crime_total_crimes_yoy_pct",
    "crime_robbery_yoy_pct",
    "crime_street_robbery_yoy_pct",
    "crime_assault_yoy_pct",
    "crime_aggravated_assault_yoy_pct",
    "crime_threats_coercion_yoy_pct",
    "crime_bike_theft_yoy_pct",
    "crime_drug_offenses_yoy_pct",
    "crime_neighborhood_crimes_yoy_pct",
    "crime_violent_crime_avg",
    # Demand (Berlin-specific naming)
    "nachfrage_plaetze_2024_25",
    "nachfrage_wuensche_2024_25",
    # Migration previous year
    "migration_2023_24",
]

# =============================================================================
# NETHERLANDS (NL) — DUO exam data + Inspectorate ratings
# =============================================================================
NL_EXTENSION = [
    # Eindexamen (final exams) — per school, per track
    "nl_exam_pass_rate",              # Slagingspercentage
    "nl_exam_avg_grade_ce",           # Centraal Examen gemiddelde (standardized part)
    "nl_exam_avg_grade_se",           # Schoolexamen gemiddelde (school-assessed part)
    "nl_exam_ce_se_difference",       # CE-SE verschil (indicator of grade inflation)
    "nl_exam_year",                   # Year of exam data
    "nl_school_track",                # vmbo-b/vmbo-k/vmbo-gt/havo/vwo
    # Inspectorate
    "nl_inspectorate_rating",         # voldoende / onvoldoende / zeer zwak / excellent
    "nl_inspectorate_year",           # Year of last inspection
    # DUO denomination
    "nl_denomination",                # Openbaar / RK / PC / Interconfessioneel / etc.
    "nl_school_board",                # Schoolbestuur name
    "nl_brin_code",                   # Original BRIN identifier
    # CBS neighbourhood stats (pre-computed by CBS)
    "nl_buurt_code",                  # CBS buurtcode for joins
    "nl_wijk_code",                   # CBS wijkcode for joins
    "nl_gemeente_code",               # CBS gemeentecode
]

# =============================================================================
# UK / ENGLAND (GB) — DfE + Ofsted data
# =============================================================================
GB_EXTENSION = [
    # GCSE (Key Stage 4) — age 16
    "gb_ks4_attainment8",             # Attainment 8 score
    "gb_ks4_progress8",               # Progress 8 score (value-added)
    "gb_ks4_progress8_lower_ci",      # Confidence interval
    "gb_ks4_progress8_upper_ci",
    "gb_ks4_pct_grade5_en_ma",        # % achieving grade 5+ in English & Maths
    "gb_ks4_ebacc_avg_point",         # EBacc average point score
    "gb_ks4_year",                    # Academic year
    # A-Level (Key Stage 5) — age 18
    "gb_ks5_avg_point_score",         # Average A-level point score
    "gb_ks5_avg_grade",               # Average grade (A*-E scale)
    "gb_ks5_value_added",             # KS5 value added score
    "gb_ks5_year",
    # Ofsted
    "gb_ofsted_overall",              # Outstanding/Good/RI/Inadequate (historical)
    "gb_ofsted_date",                 # Date of last inspection
    "gb_ofsted_report_url",           # Link to full report
    # School identifiers
    "gb_urn",                         # Unique Reference Number
    "gb_establishment_type",          # Academy/Maintained/Free School/Independent
    "gb_phase",                       # Primary/Secondary/All-through/16+
    "gb_local_authority",             # LA name
    "gb_trust_name",                  # Multi-academy trust name (if applicable)
    # Deprivation
    "gb_imd_decile",                  # Index of Multiple Deprivation decile (1=most deprived)
    "gb_fsm_pct",                     # % eligible for Free School Meals
    "gb_lsoa_code",                   # Lower Super Output Area code for joins
]

# =============================================================================
# FRANCE (FR) — IVAL/IVAC baccalauréat + brevet data
# =============================================================================
FR_EXTENSION = [
    # Baccalauréat (lycée — age 18)
    "fr_bac_pass_rate",               # Taux de réussite brut
    "fr_bac_pass_rate_expected",      # Taux de réussite attendu (value-added baseline)
    "fr_bac_value_added",             # Valeur ajoutée (actual - expected)
    "fr_bac_mention_rate",            # % with honors (mention)
    "fr_bac_access_rate",             # Taux d'accès de la seconde au bac
    "fr_bac_year",
    # Brevet (collège — age 15)
    "fr_brevet_pass_rate",            # DNB taux de réussite
    "fr_brevet_mention_rate",
    "fr_brevet_year",
    # School identifiers
    "fr_uai_code",                    # Unité Administrative Immatriculée
    "fr_academie",                    # Académie (regional education authority)
    "fr_education_priority",          # REP / REP+ (priority education zone)
    "fr_commune_code",                # INSEE commune code for joins
    "fr_departement",                 # Département number
    "fr_sector",                      # Public / Privé sous contrat / Privé hors contrat
]

# =============================================================================
# ITALY (IT) — INVALSI + MIM data
# =============================================================================
IT_EXTENSION = [
    # INVALSI (national assessment — aggregate only, not per-school)
    "it_invalsi_math_avg_province",   # Provincial average math score
    "it_invalsi_ital_avg_province",   # Provincial average Italian score
    "it_invalsi_eng_avg_province",    # Provincial average English score
    "it_invalsi_year",
    # School identifiers
    "it_codice_meccanografico",       # National school code
    "it_tipo_istituto",              # Liceo/Tecnico/Professionale/Comprensivo
    "it_indirizzo_studio",           # Study track (Classico, Scientifico, etc.)
    "it_provincia",                   # Province code
    "it_comune_code",                 # ISTAT commune code
    "it_gestione",                    # Statale / Paritaria
    # Note: Per-school academic performance not available as open data
]

# =============================================================================
# SPAIN (ES) — Limited academic data, strong demographics
# =============================================================================
ES_EXTENSION = [
    # No per-school academic data publicly available
    # Regional education system metadata
    "es_comunidad_autonoma",          # Autonomous community
    "es_provincia",                   # Province
    "es_municipio_code",              # INE municipality code
    "es_codigo_centro",               # National school code
    "es_naturaleza",                  # Público / Concertado / Privado
    "es_tipo_centro",                 # CEIP / IES / Centro Concertado / etc.
    "es_ensenanzas",                  # Educational offerings (ESO, Bachillerato, FP, etc.)
    # INE demographics (census-tract level)
    "es_seccion_censal",              # Census tract code for joins
    "es_renta_media_hogar",           # Average household income (INE Atlas)
]

# =============================================================================
# REGISTRY
# =============================================================================
COUNTRY_EXTENSIONS = {
    "DE": DE_EXTENSION,
    "NL": NL_EXTENSION,
    "GB": GB_EXTENSION,
    "FR": FR_EXTENSION,
    "IT": IT_EXTENSION,
    "ES": ES_EXTENSION,
}

COUNTRY_NAMES = {
    "DE": "Germany",
    "NL": "Netherlands",
    "GB": "United Kingdom",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
}

COUNTRY_LANGUAGES = {
    "DE": "de",
    "NL": "nl",
    "GB": "en",
    "FR": "fr",
    "IT": "it",
    "ES": "es",
}

COUNTRY_CURRENCIES = {
    "DE": "EUR",
    "NL": "EUR",
    "GB": "GBP",
    "FR": "EUR",
    "IT": "EUR",
    "ES": "EUR",
}


def get_country_extension(country_code: str) -> list[str]:
    """Get the extension columns for a country."""
    code = country_code.upper()
    if code not in COUNTRY_EXTENSIONS:
        raise ValueError(f"Unknown country code: {code}. Available: {list(COUNTRY_EXTENSIONS.keys())}")
    return COUNTRY_EXTENSIONS[code]


def get_full_schema(country_code: str) -> list[str]:
    """Get core + extension columns for a country."""
    from scripts_shared.schema.core_schema import CORE_COLUMNS
    return CORE_COLUMNS + get_country_extension(country_code)


def get_full_dataframe(country_code: str):
    """Return an empty DataFrame with core + country extension columns."""
    import pandas as pd
    return pd.DataFrame(columns=get_full_schema(country_code))
