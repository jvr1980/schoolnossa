"""
Country configuration registry — data sources, API endpoints, and pipeline
metadata for each international country.

Usage:
    from scripts_international.country_config import get_country_config
    config = get_country_config("NL")
    print(config.school_data_url)
"""

from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


@dataclass
class DataSource:
    """Metadata for a single data source."""
    name: str
    url: str
    format: str              # csv, json, api, gtfs, geojson, odata, etc.
    coverage: str            # national, city-level, regional, etc.
    access: str              # open, registration, api_key, scrape
    notes: str = ""


@dataclass
class CountryConfig:
    """Full configuration for a country pipeline."""
    country_code: str        # ISO 3166-1 alpha-2
    country_name: str
    language: str
    currency: str

    # Directory paths
    scripts_dir: Path = field(init=False)
    data_dir: Path = field(init=False)

    # Data sources by category
    school_registry: DataSource = None
    student_teacher_stats: DataSource = None
    academic_performance: DataSource = None
    traffic_safety: DataSource = None
    crime: DataSource = None
    transit: DataSource = None
    demographics: DataSource = None
    poi: DataSource = None   # Usually Google Places (universal)

    # Pipeline settings
    geocoding_needed: bool = False  # True if school registry lacks lat/lon
    school_id_field: str = ""       # Column name in source data for school ID
    school_types: list = field(default_factory=list)  # Secondary school types to include

    def __post_init__(self):
        code = self.country_code.lower()
        self.scripts_dir = PROJECT_ROOT / "scripts_international" / code
        self.data_dir = PROJECT_ROOT / f"data_{code}"


# =============================================================================
# NETHERLANDS
# =============================================================================
NL_CONFIG = CountryConfig(
    country_code="NL",
    country_name="Netherlands",
    language="nl",
    currency="EUR",
    geocoding_needed=True,  # DUO has addresses but no coordinates
    school_id_field="BRIN_NUMMER",
    school_types=["vmbo", "havo", "vwo", "vmbo-t", "mavo"],
    school_registry=DataSource(
        name="DUO Open Onderwijsdata — VO Adressen",
        url="https://duo.nl/open_onderwijsdata/voortgezet-onderwijs/adressen/vestigingen.jsp",
        format="csv",
        coverage="national",
        access="open",
        notes="BRIN+vestigingsnummer as unique ID. Geocode via BAG register.",
    ),
    student_teacher_stats=DataSource(
        name="DUO — Leerlingen & Personeel VO",
        url="https://duo.nl/open_onderwijsdata/voortgezet-onderwijs/",
        format="csv",
        coverage="national, per-school",
        access="open",
        notes="Enrollment per location per grade. Staff in FTE per institution.",
    ),
    academic_performance=DataSource(
        name="DUO Exam Monitor + Onderwijsinspectie",
        url="https://data.overheid.nl/dataset/03_voex-v1",
        format="csv",
        coverage="national, per-school",
        access="open",
        notes="Pass rates, CE/SE avg grades per school per track. Inspectorate ratings separate.",
    ),
    traffic_safety=DataSource(
        name="BRON Accident Register (Rijkswaterstaat)",
        url="https://data.overheid.nl/en/dataset/9841-verkeersongevallen---bestand-geregistreerde-ongevallen-nederland",
        format="geopackage/csv",
        coverage="national, geocoded",
        access="open",
        notes="Police-registered accidents with coordinates. Coverage gaps on minor roads.",
    ),
    crime=DataSource(
        name="CBS Misdrijven per wijk/buurt",
        url="https://opendata.cbs.nl/statline/#/CBS/nl/dataset/83648NED/table",
        format="odata/csv",
        coverage="national, buurt-level (neighborhood)",
        access="open",
        notes="Use cbsodata Python package. Crime by type at wijk/buurt level. Annual + monthly.",
    ),
    transit=DataSource(
        name="OVapi National GTFS",
        url="https://gtfs.ovapi.nl/",
        format="gtfs",
        coverage="national (all operators)",
        access="open",
        notes="52K+ stops, 2.7K+ routes. Standard GTFS — same processing as German cities.",
    ),
    demographics=DataSource(
        name="CBS Kerncijfers Wijken en Buurten",
        url="https://www.cbs.nl/nl-nl/dossier/nederland-regionaal/wijk-en-buurtstatistieken",
        format="odata/csv",
        coverage="national, buurt-level",
        access="open",
        notes="Population, income, housing, migration background at neighborhood level. "
              "Also CBS Nabijheidsstatistiek for pre-computed POI proximity.",
    ),
    poi=DataSource(
        name="CBS Nabijheidsstatistiek + Google Places",
        url="https://www.cbs.nl/nabijheidsstatistiek",
        format="odata",
        coverage="national, buurt-level (CBS) + point (Google)",
        access="open (CBS) + api_key (Google)",
        notes="CBS has pre-computed avg distance to nearest supermarket, school, GP, etc. "
              "Use for aggregate stats, Google Places for specific nearby POIs.",
    ),
)

# =============================================================================
# UK (ENGLAND)
# =============================================================================
GB_CONFIG = CountryConfig(
    country_code="GB",
    country_name="United Kingdom",
    language="en",
    currency="GBP",
    geocoding_needed=False,  # GIAS includes lat/lon
    school_id_field="URN",
    school_types=["secondary", "all-through", "16 plus"],
    school_registry=DataSource(
        name="Get Information about Schools (GIAS)",
        url="https://get-information-schools.service.gov.uk/",
        format="csv",
        coverage="England",
        access="open",
        notes="Bulk CSV download. Includes URN, coordinates, Ofsted rating, establishment type.",
    ),
    student_teacher_stats=DataSource(
        name="DfE Explore Education Statistics",
        url="https://explore-education-statistics.service.gov.uk/",
        format="csv/api",
        coverage="England, per-school",
        access="open",
        notes="School Workforce Census + School Census. Pupil-teacher ratios at school level.",
    ),
    academic_performance=DataSource(
        name="DfE Performance Tables (KS4/KS5)",
        url="https://explore-education-statistics.service.gov.uk/data-catalogue",
        format="csv/api",
        coverage="England, per-school",
        access="open",
        notes="Attainment 8, Progress 8, A-level APS. Note: Ofsted dropped single-word ratings Sep 2024.",
    ),
    traffic_safety=DataSource(
        name="STATS19 Road Casualty Data",
        url="https://www.gov.uk/government/statistical-data-sets/road-safety-open-data",
        format="csv",
        coverage="Great Britain, geocoded",
        access="open",
        notes="Police-reported injury collisions with lat/lon. Available from 1979. R package: stats19.",
    ),
    crime=DataSource(
        name="data.police.uk",
        url="https://data.police.uk/docs/",
        format="json api + csv bulk",
        coverage="England, Wales, NI (not Scotland)",
        access="open, no key",
        notes="Street-level crime with anonymized coordinates. Monthly updates. "
              "API: /api/crimes-street/all-crime?lat={lat}&lng={lng}",
    ),
    transit=DataSource(
        name="NaPTAN + BODS",
        url="https://beta-naptan.dft.gov.uk/download",
        format="csv (NaPTAN) + TransXChange (BODS)",
        coverage="Great Britain (NaPTAN) / England (BODS)",
        access="open (NaPTAN) / registration (BODS)",
        notes="NaPTAN: 400K+ stops with coordinates. BODS: timetables for service frequency.",
    ),
    demographics=DataSource(
        name="IMD 2025 + ONS Census 2021",
        url="https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025",
        format="csv/excel",
        coverage="England (IMD at LSOA level) / E+W (Census at LSOA level)",
        access="open",
        notes="7-domain deprivation index. Census: age, ethnicity, qualifications, economic activity.",
    ),
    poi=DataSource(
        name="Overpass API + Google Places",
        url="https://overpass-api.de/api/",
        format="json (Overpass) + api (Google)",
        coverage="UK (OSM) + point (Google)",
        access="open (Overpass) + api_key (Google)",
        notes="Overpass for free POI data. Google Places for detailed individual POIs.",
    ),
)

# =============================================================================
# FRANCE
# =============================================================================
FR_CONFIG = CountryConfig(
    country_code="FR",
    country_name="France",
    language="fr",
    currency="EUR",
    geocoding_needed=False,  # Annuaire includes coordinates
    school_id_field="identifiant_de_l_etablissement",
    school_types=["Lycée", "Collège"],
    school_registry=DataSource(
        name="Annuaire de l'Éducation nationale",
        url="https://www.data.gouv.fr/datasets/annuaire-de-leducation",
        format="csv + json api",
        coverage="national",
        access="open",
        notes="Single national CSV with coordinates. UAI code as school ID.",
    ),
    student_teacher_stats=DataSource(
        name="data.education.gouv.fr — Effectifs",
        url="https://data.education.gouv.fr/explore/",
        format="csv",
        coverage="national, per-school (public schools)",
        access="open",
        notes="Enrollment by school. Private school data more limited.",
    ),
    academic_performance=DataSource(
        name="IVAL (lycées) + IVAC (collèges)",
        url="https://data.education.gouv.fr/explore/dataset/fr-en-indicateurs-de-resultat-des-lycees-gt_v2/",
        format="csv",
        coverage="national, per-school",
        access="open",
        notes="Bac pass rate, value-added, mention rate. Brevet results for collèges.",
    ),
    traffic_safety=DataSource(
        name="ONISR Accidents Corporels",
        url="https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024",
        format="csv",
        coverage="national, geocoded",
        access="open",
        notes="4 CSV files per year: characteristics, locations, vehicles, users. 2005-2024.",
    ),
    crime=DataSource(
        name="SSMSI Délinquance Enregistrée",
        url="https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales",
        format="csv",
        coverage="national, commune-level",
        access="open",
        notes="14 crime categories at commune level since 2016.",
    ),
    transit=DataSource(
        name="transport.data.gouv.fr",
        url="https://transport.data.gouv.fr/datasets?type=public-transit",
        format="gtfs",
        coverage="national (all networks)",
        access="open",
        notes="National GTFS aggregator. Consolidated stop map available.",
    ),
    demographics=DataSource(
        name="INSEE FILOSOFI + Commune data",
        url="https://www.insee.fr/en/statistiques/6457611",
        format="csv/excel/parquet",
        coverage="national, commune + IRIS level",
        access="open",
        notes="IRIS-level household income for cities >10K pop. Commune-level for everything else.",
    ),
    poi=DataSource(
        name="Overpass API + Google Places",
        url="https://overpass-api.de/api/",
        format="json + api",
        coverage="France (OSM) + point (Google)",
        access="open + api_key",
        notes="Standard Overpass + Google approach.",
    ),
)

# =============================================================================
# ITALY
# =============================================================================
IT_CONFIG = CountryConfig(
    country_code="IT",
    country_name="Italy",
    language="it",
    currency="EUR",
    geocoding_needed=True,  # dati.istruzione.it has addresses but no coordinates
    school_id_field="CODICESCUOLA",
    school_types=["LICEO", "ISTITUTO TECNICO", "ISTITUTO PROFESSIONALE"],
    school_registry=DataSource(
        name="dati.istruzione.it — Anagrafe Scuole",
        url="https://dati.istruzione.it/opendata/opendata/",
        format="csv",
        coverage="national",
        access="open",
        notes="Codice meccanografico as school ID. All state + paritarie schools. Needs geocoding.",
    ),
    student_teacher_stats=DataSource(
        name="dati.istruzione.it — Alunni & Personale",
        url="https://dati.istruzione.it/opendata/opendata/",
        format="csv",
        coverage="national, per-school",
        access="open",
        notes="Separate CSVs for student enrollment (by school/grade/gender) and personnel.",
    ),
    academic_performance=DataSource(
        name="INVALSI Servizio Statistico",
        url="https://serviziostatistico.invalsi.it/en/catalogo-dati/",
        format="csv (aggregate) / spss (micro)",
        coverage="provincial aggregate (open), per-school (restricted)",
        access="open (aggregate) / registration (micro)",
        notes="SIGNIFICANT GAP: Per-school scores not publicly available. Provincial avg only.",
    ),
    traffic_safety=DataSource(
        name="ISTAT Incidenti Stradali",
        url="http://dati.istat.it/Index.aspx?DataSetCode=DCIS_INCIDENTISTR1",
        format="csv",
        coverage="national, geocoded",
        access="open",
        notes="Annual road accident database with location data.",
    ),
    crime=DataSource(
        name="ISTAT Crime Statistics",
        url="http://dati.istat.it/Index.aspx?QueryId=25097",
        format="sdmx/csv",
        coverage="provincial only",
        access="open",
        notes="SIGNIFICANT GAP: Only provincial granularity. Too coarse for school neighborhoods.",
    ),
    transit=DataSource(
        name="Per-city GTFS feeds",
        url="https://transit.land/",
        format="gtfs",
        coverage="major cities only (Milan, Rome, Turin, Naples, etc.)",
        access="open",
        notes="No national aggregator. Must source per-city from transit agencies or Transitland.",
    ),
    demographics=DataSource(
        name="ISTAT",
        url="https://demo.istat.it/",
        format="csv",
        coverage="national, municipal level",
        access="open",
        notes="Population, age structure, nationality at municipal level. Tax income data available.",
    ),
    poi=DataSource(
        name="Overpass API + Google Places",
        url="https://overpass-api.de/api/",
        format="json + api",
        coverage="Italy (OSM) + point (Google)",
        access="open + api_key",
        notes="Standard approach.",
    ),
)

# =============================================================================
# SPAIN
# =============================================================================
ES_CONFIG = CountryConfig(
    country_code="ES",
    country_name="Spain",
    language="es",
    currency="EUR",
    geocoding_needed=True,  # Most regional datasets lack coordinates
    school_id_field="CODIGO_CENTRO",
    school_types=["IES", "Centro Concertado", "Centro Privado"],
    school_registry=DataSource(
        name="Registro Estatal de Centros Docentes + Regional Portals",
        url="https://datos.gob.es/en/catalogo?tags=educacion",
        format="csv/json/shp (varies by region)",
        coverage="national (fragmented across 17 autonomous communities)",
        access="open (most regions)",
        notes="CHALLENGE: Must aggregate ~17 regional datasets. No single national CSV. "
              "Format and column names vary by region. datos.gob.es API helps discovery.",
    ),
    student_teacher_stats=DataSource(
        name="INE Estadística de Enseñanzas",
        url="https://www.ine.es/dyngs/INEbase/en/categoria.htm?c=Estadistica_P&cid=1254735573113",
        format="pc-axis/csv",
        coverage="provincial aggregate (not per-school)",
        access="open",
        notes="SIGNIFICANT GAP: No per-school student counts. Provincial aggregates only.",
    ),
    academic_performance=DataSource(
        name="N/A",
        url="",
        format="",
        coverage="",
        access="",
        notes="CRITICAL GAP: Spain does not publish per-school exam results. "
              "No equivalent of GCSE tables, IVAL, or DUO exam data.",
    ),
    traffic_safety=DataSource(
        name="DGT Accident Data",
        url="https://datos.gob.es/en/catalogo?tags=DGT",
        format="csv",
        coverage="national, geocoded",
        access="open",
        notes="Annual road accident statistics via datos.gob.es.",
    ),
    crime=DataSource(
        name="Portal Estadístico de Criminalidad",
        url="https://estadisticasdecriminalidad.ses.mir.es/publico/portalestadistico/en/",
        format="pc-axis/pdf",
        coverage="municipal level",
        access="open (manual download)",
        notes="14 crime categories at municipal level since ~2010. No API. Manual download.",
    ),
    transit=DataSource(
        name="City GTFS feeds via datos.gob.es",
        url="https://datos.gob.es/en/catalogo?tags_es=gtfs",
        format="gtfs",
        coverage="~20 major cities (Madrid, Barcelona, Valencia, Bilbao, etc.)",
        access="open",
        notes="Standard GTFS. Smaller cities and rural areas not covered.",
    ),
    demographics=DataSource(
        name="INE Atlas de Distribución de Renta + Padrón",
        url="https://www.ine.es/",
        format="pc-axis/csv/json",
        coverage="national, census-tract level (income) / municipal (population)",
        access="open",
        notes="Census-tract income data is strong. Population via Padrón. "
              "Python: ineAtlas R package, or INEbase PC-Axis files.",
    ),
    poi=DataSource(
        name="Overpass API + Google Places",
        url="https://overpass-api.de/api/",
        format="json + api",
        coverage="Spain (OSM) + point (Google)",
        access="open + api_key",
        notes="Standard approach.",
    ),
)

# =============================================================================
# REGISTRY
# =============================================================================
COUNTRY_CONFIGS = {
    "NL": NL_CONFIG,
    "GB": GB_CONFIG,
    "FR": FR_CONFIG,
    "IT": IT_CONFIG,
    "ES": ES_CONFIG,
}


def get_country_config(country_code: str) -> CountryConfig:
    """Get the full configuration for a country."""
    code = country_code.upper()
    if code not in COUNTRY_CONFIGS:
        raise ValueError(
            f"Unknown country code: {code}. "
            f"Available: {list(COUNTRY_CONFIGS.keys())}"
        )
    return COUNTRY_CONFIGS[code]


def list_countries() -> dict[str, str]:
    """Return {code: name} for all configured countries."""
    return {code: cfg.country_name for code, cfg in COUNTRY_CONFIGS.items()}


def print_data_source_summary():
    """Print a summary of data sources across all countries."""
    categories = [
        "school_registry", "student_teacher_stats", "academic_performance",
        "traffic_safety", "crime", "transit", "demographics", "poi",
    ]

    print("\nSchoolNossa International Data Sources")
    print("=" * 80)

    for code, cfg in COUNTRY_CONFIGS.items():
        print(f"\n{cfg.country_name} ({code})")
        print("-" * 40)
        for cat in categories:
            src = getattr(cfg, cat, None)
            if src is None:
                print(f"  {cat}: NOT CONFIGURED")
            elif not src.url:
                print(f"  {cat}: N/A — {src.notes[:60]}")
            else:
                print(f"  {cat}: {src.name}")
                print(f"    Format: {src.format} | Access: {src.access}")


if __name__ == "__main__":
    print_data_source_summary()
