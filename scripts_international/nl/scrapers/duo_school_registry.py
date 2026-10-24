#!/usr/bin/env python3
"""
Phase 1: Download and parse Dutch VO school data from DUO Open Onderwijsdata.

Downloads:
1. School addresses (vestigingen) — name, address, type, denomination
2. Student enrollment per vestiging — counts by grade and gender
3. Exam results per vestiging — pass rates, average CE/SE grades
4. Staff data per institution — headcount, FTE, avg age

Outputs:
    data_nl/raw/duo_vo_addresses.csv
    data_nl/raw/duo_vo_enrollment_{year}.csv
    data_nl/raw/duo_vo_exams_{year}.csv
    data_nl/raw/duo_vo_staff.xlsx
    data_nl/intermediate/nl_school_master_base.csv  (merged)

Usage:
    python duo_school_registry.py                    # Download + merge
    python duo_school_registry.py --skip-download    # Merge from cached files
"""

import argparse
import logging
import re
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

for d in [RAW_DIR, INTERMEDIATE_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# DUO URLs
#
# Vintage convention — verified against the DUO download table at
# duo.nl/open_onderwijsdata/voortgezet-onderwijs/aantal-leerlingen/:
# "Leerlingen <YYYY>" carries **peildatum 1 oktober YYYY**, i.e. the count taken
# at the start of school year YYYY/YYYY+1. So the 2025 file is SY 2025_26, NOT
# 2024_25 — it was mislabelled here until Sept 2026, which understated the
# asset's freshness by a year. Exam files are named with an explicit range
# (2024-2025 = SY 2024_25) and were already correct; exams necessarily lag
# enrollment by one year because they report a completed school year.
DUO_BASE = "https://duo.nl/open_onderwijsdata/images"

# Newest enrollment peildatum year available. Resolved at runtime by
# resolve_latest_enrollment_year(); this is the floor, not a pin — a hardcoded
# snapshot URL is how the Munich scraper silently sat 19 months stale
# (docs/DATA_REFRESH_PLAN_2026.md section 4).
ENROLLMENT_YEAR_FLOOR = 2025

# Addresses: peildatum-dated, refreshed monthly. The CKAN resource id is stable
# across refreshes; the duo.nl/images mirror is semicolon-delimited and unquoted.
ADDRESSES_URL = ("https://onderwijsdata.duo.nl/dataset/"
                 "c8e6ffdd-cc2b-44ee-880f-0ff03f72e868/resource/"
                 "5187f8d5-ff9c-4284-8e06-4311f0354956/download/vestigingenvo.csv")


def resolve_latest_enrollment_year(floor: int = ENROLLMENT_YEAR_FLOOR,
                                   lookahead: int = 2) -> int:
    """Probe forward from the known floor for a newer DUO enrollment file."""
    latest = floor
    for year in range(floor + 1, floor + 1 + lookahead):
        url = f"{DUO_BASE}/01.-leerlingen-vo-per-vestiging-naar-onderwijstype-{year}.csv"
        try:
            resp = requests.head(url, headers=HEADERS, timeout=30, allow_redirects=False)
        except requests.RequestException:
            break
        if resp.status_code != 200:
            break
        latest = year
    if latest != floor:
        logger.info(f"  Newer DUO enrollment vintage found: {latest}")
    return latest


def build_urls(latest_year: int = None) -> dict:
    """URL set for the newest enrollment vintage and the year before it."""
    latest = latest_year or resolve_latest_enrollment_year()
    prev = latest - 1
    return {
        "addresses": ADDRESSES_URL,
        f"enrollment_{latest}": (
            f"{DUO_BASE}/01.-leerlingen-vo-per-vestiging-naar-onderwijstype-{latest}.csv"),
        f"enrollment_{prev}": (
            f"{DUO_BASE}/01.-leerlingen-vo-per-vestiging-naar-onderwijstype-{prev}.csv"),
        # Exams report the last COMPLETED school year, so they trail by one.
        "exams_current": (
            f"{DUO_BASE}/geslaagden-gezakten-en-cijfers-{prev}-{latest}.csv"),
        "exams_previous": (
            f"{DUO_BASE}/geslaagden-gezakten-en-cijfers-{prev - 1}-{prev}.csv"),
        "exams_5yr": f"{DUO_BASE}/examenkandidaten-en-geslaagden-{latest - 5}-{latest}.csv",
        "staff": f"{DUO_BASE}/01.-onderwijspersoneel-vo-in-personen-2011-{latest}.xlsx",
    }


def school_year_label(peildatum_year: int) -> str:
    """Peildatum year -> school-year label, e.g. 2025 -> '2025_26'."""
    return f"{peildatum_year}_{str(peildatum_year + 1)[-2:]}"


def _newest(df, prefix: str):
    """Newest `<prefix><year>_<yy>` column present, or None."""
    hits = sorted(c for c in df.columns
                  if re.match(rf"^{re.escape(prefix)}20\d\d_\d\d$", c))
    return hits[-1] if hits else None


URLS = build_urls(ENROLLMENT_YEAR_FLOOR)

# Request headers
HEADERS = {
    "User-Agent": "SchoolNossa/1.0 (school comparison platform; contact@schoolnossa.com)"
}


def download_file(url: str, output_path: Path, force: bool = False) -> Path:
    """Download a file from URL, skip if cached."""
    if output_path.exists() and not force:
        logger.info(f"  Cached: {output_path.name} ({output_path.stat().st_size / 1024:.0f} KB)")
        return output_path

    logger.info(f"  Downloading: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    output_path.write_bytes(resp.content)
    logger.info(f"  Saved: {output_path.name} ({len(resp.content) / 1024:.0f} KB)")
    return output_path


def download_all(force: bool = False):
    """Download all DUO datasets."""
    logger.info("Downloading DUO VO datasets...")

    files = {}
    for key, url in URLS.items():
        ext = "xlsx" if url.endswith(".xlsx") else "csv"
        filename = f"duo_vo_{key}.{ext}"
        path = RAW_DIR / filename
        try:
            files[key] = download_file(url, path, force=force)
        except requests.HTTPError as e:
            logger.warning(f"  Failed to download {key}: {e}")
            files[key] = None

    return files


def _sniff_delimiter(path: Path) -> str:
    """DUO serves the same table two ways: the duo.nl/images mirror is
    semicolon-delimited and unquoted, the CKAN resource is comma-delimited and
    quoted. Sniff rather than pin, or a mirror switch silently yields a
    single-column frame and every rename misses."""
    for encoding in ("utf-8", "latin1"):
        try:
            with open(path, encoding=encoding) as fh:
                header = fh.readline()
            break
        except UnicodeDecodeError:
            continue
    else:
        return ";"
    return ";" if header.count(";") >= header.count(",") else ","


def parse_duo_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Parse a DUO CSV with Dutch conventions."""
    default_kwargs = {
        "sep": _sniff_delimiter(path),
        "encoding": "utf-8",
        "low_memory": False,
        "dtype": str,  # Read everything as string first, convert later
    }
    default_kwargs.update(kwargs)

    try:
        df = pd.read_csv(path, **default_kwargs)
    except UnicodeDecodeError:
        logger.info(f"  Retrying with latin1 encoding: {path.name}")
        default_kwargs["encoding"] = "latin1"
        df = pd.read_csv(path, **default_kwargs)

    # Strip whitespace from column names and string values
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    return df


def dutch_to_float(series: pd.Series) -> pd.Series:
    """Convert Dutch decimal comma strings to float, handling '<5' privacy masking."""
    return (
        series
        .replace("<5", None)
        .replace("x", None)
        .replace("", None)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )


def dutch_to_int(series: pd.Series) -> pd.Series:
    """Convert Dutch integer strings to int, handling '<5' and 'x' masking."""
    cleaned = (
        series
        .replace("<5", None)
        .replace("x", None)
        .replace("", None)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def load_addresses(files: dict) -> pd.DataFrame:
    """Parse school addresses into clean format."""
    logger.info("\nParsing school addresses...")
    path = files.get("addresses")
    if path is None or not path.exists():
        raise FileNotFoundError("Addresses file not found")

    df = parse_duo_csv(path)
    logger.info(f"  Raw: {len(df)} vestigingen, {len(df.columns)} columns")

    # Rename to English
    rename_map = {
        "PROVINCIE": "province",
        "BEVOEGD GEZAG NUMMER": "school_board_id",
        "INSTELLINGSCODE": "brin_code",
        "VESTIGINGSCODE": "vestiging_code",
        "VESTIGINGSNAAM": "school_name",
        "STRAATNAAM": "street_name",
        "HUISNUMMER-TOEVOEGING": "house_number",
        "POSTCODE": "postal_code",
        "PLAATSNAAM": "city",
        "GEMEENTENUMMER": "gemeente_code",
        "GEMEENTENAAM": "gemeente_name",
        "DENOMINATIE": "denomination",
        "TELEFOONNUMMER": "phone",
        "INTERNETADRES": "website",
        "ONDERWIJSSTRUCTUUR": "education_type",
    }

    df = df.rename(columns=rename_map)

    # Build full street address
    df["street_address"] = df["street_name"].fillna("") + " " + df["house_number"].fillna("")
    df["street_address"] = df["street_address"].str.strip()

    # Keep relevant columns
    keep_cols = [
        "province", "school_board_id", "brin_code", "vestiging_code",
        "school_name", "street_address", "postal_code", "city",
        "gemeente_code", "gemeente_name", "denomination", "phone",
        "website", "education_type",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    logger.info(f"  Parsed: {len(df)} schools")
    return df


def load_enrollment(files: dict) -> pd.DataFrame:
    """Parse enrollment data — aggregate total students per vestiging."""
    logger.info("\nParsing enrollment data...")

    # Label from the file's own peildatum year rather than a fixed pair, so a
    # newer DUO vintage cannot land in the previous year's column name.
    enrollment_keys = sorted(
        (k for k in files if k.startswith("enrollment_")), reverse=True)
    results = []
    for key in enrollment_keys:
        year_label = school_year_label(int(key.rsplit("_", 1)[1]))
        path = files.get(key)
        if path is None or not path.exists():
            logger.warning(f"  {key} not found, skipping")
            continue
        logger.info(f"  {key} -> school year {year_label}")

        df = parse_duo_csv(path)
        logger.info(f"  {key}: {len(df)} rows")

        # Build full vestiging code: enrollment has INSTELLINGSCODE=00AH + VESTIGINGSCODE=00
        # Addresses have VESTIGINGSCODE=00AH00 (BRIN + suffix)
        df["VESTIGINGSCODE_FULL"] = df["INSTELLINGSCODE"] + df["VESTIGINGSCODE"].str.zfill(2)

        # Sum all grade columns (LEER- OF VERBLIJFSJAAR * - MAN/VROUW) per vestiging
        grade_cols = [c for c in df.columns if "LEER- OF VERBLIJFSJAAR" in c]
        for col in grade_cols:
            df[col] = dutch_to_int(df[col])

        # Total students per vestiging
        agg = (
            df.groupby(["INSTELLINGSCODE", "VESTIGINGSCODE_FULL"])[grade_cols]
            .sum()
            .reset_index()
        )
        agg[f"students_{year_label}"] = agg[grade_cols].sum(axis=1)
        agg = agg[["INSTELLINGSCODE", "VESTIGINGSCODE_FULL", f"students_{year_label}"]]
        agg = agg.rename(columns={
            "INSTELLINGSCODE": "brin_code",
            "VESTIGINGSCODE_FULL": "vestiging_code",
        })
        results.append(agg)

    if not results:
        return pd.DataFrame()

    # Merge years
    merged = results[0]
    for df in results[1:]:
        merged = merged.merge(df, on=["brin_code", "vestiging_code"], how="outer")

    logger.info(f"  Enrollment: {len(merged)} vestigingen with student counts")
    return merged


def load_exams(files: dict) -> pd.DataFrame:
    """Parse exam results — pass rates and average grades per vestiging."""
    logger.info("\nParsing exam results...")

    results = []
    # Exam files report the last completed school year, one behind enrollment.
    latest_enrollment = max(
        (int(k.rsplit("_", 1)[1]) for k in files if k.startswith("enrollment_")),
        default=ENROLLMENT_YEAR_FLOOR)
    exam_keys = [("exams_current", school_year_label(latest_enrollment - 1)),
                 ("exams_previous", school_year_label(latest_enrollment - 2))]
    for key, year_label in exam_keys:
        path = files.get(key)
        if path is None or not path.exists():
            logger.warning(f"  {key} not found, skipping")
            continue

        df = parse_duo_csv(path)
        logger.info(f"  {key}: {len(df)} rows")

        # Convert numeric columns
        for col in ["EXAMENKANDIDATEN", "GESLAAGDEN", "GEZAKTEN"]:
            if col in df.columns:
                df[col] = dutch_to_int(df[col])
        for col in ["GEMIDDELD CIJFER SCHOOLEXAMEN", "GEMIDDELD CIJFER CENTRAAL EXAMEN",
                     "GEMIDDELD CIJFER CIJFERLIJST"]:
            if col in df.columns:
                df[col] = dutch_to_float(df[col])

        # Aggregate per vestiging (across tracks)
        agg = (
            df.groupby(["INSTELLINGSCODE", "VESTIGINGSCODE"])
            .agg({
                "EXAMENKANDIDATEN": "sum",
                "GESLAAGDEN": "sum",
                "GEZAKTEN": "sum",
                "GEMIDDELD CIJFER CENTRAAL EXAMEN": "mean",
                "GEMIDDELD CIJFER SCHOOLEXAMEN": "mean",
                "GEMIDDELD CIJFER CIJFERLIJST": "mean",
            })
            .reset_index()
        )

        # Compute pass rate
        total = agg["EXAMENKANDIDATEN"]
        agg[f"exam_pass_rate_{year_label}"] = (
            agg["GESLAAGDEN"] / total.where(total > 0)
        ).round(4)
        agg[f"exam_avg_ce_{year_label}"] = agg["GEMIDDELD CIJFER CENTRAAL EXAMEN"].round(2)
        agg[f"exam_avg_se_{year_label}"] = agg["GEMIDDELD CIJFER SCHOOLEXAMEN"].round(2)
        agg[f"exam_avg_overall_{year_label}"] = agg["GEMIDDELD CIJFER CIJFERLIJST"].round(2)
        agg[f"exam_candidates_{year_label}"] = agg["EXAMENKANDIDATEN"]

        keep = [
            "INSTELLINGSCODE", "VESTIGINGSCODE",
            f"exam_pass_rate_{year_label}", f"exam_avg_ce_{year_label}",
            f"exam_avg_se_{year_label}", f"exam_avg_overall_{year_label}",
            f"exam_candidates_{year_label}",
        ]
        agg = agg[[c for c in keep if c in agg.columns]]
        agg = agg.rename(columns={
            "INSTELLINGSCODE": "brin_code",
            "VESTIGINGSCODE": "vestiging_code",
        })
        results.append(agg)

    if not results:
        return pd.DataFrame()

    merged = results[0]
    for df in results[1:]:
        merged = merged.merge(df, on=["brin_code", "vestiging_code"], how="outer")

    logger.info(f"  Exams: {len(merged)} vestigingen with exam data")
    return merged


def load_staff(files: dict) -> pd.DataFrame:
    """Parse staff data — headcount and FTE per institution (BRIN level)."""
    logger.info("\nParsing staff data...")
    path = files.get("staff")
    if path is None or not path.exists():
        logger.warning("  Staff file not found")
        return pd.DataFrame()

    # Use the per-FUNCTIEGROEP sheet so we can isolate actual teaching staff.
    # The plain institution sheet counts ALL personnel — directie, support staff
    # (OOP/OBP) and trainees included — which inflated "teachers" enough to make
    # student_teacher_ratio meaningless (median 2.3 against a real NL VO figure
    # nearer 15-20).
    sheet = "owtype-best-instelling-functie"
    try:
        df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception as e:
        logger.warning(f"  Failed to read staff sheet {sheet}: {e}")
        try:
            df = pd.read_excel(path, sheet_name="owtype-best-instelling", dtype=str)
        except Exception as e2:
            logger.warning(f"  Failed to read staff Excel: {e2}")
            return pd.DataFrame()

    logger.info(f"  Raw staff: {len(df)} rows, {len(df.columns)} columns")

    if "FUNCTIEGROEP" in df.columns:
        before = len(df)
        teaching = df["FUNCTIEGROEP"].astype(str).str.strip().str.lower()
        # "Onderwijsgevend personeel" = teaching staff; LIO are trainee teachers
        # and are counted separately by DUO, so they stay out of the headline.
        df = df[teaching.eq("onderwijsgevend personeel")]
        logger.info(f"  Teaching staff rows: {len(df)}/{before} "
                    f"(excluded directie, OOP/OBP, LIO)")

    staff_result = pd.DataFrame()
    staff_result["brin_code"] = df["INSTELLINGSCODE"] if "INSTELLINGSCODE" in df.columns else None

    if staff_result["brin_code"] is None:
        return pd.DataFrame()

    # Values are suppressed as '*' where the count is small enough to identify
    # individuals; dutch_to_int yields NaN for those rather than 0.
    for year_suffix, label in [("2025", "teachers_current"), ("2024", "teachers_previous")]:
        col_name = f"PERSONEN {year_suffix}"
        if col_name in df.columns:
            staff_result[label] = dutch_to_int(df[col_name])

    avg_age_col = "GEMIDDELDE LEEFTIJD 2025"
    if avg_age_col in df.columns:
        staff_result["staff_avg_age"] = dutch_to_float(df[avg_age_col])

    avg_fte_col = "GEMIDDELDE FTE'S 2025"
    if avg_fte_col in df.columns:
        staff_result["staff_avg_fte"] = dutch_to_float(df[avg_fte_col])

    # Deduplicate to BRIN level (sum across onderwijstype)
    staff_result = (
        staff_result.groupby("brin_code")
        .agg({
            "teachers_current": "sum",
            "teachers_previous": "sum",
            **({
                "staff_avg_age": "mean"
            } if "staff_avg_age" in staff_result.columns else {}),
            **({
                "staff_avg_fte": "mean"
            } if "staff_avg_fte" in staff_result.columns else {}),
        })
        .reset_index()
    )

    logger.info(f"  Staff: {len(staff_result)} institutions with teacher counts")
    return staff_result


def apportion_staff_to_vestigingen(master: pd.DataFrame) -> pd.DataFrame:
    """Split institution-level staff counts across a BRIN's vestigingen.

    DUO publishes staff per *instelling* (BRIN4) while students are per
    *vestiging* (BRIN6). Merging on BRIN gives every location of a
    scholengemeenschap the institution's full staff count — Het Stedelijk's 6
    locations each showed the same 498 — which makes any per-location ratio
    meaningless. Split by each location's share of the institution's students,
    the standard apportionment, and keep the raw institution figure beside it so
    the derivation stays visible.
    """
    students_col = _newest(master, "students_")
    if students_col is None or "teachers_current" not in master.columns:
        return master

    master = master.copy()
    brin = master["brin_code"].astype(str)
    students = pd.to_numeric(master[students_col], errors="coerce")

    locations = brin.map(brin.value_counts())
    institution_students = students.groupby(brin).transform("sum")
    # Fall back to an even split when an institution reports no students at all.
    share = (students / institution_students).where(
        institution_students.gt(0), 1.0 / locations)

    for col, raw_col in (("teachers_current", "teachers_institution_current"),
                         ("teachers_previous", "teachers_institution_previous")):
        if col not in master.columns:
            continue
        institution_total = pd.to_numeric(master[col], errors="coerce")
        master[raw_col] = institution_total
        master[col] = (institution_total * share).round(1)

    master["teachers_apportioned"] = locations.gt(1)
    multi = int(locations.gt(1).sum())
    logger.info(f"  + Staff apportioned by student share across "
                f"{multi} multi-location vestigingen "
                f"({int(locations.eq(1).sum())} single-location unchanged)")
    return master


def merge_all(addresses: pd.DataFrame, enrollment: pd.DataFrame,
              exams: pd.DataFrame, staff: pd.DataFrame) -> pd.DataFrame:
    """Merge all datasets into a single school master table."""
    logger.info("\nMerging all datasets...")

    master = addresses.copy()
    logger.info(f"  Base: {len(master)} schools from addresses")

    # Merge enrollment (vestiging level)
    if not enrollment.empty:
        master = master.merge(
            enrollment, on=["brin_code", "vestiging_code"], how="left"
        )
        students_col = _newest(master, "students_")
        filled = master[students_col].notna().sum() if students_col else 0
        logger.info(f"  + Enrollment: {filled}/{len(master)} schools with student counts"
                    f" ({students_col})")

    # Merge exams (vestiging level)
    if not exams.empty:
        master = master.merge(
            exams, on=["brin_code", "vestiging_code"], how="left"
        )
        exam_col = _newest(master, "exam_pass_rate_")
        filled = master[exam_col].notna().sum() if exam_col else 0
        logger.info(f"  + Exams: {filled}/{len(master)} schools with exam data"
                    f" ({exam_col})")

    # Merge staff (BRIN level — not vestiging)
    if not staff.empty:
        master = master.merge(staff, on="brin_code", how="left")
        filled = master["teachers_current"].notna().sum() if "teachers_current" in master.columns else 0
        logger.info(f"  + Staff: {filled}/{len(master)} institutions with teacher counts")
        master = apportion_staff_to_vestigingen(master)

    # Compute student-teacher ratio
    students_col = _newest(master, "students_")
    if students_col and "teachers_current" in master.columns:
        teachers = master["teachers_current"].where(master["teachers_current"] > 0)
        master["student_teacher_ratio"] = (master[students_col] / teachers).round(1)

    logger.info(f"\n  Final: {len(master)} schools, {len(master.columns)} columns")
    return master


def main(skip_download: bool = False, force_download: bool = False):
    """Run the full Phase 1 pipeline."""
    logger.info("=" * 60)
    logger.info("NL Phase 1: DUO School Registry Download & Parse")
    logger.info("=" * 60)

    # Step 1: Download
    if skip_download:
        logger.info("Skipping download, using cached files...")
        files = {key: RAW_DIR / f"duo_vo_{key}.{'xlsx' if 'staff' in key else 'csv'}"
                 for key in URLS}
    else:
        files = download_all(force=force_download)

    # Step 2: Parse each dataset
    addresses = load_addresses(files)
    enrollment = load_enrollment(files)
    exams = load_exams(files)
    staff = load_staff(files)

    # Step 3: Merge
    master = merge_all(addresses, enrollment, exams, staff)

    # Step 4: Save
    output_path = INTERMEDIATE_DIR / "nl_school_master_base.csv"
    master.to_csv(output_path, index=False)
    logger.info(f"\nSaved: {output_path}")

    # Quality report
    logger.info("\n" + "=" * 60)
    logger.info("DATA QUALITY REPORT")
    logger.info("=" * 60)
    for col in master.columns:
        non_null = master[col].notna().sum()
        pct = non_null / len(master) * 100
        status = "+" if pct > 50 else "~" if pct > 0 else "-"
        logger.info(f"  {status} {col}: {non_null}/{len(master)} ({pct:.0f}%)")

    return master


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NL Phase 1: DUO School Registry")
    parser.add_argument("--skip-download", action="store_true", help="Use cached files")
    parser.add_argument("--force-download", action="store_true", help="Re-download all files")
    args = parser.parse_args()

    main(skip_download=args.skip_download, force_download=args.force_download)
