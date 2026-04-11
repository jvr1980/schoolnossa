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
DUO_BASE = "https://duo.nl/open_onderwijsdata/images"
URLS = {
    "addresses": f"{DUO_BASE}/02.-alle-vestigingen-vo.csv",
    "enrollment_2025": f"{DUO_BASE}/01.-leerlingen-vo-per-vestiging-naar-onderwijstype-2025.csv",
    "enrollment_2024": f"{DUO_BASE}/01.-leerlingen-vo-per-vestiging-naar-onderwijstype-2024.csv",
    "exams_2025": f"{DUO_BASE}/geslaagden-gezakten-en-cijfers-2024-2025.csv",
    "exams_2024": f"{DUO_BASE}/geslaagden-gezakten-en-cijfers-2023-2024.csv",
    "exams_5yr": f"{DUO_BASE}/examenkandidaten-en-geslaagden-2020-2025.csv",
    "staff": f"{DUO_BASE}/01.-onderwijspersoneel-vo-in-personen-2011-2025.xlsx",
}

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


def parse_duo_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Parse a DUO semicolon-delimited CSV with Dutch conventions."""
    default_kwargs = {
        "sep": ";",
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

    results = []
    for key, year_label in [("enrollment_2025", "2024_25"), ("enrollment_2024", "2023_24")]:
        path = files.get(key)
        if path is None or not path.exists():
            logger.warning(f"  {key} not found, skipping")
            continue

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
    for key, year_label in [("exams_2025", "2024_25"), ("exams_2024", "2023_24")]:
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

    # Read the institution-level sheet
    try:
        df = pd.read_excel(path, sheet_name="owtype-best-instelling", dtype=str)
    except Exception as e:
        logger.warning(f"  Failed to read staff Excel: {e}")
        return pd.DataFrame()

    logger.info(f"  Raw staff: {len(df)} rows, {len(df.columns)} columns")

    # Get most recent year columns
    year_cols = {}
    for col in df.columns:
        if "PERSONEN 2025" in col.upper() or "PERSONEN 2024" in col.upper():
            year_cols[col] = col

    # Find the total headcount column for 2025 and 2024
    staff_result = pd.DataFrame()
    staff_result["brin_code"] = df["INSTELLINGSCODE"] if "INSTELLINGSCODE" in df.columns else None

    if staff_result["brin_code"] is None:
        return pd.DataFrame()

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
        filled = master["students_2024_25"].notna().sum() if "students_2024_25" in master.columns else 0
        logger.info(f"  + Enrollment: {filled}/{len(master)} schools with student counts")

    # Merge exams (vestiging level)
    if not exams.empty:
        master = master.merge(
            exams, on=["brin_code", "vestiging_code"], how="left"
        )
        filled = master["exam_pass_rate_2024_25"].notna().sum() if "exam_pass_rate_2024_25" in master.columns else 0
        logger.info(f"  + Exams: {filled}/{len(master)} schools with exam data")

    # Merge staff (BRIN level — not vestiging)
    if not staff.empty:
        master = master.merge(staff, on="brin_code", how="left")
        filled = master["teachers_current"].notna().sum() if "teachers_current" in master.columns else 0
        logger.info(f"  + Staff: {filled}/{len(master)} schools with teacher counts")

    # Compute student-teacher ratio
    if "students_2024_25" in master.columns and "teachers_current" in master.columns:
        teachers = master["teachers_current"].where(master["teachers_current"] > 0)
        master["student_teacher_ratio"] = (master["students_2024_25"] / teachers).round(1)

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
