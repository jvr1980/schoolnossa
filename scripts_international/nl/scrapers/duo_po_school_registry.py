#!/usr/bin/env python3
"""
NL Primary (basisonderwijs) Phase 1: DUO registry + enrollment + staff.

The VO sibling of this script is duo_school_registry.py; the shape is the same
but the sources differ:

  addresses    vestigingenbo.csv (CKAN)          — 6,096 BO vestigingen
  enrollment   brin6_totaal.csv                  — long format, one row per
                                                   (BRIN6, TYPE_PO, PEILJAAR)
  staff        onderwijspersoneel-po ... .xlsx   — instelling level, as for VO

Scope is TYPE_PO == "BO", i.e. regular primary. SBO (speciaal basisonderwijs),
SO and VSO are separate school systems with their own admission routes and are
excluded rather than silently blended into the primary set.

Vintage convention matches VO: PEILJAAR 2025 means peildatum 1 oktober 2025,
which is school year 2025/26.

Output: data_nl/intermediate/nl_po_school_master_base.csv
"""

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data_nl_po"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "SchoolNossa/1.0 (school comparison platform; contact@schoolnossa.com)"}

URLS = {
    "addresses": ("https://onderwijsdata.duo.nl/dataset/"
                  "786f12ea-6224-42fd-ab72-de4d7d879535/resource/"
                  "dcc9c9a5-6d01-410b-967f-810557588ba4/download/vestigingenbo.csv"),
    "enrollment": ("https://onderwijsdata.duo.nl/dataset/"
                   "cf80e90d-ed19-4a10-a138-00c5c345cb5e/resource/"
                   "9278ae97-4014-49f4-91fc-8cc255c2595d/download/brin6_totaal.csv"),
    "staff": ("https://duo.nl/open_onderwijsdata/images/"
              "01.-onderwijspersoneel-po-in-personen-2011-2025.xlsx"),
}

PO_TYPE = "BO"   # regular primary; SBO/SO/VSO are separate systems

ADDRESS_RENAME = {
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
}


def school_year_label(peildatum_year: int) -> str:
    return f"{peildatum_year}_{str(peildatum_year + 1)[-2:]}"


def download(force: bool = False) -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    files = {}
    for key, url in URLS.items():
        suffix = ".xlsx" if url.endswith(".xlsx") else ".csv"
        path = RAW_DIR / f"duo_po_{key}{suffix}"
        if path.exists() and not force and path.stat().st_size > 1024:
            logger.info(f"  Cached: {path.name}")
        else:
            logger.info(f"  Downloading {key}...")
            resp = requests.get(url, headers=HEADERS, timeout=600)
            resp.raise_for_status()
            path.write_bytes(resp.content)
            logger.info(f"    {len(resp.content) / 1024 / 1024:.1f} MB")
        files[key] = path
    return files


def load_addresses(files: dict) -> pd.DataFrame:
    logger.info("\nParsing PO addresses...")
    df = pd.read_csv(files["addresses"], dtype=str, low_memory=False)
    logger.info(f"  Raw: {len(df)} vestigingen, {len(df.columns)} columns")

    df = df.rename(columns=ADDRESS_RENAME)
    df["street_address"] = (df.get("street_name", "").fillna("") + " "
                            + df.get("house_number", "").fillna("")).str.strip()
    # vestigingenbo ships VESTIGINGSCODE already as BRIN6 in some releases and as
    # the 2-digit suffix in others; normalise to BRIN6.
    vest = df["vestiging_code"].astype(str).str.strip()
    df["vestiging_code"] = vest.where(vest.str.len() == 6,
                                      df["brin_code"].astype(str).str.strip()
                                      + vest.str.zfill(2))

    keep = ["brin_code", "vestiging_code", "school_name", "street_address",
            "postal_code", "city", "gemeente_code", "gemeente_name", "province",
            "denomination", "phone", "website", "school_board_id"]
    df = df[[c for c in keep if c in df.columns]].drop_duplicates("vestiging_code")
    logger.info(f"  Clean: {len(df)} vestigingen")
    return df


def load_enrollment(files: dict) -> tuple[pd.DataFrame, str]:
    logger.info("\nParsing PO enrollment...")
    df = pd.read_csv(files["enrollment"], dtype={"INSTELLINGSCODE": str,
                                                 "VESTIGINGSCODE": str})
    df = df[df["TYPE_PO"].astype(str).str.upper() == PO_TYPE]
    latest = int(df["PEILJAAR"].max())
    prev = latest - 1
    logger.info(f"  TYPE_PO={PO_TYPE}, newest PEILJAAR={latest} "
                f"-> school year {school_year_label(latest)}")

    df["vestiging_code"] = (df["INSTELLINGSCODE"].str.strip()
                            + df["VESTIGINGSCODE"].astype(str).str.zfill(2))
    out = None
    for year in (latest, prev):
        chunk = (df[df["PEILJAAR"] == year]
                 .groupby("vestiging_code", as_index=False)["AANTAL_LEERLINGEN"].sum()
                 .rename(columns={"AANTAL_LEERLINGEN": f"students_{school_year_label(year)}"}))
        out = chunk if out is None else out.merge(chunk, on="vestiging_code", how="outer")
    logger.info(f"  {len(out)} vestigingen with enrollment")
    return out, school_year_label(latest)


def load_staff(files: dict, latest_year: int) -> pd.DataFrame:
    """Teaching staff per instelling (BRIN4), as for VO."""
    logger.info("\nParsing PO staff...")
    try:
        xl = pd.ExcelFile(files["staff"])
    except Exception as e:
        logger.warning(f"  Cannot open staff workbook: {e}")
        return pd.DataFrame()

    sheet = next((s for s in xl.sheet_names if "instelling" in s and "functie" in s), None)
    if sheet is None:
        logger.warning(f"  No instelling+functie sheet; have {xl.sheet_names}")
        return pd.DataFrame()

    df = pd.read_excel(files["staff"], sheet_name=sheet, dtype=str)
    if "FUNCTIEGROEP" in df.columns:
        before = len(df)
        df = df[df["FUNCTIEGROEP"].astype(str).str.strip().str.lower()
                .eq("onderwijsgevend personeel")]
        logger.info(f"  Teaching staff rows: {len(df)}/{before}")

    if "INSTELLINGSCODE" not in df.columns:
        logger.warning(f"  No INSTELLINGSCODE column; have {list(df.columns)[:8]}")
        return pd.DataFrame()

    def to_int(s):
        return pd.to_numeric(s.astype(str).str.replace(r"[^\d]", "", regex=True),
                             errors="coerce")

    out = pd.DataFrame({"brin_code": df["INSTELLINGSCODE"].astype(str).str.strip()})
    for year, label in ((latest_year, "teachers_current"),
                        (latest_year - 1, "teachers_previous")):
        col = f"PERSONEN {year}"
        if col in df.columns:
            out[label] = to_int(df[col])
    out = out.groupby("brin_code", as_index=False).sum(numeric_only=True)
    logger.info(f"  {len(out)} instellingen with teacher counts")
    return out


def apportion_staff(master: pd.DataFrame, students_col: str) -> pd.DataFrame:
    """Split institution staff across vestigingen by student share (see VO)."""
    if "teachers_current" not in master.columns:
        return master
    brin = master["brin_code"].astype(str)
    students = pd.to_numeric(master[students_col], errors="coerce")
    locations = brin.map(brin.value_counts())
    institution_students = students.groupby(brin).transform("sum")
    share = (students / institution_students).where(
        institution_students.gt(0), 1.0 / locations)
    for col, raw in (("teachers_current", "teachers_institution_current"),
                     ("teachers_previous", "teachers_institution_previous")):
        if col in master.columns:
            total = pd.to_numeric(master[col], errors="coerce")
            master[raw] = total
            master[col] = (total * share).round(1)
    master["teachers_apportioned"] = locations.gt(1)
    logger.info(f"  + Staff apportioned across {int(locations.gt(1).sum())} "
                f"multi-location vestigingen")
    return master


def main(force_download: bool = False):
    logger.info("=" * 60)
    logger.info("NL Primary (BO) Phase 1: DUO registry")
    logger.info("=" * 60)

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    files = download(force=force_download)

    addresses = load_addresses(files)
    enrollment, year_label = load_enrollment(files)
    latest_year = int(year_label.split("_")[0])
    staff = load_staff(files, latest_year)

    master = addresses.merge(enrollment, on="vestiging_code", how="left")
    students_col = f"students_{year_label}"
    logger.info(f"\n  + Enrollment: {master[students_col].notna().sum()}/{len(master)}"
                f" ({students_col})")

    if not staff.empty:
        master = master.merge(staff, on="brin_code", how="left")
        logger.info(f"  + Staff: {master['teachers_current'].notna().sum()}/{len(master)}")
        master = apportion_staff(master, students_col)
        teachers = master["teachers_current"].where(master["teachers_current"] > 0)
        master["student_teacher_ratio"] = (master[students_col] / teachers).round(1)

    master["school_type"] = "primary"
    master["education_type"] = "BO"
    master["country_code"] = "NL"
    master["students_data_year"] = year_label

    # Keep only funded regular primary schools that actually report pupils.
    before = len(master)
    master = master[master[students_col].notna()]
    logger.info(f"\n  Dropped {before - len(master)} vestigingen with no {PO_TYPE} pupils "
                f"(closed, or SBO/SO-only locations)")

    output = INTERMEDIATE_DIR / "nl_po_school_master_base.csv"
    master.to_csv(output, index=False)
    logger.info(f"\n  Final: {len(master)} primary schools, {len(master.columns)} columns")
    logger.info(f"  Saved: {output}")
    if "student_teacher_ratio" in master.columns:
        r = master["student_teacher_ratio"].dropna()
        logger.info(f"  student_teacher_ratio median {r.median():.1f} "
                    f"(NL primary reality ~17-20 per FTE)")
    return master


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()
    main(force_download=args.force_download)
