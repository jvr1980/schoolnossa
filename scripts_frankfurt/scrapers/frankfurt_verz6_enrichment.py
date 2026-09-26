#!/usr/bin/env python3
"""
Frankfurt Verzeichnis 6 Enrichment (Phase 2 — optional)

Joins Hessen Verzeichnis 6 data into the Schulwegweiser-based raw CSVs to add:
  - schulnummer      : official 4-digit HKM school ID (primary key for Berlin schema)
  - ndh_count        : non-German native language student count (belastungsstufe proxy)
  - schueler_YYYY_YY : official student count, one column per edition (newest two)

Matching: fuzzy name match (SequenceMatcher ≥ 0.75) + PLZ cross-check.
Schools without a Verzeichnis 6 match get a generated ID: "SW-{slug}".

Vintage: each edition names its survey date ("Erhebung ... vom 01. November
2025" → school year 2025/26). The student-count column is labelled from that
sentence, never from the file name, so a new edition cannot overwrite last
year's column.

Editions: the Hessen publications page links only the newest edition and old
files are deleted (verz-6_25_0.xlsx went 404 once verz-6_26 appeared). Every
edition is therefore archived as data_frankfurt/cache/verz6_{EE}.xlsx and kept
in git; older editions come from that archive only.

Input:
  data_frankfurt/raw/frankfurt_primary_schools.csv    (from Phase 1)
  data_frankfurt/raw/frankfurt_secondary_schools.csv
  data_frankfurt/raw/frankfurt_vocational_schools.csv (optional)

Output: writes schulnummer + ndh_count + schueler_* back into the same raw CSVs.

Author: Frankfurt School Data Pipeline
Created: 2026-04-06
"""

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path

import openpyxl
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR   = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR     = PROJECT_ROOT / "data_frankfurt"
RAW_DIR      = DATA_DIR / "raw"
CACHE_DIR    = DATA_DIR / "cache"

# Publications page that links the current edition of every Hessen Verzeichnis
VERZ6_INDEX_URL = "https://statistik.hessen.de/publikationen/verzeichnisse"
VERZ6_LINK_RE = re.compile(
    r"/sites/statistik\.hessen\.de/files/\d{4}-\d{2}/verz-6_(\d{2})(?:_\d+)?\.xlsx"
)
VERZ6_ARCHIVE_RE = re.compile(r"verz6_(\d{2})\.xlsx")
SURVEY_DATE_RE = re.compile(r"Erhebung an den allgemeinbildenden Schulen vom\s+\d{1,2}\.\s*\w+\s+(\d{4})")
HTTP_HEADERS = {"User-Agent": "SchoolNossa/1.0 (Frankfurt school data pipeline, educational project)"}

# Frankfurt Landkreis code in Verzeichnis 6
FFM_LANDKREIS = 412


# ── Download + parse Verzeichnis 6 ───────────────────────────────────────────

def verz6_archive_path(edition: int) -> Path:
    return CACHE_DIR / f"verz6_{edition:02d}.xlsx"


def archived_editions() -> list:
    return sorted(int(m.group(1)) for p in CACHE_DIR.glob("verz6_*.xlsx")
                  if (m := VERZ6_ARCHIVE_RE.fullmatch(p.name)))


def resolve_latest_verz6_url():
    """(edition, url) of the newest Verz6 linked on the publications page, or (None, None)."""
    try:
        r = requests.get(VERZ6_INDEX_URL, headers=HTTP_HEADERS, timeout=45)
        r.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"  Could not read {VERZ6_INDEX_URL}: {e}")
        return None, None
    links = {int(m.group(1)): "https://statistik.hessen.de" + m.group(0)
             for m in VERZ6_LINK_RE.finditer(r.text)}
    if not links:
        logger.warning(f"  No verz-6 link on {VERZ6_INDEX_URL}")
        return None, None
    edition = max(links)
    return edition, links[edition]


def get_verz6(edition=None):
    """(edition, path) of a Verz6 edition — the newest when edition is None.

    Archived editions are used as-is (published editions never change). The
    newest online edition is downloaded into the archive on first use; an
    older edition that is not archived cannot be recovered and raises.
    """
    if edition is not None and verz6_archive_path(edition).exists():
        return edition, verz6_archive_path(edition)

    online_edition, url = resolve_latest_verz6_url()
    if edition is None:
        edition = online_edition if online_edition is not None else max(archived_editions(), default=None)
        if edition is None:
            raise FileNotFoundError("No Verzeichnis 6 edition online or archived")
        if online_edition is None:
            logger.warning(f"  Publications page unavailable — using archived edition {edition}")

    path = verz6_archive_path(edition)
    if path.exists():
        logger.info(f"  Using archived Verzeichnis 6 edition {edition}: {path.name}")
        return edition, path
    if edition != online_edition:
        raise FileNotFoundError(
            f"Verzeichnis 6 edition {edition} is not archived and no longer online "
            f"(online: {online_edition})")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"  Downloading Verzeichnis 6 edition {edition} from {url}...")
    r = requests.get(url, headers=HTTP_HEADERS, timeout=120)
    r.raise_for_status()
    path.write_bytes(r.content)
    logger.info(f"  Archived: {path} ({path.stat().st_size:,} bytes)")
    return edition, path


def verz6_school_year(path: Path) -> str:
    """School year of an edition's student counts, from its survey-date sentence.

    "Erhebung ... vom 01. November 2025" → "2025_26". Refuses to guess when the
    sentence is missing, since a wrong label would overwrite another year.
    """
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        for ws in wb.worksheets:
            if ws.title == "Schulverzeichnis":
                continue
            for row in ws.iter_rows(values_only=True):
                for v in row:
                    m = SURVEY_DATE_RE.search(v) if isinstance(v, str) else None
                    if m:
                        year = int(m.group(1))
                        return f"{year}_{(year + 1) % 100:02d}"
    finally:
        wb.close()
    raise ValueError(f"No survey date ('Erhebung ... vom <Tag>. November <Jahr>') in {path.name}")


def load_verz6(path: Path) -> pd.DataFrame:
    """Load and parse one Verzeichnis 6 edition for Frankfurt schools.

    The Excel file has multiple sheets; school data is in 'Schulverzeichnis'.
    Row 0 is the header row with German column names. The edition's school
    year is attached as df.attrs["school_year"].
    """

    # Data is in the 'Schulverzeichnis' sheet, header in row 0
    df = pd.read_excel(path, sheet_name="Schulverzeichnis", header=0, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    # Rename Hessen-specific column names to internal names
    renames = {
        "Schul-nummer":    "schulnummer",
        "Landkreis":       "landkreis",
        "Name der Schule": "schulname",
        "PLZ":             "plz_verz6",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})

    # Filter to Frankfurt (Landkreis 412)
    if "landkreis" in df.columns:
        df = df[df["landkreis"] == FFM_LANDKREIS].copy()
    else:
        logger.warning("  No 'landkreis' column found — not filtering by city")

    # ndH column — "Nichtdeutscher Herkunfts-\nsprache" or similar
    ndh_cols = [c for c in df.columns
                if "nichtdeutsch" in c.lower() or "herkunft" in c.lower()
                or "ndh" in c.lower()]
    if ndh_cols:
        df = df.rename(columns={ndh_cols[0]: "ndh_count"})

    # Student total — "Schülerinnen und Schüler insgesamt ohne Vorklassen"
    schueler_col = next(
        (c for c in df.columns if "schüler" in c.lower() and "insgesamt" in c.lower()
         and "ohne" in c.lower()),
        None
    )
    if schueler_col:
        df = df.rename(columns={schueler_col: "schueler_verz6"})
        logger.info(f"  Found student count column: '{schueler_col}'")

    keep = ["schulnummer", "schulname", "plz_verz6"]
    if "ndh_count" in df.columns:
        keep.append("ndh_count")
    if "schueler_verz6" in df.columns:
        keep.append("schueler_verz6")

    available = [c for c in keep if c in df.columns]
    df = df[available].dropna(subset=["schulnummer", "schulname"])
    df["schulnummer"] = df["schulnummer"].apply(
        lambda x: str(int(float(x))).strip() if pd.notna(x) else None
    )
    df["schulname"] = df["schulname"].astype(str).str.strip()
    df.attrs["school_year"] = verz6_school_year(path)

    logger.info(f"  Loaded {len(df)} Frankfurt schools from {path.name} "
                f"(school year {df.attrs['school_year']})")
    return df


def _same_school(name, plz, verz6_row) -> bool:
    """Identity check for a schulnummer join: same postcode, or near-identical name.

    Catches historic fuzzy-match errors (Klingerschule carried Kirchnerschule's
    number) without rejecting renamed-but-same schools at the same address.
    """
    plz, v_plz = str(plz)[:5], str(verz6_row["plz_verz6"])[:5]
    if plz.isdigit() and v_plz.isdigit() and plz == v_plz:
        return True
    return SequenceMatcher(None, normalize(name), normalize(verz6_row["schulname"])).ratio() >= 0.85


def apply_verz6_counts(df: pd.DataFrame, verz6_df: pd.DataFrame):
    """Write an edition's student counts into schueler_{school_year} on schulnummer.

    Verz6 is the official statistic, so it overwrites web-researched values.
    Rows keep what they have when there is no Verz6 match (SW-* ids, schools
    missing from the edition), when the match fails the identity check, or
    when the school is vocational — Verz6 covers general-education schools
    only, so a Berufliche Schule's count is just its general-education branch.
    Returns (df, rows_written).
    """
    col = f"schueler_{verz6_df.attrs['school_year']}"
    if "schueler_verz6" not in verz6_df.columns:
        return df, 0
    verz6 = (verz6_df.dropna(subset=["schueler_verz6"])
             .drop_duplicates(subset=["schulnummer"])
             .set_index("schulnummer"))
    keys = df["schulnummer"].astype(str).str.strip()
    hit = keys.isin(verz6.index)
    if "school_type" in df.columns:
        vocational = df["school_type"].astype(str).str.contains("beruf", case=False)
        for i in df.index[hit & vocational]:
            logger.info(f"    skip {keys[i]} {df.at[i, 'schulname']!r}: vocational, Verz6 count is partial")
        hit &= ~vocational
    for i in df.index[hit]:
        if not _same_school(df.at[i, "schulname"], df.at[i, "plz"] if "plz" in df.columns else "",
                            verz6.loc[keys[i]]):
            logger.warning(f"    skip {keys[i]} {df.at[i, 'schulname']!r}: Verz6 has "
                           f"{verz6.at[keys[i], 'schulname']!r} under this number (wrong ID match)")
            hit[i] = False
    if col not in df.columns:
        df[col] = float("nan")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[hit, col] = keys[hit].map(verz6["schueler_verz6"]).astype(float)
    return df, int(hit.sum())


# ── Fuzzy matching ────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    name = str(name).lower().strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"\s*(frankfurt\s*(am\s*main)?|ffm)$", "", name)
    return name


def best_match(sw_name, sw_plz, verz6_df):
    """Return (schulnummer, score) of best Verzeichnis 6 match, or (None, 0)."""
    best_score = 0.0
    best_nr    = None
    sw_norm    = normalize(sw_name)

    for _, row in verz6_df.iterrows():
        score = SequenceMatcher(None, sw_norm, normalize(str(row["schulname"]))).ratio()
        # Boost if PLZ matches
        if sw_plz and "plz_verz6" in row and str(row["plz_verz6"]).strip() == str(sw_plz).strip():
            score = min(1.0, score + 0.05)
        if score > best_score:
            best_score = score
            best_nr    = str(row["schulnummer"])

    return (best_nr, best_score)


# ── Enrich one CSV file ───────────────────────────────────────────────────────

def enrich_file(csv_path: Path, verz6_df: pd.DataFrame) -> pd.DataFrame:
    if not csv_path.exists():
        logger.warning(f"  Not found: {csv_path}, skipping")
        return None

    df = pd.read_csv(csv_path)
    logger.info(f"  Enriching {csv_path.name} ({len(df)} schools)...")

    # Ensure columns exist
    for col in ["schulnummer", "ndh_count"]:
        if col not in df.columns:
            df[col] = None

    matched = 0
    generated = 0

    for idx, row in df.iterrows():
        sw_name = str(row.get("schulname", ""))
        sw_plz  = str(row.get("plz", ""))

        # Skip schulnummer matching if already has a real one; still try to fill stats
        existing = str(row.get("schulnummer", "") or "")
        needs_nr = not existing or existing.startswith("SW-") or existing in {"nan", "None", ""}

        if needs_nr:
            nr, score = best_match(sw_name, sw_plz, verz6_df)
        else:
            # Already has schulnummer — find its Verz6 row directly
            nr = existing
            score = 1.0

        if score >= 0.75 and nr:
            match_row = verz6_df[verz6_df["schulnummer"] == nr]
            if not match_row.empty:
                if needs_nr:
                    df.at[idx, "schulnummer"] = nr
                    matched += 1
                # Fill ndh_count if missing
                if "ndh_count" in verz6_df.columns:
                    current_ndh = df.at[idx, "ndh_count"] if "ndh_count" in df.columns else None
                    if pd.isna(current_ndh) or current_ndh in {None, ""}:
                        df.at[idx, "ndh_count"] = match_row.iloc[0]["ndh_count"]
            elif needs_nr:
                slug = str(row.get("sw_portal_slug", "")) or re.sub(r"[^a-z0-9-]", "-", sw_name.lower())
                df.at[idx, "schulnummer"] = f"SW-{slug}"
                generated += 1
        elif needs_nr:
            slug = str(row.get("sw_portal_slug", "")) or re.sub(r"[^a-z0-9-]", "-", sw_name.lower())
            df.at[idx, "schulnummer"] = f"SW-{slug}"
            generated += 1
            logger.debug(f"    No match for {sw_name!r} (best={score:.2f}) → generated ID")

    df, written = apply_verz6_counts(df, verz6_df)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"  schulnummer: {matched} from Verz6 + {generated} generated")
    logger.info(f"  schueler_{verz6_df.attrs['school_year']}: {written} from Verz6 (official)")
    return df


def apply_prior_edition(csv_path: Path, prior_df: pd.DataFrame):
    """Write the previous edition's counts (one school year earlier) on schulnummer."""
    if not csv_path.exists():
        return
    df = pd.read_csv(csv_path)
    if "schulnummer" not in df.columns:
        return
    df, written = apply_verz6_counts(df, prior_df)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"  schueler_{prior_df.attrs['school_year']}: {written} from prior edition → {csv_path.name}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Verzeichnis 6 Enrichment (schulnummer + ndH + schueler)")
    logger.info("=" * 60)

    edition, path = get_verz6()
    verz6 = load_verz6(path)

    fnames = ["frankfurt_primary_schools.csv",
              "frankfurt_secondary_schools.csv",
              "frankfurt_vocational_schools.csv"]

    for fname in fnames:
        logger.info(f"\n── {fname} ──")
        enrich_file(RAW_DIR / fname, verz6)

    # Previous edition = previous school year (archive only; Hessen deletes old files)
    logger.info(f"\n── Prior edition ({edition - 1}) ──")
    try:
        _, prior_path = get_verz6(edition - 1)
    except FileNotFoundError as e:
        logger.warning(f"  {e} — prior-year student counts left as they are")
    else:
        prior_df = load_verz6(prior_path)
        for fname in fnames:
            apply_prior_edition(RAW_DIR / fname, prior_df)

    logger.info("\nVerzeichnis 6 enrichment complete.")


if __name__ == "__main__":
    main()
