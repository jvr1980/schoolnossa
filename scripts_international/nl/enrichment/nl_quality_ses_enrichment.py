#!/usr/bin/env python3
"""
NL Phase 3b: Inspectorate quality ratings + CBS disadvantage (SES) score.

Fills two Berlin-parity fields the NL pipeline never had:

  school_quality_rating / school_quality_rating_national  <- Onderwijsinspectie
  deprivation_index / deprivation_index_national          <- CBS achterstandsscore

**Onderwijsinspectie oordelen** (CC0, peildatum 1 Sept 2026)
  ODS workbook, one row per *afdeling* (HAVO / VWO / VMBOGT separately), so
  ~3.9k VO rows collapse to ~1.6k vestigingen. We aggregate worst-case: a school
  with one 'Onvoldoende' track is not "Voldoende overall". Only ~42% of VO
  vestigingen carry a real oordeel; the rest are 'Geen eindoordeel' and stay
  NULL rather than being rendered as a negative signal. Judgements can be up to
  10 years old, so the assessment date ships alongside for staleness display.

**CBS achterstandsscore** (peildatum 1 Oct 2024, published Feb 2026)
  Use "Tabel 2" (zonder drempel): Tabel 1 zeroes out ~43% of schools below the
  funding threshold and is unusable for ranking. The raw score scales with
  school size, so normalise per pupil before bucketing into Berlin's 1-10
  belastungsstufe.

Input:  data_nl/intermediate/nl_schools_with_traffic.csv (or any prior stage)
Output: data_nl/intermediate/nl_schools_with_quality.csv
"""

import io
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INSPECTIE_URL = (
    "https://www.onderwijsinspectie.nl/site/binaries/site-content/collections/"
    "documents/2026/09/01/oordelen-1-september-2026/"
    "20260901-oordeel-en-standaard-po-so-vo.ods"
)
CBS_SES_URL = "https://www.cbs.nl/-/media/_excel/2026/02/achterstandsscores_scholen_vhv_2024.xlsx"
CBS_SES_PRO_URL = "https://www.cbs.nl/-/media/_excel/2026/02/achterstandsscores_scholen_pro_2024.xlsx"
CBS_SES_YEAR = "2024"

# Inspectorate vocabulary -> normalised core-schema rating.
# 'Geen eindoordeel' / 'Basistoezicht' are administrative states, not verdicts.
QUALITY_MAP = {
    "Goed": "excellent",
    "Voldoende": "adequate",
    "Onvoldoende": "inadequate",
    "Zeer zwak": "inadequate",
}
# Worst-first: a school is only as good as its weakest inspected track.
QUALITY_SEVERITY = ["Zeer zwak", "Onvoldoende", "Voldoende", "Goed"]


def _fetch(url: str, cache_path: Path) -> Path:
    if cache_path.exists() and cache_path.stat().st_size > 1024:
        logger.info(f"  Using cached {cache_path.name}")
        return cache_path
    logger.info(f"  Downloading {url.rsplit('/', 1)[-1]}...")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(resp.content)
    logger.info(f"    {len(resp.content) / 1024:.0f} KB")
    return cache_path


def load_inspectorate() -> pd.DataFrame:
    """Per-vestiging worst-case oordeel + assessment date."""
    logger.info("Onderwijsinspectie oordelen...")
    path = _fetch(INSPECTIE_URL, CACHE_DIR / "inspectie_oordelen_20260901.ods")
    df = pd.read_excel(path, engine="odf", dtype=str)
    logger.info(f"  {len(df)} rows, {len(df.columns)} columns")

    vo = df[df["Sector"].astype(str).str.upper().eq("VO")].copy()
    logger.info(f"  VO rows: {len(vo)}")

    vo["_brin6"] = (vo["BRIN"].astype(str).str.strip().str.upper()
                    + vo["Vestiging"].astype(str).str.strip().str.zfill(2))
    vo["_rank"] = vo["KwaliteitOnderwijs"].map(
        {v: i for i, v in enumerate(QUALITY_SEVERITY)})
    rated = vo[vo["_rank"].notna()].copy()
    logger.info(f"  Rows with a real oordeel: {len(rated)} "
                f"({rated['KwaliteitOnderwijs'].value_counts().to_dict()})")

    rated = rated.sort_values("_rank")
    agg = rated.groupby("_brin6").agg(
        oordeel=("KwaliteitOnderwijs", "first"),          # worst track
        assessed=("Vaststellingsdatum", "max"),           # newest assessment
        tracks_rated=("KwaliteitOnderwijs", "size"),
    ).reset_index()
    logger.info(f"  Distinct vestigingen with rating: {len(agg)}")
    return agg


def load_cbs_ses() -> pd.DataFrame:
    """Per-vestiging achterstandsscore (zonder drempel) + pupil counts."""
    logger.info("CBS achterstandsscores...")
    frames = []
    for url, label in ((CBS_SES_URL, "vhv"), (CBS_SES_PRO_URL, "pro")):
        try:
            path = _fetch(url, CACHE_DIR / f"cbs_achterstand_{label}_{CBS_SES_YEAR}.xlsx")
        except requests.HTTPError as exc:
            logger.warning(f"  {label}: download failed ({exc}) — skipping")
            continue

        # Tabel 1 carries the pupil count; Tabel 2 the continuous score.
        # Header on row 3, then a spacer row before the data.
        # calamine, not openpyxl: these CBS workbooks ship a styles.xml that
        # openpyxl rejects as invalid XML.
        def _sheet(name: str) -> pd.DataFrame:
            df_ = pd.read_excel(path, sheet_name=name, header=3,
                                engine="calamine", dtype=str).dropna(how="all")
            df_.columns = [str(c).strip() for c in df_.columns]
            return df_

        sheets = pd.ExcelFile(path, engine="calamine").sheet_names
        t1 = _sheet("Tabel 1")
        pupils_col = next((c for c in t1.columns if "leerling" in c.lower()), None)

        # The vhv workbook has a "zonder drempel" Tabel 2 — the only version
        # usable for ranking, since Tabel 1 zeroes every school below the
        # funding threshold. The pro workbook ships Tabel 1 only.
        if "Tabel 2" in sheets:
            score_src = _sheet("Tabel 2")
            thresholded = False
        else:
            score_src = t1
            thresholded = True
            logger.info(f"  {label}: no 'zonder drempel' sheet — using Tabel 1 "
                        f"(threshold-zeroed)")

        score_col = next((c for c in score_src.columns
                          if "achterstand" in c.lower()), None)
        if score_col is None:
            logger.warning(f"  {label}: no score column found — skipping")
            continue

        merged = pd.DataFrame({
            "_brin6": score_src[score_src.columns[0]].astype(str).str.strip().str.upper(),
            "ses_score_raw": pd.to_numeric(score_src[score_col], errors="coerce"),
            "ses_thresholded": thresholded,
        }).merge(
            pd.DataFrame({
                "_brin6": t1[t1.columns[0]].astype(str).str.strip().str.upper(),
                "ses_pupils": pd.to_numeric(t1[pupils_col], errors="coerce")
                if pupils_col else np.nan,
            }),
            on="_brin6", how="left",
        )
        merged = merged[merged["_brin6"].str.match(r"^[0-9]{2}[A-Z]{2}[0-9]{2}$", na=False)]
        logger.info(f"  {label}: {len(merged)} vestigingen")
        frames.append(merged)

    if not frames:
        return pd.DataFrame(columns=["_brin6", "ses_score_raw", "ses_pupils"])

    out = pd.concat(frames, ignore_index=True).drop_duplicates("_brin6")
    # Score scales with school size — per-pupil makes schools comparable.
    out["ses_per_pupil"] = out["ses_score_raw"] / out["ses_pupils"].where(out["ses_pupils"] > 0)
    logger.info(f"  Combined: {len(out)} vestigingen, "
                f"per-pupil median {out['ses_per_pupil'].median():.3f}")
    return out


def enrich(schools: pd.DataFrame) -> pd.DataFrame:
    """Attach quality rating + SES score on BRIN6."""
    key = "vestiging_code" if "vestiging_code" in schools.columns else "school_id"
    schools["_brin6"] = schools[key].astype(str).str.strip().str.upper().str.replace(
        r"[^A-Z0-9]", "", regex=True)

    insp = load_inspectorate()
    if not insp.empty:
        schools = schools.merge(insp, on="_brin6", how="left")
        schools["school_quality_rating_national"] = schools["oordeel"]
        schools["school_quality_rating"] = schools["oordeel"].map(QUALITY_MAP)
        schools["school_quality_assessed_date"] = pd.to_datetime(
            schools["assessed"], format="%Y%m%d", errors="coerce").dt.date.astype("object")
        schools["school_quality_source"] = "Onderwijsinspectie oordelen (CC0)"
        schools = schools.drop(columns=["oordeel", "assessed"], errors="ignore")
        got = schools["school_quality_rating"].notna().sum()
        logger.info(f"Schools with quality rating: {got}/{len(schools)} "
                    f"({100 * got / len(schools):.0f}%)")

    ses = load_cbs_ses()
    if not ses.empty:
        schools = schools.merge(ses, on="_brin6", how="left")
        schools["deprivation_index_national"] = schools["ses_score_raw"]
        # Berlin's belastungsstufe is a 1-10 band, higher = more disadvantage.
        per_pupil = schools["ses_per_pupil"]
        if per_pupil.notna().sum() > 10:
            schools["deprivation_index"] = pd.qcut(
                per_pupil.rank(method="first"), 10, labels=range(1, 11)
            ).astype("float")
            schools.loc[per_pupil.isna(), "deprivation_index"] = np.nan
        schools["deprivation_data_year"] = CBS_SES_YEAR
        schools["deprivation_data_source"] = "CBS achterstandsscore per vo-vestiging"
        got = schools["deprivation_index_national"].notna().sum()
        logger.info(f"Schools with SES score: {got}/{len(schools)} "
                    f"({100 * got / len(schools):.0f}%)")

    return schools.drop(
        columns=["_brin6", "ses_score_raw", "ses_pupils", "ses_per_pupil",
                 "ses_thresholded"],
        errors="ignore")


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 3b: Inspectorate quality + CBS SES")
    logger.info("=" * 60)

    # Free chain order: …crime -> demographics -> here. Never the POI output.
    for candidate in ("nl_schools_with_demographics.csv", "nl_schools_with_crime.csv",
                      "nl_schools_with_traffic.csv", "nl_school_master_geocoded.csv"):
        input_path = INTERMEDIATE_DIR / candidate
        if input_path.exists():
            break
    else:
        logger.error("No input intermediate found")
        sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools from {input_path.name}")

    enriched = enrich(schools)
    output = INTERMEDIATE_DIR / "nl_schools_with_quality.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
