#!/usr/bin/env python3
"""
Convert the Kriminalitätsatlas Berlin XLSX into data_berlin/raw/bezirk_crime_statistics.csv.

This is the previously missing step between crawl_crime_daten_berlin.py (which
downloads the atlas workbook into data_berlin/raw/crime_data/) and
scripts_shared/enrichment/enrich_berlin_schools_with_crime.py (which expects a
per-Bezirk CSV that no script in the repo produced).

Semantics reverse-engineered to match the existing final parquet exactly:
- Year columns come from the HZ_<year> sheets (Häufigkeitszahlen, cases per
  100k inhabitants) — NOT the raw Fallzahlen counts.
- The two most recent HZ years in the workbook are used.
- *_avg           = mean of the two years
- *_yoy_pct       = (latest - previous) / previous * 100
- violent_crime_avg = mean over years of (Raub + Straßenraub + Körperverletzung
                      insgesamt + gefährl./schwere Körperverletzung)
- safety_rank     = rank of violent_crime_avg ascending (1 = safest)
- safety_category = rank tertiles: 1-4 safe, 5-8 moderate, 9-12 elevated

Output columns carry no 'crime_' prefix — the enrichment script adds it.
"""

import logging
import re
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CRIME_DATA_DIR = PROJECT_ROOT / "data_berlin" / "raw" / "crime_data"
OUTPUT_CSV = PROJECT_ROOT / "data_berlin" / "raw" / "bezirk_crime_statistics.csv"

# German atlas header (row 4, with embedded newlines) -> English family name.
# Matching is done on a whitespace-normalized version of the header.
CATEGORY_MAP = {  # keys are headers with ALL whitespace stripped (see _norm)
    'Straftaten-insgesamt-': 'total_crimes',
    'Raub': 'robbery',
    'Straßenraub,Handtaschen-raub': 'street_robbery',
    'Körper-verletzungen-insgesamt-': 'assault',
    'Gefährl.undschwereKörper-verletzung': 'aggravated_assault',
    'Freiheits-beraubung,Nötigung,Bedrohung,Nachstellung': 'threats_coercion',
    'Fahrrad-diebstahl': 'bike_theft',
    'Rauschgift-delikte': 'drug_offenses',
    'Kieztaten': 'neighborhood_crimes',
}
VIOLENT_COMPONENTS = ['robbery', 'street_robbery', 'assault', 'aggravated_assault']


def _norm(header: str) -> str:
    # Strip ALL whitespace: the atlas headers contain soft hyphen-linebreaks
    # that pandas renders inconsistently ('Fahrrad-\ndiebstahl' vs 'Fahrrad- diebstahl').
    return re.sub(r'\s+', '', str(header))


def find_workbook() -> Path:
    candidates = sorted(
        p for p in CRIME_DATA_DIR.glob("Crime_Statistics_Districts_*.xlsx")
        if not p.name.startswith(('~$', '._'))
    )
    if not candidates:
        raise FileNotFoundError(
            f"No Crime_Statistics_Districts_*.xlsx in {CRIME_DATA_DIR} — "
            "run scripts_berlin/scrapers/crawl_crime_daten_berlin.py first")
    return candidates[-1]  # dated filenames sort chronologically


def load_hz_year(xlsx: pd.ExcelFile, year: int) -> pd.DataFrame:
    """Return one row per Bezirk with english category columns for one HZ year."""
    df = pd.read_excel(xlsx, f"HZ_{year}", header=4, dtype={0: str})
    df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.zfill(6)
    bezirke = df[df.iloc[:, 0].str.match(r'^\d{2}0000$')].copy()
    if len(bezirke) != 12:
        raise ValueError(f"HZ_{year}: expected 12 Bezirk rows, got {len(bezirke)}")

    norm_cols = {_norm(c): c for c in df.columns}
    out = pd.DataFrame({
        'lor_code': bezirke.iloc[:, 0].values,
        'bezirk_name': bezirke.iloc[:, 1].astype(str).str.strip().values,
    })
    for german, english in CATEGORY_MAP.items():
        if german not in norm_cols:
            raise KeyError(f"HZ_{year}: column {german!r} not found; "
                           f"available: {sorted(norm_cols)[:6]}...")
        out[f'{english}_{year}'] = pd.to_numeric(
            bezirke[norm_cols[german]].values, errors='coerce')
    return out


def convert(workbook_path: Path = None) -> pd.DataFrame:
    workbook_path = workbook_path or find_workbook()
    logger.info(f"Reading {workbook_path.name}")
    xlsx = pd.ExcelFile(workbook_path)

    hz_years = sorted(int(m.group(1)) for s in xlsx.sheet_names
                      if (m := re.match(r'^HZ_(\d{4})$', s)))
    if len(hz_years) < 2:
        raise ValueError(f"Need at least two HZ_<year> sheets, found {hz_years}")
    prev_year, latest_year = hz_years[-2], hz_years[-1]
    logger.info(f"Using HZ years {prev_year} + {latest_year} "
                f"(workbook covers {hz_years[0]}-{hz_years[-1]})")

    df = load_hz_year(xlsx, prev_year).merge(
        load_hz_year(xlsx, latest_year), on=['lor_code', 'bezirk_name'])

    for english in CATEGORY_MAP.values():
        prev_col, latest_col = f'{english}_{prev_year}', f'{english}_{latest_year}'
        df[f'{english}_avg'] = (df[prev_col] + df[latest_col]) / 2
        df[f'{english}_yoy_pct'] = (df[latest_col] - df[prev_col]) / df[prev_col] * 100

    df['violent_crime_avg'] = sum(df[f'{c}_avg'] for c in VIOLENT_COMPONENTS)
    df['safety_rank'] = df['violent_crime_avg'].rank(method='first').astype(int)
    df['safety_category'] = pd.cut(df['safety_rank'], bins=[0, 4, 8, 12],
                                   labels=['safe', 'moderate', 'elevated']).astype(str)

    # Round derived floats to one decimal, matching the historical output
    for col in df.columns:
        if col.endswith(('_avg', '_yoy_pct')):
            df[col] = df[col].round(1)
    return df


def main():
    df = convert()
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    logger.info(f"Wrote {OUTPUT_CSV} ({len(df)} bezirke, {len(df.columns)} columns)")
    print(df[['bezirk_name', 'total_crimes_avg', 'violent_crime_avg',
              'safety_rank', 'safety_category']]
          .sort_values('safety_rank').to_string(index=False))


if __name__ == '__main__':
    sys.exit(main())
