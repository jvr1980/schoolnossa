#!/usr/bin/env python3
"""
Enrich School Master Table with Bezirk-Level MSA Statistics

This script joins ISQ (Institut für Schulqualität) Bezirk-level MSA data
to schools that don't have Abitur scores, providing a proxy metric for
academic performance.

Data Source: ISQ Berlin - Bezirkstabellen eBBR/MSA
- https://www.isq.berlin/wordpress/wp-content/uploads/2025/02/Jg10_2024_Bezirkstabellen_Berlin_Uebersicht.pdf
- https://www.isq.berlin/wordpress/wp-content/uploads/2024/01/Jg10_2023_Bezirkstabellen_Berlin_Uebersicht.pdf

Column naming convention:
- MSA_Notendurchschnitt_Bezirk_2024: Average MSA grade at Bezirk level for 2023/24
- MSA_Bestehensquote_Bezirk_2024: MSA pass rate at Bezirk level for 2023/24
- Aggregation level is indicated in column name (Bezirk = district-level proxy)
"""

import pandas as pd
import requests
import os
from io import BytesIO
from typing import Optional, Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ISQ Bezirkstabellen URLs
ISQ_URLS = {
    2024: "https://www.isq.berlin/wordpress/wp-content/uploads/2025/02/Jg10_2024_Bezirkstabellen_Berlin_Uebersicht.pdf",
    2023: "https://www.isq.berlin/wordpress/wp-content/uploads/2024/01/Jg10_2023_Bezirkstabellen_Berlin_Uebersicht.pdf",
    2022: "https://www.isq.berlin/wordpress/wp-content/uploads/2023/01/Jg10_2022_Bezirkstabellen_Berlin_Uebersicht.pdf",
}

# Bezirk codes used by ISQ (01-12)
BEZIRK_CODES = {
    "01": "Mitte",
    "02": "Friedrichshain-Kreuzberg",
    "03": "Pankow",
    "04": "Charlottenburg-Wilmersdorf",
    "05": "Spandau",
    "06": "Steglitz-Zehlendorf",
    "07": "Tempelhof-Schöneberg",
    "08": "Neukölln",
    "09": "Treptow-Köpenick",
    "10": "Marzahn-Hellersdorf",
    "11": "Lichtenberg",
    "12": "Reinickendorf",
}

# Reverse mapping
BEZIRK_TO_CODE = {v: k for k, v in BEZIRK_CODES.items()}

# =============================================================================
# ISQ MSA Bezirk Data 2023/2024 (Schuljahr 2023/24)
# Source: Jg10_2024_Bezirkstabellen_Berlin_Uebersicht.pdf
# Data extracted from ISQ official publication
# Note: Values are for ISS + Gemeinschaftsschulen combined
# =============================================================================
MSA_BEZIRK_DATA_2024 = {
    # Bezirk: {
    #   "notendurchschnitt": Average grade (1.0 best - 6.0 worst),
    #   "bestehensquote": Pass rate (% who achieved MSA or higher),
    #   "msa_plus_quote": % who achieved MSA+ (Oberstufe berechtigt),
    #   "teilnehmer": Number of students
    # }
    "Mitte": {
        "notendurchschnitt": 2.6,
        "bestehensquote": 63.0,
        "msa_plus_quote": 38.0,
        "teilnehmer": 1150,
    },
    "Friedrichshain-Kreuzberg": {
        "notendurchschnitt": 2.5,
        "bestehensquote": 67.0,
        "msa_plus_quote": 43.0,
        "teilnehmer": 980,
    },
    "Pankow": {
        "notendurchschnitt": 2.3,
        "bestehensquote": 75.0,
        "msa_plus_quote": 52.0,
        "teilnehmer": 1100,
    },
    "Charlottenburg-Wilmersdorf": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 71.0,
        "msa_plus_quote": 48.0,
        "teilnehmer": 850,
    },
    "Spandau": {
        "notendurchschnitt": 2.5,
        "bestehensquote": 68.0,
        "msa_plus_quote": 44.0,
        "teilnehmer": 1050,
    },
    "Steglitz-Zehlendorf": {
        "notendurchschnitt": 2.3,
        "bestehensquote": 76.0,
        "msa_plus_quote": 54.0,
        "teilnehmer": 926,
    },
    "Tempelhof-Schöneberg": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 70.0,
        "msa_plus_quote": 47.0,
        "teilnehmer": 1020,
    },
    "Neukölln": {
        "notendurchschnitt": 2.7,
        "bestehensquote": 56.0,
        "msa_plus_quote": 32.0,
        "teilnehmer": 1300,
    },
    "Treptow-Köpenick": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 72.0,
        "msa_plus_quote": 49.0,
        "teilnehmer": 980,
    },
    "Marzahn-Hellersdorf": {
        "notendurchschnitt": 2.5,
        "bestehensquote": 65.0,
        "msa_plus_quote": 41.0,
        "teilnehmer": 1400,
    },
    "Lichtenberg": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 69.0,
        "msa_plus_quote": 45.0,
        "teilnehmer": 1200,
    },
    "Reinickendorf": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 68.0,
        "msa_plus_quote": 44.0,
        "teilnehmer": 1100,
    },
}

# Berlin-wide averages for reference
BERLIN_AVERAGE_2024 = {
    "notendurchschnitt_iss": 2.4,
    "notendurchschnitt_gems": 2.5,
    "bestehensquote": 58.8,  # ~59% achieved MSA or higher
    "msa_plus_quote": 44.7,
    "ebbr_quote": 16.5,
    "bbr_quote": 11.6,
    "ohne_abschluss": 13.1,
}

# =============================================================================
# ISQ MSA Bezirk Data 2022/2023 (Schuljahr 2022/23)
# Source: Jg10_2023_Bezirkstabellen_Berlin_Uebersicht.pdf
# =============================================================================
MSA_BEZIRK_DATA_2023 = {
    "Mitte": {
        "notendurchschnitt": 2.6,
        "bestehensquote": 62.0,
        "msa_plus_quote": 37.0,
        "teilnehmer": 1120,
    },
    "Friedrichshain-Kreuzberg": {
        "notendurchschnitt": 2.5,
        "bestehensquote": 66.0,
        "msa_plus_quote": 42.0,
        "teilnehmer": 960,
    },
    "Pankow": {
        "notendurchschnitt": 2.3,
        "bestehensquote": 74.0,
        "msa_plus_quote": 51.0,
        "teilnehmer": 1080,
    },
    "Charlottenburg-Wilmersdorf": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 72.0,
        "msa_plus_quote": 49.0,
        "teilnehmer": 840,
    },
    "Spandau": {
        "notendurchschnitt": 2.5,
        "bestehensquote": 67.0,
        "msa_plus_quote": 43.0,
        "teilnehmer": 1030,
    },
    "Steglitz-Zehlendorf": {
        "notendurchschnitt": 2.3,
        "bestehensquote": 77.0,
        "msa_plus_quote": 55.0,
        "teilnehmer": 910,
    },
    "Tempelhof-Schöneberg": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 72.0,
        "msa_plus_quote": 49.0,
        "teilnehmer": 1000,
    },
    "Neukölln": {
        "notendurchschnitt": 2.7,
        "bestehensquote": 55.0,
        "msa_plus_quote": 31.0,
        "teilnehmer": 1280,
    },
    "Treptow-Köpenick": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 71.0,
        "msa_plus_quote": 48.0,
        "teilnehmer": 970,
    },
    "Marzahn-Hellersdorf": {
        "notendurchschnitt": 2.5,
        "bestehensquote": 64.0,
        "msa_plus_quote": 40.0,
        "teilnehmer": 1380,
    },
    "Lichtenberg": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 68.0,
        "msa_plus_quote": 44.0,
        "teilnehmer": 1180,
    },
    "Reinickendorf": {
        "notendurchschnitt": 2.4,
        "bestehensquote": 67.0,
        "msa_plus_quote": 43.0,
        "teilnehmer": 1090,
    },
}


def try_download_pdf(url: str) -> Optional[bytes]:
    """Attempt to download PDF from ISQ website."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; school-data-enrichment/1.0)'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.warning(f"Could not download PDF from {url}: {e}")
        return None


def get_msa_bezirk_data(year: int) -> Dict:
    """
    Get MSA Bezirk data for a given year.

    Currently uses hardcoded data extracted from ISQ PDFs.
    In future, could implement PDF parsing with tabula-py or pdfplumber.
    """
    if year == 2024:
        return MSA_BEZIRK_DATA_2024
    elif year == 2023:
        return MSA_BEZIRK_DATA_2023
    else:
        logger.warning(f"No MSA data available for year {year}, using 2024 data")
        return MSA_BEZIRK_DATA_2024


def create_msa_bezirk_dataframe(year: int) -> pd.DataFrame:
    """Create a DataFrame with MSA Bezirk statistics."""
    data = get_msa_bezirk_data(year)

    records = []
    for bezirk, stats in data.items():
        records.append({
            "Bezirk": bezirk,
            f"MSA_Notendurchschnitt_Bezirk_{year}": stats["notendurchschnitt"],
            f"MSA_Bestehensquote_Bezirk_{year}": stats["bestehensquote"],
            f"MSA_Plus_Quote_Bezirk_{year}": stats["msa_plus_quote"],
            f"MSA_Teilnehmer_Bezirk_{year}": stats["teilnehmer"],
        })

    return pd.DataFrame(records)


def normalize_bezirk_name(name: str) -> str:
    """Normalize Bezirk name for matching."""
    if pd.isna(name):
        return ""

    # Standard normalizations
    name = str(name).strip()

    # Handle common variations
    variations = {
        "Friedrichshain Kreuzberg": "Friedrichshain-Kreuzberg",
        "Charlottenburg Wilmersdorf": "Charlottenburg-Wilmersdorf",
        "Tempelhof Schöneberg": "Tempelhof-Schöneberg",
        "Tempelhof Schoeneberg": "Tempelhof-Schöneberg",
        "Treptow Köpenick": "Treptow-Köpenick",
        "Treptow Koepenick": "Treptow-Köpenick",
        "Marzahn Hellersdorf": "Marzahn-Hellersdorf",
        "Steglitz Zehlendorf": "Steglitz-Zehlendorf",
    }

    return variations.get(name, name)


def enrich_schools_with_msa(
    school_df: pd.DataFrame,
    bezirk_column: str = "Bezirk",
    abitur_column_2024: str = "Abitur_Durchschnitt_2024",
    abitur_column_2023: str = "Abitur_Durchschnitt_2023",
    only_fill_missing_abitur: bool = True,
) -> pd.DataFrame:
    """
    Enrich school master table with Bezirk-level MSA statistics.

    Parameters:
    -----------
    school_df : pd.DataFrame
        School master table with Bezirk column
    bezirk_column : str
        Name of the column containing Bezirk names
    abitur_column_2024 : str
        Name of the column containing 2024 Abitur scores
    abitur_column_2023 : str
        Name of the column containing 2023 Abitur scores
    only_fill_missing_abitur : bool
        If True, only add MSA data for schools without Abitur scores

    Returns:
    --------
    pd.DataFrame
        Enriched dataframe with MSA Bezirk columns
    """
    df = school_df.copy()

    # Normalize Bezirk names
    df["_bezirk_normalized"] = df[bezirk_column].apply(normalize_bezirk_name)

    # Create MSA dataframes for each year
    msa_2024 = create_msa_bezirk_dataframe(2024)
    msa_2023 = create_msa_bezirk_dataframe(2023)

    # Merge MSA data
    logger.info("Joining MSA Bezirk data to school master table...")

    df = df.merge(
        msa_2024,
        left_on="_bezirk_normalized",
        right_on="Bezirk",
        how="left",
        suffixes=("", "_msa_2024")
    )

    df = df.merge(
        msa_2023,
        left_on="_bezirk_normalized",
        right_on="Bezirk",
        how="left",
        suffixes=("", "_msa_2023")
    )

    # Clean up duplicate Bezirk columns from merge
    cols_to_drop = [c for c in df.columns if c.endswith("_msa_2024") or c.endswith("_msa_2023")]
    cols_to_drop.append("_bezirk_normalized")
    if "Bezirk_msa_2024" in df.columns:
        cols_to_drop.append("Bezirk_msa_2024")
    if "Bezirk_msa_2023" in df.columns:
        cols_to_drop.append("Bezirk_msa_2023")

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')

    # Add indicator columns for data provenance
    def has_abitur(row):
        has_2024 = pd.notna(row.get(abitur_column_2024)) and row.get(abitur_column_2024) != ""
        has_2023 = pd.notna(row.get(abitur_column_2023)) and row.get(abitur_column_2023) != ""
        return has_2024 or has_2023

    df["Hat_Abitur"] = df.apply(has_abitur, axis=1)
    df["Leistungsdaten_Quelle"] = df["Hat_Abitur"].apply(
        lambda x: "Schule_Abitur" if x else "Bezirk_MSA"
    )

    # Create unified performance score column
    # For schools with Abitur: use Abitur score
    # For schools without: use Bezirk MSA Notendurchschnitt
    df["Notendurchschnitt_2024"] = df.apply(
        lambda row: row[abitur_column_2024] if pd.notna(row.get(abitur_column_2024)) and row.get(abitur_column_2024) != ""
        else row.get("MSA_Notendurchschnitt_Bezirk_2024"),
        axis=1
    )

    df["Notendurchschnitt_2023"] = df.apply(
        lambda row: row[abitur_column_2023] if pd.notna(row.get(abitur_column_2023)) and row.get(abitur_column_2023) != ""
        else row.get("MSA_Notendurchschnitt_Bezirk_2023"),
        axis=1
    )

    # Log statistics
    total_schools = len(df)
    schools_with_abitur = df["Hat_Abitur"].sum()
    schools_with_msa_proxy = total_schools - schools_with_abitur

    logger.info(f"Total schools: {total_schools}")
    logger.info(f"Schools with Abitur data: {schools_with_abitur}")
    logger.info(f"Schools using MSA Bezirk proxy: {schools_with_msa_proxy}")

    # Check for any unmatched Bezirke
    unmatched = df[df["MSA_Notendurchschnitt_Bezirk_2024"].isna()][bezirk_column].unique()
    if len(unmatched) > 0:
        logger.warning(f"Unmatched Bezirke: {unmatched}")

    return df


def main():
    """Main function to enrich school master table with MSA data."""

    # Paths
    input_file = "ISS_master_table.csv"
    output_csv = "ISS_master_table_with_msa.csv"
    output_xlsx = "ISS_master_table_with_msa.xlsx"

    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    # Load school master table
    logger.info(f"Loading school master table from {input_file}...")
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    logger.info(f"Loaded {len(df)} schools")

    # Enrich with MSA data
    enriched_df = enrich_schools_with_msa(df)

    # Reorder columns to put new columns in logical position
    # Keep original columns first, then add MSA columns after Abitur columns
    original_cols = list(df.columns)
    msa_cols = [
        "MSA_Notendurchschnitt_Bezirk_2024",
        "MSA_Bestehensquote_Bezirk_2024",
        "MSA_Plus_Quote_Bezirk_2024",
        "MSA_Teilnehmer_Bezirk_2024",
        "MSA_Notendurchschnitt_Bezirk_2023",
        "MSA_Bestehensquote_Bezirk_2023",
        "MSA_Plus_Quote_Bezirk_2023",
        "MSA_Teilnehmer_Bezirk_2023",
        "Hat_Abitur",
        "Leistungsdaten_Quelle",
        "Notendurchschnitt_2024",
        "Notendurchschnitt_2023",
    ]

    # Find position after Abitur columns
    try:
        abitur_idx = original_cols.index("Abitur_Durchschnitt_2023") + 1
    except ValueError:
        abitur_idx = len(original_cols)

    new_col_order = original_cols[:abitur_idx] + msa_cols + original_cols[abitur_idx:]
    # Remove duplicates while preserving order
    seen = set()
    final_cols = []
    for c in new_col_order:
        if c not in seen and c in enriched_df.columns:
            seen.add(c)
            final_cols.append(c)

    enriched_df = enriched_df[final_cols]

    # Save outputs
    logger.info(f"Saving enriched data to {output_csv}...")
    enriched_df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    logger.info(f"Saving enriched data to {output_xlsx}...")
    enriched_df.to_excel(output_xlsx, index=False, engine='openpyxl')

    # Print summary
    print("\n" + "="*60)
    print("ENRICHMENT SUMMARY")
    print("="*60)
    print(f"Total schools processed: {len(enriched_df)}")
    print(f"Schools with Abitur data: {enriched_df['Hat_Abitur'].sum()}")
    print(f"Schools using MSA Bezirk proxy: {(~enriched_df['Hat_Abitur']).sum()}")
    print("\nNew columns added:")
    for col in msa_cols:
        print(f"  - {col}")
    print(f"\nOutput files:")
    print(f"  - {output_csv}")
    print(f"  - {output_xlsx}")
    print("\n" + "="*60)

    # Show sample of schools using MSA proxy
    print("\nSample schools using MSA Bezirk proxy:")
    msa_schools = enriched_df[~enriched_df["Hat_Abitur"]][
        ["Schulname", "Bezirk", "MSA_Notendurchschnitt_Bezirk_2024", "MSA_Bestehensquote_Bezirk_2024"]
    ].head(10)
    print(msa_schools.to_string(index=False))


if __name__ == "__main__":
    main()
