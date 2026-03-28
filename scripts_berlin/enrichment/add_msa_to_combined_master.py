#!/usr/bin/env python3
"""
Add MSA Bezirk-Level Statistics to Combined School Master Table

This script enriches the combined_schools_with_metadata.csv with Bezirk-level
MSA (Mittlerer Schulabschluss) statistics for schools that don't offer/have Abitur data.

Logic:
- Schools WITH Abitur data: Keep their Abitur scores, MSA columns remain empty
- Schools WITHOUT Abitur data: Fill MSA Bezirk columns as proxy metrics

Data Source: ISQ Berlin - Bezirkstabellen eBBR/MSA 2023/2024
https://www.isq.berlin/wordpress/wp-content/uploads/2025/02/Jg10_2024_Bezirkstabellen_Berlin_Uebersicht.pdf
"""

import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ISQ MSA Bezirk Data 2023/2024 (Schuljahr 2023/24)
# Source: Jg10_2024_Bezirkstabellen_Berlin_Uebersicht.pdf
# Values are for ISS + Gemeinschaftsschulen combined
# =============================================================================
MSA_BEZIRK_DATA_2024 = {
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

# =============================================================================
# ISQ MSA Bezirk Data 2022/2023 (Schuljahr 2022/23)
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


def normalize_bezirk_name(name: str) -> str:
    """Normalize Bezirk name for matching."""
    if pd.isna(name):
        return ""

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


def has_abitur_data(row) -> bool:
    """Check if school has any Abitur data."""
    abitur_cols = ['abitur_durchschnitt_2024', 'abitur_durchschnitt_2023', 'abitur_durchschnitt_2025']
    for col in abitur_cols:
        if col in row.index:
            val = row[col]
            if pd.notna(val) and val != '' and val != 0:
                return True
    return False


def add_msa_bezirk_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add MSA Bezirk columns to dataframe.
    Only populate for schools WITHOUT Abitur data.
    """
    df = df.copy()

    # Normalize bezirk names for matching
    df['_bezirk_normalized'] = df['bezirk'].apply(normalize_bezirk_name)

    # Initialize new columns with NaN
    new_columns = [
        'msa_notendurchschnitt_bezirk_2024',
        'msa_bestehensquote_bezirk_2024',
        'msa_plus_quote_bezirk_2024',
        'msa_teilnehmer_bezirk_2024',
        'msa_notendurchschnitt_bezirk_2023',
        'msa_bestehensquote_bezirk_2023',
        'msa_plus_quote_bezirk_2023',
        'msa_teilnehmer_bezirk_2023',
        'leistungsdaten_quelle',
        'notendurchschnitt_unified_2024',
        'notendurchschnitt_unified_2023',
    ]

    for col in new_columns:
        df[col] = None

    # Track statistics
    schools_with_abitur = 0
    schools_with_msa_proxy = 0
    unmatched_bezirke = set()

    for idx, row in df.iterrows():
        bezirk = row['_bezirk_normalized']

        if has_abitur_data(row):
            # School has Abitur - use Abitur data
            schools_with_abitur += 1
            df.at[idx, 'leistungsdaten_quelle'] = 'Schule_Abitur'

            # Set unified score from Abitur
            if pd.notna(row.get('abitur_durchschnitt_2024')) and row.get('abitur_durchschnitt_2024') != '':
                df.at[idx, 'notendurchschnitt_unified_2024'] = row['abitur_durchschnitt_2024']
            if pd.notna(row.get('abitur_durchschnitt_2023')) and row.get('abitur_durchschnitt_2023') != '':
                df.at[idx, 'notendurchschnitt_unified_2023'] = row['abitur_durchschnitt_2023']

        else:
            # School has no Abitur - use MSA Bezirk proxy
            schools_with_msa_proxy += 1
            df.at[idx, 'leistungsdaten_quelle'] = 'Bezirk_MSA'

            # Get MSA data for this Bezirk
            msa_2024 = MSA_BEZIRK_DATA_2024.get(bezirk)
            msa_2023 = MSA_BEZIRK_DATA_2023.get(bezirk)

            if msa_2024:
                df.at[idx, 'msa_notendurchschnitt_bezirk_2024'] = msa_2024['notendurchschnitt']
                df.at[idx, 'msa_bestehensquote_bezirk_2024'] = msa_2024['bestehensquote']
                df.at[idx, 'msa_plus_quote_bezirk_2024'] = msa_2024['msa_plus_quote']
                df.at[idx, 'msa_teilnehmer_bezirk_2024'] = msa_2024['teilnehmer']
                df.at[idx, 'notendurchschnitt_unified_2024'] = msa_2024['notendurchschnitt']
            else:
                if bezirk:
                    unmatched_bezirke.add(bezirk)

            if msa_2023:
                df.at[idx, 'msa_notendurchschnitt_bezirk_2023'] = msa_2023['notendurchschnitt']
                df.at[idx, 'msa_bestehensquote_bezirk_2023'] = msa_2023['bestehensquote']
                df.at[idx, 'msa_plus_quote_bezirk_2023'] = msa_2023['msa_plus_quote']
                df.at[idx, 'msa_teilnehmer_bezirk_2023'] = msa_2023['teilnehmer']
                df.at[idx, 'notendurchschnitt_unified_2023'] = msa_2023['notendurchschnitt']

    # Remove helper column
    df = df.drop(columns=['_bezirk_normalized'])

    # Log statistics
    logger.info(f"Schools with Abitur data: {schools_with_abitur}")
    logger.info(f"Schools using MSA Bezirk proxy: {schools_with_msa_proxy}")

    if unmatched_bezirke:
        logger.warning(f"Unmatched Bezirke: {unmatched_bezirke}")

    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns to place MSA columns after Abitur columns."""
    cols = list(df.columns)

    # Find position after abitur columns
    abitur_cols = ['abitur_durchschnitt_2024', 'abitur_durchschnitt_2023', 'abitur_durchschnitt_2025',
                   'abitur_erfolgsquote_2024', 'abitur_erfolgsquote_2025']

    # Find last abitur column position
    last_abitur_idx = 0
    for ac in abitur_cols:
        if ac in cols:
            last_abitur_idx = max(last_abitur_idx, cols.index(ac))

    # MSA columns to insert
    msa_cols = [
        'msa_notendurchschnitt_bezirk_2024',
        'msa_bestehensquote_bezirk_2024',
        'msa_plus_quote_bezirk_2024',
        'msa_teilnehmer_bezirk_2024',
        'msa_notendurchschnitt_bezirk_2023',
        'msa_bestehensquote_bezirk_2023',
        'msa_plus_quote_bezirk_2023',
        'msa_teilnehmer_bezirk_2023',
        'leistungsdaten_quelle',
        'notendurchschnitt_unified_2024',
        'notendurchschnitt_unified_2023',
    ]

    # Remove MSA cols from their current position
    for mc in msa_cols:
        if mc in cols:
            cols.remove(mc)

    # Insert MSA cols after last abitur column
    insert_pos = last_abitur_idx + 1
    for i, mc in enumerate(msa_cols):
        cols.insert(insert_pos + i, mc)

    return df[cols]


def main():
    input_file = "combined_schools_with_metadata.csv"
    output_csv = "combined_schools_with_metadata_msa.csv"
    output_xlsx = "combined_schools_with_metadata_msa.xlsx"

    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    logger.info(f"Loading {input_file}...")
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    logger.info(f"Loaded {len(df)} schools")

    # Add MSA data
    logger.info("Adding MSA Bezirk data...")
    df = add_msa_bezirk_data(df)

    # Reorder columns
    df = reorder_columns(df)

    # Save outputs
    logger.info(f"Saving to {output_csv}...")
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    logger.info(f"Saving to {output_xlsx}...")
    df.to_excel(output_xlsx, index=False, engine='openpyxl')

    # Print summary
    print("\n" + "="*70)
    print("MSA BEZIRK ENRICHMENT SUMMARY")
    print("="*70)
    print(f"Total schools: {len(df)}")
    print(f"Schools with Abitur (MSA columns empty): {(df['leistungsdaten_quelle'] == 'Schule_Abitur').sum()}")
    print(f"Schools using MSA Bezirk proxy: {(df['leistungsdaten_quelle'] == 'Bezirk_MSA').sum()}")

    print("\nNew columns added:")
    print("  - msa_notendurchschnitt_bezirk_2024  (MSA average grade at Bezirk level)")
    print("  - msa_bestehensquote_bezirk_2024     (MSA pass rate % at Bezirk level)")
    print("  - msa_plus_quote_bezirk_2024         (% qualified for Oberstufe)")
    print("  - msa_teilnehmer_bezirk_2024         (Number of MSA participants)")
    print("  - msa_notendurchschnitt_bezirk_2023  (same for 2022/23)")
    print("  - msa_bestehensquote_bezirk_2023")
    print("  - msa_plus_quote_bezirk_2023")
    print("  - msa_teilnehmer_bezirk_2023")
    print("  - leistungsdaten_quelle              ('Schule_Abitur' or 'Bezirk_MSA')")
    print("  - notendurchschnitt_unified_2024     (Abitur if available, else MSA Bezirk)")
    print("  - notendurchschnitt_unified_2023")

    print(f"\nOutput files:")
    print(f"  - {output_csv}")
    print(f"  - {output_xlsx}")
    print("="*70)

    # Show breakdown by school type
    print("\nBreakdown by school type and data source:")
    breakdown = df.groupby(['school_type', 'leistungsdaten_quelle']).size().unstack(fill_value=0)
    print(breakdown.to_string())

    # Show sample of schools using MSA proxy
    print("\nSample ISS schools using MSA Bezirk proxy:")
    sample = df[df['leistungsdaten_quelle'] == 'Bezirk_MSA'][
        ['schulname', 'bezirk', 'msa_notendurchschnitt_bezirk_2024', 'msa_bestehensquote_bezirk_2024']
    ].head(10)
    print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
