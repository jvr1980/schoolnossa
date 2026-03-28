#!/usr/bin/env python3
"""
Combines bildung_berlin Grundschulen data and prepares it for enrichment.

This is a simplified version of the secondary school combiner since primary schools
don't have the ISS/Gymnasium dual-track complexity.

Steps:
1. Load bildung_berlin_grundschulen.csv
2. Add traegerschaft (public/private) based on schulnummer pattern
3. Prepare schema compatible with secondary schools (with blank columns for N/A fields)
4. Output clean dataset ready for enrichment

Schema Notes:
- Primary schools don't have MSA or Abitur data - these columns remain blank
- Primary schools don't have languages (Sprachen) in the same way - blank
- Primary schools have different demand metrics - adapt as available
"""

import pandas as pd
import os
import re
from pathlib import Path

# File paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_berlin_primary"

INPUT_FILE = DATA_DIR / "raw" / "bildung_berlin_grundschulen.csv"
OUTPUT_CSV = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.csv"
OUTPUT_XLSX = DATA_DIR / "intermediate" / "combined_grundschulen_with_metadata.xlsx"


def normalize_url(url):
    """Normalize URL: ensure https, remove trailing slashes."""
    if pd.isna(url) or url == '':
        return None
    url = str(url).strip()
    # Replace http with https
    if url.startswith('http://'):
        url = 'https://' + url[7:]
    # Remove trailing slash
    url = url.rstrip('/')
    return url


def determine_traegerschaft(schulnummer):
    """
    Determine if school is public or private based on schulnummer pattern.

    Schulnummer patterns for Grundschulen:
    - XXG## = Public Grundschule
    - XXP## = Private school (Privatschule)
    """
    if not schulnummer:
        return None

    schulnummer = str(schulnummer).upper().strip()

    if len(schulnummer) >= 3:
        type_char = schulnummer[2]

        if type_char == 'P':
            return 'Privat'
        elif type_char == 'G':
            return 'Öffentlich'

    return 'Öffentlich'  # Default assumption


def add_schema_columns(df):
    """
    Add all columns from secondary school schema to maintain compatibility.
    Columns not applicable to primary schools remain blank/null.
    """
    # Columns that don't apply to primary schools but maintain schema compatibility
    secondary_only_columns = [
        # Academic performance (secondary school specific)
        'abitur_durchschnitt_2024',
        'abitur_durchschnitt_2023',
        'abitur_durchschnitt_2025',
        'abitur_erfolgsquote_2024',
        'abitur_erfolgsquote_2025',
        # MSA data (secondary school specific)
        'msa_notendurchschnitt_bezirk_2024',
        'msa_bestehensquote_bezirk_2024',
        'msa_plus_quote_bezirk_2024',
        'msa_teilnehmer_bezirk_2024',
        'msa_notendurchschnitt_bezirk_2023',
        'msa_bestehensquote_bezirk_2023',
        'msa_plus_quote_bezirk_2023',
        'msa_teilnehmer_bezirk_2023',
        'notendurchschnitt_unified_2024',
        'notendurchschnitt_unified_2023',
        'leistungsdaten_quelle',
    ]

    # Columns that might have data but need to be initialized
    shared_columns = [
        # Enrollment data
        'schueler_2024_25',
        'lehrer_2024_25',
        'schueler_2023_24',
        'lehrer_2023_24',
        'schueler_2022_23',
        'lehrer_2022_23',
        # Languages (may not apply but keep for schema)
        'sprachen',
        # Demand/enrollment pressure
        'nachfrage_plaetze_2025_26',
        'nachfrage_wuensche_2025_26',
        'nachfrage_prozent_2025_26',
        'nachfrage_plaetze_2024_25',
        'nachfrage_wuensche_2024_25',
        # Demographics
        'belastungsstufe',
        'migration_2024_25',
        'migration_2023_24',
        # Geospatial (to be filled by enrichment)
        'latitude',
        'longitude',
        # Metadata
        'metadata_source',
        'gruendungsjahr',
        'besonderheiten',
    ]

    # Add secondary-only columns as blank
    for col in secondary_only_columns:
        if col not in df.columns:
            df[col] = None

    # Add shared columns as blank if not present
    for col in shared_columns:
        if col not in df.columns:
            df[col] = None

    return df


def reorder_and_clean_columns(df):
    """Reorder columns logically and ensure clean naming."""

    # Define column order (matching secondary school schema)
    identity_cols = ['schulnummer', 'schulname', 'school_type', 'schulart', 'traegerschaft', 'gruendungsjahr']
    location_cols = ['strasse', 'plz', 'ortsteil', 'bezirk']
    contact_cols = ['telefon', 'email', 'website', 'leitung']
    student_teacher_cols = [
        'schueler_2024_25', 'lehrer_2024_25',
        'schueler_2023_24', 'lehrer_2023_24',
        'schueler_2022_23', 'lehrer_2022_23'
    ]
    academic_cols = [
        'sprachen',
        'abitur_durchschnitt_2024', 'abitur_durchschnitt_2023', 'abitur_durchschnitt_2025',
        'abitur_erfolgsquote_2024', 'abitur_erfolgsquote_2025',
        'msa_notendurchschnitt_bezirk_2024', 'msa_bestehensquote_bezirk_2024',
        'msa_plus_quote_bezirk_2024', 'msa_teilnehmer_bezirk_2024',
        'msa_notendurchschnitt_bezirk_2023', 'msa_bestehensquote_bezirk_2023',
        'msa_plus_quote_bezirk_2023', 'msa_teilnehmer_bezirk_2023',
        'notendurchschnitt_unified_2024', 'notendurchschnitt_unified_2023',
        'leistungsdaten_quelle', 'besonderheiten'
    ]
    demand_cols = [
        'nachfrage_plaetze_2025_26', 'nachfrage_wuensche_2025_26', 'nachfrage_prozent_2025_26',
        'nachfrage_plaetze_2024_25', 'nachfrage_wuensche_2024_25'
    ]
    demographic_cols = [
        'belastungsstufe', 'migration_2024_25', 'migration_2023_24'
    ]
    geo_cols = ['latitude', 'longitude']
    meta_cols = ['metadata_source']

    # Build ordered column list (only include columns that exist)
    ordered_cols = []
    for col_group in [identity_cols, location_cols, contact_cols,
                      student_teacher_cols, academic_cols, demand_cols,
                      demographic_cols, geo_cols, meta_cols]:
        for col in col_group:
            if col in df.columns and col not in ordered_cols:
                ordered_cols.append(col)

    # Add any remaining columns not in our predefined order
    remaining_cols = [c for c in df.columns if c not in ordered_cols]
    if remaining_cols:
        print(f"  - Additional columns not in predefined order: {remaining_cols}")
        ordered_cols.extend(remaining_cols)

    return df[ordered_cols]


def main():
    print("=" * 70)
    print("Combining Grundschulen data for Berlin Primary Schools")
    print("=" * 70)

    # Check if input file exists
    if not INPUT_FILE.exists():
        print(f"\nError: Input file not found: {INPUT_FILE}")
        print("Please run the scraper first: bildung_berlin_grundschulen_scraper.py")
        return

    # Step 1: Load Grundschulen data
    print("\nLoading Grundschulen data...")
    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
    print(f"  - Loaded {len(df)} schools from {INPUT_FILE.name}")

    # Step 2: Add school_type column
    df['school_type'] = 'Grundschule'

    # Step 3: Normalize schulnummer
    df['schulnummer'] = df['schulnummer'].astype(str).str.strip()

    # Step 4: Add traegerschaft column
    print("\nAdding traegerschaft column...")
    df['traegerschaft'] = df['schulnummer'].apply(determine_traegerschaft)

    counts = df['traegerschaft'].value_counts()
    for t, count in counts.items():
        print(f"  - {t}: {count}")

    # Step 5: Normalize URLs
    print("\nNormalizing URLs...")
    if 'website' in df.columns:
        df['website'] = df['website'].apply(normalize_url)

    # Step 6: Add schema columns (for compatibility with secondary schools)
    print("\nAdding schema columns for compatibility...")
    df = add_schema_columns(df)

    # Step 7: Reorder columns
    df = reorder_and_clean_columns(df)

    # Ensure output directory exists
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Save outputs
    print("\nSaving output files...")
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"  - Saved: {OUTPUT_CSV}")

    df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"  - Saved: {OUTPUT_XLSX}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total Grundschulen: {len(df)}")

    traegerschaft_counts = df['traegerschaft'].value_counts()
    for t, count in traegerschaft_counts.items():
        print(f"  - {t}: {count}")

    print(f"\nColumns in output: {len(df.columns)}")
    print(f"Column names: {list(df.columns)}")

    # Show schools with/without key data
    print(f"\nData coverage:")
    print(f"  - Schools with websites: {df['website'].notna().sum()}")
    print(f"  - Schools with email: {df['email'].notna().sum()}")
    print(f"  - Schools with phone: {df['telefon'].notna().sum()}")

    # Verify no duplicate schulnummer
    duplicates = df[df.duplicated(subset=['schulnummer'], keep=False)]
    if len(duplicates) > 0:
        print(f"\n  WARNING: Found {len(duplicates)} rows with duplicate schulnummer!")
        print(duplicates[['schulnummer', 'schulname']])
    else:
        print(f"\n  No duplicate schulnummer found - data is clean!")

    # Show first few rows
    print("\nFirst 5 schools:")
    print(df[['schulnummer', 'schulname', 'bezirk', 'traegerschaft']].head().to_string())


if __name__ == "__main__":
    main()
