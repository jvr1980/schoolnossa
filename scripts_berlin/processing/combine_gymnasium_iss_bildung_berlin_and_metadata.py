#!/usr/bin/env python3
"""
Combines bildung_berlin ISS and Gymnasium data with their respective metadata tables.

Improvements over v1:
- Deduplicates schools that offer both ISS and Gymnasium (merged as "ISS-Gymnasium")
- Consolidates duplicate columns (schulname/Schulname, bezirk/Bezirk, etc.)
- Uses conditional selection: keeps non-empty value when one column is empty
- Normalizes URLs (removes trailing slashes, standardizes https)
- Clean lowercase column names with underscores

Steps:
1. Load bildung_berlin_iss.csv and bildung_berlin_gymnasien.csv
2. Deduplicate schools appearing in both (mark as ISS-Gymnasium)
3. Load and harmonize ISS_master_table.xlsx and berlin_gymnasiums_detailed_v2.xlsx
4. Join metadata to bildung_berlin data via schulnummer
5. Consolidate duplicate columns with conditional selection
6. Output clean, deduplicated dataset
"""

import pandas as pd
import os
import re

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BILDUNG_ISS_FILE = os.path.join(BASE_DIR, "bildung_berlin_iss.csv")
BILDUNG_GYM_FILE = os.path.join(BASE_DIR, "bildung_berlin_gymnasien.csv")
ISS_METADATA_FILE = os.path.join(BASE_DIR, "ISS_master_table.xlsx")
GYM_METADATA_FILE = os.path.join(BASE_DIR, "berlin_gymnasiums_detailed_v2.xlsx")

OUTPUT_CSV = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
OUTPUT_XLSX = os.path.join(BASE_DIR, "combined_schools_with_metadata.xlsx")


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


def coalesce(*values):
    """Return first non-null, non-empty value from arguments."""
    for v in values:
        if pd.notna(v) and v != '' and v != 'nan':
            return v
    return None


def determine_traegerschaft(schulnummer):
    """
    Determine if school is public or private based on schulnummer pattern.

    Schulnummer patterns:
    - XXK## = Public ISS (Kommunal)
    - XXY## = Public Gymnasium
    - XXS## = Public Sonderschule (special education)
    - XXB## = Public Berufschule
    - XXA## = Public (Willkommensschule)
    - XXP## = Private school (Privatschule)
    """
    if not schulnummer:
        return None

    schulnummer = str(schulnummer).upper().strip()

    if len(schulnummer) >= 3:
        type_char = schulnummer[2]

        if type_char == 'P':
            return 'Privat'
        elif type_char in ['K', 'Y', 'S', 'B', 'A']:
            return 'Öffentlich'

    return 'Öffentlich'  # Default assumption


def load_and_deduplicate_bildung_data():
    """
    Load the two bildung_berlin CSV files and deduplicate schools
    that appear in both (offering both ISS and Gymnasium programs).
    """
    print("Loading bildung_berlin data...")

    # Load ISS data
    df_iss = pd.read_csv(BILDUNG_ISS_FILE, encoding='utf-8-sig')
    df_iss['school_type'] = 'ISS'
    print(f"  - bildung_berlin_iss.csv: {len(df_iss)} rows")

    # Load Gymnasium data
    df_gym = pd.read_csv(BILDUNG_GYM_FILE, encoding='utf-8-sig')
    df_gym['school_type'] = 'Gymnasium'
    print(f"  - bildung_berlin_gymnasien.csv: {len(df_gym)} rows")

    # Find schools that appear in both files
    iss_ids = set(df_iss['schulnummer'].astype(str))
    gym_ids = set(df_gym['schulnummer'].astype(str))
    dual_program_ids = iss_ids.intersection(gym_ids)

    print(f"  - Schools offering both ISS and Gymnasium: {len(dual_program_ids)}")
    if len(dual_program_ids) > 0:
        print(f"    IDs: {sorted(dual_program_ids)}")

    # Process dual-program schools: keep one row, mark as ISS-Gymnasium
    # We'll keep the ISS row and update its school_type
    df_iss.loc[df_iss['schulnummer'].astype(str).isin(dual_program_ids), 'school_type'] = 'ISS-Gymnasium'

    # Remove dual-program schools from gymnasium dataframe (we kept them in ISS)
    df_gym_filtered = df_gym[~df_gym['schulnummer'].astype(str).isin(dual_program_ids)]

    print(f"  - Gymnasium rows after removing duplicates: {len(df_gym_filtered)} rows")

    # Combine (now without duplicates)
    df_combined = pd.concat([df_iss, df_gym_filtered], ignore_index=True)
    print(f"  - Combined (deduplicated): {len(df_combined)} rows")

    # Normalize schulnummer to string
    df_combined['schulnummer'] = df_combined['schulnummer'].astype(str).str.strip()

    return df_combined


def load_and_harmonize_metadata():
    """
    Load both metadata files and harmonize their column names to a standard format.
    All column names will be lowercase with underscores.
    """
    print("\nLoading metadata files...")

    # Load ISS metadata
    df_iss_meta = pd.read_excel(ISS_METADATA_FILE)
    df_iss_meta['metadata_source'] = 'ISS'
    print(f"  - ISS_master_table.xlsx: {len(df_iss_meta)} rows")

    # Load Gymnasium metadata
    df_gym_meta = pd.read_excel(GYM_METADATA_FILE)
    df_gym_meta['metadata_source'] = 'Gymnasium'
    print(f"  - berlin_gymnasiums_detailed_v2.xlsx: {len(df_gym_meta)} rows")

    # Standardized column mapping for ISS metadata
    iss_column_mapping = {
        'Schulnummer': 'meta_schulnummer',
        'Schulname': 'meta_schulname',
        'Bezirk': 'meta_bezirk',
        'Adresse': 'meta_adresse',
        'Ortsteil': 'meta_ortsteil',
        'Schueler_2024_25': 'schueler_2024_25',
        'Lehrer_2024_25': 'lehrer_2024_25',
        'Schueler_2023_24': 'schueler_2023_24',
        'Lehrer_2023_24': 'lehrer_2023_24',
        'Sprachen': 'sprachen',
        'Homepage': 'meta_homepage',
        'Nachfrage_Plaetze_2025_26': 'nachfrage_plaetze_2025_26',
        'Nachfrage_Wuensche_2025_26': 'nachfrage_wuensche_2025_26',
        'Nachfrage_Prozent_2025_26': 'nachfrage_prozent_2025_26',
        'Abitur_Durchschnitt_2024': 'abitur_durchschnitt_2024',
        'Abitur_Durchschnitt_2023': 'abitur_durchschnitt_2023',
        'Belastungsstufe': 'belastungsstufe',
        'Migration_2024_25': 'migration_2024_25',
        'Migration_2023_24': 'migration_2023_24',
        'metadata_source': 'metadata_source'
    }

    # Standardized column mapping for Gymnasium metadata
    gym_column_mapping = {
        'Schulnummer': 'meta_schulnummer',
        'Schulname': 'meta_schulname',
        'Bezirk': 'meta_bezirk',
        'Adresse': 'meta_adresse',
        'Schüler_2024/25': 'schueler_2024_25',
        'Lehrer_2024/25': 'lehrer_2024_25',
        'Schüler_2023/24': 'schueler_2023_24',
        'Lehrer_2023/24': 'lehrer_2023_24',
        'Schüler_2022/23': 'schueler_2022_23',
        'Lehrer_2022/23': 'lehrer_2022_23',
        'Sprachen': 'sprachen',
        'Homepage': 'meta_homepage',
        'Nachfrage_Plätze_2024/25': 'nachfrage_plaetze_2024_25',
        'Nachfrage_Wünsche_2024/25': 'nachfrage_wuensche_2024_25',
        'Nachfrage_Plätze_2025/26': 'nachfrage_plaetze_2025_26',
        'Nachfrage_Wünsche_2025/26': 'nachfrage_wuensche_2025_26',
        'Abiturnotendurchschnitt_2024': 'abitur_durchschnitt_2024',
        'Abiturerfolgsquote_2024': 'abitur_erfolgsquote_2024',
        'Abiturnotendurchschnitt_2025': 'abitur_durchschnitt_2025',
        'Abiturerfolgsquote_2025': 'abitur_erfolgsquote_2025',
        'metadata_source': 'metadata_source'
    }

    # Rename columns
    df_iss_meta = df_iss_meta.rename(columns=iss_column_mapping)
    df_gym_meta = df_gym_meta.rename(columns=gym_column_mapping)

    # Combine metadata
    df_metadata = pd.concat([df_iss_meta, df_gym_meta], ignore_index=True)

    # Normalize schulnummer
    df_metadata['meta_schulnummer'] = df_metadata['meta_schulnummer'].astype(str).str.strip()

    print(f"  - Combined metadata: {len(df_metadata)} rows")
    print(f"  - Metadata columns: {list(df_metadata.columns)}")

    return df_metadata


def consolidate_columns(df):
    """
    Consolidate duplicate columns using conditional selection.
    When we have two columns representing the same data, keep the non-empty value.
    """
    print("\nConsolidating duplicate columns...")

    # Define column pairs to consolidate: (primary, secondary, final_name)
    # Primary is preferred, but use secondary if primary is empty
    consolidation_pairs = [
        ('bezirk', 'meta_bezirk', 'bezirk'),
        ('ortsteil', 'meta_ortsteil', 'ortsteil'),
    ]

    for primary, secondary, final_name in consolidation_pairs:
        if primary in df.columns and secondary in df.columns:
            # Use coalesce: prefer primary, fallback to secondary
            df[final_name] = df.apply(
                lambda row: coalesce(row[primary], row[secondary]),
                axis=1
            )
            # Drop secondary column if different from final
            if secondary != final_name and secondary in df.columns:
                df = df.drop(columns=[secondary])
            print(f"  - Consolidated {primary} + {secondary} -> {final_name}")

    # Handle website/homepage separately (needs URL normalization)
    if 'website' in df.columns and 'meta_homepage' in df.columns:
        df['website'] = df.apply(
            lambda row: coalesce(
                normalize_url(row['website']),
                normalize_url(row['meta_homepage'])
            ),
            axis=1
        )
        df = df.drop(columns=['meta_homepage'])
        print(f"  - Consolidated website + meta_homepage -> website (normalized)")

    # For schulname: keep the bildung_berlin version (more detailed with school type suffix)
    # But we can drop the metadata version
    if 'meta_schulname' in df.columns:
        df = df.drop(columns=['meta_schulname'])
        print(f"  - Dropped meta_schulname (keeping schulname from bildung_berlin)")

    # Drop meta_adresse (we have strasse + plz which is more structured)
    if 'meta_adresse' in df.columns:
        df = df.drop(columns=['meta_adresse'])
        print(f"  - Dropped meta_adresse (keeping strasse + plz)")

    # Drop meta_ortsteil if it still exists
    if 'meta_ortsteil' in df.columns:
        df = df.drop(columns=['meta_ortsteil'])

    # Drop meta_bezirk if it still exists
    if 'meta_bezirk' in df.columns:
        df = df.drop(columns=['meta_bezirk'])

    return df


def merge_data(df_bildung, df_metadata):
    """Merge bildung data with metadata on school number."""
    print("\nMerging data...")

    # Perform left join: keep all bildung schools, add metadata where available
    df_merged = pd.merge(
        df_bildung,
        df_metadata,
        left_on='schulnummer',
        right_on='meta_schulnummer',
        how='left'
    )

    # Drop the duplicate schulnummer column from metadata
    if 'meta_schulnummer' in df_merged.columns:
        df_merged = df_merged.drop(columns=['meta_schulnummer'])

    # Count matches
    matched = df_merged['metadata_source'].notna().sum()
    unmatched = df_merged['metadata_source'].isna().sum()

    print(f"  - Schools with metadata: {matched}")
    print(f"  - Schools without metadata: {unmatched}")
    print(f"  - Total rows: {len(df_merged)}")

    return df_merged


def add_traegerschaft(df):
    """Add traegerschaft (public/private) column based on schulnummer pattern."""
    print("\nAdding traegerschaft column...")
    df['traegerschaft'] = df['schulnummer'].apply(determine_traegerschaft)

    counts = df['traegerschaft'].value_counts()
    for t, count in counts.items():
        print(f"  - {t}: {count}")

    return df


def reorder_and_clean_columns(df):
    """Reorder columns logically and ensure clean naming."""

    # Define column order
    identity_cols = ['schulnummer', 'schulname', 'school_type', 'schulart', 'traegerschaft']
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
        'abitur_erfolgsquote_2024', 'abitur_erfolgsquote_2025'
    ]
    demand_cols = [
        'nachfrage_plaetze_2025_26', 'nachfrage_wuensche_2025_26', 'nachfrage_prozent_2025_26',
        'nachfrage_plaetze_2024_25', 'nachfrage_wuensche_2024_25'
    ]
    demographic_cols = [
        'belastungsstufe', 'migration_2024_25', 'migration_2023_24'
    ]
    meta_cols = ['metadata_source']

    # Build ordered column list (only include columns that exist)
    ordered_cols = []
    for col_group in [identity_cols, location_cols, contact_cols,
                      student_teacher_cols, academic_cols, demand_cols,
                      demographic_cols, meta_cols]:
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
    print("Combining Gymnasium and ISS data with metadata (v2 - deduplicated)")
    print("=" * 70)

    # Step 1: Load and deduplicate bildung_berlin data
    df_bildung = load_and_deduplicate_bildung_data()

    # Step 2: Load and harmonize metadata
    df_metadata = load_and_harmonize_metadata()

    # Step 3: Merge
    df_merged = merge_data(df_bildung, df_metadata)

    # Step 4: Consolidate duplicate columns
    df_consolidated = consolidate_columns(df_merged)

    # Step 5: Add traegerschaft column
    df_with_traeger = add_traegerschaft(df_consolidated)

    # Step 6: Reorder and clean columns
    df_final = reorder_and_clean_columns(df_with_traeger)

    # Save outputs
    print("\nSaving output files...")
    df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"  - Saved: {OUTPUT_CSV}")

    df_final.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"  - Saved: {OUTPUT_XLSX}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total unique schools: {len(df_final)}")

    school_type_counts = df_final['school_type'].value_counts()
    for stype, count in school_type_counts.items():
        print(f"  - {stype}: {count}")

    print(f"\nColumns in output: {len(df_final.columns)}")
    print(f"Column names: {list(df_final.columns)}")

    # Metadata coverage
    with_meta = df_final['metadata_source'].notna().sum()
    without_meta = df_final['metadata_source'].isna().sum()
    print(f"\nMetadata coverage:")
    print(f"  - With metadata: {with_meta} ({100*with_meta/len(df_final):.1f}%)")
    print(f"  - Without metadata: {without_meta} ({100*without_meta/len(df_final):.1f}%)")

    # Show sample of schools without metadata
    unmatched = df_final[df_final['metadata_source'].isna()]
    if len(unmatched) > 0:
        print(f"\nSchools without metadata ({len(unmatched)}):")
        for _, row in unmatched.head(10).iterrows():
            print(f"  - {row['schulnummer']}: {row['schulname'][:50]} ({row['school_type']})")
        if len(unmatched) > 10:
            print(f"  ... and {len(unmatched) - 10} more")

    # Verify no duplicate schulnummer
    duplicates = df_final[df_final.duplicated(subset=['schulnummer'], keep=False)]
    if len(duplicates) > 0:
        print(f"\n⚠️  WARNING: Found {len(duplicates)} rows with duplicate schulnummer!")
        print(duplicates[['schulnummer', 'schulname', 'school_type']])
    else:
        print(f"\n✓ No duplicate schulnummer found - data is clean!")


if __name__ == "__main__":
    main()
