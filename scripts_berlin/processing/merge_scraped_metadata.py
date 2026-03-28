#!/usr/bin/env python3
"""
Merges scraped metadata into the combined_schools_with_metadata file.

This script:
1. Loads the combined schools data
2. Loads the scraped metadata for missing schools
3. Fills in missing values from scraped data
4. Saves the updated combined file
"""

import pandas as pd
import os

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMBINED_FILE = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
SCRAPED_FILE = os.path.join(BASE_DIR, "scraped_missing_metadata.csv")
SCRAPED_LLM_FILE = os.path.join(BASE_DIR, "scraped_missing_metadata_llm.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "combined_schools_with_metadata.csv")
OUTPUT_XLSX = os.path.join(BASE_DIR, "combined_schools_with_metadata.xlsx")


def coalesce(val1, val2):
    """Return first non-null value."""
    if pd.notna(val1) and val1 != '':
        return val1
    if pd.notna(val2) and val2 != '':
        return val2
    return None


def main():
    print("=" * 70)
    print("Merging Scraped Metadata into Combined Schools Data")
    print("=" * 70)

    # Load files
    print("\nLoading files...")
    df_combined = pd.read_csv(COMBINED_FILE)

    # Load LLM scraped data (preferred) or fallback to regex scraped
    if os.path.exists(SCRAPED_LLM_FILE):
        df_scraped = pd.read_csv(SCRAPED_LLM_FILE)
        print(f"  - Using LLM scraped data: {len(df_scraped)} rows")
    elif os.path.exists(SCRAPED_FILE):
        df_scraped = pd.read_csv(SCRAPED_FILE)
        print(f"  - Using regex scraped data: {len(df_scraped)} rows")
    else:
        print("  - No scraped data found!")
        return

    print(f"  - Combined schools: {len(df_combined)} rows")

    # Create a mapping from schulnummer to scraped data
    scraped_dict = df_scraped.set_index('schulnummer').to_dict('index')

    # Track updates
    updates = {
        'schueler_2024_25': 0,
        'lehrer_2024_25': 0,
        'sprachen': 0,
        'bezirk': 0,
        'abitur_durchschnitt_2024': 0,
        'gruendungsjahr': 0,
        'besonderheiten': 0,
    }

    # Add columns if not exist
    if 'gruendungsjahr' not in df_combined.columns:
        df_combined['gruendungsjahr'] = None
    if 'besonderheiten' not in df_combined.columns:
        df_combined['besonderheiten'] = None

    print("\nMerging scraped data...")

    for idx, row in df_combined.iterrows():
        schulnummer = str(row['schulnummer'])

        if schulnummer in scraped_dict:
            scraped = scraped_dict[schulnummer]

            # Update schueler if missing
            if pd.isna(row.get('schueler_2024_25')) and pd.notna(scraped.get('scraped_schueler')):
                df_combined.at[idx, 'schueler_2024_25'] = scraped['scraped_schueler']
                updates['schueler_2024_25'] += 1

            # Update lehrer if missing
            if pd.isna(row.get('lehrer_2024_25')) and pd.notna(scraped.get('scraped_lehrer')):
                df_combined.at[idx, 'lehrer_2024_25'] = scraped['scraped_lehrer']
                updates['lehrer_2024_25'] += 1

            # Update sprachen if missing
            if pd.isna(row.get('sprachen')) and pd.notna(scraped.get('scraped_sprachen')):
                df_combined.at[idx, 'sprachen'] = scraped['scraped_sprachen']
                updates['sprachen'] += 1

            # Update bezirk if missing
            if pd.isna(row.get('bezirk')) and pd.notna(scraped.get('scraped_bezirk')):
                df_combined.at[idx, 'bezirk'] = scraped['scraped_bezirk']
                updates['bezirk'] += 1

            # Update abitur if missing
            if pd.isna(row.get('abitur_durchschnitt_2024')) and pd.notna(scraped.get('scraped_abitur')):
                df_combined.at[idx, 'abitur_durchschnitt_2024'] = scraped['scraped_abitur']
                updates['abitur_durchschnitt_2024'] += 1

            # Update gruendungsjahr if missing
            if pd.isna(row.get('gruendungsjahr')) and pd.notna(scraped.get('scraped_gruendungsjahr')):
                df_combined.at[idx, 'gruendungsjahr'] = scraped['scraped_gruendungsjahr']
                updates['gruendungsjahr'] += 1

            # Update besonderheiten if missing
            if pd.isna(row.get('besonderheiten')) and pd.notna(scraped.get('scraped_besonderheiten')):
                df_combined.at[idx, 'besonderheiten'] = scraped['scraped_besonderheiten']
                updates['besonderheiten'] += 1

    print("\nUpdates made:")
    for field, count in updates.items():
        print(f"  - {field}: {count} schools updated")

    # Reorder columns to put gruendungsjahr in a logical place
    cols = list(df_combined.columns)
    if 'gruendungsjahr' in cols:
        cols.remove('gruendungsjahr')
        # Insert after traegerschaft
        if 'traegerschaft' in cols:
            traeger_idx = cols.index('traegerschaft')
            cols.insert(traeger_idx + 1, 'gruendungsjahr')
        else:
            cols.append('gruendungsjahr')
        df_combined = df_combined[cols]

    # Save
    print("\nSaving updated files...")
    df_combined.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"  - Saved: {OUTPUT_CSV}")

    df_combined.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"  - Saved: {OUTPUT_XLSX}")

    # Summary
    print("\n" + "=" * 70)
    print("FINAL DATA SUMMARY")
    print("=" * 70)
    print(f"Total schools: {len(df_combined)}")

    print(f"\nData coverage:")
    for col in ['schueler_2024_25', 'lehrer_2024_25', 'sprachen', 'bezirk',
                'abitur_durchschnitt_2024', 'traegerschaft', 'gruendungsjahr', 'besonderheiten']:
        if col in df_combined.columns:
            non_null = df_combined[col].notna().sum()
            pct = 100 * non_null / len(df_combined)
            print(f"  - {col:<25}: {non_null:3d} / {len(df_combined)} ({pct:.1f}%)")

    print(f"\nTraegerschaft breakdown:")
    print(df_combined['traegerschaft'].value_counts().to_string())


if __name__ == "__main__":
    main()
