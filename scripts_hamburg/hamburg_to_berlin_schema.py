#!/usr/bin/env python3
"""
Transform Hamburg school data to match Berlin schema exactly.

This script takes the Hamburg school master table and transforms it to have
identical columns to the Berlin schema, enabling use with the same frontend.

Transformations:
1. Rename columns that have semantic equivalents
2. Map Hamburg transit data to Berlin transit format
3. Add missing Berlin columns with NULL values
4. Drop Hamburg-only columns
5. Reorder columns to match Berlin exactly
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path

# Paths
HAMBURG_CSV = Path(__file__).parent.parent / "data_hamburg" / "final" / "hamburg_school_master_table_final.csv"
HAMBURG_EMBEDDINGS = Path(__file__).parent.parent / "data_hamburg" / "final" / "hamburg_school_master_table_final_with_embeddings.parquet"
HAMBURG_WEBSITE_DATA = Path(__file__).parent.parent / "data_hamburg" / "intermediate" / "hamburg_schools_with_website_data.csv"
BERLIN_REFERENCE = Path(__file__).parent.parent / "data_berlin" / "final" / "school_master_table_final_with_embeddings.parquet"
OUTPUT_DIR = Path(__file__).parent.parent / "data_hamburg" / "final"


def extract_plz(addr):
    """Extract 5-digit PLZ from address string."""
    if pd.isna(addr):
        return None
    match = re.search(r'\b(\d{5})\b', str(addr))
    return match.group(1) if match else None


def transform_hamburg_to_berlin_schema():
    """Transform Hamburg data to match Berlin schema."""

    print("Loading data...")
    # Read from CSV (has all 296 enriched columns) instead of stale parquet (only 196)
    hamburg = pd.read_csv(HAMBURG_CSV, low_memory=False)
    berlin = pd.read_parquet(BERLIN_REFERENCE)

    # Merge embeddings from parquet if available
    if HAMBURG_EMBEDDINGS.exists():
        emb_df = pd.read_parquet(HAMBURG_EMBEDDINGS)
        if 'embedding' in emb_df.columns and 'schulnummer' in emb_df.columns:
            hamburg['schulnummer'] = hamburg['schulnummer'].astype(str)
            emb_df['schulnummer'] = emb_df['schulnummer'].astype(str)
            emb_cols = ['schulnummer', 'embedding']
            # Also grab similar school cols if present
            for c in ['most_similar_school_01', 'most_similar_school_02', 'most_similar_school_03']:
                if c in emb_df.columns:
                    emb_cols.append(c)
            hamburg = hamburg.merge(emb_df[emb_cols], on='schulnummer', how='left', suffixes=('', '_emb'))
            print(f"  Merged embeddings from parquet ({hamburg['embedding'].notna().sum()} schools)")

    print(f"Hamburg: {len(hamburg)} schools, {len(hamburg.columns)} columns")
    print(f"Berlin reference: {len(berlin)} schools, {len(berlin.columns)} columns")

    # Load website scraper data if available
    website_data = None
    if HAMBURG_WEBSITE_DATA.exists():
        website_data = pd.read_csv(HAMBURG_WEBSITE_DATA)
        website_data['schulnummer'] = website_data['schulnummer'].astype(str)
        print(f"Website scraper data: {len(website_data)} schools, {website_data['lehrer_anzahl'].notna().sum()} with teacher counts")

    # Create output dataframe
    output = pd.DataFrame()

    # =========================================================================
    # STEP 1: Direct column mappings (rename Hamburg -> Berlin)
    # =========================================================================
    print("\nStep 1: Applying column renames...")

    column_renames = {
        'schul_email': 'email',
        'schul_telefonnr': 'telefon',
        'schul_homepage': 'website',
        'adresse_strasse_hausnr': 'strasse',
        'stadtteil': 'ortsteil',
        'name_schulleiter': 'leitung',
        'most_similar_school_01': 'most_similar_school_no_01',
        'most_similar_school_02': 'most_similar_school_no_02',
        'most_similar_school_03': 'most_similar_school_no_03',
        # Crime: map Hamburg street_crime → Berlin neighborhood_crimes (semantic equivalent)
        'crime_street_crime_2023': 'crime_neighborhood_crimes_2023',
        'crime_street_crime_2024': 'crime_neighborhood_crimes_2024',
        'crime_street_crime_avg': 'crime_neighborhood_crimes_avg',
        'crime_street_crime_yoy_pct': 'crime_neighborhood_crimes_yoy_pct',
    }

    # Apply renames
    hamburg_renamed = hamburg.rename(columns=column_renames)

    for old, new in column_renames.items():
        if old in hamburg.columns:
            print(f"  Renamed: {old} -> {new}")

    # =========================================================================
    # STEP 2: Extract/transform specific columns
    # =========================================================================
    print("\nStep 2: Extracting/transforming data...")

    # Extract PLZ from address
    hamburg_renamed['plz'] = hamburg['adresse_ort'].apply(extract_plz)
    print(f"  Extracted PLZ for {hamburg_renamed['plz'].notna().sum()}/{len(hamburg_renamed)} schools")

    # Map student count to current year
    if 'schueler_gesamt' in hamburg.columns:
        hamburg_renamed['schueler_2024_25'] = hamburg['schueler_gesamt']
        print("  Mapped schueler_gesamt -> schueler_2024_25")

    # Map Hamburg transit to Berlin format (nearest stops)
    # Berlin has transit_bus_01, transit_rail_01 etc.
    # Hamburg has nearest_bus, nearest_ubahn, nearest_sbahn

    # Map U-Bahn/S-Bahn to rail
    if 'nearest_ubahn_name' in hamburg.columns:
        hamburg_renamed['transit_rail_01_name'] = hamburg['nearest_ubahn_name']
        hamburg_renamed['transit_rail_01_distance_m'] = hamburg['nearest_ubahn_distance_m']
        print("  Mapped nearest_ubahn -> transit_rail_01")

    if 'nearest_sbahn_name' in hamburg.columns:
        hamburg_renamed['transit_rail_02_name'] = hamburg['nearest_sbahn_name']
        hamburg_renamed['transit_rail_02_distance_m'] = hamburg['nearest_sbahn_distance_m']
        print("  Mapped nearest_sbahn -> transit_rail_02")

    # Map bus
    if 'nearest_bus_name' in hamburg.columns:
        hamburg_renamed['transit_bus_01_name'] = hamburg['nearest_bus_name']
        hamburg_renamed['transit_bus_01_distance_m'] = hamburg['nearest_bus_distance_m']
        print("  Mapped nearest_bus -> transit_bus_01")

    # Map HVV stop count to Berlin format
    if 'hvv_stops_1000m' in hamburg.columns:
        hamburg_renamed['transit_stop_count_1000m'] = hamburg['hvv_stops_1000m']
        print("  Mapped hvv_stops_1000m -> transit_stop_count_1000m")

    # Map fremdsprache to sprachen
    if 'fremdsprache' in hamburg.columns:
        hamburg_renamed['sprachen'] = hamburg['fremdsprache']
        print("  Mapped fremdsprache -> sprachen")

    # Map demographics → Berlin migration columns (Stadtteil-level data)
    if 'demo_pct_migration_background' in hamburg.columns:
        hamburg_renamed['migration_2024_25'] = hamburg['demo_pct_migration_background']
        count = hamburg_renamed['migration_2024_25'].notna().sum()
        print(f"  Mapped demo_pct_migration_background -> migration_2024_25 ({count} schools)")

    # Map teacher estimates as base (website scraper will override where available)
    if 'lehrer_estimated' in hamburg.columns:
        hamburg_renamed['lehrer_2024_25'] = hamburg['lehrer_estimated']
        count = hamburg_renamed['lehrer_2024_25'].notna().sum()
        print(f"  Mapped lehrer_estimated -> lehrer_2024_25 ({count} schools, base estimate)")

    # =========================================================================
    # STEP 2b: Merge website scraper data (teacher counts, profiles)
    # =========================================================================
    if website_data is not None:
        print("\nStep 2b: Merging website scraper data...")
        hamburg_renamed['schulnummer'] = hamburg_renamed['schulnummer'].astype(str)

        # Merge teacher data
        website_cols = ['schulnummer', 'lehrer_anzahl', 'schulprofil_website', 'besonderheiten_website']
        website_subset = website_data[website_cols].drop_duplicates(subset='schulnummer')

        hamburg_renamed = hamburg_renamed.merge(
            website_subset,
            on='schulnummer',
            how='left'
        )

        # Override teacher estimates with actual scraped counts where available
        if 'lehrer_anzahl' in hamburg_renamed.columns:
            mask = hamburg_renamed['lehrer_anzahl'].notna()
            hamburg_renamed.loc[mask, 'lehrer_2024_25'] = hamburg_renamed.loc[mask, 'lehrer_anzahl']
            scraped = mask.sum()
            total = hamburg_renamed['lehrer_2024_25'].notna().sum()
            print(f"  Override lehrer_2024_25 with lehrer_anzahl where available ({scraped} scraped, {total} total)")

        # Map besonderheiten
        if 'besonderheiten_website' in hamburg_renamed.columns:
            hamburg_renamed['besonderheiten'] = hamburg_renamed['besonderheiten_website']
            besond_count = hamburg_renamed['besonderheiten'].notna().sum()
            print(f"  Mapped besonderheiten_website -> besonderheiten ({besond_count} schools)")

    # =========================================================================
    # STEP 3: Build output with exact Berlin schema
    # =========================================================================
    print("\nStep 3: Building output with Berlin schema...")

    berlin_columns = list(berlin.columns)

    for col in berlin_columns:
        if col in hamburg_renamed.columns:
            output[col] = hamburg_renamed[col]
        else:
            # Add column with NULL/appropriate default
            output[col] = None

    # Count populated vs empty
    populated = sum(1 for col in berlin_columns if col in hamburg_renamed.columns)
    print(f"  Columns from Hamburg data: {populated}/{len(berlin_columns)}")
    print(f"  Columns set to NULL: {len(berlin_columns) - populated}/{len(berlin_columns)}")

    # =========================================================================
    # STEP 4: Verify schema match
    # =========================================================================
    print("\nStep 4: Verifying schema...")

    assert list(output.columns) == berlin_columns, "Column order mismatch!"
    assert len(output.columns) == len(berlin.columns), "Column count mismatch!"
    print(f"  Schema verified: {len(output.columns)} columns match Berlin exactly")

    # =========================================================================
    # STEP 5: Data quality report
    # =========================================================================
    print("\nStep 5: Data quality report...")

    # Check key columns (including newly-recovered ones)
    key_cols = ['schulnummer', 'schulname', 'latitude', 'longitude', 'bezirk',
                'email', 'telefon', 'website', 'plz', 'strasse', 'ortsteil',
                'lehrer_2024_25', 'schueler_2024_25', 'besonderheiten',
                'transit_accessibility_score', 'description', 'embedding',
                'crime_safety_category', 'crime_safety_rank', 'crime_total_crimes_2023',
                'crime_total_crimes_2024', 'migration_2024_25',
                'tuition_display', 'income_based_tuition']

    print("\n  Key column population:")
    for col in key_cols:
        non_null = output[col].notna().sum()
        pct = non_null / len(output) * 100
        status = "✓" if pct > 50 else "○" if pct > 0 else "✗"
        print(f"    {status} {col}: {non_null}/{len(output)} ({pct:.0f}%)")

    # =========================================================================
    # STEP 6: Save output
    # =========================================================================
    print("\nStep 6: Saving output...")

    # Save as parquet
    output_parquet = OUTPUT_DIR / "hamburg_school_master_table_berlin_schema.parquet"
    output.to_parquet(output_parquet, index=False)
    print(f"  Saved: {output_parquet}")

    # Save as CSV for inspection
    output_csv = OUTPUT_DIR / "hamburg_school_master_table_berlin_schema.csv"
    output.to_csv(output_csv, index=False)
    print(f"  Saved: {output_csv}")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("TRANSFORMATION COMPLETE")
    print("=" * 60)
    print(f"Input:  {len(hamburg)} Hamburg schools, {len(hamburg.columns)} columns")
    print(f"Output: {len(output)} schools, {len(output.columns)} columns (Berlin schema)")
    print(f"\nOutput files:")
    print(f"  - {output_parquet}")
    print(f"  - {output_csv}")

    return output


if __name__ == "__main__":
    transform_hamburg_to_berlin_schema()
