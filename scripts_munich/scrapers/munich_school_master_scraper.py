#!/usr/bin/env python3
"""
Phase 1: Munich School Master Data Scraper

Downloads and processes school data from Bayern Schulsuche CSV export
and jedeschule.codefor.de for pre-geocoded coordinates.

Data Sources:
  1. km.bayern.de Schulsuche — CSV (semicolon, ISO-8859-15), ~6100 records statewide
  2. jedeschule.codefor.de — CSV with pre-geocoded coordinates (lat/lon)
  3. Nominatim — fallback geocoding for remaining schools

This script:
1. Downloads the Bayern Schulsuche CSV (all schools)
2. Filters for München (PLZ 80xxx/81xxx or Ort contains "München")
3. Filters for secondary school types (Gymnasium, Realschule, Mittelschule, etc.)
4. Downloads jedeschule.codefor.de CSV for coordinates (matched by name+PLZ)
5. Geocodes remaining unmatched schools via Nominatim
6. Scrapes school detail pages on km.bayern.de for website URLs
7. Outputs to data_munich/intermediate/munich_secondary_schools.csv

Author: Munich School Data Pipeline
Created: 2026-04-01
"""

import requests
import pandas as pd
import logging
import sys
import io
import time
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_munich"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

# Data source URLs
# The Schulsuche CSV can be obtained by searching with parameters that return all schools
# URL pattern: km.bayern.de exports CSV with semicolons
SCHULSUCHE_CSV_URL = "https://www.km.bayern.de/ministerium/schule-und-ausbildung/schulsuche.html"
JEDESCHULE_CSV_URL = "https://jedeschule.codefor.de/csv-data/jedeschule-data-2026-03-28.csv"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

HEADERS = {
    'User-Agent': 'SchoolNossa/1.0 (Munich school data pipeline, educational project)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Munich PLZ prefixes
MUNICH_PLZ_PREFIXES = ('80', '81')

# Secondary school types in Bayern Schulsuche
SECONDARY_SCHULARTEN = [
    'Gymnasium',
    'Realschule',
    'Mittelschule',
    'Wirtschaftsschule',
    'Freie Waldorfschule',
    'Förderzentrum',
    'Kolleg',
    'Abendrealschule',
    'Abendgymnasium',
]


def ensure_directories():
    for d in [RAW_DIR, INTERMEDIATE_DIR, CACHE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def download_file(url: str, description: str, encoding: str = 'utf-8') -> bytes:
    """Download a file from URL with caching."""
    logger.info(f"Downloading {description} from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        logger.info(f"Downloaded {description} ({len(response.content):,} bytes)")
        return response.content
    except requests.RequestException as e:
        logger.error(f"Failed to download {description}: {e}")
        raise


def scrape_schulsuche_csv() -> pd.DataFrame:
    """
    Download and parse the Bayern Schulsuche CSV.

    The Schulsuche at km.bayern.de provides a CSV export with these columns:
    Schulnummer; Schulart; Name; Straße; PLZ; Ort; Link

    Encoding: ISO-8859-15, Separator: semicolon
    """
    cache_file = CACHE_DIR / "bayern_schulsuche_raw.csv"

    # Check cache (7-day validity)
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 7 * 86400:
            logger.info("Loading Schulsuche CSV from cache...")
            return pd.read_csv(
                cache_file, sep=';', encoding='iso-8859-15',
                quotechar='"', dtype=str
            )

    # Try to download CSV export from Schulsuche
    # The export URL typically uses query parameters for filtering
    # We try the direct download pattern first
    csv_urls = [
        # Direct CSV export (all Bayern schools)
        "https://www.km.bayern.de/ministerium/schule-und-ausbildung/schulsuche.csv",
        # Filtered search with maximum results
        "https://www.km.bayern.de/ministerium/schule-und-ausbildung/schulsuche.html?s=&t=9999&r=&o=&u=1&m=&seite=1&format=csv",
    ]

    content = None
    for url in csv_urls:
        try:
            content = download_file(url, "Bayern Schulsuche CSV")
            # Save raw cache
            with open(cache_file, 'wb') as f:
                f.write(content)
            break
        except requests.RequestException:
            logger.warning(f"URL not available: {url}")
            continue

    if content is None:
        # Fallback: check if we have a manually placed CSV in raw/
        manual_csv = RAW_DIR / "bayern_schulsuche.csv"
        if manual_csv.exists():
            logger.info("Using manually placed CSV from raw/")
            return pd.read_csv(
                manual_csv, sep=';', encoding='iso-8859-15',
                quotechar='"', dtype=str
            )
        raise FileNotFoundError(
            "Could not download Schulsuche CSV. Please download manually from\n"
            "https://www.km.bayern.de/schulsuche and save as\n"
            f"{manual_csv}"
        )

    # Parse CSV (ISO-8859-15, semicolon-separated)
    text = content.decode('iso-8859-15')
    df = pd.read_csv(io.StringIO(text), sep=';', quotechar='"', dtype=str)

    logger.info(f"Parsed {len(df)} schools with columns: {list(df.columns)}")
    return df


def download_jedeschule_coordinates() -> pd.DataFrame:
    """Download jedeschule.codefor.de CSV for pre-geocoded coordinates."""
    cache_file = CACHE_DIR / "jedeschule_data.csv"

    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 30 * 86400:  # 30-day cache
            logger.info("Loading jedeschule.codefor.de data from cache...")
            try:
                return pd.read_csv(cache_file, dtype=str)
            except Exception:
                pass

    try:
        content = download_file(JEDESCHULE_CSV_URL, "jedeschule.codefor.de data")
        with open(cache_file, 'wb') as f:
            f.write(content)
        return pd.read_csv(io.BytesIO(content), dtype=str)
    except Exception as e:
        logger.warning(f"Could not download jedeschule data: {e}")
        return pd.DataFrame()


def geocode_nominatim(address: str, plz: str, ort: str = 'München') -> Optional[dict]:
    """Geocode a single address using Nominatim."""
    query = f"{address}, {plz} {ort}, Germany"
    try:
        response = requests.get(
            NOMINATIM_URL,
            params={'q': query, 'format': 'json', 'limit': 1, 'countrycodes': 'de'},
            headers={'User-Agent': 'SchoolNossa/1.0 (educational research project)'},
            timeout=10
        )
        response.raise_for_status()
        results = response.json()
        if results:
            return {
                'latitude': float(results[0]['lat']),
                'longitude': float(results[0]['lon']),
            }
    except Exception as e:
        logger.debug(f"Geocoding failed for {query}: {e}")
    return None


def geocode_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode schools that don't have coordinates yet."""
    df = df.copy()

    if 'latitude' not in df.columns:
        df['latitude'] = None
        df['longitude'] = None

    missing = df['latitude'].isna()
    missing_count = missing.sum()

    if missing_count == 0:
        logger.info("All schools already geocoded")
        return df

    logger.info(f"Geocoding {missing_count} schools via Nominatim...")

    # Load geocode cache
    geocode_cache_file = CACHE_DIR / "geocode_cache.json"
    geocode_cache = {}
    if geocode_cache_file.exists():
        with open(geocode_cache_file) as f:
            geocode_cache = json.load(f)

    geocoded = 0
    failed = 0

    for idx in df[missing].index:
        row = df.loc[idx]
        strasse = str(row.get('strasse', '')).strip()
        plz = str(row.get('plz', '')).strip()
        ort = str(row.get('ort', 'München')).strip()

        cache_key = f"{strasse}|{plz}|{ort}"
        if cache_key in geocode_cache:
            coords = geocode_cache[cache_key]
            if coords:
                df.at[idx, 'latitude'] = coords['latitude']
                df.at[idx, 'longitude'] = coords['longitude']
                geocoded += 1
            else:
                failed += 1
            continue

        coords = geocode_nominatim(strasse, plz, ort)
        geocode_cache[cache_key] = coords

        if coords:
            df.at[idx, 'latitude'] = coords['latitude']
            df.at[idx, 'longitude'] = coords['longitude']
            geocoded += 1
        else:
            failed += 1

        # Rate limit: max 1 req/sec for Nominatim
        time.sleep(1.1)

        if (geocoded + failed) % 20 == 0:
            logger.info(f"  Geocoded {geocoded}/{missing_count} (failed: {failed})")
            # Save cache periodically
            with open(geocode_cache_file, 'w') as f:
                json.dump(geocode_cache, f)

    # Save geocode cache
    with open(geocode_cache_file, 'w') as f:
        json.dump(geocode_cache, f)

    logger.info(f"Geocoding complete: {geocoded} succeeded, {failed} failed out of {missing_count}")
    return df


def filter_munich_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for München schools by PLZ or Ort."""
    logger.info("Filtering for München schools...")

    # Normalize column names (Schulsuche columns are in German)
    col_map = {}
    for col in df.columns:
        lower = col.lower().strip()
        if lower in ('plz', 'postleitzahl'):
            col_map[col] = 'plz'
        elif lower in ('ort', 'schulort', 'location'):
            col_map[col] = 'ort'
        elif lower in ('straße', 'strasse', 'str.', 'street'):
            col_map[col] = 'strasse'
        elif lower in ('name', 'schulname', 'name der schule'):
            col_map[col] = 'schulname'
        elif lower in ('schulnummer', 'schulnr', 'school number'):
            col_map[col] = 'schulnummer'
        elif lower in ('schulart', 'schultyp', 'school type'):
            col_map[col] = 'schulart'
        elif lower in ('link', 'url'):
            col_map[col] = 'link'

    df = df.rename(columns=col_map)

    # Filter by PLZ or Ort
    plz_mask = pd.Series(False, index=df.index)
    ort_mask = pd.Series(False, index=df.index)

    if 'plz' in df.columns:
        df['plz'] = df['plz'].astype(str).str.strip()
        plz_mask = df['plz'].str.startswith(MUNICH_PLZ_PREFIXES)

    if 'ort' in df.columns:
        df['ort'] = df['ort'].astype(str).str.strip()
        ort_mask = df['ort'].str.contains('München', case=False, na=False)

    munich_mask = plz_mask | ort_mask
    filtered = df[munich_mask].copy()

    logger.info(f"Filtered from {len(df)} to {len(filtered)} München schools")
    return filtered


def filter_secondary_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for secondary school types."""
    logger.info("Filtering for secondary school types...")

    if 'schulart' not in df.columns:
        logger.warning("No 'schulart' column found, returning all schools")
        return df

    # Match secondary types (partial match for flexibility)
    mask = pd.Series(False, index=df.index)
    for schulart in SECONDARY_SCHULARTEN:
        mask |= df['schulart'].str.contains(schulart, case=False, na=False)

    filtered = df[mask].copy()
    logger.info(f"Filtered from {len(df)} to {len(filtered)} secondary schools")

    if 'schulart' in filtered.columns:
        for st, count in filtered['schulart'].value_counts().items():
            logger.info(f"  - {st}: {count}")

    return filtered


def merge_jedeschule_coordinates(df: pd.DataFrame, jede_df: pd.DataFrame) -> pd.DataFrame:
    """Merge pre-geocoded coordinates from jedeschule.codefor.de."""
    if jede_df.empty:
        return df

    logger.info("Matching schools with jedeschule.codefor.de coordinates...")

    # Filter jedeschule for Bayern
    if 'state' in jede_df.columns:
        jede_by = jede_df[jede_df['state'].str.contains('Bayern', case=False, na=False)].copy()
    elif 'bundesland' in jede_df.columns:
        jede_by = jede_df[jede_df['bundesland'].str.contains('Bayern', case=False, na=False)].copy()
    else:
        jede_by = jede_df.copy()

    logger.info(f"jedeschule data: {len(jede_by)} Bayern schools")

    df = df.copy()
    df['latitude'] = None
    df['longitude'] = None

    # Try matching by Schulnummer first
    if 'schulnummer' in df.columns and 'id' in jede_by.columns:
        df['schulnummer_str'] = df['schulnummer'].astype(str).str.strip()
        jede_by['id_str'] = jede_by['id'].astype(str).str.strip()

        for idx, row in df.iterrows():
            match = jede_by[jede_by['id_str'] == row['schulnummer_str']]
            if not match.empty:
                lat_col = next((c for c in match.columns if 'lat' in c.lower()), None)
                lon_col = next((c for c in match.columns if 'lon' in c.lower() or 'lng' in c.lower()), None)
                if lat_col and lon_col:
                    try:
                        df.at[idx, 'latitude'] = float(match.iloc[0][lat_col])
                        df.at[idx, 'longitude'] = float(match.iloc[0][lon_col])
                    except (ValueError, TypeError):
                        pass

    # Try matching by name + PLZ for remaining
    unmatched = df['latitude'].isna()
    if unmatched.any() and 'schulname' in df.columns:
        name_col = next((c for c in jede_by.columns if 'name' in c.lower()), None)
        plz_col = next((c for c in jede_by.columns if 'plz' in c.lower() or 'zip' in c.lower()), None)

        if name_col and plz_col:
            for idx in df[unmatched].index:
                row = df.loc[idx]
                school_name = str(row.get('schulname', '')).lower().strip()
                school_plz = str(row.get('plz', '')).strip()

                candidates = jede_by[jede_by[plz_col].astype(str).str.strip() == school_plz]
                for _, cand in candidates.iterrows():
                    cand_name = str(cand[name_col]).lower().strip()
                    # Fuzzy name match: check if significant words overlap
                    if _name_similarity(school_name, cand_name) > 0.5:
                        lat_col_j = next((c for c in jede_by.columns if 'lat' in c.lower()), None)
                        lon_col_j = next((c for c in jede_by.columns if 'lon' in c.lower() or 'lng' in c.lower()), None)
                        if lat_col_j and lon_col_j:
                            try:
                                df.at[idx, 'latitude'] = float(cand[lat_col_j])
                                df.at[idx, 'longitude'] = float(cand[lon_col_j])
                                break
                            except (ValueError, TypeError):
                                pass

    matched = df['latitude'].notna().sum()
    logger.info(f"Matched coordinates from jedeschule: {matched}/{len(df)} schools")
    return df


def _name_similarity(name1: str, name2: str) -> float:
    """Simple word-overlap similarity between two school names."""
    words1 = set(re.findall(r'\w+', name1))
    words2 = set(re.findall(r'\w+', name2))
    # Remove common stopwords
    stopwords = {'der', 'die', 'das', 'und', 'in', 'am', 'an', 'zu', 'für', 'von', 'e', 'v'}
    words1 -= stopwords
    words2 -= stopwords
    if not words1 or not words2:
        return 0.0
    return len(words1 & words2) / max(len(words1), len(words2))


def scrape_school_websites(df: pd.DataFrame) -> pd.DataFrame:
    """Scrape school detail pages from km.bayern.de for website URLs."""
    df = df.copy()

    if 'link' not in df.columns:
        logger.info("No link column found, skipping website scraping")
        df['website'] = None
        return df

    if 'website' not in df.columns:
        df['website'] = None

    website_cache_file = CACHE_DIR / "school_websites_cache.json"
    website_cache = {}
    if website_cache_file.exists():
        with open(website_cache_file) as f:
            website_cache = json.load(f)

    base_url = "https://www.km.bayern.de"
    scraped = 0

    for idx, row in df.iterrows():
        link = str(row.get('link', '')).strip()
        if not link or link == 'nan':
            continue

        if link in website_cache:
            df.at[idx, 'website'] = website_cache[link]
            if website_cache[link]:
                scraped += 1
            continue

        detail_url = base_url + link if not link.startswith('http') else link
        try:
            response = requests.get(detail_url, headers=HEADERS, timeout=15)
            response.raise_for_status()

            # Look for school website URL in the detail page
            # Common patterns: "Homepage:", "Web:", href with school domain
            text = response.text
            website = None

            # Try to find homepage link
            import re
            patterns = [
                r'Homepage[:\s]*<a[^>]*href="(https?://[^"]+)"',
                r'Web[:\s]*<a[^>]*href="(https?://[^"]+)"',
                r'href="(https?://(?:www\.)?[a-z0-9.-]+\.(?:de|com|org|net)/[^"]*)"',
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    url = match.group(1)
                    # Skip km.bayern.de links
                    if 'km.bayern.de' not in url:
                        website = url
                        break

            website_cache[link] = website
            df.at[idx, 'website'] = website
            if website:
                scraped += 1

            time.sleep(0.5)  # Rate limit

        except Exception as e:
            logger.debug(f"Failed to scrape {detail_url}: {e}")
            website_cache[link] = None
            time.sleep(1)

    # Save cache
    with open(website_cache_file, 'w') as f:
        json.dump(website_cache, f)

    logger.info(f"Scraped websites for {scraped}/{len(df)} schools")
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and enrich with standard columns."""
    df = df.copy()

    # Build full address
    if 'strasse' in df.columns and 'plz' in df.columns:
        df['adresse'] = df['strasse'].fillna('') + ', ' + df['plz'].fillna('') + ' ' + df['ort'].fillna('München')
        df['adresse'] = df['adresse'].str.strip(', ')

    # Map school type
    if 'schulart' in df.columns:
        df['school_type'] = df['schulart'].str.strip()

    # Standardize metadata
    df['data_source'] = 'Bayern Kultusministerium Schulsuche'
    df['data_retrieved'] = datetime.now().strftime('%Y-%m-%d')
    df['bundesland'] = 'Bayern'
    df['stadt'] = 'München'

    return df


def save_outputs(df: pd.DataFrame):
    """Save processed data."""
    # Save to intermediate
    output_path = INTERMEDIATE_DIR / "munich_secondary_schools.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved: {output_path} ({len(df)} schools)")

    # Also save to raw for reference
    raw_path = RAW_DIR / "munich_secondary_schools_raw.csv"
    df.to_csv(raw_path, index=False, encoding='utf-8-sig')


def print_summary(df: pd.DataFrame):
    """Print summary statistics."""
    print(f"\n{'='*70}")
    print("MUNICH SCHOOL MASTER DATA SCRAPER - COMPLETE")
    print(f"{'='*70}")

    print(f"\nTotal secondary schools: {len(df)}")

    if 'schulart' in df.columns or 'school_type' in df.columns:
        type_col = 'school_type' if 'school_type' in df.columns else 'schulart'
        print("\nBy school type:")
        for st, count in df[type_col].value_counts().items():
            print(f"  - {st}: {count}")

    if 'latitude' in df.columns:
        coord_count = df['latitude'].notna().sum()
        pct = 100 * coord_count / len(df) if len(df) > 0 else 0
        print(f"\nCoordinates: {coord_count}/{len(df)} ({pct:.0f}%)")

    if 'website' in df.columns:
        web_count = df['website'].notna().sum()
        pct = 100 * web_count / len(df) if len(df) > 0 else 0
        print(f"Websites: {web_count}/{len(df)} ({pct:.0f}%)")

    print(f"\n{'='*70}")


def main():
    """Main function."""
    logger.info("=" * 60)
    logger.info("Starting Munich School Master Data Scraper")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # Step 1: Download Schulsuche CSV
        all_schools = scrape_schulsuche_csv()

        # Step 2: Filter for München
        munich_schools = filter_munich_schools(all_schools)

        # Step 3: Filter for secondary schools
        secondary = filter_secondary_schools(munich_schools)

        if len(secondary) == 0:
            logger.error("No secondary schools found in München. Check data source and filters.")
            sys.exit(1)

        # Step 4: Get coordinates from jedeschule.codefor.de
        jede_df = download_jedeschule_coordinates()
        secondary = merge_jedeschule_coordinates(secondary, jede_df)

        # Step 5: Geocode remaining via Nominatim
        secondary = geocode_schools(secondary)

        # Step 6: Scrape school detail pages for website URLs
        secondary = scrape_school_websites(secondary)

        # Step 7: Normalize columns
        secondary = normalize_columns(secondary)

        # Step 8: Save outputs
        save_outputs(secondary)

        # Print summary
        print_summary(secondary)

        logger.info("Munich School Master Data Scraper complete!")
        return secondary

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
