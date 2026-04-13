#!/usr/bin/env python3
"""
UK: Fetch school locations from OpenStreetMap via Overpass API.

GIAS (the official DfE school register) is returning 500 errors.
This script fetches secondary school locations from OSM as a reliable
alternative, then matches to DfE KS4 performance data by school name.

Also geocodes via postcodes.io for schools with postcodes but missing coords.

Output: data_gb/cache/osm_schools_england.csv
"""

import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_gb"
CACHE_DIR = DATA_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def fetch_osm_schools(cache_path: Path) -> pd.DataFrame:
    """Fetch all secondary schools in England from OpenStreetMap."""
    if cache_path.exists():
        logger.info(f"Loading cached OSM schools: {cache_path.name}")
        return pd.read_csv(cache_path)

    logger.info("Querying Overpass API for English secondary schools...")

    # Split into regional queries to avoid timeout
    # England bounding box: lat 49.9-55.8, lon -5.7 to 1.8
    # Split into North/South halves
    regions = [
        ("South", 49.9, -5.7, 52.0, 1.8),
        ("Midlands", 52.0, -5.7, 53.5, 1.8),
        ("NorthWest", 53.5, -5.7, 55.8, -1.5),
        ("NorthEast", 53.5, -1.5, 55.8, 1.8),
    ]

    all_elements = []
    for name, s, w, n, e in regions:
        query = f"""
        [out:json][timeout:120];
        (
          nwr["amenity"="school"]["isced:level"~"2|3"]({s},{w},{n},{e});
          nwr["amenity"="school"]["school:type"="secondary"]({s},{w},{n},{e});
          nwr["amenity"="school"]["school:type"="all_through"]({s},{w},{n},{e});
          nwr["amenity"="school"]["school:type"="sixth_form"]({s},{w},{n},{e});
        );
        out center;
        """
        logger.info(f"  Querying {name} England...")
        for attempt in range(3):
            try:
                resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
                resp.raise_for_status()
                elements = resp.json().get("elements", [])
                all_elements.extend(elements)
                logger.info(f"    {name}: {len(elements)} schools")
                break
            except Exception as ex:
                logger.warning(f"    {name} attempt {attempt+1} failed: {ex}")
                time.sleep(5 * (attempt + 1))  # Longer backoff
        time.sleep(3)  # Rate limit between regions

    # Deduplicate by OSM ID
    seen = set()
    elements = []
    for e in all_elements:
        eid = e.get("id")
        if eid not in seen:
            seen.add(eid)
            elements.append(e)
    logger.info(f"  Found {len(elements)} schools from Overpass")

    records = []
    for e in elements:
        tags = e.get("tags", {})
        lat = e.get("lat") or (e.get("center", {}) or {}).get("lat")
        lon = e.get("lon") or (e.get("center", {}) or {}).get("lon")

        records.append({
            "osm_id": e.get("id"),
            "osm_name": tags.get("name", ""),
            "latitude": lat,
            "longitude": lon,
            "postcode": tags.get("addr:postcode", ""),
            "street": tags.get("addr:street", ""),
            "town": tags.get("addr:city", tags.get("addr:town", "")),
            "website": tags.get("website", tags.get("contact:website", "")),
            "phone": tags.get("phone", tags.get("contact:phone", "")),
            "osm_type": tags.get("school:type", ""),
            "urn": tags.get("ref:edubase", ""),  # Some OSM entries have URN
        })

    df = pd.DataFrame(records)
    df = df[df["osm_name"].str.strip() != ""]  # Remove unnamed
    df = df.drop_duplicates(subset=["osm_name", "latitude", "longitude"])

    df.to_csv(cache_path, index=False)
    logger.info(f"  Cached: {len(df)} schools with coordinates")
    return df


def match_osm_to_ks4(osm: pd.DataFrame, ks4_path: Path) -> pd.DataFrame:
    """Match OSM schools to KS4 performance data by URN or fuzzy name match."""
    logger.info("Matching OSM schools to KS4 performance data...")

    ks4 = pd.read_csv(ks4_path, dtype=str, low_memory=False)

    # Filter to most recent year, school level, overall (not by subgroup)
    if "time_period" in ks4.columns:
        ks4 = ks4[ks4["time_period"] == ks4["time_period"].max()]
    if "geographic_level" in ks4.columns:
        ks4 = ks4[ks4["geographic_level"] == "School"]

    # Filter to "Total" sex — but be flexible about exact value
    if "sex" in ks4.columns:
        total_vals = ks4["sex"].value_counts()
        total_val = next((v for v in total_vals.index if "total" in str(v).lower() or "all" in str(v).lower()), total_vals.index[0])
        ks4 = ks4[ks4["sex"] == total_val]

    # Filter disadvantage — be flexible
    if "disadvantage_status" in ks4.columns:
        dv = ks4["disadvantage_status"].value_counts()
        all_val = next((v for v in dv.index if "all" in str(v).lower()), dv.index[0])
        ks4 = ks4[ks4["disadvantage_status"] == all_val]

    ks4 = ks4.drop_duplicates(subset=["school_urn"])
    logger.info(f"  KS4: {len(ks4)} schools after filtering")

    # Method 1: Direct URN match (some OSM entries have ref:edubase)
    osm["urn_clean"] = osm["urn"].astype(str).str.strip()
    ks4["urn_clean"] = ks4["school_urn"].astype(str).str.strip()

    merged = osm.merge(
        ks4[["urn_clean", "school_name", "school_laestab", "la_name",
             "attainment8_average", "progress8_average", "establishment_type_group"]],
        on="urn_clean", how="left", suffixes=("_osm", "_ks4")
    )
    urn_matched = merged["school_name"].notna().sum()
    logger.info(f"  URN match: {urn_matched}/{len(osm)}")

    # Method 2: Name matching for unmatched schools
    unmatched = merged[merged["school_name"].isna()].copy()
    if len(unmatched) > 0:
        # Normalize names for fuzzy matching
        def normalize(name):
            if not isinstance(name, str):
                return ""
            return (name.lower()
                    .replace("the ", "").replace("school", "").replace("college", "")
                    .replace("academy", "").replace("  ", " ").strip())

        ks4_unmatched = ks4[~ks4["urn_clean"].isin(merged[merged["school_name"].notna()]["urn_clean"])]
        ks4_unmatched = ks4_unmatched.copy()
        ks4_unmatched["name_norm"] = ks4_unmatched["school_name"].apply(normalize)

        name_matches = 0
        for idx, row in unmatched.iterrows():
            osm_norm = normalize(row["osm_name"])
            if not osm_norm:
                continue
            # Find best match
            search_str = osm_norm[:15]
            if not search_str:
                continue
            candidates = ks4_unmatched[ks4_unmatched["name_norm"].str.contains(search_str, na=False, regex=False)]
            if len(candidates) == 0:
                # Try reverse: KS4 name in OSM name
                for _, ks4_row in ks4_unmatched.iterrows():
                    if ks4_row["name_norm"][:15] and ks4_row["name_norm"][:15] in osm_norm:
                        candidates = ks4_unmatched[ks4_unmatched.index == ks4_row.name]
                        break

            if len(candidates) == 1:
                match = candidates.iloc[0]
                merged.loc[idx, "school_name"] = match["school_name"]
                merged.loc[idx, "school_laestab"] = match["school_laestab"]
                merged.loc[idx, "la_name"] = match["la_name"]
                merged.loc[idx, "attainment8_average"] = match["attainment8_average"]
                merged.loc[idx, "progress8_average"] = match["progress8_average"]
                merged.loc[idx, "urn_clean"] = match["urn_clean"]
                name_matches += 1

        logger.info(f"  Name match: {name_matches} additional matches")

    # Use OSM name where KS4 name is missing
    merged["school_name"] = merged["school_name"].fillna(merged["osm_name"])
    merged["urn"] = merged["urn_clean"]

    total_with_perf = merged["attainment8_average"].notna().sum()
    logger.info(f"  Total with performance data: {total_with_perf}/{len(merged)}")

    return merged


def geocode_missing_via_postcodes(df: pd.DataFrame) -> pd.DataFrame:
    """Use postcodes.io to fill missing lat/lon from postcodes."""
    missing = df[(df["latitude"].isna()) & (df["postcode"].notna()) & (df["postcode"] != "")]
    if len(missing) == 0:
        return df

    logger.info(f"Geocoding {len(missing)} schools via postcodes.io...")

    # Batch lookup (postcodes.io supports 100 per request)
    postcodes = missing["postcode"].tolist()
    for i in range(0, len(postcodes), 100):
        batch = postcodes[i:i+100]
        try:
            resp = requests.post("https://api.postcodes.io/postcodes",
                                json={"postcodes": batch}, timeout=15)
            resp.raise_for_status()
            results = resp.json().get("result", [])
            for r in results:
                if r and r.get("result"):
                    pc = r["query"]
                    data = r["result"]
                    mask = (df["postcode"] == pc) & df["latitude"].isna()
                    df.loc[mask, "latitude"] = data["latitude"]
                    df.loc[mask, "longitude"] = data["longitude"]
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"  postcodes.io batch failed: {e}")

    filled = df["latitude"].notna().sum()
    logger.info(f"  Coordinates: {filled}/{len(df)} after postcodes.io")
    return df


def main():
    logger.info("=" * 60)
    logger.info("UK: OpenStreetMap School Locations + KS4 Match")
    logger.info("=" * 60)

    osm = fetch_osm_schools(CACHE_DIR / "osm_schools_england.csv")
    ks4_path = DATA_DIR / "raw" / "gb_ks4.csv"

    if ks4_path.exists():
        matched = match_osm_to_ks4(osm, ks4_path)
    else:
        logger.warning("KS4 data not found — using OSM data only")
        matched = osm
        matched["school_name"] = matched["osm_name"]

    # Geocode any missing coordinates
    matched = geocode_missing_via_postcodes(matched)

    # Save
    output = DATA_DIR / "intermediate" / "gb_school_master_base.csv"
    matched.to_csv(output, index=False)
    logger.info(f"Saved: {output} ({len(matched)} schools)")

    # Also merge IMD if available
    imd_path = DATA_DIR / "raw" / "gb_imd.csv"
    if imd_path.exists():
        logger.info("Merging IMD data...")
        imd = pd.read_csv(imd_path, dtype=str, low_memory=False)
        # Find LSOA and decile columns
        cols = imd.columns.tolist()
        lsoa_col = next((c for c in cols if "lsoa" in c.lower() and "code" in c.lower()), None)
        decile_col = next((c for c in cols if "decile" in c.lower() and "multiple" in c.lower()), None)
        if lsoa_col and decile_col:
            # We'd need LSOA codes to join — but OSM doesn't have them
            # Use postcodes.io to get LSOA from postcode
            logger.info("  Would need LSOA codes from postcodes.io for IMD join")

    return matched


if __name__ == "__main__":
    main()
