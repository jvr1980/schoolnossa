#!/usr/bin/env python3
"""
Download and cache Zensus 2022 100m grid data for school catchment profiling.

Downloads 5 thematic grid datasets from destatis.de, filters to city bounding
boxes, merges into a single parquet per city with WGS84 coordinates.

CSV format (all datasets):
    GITTER_ID_100m;x_mp_100m;y_mp_100m;{value};[werterlaeuternde_Zeichen]
    - Semicolon-separated, German decimal comma
    - x_mp_100m = easting centroid (EPSG:3035), y_mp_100m = northing centroid
    - Suppressed values shown as "–" (em-dash)

Usage:
    python scripts_shared/enrichment/download_zensus_grid.py
    python scripts_shared/enrichment/download_zensus_grid.py --city berlin
    python scripts_shared/enrichment/download_zensus_grid.py --force
"""

import argparse
import io
import os
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyproj import Transformer

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data_shared" / "zensus"
RAW_DIR = CACHE_DIR / "raw"

# ---------------------------------------------------------------------------
# Zensus 2022 grid data download URLs
# ---------------------------------------------------------------------------

ZENSUS_DATASETS = {
    "population": {
        "url": "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Bevoelkerungszahl.zip",
        "value_col": "Einwohner",
    },
    "avg_age": {
        "url": "https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittsalter_in_Gitterzellen.zip",
        "value_col": "Durchschnittsalter",
    },
    "foreigner_pct": {
        "url": "https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_in_Gitterzellen.zip",
        "value_col": "AnteilAuslaender",
    },
    "avg_rent": {
        "url": "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Durchschn_Nettokaltmiete.zip",
        "value_col": "durchschnMieteQM",
    },
    "vacancy_rate": {
        "url": "https://www.destatis.de/static/DE/zensus/gitterdaten/Leerstandsquote_in_Gitterzellen.zip",
        "value_col": "Leerstandsquote",
    },
}

# City bounding boxes in WGS84: (min_lat, max_lat, min_lon, max_lon)
CITY_BBOXES = {
    "berlin": (52.33, 52.69, 13.07, 13.78),
    "hamburg": (53.38, 53.75, 9.71, 10.34),
    "koeln": (50.82, 51.10, 6.78, 7.18),
    "duesseldorf": (51.11, 51.36, 6.67, 6.93),
    "frankfurt": (50.01, 50.23, 8.47, 8.80),
}

# Transformers (cached)
_to_3035 = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)
_to_4326 = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wgs84_bbox_to_3035(bbox):
    """Convert WGS84 (lat_min, lat_max, lon_min, lon_max) → EPSG:3035 (e_min, e_max, n_min, n_max)."""
    lat_min, lat_max, lon_min, lon_max = bbox
    corners = [
        _to_3035.transform(lon_min, lat_min),
        _to_3035.transform(lon_max, lat_min),
        _to_3035.transform(lon_min, lat_max),
        _to_3035.transform(lon_max, lat_max),
    ]
    eastings = [c[0] for c in corners]
    northings = [c[1] for c in corners]
    return min(eastings), max(eastings), min(northings), max(northings)


def download_zip(url, dest_path):
    """Download a ZIP file. Skip if exists."""
    if dest_path.exists():
        size_mb = dest_path.stat().st_size / 1024 / 1024
        print(f"    Cached: {dest_path.name} ({size_mb:.1f} MB)")
        return dest_path

    import urllib.request
    print(f"    Downloading: {dest_path.name} ...", end=" ", flush=True)
    start = time.time()
    urllib.request.urlretrieve(url, str(dest_path))
    elapsed = time.time() - start
    size_mb = dest_path.stat().st_size / 1024 / 1024
    print(f"{size_mb:.1f} MB in {elapsed:.1f}s")
    return dest_path


def find_100m_csv(zip_path):
    """Find the 100m CSV filename inside a ZIP."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv") and "100m" in name.lower():
                return name
    return None


def parse_german_float(s):
    """Parse German decimal format: '36,75' → 36.75. Returns NaN for suppressed/invalid."""
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    if not s or s in ("–", "-", ".", "...", "x", "X", "/"):
        return np.nan
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return np.nan


def load_grid_csv(zip_path, value_col_name, bbox_3035=None, chunksize=500_000):
    """Load 100m grid CSV from ZIP, filter to bounding box, return DataFrame.

    Returns DataFrame with columns: easting, northing, value
    The x_mp_100m and y_mp_100m columns ARE the cell centroids in EPSG:3035.
    """
    csv_name = find_100m_csv(zip_path)
    if not csv_name:
        raise ValueError(f"No 100m CSV in {zip_path}")

    print(f"    Reading: {csv_name}")

    frames = []
    total = 0
    kept = 0

    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(csv_name) as f:
            raw = f.read()

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    reader = pd.read_csv(
        io.StringIO(text), sep=";", chunksize=chunksize,
        dtype=str, keep_default_na=False,
    )

    for chunk in reader:
        total += len(chunk)

        # Parse coordinates (these are integers, always clean)
        easting = pd.to_numeric(chunk["x_mp_100m"], errors="coerce")
        northing = pd.to_numeric(chunk["y_mp_100m"], errors="coerce")

        # Filter to bounding box
        if bbox_3035 is not None:
            e_min, e_max, n_min, n_max = bbox_3035
            mask = (
                (easting >= e_min) & (easting <= e_max) &
                (northing >= n_min) & (northing <= n_max)
            )
            chunk = chunk[mask]
            easting = easting[mask]
            northing = northing[mask]

        if len(chunk) == 0:
            continue

        # Parse value column (German decimal, with suppressed values)
        values = chunk[value_col_name].apply(parse_german_float)

        df = pd.DataFrame({
            "easting": easting.values,
            "northing": northing.values,
            "value": values.values,
        })
        frames.append(df)
        kept += len(df)

    print(f"    Rows: {total:,} total → {kept:,} in bounding box")

    if not frames:
        return pd.DataFrame(columns=["easting", "northing", "value"])
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_city(city, force=False):
    """Download, parse, filter, and cache Zensus grid data for one city."""
    output_path = CACHE_DIR / f"{city}_zensus_100m.parquet"

    if output_path.exists() and not force:
        df = pd.read_parquet(output_path)
        print(f"  [{city}] Cached: {len(df):,} cells in {output_path.name}")
        return output_path

    bbox_wgs84 = CITY_BBOXES.get(city)
    if not bbox_wgs84:
        print(f"  [{city}] ERROR: No bounding box for '{city}'")
        return None

    bbox_3035 = wgs84_bbox_to_3035(bbox_wgs84)
    print(f"\n  [{city}] Processing Zensus 2022 grid data...")
    print(f"  [{city}] EPSG:3035 bbox: E={bbox_3035[0]:.0f}-{bbox_3035[1]:.0f}, "
          f"N={bbox_3035[2]:.0f}-{bbox_3035[3]:.0f}")

    # Load each dataset
    datasets = {}
    for key, info in ZENSUS_DATASETS.items():
        print(f"\n  [{city}] {key}:")
        zip_name = info["url"].split("/")[-1]
        zip_path = RAW_DIR / zip_name

        try:
            download_zip(info["url"], zip_path)
            df = load_grid_csv(zip_path, info["value_col"], bbox_3035=bbox_3035)
            if len(df) > 0:
                datasets[key] = df
                valid = df["value"].dropna()
                print(f"    ✓ {len(df):,} cells, {len(valid):,} with valid values")
            else:
                print(f"    ⚠ No cells in bounding box")
        except Exception as e:
            print(f"    ✗ Error: {e}")
            import traceback; traceback.print_exc()

    if not datasets:
        print(f"  [{city}] ERROR: No datasets loaded")
        return None

    # Merge on easting + northing (cell centroid coordinates)
    print(f"\n  [{city}] Merging {len(datasets)} datasets...")

    # Use population as base (widest coverage)
    base_key = "population" if "population" in datasets else list(datasets.keys())[0]
    merged = datasets[base_key][["easting", "northing"]].copy()
    merged["population"] = datasets[base_key]["value"]

    for key, df in datasets.items():
        if key == base_key:
            continue
        right = df.rename(columns={"value": key})
        merged = merged.merge(right, on=["easting", "northing"], how="left")

    # Convert centroids to WGS84
    print(f"  [{city}] Converting {len(merged):,} centroids to WGS84...")
    lons, lats = _to_4326.transform(merged["easting"].values, merged["northing"].values)
    merged["lat"] = lats
    merged["lon"] = lons

    # Save
    merged.to_parquet(output_path, index=False)
    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"\n  [{city}] ✓ Saved: {output_path.name} ({len(merged):,} cells, {size_mb:.1f} MB)")

    # Stats
    for col in ["population", "avg_age", "foreigner_pct", "avg_rent", "vacancy_rate"]:
        if col in merged.columns:
            v = merged[col].dropna()
            if len(v) > 0:
                print(f"    {col}: mean={v.mean():.2f}, median={v.median():.2f}, "
                      f"range=[{v.min():.2f}, {v.max():.2f}], "
                      f"coverage={len(v)}/{len(merged)} ({100*len(v)/len(merged):.0f}%)")

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Download Zensus 2022 grid data")
    parser.add_argument("--city", default="all",
                        choices=["berlin", "hamburg", "koeln", "duesseldorf", "frankfurt", "all"])
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    cities = list(CITY_BBOXES.keys()) if args.city == "all" else [args.city]

    print("=" * 70)
    print("  Zensus 2022 Grid Data — Download & Processing")
    print("=" * 70)

    for city in cities:
        process_city(city, force=args.force)

    print("\n  Done.")


if __name__ == "__main__":
    main()
