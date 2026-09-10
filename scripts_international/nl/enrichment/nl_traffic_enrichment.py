#!/usr/bin/env python3
"""
NL Phase 3: Traffic/Road Safety Enrichment

Per-school accident counts within 500m/1000m, mirroring the German Unfallatlas
enrichment and the GB STATS19 one.

Source: Rijkswaterstaat "Verkeersongevallen Nederland" (BRON) WFS — the geocoded
release. Note this is NOT the flat ZIP on downloads.rijkswaterstaatdata.nl: that
one links accidents to NWB road segments (wegvak_id + hectometer) and carries no
coordinates, which is why the previous gemeente-level fallback here produced no
usable per-school signal. The WFS serves point geometry directly.

  Layer:    ongevallen_2022_2024 (single-year layers also exist)
  CRS:      EPSG:28992 (RD New) — metric, so radius maths is plain Euclidean
  Geometry: 'shape' column as WKT POINT
  Severity: 'verkeersongeval_afloop' -> Dodelijk / Letsel / Uitsluitend materiele schade
  Licence:  CC0 1.0

Input:  data_nl/intermediate/nl_school_master_geocoded.csv
Output: data_nl/intermediate/nl_schools_with_traffic.csv
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nl"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WFS_URL = "https://geo.rijkswaterstaat.nl/services/ogc/gdr/verkeersongevallen_nederland/ows"
WFS_LAYER = "ongevallen_2022_2024"
ACCIDENT_YEARS = "2022-2024"
RD_NEW = "EPSG:28992"

SEVERITY_COL = "verkeersongeval_afloop"
YEAR_COL = "jaar_ongeval"
FATAL_VALUE = "Dodelijk"
INJURY_VALUE = "Letsel"

# WGS84 school coordinates -> RD New. pyproj is the accurate route; without it
# we fall back to a local equirectangular approximation, which is well within
# tolerance for 500m/1000m counting at Dutch latitudes.
NL_LAT0, NL_LON0 = 52.15517440, 5.38720621  # Amersfoort, RD New origin
RD_X0, RD_Y0 = 155000.0, 463000.0


def _download_accidents(cache_path: Path) -> pd.DataFrame:
    """Fetch geocoded accidents from the WFS, cached as a slim local CSV."""
    if cache_path.exists():
        logger.info(f"Loading cached accidents: {cache_path.name}")
        return pd.read_csv(cache_path)

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": WFS_LAYER,
        "outputFormat": "csv",
        "srsName": RD_NEW,
    }
    logger.info(f"Downloading {WFS_LAYER} from Rijkswaterstaat WFS (~100MB, one-off)...")
    resp = requests.get(WFS_URL, params=params, timeout=1800, stream=True)
    resp.raise_for_status()

    raw_path = CACHE_DIR / f"bron_{WFS_LAYER}_raw.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(raw_path, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            fh.write(chunk)
            written += len(chunk)
    logger.info(f"  Downloaded {written / 1024 / 1024:.0f} MB")

    df = pd.read_csv(raw_path, low_memory=False)
    logger.info(f"  Raw: {len(df)} accidents, {len(df.columns)} columns")

    geom_col = next((c for c in df.columns if c.lower() in ("shape", "geom", "geometry")), None)
    if geom_col is None:
        logger.error(f"  No geometry column found. Columns: {list(df.columns)[:20]}")
        return pd.DataFrame()

    # "POINT (131437.148 422301.464)" -> x, y
    coords = df[geom_col].astype(str).str.extract(
        r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)")
    slim = pd.DataFrame({
        "x": pd.to_numeric(coords[0], errors="coerce"),
        "y": pd.to_numeric(coords[1], errors="coerce"),
        "severity": df.get(SEVERITY_COL),
        "year": pd.to_numeric(df.get(YEAR_COL), errors="coerce"),
    }).dropna(subset=["x", "y"])

    logger.info(f"  Geocoded: {len(slim)}/{len(df)} accidents")
    logger.info(f"  Severity mix: {slim['severity'].value_counts().head(4).to_dict()}")
    slim.to_csv(cache_path, index=False)
    return slim


def _to_rd_new(lat: pd.Series, lon: pd.Series) -> tuple[pd.Series, pd.Series]:
    """WGS84 -> RD New (EPSG:28992) metres."""
    try:
        from pyproj import Transformer
        transformer = Transformer.from_crs("EPSG:4326", RD_NEW, always_xy=True)
        x, y = transformer.transform(lon.values, lat.values)
        return pd.Series(x, index=lat.index), pd.Series(y, index=lat.index)
    except ImportError:
        logger.warning("  pyproj unavailable — using equirectangular approximation")
        m_per_deg_lat = 111132.0
        m_per_deg_lon = 111320.0 * np.cos(np.radians(NL_LAT0))
        x = RD_X0 + (lon - NL_LON0) * m_per_deg_lon
        y = RD_Y0 + (lat - NL_LAT0) * m_per_deg_lat
        return x, y


def enrich_with_traffic(schools: pd.DataFrame, accidents: pd.DataFrame) -> pd.DataFrame:
    """Count accidents within 500m/1000m of each school (Berlin/GB semantics)."""
    if accidents.empty:
        logger.error("No accident data — traffic columns left empty. "
                     "This is a failure, not a valid result.")
        for col in ("traffic_accidents_500m", "traffic_accidents_1000m",
                    "traffic_accidents_fatal_1000m", "traffic_volume_index"):
            schools[col] = np.nan
        schools["traffic_data_source"] = "BRON (download failed)"
        return schools

    lat = pd.to_numeric(schools["latitude"], errors="coerce")
    lon = pd.to_numeric(schools["longitude"], errors="coerce")
    sx, sy = _to_rd_new(lat, lon)

    ax = accidents["x"].to_numpy()
    ay = accidents["y"].to_numpy()
    severity = accidents["severity"].astype(str)
    fatal = severity.eq(FATAL_VALUE).to_numpy()
    injury = severity.isin([FATAL_VALUE, INJURY_VALUE]).to_numpy()

    n500 = np.full(len(schools), np.nan)
    n1000 = np.full(len(schools), np.nan)
    nfatal = np.full(len(schools), np.nan)
    ninjury = np.full(len(schools), np.nan)

    # Bucket accidents into a 1km grid so each school only tests nearby cells
    # instead of all ~380k points (1626 x 380k would be ~600M distance pairs).
    cell = 1000.0
    grid: dict[tuple[int, int], list[int]] = {}
    for idx, (gx, gy) in enumerate(zip((ax // cell).astype(int), (ay // cell).astype(int))):
        grid.setdefault((gx, gy), []).append(idx)

    for i, (x, y) in enumerate(zip(sx.to_numpy(), sy.to_numpy())):
        if not np.isfinite(x) or not np.isfinite(y):
            continue
        gx, gy = int(x // cell), int(y // cell)
        near: list[int] = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                near.extend(grid.get((gx + dx, gy + dy), ()))
        if not near:
            n500[i] = n1000[i] = nfatal[i] = ninjury[i] = 0
            continue
        near_idx = np.asarray(near)
        d = np.hypot(ax[near_idx] - x, ay[near_idx] - y)
        within_1000 = d <= 1000
        n500[i] = int(np.sum(d <= 500))
        n1000[i] = int(np.sum(within_1000))
        nfatal[i] = int(np.sum(within_1000 & fatal[near_idx]))
        ninjury[i] = int(np.sum(within_1000 & injury[near_idx]))
        if (i + 1) % 500 == 0:
            logger.info(f"  [{i + 1}/{len(schools)}] schools processed")

    schools["traffic_accidents_500m"] = n500
    schools["traffic_accidents_1000m"] = n1000
    schools["traffic_accidents_fatal_1000m"] = nfatal
    schools["traffic_accidents_injury_1000m"] = ninjury
    schools["traffic_accidents_year"] = ACCIDENT_YEARS

    max_acc = np.nanmax(n1000) if np.isfinite(n1000).any() else 0
    schools["traffic_volume_index"] = (
        (n1000 / max_acc * 10).round(1) if max_acc > 0 else np.nan)
    schools["traffic_data_source"] = f"BRON {ACCIDENT_YEARS} (Rijkswaterstaat, CC0)"

    filled = int(np.isfinite(n1000).sum())
    logger.info(f"Schools with traffic data: {filled}/{len(schools)}")
    if filled == 0:
        raise RuntimeError("Traffic enrichment produced zero rows — refusing to "
                           "report success (see April 2026 silent no-op).")
    logger.info(f"  Median accidents within 1000m: {np.nanmedian(n1000):.0f}")
    return schools


def main():
    logger.info("=" * 60)
    logger.info("NL Phase 3: Traffic Enrichment (BRON geocoded)")
    logger.info("=" * 60)

    input_path = INTERMEDIATE_DIR / "nl_school_master_geocoded.csv"
    if not input_path.exists():
        logger.error(f"Input not found: {input_path}")
        sys.exit(1)

    schools = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded {len(schools)} schools")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    accidents = _download_accidents(CACHE_DIR / f"bron_points_{WFS_LAYER}.csv")
    enriched = enrich_with_traffic(schools, accidents)

    output = INTERMEDIATE_DIR / "nl_schools_with_traffic.csv"
    enriched.to_csv(output, index=False)
    logger.info(f"Saved: {output}")


if __name__ == "__main__":
    main()
