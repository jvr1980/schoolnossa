#!/usr/bin/env python3
"""
Build the NL rows exactly as the live Supabase tables expect them.

The Berlin-schema parquet is the cross-country interchange format; the live
`schools` / `primary_schools` tables are a narrower, app-specific contract
(docs/LOVABLE_DATA_ADMIN_PROCESSING.md section 6). This is the adapter between
the two, and the only place NL rows are shaped for the app:

  - schulnummer   'NL-' + BRIN6. Unique per table across every city, like the
                  'STG-' / 'BY-SCHUL_' prefixes, so no future format collides.
  - city          'nl-' + gemeente slug. The app scopes every read by city and
                  PostgREST caps responses at 1,000 rows; one national bucket
                  (6,060 primary schools) would be silently truncated, while the
                  largest gemeente (Amsterdam, 196) is well under the cap. The
                  web picker is hardcoded to the German ids, so these rows stay
                  invisible there until the NL navigation ships.
  - geo_*         country -> region -> gemeente for that navigation.
  - traegerschaft the app's own vocabulary ('Öffentlich' / 'Privat'), which is
                  what its private/public rule matches on.
  - ortsteil /    display-cased gemeente / province; the UI falls back to
    bezirk        "Berlin" when these are empty.
  - transit       stop #1 is un-suffixed in the live tables
                  (transit_rail_name, not transit_rail_01_name).
  - description_en needed separately — the app has no EN/DE cross-fallback.
  - embedding     PostgREST takes vectors as '[f1,f2,...]' text.

Column set and types come from a snapshot of the live information_schema
(live_schema_source.json), so the output only holds columns that exist, typed
the way they are stored.

Output: data_shared/supabase_upload/nl_{schools,primary_schools}.jsonl
"""

import argparse
import json
import logging
import math
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


OUT_DIR = PROJECT_ROOT / "data_shared" / "supabase_upload"
LIVE_SCHEMA_SNAPSHOT = OUT_DIR / "live_schema_source.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

LEVELS = {
    "schools": {
        "berlin": PROJECT_ROOT / "data_nl/final/nl_school_master_table_berlin_schema.parquet",
        "core": PROJECT_ROOT / "data_nl/final/nl_school_master_table_final.parquet",
    },
    "primary_schools": {
        "berlin": PROJECT_ROOT / "data_nl_po/final/nl_po_school_master_table_berlin_schema.parquet",
        "core": PROJECT_ROOT / "data_nl_po/final/nl_po_school_master_table_final.parquet",
    },
}

# Columns created by the upload DDL, not yet present in the live tables.
NEW_COLUMNS = {"geo_country": "text", "geo_region": "text", "geo_municipality": "text"}

# Set by the database, never by us.
DB_MANAGED = {"id", "created_at", "updated_at"}

GERMAN_CITY_IDS = {"berlin", "hamburg", "duesseldorf", "koeln", "frankfurt",
                   "muenchen", "dresden", "stuttgart", "bremen", "leipzig"}

POSTGREST_ROW_CAP = 1000

INT32_MAX = 2**31 - 1


def live_columns() -> dict[str, dict[str, str]]:
    """{table: {column: postgres_type}} for the live tables.

    Read from a snapshot of information_schema taken through the Lovable MCP
    SQL tool: the PostgREST OpenAPI root is service-role only, so the anon key
    cannot introspect it. Refresh the snapshot whenever the live schema changes.
    """
    snap = json.loads(LIVE_SCHEMA_SNAPSHOT.read_text())
    out = {}
    for table in LEVELS:
        if table not in snap:
            raise RuntimeError(f"{LIVE_SCHEMA_SNAPSHOT.name} has no {table}")
        out[table] = dict(snap[table])
    return out


def slugify(name: str) -> str:
    ascii_ = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", ascii_.lower())).strip("-")


def _clean(value):
    """JSON-safe scalar: NaN / 'nan' / '' / 'None' -> None."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, str) and value.strip() in ("", "nan", "None", "NaN", "<NA>"):
        return None
    if value is pd.NA or value is pd.NaT:
        return None
    return value


def coerce(value, pg_type: str):
    value = _clean(value)
    if value is None:
        return None
    t = pg_type.lower()
    if "vector" in t:
        arr = np.asarray(value, dtype=float)
        return "[" + ",".join(f"{x:.7g}" for x in arr) + "]"
    if t in ("integer", "bigint", "smallint"):
        try:
            v = int(round(float(value)))
        except (TypeError, ValueError):
            return None
        return v if abs(v) <= INT32_MAX else None
    if t in ("numeric", "double precision", "real"):
        try:
            v = float(value)
        except (TypeError, ValueError):
            return None
        return None if math.isnan(v) else v
    if t == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("true", "1", "yes", "ja")
    if t in ("jsonb", "json"):
        return value
    return str(value)


def build(table: str, cols: dict[str, str]) -> list[dict]:
    paths = LEVELS[table]
    b = pd.read_parquet(paths["berlin"])
    c = pd.read_parquet(paths["core"])
    if len(b) != len(c):
        raise RuntimeError(f"{table}: berlin-schema ({len(b)}) and core ({len(c)}) row counts differ")
    c = c.set_index(c["school_id"].astype(str))
    ids = b["schulnummer"].astype(str)
    core = c.loc[ids.values].reset_index(drop=True)

    df = b.copy()
    df["schulnummer"] = "NL-" + ids.values
    df["geo_country"] = "NL"
    df["geo_region"] = core["geo_region"].values
    df["geo_municipality"] = core["geo_municipality"].values
    df["city"] = ["nl-" + slugify(m) for m in core["geo_municipality"].values]
    df["ortsteil"] = core["geo_municipality"].values
    df["bezirk"] = core["geo_region"].values
    df["traegerschaft"] = (core["ownership"].map({"public": "Öffentlich",
                                                  "private": "Privat"}).values)
    df["description_en"] = core["description"].values
    df["description"] = core["description"].values
    df["embedding"] = core["embedding"].values

    # Human-readable type. Secondary keeps DUO's onderwijsstructuur verbatim —
    # it is '/'-separated tracks, which is what a multi-select "offers HAVO"
    # filter needs. Praktijkonderwijs and primary get their Dutch names.
    subtype = core["school_subtype"].astype(str).values
    if table == "primary_schools":
        df["school_type"] = "Basisschool"
    else:
        df["school_type"] = ["Praktijkonderwijs" if s == "PRO" else s for s in subtype]
    df["schulart"] = subtype

    if "belastungsstufe" in df.columns:
        df["belastungsstufe"] = [None if pd.isna(v) else str(int(round(float(v))))
                                 for v in df["belastungsstufe"]]

    # Live tables keep stop #1 un-suffixed.
    for mode in ("rail", "tram", "bus"):
        for field in ("name", "lines", "distance_m"):
            src = f"transit_{mode}_01_{field}"
            if src in df.columns:
                df[f"transit_{mode}_{field}"] = df[src]

    df["metadata_source"] = "DUO Open Onderwijsdata (NL)"

    wanted = [col for col in cols if col not in DB_MANAGED and col in df.columns]
    missing_live = sorted(set(cols) - DB_MANAGED - set(df.columns))
    logger.info(f"{table}: {len(wanted)} columns mapped, "
                f"{len(missing_live)} live columns with no NL source (left NULL)")

    rows = []
    for rec in df[wanted].to_dict(orient="records"):
        row = {col: coerce(rec[col], cols[col]) for col in wanted}
        rows.append({k: v for k, v in row.items() if v is not None})
    return rows


def validate(table: str, rows: list[dict], cols: dict[str, str]) -> list[str]:
    problems = []
    ids = [r.get("schulnummer") for r in rows]
    if any(not i for i in ids):
        problems.append("null schulnummer")
    if len(set(ids)) != len(ids):
        problems.append(f"duplicate schulnummer ({len(ids) - len(set(ids))})")
    if any(not str(i).startswith("NL-") for i in ids if i):
        problems.append("schulnummer without NL- prefix")
    if any(not r.get("schulname") for r in rows):
        problems.append("null schulname")

    cities = pd.Series([r.get("city") for r in rows])
    if cities.isna().any():
        problems.append("null city")
    if set(cities.dropna()) & GERMAN_CITY_IDS:
        problems.append("city collides with a German city id")
    biggest = cities.value_counts().max()
    if biggest >= POSTGREST_ROW_CAP:
        problems.append(f"a city bucket has {biggest} rows (>= PostgREST cap)")

    t = pd.Series([r.get("transit_accessibility_score") for r in rows]).dropna()
    if len(t) and (t.min() < 0 or t.max() > 100):
        problems.append(f"transit score outside 0-100 ({t.min()}..{t.max()})")

    vec_cols = [c for c, typ in cols.items() if "vector" in typ.lower()]
    for vc in vec_cols:
        dims = {r[vc].count(",") + 1 for r in rows if vc in r}
        if dims and dims != {768}:
            problems.append(f"{vc} dims {dims}")

    for r in rows:
        for k, v in r.items():
            if isinstance(v, str) and k in cols and cols[k].lower() in ("integer", "numeric"):
                problems.append(f"{k} holds text")
                break
        else:
            continue
        break
    return problems


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    schema = live_columns()
    for table in schema:
        schema[table].update(NEW_COLUMNS)
    (out_dir / "live_schema.json").write_text(json.dumps(schema, indent=1, sort_keys=True))

    ok = True
    for table, cols in schema.items():
        rows = build(table, cols)
        problems = validate(table, rows, cols)
        path = out_dir / f"nl_{table}.jsonl"
        with open(path, "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")

        cities = pd.Series([r["city"] for r in rows])
        filled = {c: sum(1 for r in rows if c in r) for c in
                  ("description_de", "description_en", "embedding", "schueler_current",
                   "lehrer_current", "transit_accessibility_score", "crime_safety_rank",
                   "poi_supermarket_count_500m", "belastungsstufe", "geo_municipality")}
        logger.info(f"{table}: {len(rows)} rows, {cities.nunique()} city buckets "
                    f"(largest {cities.value_counts().max()}), "
                    f"{path.stat().st_size / 1e6:.1f} MB -> {path.name}")
        for col, n in filled.items():
            logger.info(f"    {col:30s} {n:5d}/{len(rows)}")
        if problems:
            ok = False
            for p in problems:
                logger.error(f"  PROBLEM {table}: {p}")
        else:
            logger.info(f"  {table}: validation clean")

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
