#!/usr/bin/env python3
"""
Bulk-overwrite `embedding` values in Lovable Supabase from local parquets.

Use case:
  After regenerating every city's embeddings locally with a new provider
  or dimension (see regenerate_embeddings_gemini_768.py), push the new
  vectors into Supabase so the frontend's pgvector queries use them.

Unlike upload_to_supabase.py (fill-gaps only), this script OVERWRITES
existing values because the vectors from a different provider / dim are
already stale — there's no "leave the old one" semantics here.

Match schools by `schulnummer`. Same Supabase instance + anon key as the
other uploaders — needs the `temp_anon_overwrite_embedding` policy to
be active on both tables (USING true WITH CHECK true).

Usage:
    python3 scripts_shared/upload_embeddings_to_supabase.py --dry-run
    python3 scripts_shared/upload_embeddings_to_supabase.py --cities munich_primary
    python3 scripts_shared/upload_embeddings_to_supabase.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("upload_embeddings")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SUPABASE_URL = "https://whzvzoumldeqgyrqlilt.supabase.co/rest/v1"
SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoenZ6b3VtbGRlcWd5cnFsaWx0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg3OTQ0MzEsImV4cCI6MjA4NDM3MDQzMX0."
    "ex4S1up25OAcGD8hQoOSfzf3NVAG5qCmNriixYfAAKs"
)


def _auth_key() -> str:
    return os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or SUPABASE_ANON_KEY


def _headers(write: bool = False) -> Dict[str, str]:
    key = _auth_key()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


# ---------------------------------------------------------------------------
# Targets — (city_key, supabase_table, local_parquet_path)
# ---------------------------------------------------------------------------
TARGETS: List[Tuple[str, str, str]] = [
    ("berlin",              "schools",         "data_berlin/final/school_master_table_final_with_embeddings.parquet"),
    ("berlin_primary",      "primary_schools", "data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet"),
    ("bremen",              "schools",         "data_bremen/final/bremen_school_master_table_final_with_embeddings.parquet"),
    ("dresden",             "schools",         "data_dresden/final/dresden_school_master_table_final.parquet"),
    ("frankfurt_primary",   "primary_schools", "data_frankfurt/final/frankfurt_primary_school_master_table_final_with_embeddings.parquet"),
    ("frankfurt_secondary", "schools",         "data_frankfurt/final/frankfurt_secondary_school_master_table_final_with_embeddings.parquet"),
    ("hamburg",             "schools",         "data_hamburg/final/hamburg_school_master_table_final_with_embeddings.parquet"),
    ("hamburg_primary",     "primary_schools", "data_hamburg_primary/final/hamburg_primary_school_master_table_final_with_embeddings.parquet"),
    ("leipzig",             "schools",         "data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet"),
    ("munich_primary",      "primary_schools", "data_munich/final/munich_primary_school_master_table_final_with_embeddings.parquet"),
    ("munich_secondary",    "schools",         "data_munich/final/munich_secondary_school_master_table_final_with_embeddings.parquet"),
    ("nrw_primary",         "primary_schools", "data_nrw/final/nrw_primary_school_master_table_final_with_embeddings.parquet"),
    ("nrw_secondary",       "schools",         "data_nrw/final/nrw_secondary_school_master_table_final_with_embeddings.parquet"),
    ("stuttgart_primary",   "primary_schools", "data_stuttgart/final/stuttgart_primary_school_master_table_final_with_embeddings.parquet"),
    ("stuttgart_secondary", "schools",         "data_stuttgart/final/stuttgart_secondary_school_master_table_final_with_embeddings.parquet"),
]


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _request(method: str, path: str, body=None, timeout: int = 90):
    url = f"{SUPABASE_URL}/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=_headers(write=method != "GET"), method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        content = resp.read().decode()
        return resp.status, json.loads(content) if content.strip() else None
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        return e.code, body_txt
    except Exception as e:
        # Timeouts, connection resets etc. — surface as a generic error
        return 0, f"request exception: {e}"


# ---------------------------------------------------------------------------
# Work
# ---------------------------------------------------------------------------

def _vec_to_str(vec) -> Optional[str]:
    """pgvector expects '[1.0,2.0,...]'. Accept list/tuple/ndarray."""
    if vec is None:
        return None
    try:
        floats = [float(x) for x in vec]
    except (TypeError, ValueError):
        return None
    if not floats:
        return None
    return "[" + ",".join(repr(x) for x in floats) + "]"


def process_target(city_key: str, table: str, parquet_path: Path, dry_run: bool, limit: int) -> Dict[str, int]:
    summary = {
        "city": city_key, "table": table,
        "loaded": 0, "matched_by_snr": 0,
        "pushed": 0, "dim_mismatch": 0, "http_errors": 0, "missing_in_supabase": 0,
    }
    if not parquet_path.exists():
        logger.warning(f"[{city_key}] parquet missing: {parquet_path}")
        return summary

    df = pd.read_parquet(parquet_path)
    summary["loaded"] = len(df)
    if limit:
        df = df.head(limit)

    if "schulnummer" not in df.columns or "embedding" not in df.columns:
        logger.error(f"[{city_key}] parquet missing required columns")
        return summary

    # Normalize schulnummer to str for matching
    df["schulnummer"] = df["schulnummer"].astype(str)

    # Detect local dim for sanity logging
    local_dim = None
    for v in df["embedding"]:
        if v is not None and hasattr(v, "__len__") and len(v) > 0:
            local_dim = len(v)
            break
    logger.info(f"[{city_key}] {parquet_path.name}: {len(df)} rows, local dim={local_dim}")

    # Build the work items first
    work = []
    for row in df.itertuples(index=False):
        snr = str(getattr(row, "schulnummer", "")).strip()
        vec = getattr(row, "embedding", None)
        if not snr or vec is None or not hasattr(vec, "__len__") or len(vec) == 0:
            continue
        summary["matched_by_snr"] += 1
        vec_str = _vec_to_str(vec)
        if vec_str is None:
            continue
        work.append((snr, vec_str, len(vec)))

    if dry_run:
        summary["pushed"] = len(work)
        for snr, _, dim in work[:3]:
            logger.info(f"  [{city_key}] DRY-RUN would PATCH {snr} (len={dim})")
        return summary

    pushed_counter = [0]
    lock = Lock()
    workers = int(os.environ.get("EMBEDDING_UPLOAD_WORKERS", "8"))

    def _one(item):
        snr, vec_str, _dim = item
        status, body = _request(
            "PATCH",
            f"{table}?schulnummer=eq.{snr}",
            {"embedding": vec_str},
        )
        return snr, status, body

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, item): item for item in work}
        for i, fut in enumerate(as_completed(futures), start=1):
            snr, status, body = fut.result()
            if status == 204:
                with lock:
                    pushed_counter[0] += 1
            elif status in (400, 422):
                summary["dim_mismatch"] += 1
                if summary["dim_mismatch"] <= 3:
                    logger.warning(f"  [{city_key}] {snr}: {status} {str(body)[:160]}")
            elif status in (401, 403):
                summary["http_errors"] += 1
                if summary["http_errors"] <= 3:
                    logger.warning(f"  [{city_key}] {snr}: {status} (RLS) {str(body)[:120]}")
            else:
                summary["http_errors"] += 1
                if summary["http_errors"] <= 3:
                    logger.warning(f"  [{city_key}] {snr}: {status} {str(body)[:160]}")

            if i % 50 == 0 or i == len(work):
                logger.info(f"  [{city_key}] progress {i}/{len(work)} — pushed={pushed_counter[0]}")

    summary["pushed"] = pushed_counter[0]
    logger.info(
        f"[{city_key}] done — pushed={summary['pushed']} "
        f"dim_mismatch={summary['dim_mismatch']} "
        f"http_errors={summary['http_errors']}"
    )
    return summary


# ---------------------------------------------------------------------------
# Connectivity / auth probe
# ---------------------------------------------------------------------------

def probe_write_access() -> bool:
    """Canary: try to PATCH one row's embedding back to its current value."""
    status, body = _request("GET", "schools?select=id,schulnummer,embedding&limit=1")
    if status != 200 or not body:
        logger.error(f"GET probe failed: {status} {body}")
        return False
    row = body[0]
    vec = row.get("embedding")
    # Supabase stores vector as a PG string; pass it through unchanged
    if vec is None:
        logger.warning("probe row has NULL embedding — cannot canary write")
        return True  # let the run start; it'll error if RLS is bad
    status2, body2 = _request(
        "PATCH",
        f"schools?id=eq.{row['id']}",
        {"embedding": vec},
    )
    if status2 == 204:
        logger.info("Write-access probe OK (embedding roundtrip succeeded)")
        return True
    logger.error(f"Write-access probe FAILED: {status2} {str(body2)[:200]}")
    return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="plan only")
    parser.add_argument("--cities", default="all", help="'all' or comma-separated city keys")
    parser.add_argument("--limit", type=int, default=0, help="cap rows per city (0 = none)")
    parser.add_argument("--no-probe", action="store_true", help="skip the write-access canary")
    args = parser.parse_args(argv)

    if args.cities == "all":
        targets = TARGETS
    else:
        wanted = {k.strip() for k in args.cities.split(",") if k.strip()}
        targets = [t for t in TARGETS if t[0] in wanted]
        if not targets:
            logger.error(f"No matching cities for: {args.cities}")
            return 1

    mode = "DRY RUN" if args.dry_run else "LIVE"
    logger.info("=" * 68)
    logger.info(f"EMBEDDING UPLOAD — {mode} — {len(targets)} targets")
    logger.info("=" * 68)

    if not args.dry_run and not args.no_probe:
        if not probe_write_access():
            logger.error("Aborting. Ask Lovable to install temp_anon_overwrite_embedding.")
            return 1

    summaries = []
    for city_key, table, rel_path in targets:
        summaries.append(process_target(city_key, table, PROJECT_ROOT / rel_path, args.dry_run, args.limit))

    print("\n" + "=" * 68)
    print("EMBEDDING UPLOAD SUMMARY")
    print("=" * 68)
    total_pushed = 0
    total_errors = 0
    for s in summaries:
        print(
            f"  {s['city']:<22} {s['table']:<16} loaded={s['loaded']:<5} "
            f"pushed={s['pushed']:<5} dim_mismatch={s['dim_mismatch']:<3} "
            f"http_errors={s['http_errors']}"
        )
        total_pushed += s["pushed"]
        total_errors += s["dim_mismatch"] + s["http_errors"]
    print(f"\n  TOTAL: pushed={total_pushed}, errors={total_errors}")
    if args.dry_run:
        print("  DRY RUN — nothing was written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
