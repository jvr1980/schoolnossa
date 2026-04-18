#!/usr/bin/env python3
"""
Upload admission criteria + open day data to the Lovable Supabase instance.

Reads data_shared/admission_open_days_all_german_cities.csv and PATCHes
the schools table via REST API for each school with usable data.

Prerequisites:
    1. Run data_shared/alter_schools_admission_columns.sql in the
       Supabase SQL editor to add the new columns.
    2. Ensure RLS allows anon writes (already relaxed from Abitur upload).

Usage:
    python3 scripts_shared/upload_admission_open_days_to_supabase.py --dry-run
    python3 scripts_shared/upload_admission_open_days_to_supabase.py
    python3 scripts_shared/upload_admission_open_days_to_supabase.py --force  # overwrite existing values
"""

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("upload_admission")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Same Supabase instance as upload_to_supabase.py
SUPABASE_URL = "https://whzvzoumldeqgyrqlilt.supabase.co/rest/v1"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoenZ6b3VtbGRlcWd5cnFsaWx0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg3OTQ0MzEsImV4cCI6MjA4NDM3MDQzMX0."
    "ex4S1up25OAcGD8hQoOSfzf3NVAG5qCmNriixYfAAKs"
)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

INPUT_CSV = PROJECT_ROOT / "data_shared" / "admission_open_days_all_german_cities.csv"

ADMISSION_COLUMNS = [
    "admission_criteria_bullets",
    "admission_application_window",
    "admission_notes_de",
    "open_days",
    "last_open_day_seen",
    "admission_fetched_at",
]

REQUEST_DELAY = 0.1  # seconds between API calls


def supabase_request(method: str, path: str, data=None):
    url = f"{SUPABASE_URL}/{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=HEADERS, method=method)
    try:
        resp = urllib.request.urlopen(req)
        content = resp.read().decode()
        return json.loads(content) if content.strip() else None
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        logger.error(f"  HTTP {e.code}: {body_text[:300]}")
        raise


def check_school_exists(schulnummer: str) -> bool:
    """Check if a school exists in Supabase by schulnummer."""
    path = f"schools?schulnummer=eq.{schulnummer}&select=schulnummer"
    try:
        result = supabase_request("GET", path)
        return bool(result)
    except Exception:
        return False


def check_school_has_admission(schulnummer: str) -> bool:
    """Check if the school already has admission data."""
    path = (
        f"schools?schulnummer=eq.{schulnummer}"
        f"&select=admission_criteria_bullets"
        f"&admission_criteria_bullets=not.is.null"
    )
    try:
        result = supabase_request("GET", path)
        return bool(result)
    except Exception:
        return False


def _safe_json(value, default=None):
    """Parse a JSON string from CSV, return parsed object or default."""
    if pd.isna(value) or value == "" or value == "null":
        return default
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default


def _safe_str(value) -> Optional[str]:
    if pd.isna(value) or str(value).strip() in ("", "nan", "null"):
        return None
    return str(value).strip()


def build_patch_body(row: pd.Series) -> dict:
    """Build the JSON body for a PATCH request from a CSV row."""
    body = {}

    bullets = _safe_json(row.get("admission_criteria_bullets"), [])
    if bullets:
        body["admission_criteria_bullets"] = bullets

    window = _safe_json(row.get("admission_application_window"))
    if window:
        body["admission_application_window"] = window

    notes = _safe_str(row.get("admission_notes_de"))
    if notes:
        body["admission_notes_de"] = notes

    open_days = _safe_json(row.get("open_days"), [])
    if open_days:
        body["open_days"] = open_days

    last_seen = _safe_str(row.get("last_open_day_seen"))
    if last_seen:
        body["last_open_day_seen"] = last_seen

    fetched_at = _safe_str(row.get("fetched_at"))
    if fetched_at:
        body["admission_fetched_at"] = fetched_at

    return body


def patch_school(schulnummer: str, body: dict, force: bool = False) -> str:
    """PATCH a school's admission data. Returns status string."""
    if not force:
        # Fill-gaps-only: skip if school already has admission data
        if check_school_has_admission(schulnummer):
            return "already_has_data"

    path = f"schools?schulnummer=eq.{schulnummer}"
    try:
        supabase_request("PATCH", path, body)
        return "patched"
    except urllib.error.HTTPError as e:
        return f"error_{e.code}"
    except Exception as e:
        return f"error_{e}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be uploaded without making changes")
    parser.add_argument("--force", action="store_true", help="Overwrite existing admission data (default: fill gaps only)")
    parser.add_argument("--limit", type=int, default=0, help="Cap number of schools to upload (0 = no limit)")
    args = parser.parse_args(argv)

    if not INPUT_CSV.exists():
        logger.error(f"Input file not found: {INPUT_CSV}")
        return 1

    df = pd.read_csv(INPUT_CSV, dtype=str)
    logger.info(f"Loaded {len(df)} rows from {INPUT_CSV.name}")

    # Filter to successful enrichments only
    df = df[df["gemini_status"] == "success"].copy()
    logger.info(f"After filtering to success: {len(df)} rows")

    # Deduplicate by schulnummer (keep first occurrence — some schools appear in both primary and all-schools tables)
    df = df.drop_duplicates(subset=["schulnummer"], keep="first")
    logger.info(f"After dedup by schulnummer: {len(df)} unique schools")

    if args.limit:
        df = df.head(args.limit)
        logger.info(f"--limit {args.limit} → processing {len(df)}")

    # Build patch bodies and filter out empty ones
    records = []
    for _, row in df.iterrows():
        body = build_patch_body(row)
        if body:
            records.append((row["schulnummer"], body))
    logger.info(f"{len(records)} schools have non-empty admission data to upload")

    if args.dry_run:
        logger.info("=== DRY RUN — no changes will be made ===")
        # Sample: show first 5
        for snr, body in records[:5]:
            n_bullets = len(body.get("admission_criteria_bullets", []))
            n_open = len(body.get("open_days", []))
            has_window = "window" if body.get("admission_application_window") else "no_window"
            logger.info(f"  {snr}: {n_bullets} bullets, {n_open} open_days, {has_window}")
        logger.info(f"  ... and {len(records) - 5} more")
        return 0

    # Verify connectivity: check one known school
    logger.info("Verifying Supabase connectivity...")
    test_snr = records[0][0] if records else None
    if test_snr:
        try:
            exists = check_school_exists(test_snr)
            logger.info(f"  Connectivity OK — school {test_snr} {'found' if exists else 'not found'} in Supabase")
        except Exception as e:
            logger.error(f"  Cannot connect to Supabase: {e}")
            return 1

    stats = {"patched": 0, "not_found": 0, "already_has_data": 0, "error": 0, "empty_body": 0}

    for i, (snr, body) in enumerate(records, start=1):
        # Check existence first
        if not check_school_exists(snr):
            stats["not_found"] += 1
            continue

        status = patch_school(snr, body, force=args.force)
        if status == "patched":
            stats["patched"] += 1
        elif status == "already_has_data":
            stats["already_has_data"] += 1
        else:
            stats["error"] += 1
            logger.warning(f"  [{snr}] {status}")

        if i % 100 == 0:
            logger.info(f"  progress {i}/{len(records)} — patched={stats['patched']} skipped={stats['not_found']+stats['already_has_data']} errors={stats['error']}")

        time.sleep(REQUEST_DELAY)

    logger.info("")
    logger.info("=== Upload summary ===")
    logger.info(f"Total processed:     {len(records)}")
    logger.info(f"Patched:             {stats['patched']}")
    logger.info(f"Not found in Supa:   {stats['not_found']}")
    logger.info(f"Already had data:    {stats['already_has_data']}")
    logger.info(f"Errors:              {stats['error']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
