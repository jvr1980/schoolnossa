#!/usr/bin/env python3
"""
POST the prepared NL rows into the temporary staging tables.

Lovable Cloud gives us no service-role key and the anon key is read-only by
policy, so bulk writes go through a short-lived staging table that anon may
INSERT into but never read (data_shared/supabase_sql/nl_upload_2026-09/). Rows
land in staging, are checked there, and reach the live tables in one
INSERT ... SELECT run through the Lovable MCP SQL tool — so a half-finished
upload can never leave the live tables partially populated.

~115 MB total (vectors dominate), so it goes straight from this script to
PostgREST in batches instead of through SQL tool calls.

Usage:
    python3 .../nl_upload_to_supabase.py --table schools
    python3 .../nl_upload_to_supabase.py --table primary_schools --batch 80
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts_shared.upload_to_supabase import SUPABASE_ANON_KEY, SUPABASE_URL  # noqa: E402

UPLOAD_DIR = PROJECT_ROOT / "data_shared" / "supabase_upload"
STAGING = {"schools": "nl_stage_schools", "primary_schools": "nl_stage_primary_schools"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    # Staging is insert-only for anon; asking for the rows back would need a
    # SELECT policy we deliberately do not create.
    "Prefer": "return=minimal",
}


def post_batch(url: str, rows: list[dict], attempt_limit: int = 5) -> None:
    for attempt in range(1, attempt_limit + 1):
        resp = requests.post(url, headers=HEADERS, data=json.dumps(rows), timeout=180)
        if resp.status_code in (200, 201, 204):
            return
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < attempt_limit:
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:400]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=sorted(STAGING))
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--start", type=int, default=0,
                        help="Resume from this row index after a failure")
    args = parser.parse_args()

    src = UPLOAD_DIR / f"nl_{args.table}.jsonl"
    rows = [json.loads(line) for line in src.open(encoding="utf-8")]
    url = f"{SUPABASE_URL}/{STAGING[args.table]}"
    logger.info(f"{len(rows)} rows from {src.name} -> {STAGING[args.table]} "
                f"(batch {args.batch}, starting at {args.start})")

    for i in range(args.start, len(rows), args.batch):
        chunk = rows[i:i + args.batch]
        try:
            post_batch(url, chunk)
        except Exception as exc:
            logger.error(f"Failed at rows {i}-{i + len(chunk) - 1}: {exc}")
            logger.error(f"Resume with: --table {args.table} --start {i}")
            sys.exit(1)
        done = i + len(chunk)
        if done % (args.batch * 10) == 0 or done == len(rows):
            logger.info(f"  {done}/{len(rows)}")

    logger.info(f"Posted {len(rows) - args.start} rows. Verify the staging count "
                f"with the Lovable MCP SQL tool before promoting.")


if __name__ == "__main__":
    main()
