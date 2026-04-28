#!/usr/bin/env python3
"""
One-off embedding regeneration across all cities.

Purpose:
  Replace every `embedding` vector in every city's
  `*_master_table_final_with_embeddings.parquet` with a fresh
  Gemini `gemini-embedding-001` vector at 768 dimensions, matching
  Lovable's Supabase `vector(768)` column.

Why 768:
  Lovable's Supabase column is `vector(768)` (confirmed by a PATCH
  attempt with 3072 dims that returned
  "expected 768 dimensions, not 3072"). `gemini-embedding-001`
  supports Matryoshka output dims — 128/256/512/768/1536/3072 —
  so we ask for 768 directly.

Why one coherent run:
  If we only re-embed one city, cross-city semantic similarity
  silently degrades (different cities live in different vector
  spaces). This script regenerates every city in one pass with
  identical settings, keeping the similarity math honest.

Behavior:
  For each (city_dir, parquet_file) pair below:
    1. Load the parquet.
    2. Build embedding text per row (prefers existing `description`,
       falls back to `description_de`, then a synthesized blurb).
    3. Batch-call Gemini embed_content with output_dimensionality=768.
    4. Overwrite the `embedding` column in-place.
    5. Recompute `most_similar_school_NN` columns via cosine
       similarity (within that city only).
    6. Save parquet back. CSV siblings that dropped the embedding
       column are left alone.

Env:
  GEMINI_API_KEY  — required.
  SKIP_EMBEDDINGS=1 — aborts with no changes.

Safe to re-run — it always overwrites cleanly.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd

# Load .env
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("regen_embeddings")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GEMINI_MODEL = "gemini-embedding-001"
OUTPUT_DIM = 768
BATCH_SIZE = 20

# (city_key, parquet_path_relative_to_project_root)
TARGETS: List[tuple[str, str]] = [
    ("berlin",              "data_berlin/final/school_master_table_final_with_embeddings.parquet"),
    ("berlin_primary",      "data_berlin_primary/final/grundschule_master_table_final_with_embeddings.parquet"),
    ("bremen",              "data_bremen/final/bremen_school_master_table_final_with_embeddings.parquet"),
    ("dresden",             "data_dresden/final/dresden_school_master_table_final.parquet"),
    ("frankfurt_primary",   "data_frankfurt/final/frankfurt_primary_school_master_table_final_with_embeddings.parquet"),
    ("frankfurt_secondary", "data_frankfurt/final/frankfurt_secondary_school_master_table_final_with_embeddings.parquet"),
    ("hamburg",             "data_hamburg/final/hamburg_school_master_table_final_with_embeddings.parquet"),
    ("hamburg_primary",     "data_hamburg_primary/final/hamburg_primary_school_master_table_final_with_embeddings.parquet"),
    ("leipzig",             "data_leipzig/final/leipzig_school_master_table_final_with_embeddings.parquet"),
    ("munich_primary",      "data_munich/final/munich_primary_school_master_table_final_with_embeddings.parquet"),
    ("munich_secondary",    "data_munich/final/munich_secondary_school_master_table_final_with_embeddings.parquet"),
    ("nrw_primary",         "data_nrw/final/nrw_primary_school_master_table_final_with_embeddings.parquet"),
    ("nrw_secondary",       "data_nrw/final/nrw_secondary_school_master_table_final_with_embeddings.parquet"),
    ("stuttgart_primary",   "data_stuttgart/final/stuttgart_primary_school_master_table_final_with_embeddings.parquet"),
    ("stuttgart_secondary", "data_stuttgart/final/stuttgart_secondary_school_master_table_final_with_embeddings.parquet"),
]


# ---------------------------------------------------------------------------
# Embedding text construction
# ---------------------------------------------------------------------------

def _text_for_row(row: pd.Series) -> str:
    """Pick the best available descriptive text for embedding."""
    for col in ("description", "description_de", "description_en", "embedding_text"):
        if col in row.index:
            val = row[col]
            if pd.notna(val) and str(val).strip() and str(val).lower() not in ("nan", "none", "null"):
                return str(val).strip()
    # Final fallback — cheap synthesized blurb
    name = row.get("schulname") or ""
    stype = row.get("school_type") or row.get("schulart") or ""
    city = row.get("stadt") or ""
    return f"{name} — {stype} in {city}".strip(" —")


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

def _gemini_client():
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_AI_API_KEY")
    if not api_key:
        raise SystemExit("GEMINI_API_KEY not set")
    from google import genai  # type: ignore
    return genai.Client(api_key=api_key)


def _embed_batch(client, texts: Sequence[str]) -> List[Optional[List[float]]]:
    from google.genai import types  # type: ignore

    config = types.EmbedContentConfig(
        task_type="RETRIEVAL_DOCUMENT",
        output_dimensionality=OUTPUT_DIM,
    )
    # Separate valid from empty to avoid API errors on empty strings
    valid_indices: List[int] = []
    valid_texts: List[str] = []
    for i, t in enumerate(texts):
        if t and t.strip():
            valid_indices.append(i)
            valid_texts.append(t)

    out: List[Optional[List[float]]] = [None] * len(texts)
    if not valid_texts:
        return out

    try:
        result = client.models.embed_content(
            model=GEMINI_MODEL,
            contents=valid_texts,
            config=config,
        )
        for i, ce in zip(valid_indices, result.embeddings):
            out[i] = list(ce.values)
        return out
    except Exception as batch_exc:
        logger.warning(f"    batch embed failed ({batch_exc}); retrying per-item")
        for i, t in zip(valid_indices, valid_texts):
            try:
                r = client.models.embed_content(
                    model=GEMINI_MODEL,
                    contents=t,
                    config=config,
                )
                out[i] = list(r.embeddings[0].values)
            except Exception as e2:
                logger.warning(f"    single embed failed at {i}: {e2}")
        return out


def regenerate_embeddings(df: pd.DataFrame, client) -> pd.DataFrame:
    df = df.copy()
    texts = [_text_for_row(r) for _, r in df.iterrows()]
    embeddings: List[Optional[List[float]]] = [None] * len(df)

    for start in range(0, len(texts), BATCH_SIZE):
        end = min(start + BATCH_SIZE, len(texts))
        batch_out = _embed_batch(client, texts[start:end])
        for i, v in enumerate(batch_out):
            embeddings[start + i] = v
        logger.info(
            f"    {end}/{len(texts)} — "
            f"{sum(1 for e in embeddings if e is not None)} filled"
        )
        time.sleep(0.2)  # gentle pacing

    df["embedding"] = embeddings
    return df


def compute_similar(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    df = df.copy()
    for i in range(1, top_n + 1):
        col = f"most_similar_school_{i:02d}"
        df[col] = None

    def _is_vec(x):
        return x is not None and hasattr(x, "__len__") and len(x) > 0

    valid_idx = [idx for idx, v in zip(df.index, df["embedding"]) if _is_vec(v)]
    if len(valid_idx) < 2:
        return df

    matrix = np.asarray([df.at[idx, "embedding"] for idx in valid_idx], dtype=np.float64)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1
    normalized = matrix / norms
    sim = normalized @ normalized.T

    if "schulnummer" not in df.columns:
        logger.warning("  no schulnummer column — skipping similar-school write-back")
        return df

    for i, idx in enumerate(valid_idx):
        scores = sim[i].copy()
        scores[i] = -1
        top = np.argsort(scores)[-top_n:][::-1]
        for rank, j in enumerate(top):
            neighbor_idx = valid_idx[j]
            df.at[idx, f"most_similar_school_{rank + 1:02d}"] = df.at[neighbor_idx, "schulnummer"]
    return df


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def process_target(client, city_key: str, parquet_path: Path) -> dict:
    if not parquet_path.exists():
        return {"city": city_key, "status": "missing", "filled": 0, "total": 0}
    df = pd.read_parquet(parquet_path)
    logger.info(f"[{city_key}] Loaded {len(df)} rows from {parquet_path.name}")
    df = regenerate_embeddings(df, client)
    filled = sum(
        1 for v in df["embedding"]
        if v is not None and hasattr(v, "__len__") and len(v) > 0
    )
    # Verify dimensionality
    dim = None
    for v in df["embedding"]:
        if v is not None and hasattr(v, "__len__") and len(v) > 0:
            dim = len(v)
            break
    if dim is not None and dim != OUTPUT_DIM:
        logger.error(
            f"[{city_key}] unexpected dim={dim} (wanted {OUTPUT_DIM}) — aborting write"
        )
        return {"city": city_key, "status": "dim_mismatch", "filled": filled, "total": len(df)}

    df = compute_similar(df, top_n=3)
    df.to_parquet(parquet_path, index=False)
    logger.info(f"[{city_key}] Saved {parquet_path.name} — {filled}/{len(df)} filled (dim={dim})")
    return {"city": city_key, "status": "ok", "filled": filled, "total": len(df)}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", default="all", help="'all' or comma-separated city keys")
    args = parser.parse_args(argv)

    if os.environ.get("SKIP_EMBEDDINGS", "").strip() == "1":
        logger.info("SKIP_EMBEDDINGS=1 — aborting")
        return 0

    if args.cities == "all":
        targets = TARGETS
    else:
        wanted = {k.strip() for k in args.cities.split(",") if k.strip()}
        targets = [t for t in TARGETS if t[0] in wanted]
        if not targets:
            logger.error(f"No matching cities for: {args.cities}")
            return 1

    logger.info(f"Regenerating embeddings for {len(targets)} targets at dim={OUTPUT_DIM}")
    client = _gemini_client()

    summaries = []
    for city_key, rel_path in targets:
        summaries.append(process_target(client, city_key, PROJECT_ROOT / rel_path))

    print("\n" + "=" * 60)
    print("REGENERATION COMPLETE")
    print("=" * 60)
    grand_total = 0
    grand_filled = 0
    for s in summaries:
        print(f"  {s['city']:<22}  {s['status']:<14}  {s['filled']}/{s['total']}")
        grand_total += s.get("total", 0)
        grand_filled += s.get("filled", 0)
    print(f"\n  TOTAL: {grand_filled}/{grand_total} filled at {OUTPUT_DIM} dims")
    return 0


if __name__ == "__main__":
    sys.exit(main())
