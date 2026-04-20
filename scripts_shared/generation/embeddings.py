#!/usr/bin/env python3
"""
Shared embedding helper for all city pipelines.

Provider order (Gemini-first switch, 2026-04-20):
    1. Gemini `gemini-embedding-001`  (3072-dim, RETRIEVAL_DOCUMENT task)  — PRIMARY
    2. OpenAI `text-embedding-3-large` (3072-dim)                         — fallback

Both providers produce 3072-dim vectors, so swapping provider per-run does
not change the downstream vector-DB schema. They do NOT share a vector
space, however — if you re-embed one city with Gemini you should also
re-embed any city whose rows are compared against it (cross-city
similarity = junk otherwise).

Environment variables:
    GEMINI_API_KEY       — required for Gemini
    OPENAI_API_KEY       — required for OpenAI fallback
    SKIP_EMBEDDINGS=1    — return df with embedding=None (useful for tests)
    EMBEDDING_PROVIDER   — override: 'gemini', 'openai', or 'auto' (default)

Exports:
    generate_embeddings(df, text_col='description')  — in-place, returns df
    compute_similar_schools(df, top_n=3)             — adds most_similar_school_NN
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

# Load .env if not already loaded by the caller
try:
    from dotenv import load_dotenv  # type: ignore
    ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(ENV_FILE)
except ImportError:
    pass

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-embedding-001"   # new-SDK form; no 'models/' prefix
OPENAI_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 3072

DEFAULT_BATCH_GEMINI = 20     # Gemini API caps batched content items
DEFAULT_BATCH_OPENAI = 100    # OpenAI accepts larger batches


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

def _embed_with_gemini(
    descriptions: List[str],
    batch_size: int = DEFAULT_BATCH_GEMINI,
) -> Optional[List[Optional[List[float]]]]:
    """Returns per-text embeddings (None for empty/failed rows).

    Uses the new google-genai SDK (google.genai.Client). The legacy
    google.generativeai SDK is deprecated as of 2026.
    """
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_AI_API_KEY")
    if not api_key:
        return None
    try:
        from google import genai  # type: ignore
        from google.genai import types  # type: ignore
    except ImportError:
        logger.warning("google-genai not installed — cannot use Gemini embeddings")
        return None

    client = genai.Client(api_key=api_key)
    config = types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT")
    logger.info(f"Generating embeddings with Gemini ({GEMINI_MODEL})...")

    embeddings: List[Optional[List[float]]] = [None] * len(descriptions)

    for batch_start in range(0, len(descriptions), batch_size):
        batch_end = min(batch_start + batch_size, len(descriptions))
        batch = descriptions[batch_start:batch_end]

        valid_indices = []
        valid_texts = []
        for local_i, text in enumerate(batch):
            if text and str(text).strip() and str(text).lower() not in ("nan", "none", "null"):
                valid_indices.append(local_i)
                valid_texts.append(str(text))
        if not valid_texts:
            continue

        try:
            result = client.models.embed_content(
                model=GEMINI_MODEL,
                contents=valid_texts,
                config=config,
            )
            for local_i, ce in zip(valid_indices, result.embeddings):
                embeddings[batch_start + local_i] = list(ce.values)
        except Exception as batch_exc:
            logger.warning(
                f"  Gemini batch {batch_start}-{batch_end} failed ({batch_exc}); retrying per-item"
            )
            for local_i, text in zip(valid_indices, valid_texts):
                try:
                    result = client.models.embed_content(
                        model=GEMINI_MODEL,
                        contents=text,
                        config=config,
                    )
                    embeddings[batch_start + local_i] = list(result.embeddings[0].values)
                except Exception as e2:
                    logger.warning(
                        f"  Gemini single embed failed at {batch_start + local_i}: {e2}"
                    )

        logger.info(
            f"  Gemini progress {batch_end}/{len(descriptions)} "
            f"({sum(1 for e in embeddings if e is not None)} filled)"
        )
        time.sleep(0.25)

    return embeddings


# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------

def _embed_with_openai(
    descriptions: List[str],
    batch_size: int = DEFAULT_BATCH_OPENAI,
) -> Optional[List[Optional[List[float]]]]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        import requests  # any import failure here is a real problem
    except ImportError:
        return None

    logger.info(f"Generating embeddings with OpenAI ({OPENAI_MODEL})...")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    embeddings: List[Optional[List[float]]] = [None] * len(descriptions)

    for batch_start in range(0, len(descriptions), batch_size):
        batch_end = min(batch_start + batch_size, len(descriptions))
        batch = descriptions[batch_start:batch_end]

        valid_indices = []
        valid_texts = []
        for local_i, text in enumerate(batch):
            if text and str(text).strip() and str(text).lower() not in ("nan", "none", "null"):
                valid_indices.append(local_i)
                valid_texts.append(str(text))
        if not valid_texts:
            continue

        try:
            resp = requests.post(
                "https://api.openai.com/v1/embeddings",
                headers=headers,
                json={"model": OPENAI_MODEL, "input": valid_texts},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            # OpenAI returns items in input order with an `index` field
            for item in data:
                local_i = valid_indices[item["index"]]
                embeddings[batch_start + local_i] = item["embedding"]
        except Exception as e:
            logger.warning(f"  OpenAI batch {batch_start}-{batch_end} failed: {e}")

        logger.info(f"  OpenAI progress {batch_end}/{len(descriptions)}")
        time.sleep(0.3)

    return embeddings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_embeddings(df: pd.DataFrame, text_col: str = "description") -> pd.DataFrame:
    """Add an 'embedding' column to df. Gemini primary, OpenAI fallback.

    Honors:
      SKIP_EMBEDDINGS=1          — skip and set embedding=None for every row
      EMBEDDING_PROVIDER=gemini  — use only Gemini (no fallback)
      EMBEDDING_PROVIDER=openai  — use only OpenAI
      EMBEDDING_PROVIDER=auto    — default: Gemini -> OpenAI
    """
    df = df.copy()
    if os.environ.get("SKIP_EMBEDDINGS", "").strip() == "1":
        logger.info("SKIP_EMBEDDINGS=1 — writing None into embedding column")
        df["embedding"] = [None] * len(df)
        return df

    if text_col not in df.columns:
        logger.warning(f"text_col={text_col!r} not in dataframe — embedding column set to None")
        df["embedding"] = [None] * len(df)
        return df

    descriptions = df[text_col].astype(str).tolist()
    provider = os.environ.get("EMBEDDING_PROVIDER", "auto").strip().lower()

    result: Optional[List[Optional[List[float]]]] = None
    if provider in ("auto", "gemini"):
        result = _embed_with_gemini(descriptions)
        if result is not None:
            df["embedding"] = result
            _log_summary(df, "Gemini")
            return df
        if provider == "gemini":
            logger.warning("EMBEDDING_PROVIDER=gemini but Gemini unavailable — giving up")
            df["embedding"] = [None] * len(df)
            return df

    if provider in ("auto", "openai"):
        result = _embed_with_openai(descriptions)
        if result is not None:
            df["embedding"] = result
            _log_summary(df, "OpenAI")
            return df
        if provider == "openai":
            logger.warning("EMBEDDING_PROVIDER=openai but OpenAI unavailable — giving up")
            df["embedding"] = [None] * len(df)
            return df

    logger.warning(
        "No embedding provider available. Set GEMINI_API_KEY or OPENAI_API_KEY. "
        "Writing None into embedding column."
    )
    df["embedding"] = [None] * len(df)
    return df


def _log_summary(df: pd.DataFrame, provider: str) -> None:
    filled = df["embedding"].apply(
        lambda x: x is not None and hasattr(x, "__len__") and len(x) > 0
    ).sum()
    dim = "?"
    for v in df["embedding"]:
        if v is not None and hasattr(v, "__len__") and len(v) > 0:
            dim = len(v)
            break
    logger.info(f"Embeddings via {provider}: {filled}/{len(df)} filled (dim={dim})")


def compute_similar_schools(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """Populate most_similar_school_01..NN using cosine similarity on embeddings."""
    df = df.copy()
    for i in range(1, top_n + 1):
        df[f"most_similar_school_{i:02d}"] = None

    if "embedding" not in df.columns:
        return df

    def _is_vec(x):
        return x is not None and hasattr(x, "__len__") and len(x) > 0

    valid_idx = [idx for idx, v in zip(df.index, df["embedding"]) if _is_vec(v)]
    if len(valid_idx) < 2:
        return df

    matrix = np.array([df.at[idx, "embedding"] for idx in valid_idx], dtype=np.float64)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1
    normalized = matrix / norms
    sim = normalized @ normalized.T

    for i, idx in enumerate(valid_idx):
        scores = sim[i].copy()
        scores[i] = -1
        top = np.argsort(scores)[-top_n:][::-1]
        for rank, j in enumerate(top):
            neighbor_idx = valid_idx[j]
            df.at[idx, f"most_similar_school_{rank + 1:02d}"] = df.at[neighbor_idx, "schulnummer"]

    logger.info(f"Computed top-{top_n} similar schools for {len(valid_idx)} rows")
    return df
