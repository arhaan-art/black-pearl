#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Text near-duplicate detection: datasketch MinHashLSH wrapper (design §7 layer 4)."""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from .base import normalize_text_for_dedup

logger = logging.getLogger("ingest.processors.dedup_text")

try:
    from datasketch import MinHash, MinHashLSH  # type: ignore
    HAS_DATASKETCH = True
except Exception:
    MinHash = MinHashLSH = None  # type: ignore
    HAS_DATASKETCH = False


def char_shingles(text: str, n: int = 5) -> List[str]:
    t = normalize_text_for_dedup(text)
    if len(t) <= n:
        return [t] if t else []
    return [t[i:i + n] for i in range(len(t) - n + 1)]


def minhash_for_text(text: str, num_perm: int = 128):
    m = MinHash(num_perm=num_perm)
    for s in char_shingles(text):
        m.update(s.encode("utf-8"))
    return m


class MinHashDedup:
    """In-memory LSH by default; Redis-backed when a redis client is injected
    via datasketch's storage layer by the caller (kept optional for tests)."""

    def __init__(self, threshold: float = 0.7, num_perm: int = 128,
                 verify_jaccard: float = 0.8) -> None:
        self.threshold = threshold
        self.num_perm = num_perm
        self.verify_jaccard = verify_jaccard
        self._lsh = MinHashLSH(threshold=threshold, num_perm=num_perm) if HAS_DATASKETCH else None
        self._hashes: dict = {}

    def available(self) -> bool:
        return HAS_DATASKETCH

    def insert(self, key: str, text: str) -> None:
        if not HAS_DATASKETCH or not text.strip():
            return
        m = minhash_for_text(text, self.num_perm)
        self._hashes[key] = m
        if self._lsh is not None:
            try:
                self._lsh.insert(key, m)
            except ValueError:
                pass  # duplicate key

    def query(self, text: str) -> List[Tuple[str, float]]:
        """Candidates verified by true (estimated) Jaccard >= verify threshold."""
        if not HAS_DATASKETCH or not text.strip():
            return []
        m = minhash_for_text(text, self.num_perm)
        keys = list(self._lsh.query(m)) if self._lsh is not None else list(self._hashes)
        out = []
        for k in keys:
            other = self._hashes.get(k)
            if other is None:
                continue
            j = float(m.jaccard(other))
            if j >= self.verify_jaccard:
                out.append((k, round(j, 4)))
        return sorted(out, key=lambda kv: -kv[1])

    def is_near_duplicate(self, text: str, exclude_key: Optional[str] = None) -> bool:
        return any(k != exclude_key for k, _ in self.query(text))
