#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Content-fingerprint and metadata deduplication.

Three layers: exact sha256 (idempotent-consumer key), TLSH fuzzy when the
``tlsh`` package is present, and a metadata fingerprint for records
without stable bytes. The store is in-memory by default and can be backed
by Redis/Kvrocks through the ``store`` protocol (``exists``/``put``).
"""
from __future__ import annotations

import hashlib
import logging
from typing import Dict, Optional, Protocol

logger = logging.getLogger("ingest.dedup")

try:
    import tlsh as _tlsh  # type: ignore
    HAS_TLSH = True
except Exception:
    _tlsh = None
    HAS_TLSH = False


class DedupStore(Protocol):
    def exists(self, key: str) -> bool: ...
    def put(self, key: str, value: str) -> None: ...
    def get(self, key: str) -> Optional[str]: ...


class InMemoryDedupStore:
    def __init__(self) -> None:
        self._data: Dict[str, str] = {}

    def exists(self, key: str) -> bool:
        return key in self._data

    def put(self, key: str, value: str) -> None:
        self._data[key] = value

    def get(self, key: str) -> Optional[str]:
        return self._data.get(key)


def metadata_fingerprint(meta: Dict[str, str]) -> str:
    parts = []
    for key in ("dc:title", "dc:creator", "dc:date", "mimetype", "dc:language", "source_url"):
        parts.append(str(meta.get(key, "")).strip().lower())
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def tlsh_hash(content: bytes) -> Optional[str]:
    if not HAS_TLSH or not content:
        return None
    try:
        value = _tlsh.hash(content)
        return value or None
    except Exception as exc:  # tlsh returns 'TNULL' on tiny inputs
        logger.debug("tlsh failed: %s", exc)
        return None


class DedupChecker:
    """Exact + metadata dedup with optional fuzzy TLSH linking."""

    def __init__(self, store: Optional[DedupStore] = None, tlsh_threshold: int = 52) -> None:
        self.store = store or InMemoryDedupStore()
        self.tlsh_threshold = tlsh_threshold

    def check_exact(self, sha256: str, envelope_uuid: str) -> bool:
        """Return True when this content was already seen (emit sighting only)."""
        key = f"ingest:sha256:{sha256}"
        if self.store.exists(key):
            logger.info("dedup hit sha256=%s uuid=%s", sha256, envelope_uuid)
            return True
        self.store.put(key, envelope_uuid)
        return False

    def check_metadata(self, meta: Dict[str, str], envelope_uuid: str) -> bool:
        fp = metadata_fingerprint(meta)
        key = f"ingest:meta:{fp}"
        if self.store.exists(key):
            return True
        self.store.put(key, envelope_uuid)
        return False

    def fuzzy_similarity(self, content_a: bytes, content_b: bytes) -> Optional[int]:
        ha, hb = tlsh_hash(content_a), tlsh_hash(content_b)
        if not ha or not hb or ha == "TNULL" or hb == "TNULL":
            return None
        try:
            diff = _tlsh.diffxlen(ha, hb)
            return max(0, 100 - min(diff, 100))
        except Exception:
            return None

    def is_fuzzy_duplicate(self, content_a: bytes, content_b: bytes) -> bool:
        sim = self.fuzzy_similarity(content_a, content_b)
        return sim is not None and sim >= self.tlsh_threshold
