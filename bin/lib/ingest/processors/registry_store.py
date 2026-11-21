#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Historical reprocessing registry (design §9).

Kvrocks/Redis hash ``docproc:registry:{sha256}`` with in-memory fallback
(DedupStore protocol pattern). Detects stale docs when a capability
version / engine version / config hash changes or confidence is low.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ingest.processors.registry_store")


@dataclass
class RegistryEntry:
    sha256: str
    capability_ids: List[str] = field(default_factory=list)
    envelope_uuid: str = ""
    object_global_id: str = ""
    processed_at: float = 0.0
    overall_confidence: float = 0.0
    mimetype: str = ""
    text_sha256: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RegistryEntry":
        e = cls(sha256=d.get("sha256", ""))
        for k in ("capability_ids", "envelope_uuid", "object_global_id",
                  "mimetype", "text_sha256"):
            if k in d:
                setattr(e, k, d[k])
        e.processed_at = float(d.get("processed_at", 0) or 0)
        e.overall_confidence = float(d.get("overall_confidence", 0) or 0)
        if isinstance(e.capability_ids, str):
            try:
                e.capability_ids = json.loads(e.capability_ids)
            except Exception:
                e.capability_ids = [e.capability_ids]
        return e


def text_sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


class ProcessedDocRegistry:
    def __init__(self, redis_client=None) -> None:
        self.redis = redis_client
        self._mem: Dict[str, RegistryEntry] = {}
        self._queue: List[Dict[str, Any]] = []

    # -- storage ---------------------------------------------------------
    def _redis_ok(self) -> bool:
        return self.redis is not None

    def record(self, entry: RegistryEntry) -> None:
        entry.processed_at = entry.processed_at or time.time()
        self._mem[entry.sha256] = entry
        if self._redis_ok():
            try:
                self.redis.hset(f"docproc:registry:{entry.sha256}",
                                mapping={k: (json.dumps(v) if isinstance(v, list) else str(v))
                                         for k, v in entry.to_dict().items()})
                return
            except Exception as exc:
                logger.debug("registry redis hset failed, memory only: %s", exc)

    def get(self, sha256: str) -> Optional[RegistryEntry]:
        if sha256 in self._mem:
            return self._mem[sha256]
        if self._redis_ok():
            try:
                raw = self.redis.hgetall(f"docproc:registry:{sha256}")
                if raw:
                    dec = {k.decode() if isinstance(k, bytes) else k:
                           v.decode() if isinstance(v, bytes) else v for k, v in raw.items()}
                    return RegistryEntry.from_dict(dec)
            except Exception:
                pass
        return None

    # -- staleness ---------------------------------------------------------
    def is_current(self, sha256: str, current_capability_ids: List[str],
                   accept_band: bool = True) -> bool:
        e = self.get(sha256)
        if e is None:
            return False
        if sorted(e.capability_ids) != sorted(current_capability_ids):
            return False
        if accept_band and e.overall_confidence < 0.60:
            return False
        return True

    def stale_reason(self, sha256: str, current_capability_ids: List[str]) -> Optional[str]:
        e = self.get(sha256)
        if e is None:
            return "never-processed"
        if sorted(e.capability_ids) != sorted(current_capability_ids):
            return "capability-changed"
        if e.overall_confidence < 0.60:
            return "low-confidence"
        return None

    def should_overwrite(self, sha256: str, new_confidence: float) -> bool:
        e = self.get(sha256)
        return e is None or new_confidence >= e.overall_confidence

    # -- reprocess queue ---------------------------------------------------
    def enqueue_reprocess(self, sha256: str, reason: str,
                          requested_capability: str = "") -> Dict[str, Any]:
        item = {"sha256": sha256, "reason": reason,
                "requested_capability": requested_capability}
        self._queue.append(item)
        if self._redis_ok():
            try:
                self.redis.rpush("docproc:reprocess", json.dumps(item))
            except Exception:
                pass
        return item

    def dequeue_reprocess(self) -> Optional[Dict[str, Any]]:
        if self._redis_ok():
            try:
                raw = self.redis.lpop("docproc:reprocess")
                if raw:
                    return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            except Exception:
                pass
        return self._queue.pop(0) if self._queue else None

    def reprocess_depth(self) -> int:
        if self._redis_ok():
            try:
                return int(self.redis.llen("docproc:reprocess"))
            except Exception:
                pass
        return len(self._queue)
