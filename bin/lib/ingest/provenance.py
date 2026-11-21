#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Append-only provenance / collection history.

Side records linked by ``envelope_uuid`` + ``trace_id``; source records are
never mutated. In-memory by default; pass a Redis-like client (with
``rpush``/``lrange``) to persist into the ``ail_objs`` namespace as
``prov:{envelope_uuid}`` lists.
"""
from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

from .envelope import IngestEnvelope, ProvenanceEvent

logger = logging.getLogger("ingest.provenance")


class ProvenanceStore:
    def __init__(self, redis_client=None) -> None:
        self._redis = redis_client
        self._mem: Dict[str, List[dict]] = {}
        self._sightings: List[dict] = []

    def commit(self, env: IngestEnvelope) -> None:
        for ev in env.provenance:
            self.append(env.envelope_uuid, ev)

    def append(self, envelope_uuid: str, event: ProvenanceEvent) -> None:
        payload = event.to_dict()
        payload["envelope_uuid"] = envelope_uuid
        if self._redis is not None:
            try:
                self._redis.rpush(f"prov:{envelope_uuid}", json.dumps(payload))
                return
            except Exception as exc:
                logger.warning("redis provenance failed, memory fallback: %s", exc)
        self._mem.setdefault(envelope_uuid, []).append(payload)

    def history(self, envelope_uuid: str) -> List[dict]:
        if self._redis is not None:
            try:
                rows = self._redis.lrange(f"prov:{envelope_uuid}", 0, -1) or []
                return [json.loads(r) if isinstance(r, str) else r for r in rows]
            except Exception as exc:
                logger.warning("redis history failed: %s", exc)
        return list(self._mem.get(envelope_uuid, []))

    def add_sighting(self, envelope_uuid: str, source: str,
                     sighting_type: int = 0, attribute_uuid: Optional[str] = None) -> dict:
        """Append-only sighting (0=seen, 1=false-positive, 2=expired)."""
        import time
        import uuid as _uuid
        sighting = {
            "uuid": str(_uuid.uuid4()), "envelope_uuid": envelope_uuid,
            "attribute_uuid": attribute_uuid, "source": source,
            "type": sighting_type, "date_sighting": int(time.time()),
        }
        self._sightings.append(sighting)
        return sighting

    def sightings(self) -> List[dict]:
        return list(self._sightings)
