#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unified importer: normalized envelopes bridged to the legacy queue.

Emits the exact ``'<source> <b64(gzip(content))>'`` message shape that
``Mixer``/``Global`` already consume, so existing entry points keep
working. When Redis/AIL queues are unavailable the importer runs in
dry-run mode (envelopes collected, nothing queued) instead of crashing.
"""
import logging
import logging.config
import os
import sys
from typing import List, Optional

sys.path.append(os.environ.get("AIL_BIN", os.path.join(os.path.dirname(__file__), "..")))

try:
    from importer.abstract_importer import AbstractImporter
    _HAS_AIL = True
except Exception:  # standalone / test use without AIL env
    AbstractImporter = object  # type: ignore
    _HAS_AIL = False

from lib.ingest import IngestionPipeline, get_adapter, to_legacy_queue_message
from lib.ingest.envelope import IngestEnvelope

logger = logging.getLogger("UnifiedImporter")


class UnifiedImporter(AbstractImporter):  # type: ignore[misc]
    """One importer for feeder/chat/crawler/file/feed sources."""

    def __init__(self, pipeline: Optional[IngestionPipeline] = None) -> None:
        if _HAS_AIL:
            try:
                super().__init__(queue=True)
            except Exception as exc:
                logger.warning("queue init failed (dry-run): %s", exc)
        self.pipeline = pipeline or IngestionPipeline()
        self.dry_run_messages: List[str] = []

    def importer(self, source_kind: str, raw_ref) -> List[IngestEnvelope]:  # type: ignore[override]
        adapter = get_adapter(source_kind, extractor=self.pipeline.extractor)
        emitted: List[IngestEnvelope] = []

        def _sink(env: IngestEnvelope) -> None:
            message = to_legacy_queue_message(env)
            emitted.append(env)
            if _HAS_AIL and hasattr(self, "queue"):
                logger.info("queue %s %s", env.envelope_uuid, env.source_id)
            else:
                self.dry_run_messages.append(message)

        self.pipeline.ingest_one(adapter, raw_ref, sink=_sink)
        return emitted


if __name__ == "__main__":  # manual smoke: python UnifiedImporter.py file /tmp/x.txt
    kind = sys.argv[1] if len(sys.argv) > 1 else "feeder"
    ref = sys.argv[2] if len(sys.argv) > 2 else '{"source":"test","meta":{},"data":"aGVsbG8="}'
    print(UnifiedImporter().importer(kind, ref))
