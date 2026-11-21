#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Configurable ingestion pipeline and downstream gate.

Flow: adapter -> normalize -> dedup -> validate (IngestGate) -> emit.
Malformed records are routed to the DLQ and never emitted downstream,
which is the ``Importers -> Mixer`` contract in ``configs/modules.cfg``.
"""
from __future__ import annotations

import base64
import gzip
import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from .adapters import IngestAdapter
from .config import PipelineConfig, default_pipeline
from .dedup import DedupChecker
from .envelope import IngestEnvelope
from .extract import DocumentExtractor
from .provenance import ProvenanceStore
from .retry import DeadLetterQueue, RetryExecutor, RetryPolicy
from .validate import validate_envelope

logger = logging.getLogger("ingest.pipeline")


@dataclass
class GateResult:
    passed: bool
    envelope: IngestEnvelope
    reason: str = ""
    duplicate: bool = False


class IngestGate:
    """Strict validation gate inserted before Mixer (see IngestGate module)."""

    def __init__(self, dedup: Optional[DedupChecker] = None,
                 provenance: Optional[ProvenanceStore] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.dedup = dedup or DedupChecker()
        self.provenance = provenance or ProvenanceStore()
        self.dlq = dlq or DeadLetterQueue()

    def check(self, env: IngestEnvelope) -> GateResult:
        result = validate_envelope(env)
        if not result.ok:
            env.add_provenance("VALIDATION_FAILED", processor_id="IngestGate",
                               detail="; ".join(result.errors))
            self.provenance.commit(env)
            self.dlq.push(env, stage="validate", error="; ".join(result.errors),
                          error_class="non-retryable", attempts=1)
            return GateResult(passed=False, envelope=env,
                              reason="; ".join(result.errors))
        if self.dedup.check_exact(env.content_ref.sha256, env.envelope_uuid):
            env.add_provenance("DEDUP_HIT", processor_id="IngestGate",
                               detail=env.content_ref.sha256)
            self.provenance.add_sighting(env.envelope_uuid, source=env.source_id)
            self.provenance.commit(env)
            return GateResult(passed=False, envelope=env, reason="duplicate", duplicate=True)
        env.add_provenance("ROUTE", processor_id="IngestGate", detail="to Mixer")
        self.provenance.commit(env)
        return GateResult(passed=True, envelope=env)


@dataclass
class PipelineStats:
    received: int = 0
    emitted: int = 0
    duplicates: int = 0
    failed: int = 0
    dlq_depth: int = 0


class IngestionPipeline:
    """Adapter-agnostic pipeline with retry + DLQ around each record."""

    def __init__(self, config: Optional[PipelineConfig] = None,
                 extractor: Optional[DocumentExtractor] = None) -> None:
        self.config = config or default_pipeline()
        self.extractor = extractor or DocumentExtractor(tika_url=self.config.tika_url)
        self.dedup = DedupChecker()
        self.provenance = ProvenanceStore()
        self.dlq = DeadLetterQueue()
        self.gate = IngestGate(dedup=self.dedup, provenance=self.provenance, dlq=self.dlq)
        self.retry = RetryExecutor(
            policy=RetryPolicy(max_retries=self._max_retries()), dlq=self.dlq)
        self.stats = PipelineStats()
        self._emitted: List[IngestEnvelope] = []

    def _max_retries(self) -> int:
        for stage in self.config.stages:
            if stage.name == "validate":
                return stage.max_retries
        return 3

    def ingest_one(self, adapter: IngestAdapter, raw_ref,
                   sink: Optional[Callable[[IngestEnvelope], None]] = None) -> List[GateResult]:
        results: List[GateResult] = []
        for raw in adapter.fetch(raw_ref):
            self.stats.received += 1
            holder: dict = {}

            def _build(raw=raw, adapter=adapter, holder=holder):
                env = adapter.normalize(raw)
                holder["env"] = env
                return env

            env = self.retry.run(_build, _placeholder_env(adapter), stage="normalize")
            if env is None:
                self.stats.failed += 1
                continue
            gate = self.gate.check(env)
            results.append(gate)
            if gate.duplicate:
                self.stats.duplicates += 1
            elif gate.passed:
                self.stats.emitted += 1
                self._emitted.append(env)
                if sink is not None:
                    sink(env)
            else:
                self.stats.failed += 1
        self.stats.dlq_depth = self.dlq.depth()
        return results

    def emitted(self) -> List[IngestEnvelope]:
        return list(self._emitted)

    def replay_dlq(self, sink: Optional[Callable[[IngestEnvelope], None]] = None) -> int:
        """Re-inject DLQ entries through the gate (idempotent by uuid+sha256)."""
        replayed = 0
        for entry in self.dlq.list_entries():
            env = IngestEnvelope.from_dict(entry.envelope)
            env.add_provenance("REPLAY", processor_id="ingest_replay",
                               detail=entry.failure_stage)
            self.dlq.pop(env.envelope_uuid)
            gate = self.gate.check(env)
            if gate.passed:
                replayed += 1
                self._emitted.append(env)
                if sink is not None:
                    sink(env)
        self.stats.dlq_depth = self.dlq.depth()
        return replayed


def _placeholder_env(adapter: IngestAdapter) -> IngestEnvelope:
    from .envelope import ContentRef
    return IngestEnvelope(source_id=adapter.source_id,
                          content_ref=ContentRef(sha256="0" * 64, size_bytes=0))


def to_legacy_queue_message(env: IngestEnvelope) -> str:
    """Bridge to the existing queue format: ``'<source> <b64(gzip(content))>'``."""
    payload = (env.content_text or "").encode("utf-8")
    return f"{env.source_id} {base64.b64encode(gzip.compress(payload)).decode()}"
