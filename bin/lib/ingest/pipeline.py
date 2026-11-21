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
        # Layer-2 metadata dedup (previously never called by the gate)
        meta = {k: str(v) for k, v in (env.attributes or {}).items()
                if k in ("dc:title", "dc:creator", "dc:date", "mimetype", "dc:language", "source_url")}
        if len([v for v in meta.values() if v]) >= 3 and self.dedup.check_metadata(meta, env.envelope_uuid):
            env.add_provenance("DEDUP_HIT", processor_id="IngestGate",
                               detail="metadata-fingerprint")
            self.provenance.add_sighting(env.envelope_uuid, source=env.source_id)
            self.provenance.commit(env)
            return GateResult(passed=False, envelope=env, reason="duplicate-metadata", duplicate=True)
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
        self.extractor = extractor or DocumentExtractor(
            tika_url=self.config.tika_url,
            docproc_config=self.config.docproc.to_processor_config())
        # Historical reprocessing registry (design §9)
        try:
            from .processors.registry_store import ProcessedDocRegistry
            from .processors import default_registry
            self.doc_registry = ProcessedDocRegistry()
            self._processor_registry = default_registry(
                self.config.docproc.to_processor_config())
        except Exception:
            self.doc_registry = None  # type: ignore
            self._processor_registry = None  # type: ignore
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

            # Fix unused-extractor bug: pipeline's extractor (with tika_url
            # + docproc config) is shared with the adapter.
            try:
                adapter.extractor = self.extractor
            except Exception:
                pass
            env = self.retry.run(_build, _placeholder_env(adapter), stage="normalize")
            if env is None:
                self.stats.failed += 1
                continue
            self._run_docproc_stage(env)
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

    def _run_docproc_stage(self, env: IngestEnvelope) -> None:
        """Docproc stage: registry skip/record, language, provenance, fork children.

        Runs only when an ``extract_doc`` stage is enabled; never raises
        (best-effort text + warnings, envelope never silently swallowed).
        Stage errors go through RetryExecutor budget / DLQ like normalize.
        """
        stage_names = {s.name for s in self.config.stages if s.enabled}
        if "extract_doc" not in stage_names:
            return
        try:
            import json as _json
            # Registry: skip if same capability set + accept band
            cap_ids: List[str] = []
            if self._processor_registry is not None:
                cap_ids = self._processor_registry.capability_ids(
                    self.config.docproc.to_processor_config())
            if self.doc_registry is not None and cap_ids:
                sha = env.content_ref.sha256
                if self.doc_registry.is_current(sha, cap_ids):
                    self.provenance.add_sighting(env.envelope_uuid, source=env.source_id)
                    env.attributes["docproc.skipped"] = "current"
                    return
                reason = self.doc_registry.stale_reason(sha, cap_ids)
                if reason and reason != "never-processed":
                    self.doc_registry.enqueue_reprocess(sha, reason)
            # Language on extracted text
            if env.content_text:
                try:
                    from .processors.language import detect_language_stub
                    langs = detect_language_stub(env.content_text)
                    if langs:
                        env.attributes.setdefault("dc:language", langs[0][0])
                        env.attributes["docproc.lang_conf"] = str(langs[0][1])
                except Exception as exc:
                    env.attributes["ingest.warnings"] = (
                        str(env.attributes.get("ingest.warnings", "")) + f"; language: {exc}").strip("; ")
            # Provenance + registry record
            detail = _json.dumps({"stage": "extract_doc", "capability_ids": cap_ids,
                                  "confidence": env.attributes.get("docproc.confidence", "")})
            env.add_provenance("CONTENT_MODIFIED", processor_id="docproc",
                               detail=detail)
            if self.doc_registry is not None and cap_ids:
                from .processors.registry_store import RegistryEntry, text_sha256
                try:
                    conf = float(env.attributes.get("docproc.confidence", 0) or 0)
                except Exception:
                    conf = 0.0
                if self.doc_registry.should_overwrite(env.content_ref.sha256, conf):
                    self.doc_registry.record(RegistryEntry(
                        sha256=env.content_ref.sha256, capability_ids=cap_ids,
                        envelope_uuid=env.envelope_uuid,
                        overall_confidence=conf,
                        mimetype=env.content_ref.mimetype_detected,
                        text_sha256=text_sha256(env.content_text or "")))
            # Fork child envelopes per extracted child (child text preserved)
            for child in getattr(env, "extracted_children", []) or []:
                try:
                    child_env = env.fork((child.text or "").encode("utf-8", "ignore"),
                                         mimetype=child.metadata.get("mimetype", "application/octet-stream"),
                                         attributes={**env.attributes,
                                                     "embedded_path": child.embedded_path or ""})
                    child_env.content_text = child.text or None
                    self._emitted.append(child_env)
                except Exception as exc:
                    logger.warning("child fork failed: %s", exc)
        except Exception as exc:
            logger.warning("docproc stage failed (degraded): %s", exc)
            env.attributes["ingest.warnings"] = (
                str(env.attributes.get("ingest.warnings", "")) + f"; docproc: {exc}").strip("; ")

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
