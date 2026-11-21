#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unified ingestion engine for Black Pearl (AIL fork).

Public API: adapters normalize every source into ``IngestEnvelope``;
``IngestGate`` blocks malformed/duplicate records before Mixer; failures
land in the dead-letter queue with replay (``tools/ingest_replay.py``).
"""
from .adapters import (  # noqa: F401
    ChatAdapter, FeedAdapter, FeederJsonAdapter, FileAdapter, ImporterAdapter,
    IngestAdapter, WebsiteCrawlerAdapter, get_adapter, list_adapters,
)
from .config import (  # noqa: F401
    DocProcConfig, PipelineConfig, StageConfig, default_pipeline, load_pipeline_config,
)
from .dedup import DedupChecker, InMemoryDedupStore, metadata_fingerprint  # noqa: F401
from .envelope import ContentRef, IngestEnvelope, ProvenanceEvent, RawUnit, SCHEMA_URL  # noqa: F401
from .extract import DocumentExtractor, PythonFallbackExtractor, TikaExtractor  # noqa: F401
from .pipeline import GateResult, IngestGate, IngestionPipeline, to_legacy_queue_message  # noqa: F401
from .provenance import ProvenanceStore  # noqa: F401
from .retry import DeadLetterQueue, DLQEntry, RetryExecutor, RetryPolicy  # noqa: F401
from .validate import ValidationResult, sanitize_text, validate_envelope  # noqa: F401

__all__ = [
    "ChatAdapter", "FeedAdapter", "FeederJsonAdapter", "FileAdapter",
    "ImporterAdapter", "IngestAdapter", "WebsiteCrawlerAdapter",
    "get_adapter", "list_adapters", "PipelineConfig", "StageConfig",
    "DocProcConfig", "default_pipeline", "load_pipeline_config", "DedupChecker",
    "InMemoryDedupStore", "metadata_fingerprint", "ContentRef",
    "IngestEnvelope", "ProvenanceEvent", "RawUnit", "SCHEMA_URL",
    "DocumentExtractor", "PythonFallbackExtractor", "TikaExtractor",
    "GateResult", "IngestGate", "IngestionPipeline",
    "to_legacy_queue_message", "ProvenanceStore", "DeadLetterQueue",
    "DLQEntry", "RetryExecutor", "RetryPolicy", "ValidationResult",
    "sanitize_text", "validate_envelope",
]
