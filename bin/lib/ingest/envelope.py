#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Normalized ingestion envelope (envelope-not-payload).

Every ingest unit carries an immutable content reference, typed attributes,
an immutable source ``resource`` and a UUID identity. Local DB ids never
leave the process; ``envelope_uuid`` is the idempotent upsert key.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SCHEMA_URL = "blackpearl.ingest/v1"
SCHEMA_VERSION = "1.0.0"

PROVENANCE_EVENTS = (
    "CREATE", "RECEIVE", "FETCH", "SEND", "DROP", "EXPIRE", "FORK", "JOIN",
    "CLONE", "CONTENT_MODIFIED", "ATTRIBUTES_MODIFIED", "ROUTE", "REPLAY",
    "VALIDATION_FAILED", "DEDUP_HIT",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_traceparent() -> str:
    return f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-01"


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@dataclass
class ContentRef:
    sha256: str
    size_bytes: int
    mimetype_detected: str = "application/octet-stream"
    storage: str = "inline"  # inline | pastes | redis-cache | temp-file | external
    locator: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "mimetype_detected": self.mimetype_detected,
            "storage": self.storage,
            "locator": self.locator,
        }

    @classmethod
    def from_bytes(cls, content: bytes, mimetype: str = "application/octet-stream",
                   storage: str = "inline", locator: Optional[str] = None) -> "ContentRef":
        return cls(sha256=sha256_hex(content), size_bytes=len(content),
                   mimetype_detected=mimetype, storage=storage, locator=locator)


@dataclass
class ProvenanceEvent:
    event_type: str
    processor_id: str
    timestamp: str = field(default_factory=utc_now_iso)
    parent_envelope_uuid: Optional[str] = None
    child_envelope_uuid: Optional[str] = None
    content_sha256_before: Optional[str] = None
    content_sha256_after: Optional[str] = None
    transit_uri: Optional[str] = None
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        if self.event_type not in PROVENANCE_EVENTS:
            raise ValueError(f"unknown provenance event_type: {self.event_type}")

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class RawUnit:
    """One raw unit handed from a source to an adapter."""
    source_id: str
    content: bytes
    meta: Dict[str, Any] = field(default_factory=dict)
    source_ref: Optional[str] = None
    source_timestamp: Optional[str] = None


@dataclass
class IngestEnvelope:
    source_id: str
    content_ref: ContentRef
    attributes: Dict[str, Any] = field(default_factory=dict)
    resource: Dict[str, Any] = field(default_factory=dict)
    envelope_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    schema_url: str = SCHEMA_URL
    schema_version: str = SCHEMA_VERSION
    traceparent: str = field(default_factory=new_traceparent)
    trace_id: Optional[str] = None
    parent_envelope_uuid: Optional[str] = None
    source_payload: Dict[str, Any] = field(default_factory=dict)
    source_timestamp: Optional[str] = None
    ingested_at: str = field(default_factory=utc_now_iso)
    content_text: Optional[str] = None
    provenance: List[ProvenanceEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.trace_id:
            parts = (self.traceparent or "").split("-")
            self.trace_id = parts[1] if len(parts) >= 2 and len(parts[1]) == 32 else uuid.uuid4().hex
        if not self.resource:
            self.resource = {
                "service.name": f"blackpearl.ingest.{self.source_id}",
                "source.id": self.source_id,
                "telemetry.sdk.name": "blackpearl-ingest",
                "telemetry.sdk.language": "python",
                "telemetry.sdk.version": SCHEMA_VERSION,
            }

    def fork(self, content: bytes, mimetype: str = "application/octet-stream",
             attributes: Optional[Dict[str, Any]] = None) -> "IngestEnvelope":
        """Create a child envelope; the parent is never mutated."""
        child = IngestEnvelope(
            source_id=self.source_id,
            content_ref=ContentRef.from_bytes(content, mimetype=mimetype),
            attributes=dict(attributes if attributes is not None else self.attributes),
            resource=dict(self.resource),
            parent_envelope_uuid=self.envelope_uuid,
            source_payload=self.source_payload,
            source_timestamp=self.source_timestamp,
            traceparent=self.traceparent,
            trace_id=self.trace_id,
        )
        child.add_provenance("FORK", processor_id=self.source_id,
                             parent=self.envelope_uuid, child=child.envelope_uuid)
        return child

    def add_provenance(self, event_type: str, processor_id: str,
                       parent: Optional[str] = None, child: Optional[str] = None,
                       detail: Optional[str] = None) -> ProvenanceEvent:
        ev = ProvenanceEvent(event_type=event_type, processor_id=processor_id,
                             parent_envelope_uuid=parent, child_envelope_uuid=child,
                             content_sha256_after=self.content_ref.sha256, detail=detail)
        self.provenance.append(ev)
        return ev

    def to_dict(self) -> Dict[str, Any]:
        return {
            "envelope_uuid": self.envelope_uuid,
            "schema_url": self.schema_url,
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "content_ref": self.content_ref.to_dict(),
            "attributes": self.attributes,
            "resource": self.resource,
            "traceparent": self.traceparent,
            "trace_id": self.trace_id,
            "parent_envelope_uuid": self.parent_envelope_uuid,
            "source_payload": self.source_payload,
            "source_timestamp": self.source_timestamp,
            "ingested_at": self.ingested_at,
            "content_text": self.content_text,
            "provenance": [p.to_dict() for p in self.provenance],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IngestEnvelope":
        cr = data.get("content_ref") or {}
        env = cls(
            source_id=data.get("source_id", "unknown"),
            content_ref=ContentRef(
                sha256=cr.get("sha256", ""), size_bytes=int(cr.get("size_bytes", 0)),
                mimetype_detected=cr.get("mimetype_detected", "application/octet-stream"),
                storage=cr.get("storage", "inline"), locator=cr.get("locator")),
            attributes=data.get("attributes") or {},
            resource=data.get("resource") or {},
            source_payload=data.get("source_payload") or {},
            source_timestamp=data.get("source_timestamp"),
        )
        env.envelope_uuid = data.get("envelope_uuid", env.envelope_uuid)
        env.schema_url = data.get("schema_url", SCHEMA_URL)
        env.schema_version = data.get("schema_version", SCHEMA_VERSION)
        env.traceparent = data.get("traceparent", env.traceparent)
        env.trace_id = data.get("trace_id", env.trace_id)
        env.parent_envelope_uuid = data.get("parent_envelope_uuid")
        env.ingested_at = data.get("ingested_at", env.ingested_at)
        env.content_text = data.get("content_text")
        for p in data.get("provenance") or []:
            try:
                env.provenance.append(ProvenanceEvent(**{
                    k: p[k] for k in ("event_type", "processor_id", "timestamp",
                                      "parent_envelope_uuid", "child_envelope_uuid",
                                      "content_sha256_before", "content_sha256_after",
                                      "transit_uri", "detail") if k in p}))
            except (TypeError, ValueError):
                continue
        return env
