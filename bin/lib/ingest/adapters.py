#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Common ingestion interface and source adapters.

Adapters wrap the real existing sources (FeederImporter JSON contract,
chat feeders, Lacus captures, files, live feeds) instead of rewriting
their producers. Each adapter emits the same normalized envelope.
"""
from __future__ import annotations

import base64
import gzip
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional

from .envelope import ContentRef, IngestEnvelope, RawUnit
from .extract import DocumentExtractor

logger = logging.getLogger("ingest.adapters")


class IngestAdapter(ABC):
    source_id: str = "base"

    def __init__(self, extractor: Optional[DocumentExtractor] = None) -> None:
        self.extractor = extractor or DocumentExtractor()

    @abstractmethod
    def fetch(self, raw_ref: Any) -> Iterator[RawUnit]:
        """Yield raw units for ``raw_ref`` (path, dict, URL or queue payload)."""

    def normalize(self, raw: RawUnit) -> IngestEnvelope:
        mime = DocumentExtractor.detect_mimetype(raw.content, raw.meta.get("filename"))
        env = IngestEnvelope(
            source_id=self.source_id,
            content_ref=ContentRef.from_bytes(raw.content, mimetype=mime,
                                              locator=raw.source_ref),
            attributes={"source.kind": self.source_id},
            source_payload=dict(raw.meta),
            source_timestamp=raw.source_timestamp,
        )
        env.add_provenance("RECEIVE", processor_id=self.source_id,
                           detail=raw.source_ref)
        return env

    def health(self) -> Dict[str, Any]:
        return {"source_id": self.source_id, "status": "ok"}


def _decode_feeder_data(data: Any) -> bytes:
    """Decode the legacy feeder ``data`` field (b64-gzip, b64, or plain)."""
    if data is None:
        return b""
    if isinstance(data, bytes):
        return data
    text = str(data)
    # Strict base64 only: validate=True rejects plain text containing
    # spaces/non-alphabet chars, which must pass through untouched instead
    # of being silently mangled by a lenient decode.
    for decoder in (lambda b: gzip.decompress(base64.b64decode(b, validate=True)),
                    lambda b: base64.b64decode(b, validate=True)):
        try:
            return decoder(text)
        except Exception:
            continue
    return text.encode("utf-8", "ignore")


class FeederJsonAdapter(IngestAdapter):
    """Wraps ``bin/importer/FeederImporter.py`` JSON {source,meta,data,timestamp,source_uuid}."""
    source_id = "feeder"

    def fetch(self, raw_ref: Any) -> Iterator[RawUnit]:
        data = raw_ref if isinstance(raw_ref, dict) else json.loads(raw_ref)
        content = _decode_feeder_data(data.get("data"))
        yield RawUnit(source_id=self.source_id, content=content, meta=data,
                      source_ref=data.get("source"),
                      source_timestamp=str(data.get("timestamp")) if data.get("timestamp") else None)

    def normalize(self, raw: RawUnit) -> IngestEnvelope:
        env = super().normalize(raw)
        meta = raw.meta.get("meta") or {}
        env.attributes.update({
            "source.feeder": str(raw.meta.get("source", "default")),
            "ail.object_type": str(meta.get("type", "item")),
        })
        if meta.get("id"):
            env.attributes["source.object_id"] = str(meta["id"])
        env.content_text = raw.content.decode("utf-8", "ignore") or None
        return env


class ChatAdapter(FeederJsonAdapter):
    """Wraps chat feeders (Telegram/Discord/Matrix/Rocket_Chat JSON parsers).

    There is no polling client in this repo; an external collector POSTs
    to ``api/v1/import/json/item`` or pushes ``importer:feeder``.
    """
    source_id = "chat"

    def normalize(self, raw: RawUnit) -> IngestEnvelope:
        env = super().normalize(raw)
        env.attributes["source.kind"] = "chat"
        env.attributes["chat.protocol"] = str(
            (raw.meta.get("meta") or {}).get("protocol", raw.meta.get("source", "chat")))
        return env


class WebsiteCrawlerAdapter(IngestAdapter):
    """Wraps Lacus captures / ``CrawlerCapturesProcessor`` payloads."""
    source_id = "crawler"

    def fetch(self, raw_ref: Any) -> Iterator[RawUnit]:
        capture = raw_ref if isinstance(raw_ref, dict) else json.loads(raw_ref)
        html = capture.get("html") or capture.get("data") or ""
        content = html.encode("utf-8", "ignore") if isinstance(html, str) else bytes(html)
        url = capture.get("last_redirected_url") or capture.get("url") or ""
        yield RawUnit(source_id=self.source_id, content=content,
                      meta={"url": url, "capture": capture}, source_ref=url or None)

    def normalize(self, raw: RawUnit) -> IngestEnvelope:
        env = super().normalize(raw)
        env.attributes["source.kind"] = "crawler"
        if raw.source_ref:
            env.attributes["source_url"] = raw.source_ref
        env.content_text = raw.content.decode("utf-8", "ignore")[:1_000_000] or None
        return env


class FileAdapter(IngestAdapter):
    """Wraps ``FileImporter``; non-text files go through DocImporter extraction
    instead of being rejected (the old ``is_text`` -> ``return None`` gap)."""
    source_id = "file"

    def fetch(self, raw_ref: Any) -> Iterator[RawUnit]:
        path = str(raw_ref)
        with open(path, "rb") as fh:
            content = fh.read()
        yield RawUnit(source_id=self.source_id, content=content,
                      meta={"filename": os.path.basename(path), "path": path},
                      source_ref=path)

    def normalize(self, raw: RawUnit) -> IngestEnvelope:
        env = super().normalize(raw)
        env.attributes["source.kind"] = "file"
        env.attributes["file.name"] = raw.meta.get("filename", "")
        extracted = self.extractor.extract(raw.content, filename=raw.meta.get("filename"))
        env.content_text = extracted.text or env.content_text
        for key, value in extracted.metadata.items():
            if value:
                env.attributes[key] = value
        env.attributes["ingest.extractor"] = extracted.extractor
        if extracted.warnings:
            env.attributes["ingest.warnings"] = "; ".join(extracted.warnings)
        env.children_meta = [  # type: ignore[attr-defined]
            {"embedded_path": c.embedded_path, "mimetype": c.metadata.get("mimetype", "")}
            for c in extracted.children]
        return env


class FeedAdapter(IngestAdapter):
    """Wraps feed payloads (LiveIntelFeedEngine rows / RSS-JSON items)."""
    source_id = "feed"

    def fetch(self, raw_ref: Any) -> Iterator[RawUnit]:
        items = raw_ref if isinstance(raw_ref, list) else [raw_ref]
        for item in items:
            blob = json.dumps(item, default=str).encode("utf-8") if not isinstance(item, bytes) else item
            meta = item if isinstance(item, dict) else {}
            yield RawUnit(source_id=self.source_id, content=blob, meta=meta,
                          source_ref=str(meta.get("url") or meta.get("link") or "") or None)


class ImporterAdapter(FeederJsonAdapter):
    """Generic importer adapter for sync/ZMQ-style relayed payloads."""
    source_id = "importer"


ADAPTERS: Dict[str, type] = {
    "feeder": FeederJsonAdapter, "chat": ChatAdapter, "crawler": WebsiteCrawlerAdapter,
    "website": WebsiteCrawlerAdapter, "file": FileAdapter, "feed": FeedAdapter,
    "importer": ImporterAdapter,
}


def get_adapter(source_kind: str, extractor: Optional[DocumentExtractor] = None) -> IngestAdapter:
    cls = ADAPTERS.get(source_kind)
    if cls is None:
        raise ValueError(f"unknown source kind {source_kind!r}; known: {sorted(ADAPTERS)}")
    return cls(extractor=extractor)  # type: ignore[abstract]


def list_adapters() -> List[str]:
    return sorted(ADAPTERS)
