#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Processor registry + pipeline orchestration (design §2, §8, §10).

``DocumentExtractor`` (in ``extract.py``) delegates here; every processor
is optional-import gated so a host without Java/Tesseract/Paddle still
ingests via the stdlib/PyMuPDF fallback.
"""
from __future__ import annotations

import io
import json
import logging
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..validate import sanitize_text
from .base import (BaseProcessor, Capability, CodeResult, StageConfidence,
                   StageContext, StageError, StageResult)
from .archive import ArchiveProcessor
from .image import CodeProcessor, ImageProcessor, OcrProcessor
from .metadata import MetadataNormalizer
from .office import OfficeProcessor
from .pdf import PdfEmbeddedProcessor, PdfNativeProcessor, PdfOcrProcessor

logger = logging.getLogger("ingest.processors")

try:
    from html.parser import HTMLParser
    HAS_HTML = True
except Exception:
    HAS_HTML = False


class TextProcessor(BaseProcessor):
    name = "text"
    priority = 10
    prefixes = ("text/",)
    mimetypes = frozenset({"text/plain", "text/html", "application/json",
                           "message/rfc822", "application/xml", "text/xml"})

    def supports(self, mimetype, filename=None) -> bool:
        if super().supports(mimetype, filename):
            return True
        return (filename or "").lower().endswith(
            (".txt", ".md", ".log", ".json", ".csv", ".xml", ".html", ".htm", ".eml"))

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            if ctx.mimetype == "text/html" or (ctx.filename or "").endswith((".html", ".htm")):
                from ..extract import _TextHTMLParser
                p = _TextHTMLParser()
                p.feed(ctx.content.decode("utf-8", "ignore"))
                return StageResult(text=sanitize_text("\n".join(p.parts)),
                                   metadata={"mimetype": ctx.mimetype})
            return StageResult(text=sanitize_text(ctx.content.decode("utf-8", "ignore")),
                               metadata={"mimetype": ctx.mimetype})
        return self._timed(ctx, _run)


class EmlProcessor(BaseProcessor):
    name = "eml"
    priority = 10
    mimetypes = frozenset({"message/rfc822"})

    def supports(self, mimetype, filename=None) -> bool:
        return super().supports(mimetype, filename) or (filename or "").lower().endswith(".eml")

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            import email
            from ..extract import ExtractedDoc
            msg = email.message_from_bytes(ctx.content)
            meta = {"dc:title": str(msg.get("Subject", "")), "mimetype": "message/rfc822",
                    "dc:creator": str(msg.get("From", "")), "dc:date": str(msg.get("Date", ""))}
            children, texts = [], []
            if msg.is_multipart():
                for part in msg.walk():
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    fname = part.get_filename()
                    if fname:
                        child = ExtractedDoc(text=sanitize_text(payload.decode("utf-8", "ignore")),
                                             metadata={"mimetype": part.get_content_type()},
                                             embedded_path=fname)
                        child._bytes = payload  # type: ignore[attr-defined]
                        children.append(child)
                        texts.append(child.text)
                    elif part.get_content_type() == "text/plain":
                        texts.append(payload.decode("utf-8", "ignore"))
            else:
                texts.append((msg.get_payload(decode=True) or b"").decode("utf-8", "ignore"))
            return StageResult(text=sanitize_text("\n".join(texts)), metadata=meta, children=children)
        return self._timed(ctx, _run)


class ProcessorRegistry:
    def __init__(self) -> None:
        self._processors: List[Any] = []

    def register(self, processor) -> None:
        self._processors.append(processor)
        self._processors.sort(key=lambda p: p.priority)

    def all(self) -> List[Any]:
        return list(self._processors)

    def capability_ids(self, config: Optional[Dict[str, Any]] = None) -> List[str]:
        ids = []
        for p in self._processors:
            if hasattr(p, "capability"):
                ids.append(p.capability(config).capability_id)
        return ids

    def resolve(self, mimetype: str, filename: Optional[str] = None) -> List[Any]:
        chain = [p for p in self._processors
                 if p.supports(mimetype, filename) and p.available()]
        return sorted(chain, key=lambda p: p.priority)

    def resolve_all_available(self) -> List[Any]:
        return [p for p in self._processors if p.available()]


def default_registry(config: Optional[Dict[str, Any]] = None) -> ProcessorRegistry:
    cfg = config or {}
    reg = ProcessorRegistry()
    reg.register(PdfNativeProcessor())
    reg.register(PdfOcrProcessor())
    reg.register(PdfEmbeddedProcessor())
    reg.register(ImageProcessor())
    reg.register(OcrProcessor())
    reg.register(CodeProcessor())
    reg.register(ArchiveProcessor(max_embedded=int(cfg.get("max_embedded", 50))))
    reg.register(OfficeProcessor())
    reg.register(EmlProcessor())
    reg.register(TextProcessor())
    return reg


@dataclass
class DocPipelineResult:
    text: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    children: List[Any] = field(default_factory=list)
    codes: List[CodeResult] = field(default_factory=list)
    confidence: StageConfidence = field(default_factory=StageConfidence)
    warnings: List[str] = field(default_factory=list)
    errors: List[StageError] = field(default_factory=list)
    capability_ids: List[str] = field(default_factory=list)
    extractor: str = "docproc"
    stage_results: List[StageResult] = field(default_factory=list)


def run_doc_pipeline(content: bytes, mimetype: str, filename: Optional[str] = None,
                     config: Optional[Dict[str, Any]] = None,
                     registry: Optional[ProcessorRegistry] = None,
                     languages_hint: Optional[List[str]] = None,
                     ocr_engines=None) -> DocPipelineResult:
    """Registry chain -> OCR cascade if empty/low-conf -> codes -> metadata -> language."""
    cfg = dict(config or {})
    reg = registry or default_registry(cfg)
    ctx = StageContext(content=content, mimetype=mimetype, filename=filename,
                       languages_hint=languages_hint or ["en"], config=cfg)
    result = DocPipelineResult()
    chain = reg.resolve(mimetype, filename)
    # OOXML is a zip: route to office, not archive
    if mimetype.startswith("application/vnd.openxmlformats"):
        chain = [p for p in chain if p.name in ("office", "text")] or chain
    primary_text = ""
    for proc in chain:
        if proc.name in ("ocr", "codes", "pdf-ocr", "pdf-embedded"):
            continue
        sr = proc.process(ctx)
        result.stage_results.append(sr)
        result.capability_ids.append(sr.capability_id)
        result.warnings += sr.warnings
        result.errors += sr.errors
        result.children += sr.children
        if sr.text and not primary_text:
            primary_text = sr.text
            result.extractor = sr.extractor or proc.name
        for k, v in sr.metadata.items():
            result.metadata.setdefault(k, v)
        result.confidence = sr.confidence
        if primary_text:
            break

    # Expand embedded children bytes recursively (1 level, capped)
    expanded: List[Any] = []
    for child in result.children:
        data = getattr(child, "_bytes", None)
        if data:
            from ..extract import _detect_mimetype
            cmime = _detect_mimetype(data, getattr(child, "embedded_path", None))
            had_direct_text = bool((getattr(child, "text", "") or "").strip())
            sub = run_doc_pipeline(data, cmime, getattr(child, "embedded_path", None),
                                   cfg, reg, languages_hint)
            child.text = sub.text
            child.metadata.update(sub.metadata)
            # Text from binary children (nested archives, embedded files) is
            # not in primary_text yet; direct-text children already are.
            if sub.text and not had_direct_text and sub.text not in primary_text:
                primary_text = f"{primary_text}\n{sub.text}" if primary_text else sub.text
            expanded += sub.children
        # strip helper attr for cleanliness
        if hasattr(child, "_bytes"):
            try:
                delattr(child, "_bytes")
            except Exception:
                pass
    result.children += expanded

    # OCR cascade if text empty or low confidence
    need_ocr = (not primary_text.strip()) and mimetype.startswith(("image/", "application/pdf"))
    if need_ocr:
        ocr = OcrProcessor(engines=ocr_engines) if ocr_engines is not None else OcrProcessor()
        ocr_ctx = StageContext(content=content, mimetype=mimetype, filename=filename,
                               languages_hint=languages_hint or ["en"], config=cfg)
        sr = ocr.process(ocr_ctx)
        result.stage_results.append(sr)
        result.capability_ids.append(sr.capability_id)
        result.warnings += sr.warnings
        result.errors += sr.errors
        if sr.text:
            primary_text = sr.text
            result.confidence = sr.confidence
            result.extractor = sr.extractor or "ocr"

    # Codes on images / pdf
    if mimetype.startswith("image/") or mimetype == "application/pdf":
        try:
            sr = CodeProcessor().process(ctx)
            result.codes = sr.codes
            result.stage_results.append(sr)
            if sr.codes:
                best = max((c.quality_raw or c.confidence or 0) for c in sr.codes)
                result.confidence.code_best_quality = float(best)
        except Exception as exc:
            result.warnings.append(f"codes failed: {exc}")

    # Metadata normalize (detected mime wins)
    meta, mw = MetadataNormalizer().normalize(result.metadata, mimetype,
                                              filename or "", len(content))
    result.metadata = meta
    result.warnings += mw
    result.confidence.metadata_completeness = MetadataNormalizer().completeness(meta)
    if "doc.word_count" not in result.metadata and primary_text:
        result.metadata["doc.word_count"] = str(len(primary_text.split()))
    result.text = sanitize_text(primary_text)
    return result
