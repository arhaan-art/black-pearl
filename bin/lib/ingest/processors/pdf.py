#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PDF processors: native text (PyMuPDF), OCR layer, embedded files."""
from __future__ import annotations

import logging
from typing import List

from ..validate import sanitize_text
from .base import BaseProcessor, StageContext, StageResult, doc_confidence
from .metadata import MetadataNormalizer
from .ocr_engines import OcrMyPdfEngine

logger = logging.getLogger("ingest.processors.pdf")

try:
    import fitz  # PyMuPDF
    HAS_FITZ = True
except Exception:
    try:
        import pymupdf as fitz  # type: ignore
        HAS_FITZ = True
    except Exception:
        fitz = None  # type: ignore
        HAS_FITZ = False


class PdfNativeProcessor(BaseProcessor):
    name = "pdf-native"
    priority = 10
    mimetypes = frozenset({"application/pdf"})

    def available(self) -> bool:
        return HAS_FITZ

    def process(self, ctx: StageContext) -> StageResult:
        return self._timed(ctx, lambda: self._run(ctx))

    def _run(self, ctx: StageContext) -> StageResult:
        warnings: List[str] = []
        doc = fitz.open(stream=ctx.content, filetype="pdf")
        pages = [page.get_text() for page in doc]
        text = sanitize_text("\n".join(pages))
        meta_raw = {k: str(v) for k, v in (doc.metadata or {}).items() if v}
        meta_raw["doc.page_count"] = str(doc.page_count)
        if doc.is_encrypted:
            meta_raw["pdf.encrypted"] = "true"
        meta_raw["doc.word_count"] = str(len(text.split()))
        meta, mw = MetadataNormalizer().normalize(meta_raw, "application/pdf",
                                                  ctx.filename or "", len(ctx.content))
        warnings += mw
        if not text.strip():
            warnings.append("no text layer extracted; OCR stage required")
        conf = doc_confidence(1.0 if text.strip() else 0.0, text, doc.page_count)
        conf.metadata_completeness = MetadataNormalizer().completeness(meta)
        result = StageResult(text=text, metadata=meta, warnings=warnings, confidence=conf)

        # embedded files -> children
        try:
            for i in range(doc.embfile_count()):
                info = doc.embfile_info(i)
                data = doc.embfile_get(i)
                from ..extract import ExtractedDoc
                child = ExtractedDoc(text="", metadata={"mimetype": "application/octet-stream"},
                                     embedded_path=str(info.get("filename") or f"embedded-{i}"))
                child._bytes = data  # type: ignore[attr-defined]
                result.children.append(child)
        except Exception as exc:
            warnings.append(f"embedded extraction failed: {exc}")
        return result


class PdfOcrProcessor(BaseProcessor):
    name = "pdf-ocr"
    priority = 20
    mimetypes = frozenset({"application/pdf"})

    def available(self) -> bool:
        return OcrMyPdfEngine().available()

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            engine = OcrMyPdfEngine()
            for mode in ("redo", "force"):
                res = engine.ocr_pdf(ctx.content, ctx.languages_hint, mode=mode)
                if res.text.strip():
                    text = sanitize_text(res.text)
                    meta, mw = MetadataNormalizer().normalize({}, "application/pdf",
                                                              ctx.filename or "", len(ctx.content))
                    return StageResult(text=text, metadata=meta, warnings=res.warnings + mw,
                                       confidence=doc_confidence(res.mean_conf or None, text))
                last = res
            return StageResult(text="", warnings=(last.warnings if 'last' in dir() else []) + ["pdf OCR produced no text"],
                               confidence=doc_confidence(0.0, ""))
        return self._timed(ctx, _run)


class PdfEmbeddedProcessor(BaseProcessor):
    name = "pdf-embedded"
    priority = 30
    mimetypes = frozenset({"application/pdf"})

    def available(self) -> bool:
        return HAS_FITZ

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            # Children are produced by PdfNativeProcessor; this stage exists so
            # the registry can route embedded-only jobs explicitly.
            return StageResult(warnings=[])
        return self._timed(ctx, _run)
