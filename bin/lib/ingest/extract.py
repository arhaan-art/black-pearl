#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Document extraction: Apache Tika REST backend with Python fallback.

The fallback uses only the standard library plus optional PyMuPDF, so
ingestion keeps working when Java/Tika are absent (the current host has
no Java). Extracted text is always sanitised before downstream use.
Embedded files inside archives become child results (recursive).
"""
from __future__ import annotations

import email
import io
import json
import logging
import os
import urllib.request
import zipfile
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional

from .validate import sanitize_text

logger = logging.getLogger("ingest.extract")

TEXT_EXTS = {".txt", ".md", ".log", ".json", ".csv", ".xml", ".html", ".htm"}


@dataclass
class ExtractedDoc:
    text: str
    metadata: Dict[str, str] = field(default_factory=dict)
    children: List["ExtractedDoc"] = field(default_factory=list)
    extractor: str = "python-fallback"
    warnings: List[str] = field(default_factory=list)
    embedded_path: Optional[str] = None


class _TextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data.strip())


def _detect_mimetype(content: bytes, filename: Optional[str] = None) -> str:
    try:
        import magic  # python-magic, already a repo dependency
        return magic.from_buffer(content, mime=True)
    except Exception:
        pass
    if filename:
        ext = os.path.splitext(filename)[1].lower()
        mapping = {".pdf": "application/pdf", ".txt": "text/plain",
                   ".html": "text/html", ".json": "application/json",
                   ".zip": "application/zip", ".eml": "message/rfc822",
                   ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
        if ext in mapping:
            return mapping[ext]
    if content[:4] == b"%PDF":
        return "application/pdf"
    if content[:2] == b"PK":
        return "application/zip"
    try:
        content.decode("utf-8")
        return "text/plain"
    except Exception:
        return "application/octet-stream"


class PythonFallbackExtractor:
    """Stdlib extractor: text, HTML, EML, ZIP (recursive), PDF via PyMuPDF."""

    def __init__(self, max_embedded: int = 50, max_bytes: int = 20_000_000) -> None:
        self.max_embedded = max_embedded
        self.max_bytes = max_bytes

    def extract(self, content: bytes, filename: Optional[str] = None,
                mimetype: Optional[str] = None) -> ExtractedDoc:
        mime = mimetype or _detect_mimetype(content, filename)
        warnings: List[str] = []
        if len(content) > self.max_bytes:
            warnings.append(f"content truncated at {self.max_bytes} bytes")
            content = content[: self.max_bytes]

        if mime == "application/pdf" or (filename or "").lower().endswith(".pdf"):
            return self._extract_pdf(content, warnings)
        if mime == "application/zip" or zipfile.is_zipfile(io.BytesIO(content)):
            return self._extract_zip(content, warnings)
        if mime in ("message/rfc822",) or (filename or "").lower().endswith(".eml"):
            return self._extract_eml(content, warnings)
        if mime == "text/html" or (filename or "").lower().endswith((".html", ".htm")):
            parser = _TextHTMLParser()
            parser.feed(content.decode("utf-8", "ignore"))
            return ExtractedDoc(text=sanitize_text("\n".join(parser.parts)),
                                metadata={"mimetype": mime}, warnings=warnings)
        # OOXML (docx/xlsx/pptx) are zips with document.xml
        if mime.startswith("application/vnd.openxmlformats"):
            return self._extract_zip(content, warnings)
        text = content.decode("utf-8", "ignore")
        return ExtractedDoc(text=sanitize_text(text), metadata={"mimetype": mime},
                            warnings=warnings)

    def _extract_pdf(self, content: bytes, warnings: List[str]) -> ExtractedDoc:
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(stream=content, filetype="pdf")
            pages = [page.get_text() for page in doc]
            meta = {k: str(v) for k, v in (doc.metadata or {}).items() if v}
            canonical = {"dc:title": meta.get("title", ""), "dc:creator": meta.get("author", ""),
                         "dc:date": meta.get("creationDate", ""), "mimetype": "application/pdf"}
            return ExtractedDoc(text=sanitize_text("\n".join(pages)), metadata=canonical,
                                extractor="pymupdf", warnings=warnings)
        except Exception as exc:
            warnings.append(f"pymupdf unavailable/failed: {exc}")
            return ExtractedDoc(text="", metadata={"mimetype": "application/pdf"},
                                warnings=warnings + ["no text layer extracted; OCR stage required"])

    def _extract_zip(self, content: bytes, warnings: List[str]) -> ExtractedDoc:
        children: List[ExtractedDoc] = []
        texts: List[str] = []
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = zf.namelist()
                if len(names) > self.max_embedded:
                    warnings.append(f"archive has {len(names)} entries; capped at {self.max_embedded}")
                for name in names[: self.max_embedded]:
                    if name.endswith("/"):
                        continue
                    try:
                        data = zf.read(name)
                    except Exception as exc:
                        warnings.append(f"embedded {name}: {exc}")
                        continue
                    child = self.extract(data, filename=name)
                    child.embedded_path = name
                    children.append(child)
                    if child.text:
                        texts.append(child.text)
        except Exception as exc:
            warnings.append(f"zip extract failed: {exc}")
        return ExtractedDoc(text=sanitize_text("\n".join(texts)),
                            metadata={"mimetype": "application/zip"},
                            children=children, warnings=warnings)

    def _extract_eml(self, content: bytes, warnings: List[str]) -> ExtractedDoc:
        children: List[ExtractedDoc] = []
        texts: List[str] = []
        try:
            msg = email.message_from_bytes(content)
            meta = {"dc:title": str(msg.get("Subject", "")), "mimetype": "message/rfc822",
                    "dc:creator": str(msg.get("From", "")), "dc:date": str(msg.get("Date", ""))}
            if msg.is_multipart():
                for part in msg.walk():
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    fname = part.get_filename()
                    if fname:
                        child = self.extract(payload, filename=fname)
                        child.embedded_path = fname
                        children.append(child)
                        if child.text:
                            texts.append(child.text)
                    elif part.get_content_type() == "text/plain":
                        texts.append(payload.decode("utf-8", "ignore"))
            else:
                payload = msg.get_payload(decode=True) or b""
                texts.append(payload.decode("utf-8", "ignore"))
            return ExtractedDoc(text=sanitize_text("\n".join(texts)), metadata=meta,
                                children=children, warnings=warnings)
        except Exception as exc:
            warnings.append(f"eml parse failed: {exc}")
            return ExtractedDoc(text="", metadata={"mimetype": "message/rfc822"}, warnings=warnings)


class TikaExtractor:
    """Apache Tika Server backend (plain REST, localhost only)."""

    def __init__(self, base_url: str = "http://127.0.0.1:9998", timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def available(self) -> bool:
        try:
            with urllib.request.urlopen(f"{self.base_url}/version", timeout=5) as resp:
                return resp.status == 200
        except Exception as exc:
            logger.info("tika unavailable at %s: %s", self.base_url, exc)
            return False

    def extract(self, content: bytes, filename: Optional[str] = None,
                mimetype: Optional[str] = None) -> ExtractedDoc:
        req = urllib.request.Request(f"{self.base_url}/rmeta", data=content, method="PUT",
                                     headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            docs = json.loads(resp.read().decode("utf-8", "ignore"))
        children: List[ExtractedDoc] = []
        texts: List[str] = []
        meta: Dict[str, str] = {}
        for i, doc in enumerate(docs if isinstance(docs, list) else [docs]):
            text = sanitize_text(str(doc.get("X-TIKA:content") or doc.get("content") or ""))
            if i == 0:
                meta = {"dc:title": str(doc.get("dc:title") or doc.get("title") or ""),
                        "dc:creator": str(doc.get("dc:creator") or doc.get("Author") or ""),
                        "dc:date": str(doc.get("dc:date") or doc.get("dcterms:created") or ""),
                        "mimetype": str(doc.get("Content-Type") or "application/octet-stream")}
                texts.append(text)
            else:
                children.append(ExtractedDoc(
                    text=text, extractor="tika",
                    embedded_path=str(doc.get("resourceName") or f"embedded-{i}"),
                    metadata={"mimetype": str(doc.get("Content-Type") or "")}))
        return ExtractedDoc(text="\n".join(t for t in texts if t), metadata=meta,
                            children=children, extractor="tika")


class DocumentExtractor:
    """Facade: Tika when reachable, otherwise the Python fallback."""

    def __init__(self, tika_url: Optional[str] = None, prefer_tika: bool = True) -> None:
        self.fallback = PythonFallbackExtractor()
        self.tika = TikaExtractor(tika_url) if tika_url else None
        self.prefer_tika = prefer_tika

    def extract(self, content: bytes, filename: Optional[str] = None,
                mimetype: Optional[str] = None) -> ExtractedDoc:
        if self.prefer_tika and self.tika is not None and self.tika.available():
            try:
                return self.tika.extract(content, filename=filename, mimetype=mimetype)
            except Exception as exc:
                logger.warning("tika extract failed, fallback: %s", exc)
        return self.fallback.extract(content, filename=filename, mimetype=mimetype)

    @staticmethod
    def detect_mimetype(content: bytes, filename: Optional[str] = None) -> str:
        return _detect_mimetype(content, filename)
