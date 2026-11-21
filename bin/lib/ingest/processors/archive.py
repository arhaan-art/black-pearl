#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Archive (zip/tar/gz/bz2/lzma) and Office (OOXML parsed) processors."""
from __future__ import annotations

import bz2
import gzip
import io
import logging
import lzma
import tarfile
import zipfile
import xml.etree.ElementTree as ET
from typing import List

from ..validate import sanitize_text
from .base import BaseProcessor, StageContext, StageResult

logger = logging.getLogger("ingest.processors.archive")

ARCHIVE_MIMES = frozenset({
    "application/zip", "application/x-tar", "application/gzip",
    "application/x-bzip2", "application/x-xz", "application/x-7z-compressed",
    "application/vnd.rar", "application/x-rar-compressed",
})
OOXML_MIMES = frozenset({
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
})
OLE_MIMES = frozenset({"application/msword", "application/vnd.ms-excel",
                       "application/vnd.ms-powerpoint"})


class ArchiveProcessor(BaseProcessor):
    name = "archive"
    priority = 10
    mimetypes = ARCHIVE_MIMES

    def __init__(self, max_embedded: int = 50) -> None:
        self.max_embedded = max_embedded

    def supports(self, mimetype, filename=None) -> bool:
        if super().supports(mimetype, filename):
            return True
        fn = (filename or "").lower()
        return fn.endswith((".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".gz", ".bz2", ".xz"))

    def process(self, ctx: StageContext) -> StageResult:
        return self._timed(ctx, lambda: self._run(ctx))

    @staticmethod
    def _direct_text(data: bytes) -> str:
        # Binary entries (nested archives, images) carry no direct text;
        # their text comes from recursive expansion of _bytes, so decoding
        # them with errors="ignore" only pollutes the parent text.
        if data[:4] in (b"PK\x03\x04",) or data[:2] in (b"\x1f\x8b", b"BZ", b"\xff\xd8", b"\x89P"):
            return ""
        try:
            text = data.decode("utf-8")
        except (UnicodeDecodeError, ValueError):
            return ""
        if "\x00" in text:
            return ""
        return sanitize_text(text[:200000])

    def _children_from_entries(self, entries, ctx, warnings):
        from ..extract import ExtractedDoc
        children, texts = [], []
        if len(entries) > self.max_embedded:
            warnings.append(f"archive has {len(entries)} entries; capped at {self.max_embedded}")
        for name, data in entries[: self.max_embedded]:
            if name.endswith("/"):
                continue
            child = ExtractedDoc(text=self._direct_text(data),
                                 metadata={"mimetype": "application/octet-stream"},
                                 embedded_path=name)
            child._bytes = data  # type: ignore[attr-defined]
            children.append(child)
            if child.text:
                texts.append(child.text)
        return children, texts

    def _run(self, ctx: StageContext) -> StageResult:
        warnings: List[str] = []
        entries = []
        content, mime = ctx.content, ctx.mimetype
        try:
            if mime == "application/zip" or zipfile.is_zipfile(io.BytesIO(content)):
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    entries = [(n, zf.read(n)) for n in zf.namelist() if not n.endswith("/")]
            elif mime in ("application/x-tar", "application/gzip") or (ctx.filename or "").endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")):
                with tarfile.open(fileobj=io.BytesIO(content), mode="r:*") as tf:
                    for m in tf.getmembers():
                        if m.isfile():
                            f = tf.extractfile(m)
                            entries.append((m.name, f.read() if f else b""))
            elif mime == "application/gzip" or (ctx.filename or "").endswith(".gz"):
                entries = [((ctx.filename or "decompressed").removesuffix(".gz"), gzip.decompress(content))]
            elif mime == "application/x-bzip2":
                entries = [("decompressed", bz2.decompress(content))]
            elif mime == "application/x-xz":
                entries = [("decompressed", lzma.decompress(content))]
            else:
                warnings.append(f"archive mime {mime} requires Tika (7z/rar)")
                return StageResult(warnings=warnings, metadata={"mimetype": mime})
        except Exception as exc:
            # plain single-file gzip (not a tarball) fallback
            try:
                data = gzip.decompress(content)
                entries = [((ctx.filename or "decompressed").removesuffix(".gz"), data)]
            except Exception:
                warnings.append(f"archive extract failed: {exc}")
                return StageResult(warnings=warnings, metadata={"mimetype": mime})
        children, texts = self._children_from_entries(entries, ctx, warnings)
        meta = {"mimetype": mime, "archive.entry_count": str(len(entries))}
        return StageResult(text=sanitize_text("\n".join(texts)), metadata=meta,
                           children=children, warnings=warnings)


__all__ = ["ArchiveProcessor", "ARCHIVE_MIMES"]
