#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Office processor: OOXML parsed runs/tables; legacy OLE via Tika only."""
from __future__ import annotations

import io
import zipfile
import xml.etree.ElementTree as ET
from typing import List

from ..validate import sanitize_text
from .base import BaseProcessor, StageContext, StageResult

OOXML_MIMES = frozenset({
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
})
OLE_MIMES = frozenset({"application/msword", "application/vnd.ms-excel",
                       "application/vnd.ms-powerpoint"})
_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


class OfficeProcessor(BaseProcessor):
    name = "office"
    priority = 10
    mimetypes = OOXML_MIMES | OLE_MIMES

    def supports(self, mimetype, filename=None) -> bool:
        if super().supports(mimetype, filename):
            return True
        return (filename or "").lower().endswith((".docx", ".xlsx", ".pptx", ".doc", ".xls", ".ppt"))

    def process(self, ctx: StageContext) -> StageResult:
        return self._timed(ctx, lambda: self._run(ctx))

    def _run(self, ctx: StageContext) -> StageResult:
        warnings: List[str] = []
        if ctx.mimetype in OLE_MIMES:
            warnings.append("legacy OLE format requires Tika")
            return StageResult(warnings=warnings, metadata={"mimetype": ctx.mimetype})
        texts: List[str] = []
        try:
            with zipfile.ZipFile(io.BytesIO(ctx.content)) as zf:
                names = zf.namelist()
                targets = [n for n in names if n.endswith(("document.xml", "sharedStrings.xml"))
                           or "/slides/slide" in n or "/worksheets/sheet" in n]
                for name in targets:
                    root = ET.fromstring(zf.read(name))
                    found = []
                    for para in root.iter(f"{_W}p"):
                        runs = [t.text or "" for t in para.iter(f"{_W}t")]
                        if runs:
                            found.append("".join(runs))
                    if name.endswith("sharedStrings.xml"):
                        found += [t.text or "" for t in root.iter(f"{_S}t")]
                    if not found:
                        found = [t.text or "" for t in root.iter() if (t.text or "").strip()]
                    texts += found
        except Exception as exc:
            warnings.append(f"office extract failed: {exc}")
        return StageResult(text=sanitize_text("\n".join(t for t in texts if t.strip())),
                           metadata={"mimetype": ctx.mimetype}, warnings=warnings)
