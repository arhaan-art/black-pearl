#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OCR engines: Tesseract (pytesseract TSV), OCRmyPDF, Paddle (subprocess), EasyOCR fallback.

Every engine is optional-import / binary gated like ``dedup.HAS_TLSH`` so a
host without Java/Tesseract/Paddle still ingests via the PyMuPDF fallback.
"""
from __future__ import annotations

import io
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import List, Optional

from .base import parse_tesseract_tsv

logger = logging.getLogger("ingest.processors.ocr")

try:
    import pytesseract  # type: ignore
    HAS_PYTESSERACT = True
except Exception:
    pytesseract = None  # type: ignore
    HAS_PYTESSERACT = False


@dataclass
class OcrWord:
    text: str
    conf: float


@dataclass
class OcrEngineResult:
    text: str = ""
    mean_conf: float = 0.0
    words: List[OcrWord] = field(default_factory=list)
    engine: str = ""
    warnings: List[str] = field(default_factory=list)


def _tesseract_version() -> str:
    try:
        out = subprocess.run(["tesseract", "--version"], capture_output=True, text=True, timeout=10)
        return (out.stdout or out.stderr).splitlines()[0].strip() if (out.stdout or out.stderr) else "unknown"
    except Exception:
        return "unknown"


class TesseractEngine:
    name = "tesseract"

    def __init__(self, psm: int = 6, word_conf_min: float = 0.60) -> None:
        self.psm = psm
        self.word_conf_min = word_conf_min
        self._version: Optional[str] = None

    def available(self) -> bool:
        return HAS_PYTESSERACT and shutil.which("tesseract") is not None

    @property
    def engine_version(self) -> str:
        if self._version is None:
            self._version = _tesseract_version()
        return self._version

    def ocr_image(self, image_bytes: bytes, languages: Optional[List[str]] = None,
                  psm: Optional[int] = None) -> OcrEngineResult:
        if not self.available():
            return OcrEngineResult(engine=self.name, warnings=["tesseract unavailable"])
        from PIL import Image  # local import: Pillow optional in prod venv
        img = Image.open(io.BytesIO(image_bytes))
        if img.info.get("dpi", (72, 72))[0] < 300:
            warn = "image below 300 DPI; upscaled"
            img = img.resize((img.width * 2, img.height * 2))
        else:
            warn = ""
        lang = "+".join(languages or ["eng"])
        # tessdata codes (eng/fra/deu), not BCP47
        lang = lang.replace("en", "eng").replace("fr", "fra").replace("de", "deu")
        cfg = f"--oem 1 --psm {psm if psm is not None else self.psm}"
        tsv = pytesseract.image_to_data(img, lang=lang, config=cfg, output_type=pytesseract.Output.STRING)
        text, mean, _kept = parse_tesseract_tsv(tsv, self.word_conf_min)
        return OcrEngineResult(text=text, mean_conf=mean, engine=self.name,
                               warnings=[warn] if warn else [])


class OcrMyPdfEngine:
    name = "ocrmypdf"

    def available(self) -> bool:
        if shutil.which("ocrmypdf"):
            return True
        try:
            import ocrmypdf  # noqa: F401
            return shutil.which("tesseract") is not None
        except Exception:
            return False

    def ocr_pdf(self, content: bytes, languages: Optional[List[str]] = None,
                mode: str = "redo") -> OcrEngineResult:
        if not self.available():
            return OcrEngineResult(engine=self.name, warnings=["ocrmypdf/tesseract unavailable"])
        import tempfile, os
        langs = [l.replace("en", "eng").replace("fr", "fra").replace("de", "deu")
                 for l in (languages or ["eng"])]
        with tempfile.TemporaryDirectory() as tmp:
            src, dst, side = (os.path.join(tmp, n) for n in ("in.pdf", "out.pdf", "side.txt"))
            with open(src, "wb") as fh:
                fh.write(content)
            cmd = ["ocrmypdf", "--mode", mode, "--sidecar", side, "-l", "+".join(langs), src, dst]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if proc.returncode != 0:
                return OcrEngineResult(engine=self.name, warnings=[f"ocrmypdf {mode} failed: {proc.stderr[-300:]}"])
            text = open(side, encoding="utf-8", errors="ignore").read() if os.path.exists(side) else ""
            return OcrEngineResult(text=text, mean_conf=0.0, engine=self.name)


class PaddleEngine:
    """PaddleOCR in an isolated venv subprocess (numpy/opencv conflicts avoided)."""
    name = "paddle"

    def __init__(self, venv_python: Optional[str] = None, rec_thresh: float = 0.5) -> None:
        self.venv_python = venv_python
        self.rec_thresh = rec_thresh

    def available(self) -> bool:
        if self.venv_python:
            import os
            return os.path.exists(self.venv_python)
        try:
            import paddleocr  # noqa: F401
            return True
        except Exception:
            return False

    def ocr_image(self, image_bytes: bytes, languages: Optional[List[str]] = None) -> OcrEngineResult:
        # Direct import path; subprocess isolation is used when venv_python is set
        # by the caller via a wrapper script (see design §3). Here we keep it simple:
        if self.venv_python:
            return OcrEngineResult(engine=self.name, warnings=["paddle subprocess venv configured; use wrapper"])
        try:
            from paddleocr import PaddleOCR  # type: ignore
            import tempfile, os
            ocr = PaddleOCR(lang="en", use_angle_cls=False)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fh:
                fh.write(image_bytes)
                path = fh.name
            try:
                result = ocr.predict(path)  # v3 API
                texts, scores = [], []
                for res in (result or []):
                    data = res if isinstance(res, dict) else getattr(res, "json", {}) or {}
                    inner = data.get("res", data) if isinstance(data, dict) else {}
                    texts += list(inner.get("rec_texts") or [])
                    scores += [float(s) for s in (inner.get("rec_scores") or []) if float(s) >= self.rec_thresh]
                mean = round(sum(scores) / len(scores), 4) if scores else 0.0
                return OcrEngineResult(text=" ".join(texts), mean_conf=mean, engine=self.name)
            finally:
                os.unlink(path)
        except Exception as exc:
            return OcrEngineResult(engine=self.name, warnings=[f"paddle failed: {exc}"])


class EasyOcrEngine:
    name = "easyocr"

    def available(self) -> bool:
        try:
            import easyocr  # noqa: F401
            return True
        except Exception:
            return False

    def ocr_image(self, image_bytes: bytes, languages: Optional[List[str]] = None) -> OcrEngineResult:
        try:
            import easyocr, tempfile, os
            reader = easyocr.Reader(list(languages or ["en"]), gpu=False, verbose=False)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fh:
                fh.write(image_bytes)
                path = fh.name
            try:
                rows = reader.readtext(path)
                texts = [t for (_b, t, s) in rows if float(s) >= 0.2 and len(t) > 1]
                scores = [float(s) for (_b, _t, s) in rows if float(s) >= 0.2]
                mean = round(sum(scores) / len(scores), 4) if scores else 0.0
                return OcrEngineResult(text=" ".join(texts), mean_conf=mean, engine=self.name)
            finally:
                os.unlink(path)
        except Exception as exc:
            return OcrEngineResult(engine=self.name, warnings=[f"easyocr failed: {exc}"])
