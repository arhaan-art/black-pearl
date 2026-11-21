#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Image processor, OCR cascade and QR/barcode structured extraction."""
from __future__ import annotations

import base64
import io
import logging
from typing import List, Optional

from ..validate import sanitize_text
from .base import (OCR_ACCEPT, BaseProcessor, CodeResult, StageContext,
                   StageResult, doc_confidence)
from .metadata import MetadataNormalizer, extract_exif
from .ocr_engines import EasyOcrEngine, OcrMyPdfEngine, PaddleEngine, TesseractEngine

logger = logging.getLogger("ingest.processors.image")

try:
    from PIL import Image as _PILImage
    HAS_PIL = True
except Exception:
    HAS_PIL = False

try:
    import cv2  # type: ignore
    HAS_CV2 = True
except Exception:
    cv2 = None  # type: ignore
    HAS_CV2 = False

try:
    import zxingcpp  # type: ignore
    HAS_ZXING = True
except Exception:
    zxingcpp = None  # type: ignore
    HAS_ZXING = False

try:
    from pyzbar.pyzbar import decode as _pyzbar_decode  # type: ignore
    HAS_PYZBAR = True
except Exception:
    _pyzbar_decode = None
    HAS_PYZBAR = False

IMAGE_MIMES = frozenset({"image/png", "image/jpeg", "image/gif", "image/webp",
                         "image/bmp", "image/tiff", "image/svg+xml"})


def _rect_from_polygon(poly) -> dict:
    xs = [p[0] for p in poly]
    ys = [p[1] for p in poly]
    return {"left": min(xs), "top": min(ys), "width": max(xs) - min(xs), "height": max(ys) - min(ys)}


def _content_fields(raw: bytes):
    try:
        text = raw.decode("utf-8")
        return text, None
    except Exception:
        return raw.decode("utf-8", "replace"), base64.b64encode(raw).decode()


class CodeProcessor(BaseProcessor):
    """Decoder order: zxingcpp -> cv2 -> pyzbar -> qreader (design §4)."""
    name = "codes"
    priority = 40
    mimetypes = IMAGE_MIMES | {"application/pdf"}

    def available(self) -> bool:
        return HAS_ZXING or HAS_CV2 or HAS_PYZBAR

    def extract_codes(self, image_bytes: bytes, page: Optional[int] = None,
                      order: Optional[List[str]] = None) -> List[CodeResult]:
        results: List[CodeResult] = []
        order = order or ["zxingcpp", "cv2", "pyzbar", "qreader"]
        cap = self.capability().capability_id
        for decoder in order:
            if decoder == "zxingcpp" and HAS_ZXING:
                results += self._zxing(image_bytes, page, cap)
            elif decoder == "cv2" and HAS_CV2:
                results += self._cv2(image_bytes, page, cap)
            elif decoder == "pyzbar" and HAS_PYZBAR:
                results += self._pyzbar(image_bytes, page, cap)
            if results:
                break  # first decoder that finds codes wins (repo behaviour)
        return results

    def _zxing(self, image_bytes, page, cap):
        out = []
        from PIL import Image
        try:
            img = Image.open(io.BytesIO(image_bytes))
            img.load()
        except Exception:
            return out  # non-image bytes (e.g. PDF): no codes, not an error
        for b in zxingcpp.read_barcodes(img):
            raw = b.bytes if hasattr(b, "bytes") and b.bytes else (b.text or "").encode()
            text, b64 = _content_fields(raw if isinstance(raw, bytes) else str(raw).encode())
            pos = b.position
            poly = [[pos.top_left.x, pos.top_left.y], [pos.top_right.x, pos.top_right.y],
                    [pos.bottom_right.x, pos.bottom_right.y], [pos.bottom_left.x, pos.bottom_left.y]]
            fmt = str(b.format)
            kind = "qrcode" if "QR" in fmt else "barcode"
            out.append(CodeResult(kind=kind, format=fmt, content=text, content_bytes_b64=b64,
                                  polygon=poly, rect=_rect_from_polygon(poly),
                                  orientation_deg=getattr(b, "orientation", None),
                                  decoder="zxingcpp", page=page, capability_id=cap))
        return out

    def _cv2(self, image_bytes, page, cap):
        out = []
        import numpy as np
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            return out
        detector = cv2.QRCodeDetector()
        for candidate in (img, cv2.bitwise_not(img)):
            try:
                ok, decoded, points, _s = detector.detectAndDecodeMulti(candidate)
                if ok and points is not None:
                    for text, pts in zip(decoded, points):
                        if not text:
                            continue
                        poly = [[int(x), int(y)] for x, y in pts]
                        out.append(CodeResult(kind="qrcode", format="QR Code", content=str(text),
                                              polygon=poly, rect=_rect_from_polygon(poly),
                                              confidence=None, quality_raw=None,
                                              decoder="cv2", page=page, capability_id=cap))
                    if out:
                        return out
            except Exception:
                pass
            data, points, _s = detector.detectAndDecode(candidate)
            if data:
                poly = [[int(x), int(y)] for x, y in points[0]] if points is not None else None
                out.append(CodeResult(kind="qrcode", format="QR Code", content=str(data),
                                      polygon=poly, rect=_rect_from_polygon(poly) if poly else None,
                                      decoder="cv2", page=page, capability_id=cap))
                return out
        return out

    def _pyzbar(self, image_bytes, page, cap):
        out = []
        from PIL import Image
        try:
            img = Image.open(io.BytesIO(image_bytes))
            img.load()
        except Exception:
            return out  # non-image bytes: no codes, not an error
        for d in _pyzbar_decode(img):
            text, b64 = _content_fields(d.data)
            poly = [[p.x, p.y] for p in d.polygon]
            fmt = str(d.type)
            kind = "qrcode" if fmt == "QRCODE" else "barcode"
            out.append(CodeResult(kind=kind, format=fmt, content=text, content_bytes_b64=b64,
                                  polygon=poly, rect={"left": d.rect.left, "top": d.rect.top,
                                                      "width": d.rect.width, "height": d.rect.height},
                                  orientation_deg=getattr(d, "orientation", None),
                                  quality_raw=float(d.quality) if getattr(d, "quality", None) is not None else None,
                                  decoder="pyzbar", page=page, capability_id=cap))
        return out

    def process(self, ctx: StageContext) -> StageResult:
        return self._timed(ctx, lambda: StageResult(
            codes=self.extract_codes(ctx.content, order=ctx.config.get("code_decoders"))))


class ImageProcessor(BaseProcessor):
    name = "image"
    priority = 10
    mimetypes = IMAGE_MIMES
    prefixes = ("image/",)

    def available(self) -> bool:
        return HAS_PIL

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            meta, warnings = {}, []
            exif_meta, ew = extract_exif(ctx.content)
            meta.update(exif_meta)
            warnings += ew
            meta, mw = MetadataNormalizer().normalize(meta, ctx.mimetype,
                                                      ctx.filename or "", len(ctx.content))
            warnings += mw
            conf = doc_confidence(None, "")
            conf.metadata_completeness = MetadataNormalizer().completeness(meta)
            return StageResult(metadata=meta, warnings=warnings, confidence=conf)
        return self._timed(ctx, _run)


class OcrProcessor(BaseProcessor):
    """OCR cascade with quality bands + auto-reprocess (design §3).

    Order: born-digital handled by caller (skip); then engine order from
    config (ocrmypdf,tesseract,paddle,easyocr); max 2 extra attempts;
    best-scoring result is always kept, never discarded.
    """
    name = "ocr"
    priority = 20
    mimetypes = IMAGE_MIMES | {"application/pdf"}
    prefixes = ("image/",)

    def __init__(self, engines=None, accept: float = OCR_ACCEPT,
                 reprocess_below: float = 0.60) -> None:
        self._engines = engines  # injectable stubs for tests
        self.accept = accept
        self.reprocess_below = reprocess_below

    def available(self) -> bool:
        return True  # cascade degrades to empty text + warning

    def _engine_list(self, ctx: StageContext):
        if self._engines is not None:
            return self._engines
        order = ctx.config.get("ocr_engine_order") or ["tesseract", "paddle", "easyocr"]
        engines = []
        for name in order:
            if name == "tesseract":
                engines.append(TesseractEngine(psm=int(ctx.config.get("tesseract_psm", 6)),
                                               word_conf_min=float(ctx.config.get("word_conf_min", 0.60))))
            elif name == "paddle":
                engines.append(PaddleEngine(rec_thresh=0.5))
            elif name == "easyocr":
                engines.append(EasyOcrEngine())
            elif name == "ocrmypdf":
                engines.append(OcrMyPdfEngine())
        return engines

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            best_text, best_conf, best_engine, warnings = "", -1.0, "", []
            attempts = 0
            max_attempts = 3  # 1 + 2 extra reprocess attempts
            for engine in self._engine_list(ctx):
                if attempts >= max_attempts:
                    break
                if hasattr(engine, "available") and not engine.available():
                    continue
                attempts += 1
                if ctx.mimetype == "application/pdf" and engine.name == "ocrmypdf":
                    res = engine.ocr_pdf(ctx.content, ctx.languages_hint)
                elif hasattr(engine, "ocr_image"):
                    res = engine.ocr_image(ctx.content, ctx.languages_hint)
                else:
                    continue
                warnings += list(getattr(res, "warnings", []) or [])
                conf = float(getattr(res, "mean_conf", 0.0) or 0.0)
                text = sanitize_text(getattr(res, "text", "") or "")
                if text and (conf > best_conf or not best_text):
                    best_text, best_conf, best_engine = text, conf, engine.name
                if best_conf >= self.accept and best_text:
                    break
                if best_conf >= self.reprocess_below and best_text:
                    warnings.append("ocr:low-confidence")
                    break
                # else: cascade to next engine / alternate psm handled by next engine
            if best_conf < 0:
                best_conf = 0.0
            if not best_text:
                warnings.append("ocr produced no text")
            elif best_conf < self.reprocess_below:
                warnings.append("ocr:low-confidence")
            conf_obj = doc_confidence(best_conf if best_text else 0.0, best_text)
            conf_obj.per_stage["ocr_engine"] = best_engine
            return StageResult(text=best_text, warnings=warnings, confidence=conf_obj,
                               extractor=f"ocr:{best_engine}" if best_engine else "ocr")
        return self._timed(ctx, _run)
