#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Processor protocol, stage context/result and confidence model."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

logger = logging.getLogger("ingest.processors.base")

OCR_ACCEPT = 0.85
OCR_REPROCESS_BELOW = 0.60
WORD_CONF_MIN = 0.60


def config_hash(config: Optional[Dict[str, Any]] = None) -> str:
    raw = json.dumps(config or {}, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass
class Capability:
    name: str
    version: str = "1.0.0"
    engine_version: str = "stdlib"
    cfg_hash: str = ""

    def __post_init__(self) -> None:
        if not self.cfg_hash:
            self.cfg_hash = config_hash({"name": self.name, "version": self.version})

    @property
    def capability_id(self) -> str:
        return f"{self.name}@{self.version}+{self.engine_version}#{self.cfg_hash[:8]}"


@dataclass
class StageError:
    stage: str
    capability_id: str = ""
    error_class: str = "non-retryable"  # retryable | non-retryable
    message: str = ""
    exception_type: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class StageConfidence:
    text_conf: Optional[float] = None
    ocr_mean_conf: Optional[float] = None
    lang_conf: Optional[float] = None
    code_best_quality: Optional[float] = None
    metadata_completeness: Optional[float] = None
    per_stage: Dict[str, float] = field(default_factory=dict)

    @property
    def overall(self) -> float:
        vals = [v for v in (self.text_conf, self.ocr_mean_conf, self.lang_conf,
                            self.metadata_completeness) if v is not None]
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    def band(self) -> str:
        o = self.ocr_mean_conf if self.ocr_mean_conf is not None else self.overall
        if o >= OCR_ACCEPT:
            return "accept"
        if o >= OCR_REPROCESS_BELOW:
            return "accept-low"
        return "reprocess"

    def to_dict(self) -> Dict[str, Any]:
        d = dict(self.__dict__)
        d["overall"] = self.overall
        d["band"] = self.band()
        return d


@dataclass
class CodeResult:
    kind: str  # qrcode | barcode
    format: str = ""
    content: str = ""
    content_bytes_b64: Optional[str] = None
    polygon: Optional[List[List[int]]] = None
    rect: Optional[Dict[str, int]] = None
    orientation_deg: Optional[int] = None
    quality_raw: Optional[float] = None
    confidence: Optional[float] = None
    decoder: str = ""
    page: Optional[int] = None
    capability_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class StageContext:
    content: bytes = b""
    mimetype: str = "application/octet-stream"
    filename: Optional[str] = None
    embedded_path: Optional[str] = None
    envelope: Any = None
    languages_hint: List[str] = field(default_factory=lambda: ["en"])
    traceparent: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StageResult:
    text: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    children: List[Any] = field(default_factory=list)
    codes: List[CodeResult] = field(default_factory=list)
    confidence: StageConfidence = field(default_factory=StageConfidence)
    warnings: List[str] = field(default_factory=list)
    errors: List[StageError] = field(default_factory=list)
    capability_id: str = ""
    duration_ms: float = 0.0
    extractor: str = ""


@runtime_checkable
class DocumentProcessor(Protocol):
    name: str
    priority: int
    mimetypes: Any

    def available(self) -> bool: ...
    def supports(self, mimetype: str, filename: Optional[str] = None) -> bool: ...
    def process(self, ctx: StageContext) -> StageResult: ...


class BaseProcessor:
    name = "base"
    version = "1.0.0"
    engine_version = "stdlib"
    priority = 100
    mimetypes: frozenset = frozenset()
    prefixes: tuple = ()
    _available_cache: Optional[bool] = None

    def capability(self, cfg: Optional[Dict[str, Any]] = None) -> Capability:
        return Capability(self.name, self.version, self.engine_version,
                          config_hash(cfg or {}))

    def available(self) -> bool:
        return True

    def supports(self, mimetype: str, filename: Optional[str] = None) -> bool:
        if mimetype in self.mimetypes:
            return True
        return any(mimetype.startswith(p) for p in self.prefixes)

    def process(self, ctx: StageContext) -> StageResult:  # pragma: no cover
        raise NotImplementedError

    def _timed(self, ctx: StageContext, fn) -> StageResult:
        start = time.time()
        try:
            result = fn()
        except Exception as exc:
            cap = self.capability(ctx.config)
            result = StageResult(capability_id=cap.capability_id, extractor=self.name,
                                 errors=[StageError(self.name, cap.capability_id,
                                                    "non-retryable", str(exc), type(exc).__name__)],
                                 warnings=[f"{self.name} failed: {exc}"])
            logger.warning("%s failed: %s", self.name, exc)
        result.duration_ms = round((time.time() - start) * 1000, 2)
        if not result.capability_id:
            result.capability_id = self.capability(ctx.config).capability_id
        if not result.extractor:
            result.extractor = self.name
        return result


def parse_tesseract_tsv(tsv: str, word_conf_min: float = WORD_CONF_MIN) -> "tuple[str, float, int]":
    """Parse tesseract TSV; drop conf<word_conf_min or -1. Returns (text, mean_conf, kept)."""
    words: List[str] = []
    confs: List[float] = []
    for i, line in enumerate((tsv or "").splitlines()):
        if i == 0 and line.startswith("level"):
            continue
        parts = line.split("\t")
        if len(parts) < 12:
            continue
        try:
            conf = float(parts[10])
        except ValueError:
            continue
        text = parts[11].strip()
        if not text or conf < 0:
            continue
        c = conf / 100.0
        if c < word_conf_min:
            continue
        words.append(text)
        confs.append(c)
    if not words:
        return "", 0.0, 0
    weighted = sum(c * len(w) for c, w in zip(confs, words)) / max(1, sum(len(w) for w in words))
    return " ".join(words), round(weighted, 4), len(words)


def doc_confidence(mean_conf: Optional[float], text: str, pages: int = 1) -> StageConfidence:
    chars = len(text or "")
    coverage = min(1.0, chars / 200.0) if pages <= 1 else min(1.0, (chars / pages) / 200.0)
    text_conf = round(coverage, 4) if text else 0.0
    return StageConfidence(text_conf=text_conf, ocr_mean_conf=mean_conf,
                           per_stage={"coverage": round(coverage, 4)})


def normalize_text_for_dedup(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()
