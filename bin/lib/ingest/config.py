#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Configurable pipeline loading (YAML / JSON / INI).

YAML is used when PyYAML is installed; JSON and INI (ConfigLoader style)
always work with the standard library, so no new hard dependency is added.
"""
from __future__ import annotations

import configparser
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ingest.config")

DEFAULT_STAGES = ["detect_mimetype", "dedup_exact", "extract_doc", "normalize",
                  "validate", "dedup_fuzzy", "provenance_commit", "emit_legacy"]


@dataclass
class StageConfig:
    name: str
    enabled: bool = True
    max_retries: int = 3
    penalty_seconds: float = 30.0
    queue_count_limit: int = 10_000
    queue_bytes_limit: int = 1_073_741_824


@dataclass
class DocProcConfig:
    """Document/multimedia processing config (design §10)."""
    ocr_engine_order: List[str] = field(default_factory=lambda: ["ocrmypdf", "tesseract", "paddle", "easyocr"])
    ocr_accept: float = 0.85
    ocr_reprocess_below: float = 0.60
    tesseract_psm: int = 6
    word_conf_min: float = 0.60
    code_decoders: List[str] = field(default_factory=lambda: ["zxingcpp", "cv2", "pyzbar", "qreader"])
    minhash_enabled: bool = False
    lingua_enabled: bool = False
    translation_provider: str = "libretranslate"
    max_embedded: int = 50
    max_bytes: int = 20_000_000

    def to_processor_config(self) -> Dict[str, Any]:
        return {"ocr_engine_order": list(self.ocr_engine_order),
                "ocr_accept": self.ocr_accept,
                "ocr_reprocess_below": self.ocr_reprocess_below,
                "tesseract_psm": self.tesseract_psm,
                "word_conf_min": self.word_conf_min,
                "code_decoders": list(self.code_decoders),
                "minhash_enabled": self.minhash_enabled,
                "lingua_enabled": self.lingua_enabled,
                "translation_provider": self.translation_provider,
                "max_embedded": self.max_embedded, "max_bytes": self.max_bytes}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DocProcConfig":
        cfg = cls()
        for k in cfg.__dataclass_fields__:  # type: ignore[attr-defined]
            if k in data:
                v = data[k]
                if k in ("ocr_engine_order", "code_decoders") and isinstance(v, str):
                    v = [s.strip() for s in v.split(",") if s.strip()]
                elif k in ("ocr_accept", "ocr_reprocess_below", "word_conf_min"):
                    v = float(v)
                elif k in ("tesseract_psm", "max_embedded", "max_bytes"):
                    v = int(v)
                elif k in ("minhash_enabled", "lingua_enabled"):
                    v = bool(v) if not isinstance(v, str) else v.lower() in ("1", "true", "yes")
                setattr(cfg, k, v)
        return cfg


@dataclass
class PipelineConfig:
    source_id: str = "default"
    stages: List[StageConfig] = field(default_factory=list)
    tika_url: Optional[str] = None
    gate_enabled: bool = True
    docproc: DocProcConfig = field(default_factory=DocProcConfig)
    raw: Dict[str, Any] = field(default_factory=dict)


def default_pipeline(source_id: str = "default") -> PipelineConfig:
    return PipelineConfig(source_id=source_id,
                          stages=[StageConfig(name=s) for s in DEFAULT_STAGES])


def _from_dict(data: Dict[str, Any], source_id: str) -> PipelineConfig:
    stages: List[StageConfig] = []
    for item in data.get("stages", DEFAULT_STAGES):
        if isinstance(item, str):
            stages.append(StageConfig(name=item))
        else:
            stages.append(StageConfig(
                name=item.get("name", "normalize"),
                enabled=bool(item.get("enabled", True)),
                max_retries=int(item.get("max_retries", 3)),
                penalty_seconds=float(item.get("penalty_seconds", 30.0)),
                queue_count_limit=int(item.get("queue_count_limit", 10_000)),
                queue_bytes_limit=int(item.get("queue_bytes_limit", 1_073_741_824))))
    docproc = DocProcConfig.from_dict(data.get("docproc") or {})
    return PipelineConfig(source_id=data.get("source_id", source_id), stages=stages,
                          tika_url=data.get("tika_url"), gate_enabled=bool(data.get("gate_enabled", True)),
                          docproc=docproc, raw=data)


def load_pipeline_config(path: Optional[str] = None, source_id: str = "default") -> PipelineConfig:
    """Load ``configs/ingest.cfg`` (INI) / ``.json`` / ``.yaml``; default when absent."""
    if not path:
        candidates = []
        home = os.environ.get("AIL_HOME")
        if home:
            candidates += [os.path.join(home, "configs", n)
                           for n in ("ingest.cfg", "ingest.json", "ingest.yaml", "ingest.yml")]
        candidates += ["configs/ingest.cfg", "configs/ingest.json"]
        path = next((c for c in candidates if os.path.exists(c)), None)
    if not path or not os.path.exists(path):
        logger.info("no ingest config found; using defaults for %s", source_id)
        return default_pipeline(source_id)
    logger.info("loading ingest config %s", path)
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml  # type: ignore
            with open(path, encoding="utf-8") as fh:
                return _from_dict(yaml.safe_load(fh) or {}, source_id)
        except ImportError:
            logger.warning("PyYAML absent; cannot read %s, using defaults", path)
            return default_pipeline(source_id)
    if path.endswith(".json"):
        with open(path, encoding="utf-8") as fh:
            return _from_dict(json.load(fh), source_id)
    parser = configparser.ConfigParser()
    parser.read(path)
    section = source_id if parser.has_section(source_id) else (
        "ingest" if parser.has_section("ingest") else parser.sections()[0] if parser.sections() else None)
    if section is None:
        return default_pipeline(source_id)
    names = [s.strip() for s in parser.get(section, "stages", fallback=",".join(DEFAULT_STAGES)).split(",") if s.strip()]
    docproc_data: Dict[str, Any] = {}
    if parser.has_section("docproc"):
        docproc_data = dict(parser.items("docproc"))
    return PipelineConfig(
        source_id=section,
        stages=[StageConfig(name=n) for n in names],
        tika_url=parser.get(section, "tika_url", fallback=None) or None,
        gate_enabled=parser.getboolean(section, "gate_enabled", fallback=True),
        docproc=DocProcConfig.from_dict(docproc_data))
