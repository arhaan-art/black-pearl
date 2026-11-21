#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Strict schema validation. A failing envelope never reaches Mixer/Global."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List

from .envelope import IngestEnvelope
from .schema import (
    FILE_MAX_SIZE, MISP_CATEGORIES, MISP_TYPES, OBJECT_TYPES,
    SUPPORTED_SCHEMA_VERSIONS, TEXT_MAX_SIZE,
)

logger = logging.getLogger("ingest.validate")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_TRACE_RE = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$")


@dataclass
class ValidationResult:
    ok: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    retryable: bool = False  # validation failures are never retryable


def sanitize_text(text: str) -> str:
    """Strip dangerous URI schemes, formula cells and control chars."""
    if not text:
        return text
    text = re.sub(r"(?i)javascript\s*:", "javascript-blocked:", text)
    text = re.sub(r"(?i)data\s*:\s*text/html", "data-blocked:text/html", text)
    out = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("=", "+", "@")) and "|" in stripped:
            line = "'" + line
        out.append(line)
    cleaned = "\n".join(out)
    return "".join(ch for ch in cleaned if ch == "\n" or ch == "\t" or ord(ch) >= 32)


def validate_envelope(env: IngestEnvelope) -> ValidationResult:
    errors: List[str] = []
    warnings: List[str] = []

    if env.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        errors.append(f"unsupported schema_version: {env.schema_version}")
    if not _UUID_RE.match(env.envelope_uuid or ""):
        errors.append("envelope_uuid must be an RFC4122 UUID")
    if not env.source_id:
        errors.append("source_id is required")
    if not env.resource.get("service.name"):
        errors.append("resource.service.name is required (immutable source identity)")
    if not env.resource.get("source.id"):
        errors.append("resource.source.id is required")

    cr = env.content_ref
    if not cr or not _SHA256_RE.match(cr.sha256 or ""):
        errors.append("content_ref.sha256 must be 64 lowercase hex chars")
    if cr and cr.size_bytes < 0:
        errors.append("content_ref.size_bytes must be >= 0")
    if cr and cr.size_bytes > FILE_MAX_SIZE:
        errors.append(f"content exceeds FILE_MAX_SIZE ({FILE_MAX_SIZE})")
    if env.content_text and len(env.content_text.encode("utf-8", "ignore")) > TEXT_MAX_SIZE:
        warnings.append("content_text exceeds TEXT_MAX_SIZE; downstream truncation may apply")

    if env.traceparent and not _TRACE_RE.match(env.traceparent):
        errors.append("traceparent must be W3C '00-<32hex>-<16hex>-01'")

    for key, value in (env.attributes or {}).items():
        if not isinstance(key, str) or not key:
            errors.append(f"attribute key must be a non-empty string: {key!r}")
            continue
        if not isinstance(value, (str, bool, int, float, list, dict, type(None))):
            errors.append(f"attribute {key!r} has unsupported type {type(value).__name__}")
        if key == "ail.object_type" and value not in OBJECT_TYPES:
            errors.append(f"unknown ail.object_type: {value!r}")
        if key == "misp.category" and value not in MISP_CATEGORIES:
            errors.append(f"unknown misp.category: {value!r}")
        if key == "misp.type" and value not in MISP_TYPES:
            errors.append(f"unknown misp.type: {value!r}")

    # mimetype mismatch: declared vs detected must agree when both present
    declared = (env.attributes or {}).get("mimetype")
    if declared and cr and cr.mimetype_detected and declared != cr.mimetype_detected:
        warnings.append(f"mimetype mismatch declared={declared} detected={cr.mimetype_detected}")

    result = ValidationResult(ok=not errors, errors=errors, warnings=warnings)
    if not result.ok:
        logger.warning("validation failed uuid=%s errors=%s", env.envelope_uuid, errors)
    return result
