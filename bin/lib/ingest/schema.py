#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Canonical field registry and allowlists for strict validation.

Mirrors the hand-rolled allowlist style of the existing codebase
(``ail_objects.is_valid_object_type``) instead of adding a schema library.
Object types are read from ``ail_core.AIL_OBJECTS`` when available and
fall back to a vendored copy so the ingest package stays importable and
testable without Redis/Kvrocks.
"""
from __future__ import annotations

from typing import Dict, FrozenSet

try:  # live repo values when AIL is importable
    from lib.ail_core import AIL_OBJECTS as _AIL_OBJECTS  # type: ignore
    OBJECT_TYPES: FrozenSet[str] = frozenset(_AIL_OBJECTS.keys())
except Exception:  # pragma: no cover - import fallback for standalone use
    OBJECT_TYPES = frozenset({
        "author", "barcode", "chat", "chat-subchannel", "chat-thread",
        "cookie-name", "cve", "cryptocurrency", "decoded", "domain",
        "dom-hash", "etag", "favicon", "file-name", "forum", "forum-thread",
        "gtracker", "hhhash", "ip", "item", "image", "mail", "message",
        "ocr", "pdf", "pgp", "post", "qrcode", "screenshot", "ssh-key",
        "subforum", "title", "user-account", "username",
    })

# MISP-style category/type allowlists (subset used by the gate).
MISP_CATEGORIES: FrozenSet[str] = frozenset({
    "network-activity", "payload-delivery", "payload-installation",
    "persistence-mechanism", "social-network", "targeting-data",
    "text", "file", "financial-fraud", "support-tool", "other",
})
MISP_TYPES: FrozenSet[str] = frozenset({
    "ip-dst", "ip-src", "domain", "hostname", "url", "email", "email-src",
    "email-dst", "text", "filename", "md5", "sha1", "sha256", "sha512",
    "ssdeep", "user-agent", "datetime", "other",
})

# Dublin-Core-ish canonical document fields (IntelMQ harmonization pattern).
CANONICAL_DOC_FIELDS: Dict[str, str] = {
    "dc:title": "string",
    "dc:creator": "string",
    "dc:date": "string",
    "dc:language": "string",
    "mimetype": "string",
    "source_url": "string",
}

# OTel-style dotted attribute names the gate accepts without warning.
KNOWN_ATTRIBUTE_PREFIXES = (
    "service.", "telemetry.sdk.", "host.", "http.", "db.", "dc:",
    "source.", "ingest.", "crawler.", "chat.", "file.", "feed.",
)

TEXT_MAX_SIZE = 1_000_000
FILE_MAX_SIZE = 1_000_000_000

SUPPORTED_SCHEMA_VERSIONS = ("1.0.0",)
