#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Metadata normalization to the canonical doc field set (design §5)."""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("ingest.processors.metadata")

# BCP47 normalizer is reused from lib.Language when available; a strict
# fallback keeps the ingest package importable standalone (tests).
try:
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    from lib.Language import normalize_bcp47_tag  # type: ignore
except Exception:
    def normalize_bcp47_tag(tag):  # type: ignore
        if not tag or not isinstance(tag, str):
            return None
        parts = tag.strip().replace("_", "-").split("-")
        primary = parts[0].lower()
        if not re.fullmatch(r"[a-z]{2,3}", primary):
            return None
        out = [primary]
        for p in parts[1:]:
            if re.fullmatch(r"[A-Za-z]{4}", p):
                out.append(p.title())
            elif re.fullmatch(r"([A-Za-z]{2}|[0-9]{3})", p):
                out.append(p.upper())
            else:
                return None
        return "-".join(out)


def _pdf_date_to_iso(raw: str) -> str:
    raw = (raw or "").strip()
    m = re.match(r"D:(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?", raw)
    if m:
        y, mo, d, h, mi, s = (g or "01" for g in m.groups())
        try:
            return datetime(int(y), int(mo), int(d), int(h), int(mi), int(s)).isoformat()
        except ValueError:
            return raw
    m = re.match(r"(\d{4}):(\d{2}):(\d{2}) (\d{2}):(\d{2}):(\d{2})", raw)  # EXIF
    if m:
        try:
            return datetime(*[int(g) for g in m.groups()]).isoformat()
        except ValueError:
            return raw
    return raw


def _gps_decimal(gps: dict) -> Tuple[Optional[str], Optional[str]]:
    def dec(value, ref) -> Optional[float]:
        try:
            parts = value if isinstance(value, (list, tuple)) else [value]
            d, m, s = (float(parts[i]) if i < len(parts) else 0.0 for i in range(3))
            v = d + m / 60 + s / 3600
            if ref in ("S", "W"):
                v = -v
            return round(v, 6)
        except Exception:
            return None
    try:
        lat = dec(gps.get(2), gps.get(1))
        lon = dec(gps.get(4), gps.get(3))
        return (str(lat) if lat is not None else None, str(lon) if lon is not None else None)
    except Exception:
        return None, None


_TIKA_MAP = {
    "dc:title": "dc:title", "title": "dc:title", "dc:creator": "dc:creator",
    "Author": "dc:creator", "author": "pdf.author", "dc:date": "dc:date",
    "dcterms:created": "dc:date", "CreationDate": "dc:date",
    "Content-Type": "mimetype", "resourceName": "file.name",
    "dc:publisher": "dc:publisher", "publisher": "dc:publisher",
    "producer": "pdf.producer", "Producer": "pdf.producer",
}


class MetadataNormalizer:
    """Map Tika / fitz / PIL-EXIF metadata onto canonical fields (all str)."""

    def normalize(self, raw: Dict, mimetype: str = "", filename: str = "",
                  size_bytes: int = 0) -> Tuple[Dict[str, str], List[str]]:
        out: Dict[str, str] = {}
        warnings: List[str] = []
        leftover: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            if value is None or value == "":
                continue
            skey, sval = str(key), str(value)
            canon = _TIKA_MAP.get(skey)
            if canon:
                if canon == "dc:date":
                    sval = _pdf_date_to_iso(sval)
                out[canon] = sval
            elif skey.startswith(("dc:", "file.", "doc.", "img.", "exif.", "pdf.", "archive.", "meta.raw:")):
                out[skey] = sval
            else:
                leftover[skey] = sval
        if mimetype:
            out["mimetype"] = mimetype  # detected wins over declared
        if filename:
            out.setdefault("file.name", filename)
        if size_bytes:
            out["file.size_bytes"] = str(size_bytes)
        if "dc:language" in out:
            norm = normalize_bcp47_tag(out["dc:language"])
            if norm:
                out["dc:language"] = norm
            else:
                warnings.append(f"invalid dc:language dropped: {out.pop('dc:language')}")
        for k, v in leftover.items():
            out[f"meta.raw:{k}"] = v
        # author mirror
        if out.get("dc:creator") and "pdf.author" not in out and mimetype == "application/pdf":
            out["pdf.author"] = out["dc:creator"]
        return out, warnings

    def completeness(self, meta: Dict[str, str], expected: Optional[List[str]] = None) -> float:
        exp = expected or ["dc:title", "mimetype", "file.name"]
        if not exp:
            return 0.0
        return round(sum(1 for k in exp if meta.get(k)) / len(exp), 4)


def extract_exif(content: bytes) -> Tuple[Dict[str, str], List[str]]:
    warnings: List[str] = []
    try:
        import io
        from PIL import Image, ExifTags
        img = Image.open(io.BytesIO(content))
        out: Dict[str, str] = {"img.width": str(img.width), "img.height": str(img.height),
                               "img.color_space": str(img.mode)}
        exif = img.getexif()
        if exif:
            for tag_id, val in exif.items():
                name = ExifTags.TAGS.get(tag_id, str(tag_id))
                if name == "Make":
                    out["exif.make"] = str(val)
                elif name == "Model":
                    out["exif.model"] = str(val)
                elif name == "DateTimeOriginal":
                    out["dc:date"] = _pdf_date_to_iso(str(val))
            gps_ifd = None
            try:
                gps_ifd = exif.get_ifd(0x8825)  # GPS IFD 34853
            except Exception:
                gps_ifd = exif.get(34853)  # type: ignore
            if gps_ifd:
                raw_gps = dict(gps_ifd) if not isinstance(gps_ifd, dict) else gps_ifd
                # PIL GPS dict keys may be ints 1..4
                norm = {int(k): v for k, v in raw_gps.items() if str(k).isdigit() or isinstance(k, int)}
                lat, lon = _gps_decimal(norm or raw_gps)
                if lat:
                    out["exif.gps_lat"] = lat
                if lon:
                    out["exif.gps_lon"] = lon
        return out, warnings
    except Exception as exc:
        return {}, [f"exif failed: {exc}"]
