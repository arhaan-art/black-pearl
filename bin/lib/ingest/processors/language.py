#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Language detection stage + translation provider interface (design §6)."""
from __future__ import annotations

import logging
from typing import List, Optional, Protocol, Tuple

from .base import BaseProcessor, StageContext, StageResult
from .metadata import normalize_bcp47_tag

logger = logging.getLogger("ingest.processors.language")

try:
    import lingua  # type: ignore
    HAS_LINGUA = True
except Exception:
    lingua = None
    HAS_LINGUA = False


def detect_language_stub(text: str) -> List[Tuple[str, float]]:
    """Offline heuristic fallback when gcld3/picolang/lingua are absent.

    Only used so the pipeline degrades instead of failing; production uses
    LanguagesDetector (gcld3>=150ch / picolang<150ch) when importable.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        from lib.Language import LanguagesDetector  # type: ignore
        det = LanguagesDetector()
        out = []
        for lang, conf in (det.detect(text) or []):
            norm = normalize_bcp47_tag(str(lang))
            if norm:
                out.append((norm, float(conf)))
        if out:
            return out
    except Exception:
        pass
    if HAS_LINGUA and text:
        try:
            from lingua import LanguageDetectorBuilder  # type: ignore
            detector = LanguageDetectorBuilder.from_all_languages().build()
            vals = detector.compute_language_confidence_values(text[:2000])
            return [(normalize_bcp47_tag(v.language.iso_code_639_1.name.lower()) or "en",
                     float(v.value)) for v in vals[:3] if v.value > 0]
        except Exception:
            pass
    return [("en", 0.5)] if text.strip() else []


class TranslationProvider(Protocol):
    name: str
    def available(self) -> bool: ...
    def languages(self) -> List[str]: ...
    def translate(self, text: str, source: str, target: str) -> Optional[str]: ...


class NullProvider:
    """Offline degrade-not-fail provider: stores source text, tags unavailable."""
    name = "null"

    def available(self) -> bool:
        return True

    def languages(self) -> List[str]:
        return []

    def translate(self, text, source, target):
        return None


class ArgosOfflineProvider:
    name = "argos"

    def available(self) -> bool:
        try:
            import argostranslate  # noqa: F401
            return True
        except Exception:
            return False

    def languages(self) -> List[str]:
        try:
            from argostranslate import translate as _t
            return [l.code for l in _t.get_installed_languages()]
        except Exception:
            return []

    def translate(self, text, source, target):
        try:
            from argostranslate import translate as _t
            s = normalize_bcp47_tag(source)
            t = normalize_bcp47_tag(target)
            if not s or not t:
                return None
            return _t.translate(text, s.split("-")[0], t.split("-")[0])
        except Exception as exc:
            logger.warning("argos translate failed: %s", exc)
            return None


class LibreTranslateProvider:
    name = "libretranslate"

    def available(self) -> bool:
        try:
            import sys, os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            from lib.Language import LanguageTranslator  # noqa: F401
            return True
        except Exception:
            return False

    def languages(self) -> List[str]:
        try:
            import sys, os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            from lib.Language import LanguageTranslator
            tr = LanguageTranslator()
            langs = tr.get_translation_languages() if hasattr(tr, "get_translation_languages") else []
            out = []
            for l in (langs or []):
                code = l.get("code") if isinstance(l, dict) else str(l)
                norm = normalize_bcp47_tag(code)
                if norm:
                    out.append(norm)
            return out
        except Exception:
            return []

    def translate(self, text, source, target):
        try:
            import sys, os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            from lib.Language import LanguageTranslator, normalize_bcp47_tag as _n, convert_iso3_code
            s, t = _n(source), _n(target)
            if not s or not t:
                return None
            tr = LanguageTranslator()
            fn = getattr(tr, "translate", None)
            if fn is None:
                return None
            return fn(text, convert_iso3_code(s.split("-")[0]), convert_iso3_code(t.split("-")[0]))
        except Exception as exc:
            logger.warning("libretranslate failed: %s", exc)
            return None


def get_translation_provider(name: str = "libretranslate"):
    if name == "argos":
        p = ArgosOfflineProvider()
        return p if p.available() else NullProvider()
    if name == "null":
        return NullProvider()
    p = LibreTranslateProvider()
    return p if p.available() else NullProvider()


class LanguageStage(BaseProcessor):
    name = "language"
    priority = 50
    prefixes = ("text/", "image/", "application/")

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            text = ctx.config.get("_text", "")
            if not text:
                return StageResult(warnings=["language: no text"])
            langs = detect_language_stub(text)
            meta = {}
            conf = None
            if langs:
                meta["dc:language"] = langs[0][0]
                conf = float(langs[0][1])
            from .base import StageConfidence
            return StageResult(metadata=meta,
                               confidence=StageConfidence(lang_conf=conf))
        return self._timed(ctx, _run)


class TranslationStage(BaseProcessor):
    name = "translation"
    priority = 60

    def __init__(self, provider=None) -> None:
        self.provider = provider or NullProvider()

    def translate_text(self, text: str, source: str, target: str) -> Optional[str]:
        if not self.provider.available():
            return None
        s, t = normalize_bcp47_tag(source), normalize_bcp47_tag(target)
        if not s or not t:
            return None
        langs = self.provider.languages()
        if langs and (s not in langs or t not in langs):
            return None
        return self.provider.translate(text, s, t)

    def process(self, ctx: StageContext) -> StageResult:
        def _run() -> StageResult:
            text = ctx.config.get("_text", "")
            target = ctx.config.get("translation_target", "")
            source = ctx.config.get("translation_source", "auto")
            if not text or not target:
                return StageResult(warnings=["translation: missing text/target"])
            out = self.translate_text(text, source if source != "auto" else "en", target)
            if out is None:
                return StageResult(text=text, warnings=["translation:unavailable"])
            return StageResult(text=out)
        return self._timed(ctx, _run)
