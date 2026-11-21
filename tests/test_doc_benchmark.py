#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ground-truth benchmark + failure/regression tests for docproc.

Corpus ground truth lives in tests/docproc_corpus.py (generated from
literals, independent of pipeline output). OCR engines are unavailable
on this host (no Tesseract), so rendered-text OCR is asserted as a
graceful-degradation case, not as accuracy.
"""
import io
import os
import sys
import unittest
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
sys.path.insert(0, os.path.dirname(__file__))

from docproc_corpus import DEDUP_PAIRS, build_corpus, cer, norm, wer  # noqa: E402
from lib.ingest.processors import run_doc_pipeline  # noqa: E402
from lib.ingest.processors.base import StageConfidence, StageContext, doc_confidence  # noqa: E402
from lib.ingest.processors.dedup_text import HAS_DATASKETCH, MinHashDedup  # noqa: E402
from lib.ingest.processors.image import CodeProcessor, OcrProcessor  # noqa: E402
from lib.ingest.processors.language import (  # noqa: E402
    NullProvider, TranslationStage, detect_language_stub, get_translation_provider)
from lib.ingest.processors.metadata import MetadataNormalizer  # noqa: E402
from lib.ingest.processors.registry_store import ProcessedDocRegistry, RegistryEntry  # noqa: E402

CORPUS = build_corpus()
BY_NAME = {i.name: i for i in CORPUS}


def _run(item):
    return run_doc_pipeline(item.data, item.mimetype, item.filename)


class _Stub:
    def __init__(self, name, text, conf):
        self.name, self._t, self._c = name, text, conf
    def available(self):
        return True
    def ocr_image(self, content, languages=None):
        r = type("R", (), {})()
        r.text, r.mean_conf, r.warnings = self._t, self._c, []
        return r


class TestCorpusIntegrity(unittest.TestCase):
    def test_corpus_has_known_gt(self):
        self.assertGreaterEqual(len(CORPUS), 15)
        for it in CORPUS:
            self.assertTrue(it.ground_truth or it.qr_payloads, it.name)
            self.assertTrue(it.data, it.name)

    def test_metrics_sanity(self):
        self.assertEqual(cer("abc", "abc"), 0.0)
        self.assertEqual(cer("abc", "abd"), round(1 / 3, 4))
        self.assertEqual(wer("a b c", "a x c"), round(1 / 3, 4))
        self.assertEqual(cer("", ""), 0.0)


class TestExtractionAccuracy(unittest.TestCase):
    def test_born_digital_pdf_low_cer(self):
        for name in ("pdf-single", "pdf-multi", "pdf-numeric"):
            it, r = BY_NAME[name], _run(BY_NAME[name])
            self.assertLessEqual(cer(it.ground_truth, r.text), 0.05, f"{name}: {r.text!r}")
            self.assertLessEqual(wer(it.ground_truth, r.text), 0.2, name)

    def test_text_exact(self):
        for name in ("text-plain", "text-french", "text-long"):
            it = BY_NAME[name]
            self.assertEqual(norm(_run(it).text), norm(it.ground_truth), name)

    def test_office_and_archive_contain_gt(self):
        for name in ("docx-1", "xlsx-1", "zip-1", "targz-1", "gzip-1", "eml-1"):
            it = BY_NAME[name]
            self.assertIn(norm(it.ground_truth.split("\n")[0]), norm(_run(it).text), name)

    def test_pdf_metadata_ground_truth(self):
        r = _run(BY_NAME["pdf-single"])
        self.assertEqual(r.metadata.get("dc:title"), "Fox Doc")
        self.assertEqual(r.metadata.get("pdf.author"), "Jane Doe")
        self.assertEqual(r.metadata.get("doc.page_count"), "1")
        # regression: re-normalisation must not double-prefix meta.raw:*
        self.assertFalse(any(k.startswith("meta.raw:meta.raw:") for k in r.metadata), r.metadata)

    def test_nested_archive_no_binary_pollution(self):
        # regression: inner.zip bytes decoded with errors=ignore polluted text
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, "w") as z:
            z.writestr("deep.txt", "deep nested text")
        outer = io.BytesIO()
        with zipfile.ZipFile(outer, "w") as z:
            z.writestr("inner.zip", inner.getvalue())
            z.writestr("top.txt", "top level text")
        r = run_doc_pipeline(outer.getvalue(), "application/zip", "outer.zip")
        self.assertIn("deep nested text", r.text)
        self.assertIn("top level text", r.text)
        self.assertNotIn("PK", r.text)


class TestQRBenchmark(unittest.TestCase):
    def test_all_qr_payloads_exact(self):
        qr_items = [i for i in CORPUS if i.qr_payloads]
        self.assertEqual(len(qr_items), 5)
        for it in qr_items:
            codes = CodeProcessor().extract_codes(it.data)
            self.assertEqual([c.content for c in codes], it.qr_payloads, it.name)
            self.assertIn(codes[0].decoder, ("zxingcpp", "cv2", "pyzbar"))
            # pipeline surface agrees
            self.assertEqual([c.content for c in _run(it).codes], it.qr_payloads, it.name)

    def test_blank_image_no_codes_no_crash(self):
        from PIL import Image
        buf = io.BytesIO(); Image.new("RGB", (50, 50), "white").save(buf, format="PNG")
        self.assertEqual(CodeProcessor().extract_codes(buf.getvalue()), [])
        self.assertEqual(CodeProcessor().extract_codes(b"not an image"), [])
        # regression: PDF bytes are not an image-decoder error
        self.assertEqual(CodeProcessor().extract_codes(BY_NAME["pdf-single"].data), [])


class TestFailureModes(unittest.TestCase):
    def test_corrupt_pdf_tracks_stage_error(self):
        r = run_doc_pipeline(b"not a pdf at all", "application/pdf", "bad.pdf")
        self.assertEqual(r.text, "")
        self.assertTrue(any(e.stage == "pdf-native" for e in r.errors), r.errors)
        self.assertTrue(any("pdf-native failed" in w for w in r.warnings))

    def test_corrupt_zip_and_docx_warn_not_raise(self):
        r = run_doc_pipeline(b"PK\x03\x04corrupt", "application/zip", "bad.zip")
        self.assertEqual(r.text, ""); self.assertTrue(r.warnings)
        r2 = run_doc_pipeline(b"not zip", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "bad.docx")
        self.assertEqual(r2.text, ""); self.assertTrue(any("office" in w for w in r2.warnings))

    def test_unsupported_and_empty_degrade(self):
        r = run_doc_pipeline(b"\x00\x01\x02random", "application/octet-stream", "x.bin")
        self.assertEqual(r.text, ""); self.assertEqual(r.metadata.get("file.name"), "x.bin")
        self.assertEqual(run_doc_pipeline(b"", "text/plain", "e.txt").text, "")
        r3 = run_doc_pipeline(b"\xff\xd8\xff corrupt jpeg", "image/jpeg", "bad.jpg")
        self.assertEqual(r3.text, "")  # no crash; OCR/code stages degrade

    def test_rendered_text_image_ocr_unavailable_degrades(self):
        # No Tesseract/Paddle/EasyOCR on this host: expect empty text +
        # explicit warning + reprocess band, never a crash or fake text.
        r = _run(BY_NAME["image-rendered-text"])
        self.assertEqual(r.text, "")
        self.assertTrue(any("ocr" in w for w in r.warnings), r.warnings)
        self.assertEqual(r.confidence.band(), "reprocess")


class TestOcrCascade(unittest.TestCase):
    def _ctx(self):
        return StageContext(content=b"x", mimetype="image/png")

    def test_low_conf_reprocesses_to_next_engine(self):
        r = OcrProcessor(engines=[_Stub("s1", "bad", 0.4), _Stub("s2", "good text", 0.92)]).process(self._ctx())
        self.assertEqual(r.text, "good text")
        self.assertEqual(r.confidence.band(), "accept")

    def test_all_low_best_kept_with_warning(self):
        r = OcrProcessor(engines=[_Stub("s1", "first", 0.3), _Stub("s2", "second", 0.5),
                                  _Stub("s3", "third", 0.2)]).process(self._ctx())
        self.assertEqual(r.text, "second")
        self.assertTrue(any("low-confidence" in w for w in r.warnings))
        self.assertEqual(r.confidence.band(), "reprocess")

    def test_accept_low_band(self):
        r = OcrProcessor(engines=[_Stub("s1", "mid text", 0.7)]).process(self._ctx())
        self.assertEqual(r.confidence.band(), "accept-low")
        self.assertTrue(any("low-confidence" in w for w in r.warnings))

    def test_max_three_attempts(self):
        engines = [_Stub(f"s{i}", f"t{i}", 0.1) for i in range(5)]
        r = OcrProcessor(engines=engines).process(self._ctx())
        self.assertIn(r.text, ("t0", "t1", "t2"))


class TestConfidenceAndMetadata(unittest.TestCase):
    def test_band_boundaries(self):
        for conf, band in [(0.85, "accept"), (0.84, "accept-low"), (0.60, "accept-low"), (0.59, "reprocess")]:
            self.assertEqual(StageConfidence(ocr_mean_conf=conf).band(), band, conf)

    def test_doc_confidence_coverage(self):
        self.assertEqual(doc_confidence(1.0, "").text_conf, 0.0)
        self.assertGreater(doc_confidence(1.0, "x" * 300).text_conf, 0.9)

    def test_metadata_date_and_invalid_language(self):
        meta, warns = MetadataNormalizer().normalize(
            {"CreationDate": "D:20200102030405", "dc:language": "bogus!!", "Title": "T"},
            "application/pdf", "f.pdf", 5)
        self.assertTrue(meta["dc:date"].startswith("2020-01-02"))
        self.assertNotIn("dc:language", meta); self.assertTrue(warns)
        meta2, _ = MetadataNormalizer().normalize({"dc:language": "EN_us"}, "text/plain", "f", 1)
        self.assertEqual(meta2.get("dc:language"), "en-US")

    def test_exif_image_metadata(self):
        from PIL import Image
        img = Image.new("RGB", (20, 10)); exif = img.getexif()
        exif[271] = "TestMake"; exif[272] = "TestModel"
        buf = io.BytesIO(); img.save(buf, format="JPEG", exif=exif)
        r = run_doc_pipeline(buf.getvalue(), "image/jpeg", "p.jpg")
        self.assertEqual(r.metadata.get("exif.make"), "TestMake")
        self.assertEqual(r.metadata.get("img.width"), "20")


class TestLanguageTranslation(unittest.TestCase):
    def test_language_fallback_never_crashes(self):
        out = detect_language_stub("Hello world this is an English benchmark sentence")
        self.assertTrue(out); self.assertEqual(out[0][0], "en")  # stub fallback on this host
        self.assertEqual(detect_language_stub(""), [])

    def test_translation_fallback_degrades(self):
        self.assertIsInstance(get_translation_provider("libretranslate"), NullProvider)
        stage = TranslationStage(provider=NullProvider())
        self.assertIsNone(stage.translate_text("hello", "en", "fr"))
        self.assertIsNone(stage.translate_text("hello", "not-a-lang!!", "fr"))
        r = stage.process(StageContext(content=b"", config={"_text": "hello", "translation_target": "fr"}))
        self.assertEqual(r.text, "hello")
        self.assertTrue(any("translation:unavailable" in w for w in r.warnings))


@unittest.skipUnless(HAS_DATASKETCH, "datasketch absent")
class TestDedupGroundTruth(unittest.TestCase):
    def test_pairs(self):
        from docproc_corpus import DEDUP_BASE
        d = MinHashDedup(); d.insert("base", DEDUP_BASE)
        for label, _b, variant, expected in DEDUP_PAIRS:
            if label == "exact":
                continue  # self-match trivially true; covered by query below
            self.assertEqual(bool(d.query(variant)), expected, label)
        self.assertEqual(d.query(DEDUP_BASE)[0][0], "base")
        self.assertTrue(d.is_near_duplicate(DEDUP_BASE.replace("forest", "woods", 1)))


class TestHistoricalReprocess(unittest.TestCase):
    def test_stale_reasons_and_queue(self):
        reg = ProcessedDocRegistry()
        self.assertEqual(reg.stale_reason("a" * 64, ["x@1#aa"]), "never-processed")
        reg.record(RegistryEntry(sha256="a" * 64, capability_ids=["x@1#aa"], overall_confidence=0.9))
        self.assertTrue(reg.is_current("a" * 64, ["x@1#aa"]))
        self.assertEqual(reg.stale_reason("a" * 64, ["x@2#bb"]), "capability-changed")
        reg.record(RegistryEntry(sha256="b" * 64, capability_ids=["x@1#aa"], overall_confidence=0.4))
        self.assertEqual(reg.stale_reason("b" * 64, ["x@1#aa"]), "low-confidence")
        self.assertFalse(reg.is_current("b" * 64, ["x@1#aa"]))
        reg.enqueue_reprocess("a" * 64, "capability-changed")
        reg.enqueue_reprocess("b" * 64, "low-confidence")
        self.assertEqual(reg.reprocess_depth(), 2)
        self.assertEqual(reg.dequeue_reprocess()["reason"], "capability-changed")
        self.assertFalse(reg.should_overwrite("a" * 64, 0.5))
        self.assertTrue(reg.should_overwrite("a" * 64, 0.9))

    def test_entry_roundtrip(self):
        e = RegistryEntry.from_dict({"sha256": "c" * 64, "capability_ids": '["x@1#aa"]', "overall_confidence": "0.7"})
        self.assertEqual(e.capability_ids, ["x@1#aa"]); self.assertEqual(e.overall_confidence, 0.7)


class TestBenchmarkSmoke(unittest.TestCase):
    def test_benchmark_metrics_and_throughput(self):
        from benchmark_docproc import run_benchmark
        res = run_benchmark(repeats=1)
        self.assertEqual(res["qr"], {"total": 5, "exact_ok": 5, "accuracy": 1.0})
        self.assertLessEqual(res["by_category"]["pdf"]["mean_cer"], 0.05)
        self.assertEqual(res["by_category"]["text"]["exact_rate"], 1.0)
        for cat, stats in res["by_category"].items():
            self.assertGreater(stats["docs_per_sec"], 0, cat)
            self.assertGreaterEqual(stats["mean_ms"], 0, cat)
        # rendered-text OCR contributes CER 1.0 on this host (no OCR engine)
        img = [r for r in res["per_item"] if r["name"] == "image-rendered-text"][0]
        self.assertEqual(img["cer"], 1.0)


if __name__ == "__main__":
    unittest.main()
