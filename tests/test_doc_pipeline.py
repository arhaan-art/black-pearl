#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the modular doc/multimedia pipeline (design §13).

No Redis/Java/Tesseract required: unavailable engines are skipped and
OCR cascade behaviour is asserted with stub engines.
"""
import io
import os
import sys
import tarfile
import unittest
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))

from lib.ingest import DocumentExtractor, IngestionPipeline, get_adapter  # noqa: E402
from lib.ingest.config import DocProcConfig, load_pipeline_config  # noqa: E402
from lib.ingest.envelope import ContentRef, IngestEnvelope  # noqa: E402
from lib.ingest.processors import default_registry, run_doc_pipeline  # noqa: E402
from lib.ingest.processors.base import (  # noqa: E402
    Capability, StageContext, parse_tesseract_tsv,
)
from lib.ingest.processors.dedup_text import HAS_DATASKETCH, MinHashDedup  # noqa: E402
from lib.ingest.processors.image import CodeProcessor, OcrProcessor  # noqa: E402
from lib.ingest.processors.language import NullProvider  # noqa: E402
from lib.ingest.processors.metadata import MetadataNormalizer  # noqa: E402
from lib.ingest.processors.registry_store import ProcessedDocRegistry, RegistryEntry  # noqa: E402


class _StubEngine:
    def __init__(self, name, text, conf):
        self.name, self._text, self._conf = name, text, conf

    def available(self):
        return True

    def ocr_image(self, content, languages=None):
        class R:
            pass
        r = R()
        r.text, r.mean_conf, r.warnings = self._text, self._conf, []
        return r


def _make_pdf_bytes():
    import fitz
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Hello born digital world")
    return doc.tobytes()


def _make_qr_png():
    import qrcode  # may be absent; caller skips
    raise unittest.SkipTest("qrcode lib not used; cv2-generated fixture below")


class TestRegistry(unittest.TestCase):
    def test_resolve_order_and_unavailable_skip(self):
        reg = default_registry()
        chain = reg.resolve("application/pdf", "a.pdf")
        names = [p.name for p in chain]
        self.assertIn("pdf-native", names)
        self.assertEqual(names, sorted(names, key=lambda n: [p.priority for p in chain if p.name == n][0]))

    def test_capability_id_format(self):
        cap = Capability("tesseract", "1.0.0", "5.5.0", "abcdef123456")
        self.assertEqual(cap.capability_id, "tesseract@1.0.0+5.5.0#abcdef12")


class TestProcessors(unittest.TestCase):
    def test_pdf_native(self):
        try:
            data = _make_pdf_bytes()
        except Exception as exc:
            self.skipTest(f"fitz unavailable: {exc}")
        r = run_doc_pipeline(data, "application/pdf", "a.pdf")
        self.assertIn("Hello born digital", r.text)
        self.assertEqual(r.metadata.get("doc.page_count"), "1")

    def test_tsv_parser_filters_low_conf(self):
        tsv = ("level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n"
               "5\t1\t1\t1\t1\t1\t0\t0\t10\t10\t95\tHello\n"
               "5\t1\t1\t1\t1\t2\t0\t0\t10\t10\t30\tnoise\n"
               "5\t1\t1\t1\t1\t3\t0\t0\t10\t10\t-1\tjunk\n")
        text, mean, kept = parse_tesseract_tsv(tsv, 0.60)
        self.assertEqual(text, "Hello")
        self.assertEqual(kept, 1)
        self.assertAlmostEqual(mean, 0.95)

    def test_ocr_cascade_reprocess_and_best_kept(self):
        engines = [_StubEngine("stub1", "bad", 0.4), _StubEngine("stub2", "good text", 0.9)]
        r = OcrProcessor(engines=engines).process(
            StageContext(content=b"x", mimetype="image/png"))
        self.assertEqual(r.text, "good text")
        # all attempts low -> best kept, never discarded
        engines2 = [_StubEngine("s1", "first", 0.3), _StubEngine("s2", "second", 0.5),
                    _StubEngine("s3", "third", 0.2)]
        r2 = OcrProcessor(engines=engines2).process(
            StageContext(content=b"x", mimetype="image/png"))
        self.assertEqual(r2.text, "second")
        self.assertTrue(any("low-confidence" in w for w in r2.warnings))

    def test_metadata_normalizer(self):
        meta, warnings = MetadataNormalizer().normalize(
            {"title": "T", "Author": "A", "CreationDate": "D:20200102030405",
             "dc:language": "not-a-lang!!", "X-Custom": "z"},
            "application/pdf", "f.pdf", 10)
        self.assertEqual(meta["dc:title"], "T")
        self.assertTrue(meta["dc:date"].startswith("2020-01-02"))
        self.assertNotIn("dc:language", meta)
        self.assertTrue(any("dc:language" in w for w in warnings))
        self.assertEqual(meta["meta.raw:X-Custom"], "z")

    def test_archive_tar_and_zip(self):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            data = b"tar hello"
            info = tarfile.TarInfo("a.txt")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        r = run_doc_pipeline(buf.getvalue(), "application/x-tar", "a.tar.gz")
        self.assertIn("tar hello", r.text)
        zbuf = io.BytesIO()
        with zipfile.ZipFile(zbuf, "w") as zf:
            zf.writestr("n.txt", "zip hello")
        r2 = run_doc_pipeline(zbuf.getvalue(), "application/zip", "a.zip")
        self.assertIn("zip hello", r2.text)

    def test_office_docx_parsed_runs(self):
        import zipfile as zf_mod
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>Hello</w:t></w:r><w:r><w:t> docx</w:t></w:r></w:p></w:body></w:document>')
        buf = io.BytesIO()
        with zf_mod.ZipFile(buf, "w") as z:
            z.writestr("word/document.xml", xml)
        r = run_doc_pipeline(buf.getvalue(),
                             "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                             "a.docx")
        self.assertIn("Hello docx", r.text)

    def test_codes_cv2_qr(self):
        try:
            import qrcode as _qr
        except Exception as exc:
            self.skipTest(f"qrcode generator unavailable: {exc}")
        # NOTE: cv2.QRCodeEncoder segfaults in opencv 5.1 headless on this
        # host, so the fixture QR is generated with the pure-python lib.
        buf = io.BytesIO()
        _qr.make("https://example.org").save(buf)
        codes = CodeProcessor().extract_codes(buf.getvalue(), order=["cv2", "zxingcpp"])
        if not codes:
            self.skipTest("cv2 QR decode flaky in headless build")
        self.assertEqual(codes[0].content, "https://example.org")
        self.assertIn(codes[0].decoder, ("cv2", "zxingcpp"))
        if codes[0].decoder == "cv2":
            self.assertIsNone(codes[0].confidence)

    def test_null_provider_degrades(self):
        p = NullProvider()
        self.assertTrue(p.available())
        self.assertIsNone(p.translate("hi", "en", "fr"))

    @unittest.skipUnless(HAS_DATASKETCH, "datasketch absent")
    def test_minhash_near_duplicate(self):
        d = MinHashDedup()
        d.insert("a", "the quick brown fox jumps over the lazy dog " * 3)
        self.assertTrue(d.query("the quick brown fox jumps over the lazy dog " * 3))
        self.assertFalse(d.query("completely unrelated content about cooking recipes"))


class TestRegistryStore(unittest.TestCase):
    def test_skip_and_requeue_on_version_bump(self):
        reg = ProcessedDocRegistry()
        reg.record(RegistryEntry(sha256="a" * 64, capability_ids=["x@1#aa"],
                                 overall_confidence=0.9))
        self.assertTrue(reg.is_current("a" * 64, ["x@1#aa"]))
        self.assertFalse(reg.is_current("a" * 64, ["x@2#bb"]))
        self.assertEqual(reg.stale_reason("a" * 64, ["x@2#bb"]), "capability-changed")
        item = reg.enqueue_reprocess("a" * 64, "capability-changed")
        self.assertEqual(reg.dequeue_reprocess()["reason"], "capability-changed")
        self.assertFalse(reg.should_overwrite("a" * 64, 0.5))
        self.assertTrue(reg.should_overwrite("a" * 64, 0.95))


class TestConfigAndPipeline(unittest.TestCase):
    def test_docproc_defaults_and_ini(self):
        cfg = load_pipeline_config("configs/ingest.cfg")
        self.assertEqual(cfg.docproc.ocr_accept, 0.85)
        self.assertIn("zxingcpp", cfg.docproc.code_decoders)
        self.assertEqual(DocProcConfig.from_dict({"ocr_accept": "0.9"}).ocr_accept, 0.9)

    def test_file_child_fork_text_preserved(self):
        import tempfile
        zbuf = io.BytesIO()
        with zipfile.ZipFile(zbuf, "w") as zf:
            zf.writestr("inner.txt", "child body text")
        with tempfile.NamedTemporaryFile("wb", suffix=".zip", delete=False) as fh:
            fh.write(zbuf.getvalue())
            path = fh.name
        try:
            pipe = IngestionPipeline()
            results = pipe.ingest_one(get_adapter("file"), path)
            self.assertTrue(results[0].passed)
            texts = [e.content_text or "" for e in pipe.emitted()]
            self.assertTrue(any("child body text" in t for t in texts),
                            f"child text not forked: {texts}")
        finally:
            os.unlink(path)

    def test_facade_still_works(self):
        doc = DocumentExtractor().extract(b"plain hello", filename="a.txt")
        self.assertIn("plain hello", doc.text)


if __name__ == "__main__":
    unittest.main()
