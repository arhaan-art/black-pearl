#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unified ingestion engine tests (no Redis/Kvrocks required)."""
import base64
import gzip
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))

from lib.ingest import (  # noqa: E402
    DeadLetterQueue, IngestEnvelope, IngestionPipeline, PythonFallbackExtractor,
    RetryExecutor, RetryPolicy, get_adapter, list_adapters, sanitize_text,
    to_legacy_queue_message, validate_envelope,
)
from lib.ingest.dedup import DedupChecker, metadata_fingerprint  # noqa: E402
from lib.ingest.envelope import ContentRef  # noqa: E402


def feeder_payload(text="hello world", source="ail_feeder_test"):
    data = base64.b64encode(gzip.compress(text.encode())).decode()
    return {"source": source, "meta": {"id": "m1"}, "data": data,
            "timestamp": 1700000000, "source_uuid": "u1"}


class TestEnvelopeValidate(unittest.TestCase):
    def test_valid_feeder_envelope(self):
        adapter = get_adapter("feeder")
        env = list(adapter.fetch(feeder_payload()))[0]
        env = adapter.normalize(env)
        self.assertTrue(validate_envelope(env).ok, validate_envelope(env).errors)
        self.assertEqual(env.attributes["ail.object_type"], "item")

    def test_bad_object_type_blocked(self):
        env = IngestEnvelope(source_id="feeder", content_ref=ContentRef.from_bytes(b"x"))
        env.attributes["ail.object_type"] = "not-a-type"
        self.assertFalse(validate_envelope(env).ok)

    def test_sanitize(self):
        self.assertNotIn("javascript:", sanitize_text("javascript:alert(1)"))
        self.assertEqual(sanitize_text("a\x00b"), "ab")


class TestDedupRetryDlq(unittest.TestCase):
    def test_exact_dedup_and_metadata(self):
        checker = DedupChecker()
        self.assertFalse(checker.check_exact("a" * 64, "u1"))
        self.assertTrue(checker.check_exact("a" * 64, "u2"))
        self.assertEqual(metadata_fingerprint({"dc:title": "T"}),
                         metadata_fingerprint({"dc:title": "t"}))

    def test_retry_exhaustion_goes_to_dlq(self):
        dlq = DeadLetterQueue()
        executor = RetryExecutor(policy=RetryPolicy(max_retries=2, base_delay_seconds=0),
                                 dlq=dlq, sleeper=lambda _: None)
        env = IngestEnvelope(source_id="feeder", content_ref=ContentRef.from_bytes(b"x"))

        def boom():
            raise RuntimeError("nope")

        self.assertIsNone(executor.run(boom, env, stage="normalize"))
        self.assertEqual(dlq.depth(), 1)

    def test_penalize_is_per_record(self):
        executor = RetryExecutor(sleeper=lambda _: None)
        env = IngestEnvelope(source_id="feeder", content_ref=ContentRef.from_bytes(b"x"))
        executor.penalize(env)
        self.assertTrue(executor.is_penalized(env))
        executor.clear_penalty(env)
        self.assertFalse(executor.is_penalized(env))


class TestAdaptersPipeline(unittest.TestCase):
    def test_adapters_registered(self):
        for kind in ("feeder", "chat", "crawler", "website", "file", "feed", "importer"):
            self.assertIn(kind, list_adapters())

    def test_chat_and_crawler(self):
        chat = get_adapter("chat")
        env = chat.normalize(list(chat.fetch(feeder_payload("hi", "telegram")))[0])
        self.assertEqual(env.attributes["source.kind"], "chat")
        crawler = get_adapter("crawler")
        env2 = crawler.normalize(list(crawler.fetch(
            {"html": "<p>x</p>", "last_redirected_url": "https://example.org/"}))[0])
        self.assertEqual(env2.attributes["source_url"], "https://example.org/")

    def test_pipeline_emits_and_dedups(self):
        pipeline = IngestionPipeline()
        adapter = get_adapter("feeder")
        first = pipeline.ingest_one(adapter, feeder_payload())
        second = pipeline.ingest_one(adapter, feeder_payload())
        self.assertTrue(first[0].passed)
        self.assertTrue(second[0].duplicate)
        self.assertEqual(pipeline.stats.emitted, 1)
        self.assertTrue(to_legacy_queue_message(pipeline.emitted()[0]).startswith("feeder "))

    def test_file_adapter_extracts_zip_children(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("a.txt", "inside text")
        with tempfile.NamedTemporaryFile("wb", suffix=".zip", delete=False) as fh:
            fh.write(buffer.getvalue())
            path = fh.name
        try:
            adapter = get_adapter("file")
            env = adapter.normalize(list(adapter.fetch(path))[0])
            self.assertIn("inside text", env.content_text or "")
            self.assertTrue(getattr(env, "children_meta", []))
        finally:
            os.unlink(path)


class TestExtractor(unittest.TestCase):
    def test_fallback_text_and_eml(self):
        extractor = PythonFallbackExtractor()
        self.assertIn("hello", extractor.extract(b"hello", filename="a.txt").text)
        eml = b"Subject: Hi\r\nFrom: a@b.c\r\n\r\nbody text"
        doc = extractor.extract(eml, filename="m.eml")
        self.assertEqual(doc.metadata.get("dc:title"), "Hi")
        self.assertIn("body text", doc.text)


if __name__ == "__main__":
    unittest.main()
