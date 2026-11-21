#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Comprehensive tests for the unified ingestion engine.

Covers unit, integration, regression and failure-recovery behaviour for
every ingestion source and every critical pipeline stage (validate -> dedup
-> retry -> DLQ replay -> provenance -> extraction). No Redis/Kvrocks or
Java/Tika server required; external services are replaced by failing stubs
so the documented fallback paths are what gets asserted.
"""
import base64
import gzip
import io
import json
import os
import statistics
import sys
import tempfile
import time
import unittest
import zipfile
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))

from lib.ingest import (  # noqa: E402
    DeadLetterQueue, DocumentExtractor, IngestEnvelope, IngestGate,
    IngestionPipeline, ProvenanceStore, PythonFallbackExtractor,
    RetryExecutor, RetryPolicy, TikaExtractor, default_pipeline,
    get_adapter, list_adapters, load_pipeline_config, sanitize_text,
    to_legacy_queue_message, validate_envelope,
)
from lib.ingest.config import StageConfig  # noqa: E402
from lib.ingest.dedup import DedupChecker, InMemoryDedupStore, metadata_fingerprint  # noqa: E402
from lib.ingest.envelope import ContentRef, ProvenanceEvent  # noqa: E402
from lib.ingest.retry import DLQEntry  # noqa: E402
from lib.ingest.schema import FILE_MAX_SIZE  # noqa: E402


def feeder_payload(text="hello world", source="ail_feeder_test", oid="m1"):
    data = base64.b64encode(gzip.compress(text.encode())).decode()
    return {"source": source, "meta": {"id": oid}, "data": data,
            "timestamp": 1700000000, "source_uuid": "u1"}


def valid_env(**kw):
    env = IngestEnvelope(source_id=kw.pop("source_id", "feeder"),
                         content_ref=ContentRef.from_bytes(b"x"), **kw)
    env.attributes["ail.object_type"] = "item"
    return env


class _FailingRedis:
    """Redis-like stub whose every call fails (fallback-path probe)."""
    def rpush(self, *a, **k): raise ConnectionError("down")
    def lrange(self, *a, **k): raise ConnectionError("down")
    def hset(self, *a, **k): raise ConnectionError("down")
    def lrem(self, *a, **k): raise ConnectionError("down")
    def delete(self, *a, **k): raise ConnectionError("down")
    def llen(self, *a, **k): raise ConnectionError("down")


# ---------------------------------------------------------------- envelope
class TestEnvelopeUnit(unittest.TestCase):
    def test_roundtrip(self):
        env = valid_env()
        env.content_text = "body"
        env.add_provenance("RECEIVE", processor_id="feeder")
        restored = IngestEnvelope.from_dict(env.to_dict())
        self.assertEqual(restored.envelope_uuid, env.envelope_uuid)
        self.assertEqual(restored.content_ref.sha256, env.content_ref.sha256)
        self.assertEqual(restored.trace_id, env.trace_id)
        self.assertTrue(validate_envelope(restored).ok)

    def test_fork_keeps_parent_untouched(self):
        parent = valid_env()
        child = parent.fork(b"child bytes")
        self.assertEqual(child.parent_envelope_uuid, parent.envelope_uuid)
        self.assertEqual(child.trace_id, parent.trace_id)
        self.assertEqual(child.traceparent, parent.traceparent)
        self.assertTrue(any(p.event_type == "FORK" for p in child.provenance))
        self.assertFalse(any(p.event_type == "FORK" for p in parent.provenance))
        self.assertNotEqual(child.content_ref.sha256, parent.content_ref.sha256)

    def test_unknown_provenance_event_rejected(self):
        with self.assertRaises(ValueError):
            ProvenanceEvent(event_type="NOPE", processor_id="x")

    def test_content_ref_hash(self):
        ref = ContentRef.from_bytes(b"abc")
        self.assertEqual(ref.size_bytes, 3)
        self.assertEqual(len(ref.sha256), 64)


# -------------------------------------------------------------- validation
class TestValidationStrict(unittest.TestCase):
    def test_rejects_unsupported_version(self):
        env = valid_env(); env.schema_version = "9.9.9"
        r = validate_envelope(env); self.assertFalse(r.ok)
        self.assertIn("unsupported schema_version", " ".join(r.errors))

    def test_rejects_bad_uuid_sha_trace(self):
        env = valid_env(); env.envelope_uuid = "not-a-uuid"
        env.content_ref.sha256 = "ZZZ"
        env.traceparent = "bogus"
        errors = " ".join(validate_envelope(env).errors)
        self.assertIn("envelope_uuid", errors)
        self.assertIn("sha256", errors)
        self.assertIn("traceparent", errors)

    def test_rejects_missing_identity(self):
        env = IngestEnvelope(source_id="", content_ref=ContentRef.from_bytes(b"x"))
        env.resource = {}
        self.assertFalse(validate_envelope(env).ok)

    def test_rejects_oversize_and_negative(self):
        env = valid_env(); env.content_ref.size_bytes = FILE_MAX_SIZE + 1
        self.assertFalse(validate_envelope(env).ok)
        env2 = valid_env(); env2.content_ref.size_bytes = -1
        self.assertFalse(validate_envelope(env2).ok)

    def test_rejects_unknown_misp_and_attr_type(self):
        env = valid_env()
        env.attributes["misp.category"] = "nope"
        env.attributes["misp.type"] = "nope"
        env.attributes["bad"] = object()
        r = validate_envelope(env); self.assertFalse(r.ok)
        self.assertGreaterEqual(len(r.errors), 3)

    def test_mimetype_mismatch_warns_not_blocks(self):
        env = valid_env(); env.attributes["mimetype"] = "text/plain"
        env.content_ref.mimetype_detected = "application/pdf"
        r = validate_envelope(env)
        self.assertTrue(r.ok); self.assertTrue(r.warnings)

    def test_validation_never_retryable(self):
        self.assertFalse(validate_envelope(valid_env()).retryable)

    def test_sanitize_formula_and_schemes(self):
        self.assertNotIn("javascript:", sanitize_text("JAVASCRIPT:alert(1)"))
        self.assertTrue(sanitize_text("=SUM(A1|A2)").startswith("'"))
        self.assertEqual(sanitize_text(""), "")
        self.assertEqual(sanitize_text("a\x00\x1fb"), "ab")


# ------------------------------------------------------------------- dedup
class TestDedupUnit(unittest.TestCase):
    def test_metadata_case_insensitive(self):
        self.assertEqual(
            metadata_fingerprint({"dc:title": "Hello", "dc:creator": " A "}),
            metadata_fingerprint({"dc:title": "hello", "dc:creator": "a"}))

    def test_check_metadata(self):
        c = DedupChecker()
        self.assertFalse(c.check_metadata({"dc:title": "T"}, "u1"))
        self.assertTrue(c.check_metadata({"dc:title": "t"}, "u2"))

    def test_fuzzy_without_tlsh_is_none(self):
        c = DedupChecker()
        self.assertIsNone(c.fuzzy_similarity(b"a" * 200, b"a" * 200))
        self.assertFalse(c.is_fuzzy_duplicate(b"a" * 200, b"a" * 200))

    def test_store_protocol(self):
        s = InMemoryDedupStore()
        self.assertFalse(s.exists("k")); s.put("k", "v")
        self.assertTrue(s.exists("k")); self.assertEqual(s.get("k"), "v")


# -------------------------------------------------------------- provenance
class TestProvenanceUnit(unittest.TestCase):
    def test_append_only_history(self):
        store = ProvenanceStore()
        env = valid_env()
        store.append(env.envelope_uuid, ProvenanceEvent("RECEIVE", "feeder"))
        store.append(env.envelope_uuid, ProvenanceEvent("ROUTE", "gate"))
        hist = store.history(env.envelope_uuid)
        self.assertEqual([h["event_type"] for h in hist], ["RECEIVE", "ROUTE"])
        self.assertEqual(store.history("unknown-uuid"), [])

    def test_sightings(self):
        store = ProvenanceStore()
        s = store.add_sighting("u1", "feeder", sighting_type=1)
        self.assertEqual(s["type"], 1)
        self.assertEqual(len(store.sightings()), 1)

    def test_redis_failure_falls_back_to_memory(self):
        store = ProvenanceStore(redis_client=_FailingRedis())
        store.append("u1", ProvenanceEvent("RECEIVE", "feeder"))
        self.assertEqual(len(store.history("u1")), 1)


# ------------------------------------------------------------------- retry
class TestRetryDlqUnit(unittest.TestCase):
    def _executor(self, dlq=None, **policy):
        return RetryExecutor(policy=RetryPolicy(base_delay_seconds=0, **policy),
                             dlq=dlq or DeadLetterQueue(), sleeper=lambda _: None)

    def test_backoff_delays_grow(self):
        p = RetryPolicy(base_delay_seconds=1.0, backoff_factor=2.0)
        self.assertEqual([p.delay_for(a) for a in (1, 2, 3)], [1.0, 2.0, 4.0])

    def test_success_never_touches_dlq(self):
        ex = self._executor(); env = valid_env()
        self.assertEqual(ex.run(lambda: 42, env, stage="normalize"), 42)
        self.assertEqual(ex.dlq.depth(), 0)

    def test_transient_failure_recovers(self):
        ex = self._executor(max_retries=3); env = valid_env()
        calls = {"n": 0}
        def flaky():
            calls["n"] += 1
            if calls["n"] < 3: raise RuntimeError("flaky")
            return "ok"
        self.assertEqual(ex.run(flaky, env, stage="normalize"), "ok")
        self.assertEqual(ex.dlq.depth(), 0)

    def test_non_retryable_goes_straight_to_dlq(self):
        ex = self._executor(max_retries=3); env = valid_env()
        calls = {"n": 0}
        def boom(): calls["n"] += 1; raise ValueError("bad")
        self.assertIsNone(ex.run(boom, env, stage="validate", non_retryable=True))
        self.assertEqual(calls["n"], 1)
        self.assertEqual(ex.dlq.list_entries()[0].error_class, "non-retryable")

    def test_dlq_crud_and_entry_roundtrip(self):
        dlq = DeadLetterQueue()
        env = valid_env()
        dlq.push(env, stage="normalize", error="x", error_class="retryable", attempts=2)
        self.assertEqual(dlq.depth(), 1)
        entry = dlq.list_entries()[0]
        self.assertEqual(DLQEntry.from_dict(entry.to_dict()).attempts, 2)
        self.assertIsNotNone(dlq.pop(env.envelope_uuid))
        self.assertEqual(dlq.depth(), 0)
        self.assertIsNone(dlq.pop("missing"))

    def test_dlq_redis_failure_falls_back(self):
        dlq = DeadLetterQueue(redis_client=_FailingRedis())
        dlq.push(valid_env(), stage="s", error="e", error_class="retryable", attempts=1)
        self.assertEqual(dlq.depth(), 1)

    def test_yield_after_consecutive_errors(self):
        sleeps = []
        ex = RetryExecutor(policy=RetryPolicy(max_retries=5, base_delay_seconds=0,
                                              yield_after_errors=2, yield_seconds=7),
                           dlq=DeadLetterQueue(), sleeper=sleeps.append)
        def boom(): raise RuntimeError("x")
        self.assertIsNone(ex.run(boom, valid_env(), stage="s"))
        self.assertIn(7, sleeps)


# --------------------------------------------------------------- extraction
class TestExtractionUnit(unittest.TestCase):
    def setUp(self): self.ex = PythonFallbackExtractor()

    def test_html_strips_tags_and_sanitises(self):
        doc = self.ex.extract(b"<p>hi<script>alert(1)</script></p>", filename="a.html")
        self.assertIn("hi", doc.text); self.assertNotIn("<p>", doc.text)

    def test_json_and_csv_text(self):
        self.assertIn('"k"', self.ex.extract(b'{"k": 1}', filename="a.json").text)

    def test_eml_multipart_with_attachment(self):
        raw = (b"Subject: Report\r\nFrom: a@b.c\r\n"
               b"MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n"
               b"--B\r\nContent-Type: text/plain\r\n\r\nsee attached\r\n"
               b"--B\r\nContent-Type: text/plain; name=note.txt\r\n"
               b"Content-Disposition: attachment; filename=note.txt\r\n\r\n"
               b"attached body\r\n--B--\r\n")
        doc = self.ex.extract(raw, filename="m.eml")
        self.assertEqual(doc.metadata["dc:title"], "Report")
        self.assertIn("attached body", doc.text)
        self.assertTrue(doc.children)

    def test_nested_zip_recursion(self):
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, "w") as zf: zf.writestr("deep.txt", "deep text")
        outer = io.BytesIO()
        with zipfile.ZipFile(outer, "w") as zf: zf.writestr("inner.zip", inner.getvalue())
        doc = self.ex.extract(outer.getvalue(), filename="o.zip")
        self.assertIn("deep text", doc.text)

    def test_zip_cap_and_corrupt_archive(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for i in range(55): zf.writestr(f"f{i}.txt", "x")
        doc = PythonFallbackExtractor(max_embedded=5).extract(buf.getvalue(), filename="big.zip")
        self.assertEqual(len(doc.children), 5)
        self.assertTrue(any("capped" in w for w in doc.warnings))
        bad = self.ex.extract(b"PK not a zip", filename="bad.zip")
        self.assertTrue(bad.warnings)

    def test_pdf_without_text_layer_warns(self):
        doc = self.ex.extract(b"%PDF-1.4 garbage", filename="a.pdf")
        self.assertEqual(doc.metadata.get("mimetype"), "application/pdf")
        self.assertTrue(doc.warnings)

    def test_oversize_truncation_warning(self):
        doc = PythonFallbackExtractor(max_bytes=4).extract(b"123456789", filename="a.txt")
        self.assertTrue(any("truncated" in w for w in doc.warnings))

    def test_tika_unavailable_and_facade_fallback(self):
        tika = TikaExtractor(base_url="http://127.0.0.1:1", timeout=1)
        self.assertFalse(tika.available())
        facade = DocumentExtractor(tika_url="http://127.0.0.1:1")
        doc = facade.extract(b"plain text", filename="a.txt")
        self.assertIn("plain text", doc.text)
        self.assertEqual(doc.extractor, "python-fallback")

    def test_detect_mimetype(self):
        self.assertEqual(DocumentExtractor.detect_mimetype(b"%PDF-1.4"), "application/pdf")
        self.assertEqual(DocumentExtractor.detect_mimetype(b"hello", "a.bin"), "text/plain")


# ------------------------------------------------------------------ config
class TestConfigUnit(unittest.TestCase):
    def test_default_and_missing_file(self):
        cfg = default_pipeline()
        self.assertIn("validate", [s.name for s in cfg.stages])
        self.assertEqual(load_pipeline_config("/nonexistent/x.cfg").source_id, "default")

    def test_json_and_ini_loading(self):
        with tempfile.TemporaryDirectory() as tmp:
            jpath = os.path.join(tmp, "ingest.json")
            with open(jpath, "w") as fh:
                json.dump({"source_id": "feeder",
                           "stages": [{"name": "validate", "max_retries": 5}]}, fh)
            cfg = load_pipeline_config(jpath)
            self.assertEqual(cfg.stages[0].max_retries, 5)
            ipath = os.path.join(tmp, "ingest.cfg")
            with open(ipath, "w") as fh:
                fh.write("[ingest]\nstages = normalize,validate\ntika_url = http://127.0.0.1:9998\n")
            cfg = load_pipeline_config(ipath)
            self.assertEqual(cfg.tika_url, "http://127.0.0.1:9998")
            self.assertEqual([s.name for s in cfg.stages], ["normalize", "validate"])

    def test_stage_defaults(self):
        s = StageConfig(name="validate")
        self.assertTrue(s.enabled); self.assertEqual(s.max_retries, 3)


# ----------------------------------------------------------------- adapters
class TestAdaptersAllSources(unittest.TestCase):
    def test_registry_and_unknown_kind(self):
        self.assertEqual(list_adapters(),
                         ["chat", "crawler", "feed", "feeder", "file", "importer", "website"])
        with self.assertRaises(ValueError):
            get_adapter("pigeon")
        self.assertEqual(get_adapter("website").health()["status"], "ok")

    def test_feeder_decodings(self):
        a = get_adapter("feeder")
        gzip_env = a.normalize(list(a.fetch(feeder_payload("gz text")))[0])
        self.assertEqual(gzip_env.content_text, "gz text")
        b64 = {"source": "s", "meta": {}, "data": base64.b64encode(b"plain").decode()}
        self.assertEqual(a.normalize(list(a.fetch(b64))[0]).content_text, "plain")
        raw = {"source": "s", "meta": {}, "data": "just text"}
        self.assertEqual(a.normalize(list(a.fetch(raw))[0]).content_text, "just text")

    def test_chat_protocol_and_importer(self):
        chat = get_adapter("chat")
        payload = feeder_payload("msg"); payload["meta"]["protocol"] = "telegram"
        env = chat.normalize(list(chat.fetch(payload))[0])
        self.assertEqual(env.attributes["chat.protocol"], "telegram")
        imp = get_adapter("importer")
        env = imp.normalize(list(imp.fetch(feeder_payload("via importer")))[0])
        self.assertEqual(env.content_text, "via importer")

    def test_feed_multi_item(self):
        feed = get_adapter("feed")
        units = list(feed.fetch([{"url": "https://a/", "title": "1"},
                                 {"url": "https://b/", "title": "2"}]))
        self.assertEqual(len(units), 2)
        env = feed.normalize(units[0])
        self.assertTrue(validate_envelope(env).ok)

    def test_file_binary_and_missing(self):
        with tempfile.NamedTemporaryFile("wb", suffix=".bin", delete=False) as fh:
            fh.write(b"\x00\x01binary"); path = fh.name
        try:
            env = get_adapter("file").normalize(list(get_adapter("file").fetch(path))[0])
            self.assertEqual(env.attributes["source.kind"], "file")
        finally: os.unlink(path)
        with self.assertRaises(FileNotFoundError):
            list(get_adapter("file").fetch("/nonexistent/nope.bin"))


# ------------------------------------------------- gate / pipeline stages
class TestGateStages(unittest.TestCase):
    def test_gate_pass_routes_and_commits_provenance(self):
        gate = IngestGate(); env = valid_env()
        r = gate.check(env)
        self.assertTrue(r.passed)
        self.assertTrue(any(h["event_type"] == "ROUTE"
                            for h in gate.provenance.history(env.envelope_uuid)))

    def test_gate_rejects_malformed_to_dlq_never_emits(self):
        pipe = IngestionPipeline()
        env = valid_env(); env.attributes["ail.object_type"] = "bogus"
        r = pipe.gate.check(env)
        self.assertFalse(r.passed); self.assertFalse(r.duplicate)
        self.assertEqual(pipe.dlq.depth(), 1)
        self.assertEqual(pipe.emitted(), [])
        self.assertTrue(any(h["event_type"] == "VALIDATION_FAILED"
                            for h in pipe.provenance.history(env.envelope_uuid)))

    def test_gate_dedup_emits_sighting_only(self):
        gate = IngestGate()
        self.assertTrue(gate.check(valid_env() if False else self._env(b"same")).passed)
        dup = gate.check(self._env(b"same"))
        self.assertTrue(dup.duplicate)
        self.assertEqual(len(gate.provenance.sightings()), 1)

    @staticmethod
    def _env(content):
        env = IngestEnvelope(source_id="feeder",
                             content_ref=ContentRef.from_bytes(content))
        env.attributes["ail.object_type"] = "item"
        return env

    def test_legacy_bridge_format_regression(self):
        env = valid_env(); env.content_text = "legacy body"
        msg = to_legacy_queue_message(env)
        source, payload = msg.split(" ", 1)
        self.assertEqual(source, "feeder")
        self.assertEqual(gzip.decompress(base64.b64decode(payload)).decode(), "legacy body")


# -------------------------------------------------------------- integration
class TestPipelineIntegration(unittest.TestCase):
    def test_every_source_end_to_end(self):
        cases = {
            "feeder": feeder_payload("feeder body"),
            "chat": feeder_payload("chat body", "telegram"),
            "importer": feeder_payload("importer body"),
            "crawler": {"html": "<p>crawled</p>", "last_redirected_url": "https://example.org/"},
            "website": {"html": "<p>site</p>", "url": "https://example.org/"},
            "feed": [{"url": "https://x/", "title": "t"}],
        }
        for kind, raw in cases.items():
            with self.subTest(source=kind):
                pipe = IngestionPipeline()
                results = pipe.ingest_one(get_adapter(kind), raw)
                self.assertTrue(all(r.passed for r in results), results)
                self.assertEqual(pipe.stats.emitted, len(results))
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            fh.write("file body"); path = fh.name
        try:
            pipe = IngestionPipeline()
            self.assertTrue(pipe.ingest_one(get_adapter("file"), path)[0].passed)
        finally: os.unlink(path)

    def test_malformed_record_blocked_end_to_end(self):
        pipe = IngestionPipeline()
        bad = feeder_payload("x"); bad["meta"] = {"type": "bogus-type"}
        results = pipe.ingest_one(get_adapter("feeder"), bad)
        self.assertFalse(results[0].passed)
        self.assertEqual(pipe.stats.emitted, 0)
        self.assertEqual(pipe.dlq.depth(), 1)

    def test_sink_receives_only_passed(self):
        pipe = IngestionPipeline(); seen = []
        dup = feeder_payload("dup")
        pipe.ingest_one(get_adapter("feeder"), dup, sink=seen.append)
        pipe.ingest_one(get_adapter("feeder"), dup, sink=seen.append)
        self.assertEqual(len(seen), 1)
        self.assertEqual(pipe.stats.duplicates, 1)


class _FlakyAdapter:
    source_id = "feeder"
    def __init__(self, inner, failures): self.inner, self.failures, self.calls = inner, failures, 0
    def fetch(self, raw): return self.inner.fetch(raw)
    def normalize(self, raw):
        self.calls += 1
        if self.calls <= self.failures: raise RuntimeError("transient")
        return self.inner.normalize(raw)


class TestFailureRecovery(unittest.TestCase):
    def test_pipeline_recovers_from_transient_normalize_failure(self):
        pipe = IngestionPipeline()
        flaky = _FlakyAdapter(get_adapter("feeder"), failures=2)
        results = pipe.ingest_one(flaky, feeder_payload("recovered"))
        self.assertTrue(results[0].passed)
        self.assertEqual(pipe.dlq.depth(), 0)

    def test_pipeline_exhaustion_lands_in_dlq(self):
        pipe = IngestionPipeline()
        flaky = _FlakyAdapter(get_adapter("feeder"), failures=99)
        with mock.patch("time.sleep", lambda _: None):
            results = pipe.ingest_one(flaky, feeder_payload("doomed"))
        self.assertEqual(results, [])
        self.assertEqual(pipe.stats.failed, 1)
        self.assertEqual(pipe.dlq.depth(), 1)

    def test_dlq_replay_reinjects_valid_entry(self):
        pipe = IngestionPipeline()
        env = valid_env(); env.content_text = "replayed"
        pipe.dlq.push(env, stage="validate", error="old", error_class="retryable", attempts=1)
        self.assertEqual(pipe.replay_dlq(), 1)
        self.assertEqual(pipe.emitted()[0].content_text, "replayed")
        self.assertEqual(pipe.dlq.depth(), 0)
        hist = pipe.provenance.history(env.envelope_uuid)
        self.assertTrue(any(h["event_type"] == "REPLAY" for h in hist))

    def test_dlq_replay_still_invalid_not_emitted(self):
        pipe = IngestionPipeline()
        env = valid_env(); env.attributes["ail.object_type"] = "bogus"
        pipe.dlq.push(env, stage="validate", error="old", error_class="non-retryable", attempts=1)
        self.assertEqual(pipe.replay_dlq(), 0)
        self.assertEqual(pipe.emitted(), [])

    def test_penalize_isolation_between_records(self):
        ex = RetryExecutor(sleeper=lambda _: None)
        a, b = valid_env(), valid_env()
        ex.penalize(a)
        self.assertTrue(ex.is_penalized(a)); self.assertFalse(ex.is_penalized(b))


class TestEntryPointsRegression(unittest.TestCase):
    def test_unified_importer_dry_run(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
        from importer.UnifiedImporter import UnifiedImporter
        importer = UnifiedImporter()
        emitted = importer.importer("feeder", feeder_payload("dry run body"))
        self.assertEqual(len(emitted), 1)
        source, payload = importer.dry_run_messages[0].split(" ", 1)
        self.assertEqual(source, "feeder")
        self.assertEqual(gzip.decompress(base64.b64decode(payload)).decode(), "dry run body")

    def test_existing_entry_points_untouched(self):
        root = os.path.join(os.path.dirname(__file__), "..")
        for rel in ("bin/importer/FeederImporter.py", "bin/importer/FileImporter.py",
                    "bin/modules/Mixer.py", "bin/modules/Global.py", "configs/modules.cfg"):
            self.assertTrue(os.path.exists(os.path.join(root, rel)), rel)
        with open(os.path.join(root, "configs", "modules.cfg")) as fh:
            cfg = fh.read()
        self.assertIn("[UnifiedImporter]", cfg); self.assertIn("[IngestGate]", cfg)


if __name__ == "__main__":
    unittest.main()
