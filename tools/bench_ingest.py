#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ingestion throughput/latency benchmark (stdlib only, no Redis/Tika).

Usage: python3 tools/bench_ingest.py [records]
Prints records/sec and mean/p95 latency per stage. Numbers are host-local
and intended as a regression baseline, not an absolute guarantee.
"""
import base64
import gzip
import io
import os
import statistics
import sys
import time
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))

from lib.ingest import (IngestionPipeline, PythonFallbackExtractor,
                        get_adapter, validate_envelope)
from lib.ingest.dedup import DedupChecker
from lib.ingest.envelope import ContentRef, IngestEnvelope


def feeder_payload(text, i):
    data = base64.b64encode(gzip.compress(text.encode())).decode()
    return {"source": "bench", "meta": {"id": str(i)}, "data": data,
            "timestamp": 1700000000, "source_uuid": "u"}


def bench(label, fn, n):
    lat = []
    start = time.perf_counter()
    for i in range(n):
        t0 = time.perf_counter()
        fn(i)
        lat.append((time.perf_counter() - t0) * 1000)
    total = time.perf_counter() - start
    lat.sort()
    print(f"{label}: n={n} {n/total:,.0f} rec/s "
          f"mean={statistics.mean(lat):.3f}ms p95={lat[int(n*0.95)-1]:.3f}ms")


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    text = "benchmark body " * 20
    envs = []
    adapter = get_adapter("feeder")
    for i in range(50):
        for raw in adapter.fetch(feeder_payload(text, i)):
            envs.append(adapter.normalize(raw))

    bench("validate_envelope", lambda i: validate_envelope(envs[i % len(envs)]), n)

    checker = DedupChecker()
    bench("dedup_exact(new)", lambda i: checker.check_exact(f"{i:064x}", "u"), n)

    pipe = IngestionPipeline()
    ad = get_adapter("feeder")
    bench("pipeline_ingest(feeder)", lambda i: pipe.ingest_one(ad, feeder_payload(f"{text}-{i}", i)), min(n, 1000))

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for j in range(10):
            zf.writestr(f"f{j}.txt", text)
    blob = buf.getvalue()
    ex = PythonFallbackExtractor()
    bench("extract_zip(10 files)", lambda i: ex.extract(blob, filename="b.zip"), min(n, 500))
    bench("extract_text", lambda i: ex.extract(text.encode(), filename="b.txt"), n)
    print(f"pipeline stats: emitted={pipe.stats.emitted} duplicates={pipe.stats.duplicates} "
          f"dlq={pipe.dlq.depth()}")


if __name__ == "__main__":
    main()
