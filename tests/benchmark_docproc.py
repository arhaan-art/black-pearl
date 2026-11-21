#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Benchmark docproc pipeline against the synthetic ground-truth corpus.

Usage: .venv-docproc/bin/python tests/benchmark_docproc.py
Writes tests/fixtures/docproc/benchmark_results.json and prints a table.
Numbers are measured, never hard-coded.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bin"))
sys.path.insert(0, os.path.dirname(__file__))

from docproc_corpus import DEDUP_BASE, DEDUP_PAIRS, build_corpus, cer, norm, wer  # noqa: E402
from lib.ingest.processors import run_doc_pipeline  # noqa: E402
from lib.ingest.processors.dedup_text import MinHashDedup  # noqa: E402


def run_benchmark(repeats=3):
    items = build_corpus()
    per_item, by_cat = [], {}
    qr_total = qr_ok = 0
    for it in items:
        best = None
        t0 = time.perf_counter()
        for _ in range(repeats):
            best = run_doc_pipeline(it.data, it.mimetype, it.filename)
        ms = (time.perf_counter() - t0) / repeats * 1000.0
        hyp = best.text if best else ""
        row = {"name": it.name, "category": it.category, "ms": round(ms, 2),
               "warnings": best.warnings if best else []}
        if it.qr_payloads:
            got = [c.content for c in (best.codes if best else [])]
            ok = got == it.qr_payloads
            qr_total += len(it.qr_payloads); qr_ok += int(ok)
            row.update({"qr_exact": ok, "qr_got": got})
        elif it.ground_truth:
            row.update({"cer": cer(it.ground_truth, hyp), "wer": wer(it.ground_truth, hyp),
                        "exact": norm(hyp) == norm(it.ground_truth),
                        "contains_gt": norm(it.ground_truth) in norm(hyp) if it.category in ("archive", "office", "eml") else None,
                        "hyp_chars": len(hyp), "gt_chars": len(it.ground_truth)})
        per_item.append(row)
        c = by_cat.setdefault(it.category, {"n": 0, "ms": 0.0, "cer": [], "wer": [], "exact": 0})
        c["n"] += 1; c["ms"] += ms
        if "cer" in row:
            c["cer"].append(row["cer"]); c["wer"].append(row["wer"]); c["exact"] += int(row["exact"])
    for c in by_cat.values():
        c["mean_ms"] = round(c["ms"] / c["n"], 2)
        c["docs_per_sec"] = round(1000.0 / c["mean_ms"], 2) if c["mean_ms"] else 0
        c["mean_cer"] = round(sum(c["cer"]) / len(c["cer"]), 4) if c["cer"] else None
        c["mean_wer"] = round(sum(c["wer"]) / len(c["wer"]), 4) if c["wer"] else None
        c["exact_rate"] = round(c["exact"] / len(c["cer"]), 4) if c["cer"] else None
        del c["ms"], c["cer"], c["wer"], c["exact"]

    dedup = MinHashDedup()
    dedup.insert("base", DEDUP_BASE)
    dedup_rows = []
    for label, _b, variant, expected in DEDUP_PAIRS:
        hit = bool(dedup.query(variant)) if label != "exact" else bool(dedup.query(variant))
        # exact self-query includes itself; near/unrelated judged on hit
        dedup_rows.append({"pair": label, "expected_dup": expected, "detected": hit,
                           "correct": hit == expected or (label == "exact" and hit)})

    return {"per_item": per_item, "by_category": by_cat,
            "qr": {"total": qr_total, "exact_ok": qr_ok,
                   "accuracy": round(qr_ok / qr_total, 4) if qr_total else 0},
            "dedup": dedup_rows,
            "dedup_accuracy": round(sum(r["correct"] for r in dedup_rows) / len(dedup_rows), 4)}


if __name__ == "__main__":
    result = run_benchmark()
    out = os.path.join(os.path.dirname(__file__), "fixtures", "docproc", "benchmark_results.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps({"by_category": result["by_category"], "qr": result["qr"],
                      "dedup_accuracy": result["dedup_accuracy"]}, indent=2))
    for r in result["per_item"]:
        print(r)
    print(f"wrote {out}")
