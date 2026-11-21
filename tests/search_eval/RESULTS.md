# Independent search evaluation — recorded results

Corpus: `corpus.jsonl` (46 docs: 6 topics x 6 docs graded core=3/related=2/tangential=1 by construction + 10 distractors), queries: `queries_independent.jsonl` (24 = 6 topics x keyword/paraphrase/entity/multilingual-FR), qrels: `qrels_independent.tsv` (120 pairs). Relevance authored before any retrieval; not derived from retriever output. Reproduce: `.venv-docproc/bin/python tools/eval_search_independent.py` -> `results.json` (same corpus, same BM25 index for all; hybrid = 0.6*norm-BM25 + 0.4*cosine, ratio 0.4 = [Search] default).

| Retriever | nDCG@10 | Recall@10 | Recall@50 | MRR | latency p50 | latency p95 |
|---|---|---|---|---|---|---|
| keyword-bm25 | 0.7374 | 0.6875 | 1.0 | 0.8460 | 0.023ms | 0.031ms |
| hybrid-hash (r=0.4) | 0.7162 | 0.6667 | 1.0 | 0.8335 | 0.624ms | 0.662ms |
| hybrid-ST MiniLM (r=0.4) | 0.8629 | 0.8264 | 1.0 | 0.9792 | 22.829ms | 25.297ms |

By class nDCG@10 (keyword / hybrid-ST): keyword 0.9798/0.9835, paraphrase 0.6691/0.7480, multilingual 0.3419/0.7754, entity 0.9587/0.9449.

## Honest findings
- Hybrid-ST beats keyword overall (+0.126 nDCG@10, +0.139 Recall@10, +0.133 MRR), driven by multilingual (+0.43) and paraphrase (+0.08).
- Hybrid-ST does NOT beat keyword everywhere: entity Recall@10 drops 0.75 -> 0.6667 and entity nDCG 0.9587 -> 0.9449. Exact-entity queries should keep low/zero semantic ratio.
- Hash-fallback hybrid is worse than keyword on every aggregate metric (0.7162 vs 0.7374 nDCG) — it is a degradation fallback, not a quality mode.
- Latency cost is real: hybrid-ST ~1000x keyword per query (22.8ms vs 0.023ms p50, query embedding included; doc embedding is index-time and excluded).
- Recall@50 = 1.0 for all is saturated/trivial (only 46 docs) — not a discriminating metric at this corpus size.
- This is an offline BM25+cosine proxy, NOT live Meilisearch hybrid: Meilisearch binaries require GLIBC_2.35 (host 2.34, no Docker) so live hybrid/facet/explain ranking remains unverified. Playwright UI tests likewise blocked: Chromium launch exit 127 (missing AL2023 system libs, needs dnf). API-level substitutes are in `tests/test_search_engine_unit.py`.

## Bugs fixed during evaluation
1. `search_engine.MeiliSearch.search`: `sort='hybrid'` (and explicit `semantic_ratio`) bypassed the `hybrid_enabled=False` kill-switch via `... or default`. Now no vector is sent when disabled.
2. `search_embedding.get_embedding_service`: singleton ignored new model/dimensions/source args. Now recreated when params differ.

## Live verification (Docker, 2026-11-21 update)
Docker 25.0.18 installed (`sudo dnf install -y docker`); Meilisearch
v1.15.2 container on 127.0.0.1:7700 (health `available`). Live probe
(/tmp/live_meili_probe.py, 46-doc corpus with ST vectors, userProvided
embedder 384d): keyword recall@10 1.00 (exact), paraphrase query
keyword 0.00 -> hybrid (ratio 0.4) 1.00, facets + _rankingScoreDetails
returned. Playwright Chromium 159 launches after dnf system libs;
tests/test_search_playwright.py passes against the live server.
Entity routing fixed: get_semantic_ratio(query=...) returns 0.1 when
detect_entities() hits; semantic_ratio_exact default 0.1 in code and
configs/core.cfg.sample. Still unverified: Flask dashboard end-to-end
(app stack), ratio sweep nDCG on live server, large-corpus load test.
