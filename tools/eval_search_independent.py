#!/usr/bin/env python3
"""Independent baseline-vs-hybrid evaluation (no Meilisearch required).

Same corpus/index for all retrievers. Relevance is by construction
(topic/tier in corpus generation), authored before any retrieval.
Retrievers: BM25 keyword-only; hybrid = (1-ratio)*norm(BM25)+ratio*cosine
with ratio=0.4 (the [Search] default), using HashEmbedding or, when
available, sentence-transformers. Run with .venv-docproc/bin/python
for the real ST backend; system python falls back to hash and is
labelled as such in results.
"""
import json, math, os, re, sys, time
from collections import Counter, defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin', 'lib'))
from search_embedding import EmbeddingService, HashEmbedding  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), '..', 'tests', 'search_eval')
TOK = re.compile(r"\w+", re.UNICODE)

def load():
    docs = [json.loads(l) for l in open(os.path.join(ROOT, 'corpus.jsonl'))]
    queries = [json.loads(l) for l in open(os.path.join(ROOT, 'queries_independent.jsonl'))]
    qrels = defaultdict(dict)
    for line in open(os.path.join(ROOT, 'qrels_independent.tsv')):
        q, d, g = line.rstrip('\n').split('\t'); qrels[q][d] = int(g)
    return docs, queries, qrels

class BM25:
    def __init__(self, docs, k1=1.5, b=0.75):
        self.ids = [d['id'] for d in docs]
        self.doc_tokens = [TOK.findall((d.get('title','')+' '+d['content']).lower()) for d in docs]
        self.N = len(docs); self.k1, self.b = k1, b
        self.avgdl = sum(len(t) for t in self.doc_tokens) / max(1, self.N)
        df = Counter()
        for t in self.doc_tokens: df.update(set(t))
        self.idf = {t: math.log(1 + (self.N - c + 0.5) / (c + 0.5)) for t, c in df.items()}
        self.tf = [Counter(t) for t in self.doc_tokens]
    def scores(self, query):
        qt = TOK.findall(query.lower()); out = []
        for i, tf in enumerate(self.tf):
            dl = len(self.doc_tokens[i]); s = 0.0
            for t in qt:
                f = tf.get(t, 0)
                if f: s += self.idf.get(t, 0) * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * dl / self.avgdl))
            out.append(s)
        return out

def ndcg(ranked, rel, k=10):
    dcg = sum((2**rel.get(d, 0)-1)/math.log2(i+2) for i, d in enumerate(ranked[:k]))
    ideal = sorted(rel.values(), reverse=True)
    idcg = sum((2**v-1)/math.log2(i+2) for i, v in enumerate(ideal[:k]))
    return dcg/idcg if idcg else 0.0

def recall(ranked, rel, k):
    relevant = {d for d, g in rel.items() if g > 0}
    return len(set(ranked[:k]) & relevant)/len(relevant) if relevant else 0.0

def mrr(ranked, rel):
    for i, d in enumerate(ranked, 1):
        if rel.get(d, 0) > 0: return 1.0/i
    return 0.0

def pct(vals, p):
    s = sorted(vals); return s[min(len(s)-1, int(round(p/100*len(s))-1))] if s else 0.0

def evaluate(name, rank_fn, queries, qrels):
    rows, lat = [], []
    for q in queries:
        t0 = time.perf_counter(); ranked = rank_fn(q['query']); lat.append((time.perf_counter()-t0)*1000)
        rel = qrels.get(q['id'], {})
        rows.append({'query': q['id'], 'class': q['class'], 'ndcg10': ndcg(ranked, rel),
                     'recall10': recall(ranked, rel, 10), 'recall50': recall(ranked, rel, 50), 'mrr': mrr(ranked, rel)})
    agg = {m: round(sum(r[m] for r in rows)/len(rows), 4) for m in ('ndcg10', 'recall10', 'recall50', 'mrr')}
    by_class = {}
    for c in sorted({r['class'] for r in rows}):
        sub = [r for r in rows if r['class'] == c]
        by_class[c] = {m: round(sum(r[m] for r in sub)/len(sub), 4) for m in ('ndcg10', 'recall10', 'mrr')}
    return {'retriever': name, **agg, 'latency_ms': {'p50': round(pct(lat, 50), 3), 'p95': round(pct(lat, 95), 3)},
            'per_query': rows, 'by_class': by_class}

def main():
    docs, queries, qrels = load()
    bm = BM25(docs); ids = bm.ids
    def keyword(q):
        s = bm.scores(q); return [ids[i] for i in sorted(range(len(ids)), key=lambda i: -s[i])]
    doc_texts = [d.get('title','')+' '+d['content'] for d in docs]
    results = [evaluate('keyword-bm25', keyword, queries, qrels)]
    for label, svc in [('hybrid-hash(r=0.4)', None), ('hybrid-st(r=0.4)', 'st')]:
        if svc == 'st':
            embedder = EmbeddingService('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2', 384)
            doc_vecs = embedder.embed_texts(doc_texts)
            if embedder.backend == 'hash-fallback':
                results.append({'retriever': label, 'skipped': 'sentence-transformers unavailable'}); continue
            qvec = lambda q: embedder.embed_query(q)  # noqa: E731
        else:
            h = HashEmbedding(384); doc_vecs = h.embed(doc_texts); qvec = lambda q: h.embed([q])[0]  # noqa: E731
        def hybrid(q, doc_vecs=doc_vecs, qvec=qvec):
            bs = bm.scores(q); mx = max(bs) or 1.0; qv = qvec(q)
            hs = [0.6*(bs[i]/mx) + 0.4*sum(a*b for a, b in zip(qv, doc_vecs[i])) for i in range(len(ids))]
            return [ids[i] for i in sorted(range(len(ids)), key=lambda i: -hs[i])]
        results.append(evaluate(label, hybrid, queries, qrels))
    out = {'corpus_docs': len(docs), 'queries': len(queries), 'qrel_pairs': sum(len(v) for v in qrels.values()),
           'note': 'doc/query embedding time excluded from latency (index-time cost); latency is per-query ranking over precomputed vectors+BM25',
           'results': [{k: v for k, v in r.items() if k != 'per_query'} | ({'per_query': r['per_query']} if 'per_query' in r else {}) for r in results]}
    with open(os.path.join(ROOT, 'results.json'), 'w') as f: json.dump(out, f, indent=2)
    print(json.dumps([{k: v for k, v in r.items() if k not in ('per_query',)} for r in results], indent=2))
    return out

if __name__ == '__main__':
    main()
