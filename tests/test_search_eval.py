"""Independent eval corpus integrity + metric tests (no Meilisearch needed)."""
import json, os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'tools'))
from eval_search_independent import BM25, ndcg, recall, mrr, load  # noqa: E402

def test_corpus_independent_and_labelled():
    docs, queries, qrels = load()
    assert len(docs) >= 40 and len(queries) >= 20
    assert {q['class'] for q in queries} == {'keyword', 'paraphrase', 'entity', 'multilingual'}
    doc_ids = {d['id'] for d in docs}
    for qid, rel in qrels.items():
        assert rel and set(rel) <= doc_ids and all(g in (1, 2, 3) for g in rel.values())
    # relevance authored by topic construction, checked without any retriever
    by_id = {d['id']: d for d in docs}
    for q in queries:
        for did, g in qrels[q['id']].items():
            if q['class'] != 'entity': assert by_id[did]['topic'] == q['topic']

def test_metrics_known_values():
    assert ndcg(['a', 'b'], {'a': 3, 'b': 0}) > 0.9
    assert recall(['a'], {'a': 1, 'b': 1}, 10) == 0.5
    assert mrr(['x', 'a'], {'a': 1}) == 0.5 and mrr(['x'], {'a': 1}) == 0.0

def test_bm25_ranks_exact_entity_first():
    docs, queries, qrels = load(); bm = BM25(docs)
    for q in queries:
        if q['class'] == 'entity':
            s = bm.scores(q['query']); top = bm.ids[max(range(len(s)), key=lambda i: s[i])]
            assert qrels[q['id']][top] == 3

def test_results_file_recorded():
    p = os.path.join(ROOT, 'tests', 'search_eval', 'results.json')
    assert os.path.exists(p), "run tools/eval_search_independent.py first"
    data = json.load(open(p)); names = [r['retriever'] for r in data['results']]
    assert 'keyword-bm25' in names and any('hybrid-st' in n for n in names)
    for r in data['results']:
        if 'ndcg10' in r: assert 0 <= r['ndcg10'] <= 1 and r['latency_ms']['p95'] >= r['latency_ms']['p50']
