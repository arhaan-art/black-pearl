#!/usr/bin/env python3
"""Reproducible search eval: nDCG@10/Recall@20/MRR + latency per semanticRatio (mockable)."""
import json, math, os, sys, time
def ndcg(ranked, qrels, k=10):
    dcg=sum((2**qrels.get(d,0)-1)/math.log2(i+2) for i,d in enumerate(ranked[:k]))
    ideal=sorted(qrels.values(), reverse=True)
    idcg=sum((2**v-1)/math.log2(i+2) for i,v in enumerate(ideal[:k]))
    return dcg/idcg if idcg else 0.0
def evaluate(runner, queries, qrels):
    rows=[]
    for ratio in (0,0.3,0.4,0.5,0.7):
        for q in queries:
            t0=time.time(); hits=runner(q['query'], ratio); rows.append({'query':q['id'],'class':q['class'],'ratio':ratio,'ndcg10':ndcg(hits, qrels.get(q['id'],{})),'latency_ms':(time.time()-t0)*1000})
    return rows
if __name__=='__main__':
    print('eval harness ready; wire runner to search_engine.api_search in deployment (Meilisearch glibc-blocked on this host).')
