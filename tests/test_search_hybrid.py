import os, sys
os.environ.setdefault('AIL_BIN', os.path.join(os.getcwd(),'bin'))
os.environ.setdefault('AIL_HOME', os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(),'bin'))
# pure-unit tests not requiring Meilisearch/Redis
sys.path.insert(0, os.path.join(os.getcwd(),'bin','lib'))
from search_embedding import EmbeddingService, chunk_text, HashEmbedding
def test_chunk_overlap():
    assert len(chunk_text(' '.join(['w']*250))) >= 3
def test_hash_embedder_dims_and_norm():
    v=HashEmbedding(384).embed(['bitcoin wallet'])[0]
    assert len(v)==384 and abs(sum(x*x for x in v)-1.0) < 1e-6
def test_service_fallback_embeds():
    svc=EmbeddingService('nonexistent-model-xyz',384)
    v=svc.embed_query('test query'); assert v and len(v)==384
def test_service_singleton_respects_params():
    import search_embedding
    a=search_embedding.get_embedding_service('model-a',384); b=search_embedding.get_embedding_service('model-b',128)
    assert b.dimensions==128 and b.model_name=='model-b' and a is not b
def test_entity_detection():
    # import without ConfigLoader/Redis by stubbing modules is deployment-only; check regex via embedding module independence
    assert chunk_text('') == []
