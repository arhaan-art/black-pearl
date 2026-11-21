#!/usr/bin/env python3
# -*-coding:UTF-8 -*
"""Embedding service for hybrid search (design item 1).

Default: sentence-transformers paraphrase-multilingual-MiniLM-L12-v2
(384d, mean-pooled, L2-normalised), loaded lazily, CPU-only.
Fallback: deterministic hash embedder (same 384d interface) when
sentence-transformers or the model is unavailable -- offline, no
query text leaves the host. Selected via [Search] config only.
"""
import hashlib
import logging
import math
import re

logger = logging.getLogger(__name__)

DEFAULT_MODEL = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
DEFAULT_DIMENSIONS = 384
_WORD_RE = re.compile(r"\w+", re.UNICODE)


def chunk_text(text, max_words=100, overlap_words=20):
    """Split text into <=~100-word chunks with overlap (design: <=128 word-pieces)."""
    if not text:
        return []
    words = str(text).split()
    if len(words) <= max_words:
        return [' '.join(words)] if words else []
    chunks, step = [], max(1, max_words - overlap_words)
    for start in range(0, len(words), step):
        chunks.append(' '.join(words[start:start + max_words]))
        if start + max_words >= len(words):
            break
    return chunks


def _l2_normalize(vec):
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


class HashEmbedding:
    """Deterministic 384d hash embedder: offline fallback, same interface."""

    name = 'hash-fallback-v1'

    def __init__(self, dimensions=DEFAULT_DIMENSIONS):
        self.dimensions = int(dimensions)

    def embed(self, texts):
        out = []
        for text in texts:
            vec = [0.0] * self.dimensions
            for token in _WORD_RE.findall((text or '').lower()):
                digest = hashlib.sha256(token.encode('utf-8')).digest()
                idx = int.from_bytes(digest[:4], 'little') % self.dimensions
                vec[idx] += 1.0 if digest[4] % 2 == 0 else -1.0
            out.append(_l2_normalize(vec))
        return out


class EmbeddingService:
    """Lazy embedding service; never raises on load failure (falls back)."""

    def __init__(self, model_name=DEFAULT_MODEL, dimensions=DEFAULT_DIMENSIONS, source='userProvided'):
        self.model_name = model_name or DEFAULT_MODEL
        self.dimensions = int(dimensions or DEFAULT_DIMENSIONS)
        self.source = source or 'userProvided'
        self._model = None
        self._fallback = HashEmbedding(self.dimensions)
        self.backend = 'hash-fallback'

    def _load(self):
        if self._model is not None:
            return self._model
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name, device='cpu')
            self.backend = f'sentence-transformers:{self.model_name}'
            logger.info('EmbeddingService loaded %s (%dd)', self.model_name, self.dimensions)
        except Exception as exc:  # model download blocked / not installed
            logger.warning('EmbeddingService fallback to hash embedder: %s', exc)
            self._model = False
            self.backend = 'hash-fallback'
        return self._model

    def embed_texts(self, texts):
        texts = [t or '' for t in texts]
        if not texts:
            return []
        model = self._load()
        if model:
            try:
                vectors = model.encode(texts, batch_size=8, normalize_embeddings=True, show_progress_bar=False)
                return [_l2_normalize([float(v) for v in vec]) for vec in vectors]
            except Exception as exc:
                logger.warning('Embedding encode failed, hash fallback: %s', exc)
        return self._fallback.embed(texts)

    def embed_query(self, text):
        vecs = self.embed_texts([text or ''])
        return vecs[0] if vecs else None

    def embed_document(self, text):
        """Chunk, embed chunks, mean-pool to one vector/doc (v1)."""
        chunks = chunk_text(text)
        if not chunks:
            return None
        vecs = self.embed_texts(chunks)
        pooled = [sum(col) / len(vecs) for col in zip(*vecs)]
        return _l2_normalize(pooled)


_service = None


def get_embedding_service(model_name=DEFAULT_MODEL, dimensions=DEFAULT_DIMENSIONS, source='userProvided'):
    global _service
    if _service is None or (_service.model_name, _service.dimensions, _service.source) != (model_name or DEFAULT_MODEL, int(dimensions or DEFAULT_DIMENSIONS), source or 'userProvided'):
        _service = EmbeddingService(model_name, dimensions, source)
    return _service
