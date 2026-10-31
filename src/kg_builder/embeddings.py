"""Local CPU embeddings for research problem descriptions.

Construction/import is offline. The FastEmbed model is loaded (and potentially
downloaded) only on the first nonempty encode call. Callers persist model_id
with vectors and must not compare vectors from different representations.
"""
from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

import numpy as np


class EmbeddingModel:
    def __init__(self, model_name: str = "BAAI/bge-small-en-v1.5", cache_dir: str | Path | None = None,
                 threads: int = 2):
        if not isinstance(model_name, str) or not model_name.strip():
            raise ValueError("model_name must be nonempty")
        if not isinstance(threads, int) or isinstance(threads, bool) or threads < 1:
            raise ValueError("threads must be a positive integer")
        self.model_name = model_name
        self.cache_dir = str(cache_dir) if cache_dir is not None else None
        self.threads = threads
        self._model = None
        self._dimension: int | None = None

    @property
    def model_id(self) -> str:
        # A change in input representation or normalization requires a new ID.
        return f"fastembed-{version('fastembed')}:{self.model_name}:research-problem-text-v1:l2-v1"

    def encode(self, texts: list[str]) -> np.ndarray:
        if not isinstance(texts, list) or any(not isinstance(t, str) for t in texts):
            raise TypeError("texts must be a list of strings")
        if not texts:
            return np.empty((0, self._dimension or 0), dtype=np.float32)
        if self._model is None:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(model_name=self.model_name, cache_dir=self.cache_dir,
                                        threads=self.threads)
        # FastEmbed yields one vector per text; batch_size bounds encoder memory.
        array = np.asarray(list(self._model.embed(texts, batch_size=32)), dtype=np.float32)
        if array.ndim != 2 or array.shape[0] != len(texts) or not array.shape[1]:
            raise ValueError("encoder returned an invalid shape")
        if self._dimension is not None and self._dimension != array.shape[1]:
            raise ValueError("encoder dimension changed")
        if not np.all(np.isfinite(array)):
            raise ValueError("encoder returned nonfinite values")
        norms = np.linalg.norm(array, axis=1)
        if not np.all(np.isfinite(norms)) or np.any(norms <= 0):
            raise ValueError("encoder returned a zero vector")
        array /= norms[:, None]
        self._dimension = array.shape[1]
        return array
