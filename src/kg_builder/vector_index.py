"""In-memory HNSW search over stable item IDs.

Reconstruct once from persisted SQLite vectors when opening a store: this cold
step is O(N) in vector count. Each later upsert/delete updates HNSW locally;
the SQLite store remains authoritative and no HNSW binary is persisted.
"""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np


def _unit_vector(vector: Sequence[float], dimension: int | None = None) -> np.ndarray:
    array = np.asarray(vector, dtype=np.float32)
    if array.ndim != 1 or not array.size or (dimension is not None and array.size != dimension):
        raise ValueError("vector must be one-dimensional with the index dimension")
    if not np.all(np.isfinite(array)):
        raise ValueError("vector must contain finite values")
    norm = float(np.linalg.norm(array))
    if not np.isfinite(norm) or norm <= 0 or not np.isclose(norm, 1.0, rtol=1e-3, atol=1e-3):
        raise ValueError("vector must have nonzero unit L2 norm")
    return array / norm


class HNSWIndex:
    def __init__(self, vectors: dict[str, Sequence[float]], model_id: str, seed: int = 42):
        if not isinstance(model_id, str) or not model_id.strip():
            raise ValueError("model_id must be nonempty")
        if not isinstance(seed, int) or isinstance(seed, bool):
            raise ValueError("seed must be an integer")
        self.model_id = model_id
        self.seed = seed
        self._vectors: dict[str, np.ndarray] = {}
        self._labels: dict[str, int] = {}
        self._ids: dict[int, str] = {}
        self._dimension: int | None = None
        self._index = None
        self._next_label = 0
        for item_id, vector in sorted(vectors.items()):
            self.upsert(item_id, vector)

    def _initialize(self, dimension: int) -> None:
        import hnswlib

        index = hnswlib.Index(space="cosine", dim=dimension)
        index.init_index(max_elements=16, ef_construction=200, M=16, random_seed=self.seed,
                         allow_replace_deleted=True)
        index.set_ef(100)
        index.set_num_threads(1)
        self._index = index
        self._dimension = dimension

    def upsert(self, item_id: str, vector: Sequence[float]) -> None:
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("item_id must be a nonempty string")
        array = _unit_vector(vector, self._dimension)
        if self._index is None:
            self._initialize(array.size)
        if item_id in self._labels:
            # hnswlib updates an existing label in place, retaining its ID.
            label = self._labels[item_id]
            self._index.add_items(array.reshape(1, -1), np.asarray([label]))
        else:
            if self._index.get_current_count() >= self._index.get_max_elements():
                self._index.resize_index(max(16, self._index.get_max_elements() * 2))
            label = self._next_label
            self._next_label += 1
            self._index.add_items(array.reshape(1, -1), np.asarray([label]), replace_deleted=True)
            self._labels[item_id] = label
            self._ids[label] = item_id
        self._vectors[item_id] = array

    def delete(self, item_id: str) -> None:
        if item_id not in self._labels:
            return
        label = self._labels.pop(item_id)
        del self._ids[label]
        del self._vectors[item_id]
        self._index.mark_deleted(label)

    def search(self, vector: Sequence[float], k: int = 10) -> list[dict]:
        if not isinstance(k, int) or isinstance(k, bool) or k < 0:
            raise ValueError("k must be a nonnegative integer")
        query = _unit_vector(vector, self._dimension)
        if not self._vectors or k == 0:
            return []
        count = min(k, len(self._vectors))
        self._index.set_ef(max(100, count))
        labels, _ = self._index.knn_query(query, k=count)
        matches = [(self._ids[int(label)], float(np.dot(query, self._vectors[self._ids[int(label)]])))
                   for label in labels[0]]
        matches.sort(key=lambda item: (-item[1], item[0]))
        return [{"id": item_id, "similarity": similarity} for item_id, similarity in matches]
