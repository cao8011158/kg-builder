"""HNSW search over stable item IDs, with a portable checked binary snapshot."""
from __future__ import annotations

from collections.abc import Sequence
import hashlib
import json
from pathlib import Path
import time
import uuid

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


def _publish_directory(stage: Path, target: Path) -> None:
    """Rename atomically, allowing brief Windows file-scanner handle contention."""
    for attempt in range(5):
        try:
            stage.rename(target)
            return
        except PermissionError as error:
            if getattr(error, 'winerror', None) != 5 or attempt == 4:
                raise
            time.sleep(0.1)


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

    def search(self, vector: Sequence[float], k: int = 10, *, allowed_ids: set[str] | None = None,
               allowed_labels: set[int] | None = None) -> list[dict]:
        if not isinstance(k, int) or isinstance(k, bool) or k < 0:
            raise ValueError("k must be a nonnegative integer")
        query = _unit_vector(vector, self._dimension)
        if not self._vectors or k == 0:
            return []
        if allowed_ids is not None and allowed_labels is not None:
            raise ValueError('Provide allowed IDs or labels, not both')
        if allowed_ids is not None:
            allowed_labels = {self._labels[item_id] for item_id in allowed_ids if item_id in self._labels}
        if allowed_labels is not None:
            if not allowed_labels:
                return []
            count = min(k, len(allowed_labels))
        else:
            allowed_labels = None
            count = min(k, len(self._vectors))
        self._index.set_ef(max(100, count))
        if allowed_labels is None:
            labels, _ = self._index.knn_query(query, k=count)
        else:
            labels, _ = self._index.knn_query(query, k=count,
                filter=lambda label: label in allowed_labels)
        matches = [(self._ids[int(label)], float(np.dot(query, self._vectors[self._ids[int(label)]])))
                   for label in labels[0]]
        matches.sort(key=lambda item: (-item[1], item[0]))
        return [{"id": item_id, "similarity": similarity} for item_id, similarity in matches]

    def save(self, directory: str | Path) -> None:
        """Atomically publish a snapshot; an existing destination is never replaced."""
        if self._index is None:
            raise ValueError("Cannot save an empty HNSW index")
        target = Path(directory).absolute()
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise FileExistsError(target)
        stage = target.parent / ('.' + target.name + '-' + uuid.uuid4().hex + '.tmp')
        stage.mkdir()
        try:
            rows = [{'id': item_id, 'label': self._labels[item_id]}
                    for item_id in sorted(self._vectors)]
            mapping = json.dumps(rows, ensure_ascii=False, sort_keys=True,
                                 separators=(',', ':')).encode('utf-8')
            (stage / 'mapping.json').write_bytes(mapping)
            matrix = np.stack([self._vectors[row['id']] for row in rows])
            np.save(stage / 'vectors.npy', matrix, allow_pickle=False)
            vector_bytes = (stage / 'vectors.npy').read_bytes()
            self._index.save_index(str(stage / 'index.bin'))
            binary = (stage / 'index.bin').read_bytes()
            metadata = {'format': 'kg-hnsw/1', 'model_id': self.model_id,
                        'dimension': self._dimension, 'seed': self.seed,
                        'count': len(rows), 'next_label': self._next_label,
                        'mapping_sha256': hashlib.sha256(mapping).hexdigest(),
                        'vectors_sha256': hashlib.sha256(vector_bytes).hexdigest(),
                        'index_sha256': hashlib.sha256(binary).hexdigest()}
            (stage / 'manifest.json').write_text(json.dumps(metadata, sort_keys=True), encoding='utf-8')
            _publish_directory(stage, target)
        finally:
            if stage.exists():
                for name in ('manifest.json', 'mapping.json', 'vectors.npy', 'index.bin'):
                    (stage / name).unlink(missing_ok=True)
                stage.rmdir()

    @classmethod
    def load(cls, directory: str | Path, *, expected_model_id: str | None = None) -> 'HNSWIndex':
        """Load hnswlib's saved graph without reinserting document vectors."""
        path = Path(directory)
        metadata = json.loads((path / 'manifest.json').read_text(encoding='utf-8'))
        if metadata.get('format') != 'kg-hnsw/1':
            raise ValueError('Unsupported HNSW snapshot format')
        if expected_model_id is not None and metadata.get('model_id') != expected_model_id:
            raise ValueError('HNSW model mismatch')
        mapping = (path / 'mapping.json').read_bytes()
        vector_bytes = (path / 'vectors.npy').read_bytes()
        binary = (path / 'index.bin').read_bytes()
        if (hashlib.sha256(mapping).hexdigest() != metadata.get('mapping_sha256') or
                hashlib.sha256(vector_bytes).hexdigest() != metadata.get('vectors_sha256') or
                hashlib.sha256(binary).hexdigest() != metadata.get('index_sha256')):
            raise ValueError('Corrupt HNSW snapshot')
        rows = json.loads(mapping)
        from io import BytesIO
        matrix = np.load(BytesIO(vector_bytes), allow_pickle=False)
        dimension = metadata['dimension']
        if (not isinstance(dimension, int) or dimension < 1 or len(rows) != metadata['count'] or
                matrix.dtype != np.float32 or matrix.shape != (len(rows), dimension)):
            raise ValueError('Invalid HNSW snapshot metadata')
        self = cls({}, metadata['model_id'], seed=metadata['seed'])
        self._dimension = dimension
        for row, vector in zip(rows, matrix):
            item_id, label = row['id'], row['label']
            if not isinstance(item_id, str) or not item_id or item_id in self._labels or label in self._ids:
                raise ValueError('Invalid HNSW ID mapping')
            self._vectors[item_id] = _unit_vector(vector, dimension)
            self._labels[item_id] = label
            self._ids[label] = item_id
        import hnswlib
        self._index = hnswlib.Index(space='cosine', dim=dimension)
        self._index.load_index(str(path / 'index.bin'), allow_replace_deleted=True)
        self._index.set_ef(100)
        self._index.set_num_threads(1)
        if self._index.get_current_count() < len(rows):
            raise ValueError('HNSW graph is missing mapped vectors')
        self._next_label = metadata['next_label']
        if self._next_label <= max(self._ids, default=-1):
            raise ValueError('Invalid HNSW next label')
        return self
