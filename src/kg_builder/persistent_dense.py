"""Frozen six-kind HNSW snapshot built solely from PostgreSQL document-vector cache."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import uuid

from .paper_dense import _papers
from .semantic_store import SemanticStore, digest
from .vector_index import HNSWIndex, _publish_directory

FORMAT = 'kg-persistent-dense/1'
KINDS = ('Paper', 'ResearchProblem', 'Method', 'Benchmark', 'Contribution', 'ResearchArea')


def _rows(db, papers):
    for row in papers:
        yield 'Paper', row['node_id'], row['title'] + '\n' + (row['abstract'] or '')
    for kind in ('ResearchProblem', 'Method', 'Benchmark'):
        for row in db.execute('SELECT entity_id,name,description FROM entities WHERE kind=%s ORDER BY entity_id', (kind,)):
            yield kind, row['entity_id'], row['name'] + '\n' + row['description']
    for row in db.execute('SELECT contribution_id,description FROM contributions ORDER BY contribution_id'):
        yield 'Contribution', row['contribution_id'], row['description']
    for row in db.execute('SELECT area_id,name,description FROM research_areas ORDER BY area_id'):
        yield 'ResearchArea', row['area_id'], row['name'] + '\n' + row['description']


def _source(store):
    meta = {r['key']: r['value'] for r in store.db.execute(
        "SELECT key,value FROM meta WHERE key IN ('neo4j_run_id','snapshot_hash')")}
    papers = _papers(store.db)
    records = [{'kind': kind, 'node_id': node_id, 'text_hash': digest(text)}
               for kind, node_id, text in _rows(store.db, papers)]
    # HNSWIndex historically inserted the complete ID map in sorted order.
    # Preserve that order while streaming bounded vector-cache batches.
    records.sort(key=lambda row: row['kind'] + '\0' + row['node_id'])
    keys = [(r['kind'], r['node_id']) for r in records]
    if len(keys) != len(set(keys)) or not records:
        raise ValueError('Empty or duplicated dense source')
    source_fingerprint = digest([{'node_id': row['node_id'], 'version_id': row['version_id'],
                                  'source_hash': row['source_hash'], 'text_hash': row['text_hash']}
                                 for row in papers])
    return meta, records, source_fingerprint


def _cache_batch(store, model_id, records):
    hashes = list({row['text_hash'] for row in records})
    rows = store.db.execute('SELECT text_hash,vector_json FROM embedding_cache '
                            'WHERE model_id=%s AND text_hash=ANY(%s)', (model_id, hashes)).fetchall()
    cached = {row['text_hash']: json.loads(row['vector_json']) for row in rows}
    if len(cached) != len(hashes):
        missing = len(hashes) - len(cached)
        raise ValueError(f'{missing} document vectors missing from PostgreSQL cache')
    return cached


def build(run_id: str, index_dir: str | Path, model_id: str, *, query_model_id: str | None = None,
          database_url: str | None = None, batch_size: int = 512) -> dict:
    """Build a new immutable directory using read-only repeatable-read PostgreSQL."""
    if not isinstance(model_id, str) or not model_id.strip():
        raise ValueError('model_id required')
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError('positive batch_size required')
    target = Path(index_dir).absolute()
    if target.exists():
        raise FileExistsError(target)
    store = SemanticStore(run_id, database_url=database_url, readonly=True, initialize=False)
    try:
        with store.db.transaction():
            store.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            meta, records, source_fingerprint = _source(store)
            index = HNSWIndex({}, model_id=model_id)
            dimension = None
            for start in range(0, len(records), batch_size):
                chunk = records[start:start + batch_size]
                cache = _cache_batch(store, model_id, chunk)
                for row in chunk:
                    item_id = row['kind'] + '\0' + row['node_id']
                    vector = cache[row['text_hash']]
                    if dimension is None:
                        dimension = len(vector)
                    if len(vector) != dimension:
                        raise ValueError('Inconsistent document vector dimensions')
                    index.upsert(item_id, vector)
                del cache
        kind_counts = {kind: sum(r['kind'] == kind for r in records) for kind in KINDS}
        manifest = {'format': FORMAT, 'run_id': run_id, 'graph_namespace': meta.get('neo4j_run_id'),
                    'snapshot_hash': meta.get('snapshot_hash'), 'model_id': model_id,
                    'query_model_id': query_model_id or model_id, 'dimension': dimension,
                    'count': len(records), 'kind_counts': kind_counts,
                    'records_hash': digest(records), 'source_fingerprint': source_fingerprint,
                    'source_schema': store.store.schema}
        target.parent.mkdir(parents=True, exist_ok=True)
        stage = target.parent / ('.' + target.name + '-' + uuid.uuid4().hex + '.tmp')
        stage.mkdir()
        try:
            index.save(stage / 'hnsw')
            (stage / 'records.json').write_text(json.dumps(records, ensure_ascii=False,
                sort_keys=True, separators=(',', ':')), encoding='utf-8')
            manifest['records_sha256'] = hashlib.sha256((stage / 'records.json').read_bytes()).hexdigest()
            manifest['hnsw_manifest_sha256'] = hashlib.sha256((stage / 'hnsw' / 'manifest.json').read_bytes()).hexdigest()
            (stage / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False,
                sort_keys=True, indent=2), encoding='utf-8')
            _publish_directory(stage, target)
        finally:
            if stage.exists():
                for file in sorted(stage.rglob('*'), reverse=True):
                    if file.is_file(): file.unlink()
                    else: file.rmdir()
                stage.rmdir()
        return manifest
    finally:
        store.close()


def verify_source(run_id: str, index_dir: str | Path, *, database_url: str | None = None) -> dict:
    """Explicitly compare a saved index to its current PostgreSQL source."""
    index = PersistentDenseIndex(index_dir, expected_run_id=run_id)
    store = SemanticStore(run_id, database_url=database_url, readonly=True, initialize=False)
    try:
        with store.db.transaction():
            store.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            meta, records, source_fingerprint = _source(store)
        expected = index.metadata
        actual = {'graph_namespace': meta.get('neo4j_run_id'), 'snapshot_hash': meta.get('snapshot_hash'),
                  'source_schema': store.store.schema, 'records_hash': digest(records),
                  'source_fingerprint': source_fingerprint, 'count': len(records)}
        for key, value in actual.items():
            if expected.get(key) != value:
                raise ValueError(f'Persistent dense source {key} differs')
        return actual
    finally:
        store.close()


class PersistentDenseIndex:
    """Read an immutable HNSW graph; warm load never opens PostgreSQL."""
    def __init__(self, index_dir: str | Path, *, expected_run_id: str | None = None,
                 expected_graph_namespace: str | None = None, expected_model_id: str | None = None):
        path = Path(index_dir)
        metadata = json.loads((path / 'manifest.json').read_text(encoding='utf-8'))
        if metadata.get('format') != FORMAT:
            raise ValueError('Unsupported persistent dense format')
        for key, expected in (('run_id', expected_run_id), ('graph_namespace', expected_graph_namespace),
                              ('model_id', expected_model_id)):
            if expected is not None and metadata.get(key) != expected:
                raise ValueError(f'Dense {key} mismatch')
        payload = (path / 'records.json').read_bytes()
        hnsw_manifest = (path / 'hnsw' / 'manifest.json').read_bytes()
        if (hashlib.sha256(payload).hexdigest() != metadata['records_sha256'] or
                hashlib.sha256(hnsw_manifest).hexdigest() != metadata['hnsw_manifest_sha256']):
            raise ValueError('Corrupt persistent dense metadata')
        records = json.loads(payload)
        if (len(records) != metadata['count'] or digest(records) != metadata['records_hash'] or
                dict(Counter(r['kind'] for r in records)) != {k: v for k, v in metadata['kind_counts'].items() if v}):
            raise ValueError('Persistent dense record identity mismatch')
        self.index = HNSWIndex.load(path / 'hnsw', expected_model_id=metadata['model_id'])
        if len(self.index._vectors) != len(records) or self.index._dimension != metadata['dimension']:
            raise ValueError('Persistent dense graph identity mismatch')
        self.metadata = metadata
        self._records = {r['kind'] + '\0' + r['node_id']: r for r in records}
        if set(self._records) != set(self.index._vectors):
            raise ValueError('Persistent dense IDs mismatch')
        labels_by_kind = {kind: set() for kind in KINDS}
        for key, record in self._records.items():
            labels_by_kind[record['kind']].add(self.index._labels[key])
        self._labels_by_kind = labels_by_kind
        self._filter_cache = {}

    def search(self, vector, types=None, top_k: int = 10) -> list[dict]:
        if type(top_k) is not int or top_k < 1:
            raise ValueError('positive top_k required')
        selected = set(KINDS if types is None else types)
        if not selected <= set(KINDS):
            raise ValueError('Unknown dense kind')
        if not selected:
            return []
        if len(selected) == len(KINDS):
            hits = self.index.search(vector, k=top_k)
        else:
            key = frozenset(selected)
            if key not in self._filter_cache:
                self._filter_cache[key] = set().union(*(self._labels_by_kind[kind] for kind in key))
            hits = self.index.search(vector, k=top_k, allowed_labels=self._filter_cache[key])
        return [{'node_id': self._records[h['id']]['node_id'],
                 'kind': self._records[h['id']]['kind'], 'similarity': h['similarity']}
                for h in hits]
