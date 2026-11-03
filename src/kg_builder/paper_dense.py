"""Frozen Paper document vectors and dense-only retrieval over the PostgreSQL cache."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from .semantic_store import SemanticStore, digest
from .vector_index import HNSWIndex, _publish_directory

FORMAT = 'kg-paper-dense/1'


def _papers(db):
    # The source extraction pins the version. Current paper version is deliberately ignored.
    rows=db.execute('''SELECT s.paper_id AS node_id,s.title,s.version_id,s.version_label,
        s.source_url,s.source_hash,v.abstract,v.version_id AS found_version
        FROM sources s LEFT JOIN paper_versions v
          ON v.version_id=s.version_id AND v.paper_id=s.paper_id
        ORDER BY s.paper_id,s.extraction_id''').fetchall()
    selected={}
    for row in rows:
        if row['found_version'] is None:
            raise ValueError('Paper source has no pinned version')
        item={k:v for k,v in row.items() if k!='found_version'}
        prior=selected.get(row['node_id'])
        if prior is not None:
            fields=('title','version_id','version_label','source_url','abstract')
            if any(prior[key]!=item[key] for key in fields):
                raise ValueError('Paper sources pin conflicting versions or text')
            continue
        selected[row['node_id']]=item
    return [{**row,'text_hash':digest(row['title']+'\n'+(row['abstract'] or ''))}
            for row in selected.values()]


def _snapshot(store):
    with store.db.transaction():
        store.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        meta={r['key']:r['value'] for r in store.db.execute(
            "SELECT key,value FROM meta WHERE key IN ('neo4j_run_id','snapshot_hash')")}
        records=_papers(store.db)
    return meta, records


def _identity(run_id, store, model, meta, records):
    return {'format':FORMAT,'run_id':run_id,'source_schema':store.store.schema,
            'graph_namespace':meta.get('neo4j_run_id'),'snapshot_hash':meta.get('snapshot_hash'),
            'model_id':model.model_id,'query_model_id':model.query_model_id,
            'representation':'title\\nsource-pinned abstract','count':len(records),
            'records_hash':digest(records)}


def _cached_vectors(db, model_id, hashes):
    cached=db.execute('''SELECT text_hash,vector_json FROM embedding_cache
        WHERE model_id=%s AND text_hash=ANY(%s)''',(model_id,hashes)).fetchall()
    vectors={row['text_hash']:json.loads(row['vector_json']) for row in cached}
    if len(vectors)!=len(set(hashes)):raise ValueError('Paper document vectors missing')
    return vectors


def build(run_id: str, index_dir: str | Path, model, *, database_url: str | None = None,
          batch_size: int = 64) -> dict:
    """Prepare missing Paper vectors in checkpointed batches, then publish a manifest.

    PostgreSQL embedding_cache owns vectors. A failed build leaves completed cache rows
    reusable. An existing manifest is accepted only for the same frozen source.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 64:
        raise ValueError('batch_size must be 1..64')
    target=Path(index_dir).absolute()
    store=SemanticStore(run_id,model=model,database_url=database_url,initialize=False)
    try:
        meta,records=_snapshot(store)
        manifest=_identity(run_id,store,model,meta,records)
        if not records:raise ValueError('No pinned Paper sources')
        if target.exists():
            previous=json.loads((target/'manifest.json').read_text(encoding='utf-8'))
            if previous!=manifest:raise ValueError('Paper dense index source or model differs')
            _cached_vectors(store.db,model.model_id,[row['text_hash'] for row in records])
            HNSWIndex.load(target/'hnsw',expected_model_id=model.model_id)
            return previous
        for start in range(0,len(records),batch_size):
            chunk=records[start:start+batch_size]
            store._embed([row['title']+'\n'+(row['abstract'] or '') for row in chunk])
        final_meta,final_records=_snapshot(store)
        if _identity(run_id,store,model,final_meta,final_records)!=manifest:
            raise ValueError('Paper source changed during vector preparation')
        target.parent.mkdir(parents=True,exist_ok=True)
        staging=target.parent/('.'+target.name+'-'+uuid.uuid4().hex+'.tmp')
        staging.mkdir()
        try:
            vectors=_cached_vectors(store.db,model.model_id,[row['text_hash'] for row in records])
            HNSWIndex({row['node_id']:vectors[row['text_hash']] for row in records},
                      model_id=model.model_id).save(staging/'hnsw')
            (staging/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,
                sort_keys=True),encoding='utf-8')
            (staging/'records.json').write_text(json.dumps([{k:v for k,v in row.items() if k!='abstract'}
                for row in records],ensure_ascii=False,sort_keys=True),encoding='utf-8')
            _publish_directory(staging,target)
        finally:
            if staging.exists():
                (staging/'manifest.json').unlink(missing_ok=True)
                (staging/'records.json').unlink(missing_ok=True)
                if (staging/'hnsw').exists():
                    for path in (staging/'hnsw').iterdir():path.unlink()
                    (staging/'hnsw').rmdir()
                staging.rmdir()
        return manifest
    finally:
        store.close()


class PaperDenseIndex:
    """Cold load validates a frozen source and builds HNSW once per instance."""
    def __init__(self, run_id: str, index_dir: str | Path, model, *, database_url: str | None = None):
        path=Path(index_dir)
        manifest=json.loads((path/'manifest.json').read_text(encoding='utf-8'))
        records=json.loads((path/'records.json').read_text(encoding='utf-8'))
        self.store=SemanticStore(run_id,model=model,database_url=database_url,
                                 readonly=True,initialize=False)
        try:
            meta,current=_snapshot(self.store)
            expected=_identity(run_id,self.store,model,meta,current)
            if manifest!=expected or records!=[{k:v for k,v in row.items() if k!='abstract'}
                                                for row in current]:
                raise ValueError('Paper dense index source or model differs')
            self.index=HNSWIndex.load(path/'hnsw',expected_model_id=model.model_id)
            if set(self.index._vectors)!={row['node_id'] for row in records}:
                raise ValueError('Paper dense index IDs differ')
            self.records={row['node_id']:row for row in records}
            self.manifest=manifest
        except BaseException:
            self.store.close()
            raise

    def close(self):self.store.close()
    def __enter__(self):return self
    def __exit__(self,*_):self.close()

    def search(self, query: str, *, top_k: int = 10) -> list[dict]:
        if not isinstance(query,str) or not query.strip():raise ValueError('Nonempty query required')
        if type(top_k) is not int or not 1 <= top_k <= 100:raise ValueError('top_k must be 1..100')
        vector=self.store._embed_query([query])[0]
        return [{'kind':'Paper','node_id':hit['id'],'graph_namespace':self.manifest['graph_namespace'],
                 'similarity':hit['similarity'],**self.records[hit['id']]}
                for hit in self.index.search(vector,k=top_k)]
