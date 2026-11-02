"""Offline Voyage wiring checks with real hnswlib and synthetic frozen snapshots."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kg_builder.embeddings import VoyageEmbeddingModel
from kg_builder.semantic_store import SemanticStore, digest
from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash
from kg_builder.vector_index import HNSWIndex
from tests.postgres_snapshot_graph import fixture, GraphPostgresCase


def unit(axis=0, dimension=1024):
    result = [0.0] * dimension
    result[axis] = 1.0
    return result


def snapshot(path, n=3):
    value = fixture(path, n)
    model = VoyageEmbeddingModel()
    for row in value['vectors']:
        row['model_id'] = model.model_id
        row['vector'] = unit(0 if row['kind'] == 'ResearchProblem' else 1)
    save(path, value)
    return value


def save(path, value):
    value['snapshot_hash'] = canonical_hash({k:v for k,v in value.items() if k != 'snapshot_hash'})
    path.write_text(json.dumps(value), encoding='utf-8')


class RoleModel:
    model_id = VoyageEmbeddingModel().model_id
    query_model_id = VoyageEmbeddingModel().query_model_id
    def __init__(self):
        self.documents = []
        self.queries = []
    def encode(self, texts):
        self.documents.extend(texts)
        return [unit() for _ in texts]
    def encode_query(self, texts):
        self.queries.extend(texts)
        return [unit(1) for _ in texts]



import uuid
from psycopg import sql, connect

class VoyagePipelineTests(GraphPostgresCase):
    def setUp(self):
        super().setUp()
        self.semantic_schemas=[]
        self.addCleanup(self.clean_semantics)

    def new_store(self,model):
        store=SemanticStore('voyage-semantic-'+uuid.uuid4().hex,model,database_url=self.database_url)
        self.semantic_schemas.append(store.store.schema)
        return store

    def clean_semantics(self):
        with connect(self.database_url,autocommit=True) as conn:
            for schema in self.semantic_schemas:
                conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))

    def test_snapshot_import_candidates_cluster_reuse_frozen_vectors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root/'snapshot.json'; snapshot(source)
            with patch.object(VoyageEmbeddingModel, 'encode', side_effect=AssertionError('No API reencoding')):
                job = SnapshotGraph(source, root/'job', k=2)
                try:
                    job._prepare_store()
                    job._candidates()
                    self.assertGreater(job.db.execute('SELECT count(*) FROM candidates').fetchone()['count'], 0)
                    self.assertEqual(job._compatible_model().model_id, VoyageEmbeddingModel().model_id)
                    store = SemanticStore(job.run_id, job._compatible_model(),database_url=self.database_url)
                    try:
                        proposal = store.cluster_problems(k=2)
                        self.assertEqual(proposal['diagnostics']['point_count'], 3)
                        self.assertLessEqual(proposal['diagnostics']['candidate_comparisons'], 6)
                        self.assertEqual(store.stats['embeddings_computed'], 0)
                        self.assertEqual(store.stats['embedding_cache_hits'], 3)
                    finally: store.close()
                finally: job.close()


    def test_resume_restores_exact_model_without_credentials_or_encoding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); source=root/'snapshot.json'; snapshot(source)
            job=SnapshotGraph(source,root/'job')
            try: job._prepare_store()
            finally: job.close()
            with patch.dict('os.environ',{},clear=True), patch.object(VoyageEmbeddingModel,'encode',side_effect=AssertionError('encode')):
                resumed=SnapshotGraph(source,root/'job',read_input=False,database_url=self.database_url)
                try:
                    self.assertEqual(resumed._compatible_model().model_id,VoyageEmbeddingModel().model_id)
                    self.assertEqual(resumed.status()['phase'],'imported')
                finally: resumed.close()
            wrong=RoleModel(); wrong.model_id='wrong-model'
            with self.assertRaisesRegex(ValueError,'differs from snapshot'):
                SnapshotGraph(source,root/'job',model=wrong,read_input=False)


    def test_snapshot_provenance_mixed_models_and_dimensions_fail_closed(self):
        for mode in ('text','mixed-model','mixed-dimension','wrong-declared-dimension'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp); source=root/'snapshot.json'; value=snapshot(source)
                if mode=='text': value['vectors'][0]['text']='other text'
                elif mode=='mixed-model': value['vectors'][0]['model_id']='foreign-model'
                elif mode=='mixed-dimension': value['vectors'][1]['vector']=[1.0,0.0]
                else:
                    for row in value['vectors']: row['vector']=[1.0,0.0]
                save(source,value)
                job=SnapshotGraph(source,root/'job')
                try:
                    with self.assertRaises(ValueError):job._prepare_store()
                    self.assertEqual(job._meta('phase'),'new')
                finally:job.close()


    def test_document_query_cache_roles_are_separate_and_fake_encode_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            model=RoleModel(); store=self.new_store(model)
            try:
                self.assertEqual(store._embed(['same text'])[0],unit())
                self.assertEqual(store._embed_query(['same text'])[0],unit(1))
                store._embed(['same text']);store._embed_query(['same text'])
                self.assertEqual(model.documents,['same text']);self.assertEqual(model.queries,['same text'])
                self.assertEqual(store.db.execute('SELECT count(*) FROM embedding_cache').fetchone()['count'],2)
                class Fake:
                    model_id='fake'
                    def encode(self,texts):return [[1.0,0.0] for _ in texts]
                store.model=Fake()
                self.assertEqual(store._embed_query(['fallback']),[[1.0,0.0]])
            finally:store.close()


    def test_public_search_uses_query_route_all_kinds_and_bounded_hot_hnsw(self):
        for count in (8,160):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as tmp:
                model=RoleModel();store=self.new_store(model)
                try:
                    for i in range(count):
                        text=f'entity {i}\ndescription'
                        store.db.execute('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
                            (f'e:{i}','Method',f'entity {i}','description','source','[]','r'))
                        store.db.execute('INSERT INTO embedding_cache VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                            (model.model_id,digest(text),json.dumps(unit())))
                    store.db.execute('INSERT INTO sources VALUES(%s,%s,%s,%s,%s,%s,%s)',('x','p','v','title',None,None,'hash'))
                    store.db.execute('INSERT INTO contributions VALUES(%s,%s,%s,%s,%s)',('c','x','c','a contribution','{}'))
                    store.db.execute('INSERT INTO research_areas VALUES(%s,%s,%s,%s)',('area:1','area','description','r'))
                    store.db.commit()
                    calls=[];original=HNSWIndex.search
                    def search(index,vector,k=10):
                        result=original(index,vector,k)
                        calls.append((k,len(result)))
                        return result
                    with patch.object(HNSWIndex,'search',search):
                        first=store.search('unknown query',limit=2,kind='Method')
                        builds=store.stats['dense_index_builds'];hits=store.stats['embedding_cache_hits']
                        store.search('unknown query',limit=2,kind='Method')
                    self.assertEqual(len(first['results']),2)
                    self.assertEqual(builds,1);self.assertEqual(store.stats['dense_index_builds'],1)
                    self.assertEqual(store.stats['embedding_cache_hits']-hits,1)
                    self.assertEqual(model.documents,[])
                    self.assertEqual(model.queries,['unknown query'])
                    self.assertEqual(calls,[(100,min(100,count)),(100,min(100,count))])
                    store.search('another query',kind='Contribution')
                    store.search('area query',kind='ResearchArea')
                    self.assertEqual(model.queries,['unknown query','another query','area query'])
                    self.assertEqual(model.documents,['a contribution','area\ndescription'])
                finally:store.close()
