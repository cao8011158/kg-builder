"""Native PostgreSQL checks with an offline role-aware encoder."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import psycopg
from psycopg import sql

from kg_builder.paper_dense import PaperDenseIndex, build
from kg_builder.persistent_dense import PersistentDenseIndex, build as build_persistent, verify_source
import kg_builder.persistent_dense as persistent_dense
from kg_builder.postgres_graph import GraphStore
from kg_builder.semantic_store import SemanticStore, digest
from kg_builder.vector_index import HNSWIndex


class Model:
    model_id='offline:paper-document'
    query_model_id='offline:paper-query'
    def __init__(self):self.documents=[];self.queries=[]
    def encode(self,texts):
        self.documents.extend(texts)
        return [[1.,0.] if 'alpha' in text else [0.,1.] for text in texts]
    def encode_query(self,texts):
        self.queries.extend(texts)
        return [[1.,0.] for _ in texts]


@unittest.skipUnless(os.environ.get('KG_TEST_DATABASE_URL'),'KG_TEST_DATABASE_URL required')
class PaperDenseTests(unittest.TestCase):
    def setUp(self):
        self.url=os.environ['KG_TEST_DATABASE_URL']
        self.run='paper-dense-test-'+uuid.uuid4().hex
        self.store=GraphStore(self.run,self.url)
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'paper-index'
        self.addCleanup(self._cleanup)
        db=self.store.db
        db.execute("INSERT INTO meta VALUES('neo4j_run_id','graph:test'),('snapshot_hash','snapshot:test')")
        for paper,title,pinned,current in [('p:a','Alpha title','alpha pinned','wrong current'),
                                            ('p:b','Beta title','beta pinned','also wrong')]:
            db.execute('INSERT INTO papers(paper_id,title,current_version_id,created_at,updated_at) '
                       'VALUES(%s,%s,%s,%s,%s)',(paper,title,current,'now','now'))
            for version,abstract in [(pinned, 'alpha text' if paper=='p:a' else 'beta text'),
                                     (current,'do not embed current')]:
                db.execute('''INSERT INTO paper_versions(version_id,paper_id,version_label,
                    version_status,abstract,created_at) VALUES(%s,%s,%s,'ok',%s,'now')''',
                    (version,paper,version,abstract))
            db.execute('''INSERT INTO sources(extraction_id,paper_id,version_id,title,source_url,
                version_label,source_hash) VALUES(%s,%s,%s,%s,%s,%s,%s)''',
                ('x:'+paper,paper,pinned,title,'https://example.org/'+paper,pinned,'hash:'+paper))

    def _cleanup(self):
        self.store.close()
        self.temp.cleanup()
        with psycopg.connect(self.url,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(self.store.schema)))

    def test_pinned_identity_resume_dense_search_and_change(self):
        model=Model()
        manifest=build(self.run,self.path,model,database_url=self.url,batch_size=1)
        self.assertEqual(manifest['count'],2)
        self.assertEqual(model.documents,['Alpha title\nalpha text','Beta title\nbeta text'])
        self.assertEqual(self.store.db.execute('SELECT count(*) AS n FROM embedding_cache').fetchone()['n'],2)
        build(self.run,self.path,model,database_url=self.url,batch_size=1)
        self.assertEqual(len(model.documents),2)
        with PaperDenseIndex(self.run,self.path,model,database_url=self.url) as index:
            first=index.search('find alpha',top_k=1)
            self.assertEqual(first[0]['node_id'],'p:a')
            self.assertEqual(first[0]['version_id'],'alpha pinned')
            self.assertEqual(first[0]['graph_namespace'],'graph:test')
            self.assertEqual(first[0]['title'],'Alpha title')
            self.assertEqual(index.search('find alpha',top_k=1)[0]['node_id'],'p:a')
            self.assertEqual(index.store.stats['embeddings_computed'],1)
            self.assertEqual(index.store.stats['dense_index_builds'],0)
            self.assertEqual(len(model.documents),2)
            with self.assertRaises(ValueError):index.search(' ')
        self.assertEqual(model.queries,['find alpha'])
        semantic=SemanticStore(self.run,model=model,database_url=self.url,readonly=True,initialize=False)
        try:
            self.assertEqual(semantic._index_for('Paper').search([1.,0.],k=1)[0]['id'],'p:a')
            self.assertEqual(semantic.stats['dense_index_builds'],1)
        finally:semantic.close()
        self.store.db.execute("UPDATE paper_versions SET abstract='changed' WHERE version_id='alpha pinned'")
        with self.assertRaisesRegex(ValueError,'differs'):
            PaperDenseIndex(self.run,self.path,model,database_url=self.url)
        with self.assertRaisesRegex(ValueError,'differs'):
            build(self.run,self.path,model,database_url=self.url)

    def test_failed_batch_resumes_from_persisted_cache(self):
        class FailingModel(Model):
            def encode(self,texts):
                if any('Beta' in text for text in texts):raise RuntimeError('planned interruption')
                return super().encode(texts)
        first=FailingModel()
        with self.assertRaisesRegex(RuntimeError,'interruption'):
            build(self.run,self.path,first,database_url=self.url,batch_size=1)
        self.assertFalse(self.path.exists())
        self.assertEqual(self.store.db.execute('SELECT count(*) AS n FROM embedding_cache').fetchone()['n'],1)
        second=Model()
        build(self.run,self.path,second,database_url=self.url,batch_size=1)
        self.assertEqual(second.documents,['Beta title\nbeta text'])

    def test_missing_or_conflicting_pinned_source_rejected(self):
        self.store.db.execute('''INSERT INTO sources(extraction_id,paper_id,version_id,title,
            source_url,version_label,source_hash) VALUES(%s,%s,%s,%s,%s,%s,%s)''',
            ('x:missing','p:a','absent','Alpha title','https://example.org/p:a','absent','other'))
        with self.assertRaisesRegex(ValueError,'no pinned version'):
            build(self.run,self.path,Model(),database_url=self.url)
        self.store.db.execute("DELETE FROM sources WHERE extraction_id='x:missing'")
        self.store.db.execute('''INSERT INTO sources(extraction_id,paper_id,version_id,title,
            source_url,version_label,source_hash) VALUES(%s,%s,%s,%s,%s,%s,%s)''',
            ('x:conflict','p:a','wrong current','Alpha title','https://example.org/p:a',
             'wrong current','other'))
        with self.assertRaisesRegex(ValueError,'conflicting'):
            build(self.run,self.path,Model(),database_url=self.url)
        self.store.db.execute("DELETE FROM sources WHERE extraction_id='x:conflict'")
        self.store.db.execute('''INSERT INTO sources(extraction_id,paper_id,version_id,title,
            source_url,version_label,source_hash) VALUES(%s,%s,%s,%s,%s,%s,%s)''',
            ('x:foreign','p:a','also wrong','Alpha title','https://example.org/p:a',
             'also wrong','other'))
        with self.assertRaisesRegex(ValueError,'no pinned version'):
            build(self.run,self.path,Model(),database_url=self.url)

    def test_published_index_requires_its_cached_vectors(self):
        model=Model()
        build(self.run,self.path,model,database_url=self.url)
        self.store.db.execute('DELETE FROM embedding_cache')
        with self.assertRaisesRegex(ValueError,'vectors missing'):
            build(self.run,self.path,model,database_url=self.url)

    def test_six_kind_persistent_build_uses_only_cached_document_vectors(self):
        model=Model()
        build(self.run,self.path,model,database_url=self.url)
        rows=[('ResearchProblem','e:r','Problem','alpha problem'),
              ('Method','e:m','Method','beta method'),
              ('Benchmark','e:b','Benchmark','beta benchmark')]
        for kind,eid,name,description in rows:
            self.store.db.execute('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
                (eid,kind,name,description,'source_authored','[]','rev'))
        self.store.db.execute('INSERT INTO contributions VALUES(%s,%s,%s,%s,%s)',
            ('c:1','x:p:a','k','alpha contribution','{}'))
        self.store.db.execute('INSERT INTO research_areas VALUES(%s,%s,%s,%s)',
            ('a:1','Area','alpha area','rev'))
        for text in ['Problem\nalpha problem','Method\nbeta method','Benchmark\nbeta benchmark',
                     'alpha contribution','Area\nalpha area']:
            vector=[1.,0.] if 'alpha' in text else [0.,1.]
            self.store.db.execute('INSERT INTO embedding_cache VALUES(%s,%s,%s)',
                (model.model_id,digest(text),json.dumps(vector)))
        target=Path(self.temp.name)/'six-kind'
        events=[]
        original_batch=persistent_dense._cache_batch
        original_upsert=HNSWIndex.upsert
        def cache_batch(*args):
            events.append('fetch')
            return original_batch(*args)
        def upsert(index,item_id,vector):
            events.append('upsert')
            return original_upsert(index,item_id,vector)
        with patch('kg_builder.persistent_dense._cache_batch',side_effect=cache_batch), \
             patch.object(HNSWIndex,'upsert',autospec=True,side_effect=upsert):
            manifest=build_persistent(self.run,target,model.model_id,
                query_model_id=model.query_model_id,database_url=self.url,batch_size=2)
        self.assertEqual(events[:4],['fetch','upsert','upsert','fetch'])
        self.assertEqual(manifest['count'],7)
        self.assertEqual(set(manifest['kind_counts']),
            {'Paper','ResearchProblem','Method','Benchmark','Contribution','ResearchArea'})
        self.assertEqual(manifest['kind_counts']['Paper'],2)
        with patch('kg_builder.vector_index.HNSWIndex.upsert',side_effect=AssertionError('reinsert')):
            index=PersistentDenseIndex(target,expected_run_id=self.run,
                expected_graph_namespace='graph:test',expected_model_id=model.model_id)
        self.assertEqual(index.search([1.,0.],types=['ResearchArea'],top_k=1)[0]['node_id'],'a:1')
        self.assertEqual(index.search([1.,0.],types=['Paper'],top_k=1)[0]['kind'],'Paper')
        self.assertEqual(verify_source(self.run,target,database_url=self.url)['count'],7)
        self.store.db.execute("UPDATE research_areas SET description='changed' WHERE area_id='a:1'")
        with self.assertRaisesRegex(ValueError,'differs'):
            verify_source(self.run,target,database_url=self.url)
