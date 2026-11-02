"""Native PostgreSQL graph bootstrap, identity, and readonly semantics."""
import concurrent.futures
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid

import psycopg
from psycopg import sql

from kg_builder.postgres_graph import GraphStore, graph_run_identity
from kg_builder.semantic_store import SemanticStore


class GraphIdentityTests(unittest.TestCase):
    def test_relocation_and_immutable_explicit_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            first=Path(temporary)/'first'
            run=graph_run_identity(first)
            renamed=first.rename(Path(temporary)/'renamed')
            self.assertEqual(graph_run_identity(renamed),run)
            self.assertEqual(graph_run_identity(renamed,run),run)
            self.assertEqual(set(json.loads((renamed/'graph-job.json').read_text())),{'run_id','backend','revision'})
            with self.assertRaisesRegex(ValueError,'cannot be changed'):graph_run_identity(renamed,'other')

    def test_legacy_job_rejected_without_descriptor(self):
        with tempfile.TemporaryDirectory() as temporary:
            (Path(temporary)/'graph.sqlite').touch()
            with self.assertRaisesRegex(ValueError,'fresh'):graph_run_identity(temporary)
            self.assertFalse((Path(temporary)/'graph-job.json').exists())

    def test_concurrent_descriptor_publication(self):
        with tempfile.TemporaryDirectory() as temporary:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                identities=list(pool.map(lambda _:graph_run_identity(temporary),range(8)))
            self.assertEqual(len(set(identities)),1)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.url=os.environ['KG_TEST_DATABASE_URL']
        self.run='semantic-persistence-'+uuid.uuid4().hex
        self.store=GraphStore(self.run,self.url)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.store.close()
        with psycopg.connect(self.url,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(self.store.schema)))

    def test_unified_native_tables_and_rollback(self):
        self.assertTrue(self.store.db.autocommit)
        for table in ('papers','extraction_records','mentions','tasks','entity_fts'):
            self.assertIsNotNone(self.store.db.execute('SELECT to_regclass(%s) AS name',(self.store.schema+'.'+table,)).fetchone()['name'])
        with self.assertRaisesRegex(ValueError,'rollback'):
            with self.store.db.transaction():
                self.store.db.execute("INSERT INTO meta VALUES('atomic','value')")
                raise ValueError('rollback')
        self.assertIsNone(self.store.db.execute("SELECT * FROM meta WHERE key='atomic'").fetchone())

    def test_concurrent_bootstrap_and_readiness_reuse(self):
        self.store.db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.store.schema)))
        def open_store(_):
            with GraphStore(self.run,self.url) as store:
                return store.db.execute('SELECT revision FROM schema_bootstrap WHERE id=1').fetchone()['revision']
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(len(set(pool.map(open_store,range(4)))),1)
        with self.store.connect() as conn:
            with conn.transaction():
                conn.execute('CREATE TABLE preserved_marker(id INTEGER)')
        with GraphStore(self.run,self.url) as opened:
            self.assertIsNotNone(opened.db.execute("SELECT to_regclass('preserved_marker') AS name").fetchone()['name'])

    def test_readonly_embeddings_do_not_mutate_cache(self):
        class Model:
            model_id='offline:test'
            def encode(self,texts):return [[1.,0.] for _ in texts]
        semantic=SemanticStore(self.run,Model(),database_url=self.url,readonly=True,initialize=False)
        self.addCleanup(semantic.close)
        self.assertEqual(semantic._embed_query(['new uncached text']),[[1.,0.]])
        self.assertEqual(semantic._embed_query(['new uncached text']),[[1.,0.]])
        self.assertEqual(semantic.stats['embeddings_computed'],1)
        self.assertEqual(semantic.stats['embedding_cache_hits'],1)
        self.assertEqual(self.store.db.execute('SELECT count(*) AS n FROM embedding_cache').fetchone()['n'],0)
        with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
            semantic.db.execute("INSERT INTO meta VALUES('readonly','fail')")
        self.assertIsNotNone(semantic.db.execute('SELECT 1 AS n').fetchone())

    def test_bad_readiness_is_not_repaired(self):
        self.store.db.execute("UPDATE schema_bootstrap SET revision='invalid'")
        with self.assertRaisesRegex(ValueError,'version'):GraphStore(self.run,self.url)
        self.assertEqual(self.store.db.execute('SELECT revision FROM schema_bootstrap').fetchone()['revision'],'invalid')


if __name__=='__main__':unittest.main()
