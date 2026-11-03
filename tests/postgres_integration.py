"""Run explicitly with KG_TEST_DATABASE_URL against a disposable PostgreSQL DB."""
import os
import threading
import unittest
import uuid

from psycopg import sql

from kg_builder.postgres import PreparationStore
from kg_builder.postgres_source import PostgresSourceRepository


class PostgresSourceTests(unittest.TestCase):
    def setUp(self):
        url=os.environ.get('KG_TEST_DATABASE_URL')
        if not url:
            raise RuntimeError('KG_TEST_DATABASE_URL is required for PostgreSQL integration tests')
        self.store=PreparationStore('test-'+uuid.uuid4().hex,database_url=url,max_connections=4)
        self.store.initialize()
        self.repo=PostgresSourceRepository(self.store)

    def tearDown(self):
        with self.store.pool.connection() as conn, conn.transaction():
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.store.workflow_schema)))
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.store.source_schema)))
        self.store.close()

    def test_schema_isolation_and_transaction_rollback(self):
        with self.store.transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM papers').fetchone()['n'],0)
            self.assertEqual(conn.execute(sql.SQL('SELECT count(*) AS n FROM {}').format(
                self.store.source_table('papers'))).fetchone()['n'],0)
        with self.assertRaisesRegex(ValueError,'rollback marker'):
            with self.store.transaction() as conn:
                pid,vid=self.repo.upsert_paper(conn,{'title':'Source A','doi':'10.1000/rollback'},'openalex','r1','q')
                conn.execute('INSERT INTO papers(paper_id,version_id,title,text,source_url,fingerprint,origin_source,origin_id,discovered) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                             (pid,vid,'Source A','A','url','finger','openalex','r1',1.0))
                raise ValueError('rollback marker')
        with self.store.transaction() as conn:
            self.assertEqual(conn.execute(sql.SQL('SELECT count(*) AS n FROM {} WHERE doi=%s').format(
                self.store.source_table('papers')),('10.1000/rollback',)).fetchone()['n'],0)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM papers').fetchone()['n'],0)

    def test_alias_versions_and_idempotent_source(self):
        with self.store.transaction() as conn:
            first,vid=self.repo.upsert_paper(conn,{'title':'One','doi':'10.1000/one',
                'arxiv_id':'2401.01234','version_label':'v1','abstract':'Text',
                'url':'https://arxiv.org/abs/2401.01234v1'},'arxiv','2401.01234','q')
            second,vid2=self.repo.upsert_paper(conn,{'title':'Changed metadata',
                'doi':'10.1000/two','arxiv_id':'2401.01234','version_label':'v2',
                'url':'https://arxiv.org/abs/2401.01234v2'},'openalex','W123','q')
            self.assertEqual(first,second)
            self.assertNotEqual(vid,vid2)
            self.assertEqual(conn.execute(sql.SQL('SELECT count(*) AS n FROM {} WHERE paper_id=%s').format(
                self.store.source_table('paper_doi_aliases')),(first,)).fetchone()['n'],2)
            self.assertEqual(conn.execute(sql.SQL('SELECT current_version_id FROM {} WHERE paper_id=%s').format(
                self.store.source_table('papers')),(first,)).fetchone()['current_version_id'],vid2)

    def test_conflicting_identifiers_rejected(self):
        with self.store.transaction() as conn:
            self.repo.upsert_paper(conn,{'doi':'10.1000/a'},'openalex','a','q')
            self.repo.upsert_paper(conn,{'arxiv_id':'2401.00001'},'arxiv','2401.00001','q')
        with self.assertRaisesRegex(ValueError,'different papers'):
            with self.store.transaction() as conn:
                self.repo.upsert_paper(conn,{'doi':'10.1000/a','arxiv_id':'2401.00001'},'openalex','bridge','q')

    def test_concurrent_same_identity(self):
        barrier=threading.Barrier(3)
        results=[]
        errors=[]
        def insert(source_id):
            try:
                barrier.wait()
                with self.store.transaction() as conn:
                    results.append(self.repo.upsert_paper(conn,{'doi':'10.1000/concurrent',
                        'title':'Work'},'openalex',source_id,'q')[0])
            except BaseException as exc:
                errors.append(exc)
        threads=[threading.Thread(target=insert,args=(str(i),)) for i in range(2)]
        for t in threads:t.start()
        barrier.wait()
        for t in threads:t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertFalse(errors,errors)
        self.assertEqual(len(set(results)),1)
        with self.store.transaction() as conn:
            self.assertEqual(conn.execute(sql.SQL('SELECT count(*) AS n FROM {} WHERE doi=%s').format(
                self.store.source_table('papers')),('10.1000/concurrent',)).fetchone()['n'],1)

    def test_concurrent_initialization_of_same_run(self):
        # Start with no schemas so both pools race through first-run DDL.
        url=os.environ['KG_TEST_DATABASE_URL']
        run_id='test-init-'+uuid.uuid4().hex
        stores=[PreparationStore(run_id,database_url=url,max_connections=2)
                for _ in range(2)]
        barrier=threading.Barrier(3)
        errors=[]
        def initialize(store):
            try:
                barrier.wait()
                store.initialize()
            except BaseException as exc:
                errors.append(exc)
        threads=[threading.Thread(target=initialize,args=(s,)) for s in stores]
        try:
            for t in threads:t.start()
            barrier.wait()
            for t in threads:t.join(10)
            self.assertFalse(any(t.is_alive() for t in threads))
            self.assertFalse(errors,errors)
            with stores[0].transaction() as conn:
                self.assertEqual(conn.execute('SELECT count(*) AS n FROM full_pages').fetchone()['n'],0)
        finally:
            with stores[0].pool.connection() as conn, conn.transaction():
                conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(stores[0].workflow_schema)))
                conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(stores[0].source_schema)))
            for store in stores:store.close()

    def test_extraction_idempotency_and_concurrent_natural_key(self):
        with self.store.transaction() as conn:
            pid,vid=self.repo.upsert_paper(conn,{'doi':'10.1000/extraction',
                'version_label':'v1','url':'https://example.org/paper'},'openalex','e','q')
        payload={'schema_version':'research-kg-extraction/0.3-retrieval',
                 'paper_id':pid,'version_id':vid,
                 'paper_version':{'version_label':'v1','source_url':'https://example.org/paper'},
                 'extractor_type':'external','extractor_version':'test-v1',
                 'source_scope':'abstract_only','status':'draft_requires_review',
                 'contributions':[],'research_problems':[],'methods':[],'benchmarks':[]}
        barrier=threading.Barrier(3)
        ids=[]
        errors=[]
        def ingest():
            try:
                barrier.wait()
                with self.store.transaction() as conn:
                    ids.append(self.repo.ingest_extraction(conn,payload))
            except BaseException as exc:
                errors.append(exc)
        threads=[threading.Thread(target=ingest) for _ in range(2)]
        for t in threads:t.start()
        barrier.wait()
        for t in threads:t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertFalse(errors,errors)
        self.assertEqual(len(set(ids)),1)
        with self.store.transaction() as conn:
            row=conn.execute(sql.SQL('SELECT extracted_at FROM {} WHERE paper_id=%s').format(
                self.store.source_table('extraction_records')),(pid,)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(self.repo.ingest_extraction(conn,dict(reversed(list(payload.items())))),ids[0])
            self.assertEqual(conn.execute(sql.SQL('SELECT extracted_at FROM {} WHERE paper_id=%s').format(
                self.store.source_table('extraction_records')),(pid,)).fetchone(),row)
