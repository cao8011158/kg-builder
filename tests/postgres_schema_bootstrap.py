"""UUID-isolated PostgreSQL schema readiness tests, no external services beyond test DB."""
import os
import threading
import unittest
import uuid
from unittest.mock import patch
import psycopg
from psycopg import sql
from kg_builder import postgres as pg
from kg_builder.postgres_source import PostgresSourceRepository


class SchemaBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.run_id='bootstrap-'+uuid.uuid4().hex
        self.stores=[]
        self.addCleanup(self.cleanup)
    def store(self):
        store=pg.PreparationStore(self.run_id,database_url=os.environ['KG_TEST_DATABASE_URL'],max_connections=1)
        self.stores.append(store)
        return store
    def cleanup(self):
        if self.stores:
            with self.stores[0].pool.connection() as conn,conn.transaction():
                for schema in (self.stores[0].workflow_schema,self.stores[0].source_schema):
                    conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
        for store in self.stores:store.close()
    def marker(self,store):
        with store.transaction() as conn:return conn.execute('SELECT * FROM schema_bootstrap WHERE id=1').fetchone()
    def test_reopen_while_source_writer_holds_rowexclusive_executes_zero_ddl(self):
        first=self.store();first.initialize();cold=self.store()
        with cold.pool.connection() as conn:
            conn.execute("SET lock_timeout='350ms'");conn.commit()
        creates=[];original=psycopg.Connection.execute
        def execute(conn,query,*args,**kwargs):
            text=query if isinstance(query,str) else query.as_string(conn)
            if text.lstrip().upper().startswith('CREATE'):creates.append(text)
            return original(conn,query,*args,**kwargs)
        with first.transaction() as writer:
            writer.execute(sql.SQL('LOCK TABLE {} IN ROW EXCLUSIVE MODE').format(first.source_table('extraction_records')))
            with patch.object(psycopg.Connection,'execute',execute):
                cold.initialize();cold.initialize()
        self.assertEqual(creates,[])
    def test_concurrent_first_bootstrap_has_single_committed_marker(self):
        stores=[self.store(),self.store()];barrier=threading.Barrier(3);errors=[]
        def init(store):
            try:barrier.wait();store.initialize()
            except BaseException as exc:errors.append(type(exc).__name__)
        threads=[threading.Thread(target=init,args=(s,)) for s in stores]
        for thread in threads:thread.start()
        barrier.wait()
        for thread in threads:thread.join(10)
        self.assertFalse(any(t.is_alive() for t in threads));self.assertEqual(errors,[])
        marker=self.marker(stores[0]);self.assertEqual(marker['run_id'],self.run_id)
        with stores[0].transaction() as conn:self.assertEqual(conn.execute('SELECT count(*) AS n FROM schema_bootstrap').fetchone()['n'],1)
    def test_failed_bootstrap_rolls_back_marker_and_all_new_schemas(self):
        store=self.store()
        with patch.object(pg,'WORKFLOW_DDL',pg.WORKFLOW_DDL+'; SELECT * FROM definitely_missing_bootstrap_table;'):
            with self.assertRaises(psycopg.errors.UndefinedTable):store.initialize()
        with store.pool.connection() as conn:
            self.assertIsNone(conn.execute('SELECT to_regnamespace(%s) AS name',(store.workflow_schema,)).fetchone()['name'])
            self.assertIsNone(conn.execute('SELECT to_regnamespace(%s) AS name',(store.source_schema,)).fetchone()['name'])
        store.initialize();self.assertEqual(self.marker(store)['run_id'],self.run_id)
    def test_interruption_after_marker_insert_does_not_publish_readiness(self):
        store=self.store();original=psycopg.Connection.execute
        def execute(conn,query,*args,**kwargs):
            result=original(conn,query,*args,**kwargs)
            text=query if isinstance(query,str) else query.as_string(conn)
            if text.startswith('INSERT INTO ') and '.schema_bootstrap VALUES' in text:
                raise RuntimeError('interrupted before commit')
            return result
        with patch.object(psycopg.Connection,'execute',execute):
            with self.assertRaisesRegex(RuntimeError,'interrupted before commit'):store.initialize()
        with store.pool.connection() as conn:
            self.assertIsNone(conn.execute('SELECT to_regnamespace(%s) AS name',(store.workflow_schema,)).fetchone()['name'])
            self.assertIsNone(conn.execute('SELECT to_regnamespace(%s) AS name',(store.source_schema,)).fetchone()['name'])
        store.initialize();self.assertEqual(self.marker(store)['run_id'],self.run_id)
    def test_marker_mismatch_and_business_identity_fail_closed(self):
        store=self.store();store.initialize()
        for column,value in (('revision','unknown'),('ddl_fingerprint','unknown'),('run_id','other')):
            with self.subTest(column=column):
                original=self.marker(store)[column]
                with store.transaction() as conn:conn.execute(sql.SQL('UPDATE schema_bootstrap SET {}=%s').format(sql.Identifier(column)),(value,))
                with self.assertRaises(ValueError):self.store().initialize()
                with store.transaction() as conn:conn.execute(sql.SQL('UPDATE schema_bootstrap SET {}=%s').format(sql.Identifier(column)),(original,))
        with store.transaction() as conn:conn.execute("INSERT INTO run(id,run_id,config_json,collection_state,started) VALUES(1,'wrong','{}','open',1)")
        with self.assertRaisesRegex(ValueError,'identity'):self.store().initialize()
    def test_premarker_bootstrap_preserves_source_work_and_configuration(self):
        store=self.store();store.initialize()
        with store.transaction() as conn:
            pid,vid=PostgresSourceRepository(store).upsert_paper(conn,{'title':'Preserved','doi':'10.1000/bootstrap'},'openalex','keep','q')
            conn.execute('INSERT INTO run(id,run_id,config_json,collection_state,started,error) VALUES(1,%s,%s,%s,1,%s)',(self.run_id,'{"keep":true}','open','keep-error'))
            conn.execute("INSERT INTO papers(paper_id,version_id,title,text,source_url,fingerprint,origin_source,origin_id,discovered) VALUES(%s,%s,'Preserved','keep','url','finger','openalex','keep',1)",(pid,vid))
            conn.execute('DROP TABLE schema_bootstrap')
        cold=self.store();cold.initialize()
        self.assertEqual(self.marker(cold)['run_id'],self.run_id)
        with cold.transaction() as conn:
            row=conn.execute('SELECT * FROM run WHERE id=1').fetchone()
            self.assertEqual(row['config_json'],'{"keep":true}');self.assertEqual(row['error'],'keep-error')
            self.assertEqual(conn.execute('SELECT text FROM papers WHERE paper_id=%s',(pid,)).fetchone()['text'],'keep')
            self.assertEqual(conn.execute(sql.SQL('SELECT title FROM {} WHERE paper_id=%s').format(store.source_table('papers')),(pid,)).fetchone()['title'],'Preserved')
    def test_empty_marker_is_not_automatically_repaired(self):
        store=self.store();store.initialize()
        with store.transaction() as conn:conn.execute('DELETE FROM schema_bootstrap')
        with self.assertRaises(ValueError):self.store().initialize()
