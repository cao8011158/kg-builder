"""Native PostgreSQL frozen-source BM25 export; isolated schema only."""
import os
import tempfile
import unittest
import uuid
from pathlib import Path

import psycopg
from psycopg import sql

from kg_builder.bm25_retrieval import BM25Index, build
from kg_builder.postgres_graph import GraphStore


class NativeBM25Tests(unittest.TestCase):
    def setUp(self):
        self.url=os.environ['KG_TEST_DATABASE_URL']
        self.run='bm25-test-'+uuid.uuid4().hex
        self.store=GraphStore(self.run,self.url)
        self.addCleanup(self.cleanup)
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def cleanup(self):
        self.store.close()
        with psycopg.connect(self.url,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(self.store.schema)))

    def test_pinned_source_six_types_and_no_overwrite(self):
        db=self.store.db
        db.execute("INSERT INTO papers(paper_id,title,current_version_id,created_at,updated_at) VALUES('p1','Current different title','v2','now','now')")
        for vid,abstract in [('v1','quasar pinned abstract'),('v2','unpublished current abstract')]:
            db.execute("INSERT INTO paper_versions(version_id,paper_id,version_status,abstract,created_at) VALUES(%s,'p1','active',%s,'now')",(vid,abstract))
        db.execute("INSERT INTO sources VALUES('x1','p1','v1','Pinned quasar title','https://example.test/p1','version1','hash')")
        db.execute("INSERT INTO meta VALUES('neo4j_run_id','graph:test')")
        for kind in ('ResearchProblem','Method','Benchmark'):
            db.execute("INSERT INTO entities(entity_id,kind,name,description,name_origin,aliases_json,revision) VALUES(%s,%s,%s,'quasar description','human','[]','r1')",('e:'+kind,kind,kind+' quasar'))
        db.execute("INSERT INTO contributions VALUES('c1','x1','item','quasar contribution','{}')")
        db.execute("INSERT INTO research_areas VALUES('a1','quasar area','area description','r1')")
        target=Path(self.temp.name)/'index'
        manifest=build(self.run,target,database_url=self.url)
        self.assertEqual(manifest['counts'],{'Paper':1,'ResearchProblem':1,'Method':1,'Benchmark':1,'Contribution':1,'ResearchArea':1})
        self.assertEqual(manifest['graph_namespace'],'graph:test')
        with BM25Index(target,expected_run_id=self.run) as index:
            paper=index.search('pinned',types=['Paper'])[0]
            self.assertEqual((paper['node_id'],paper['version_id'],paper['title']),('p1','v1','Pinned quasar title'))
            self.assertEqual({hit['type'] for hit in index.search('quasar',top_k=6)},set(manifest['counts']))
            self.assertEqual(index.search('unpublished',types=['Paper']),[])
        with self.assertRaises(FileExistsError):build(self.run,target,database_url=self.url)

    def test_all_stopword_source_is_recorded_but_unsearchable(self):
        db=self.store.db
        db.execute("INSERT INTO papers(paper_id,title,created_at,updated_at) VALUES('p1','and the','now','now')")
        db.execute("INSERT INTO paper_versions(version_id,paper_id,version_status,abstract,created_at) VALUES('v1','p1','active','and the','now')")
        db.execute("INSERT INTO sources VALUES('x1','p1','v1','and the',NULL,NULL,'hash')")
        target=Path(self.temp.name)/'empty'
        manifest=build(self.run,target,database_url=self.url)
        self.assertEqual(manifest['counts']['Paper'],1)
        self.assertEqual(manifest['unsearchable_counts']['Paper'],1)
        with BM25Index(target) as index:self.assertEqual(index.search('and',types=['Paper']),[])


if __name__=='__main__':unittest.main()
