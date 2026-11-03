"""Offline native incremental projection and transactional driver tests."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from kg_builder.neo4j_store import Neo4jStore, node, edge
from kg_builder.snapshot_graph import SnapshotGraph
from tests.test_snapshot_graph import fixture, FakeEmbedding, pairs, member_result

from tests.postgres_graph_cli_fixtures import NativeGraphCase
from tests.test_neo4j_projection import FakeDriver

class ProjectionTests(NativeGraphCase):
    def setUp(self):
        super().setUp()
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.source=self.root/'snapshot.json';fixture(self.source,4)
        self.driver=FakeDriver();self.publisher=Neo4jStore(self.driver)
    def job(self, **kwargs):
        job=self.make_job(self.source,self.root/'job',publisher=self.publisher,model=FakeEmbedding(),client=object(),k=1,batch_size=1,**kwargs)
        self.addCleanup(job.close);return job
    def test_failed_delivery_retains_pending_and_replays_before_semantics(self):
        job=self.job();self.driver.fail=True
        with self.assertRaises(OSError):job.run()
        self.assertEqual(job._meta('phase'),'new')
        self.assertEqual(job.db.execute('SELECT count(*) FROM graph_outbox WHERE delivered=0').fetchone()['count'],1)
        self.assertIsNone(job._meta('completed_at'))
        self.driver.fail=False
        # The committed event survives and is delivered without another semantic call.
        job._drain();self.assertEqual(len(self.driver.events),1)
        with job.db.transaction():job.db.execute('UPDATE graph_outbox SET delivered=0')
        job._drain();self.assertEqual(len(self.driver.events),1)
        source=next(v for v in self.driver.nodes.values() if 'snapshot_hash' in v)
        self.assertEqual(source['snapshot_hash'],job.snapshot['snapshot_hash'])
    def test_bolt_commit_before_ack_crash_reopens_and_replays_once(self):
        job=self.job()
        with job.db.transaction():
            job._enqueue('test','crash',{'nodes':[node('p','Paper',title='committed')],'edges':[]})
            job.db.execute("CREATE FUNCTION fail_ack_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'ack interrupted'; END $$")
        import psycopg
        with job.db.transaction():job.db.execute('CREATE TRIGGER fail_ack BEFORE UPDATE OF delivered ON graph_outbox FOR EACH ROW EXECUTE FUNCTION fail_ack_fn()')
        with self.assertRaisesRegex(psycopg.errors.RaiseException,'ack interrupted'):job._drain()
        self.assertEqual(len(self.driver.events),1)
        self.assertEqual(job.db.execute('SELECT delivered FROM graph_outbox').fetchone()['delivered'],0)
        with job.db.transaction():job.db.execute('DROP TRIGGER fail_ack ON graph_outbox')
        job.close()
        reopened=self.job();reopened._drain()
        self.assertEqual(len(self.driver.events),1)
        self.assertEqual(reopened.status()['neo4j_projection']['delivered'],1)
        self.assertEqual(reopened.status()['neo4j_projection']['pending'],0)

    def test_group_delivery_failure_resume_keeps_committed_member_reviews(self):
        calls=[];failed=[]
        original_publish=self.publisher.publish
        def publish(run_id,payload):
            if payload['event_id'].startswith('group:') and not failed:
                failed.append(payload['event_id'])
                raise OSError('group delivery interrupted')
            return original_publish(run_id,payload)
        self.publisher.publish=publish
        def reviewed(items,client,**kwargs):
            results=[]
            for item in items:
                calls.append(item['membership_id'])
                result=member_result(item)
                result['raw']['requests']=[{'request_id':item['membership_id'],'attempts':1}]
                results.append(result)
            return results
        def singleton_clusters(vectors,**kwargs):
            return {'clusters':[[mid] for mid in sorted(vectors)],'edges':[],'diagnostics':{'algorithm':'leiden-cpm'}}
        with patch('kg_builder.snapshot_graph.cluster_problem_vectors',singleton_clusters),patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_group_members',reviewed):
            job=self.job()
            with self.assertRaisesRegex(OSError,'group delivery interrupted'):job.run()
            self.assertEqual(job._meta('phase'),'judged')
            committed={tuple(row.values()) for row in job.db.execute('SELECT group_id,mention_id,audit FROM member_reviews')}
            self.assertTrue(committed)
            self.assertEqual(job.db.execute('SELECT count(*) FROM graph_outbox WHERE delivered=0').fetchone()['count'],1)
            original_calls=list(calls)
            job.close()
            resumed=self.job();state=resumed.run()
            self.assertEqual(state['phase'],'complete')
            self.assertEqual(state['neo4j_projection']['pending'],0)
            self.assertEqual(len(calls),len(set(calls)))
            self.assertEqual(calls[:len(original_calls)],original_calls)
            recovered={tuple(row.values()) for row in resumed.db.execute('SELECT group_id,mention_id,audit FROM member_reviews')}
            self.assertTrue(committed <= recovered)
            self.assertEqual(resumed.db.execute('SELECT count(*) FROM jev_requests').fetchone()['count'],len(calls))
            db=self.connection(self.root/'job')
            try:
                ids={row['entity_id'] for row in db.execute('SELECT entity_id FROM entities')}
                self.assertTrue(all((resumed._meta('neo4j_run_id'),eid) in self.driver.nodes for eid in ids))
            finally:db.close()

    def test_atomic_outbox_and_source_immutable_pins(self):
        job=self.job()
        with self.assertRaises(RuntimeError):
            with job.db.transaction():
                job._enqueue('test','rollback',{'nodes':[],'edges':[]})
                job._set_meta('rollback_marker','yes')
                raise RuntimeError('rollback')
        self.assertIsNone(job._meta('rollback_marker'))
        self.assertFalse(job.db.execute('SELECT 1 FROM graph_outbox').fetchone())
        job._prepare_store()
        self.assertEqual(len(self.driver.events),4)
        self.assertTrue(any(v.get('roles')==['proposed'] for v in self.driver.nodes.values()))
        self.assertTrue(any(v.get('source_url')=='https://example.org/1' for v in self.driver.nodes.values()))
        self.assertEqual(sum(e[1]=='RESOLVES_TO' for e in self.driver.edges),4)
        self.assertFalse(any('Method' in q and q.startswith('MERGE') for q,_ in self.driver.queries))
    def test_group_finishes_before_other_name_and_postgres_identity_matches(self):
        def clusters(vectors,**kwargs):
            mids=sorted(vectors);return {'clusters':[mids[:2],mids[2:]],'edges':[],'diagnostics':{'algorithm':'leiden-cpm'}}
        def reviewed(items,client,**kwargs):return [member_result(i,'not_belongs',.7) for i in items]
        with patch('kg_builder.snapshot_graph.cluster_problem_vectors',clusters),patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_group_members',reviewed):
            job=self.job();job.run()
            tasks=job.claim('worker',2);self.assertEqual(len(tasks),2)
            task=tasks[0]
            job.submit([{**{k:task[k] for k in ('task_id','lease_token','input_hash')},'result':{'group_id':task['group_id'],'name':'Area one','description':'Definition'}}])
            self.assertEqual(job.db.execute("SELECT count(*) FROM tasks WHERE status!='done'").fetchone()['count'],1)
            areas=[v for v in self.driver.nodes.values() if v.get('name')=='Area one'];self.assertEqual(len(areas),1)
            self.assertEqual(sum(e[1]=='MEMBER_OF' for e in self.driver.edges),2)
            task=tasks[1]
            job.submit([{**{k:task[k] for k in ('task_id','lease_token','input_hash')},'result':{'group_id':task['group_id'],'name':'Area two','description':'Definition'}}])
            state=job.run();self.assertEqual(state['phase'],'complete');self.assertEqual(state['neo4j_projection']['pending'],0)
            db=self.connection(self.root/'job')
            try:
                ids={r['entity_id'] for r in db.execute('SELECT entity_id FROM entities')}
                self.assertTrue(all((job._meta('neo4j_run_id'),eid) in self.driver.nodes for eid in ids))
                self.assertEqual(db.execute("SELECT count(*) FROM entities WHERE kind='ResearchProblem'").fetchone()['count'],4)
            finally:db.close()
            self.assertLessEqual(state['neo4j_projection']['first_group_delivery_at'],state['completed_at'])
            self.assertFalse(any(e[1]=='SAME' for e in self.driver.edges))

if __name__=='__main__':unittest.main()
