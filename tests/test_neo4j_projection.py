"""Offline native incremental projection and transactional driver tests."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from kg_builder.neo4j_store import Neo4jStore, node, edge
from kg_builder.snapshot_graph import SnapshotGraph
from test_snapshot_graph import fixture, FakeEmbedding, pairs, member_result

class Result:
    def __init__(self, record=None):self.record=record
    def consume(self):return None
    def single(self):return self.record

class FakeDriver:
    def __init__(self):self.nodes={};self.edges=set();self.events={};self.queries=[];self.fail=False
    def session(self, **kwargs):return self
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def execute_write(self,fn,*args):
        before=copy.deepcopy((self.nodes,self.edges,self.events))
        try:
            result=fn(self,*args)
            if self.fail:raise OSError('unavailable')
            return result
        except BaseException:
            self.nodes,self.edges,self.events=before
            raise
    def run(self, query, **p):
        self.queries.append((query,p))
        if query.startswith('CREATE CONSTRAINT'):return Result()
        run=p['run_id']
        if 'KGEvent' in query:
            key=(run,p['event_id']);self.events.setdefault(key,p['payload_hash'])
            return Result({'payload_hash':self.events[key]})
        if query.startswith('MERGE (n:KGNode'):
            key=(run,p['node_id']);self.nodes.setdefault(key,{}).update(p['properties']);return Result()
        a,b=(run,p['source']),(run,p['target'])
        if a not in self.nodes or b not in self.nodes:return Result({'written':0})
        relation=query.split('[r:')[1].split(']')[0]
        self.edges.add((a,relation,b));return Result({'written':1})
    def close(self):pass

class ProjectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.source=self.root/'snapshot.json';fixture(self.source,4)
        self.driver=FakeDriver();self.publisher=Neo4jStore(self.driver)
    def job(self, **kwargs):
        job=SnapshotGraph(self.source,self.root/'job',publisher=self.publisher,model=FakeEmbedding(),client=object(),k=1,batch_size=1,**kwargs)
        self.addCleanup(job.close);return job
    def test_driver_idempotence_namespace_payload_conflict_and_rollback(self):
        payload={'event_id':'test:1','nodes':[node('a','Paper',title='A'),node('b','PaperVersion')],'edges':[edge('a','b','HAS_VERSION')]}
        self.publisher.publish('one',payload);self.publisher.publish('one',payload)
        self.publisher.publish('two',payload)
        self.assertEqual(len(self.driver.edges),2)
        changed=copy.deepcopy(payload);changed['nodes'][0]['properties']['title']='changed'
        with self.assertRaisesRegex(ValueError,'payload changed'):self.publisher.publish('one',changed)
        bad={'event_id':'test:bad','nodes':[node('c','Paper')],'edges':[edge('c','absent','HAS_VERSION')]}
        with self.assertRaisesRegex(ValueError,'endpoint'):self.publisher.publish('one',bad)
        self.assertNotIn(('one','c'),self.driver.nodes)
        self.assertNotIn(('one','test:bad'),self.driver.events)
        for q,p in self.driver.queries:
            if not q.startswith('CREATE'):self.assertIn('run_id',p)
        with self.assertRaisesRegex(ValueError,'label'):self.publisher.publish('one',{'event_id':'bad','nodes':[node('x','Bad` label')],'edges':[]})
    def test_failed_delivery_retains_pending_and_replays_before_semantics(self):
        job=self.job();self.driver.fail=True
        with self.assertRaises(OSError):job.run()
        self.assertEqual(job._meta('phase'),'new')
        self.assertEqual(job.db.execute('SELECT count(*) FROM graph_outbox WHERE delivered=0').fetchone()[0],1)
        self.assertIsNone(job._meta('completed_at'))
        self.driver.fail=False
        # The committed event survives and is delivered without another semantic call.
        job._drain();self.assertEqual(len(self.driver.events),1)
        with job.db:job.db.execute('UPDATE graph_outbox SET delivered=0')
        job._drain();self.assertEqual(len(self.driver.events),1)
        source=next(v for v in self.driver.nodes.values() if 'snapshot_hash' in v)
        self.assertEqual(source['snapshot_hash'],job.snapshot['snapshot_hash'])
    def test_bolt_commit_before_ack_crash_reopens_and_replays_once(self):
        job=self.job()
        with job.db:
            job._enqueue('test','crash',{'nodes':[node('p','Paper',title='committed')],'edges':[]})
            job.db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE OF delivered ON graph_outbox BEGIN SELECT RAISE(ABORT,'ack interrupted'); END")
        import sqlite3
        with self.assertRaisesRegex(sqlite3.IntegrityError,'ack interrupted'):job._drain()
        self.assertEqual(len(self.driver.events),1)
        self.assertEqual(job.db.execute('SELECT delivered FROM graph_outbox').fetchone()[0],0)
        with job.db:job.db.execute('DROP TRIGGER fail_ack')
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
            committed={tuple(row) for row in job.db.execute('SELECT group_id,mention_id,audit FROM member_reviews')}
            self.assertTrue(committed)
            self.assertEqual(job.db.execute('SELECT count(*) FROM graph_outbox WHERE delivered=0').fetchone()[0],1)
            original_calls=list(calls)
            job.close()
            resumed=self.job();state=resumed.run()
            self.assertEqual(state['phase'],'complete')
            self.assertEqual(state['neo4j_projection']['pending'],0)
            self.assertEqual(len(calls),len(set(calls)))
            self.assertEqual(calls[:len(original_calls)],original_calls)
            recovered={tuple(row) for row in resumed.db.execute('SELECT group_id,mention_id,audit FROM member_reviews')}
            self.assertTrue(committed <= recovered)
            self.assertEqual(resumed.db.execute('SELECT count(*) FROM jev_requests').fetchone()[0],len(calls))
            import sqlite3
            db=sqlite3.connect(self.root/'job'/'semantic.sqlite')
            try:
                ids={row[0] for row in db.execute('SELECT entity_id FROM entities')}
                self.assertTrue(all((resumed._meta('neo4j_run_id'),eid) in self.driver.nodes for eid in ids))
            finally:db.close()

    def test_atomic_outbox_and_source_immutable_pins(self):
        job=self.job()
        with self.assertRaises(RuntimeError):
            with job.db:
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
    def test_group_finishes_before_other_name_and_sqlite_identity_matches(self):
        def clusters(vectors,**kwargs):
            mids=sorted(vectors);return {'clusters':[mids[:2],mids[2:]],'edges':[],'diagnostics':{'algorithm':'leiden-cpm'}}
        def reviewed(items,client,**kwargs):return [member_result(i,'not_belongs',.7) for i in items]
        with patch('kg_builder.snapshot_graph.cluster_problem_vectors',clusters),patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_group_members',reviewed):
            job=self.job();job.run()
            tasks=job.claim('worker',2);self.assertEqual(len(tasks),2)
            task=tasks[0]
            job.submit([{**{k:task[k] for k in ('task_id','lease_token','input_hash')},'result':{'group_id':task['group_id'],'name':'Area one','description':'Definition'}}])
            self.assertEqual(job.db.execute("SELECT count(*) FROM tasks WHERE status!='done'").fetchone()[0],1)
            areas=[v for v in self.driver.nodes.values() if v.get('name')=='Area one'];self.assertEqual(len(areas),1)
            self.assertEqual(sum(e[1]=='MEMBER_OF' for e in self.driver.edges),2)
            task=tasks[1]
            job.submit([{**{k:task[k] for k in ('task_id','lease_token','input_hash')},'result':{'group_id':task['group_id'],'name':'Area two','description':'Definition'}}])
            state=job.run();self.assertEqual(state['phase'],'complete');self.assertEqual(state['neo4j_projection']['pending'],0)
            import sqlite3
            db=sqlite3.connect(self.root/'job'/'semantic.sqlite')
            try:
                ids={r[0] for r in db.execute('SELECT entity_id FROM entities')}
                self.assertTrue(all((job._meta('neo4j_run_id'),eid) in self.driver.nodes for eid in ids))
                self.assertEqual(db.execute("SELECT count(*) FROM entities WHERE kind='ResearchProblem'").fetchone()[0],4)
            finally:db.close()
            self.assertLessEqual(state['neo4j_projection']['first_group_delivery_at'],state['completed_at'])
            self.assertFalse(any(e[1]=='SAME' for e in self.driver.edges))

if __name__=='__main__':unittest.main()

