"""Native PostgreSQL public graph CLI behavior, with offline API doubles."""
import json
import contextlib
import io
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from tests.test_snapshot_graph import fixture
from tests.test_neo4j_cli import script
from tests.postgres_graph_cli_fixtures import NativeGraphCase


class NativeNeo4jCliTests(NativeGraphCase):
    def invoke(self,module,args):
        with contextlib.redirect_stdout(io.StringIO()):return module.main(args)

    def test_status_and_claim_cold_hot_do_not_construct_api_clients(self):
        cli=script('build_snapshot_graph')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=1)
            base=['--snapshot',str(source),'--job-dir',str(root/'job'),'--neo4j',
                  '--embedding-provider','voyage','--voyage-concurrency','3']
            with patch('kg_builder.embeddings.create_embedding_model',side_effect=AssertionError('embedding client')), \
                 patch('kg_builder.decision_api.JevClient',side_effect=AssertionError('decision client')), \
                 patch('kg_builder.neo4j_store.Neo4jStore.from_env',side_effect=AssertionError('graph client')):
                self.invoke(cli,base+['status'])
                self.invoke(cli,base+['status'])
                self.invoke(cli,base+['claim','--worker','offline','--output',str(root/'claim.json')])
                self.assertEqual(json.loads((root/'claim.json').read_text()),[])
                with self.connection(root/'job'):pass

    def test_neo4j_submit_reviews_publishes_before_other_name_and_replay_is_cached(self):
        from contextlib import closing
        from kg_builder.snapshot_graph import SnapshotGraph
        from tests.test_snapshot_graph import FakeEmbedding, pairs, member_result
        class Publisher:
            def __init__(self):self.events={};self.closes=0
            def publish(self,run_id,payload):self.events[(run_id,payload['event_id'])]=payload
            def close(self):self.closes+=1
        publisher=Publisher();client=MagicMock();calls=[]
        clock={'wall':100.0,'cpu':20.0}
        def clusters(vectors,**kwargs):
            mids=sorted(vectors)
            return {'clusters':[mids[:2],mids[2:]],'edges':[],'diagnostics':{'algorithm':'leiden-cpm'}}
        def reviewed(items,actual_client,**kwargs):
            self.assertIs(actual_client,client)
            calls.append(items)
            clock['wall']+=7.0;clock['cpu']+=3.0
            return [member_result(item) for item in items]
        cli=script('build_snapshot_graph')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=4)
            with patch('kg_builder.snapshot_graph.cluster_problem_vectors',clusters), \
                 patch('kg_builder.decision_api.judge_pairs',pairs), \
                 patch('kg_builder.decision_api.review_group_members',side_effect=lambda items,*args,**kwargs:[member_result(item) for item in items]):
                job=self.make_job(source,root/'job',publisher=publisher,model=FakeEmbedding(),client=object(),k=1,batch_size=1)
                try:
                    job.run();tasks=job.claim('name-worker',2)
                    self.assertEqual(len(tasks),2)
                    before_timing=job.status()
                finally:job.close()
            task=tasks[0];submission=root/'name.json'
            submission.write_text(json.dumps([{**{key:task[key] for key in ('task_id','lease_token','input_hash')},
                'result':{'group_id':task['group_id'],'name':'Published first area','description':'Grounded summary'}}]),encoding='utf-8')
            argv=['--snapshot',str(source),'--job-dir',str(root/'job'),'--neo4j','submit','--input',str(submission)]
            with patch('kg_builder.neo4j_store.Neo4jStore.from_env',return_value=publisher), \
                 patch('kg_builder.decision_api.JevClient',return_value=client) as create_client, \
                 patch('kg_builder.decision_api.review_group_members',side_effect=reviewed):
                with patch('kg_builder.snapshot_graph.time.perf_counter',side_effect=lambda:clock['wall']), \
                     patch('kg_builder.snapshot_graph.time.process_time',side_effect=lambda:clock['cpu']):
                    self.invoke(cli,argv)
                create_client.assert_called_once_with(model='jev-1.14.0',max_concurrency=8)
                self.assertEqual(len(calls),1)
                self.assertEqual(len(calls[0]),2)
                with self.connection(root/'job') as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE status!='done'").fetchone()['count'],1)
                    timing={r['name']:r['seconds'] for r in db.execute("SELECT name,seconds FROM stage WHERE name IN ('jev_area_member_wall','process_cpu:jev_area_member_wall')")}
                    self.assertAlmostEqual(timing['jev_area_member_wall']-before_timing['stage_seconds'].get('jev_area_member_wall',0),7.0)
                    self.assertAlmostEqual(timing['process_cpu:jev_area_member_wall']-before_timing['stage_process_cpu_seconds'].get('jev_area_member_wall',0),3.0)
                    before=db.execute('SELECT group_id,mention_id,audit FROM member_reviews WHERE group_id=%s ORDER BY mention_id',(task['group_id'],)).fetchall()
                self.assertEqual(len(before),2)
                first_events=len(publisher.events)
                self.assertTrue(any(node.get('properties',{}).get('name')=='Published first area'
                    for event in publisher.events.values() for node in event['nodes']))
                self.invoke(cli,argv)
                self.assertEqual(len(calls),1)
                self.assertEqual(len(publisher.events),first_events)
                with self.connection(root/'job') as db:
                    self.assertEqual(db.execute('SELECT group_id,mention_id,audit FROM member_reviews WHERE group_id=%s ORDER BY mention_id',(task['group_id'],)).fetchall(),before)
                    self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE status!='done'").fetchone()['count'],1)
            self.assertEqual(publisher.closes,2)
            self.assertEqual(client.close.call_count,2)
