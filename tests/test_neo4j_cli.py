"""Offline public CLI forwarding and metadata-only graph operations."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from tests.test_snapshot_graph import fixture


def script(name):
    spec=importlib.util.spec_from_file_location('cli_'+name,Path(__file__).resolve().parents[1]/'scripts'/f'{name}.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


class Neo4jCliTests(unittest.TestCase):
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

    def test_run_forwards_voyage_options_and_env_publisher_and_closes(self):
        cli=script('build_snapshot_graph');job=MagicMock();model=MagicMock();job.model=model;job.run.return_value={}
        publisher=MagicMock()
        with patch.object(cli,'SnapshotGraph',return_value=job) as factory, \
             patch('kg_builder.embeddings.create_embedding_model',return_value=model) as embedding, \
             patch('kg_builder.decision_api.JevClient'), \
             patch('kg_builder.neo4j_store.Neo4jStore.from_env',return_value=publisher):
            self.invoke(cli,['--snapshot','s.json','--job-dir','job','--neo4j','--embedding-provider','voyage',
                '--voyage-concurrency','6','--voyage-rpm','123','--voyage-tpm','456','run'])
        self.assertEqual(embedding.call_args.kwargs['max_concurrency'],6)
        self.assertEqual(embedding.call_args.kwargs['provider'],'voyage')
        self.assertEqual(factory.call_args.kwargs['voyage_options'],dict(max_concurrency=6,requests_per_minute=123,tokens_per_minute=456))
        self.assertIs(factory.call_args.kwargs['publisher'],publisher)
        job.close.assert_called_once();publisher.close.assert_called_once();model.close.assert_called_once()

    def test_implicit_model_is_restored_by_graph_and_not_substituted(self):
        cli=script('build_snapshot_graph');job=MagicMock();job.model=None;job.run.return_value={}
        with patch.object(cli,'SnapshotGraph',return_value=job) as factory, \
             patch('kg_builder.embeddings.create_embedding_model',side_effect=AssertionError('unexpected override')), \
             patch('kg_builder.decision_api.JevClient'):
            self.invoke(cli,['--snapshot','s','--job-dir','j','--voyage-concurrency','5','run'])
        self.assertIsNone(factory.call_args.kwargs['model'])
        self.assertEqual(factory.call_args.kwargs['voyage_options']['max_concurrency'],5)

    def test_missing_neo4j_env_fails_before_decisions_without_secret_values(self):
        cli=script('build_snapshot_graph')
        with patch.dict('os.environ',{'NEO4J_PASSWORD':'test-secret-do-not-print'},clear=True), \
             patch('kg_builder.decision_api.JevClient',side_effect=AssertionError('decision client')):
            with self.assertRaisesRegex(ValueError,'NEO4J_URI') as caught:
                self.invoke(cli,['--snapshot','s','--job-dir','j','--neo4j','run'])
            self.assertNotIn('test-secret-do-not-print',str(caught.exception))

    def test_preparation_snapshot_forwards_options_and_closes_model(self):
        for name,owner in [('prepare_pipeline','PreparationWorkflow'),('prepare_full_pipeline','FullPreparationWorkflow')]:
            with self.subTest(cli=name):
                cli=script(name);wf=MagicMock();wf.snapshot.return_value={'snapshot_hash':'hash','counts':{}}
                model=MagicMock()
                with patch.object(cli,owner,return_value=wf), \
                     patch('kg_builder.embeddings.create_embedding_model',return_value=model) as factory:
                    self.invoke(cli,['--run-id','offline','--run-dir','run','snapshot','--output','new.json',
                        '--embedding-provider','voyage','--voyage-concurrency','7','--voyage-rpm','88','--voyage-tpm','999'])
                self.assertEqual(factory.call_args.kwargs['max_concurrency'],7)
                self.assertEqual(factory.call_args.kwargs['requests_per_minute'],88)
                self.assertEqual(factory.call_args.kwargs['tokens_per_minute'],999)
                self.assertIs(wf.embedding,model);wf.close.assert_called_once();model.close.assert_called_once()

    def test_semantic_search_forwards_options(self):
        from kg_builder import semantic_cli
        model=MagicMock();store=MagicMock();store.search.return_value={}
        with patch.object(semantic_cli,'SemanticStore',return_value=store), \
             patch('kg_builder.embeddings.create_embedding_model',return_value=model) as factory:
            self.invoke(semantic_cli,['--sidecar','s','--embedding-provider','voyage','--voyage-concurrency','4','search','memory'])
        self.assertEqual(factory.call_args.kwargs['max_concurrency'],4)
        store.close.assert_called_once();model.close.assert_called_once()

    def test_validation_restores_frozen_model_with_options(self):
        cli=script('validate_snapshot_graph');model=MagicMock();model.model_id='voyage-frozen'
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cases=root/'cases.json';cases.write_text(json.dumps({'queries':[{'expected_paper_ids':['p']}]}))
            with patch('kg_builder.snapshot_graph.read_snapshot',return_value=({'vectors':[{'model_id':'voyage-frozen'}]},'hash')), \
                 patch('kg_builder.embeddings.embedding_model_from_id',return_value=model) as restore, \
                 patch('kg_builder.embeddings.create_embedding_model',side_effect=AssertionError('BGE override')), \
                 patch.object(cli,'validate_graph',return_value={'structural':{'passed':True},'quality':{'status':'ok'}}):
                self.assertEqual(self.invoke(cli,['--snapshot',str(root/'s'),'--job-dir',str(root/'j'),
                    '--output',str(root/'out.json'),'--cases',str(cases),'--voyage-concurrency','2']),0)
            self.assertEqual(restore.call_args.args,('voyage-frozen',))
            self.assertEqual(restore.call_args.kwargs['max_concurrency'],2);model.close.assert_called_once()

    def test_neo4j_submit_reviews_publishes_before_other_name_and_replay_is_cached(self):
        from contextlib import closing
        import sqlite3
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
                job=SnapshotGraph(source,root/'job',publisher=publisher,model=FakeEmbedding(),client=object(),k=1,batch_size=1)
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
                create_client.assert_called_once_with(model='jev-1.14.0',max_concurrency=4)
                self.assertEqual(len(calls),1)
                self.assertEqual(len(calls[0]),2)
                with closing(sqlite3.connect(root/'job'/'graph.sqlite')) as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE status!='done'").fetchone()[0],1)
                    timing=dict(db.execute("SELECT name,seconds FROM stage WHERE name IN ('jev_area_member_wall','process_cpu:jev_area_member_wall')"))
                    self.assertAlmostEqual(timing['jev_area_member_wall']-before_timing['stage_seconds'].get('jev_area_member_wall',0),7.0)
                    self.assertAlmostEqual(timing['process_cpu:jev_area_member_wall']-before_timing['stage_process_cpu_seconds'].get('jev_area_member_wall',0),3.0)
                    before=db.execute('SELECT group_id,mention_id,audit FROM member_reviews WHERE group_id=? ORDER BY mention_id',(task['group_id'],)).fetchall()
                self.assertEqual(len(before),2)
                first_events=len(publisher.events)
                self.assertTrue(any(node.get('properties',{}).get('name')=='Published first area'
                    for event in publisher.events.values() for node in event['nodes']))
                self.invoke(cli,argv)
                self.assertEqual(len(calls),1)
                self.assertEqual(len(publisher.events),first_events)
                with closing(sqlite3.connect(root/'job'/'graph.sqlite')) as db:
                    self.assertEqual(db.execute('SELECT group_id,mention_id,audit FROM member_reviews WHERE group_id=? ORDER BY mention_id',(task['group_id'],)).fetchall(),before)
                    self.assertEqual(db.execute("SELECT count(*) FROM tasks WHERE status!='done'").fetchone()[0],1)
            self.assertEqual(publisher.closes,2)
            self.assertEqual(client.close.call_count,2)

    def test_provider_choice_is_validated(self):
        cli=script('build_snapshot_graph')
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):
            cli.main(['--snapshot','s','--job-dir','j','--embedding-provider','invalid','status'])


if __name__=='__main__':unittest.main()
