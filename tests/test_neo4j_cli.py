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
            self.invoke(semantic_cli,['--run-id','s','--embedding-provider','voyage','--voyage-concurrency','4','search','memory'])
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

    def test_graph_run_and_neo4j_submit_default_shared_budget_is_eight(self):
        cli=script('build_snapshot_graph')
        with tempfile.TemporaryDirectory() as tmp:
            payload=Path(tmp)/'names.json';payload.write_text('[]')
            for command in (['run'],['submit','--input',str(payload)]):
                job=MagicMock();job.model=None
                job.run.return_value={};job.submit.return_value={}
                with patch.object(cli,'SnapshotGraph',return_value=job) as graph, \
                     patch('kg_builder.decision_api.JevClient') as create_client, \
                     patch('kg_builder.neo4j_store.Neo4jStore.from_env'):
                    self.invoke(cli,['--snapshot','s','--job-dir','j','--run-id','explicit','--neo4j',*command])
                self.assertEqual(create_client.call_args.kwargs['max_concurrency'],8)
                self.assertEqual(graph.call_args.kwargs['run_id'],'explicit')
                self.assertIsNone(graph.call_args.kwargs['k'])
                self.assertIsNone(graph.call_args.kwargs['name_mode'])

    def test_semantic_ingest_selects_native_source_run(self):
        from kg_builder import semantic_cli
        store=MagicMock();store.ingest.return_value={}
        with patch.object(semantic_cli,'SemanticStore',return_value=store) as factory:
            self.invoke(semantic_cli,['--run-id','semantic','ingest','--source-run-id','source','--extraction-id','extr:one'])
        factory.assert_called_once_with('semantic',model=None)
        store.ingest.assert_called_once_with('source',['extr:one'])

    def test_provider_choice_is_validated(self):
        cli=script('build_snapshot_graph')
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):
            cli.main(['--snapshot','s','--job-dir','j','--embedding-provider','invalid','status'])


if __name__=='__main__':unittest.main()
