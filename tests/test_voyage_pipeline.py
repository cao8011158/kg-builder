"""Offline Voyage wiring checks with real hnswlib and synthetic frozen snapshots."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kg_builder.embeddings import VoyageEmbeddingModel
from kg_builder.semantic_store import SemanticStore, digest
from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash
from kg_builder.vector_index import HNSWIndex
from tests.postgres_snapshot_graph import fixture


def unit(axis=0, dimension=1024):
    result = [0.0] * dimension
    result[axis] = 1.0
    return result


def snapshot(path, n=3):
    value = fixture(path, n)
    model = VoyageEmbeddingModel()
    for row in value['vectors']:
        row['model_id'] = model.model_id
        row['vector'] = unit(0 if row['kind'] == 'ResearchProblem' else 1)
    save(path, value)
    return value


def save(path, value):
    value['snapshot_hash'] = canonical_hash({k:v for k,v in value.items() if k != 'snapshot_hash'})
    path.write_text(json.dumps(value), encoding='utf-8')


class RoleModel:
    model_id = VoyageEmbeddingModel().model_id
    query_model_id = VoyageEmbeddingModel().query_model_id
    def __init__(self):
        self.documents = []
        self.queries = []
    def encode(self, texts):
        self.documents.extend(texts)
        return [unit() for _ in texts]
    def encode_query(self, texts):
        self.queries.extend(texts)
        return [unit(1) for _ in texts]


class VoyagePipelineTests(unittest.TestCase):
    def test_validation_restores_frozen_model_by_default_and_structural_stays_offline(self):
        script=Path(__file__).resolve().parents[1]/'scripts'/'validate_snapshot_graph.py'
        spec=importlib.util.spec_from_file_location('test_validate_snapshot',script)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';snapshot(source)
            cases=root/'cases.json';cases.write_text(json.dumps({'queries':[{'query':'q','expected_paper_ids':['p']}]}))
            result={'structural':{'passed':True},'quality':{'status':'tested'}}
            with patch.object(module,'validate_graph',return_value=result) as validate, patch('builtins.print'):
                module.main(['--snapshot',str(source),'--job-dir',str(root/'job'),
                             '--output',str(root/'result.json'),'--cases',str(cases)])
                self.assertEqual(validate.call_args.args[3].model_id,VoyageEmbeddingModel().model_id)
            with patch.object(module,'validate_graph',return_value=result) as validate, \
                    patch('kg_builder.embeddings.embedding_model_from_id',side_effect=AssertionError('No encoder')), \
                    patch('builtins.print'):
                module.main(['--snapshot',str(source),'--job-dir',str(root/'job'),
                             '--output',str(root/'structural.json')])
                self.assertIsNone(validate.call_args.args[3])

    def test_preparation_snapshot_clis_forward_explicit_voyage_settings(self):
        from unittest.mock import MagicMock
        scripts=Path(__file__).resolve().parents[1]/'scripts'
        for filename, workflow_name in [('prepare_pipeline.py','PreparationWorkflow'),
                                         ('prepare_full_pipeline.py','FullPreparationWorkflow')]:
            with self.subTest(filename=filename):
                spec=importlib.util.spec_from_file_location('test_'+filename[:-3],scripts/filename)
                module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
                workflow=MagicMock()
                workflow.snapshot.return_value={'snapshot_hash':'frozen','counts':{}}
                model=RoleModel()
                with patch.object(module,workflow_name,return_value=workflow), \
                        patch('kg_builder.embeddings.create_embedding_model',return_value=model) as factory, \
                        patch('builtins.print'):
                    module.main(['--run-dir','unused','--run-id','test','snapshot','--output','unused.json',
                                 '--embedding-provider','voyage','--embedding-model','voyage-4-large',
                                 '--embedding-dimension','1024'])
                factory.assert_called_once_with(provider='voyage',model_name='voyage-4-large',
                                                dimension=1024,cache_dir=None,max_concurrency=8,
                                                requests_per_minute=2000,tokens_per_minute=3000000)
                self.assertIs(workflow.embedding,model)

    def test_semantic_cli_forwards_model_and_keeps_status_only_operations_offline(self):
        from unittest.mock import MagicMock
        from kg_builder import semantic_cli
        store=MagicMock();store.search.return_value={'results':[]};store.get_entity.return_value={}
        with patch.object(semantic_cli,'SemanticStore',return_value=store), \
                patch('kg_builder.embeddings.create_embedding_model',return_value=RoleModel()) as factory, \
                patch('builtins.print'):
            semantic_cli.main(['--run-id','unused','--embedding-provider','voyage',
                              '--embedding-model','voyage-4-large','--embedding-dimension','1024',
                              'search','query'])
            self.assertEqual(factory.call_args.kwargs['provider'],'voyage')
            factory.reset_mock()
            semantic_cli.main(['--run-id','unused','--embedding-provider','voyage','entity','e:1'])
            factory.assert_not_called()

    def test_native_hnsw_1024_dimension_and_query_rejection(self):
        index = HNSWIndex({'a':unit(), 'b':unit(1)}, VoyageEmbeddingModel().model_id)
        self.assertIsNotNone(index._index)
        self.assertEqual(index._dimension, 1024)
        self.assertEqual(index.search(unit(), 1)[0]['id'], 'a')
        with self.assertRaises(ValueError): index.search([1.0, 0.0], 1)







if __name__=='__main__':unittest.main()
