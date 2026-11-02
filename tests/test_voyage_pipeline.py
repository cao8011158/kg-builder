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
from test_snapshot_graph import fixture


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
            semantic_cli.main(['--sidecar','unused','--embedding-provider','voyage',
                              '--embedding-model','voyage-4-large','--embedding-dimension','1024',
                              'search','query'])
            self.assertEqual(factory.call_args.kwargs['provider'],'voyage')
            factory.reset_mock()
            semantic_cli.main(['--sidecar','unused','--embedding-provider','voyage','entity','e:1'])
            factory.assert_not_called()

    def test_native_hnsw_1024_dimension_and_query_rejection(self):
        index = HNSWIndex({'a':unit(), 'b':unit(1)}, VoyageEmbeddingModel().model_id)
        self.assertIsNotNone(index._index)
        self.assertEqual(index._dimension, 1024)
        self.assertEqual(index.search(unit(), 1)[0]['id'], 'a')
        with self.assertRaises(ValueError): index.search([1.0, 0.0], 1)

    def test_snapshot_import_candidates_cluster_reuse_frozen_vectors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root/'snapshot.json'; snapshot(source)
            with patch.object(VoyageEmbeddingModel, 'encode', side_effect=AssertionError('No API reencoding')):
                job = SnapshotGraph(source, root/'job', k=2)
                try:
                    job._prepare_store()
                    job._candidates()
                    self.assertGreater(job.db.execute('SELECT count(*) FROM candidates').fetchone()[0], 0)
                    self.assertEqual(job._compatible_model().model_id, VoyageEmbeddingModel().model_id)
                    store = SemanticStore(root/'job'/'semantic.sqlite', job._compatible_model())
                    try:
                        proposal = store.cluster_problems(k=2)
                        self.assertEqual(proposal['diagnostics']['point_count'], 3)
                        self.assertLessEqual(proposal['diagnostics']['candidate_comparisons'], 6)
                        self.assertEqual(store.stats['embeddings_computed'], 0)
                        self.assertEqual(store.stats['embedding_cache_hits'], 3)
                    finally: store.close()
                finally: job.close()

    def test_resume_restores_exact_model_without_credentials_or_encoding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); source=root/'snapshot.json'; snapshot(source)
            job=SnapshotGraph(source,root/'job')
            try: job._prepare_store()
            finally: job.close()
            with patch.dict('os.environ',{},clear=True), patch.object(VoyageEmbeddingModel,'encode',side_effect=AssertionError('encode')):
                resumed=SnapshotGraph(source,root/'job',read_input=False)
                try:
                    self.assertEqual(resumed._compatible_model().model_id,VoyageEmbeddingModel().model_id)
                    self.assertEqual(resumed.status()['phase'],'imported')
                finally: resumed.close()
            wrong=RoleModel(); wrong.model_id='wrong-model'
            with self.assertRaisesRegex(ValueError,'differs from snapshot'):
                SnapshotGraph(source,root/'job',model=wrong,read_input=False)

    def test_snapshot_provenance_mixed_models_and_dimensions_fail_closed(self):
        for mode in ('text','mixed-model','mixed-dimension','wrong-declared-dimension'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp); source=root/'snapshot.json'; value=snapshot(source)
                if mode=='text': value['vectors'][0]['text']='other text'
                elif mode=='mixed-model': value['vectors'][0]['model_id']='foreign-model'
                elif mode=='mixed-dimension': value['vectors'][1]['vector']=[1.0,0.0]
                else:
                    for row in value['vectors']: row['vector']=[1.0,0.0]
                save(source,value)
                job=SnapshotGraph(source,root/'job')
                try:
                    with self.assertRaises(ValueError):job._prepare_store()
                    self.assertEqual(job._meta('phase'),'new')
                finally:job.close()

    def test_document_query_cache_roles_are_separate_and_fake_encode_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            model=RoleModel(); store=SemanticStore(Path(tmp)/'semantic.sqlite',model)
            try:
                self.assertEqual(store._embed(['same text'])[0],unit())
                self.assertEqual(store._embed_query(['same text'])[0],unit(1))
                store._embed(['same text']);store._embed_query(['same text'])
                self.assertEqual(model.documents,['same text']);self.assertEqual(model.queries,['same text'])
                self.assertEqual(store.db.execute('SELECT count(*) FROM embedding_cache').fetchone()[0],2)
                class Fake:
                    model_id='fake'
                    def encode(self,texts):return [[1.0,0.0] for _ in texts]
                store.model=Fake()
                self.assertEqual(store._embed_query(['fallback']),[[1.0,0.0]])
            finally:store.close()

    def test_public_search_uses_query_route_all_kinds_and_bounded_hot_hnsw(self):
        for count in (8,160):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as tmp:
                model=RoleModel();store=SemanticStore(Path(tmp)/'semantic.sqlite',model)
                try:
                    for i in range(count):
                        text=f'entity {i}\ndescription'
                        store.db.execute('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',
                            (f'e:{i}','Method',f'entity {i}','description','source','[]','r'))
                        store.db.execute('INSERT OR IGNORE INTO embedding_cache VALUES(?,?,?)',
                            (model.model_id,digest(text),json.dumps(unit())))
                    store.db.execute('INSERT INTO sources VALUES(?,?,?,?,?,?,?)',('x','p','v','title',None,None,'hash'))
                    store.db.execute('INSERT INTO contributions VALUES(?,?,?,?,?)',('c','x','c','a contribution','{}'))
                    store.db.execute('INSERT INTO research_areas VALUES(?,?,?,?)',('area:1','area','description','r'))
                    store.db.commit()
                    calls=[];original=HNSWIndex.search
                    def search(index,vector,k=10):
                        result=original(index,vector,k)
                        calls.append((k,len(result)))
                        return result
                    with patch.object(HNSWIndex,'search',search):
                        first=store.search('unknown query',limit=2,kind='Method')
                        builds=store.stats['dense_index_builds'];hits=store.stats['embedding_cache_hits']
                        store.search('unknown query',limit=2,kind='Method')
                    self.assertEqual(len(first['results']),2)
                    self.assertEqual(builds,1);self.assertEqual(store.stats['dense_index_builds'],1)
                    self.assertEqual(store.stats['embedding_cache_hits']-hits,1)
                    self.assertEqual(model.documents,[])
                    self.assertEqual(model.queries,['unknown query'])
                    self.assertEqual(calls,[(100,min(100,count)),(100,min(100,count))])
                    store.search('another query',kind='Contribution')
                    store.search('area query',kind='ResearchArea')
                    self.assertEqual(model.queries,['unknown query','another query','area query'])
                    self.assertEqual(model.documents,['a contribution','area\ndescription'])
                finally:store.close()


if __name__=='__main__':unittest.main()
