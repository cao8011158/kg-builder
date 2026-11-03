"""Native PostgreSQL snapshot batching; explicitly run with KG_TEST_DATABASE_URL.

Only UUID-isolated fixture schemas are created/dropped. No embedding/API services
are called: real item review and snapshot export consume deterministic adapters.
"""
import asyncio
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

from kg_builder.preparation import PreparationWorkflow, snapshot_digest
from tests.postgres_fixtures import PostgresCase, source, draft, scope


class RecordingEncoder:
    model_id='test:windowed-unit-four-dimensional'

    def __init__(self):
        self.calls=[]
        self.closed=False

    @staticmethod
    def vector(text):
        position=hashlib.sha256(text.encode('utf-8')).digest()[0] % 4
        return [float(index==position) for index in range(4)]

    def encode(self,texts):
        assert not self.closed
        self.calls.append(list(texts))
        return [self.vector(text) for text in texts]

    def close(self):
        self.closed=True


def approve_items(items,client,min_confidence):
    return [dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],
        passed=True,probability=.99,confidence=.2,decision_metric='probability',issues=[],raw={}) for x in items]


class VoyageSnapshotWindowTests(PostgresCase):
    def setUp(self):
        super().setUp()
        self.w=self.workflow(PreparationWorkflow,target=1,client=object(),scope_fn=scope,
            review_fn=approve_items,review_policy='item-v1',per_query=1,
            collectors={'openalex':lambda *args:[], 'arxiv':lambda *args:[]})
        self.w.add_source(source(88001),'arxiv','native snapshot window','agent_memory')
        asyncio.run(self._prepare())

    async def _prepare(self):
        running=asyncio.create_task(self.w.run(poll_seconds=.01))
        try:
            async def submit_when_claimable():
                while True:
                    tasks=self.w.claim('extract',1,'native-window-worker')
                    if tasks:break
                    await asyncio.sleep(.01)
                task=tasks[0];payload=draft(task)
                payload['contributions']=[dict(key=task['version_id']+f'#c{i:04d}',
                    description=f'Unique contribution {i:04d} for deterministic snapshot ordering.') for i in range(270)]
                payload['research_problems']=[dict(key='problem_1',name='Recall problem',description='Recall the stored history.')]
                payload['methods']=[dict(key='method_1',name='Persistent memory',description='Store agent history.',role='proposed')]
                payload['benchmarks']=[dict(key='benchmark_1',name='Recall evaluation',description='Evaluate history recall.',roles=['used'],version_or_subset=None)]
                self.assertEqual(self.w.submit([dict(task_id=task['task_id'],input_hash=task['input_hash'],
                    lease_token=task['lease_token'],result=payload)])[0]['status'],'accepted')
            await asyncio.wait_for(submit_when_claimable(),20)
            state=await asyncio.wait_for(running,20)
            self.assertEqual(state['review'],{'ready':1})
            self.assertIsNone(state['error'])
        finally:
            if not running.done():
                running.cancel()
                with contextlib.suppress(asyncio.CancelledError):await running

    def _assert_output(self,snapshot,encoder,chunk_sizes):
        self.assertEqual(snapshot['snapshot_hash'],snapshot_digest(snapshot))
        expected=[]
        for extraction in snapshot['extractions']:
            for field,kind in [('research_problems','ResearchProblem'),('methods','Method'),
                               ('benchmarks','Benchmark'),('contributions','Contribution')]:
                for item in extraction[field]:
                    text=(item['name']+'\n' if item.get('name') else '')+item['description']
                    expected.append(dict(item_key=extraction['paper_id']+'::'+item['key'],kind=kind,text=text,
                        model_id=encoder.model_id,vector=encoder.vector(text)))
        self.assertEqual(len(expected),273)
        self.assertEqual(snapshot['vectors'],expected)
        self.assertEqual([len(call) for call in encoder.calls],chunk_sizes)
        self.assertEqual([text for call in encoder.calls for text in call],[row['text'] for row in expected])
        self.assertTrue(all(len(row['vector'])==4 for row in snapshot['vectors']))
        approved=snapshot['review_metadata'][snapshot['extractions'][0]['paper_id']]['items']
        self.assertEqual(len(approved),273)
        self.assertTrue(all(item['state']=='approved' for item in approved))

    def _exercise_window(self,window,expected_chunks):
        encoder=RecordingEncoder()
        if window is not None:encoder.encode_window_size=window
        self.w.embedding=encoder
        path=Path(self.tmp.name)/'snapshot.json'
        first=self.w.snapshot(path)
        self._assert_output(first,encoder,expected_chunks)
        raw=path.read_bytes();mtime=path.stat().st_mtime_ns
        encoder.calls.clear()
        second=self.w.snapshot(path)
        self._assert_output(second,encoder,expected_chunks)
        self.assertEqual(first,second)
        self.assertEqual(path.read_bytes(),raw)
        self.assertEqual(path.stat().st_mtime_ns,mtime)
        encoder.close()
        self.assertTrue(encoder.closed)

    def test_public_snapshot_uses_64_input_windows(self):
        self._exercise_window(64,[64,64,64,64,17])

    def test_public_snapshot_uses_256_input_windows(self):
        self._exercise_window(256,[256,17])

    def test_generic_encoder_retains_32_input_windows(self):
        self._exercise_window(None,[32]*8+[17])

    def test_public_snapshot_cli_uses_window_and_closes_native_workflow_and_model(self):
        script=Path(__file__).resolve().parents[1]/'scripts'/'prepare_pipeline.py'
        spec=importlib.util.spec_from_file_location('native_snapshot_cli',script)
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        encoder=RecordingEncoder();encoder.encode_window_size=256
        created=[]
        def native_workflow(run_dir,**kwargs):
            wf=PreparationWorkflow(run_dir,database_url=self.database_url,client=object(),**kwargs)
            self._workflows.append(wf)
            close_spy=patch.object(wf,'close',wraps=wf.close)
            spy=close_spy.start();self.addCleanup(close_spy.stop)
            created.append((wf,spy));return wf
        output=Path(self.tmp.name)/'cli-snapshot.json'
        with patch.object(cli,'PreparationWorkflow',side_effect=native_workflow), \
             patch('kg_builder.embeddings.create_embedding_model',return_value=encoder) as factory, \
             contextlib.redirect_stdout(io.StringIO()):
            cli.main(['--run-dir',self.tmp.name,'--run-id',self.w.store.run_id,'snapshot','--output',str(output),
                '--embedding-provider','voyage','--voyage-concurrency','8'])
        self._assert_output(json.loads(output.read_text(encoding='utf-8')),encoder,[256,17])
        self.assertEqual(factory.call_args.kwargs['max_concurrency'],8)
        self.assertTrue(encoder.closed)
        self.assertEqual(len(created),1)
        created[0][1].assert_called_once()
