import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION
from kg_builder.semantic_store import SemanticStore


def fixture(path, n=2):
    papers=[];extractions=[];vectors=[]
    for n in range(1,n+1):
        pid=f'paper:{n}';vid=f'{pid}@v1';url=f'https://example.org/{n}'
        papers.append({'paper_id':pid,'title':f'Title {n}','version_id':vid,
                       'paper_version':{'version_label':'v1','source_url':url},'text':f'Text {n}', 'source_hash':None})
        papers[-1]['source_hash']=canonical_hash({**{k:papers[-1][k] for k in ('paper_id','version_id','title','text')},'version_label':'v1','source_url':url})
        items={
            'research_problems':[{'key':'problem_1','name':f'Memory problem {n}','description':'Recall over time'}],
            'methods':[{'key':'method_1','name':f'Method {n}','description':'Persistent memory','role':'proposed'}],
            'benchmarks':[{'key':'benchmark_1','name':f'Benchmark {n}','description':'Memory task','roles':['used'],'version_or_subset':None}],
            'contributions':[{'key':f'{vid}#c','description':'A contribution'}]}
        extractions.append({'schema_version':RETRIEVAL_SCHEMA_VERSION,'paper_id':pid,'version_id':vid,
                            'paper_version':papers[-1]['paper_version'],'extractor_type':'model','extractor_version':'v1',
                            'source_scope':'abstract_only','status':'reviewed',**items})
        for kind,field in [('ResearchProblem','research_problems'),('Method','methods'),('Benchmark','benchmarks'),('Contribution','contributions')]:
            item=items[field][0]
            vectors.append({'item_key':pid+'::'+item['key'],'kind':kind,'text':item['name']+'\n'+item['description'] if kind!='Contribution' else item['description'],
                            'model_id':'test-embed','vector':[1.0,0.0] if kind=='ResearchProblem' else ([0.0,1.0] if kind=='Method' else [0.70710678,0.70710678])})
    value={'schema_version':'research-kg-prepared-snapshot/1','run_id':'test','papers':papers,
           'extractions':extractions,'vectors':vectors,'counts':{'accepted':n}}
    value['snapshot_hash']=canonical_hash(value)
    path.write_text(json.dumps(value),encoding='utf-8')
    return value


def pairs(items,client,**kwargs):
    return [{'pair_id':p['pair_id'],'decision':'different' if p['kind']=='Benchmark' else 'same','raw':{'test':True}} for p in items]


def groups(items,client,**kwargs):
    return [{'group_id':g['group_id'],'passed':True,'issues':[],'raw':{'test':True}} for g in items]


def member_result(item,choice='belongs',probability=0.2,confidence=0.9):
    return {'membership_id':item['membership_id'],'choice':choice,'probability':probability,
            'confidence':confidence,'decision_metric':'probability',
            'excluded':choice=='not_belongs' and probability>0.7,
            'raw':{'answers':{'membership':{'choice':choice,'confidence':confidence,
                    'probabilities':{'belongs':1-probability,'not_belongs':probability,'insufficient':0}}}}}


class FakeEmbedding:
    model_id='test-embed'
    def encode(self,texts): return [[1.0,0.0] for _ in texts]


import os
import uuid
from psycopg import sql, connect
from kg_builder.postgres_graph import GraphStore, graph_run_identity

class GraphPostgresCase(unittest.TestCase):
    def setUp(self):
        self.database_url=os.environ.get('KG_TEST_DATABASE_URL')
        if not self.database_url:raise RuntimeError('KG_TEST_DATABASE_URL is required')
        self.graphs=[]
        original=SnapshotGraph.__init__
        def initialize(graph,*args,**kwargs):
            kwargs.setdefault('database_url',self.database_url)
            try:return original(graph,*args,**kwargs)
            finally:
                if hasattr(graph,'graph_store'):self.graphs.append(graph)
        patcher=patch.object(SnapshotGraph,'__init__',initialize)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.clean_graphs)
        # Direct SemanticStore/GraphStore opens use the same disposable database.
        env=patch.dict(os.environ,{'KG_DATABASE_URL':self.database_url})
        env.start();self.addCleanup(env.stop)

    def clean_graphs(self):
        schemas=set()
        for graph in self.graphs:
            schemas.add(graph.graph_store.schema)
            graph.close()
        with connect(self.database_url,autocommit=True) as db:
            for schema in schemas:
                db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))

class SnapshotGraphTests(GraphPostgresCase):
    def setUp(self):
        super().setUp()
        from kg_builder import snapshot_graph
        original=snapshot_graph.SnapshotGraph.__init__
        def legacy_init(instance, *args, **kwargs):
            kwargs.setdefault('graph_policy','legacy')
            return original(instance,*args,**kwargs)
        patcher=patch.object(snapshot_graph.SnapshotGraph,'__init__',legacy_init)
        patcher.start();self.addCleanup(patcher.stop)

    def test_jev_http_intervals_are_distinct_from_backoff_and_unique_on_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            job=SnapshotGraph(source,root/'job')
            raw={'raw':{'requests':[
                {'request_id':'first','request_duration_s':2,'total_elapsed_s':7,'attempts':2,
                 'started_at':10,'finished_at':17,'http_intervals':[
                     {'started_at':10,'finished_at':11},{'started_at':16,'finished_at':17}]},
                {'request_id':'second','request_duration_s':1,'total_elapsed_s':1,'attempts':1,
                 'started_at':10.5,'finished_at':11.5,'http_intervals':[
                     {'started_at':10.5,'finished_at':11.5}]}]}}
            with job.db.transaction():job._record_requests(raw);job._record_requests(raw)
            state=job.status()
            self.assertEqual(state['jev_request_count'],2)
            self.assertEqual(state['jev_request_peak_concurrency'],2)
            self.assertAlmostEqual(state['jev_request_wall_union_seconds'],2.5)
            self.assertEqual(state['jev_http_attempt_intervals'],3)
            self.assertEqual(state['stage_seconds']['jev_api_request_seconds'],3)
            job.close()

    def test_item_snapshot_requires_reviewed_exact_approved_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'snapshot.json'
            value=fixture(source)
            value['config']={'review_policy':'item-v1'}
            value['review_metadata']={x['paper_id']:{'policy':'item-v1','items':[
                {'field':field,'key':item['key'],'state':'approved','approved_payload_hash':canonical_hash(item)}
                for field in ('research_problems','methods','benchmarks','contributions')
                for item in x[field]]} for x in value['extractions']}
            def save():
                value['snapshot_hash']=canonical_hash({k:v for k,v in value.items() if k!='snapshot_hash'})
                source.write_text(json.dumps(value),encoding='utf-8')
            save()
            job=SnapshotGraph(source,Path(tmp)/'job');job.close()
            value['review_metadata']['paper:1']['items'][0]['state']='dropped';save()
            with self.assertRaisesRegex(ValueError,'differs from approved'):
                SnapshotGraph(source,Path(tmp)/'other')
            value['review_metadata']['paper:1']['items'][0]['state']='approved'
            value['extractions'][0]['status']='draft';save()
            with self.assertRaisesRegex(ValueError,'Unreviewed'):
                SnapshotGraph(source,Path(tmp)/'third')

    def test_parallel_api_names_bind_out_of_order_and_release_failed_own_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=4)
            barrier=threading.Barrier(2)
            lock=threading.Lock();active=[0];peak=[0];calls=[0]
            class Namer:
                def generate_json(self,prompt,state,*,required_keys):
                    with lock:
                        active[0]+=1;peak[0]=max(peak[0],active[0]);calls[0]+=1
                        first_wave=calls[0]<=2
                    try:
                        if first_wave:barrier.wait(timeout=5)
                        # Reversed completion demonstrates binding by group ID.
                        if state['members'][0]['paper_id']=='paper:1':
                            threading.Event().wait(.04)
                        return {'group_id':state['group_id'],'name':state['members'][0]['paper_id'],
                                'description':'Source grounded'}
                    finally:
                        with lock:active[0]-=1
            def distinct(items,client,**kwargs):
                return [{**r,'decision':'different'} for r in pairs(items,client)]
            with patch('kg_builder.decision_api.judge_pairs',distinct),patch('kg_builder.decision_api.review_groups',groups):
                job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding(),
                                  name_mode='api',namer=Namer(),name_concurrency=2)
                state=job.run()
                self.assertEqual(state['phase'],'complete')
                self.assertEqual(state['name_tasks']['done'],4)
                self.assertEqual(peak[0],2)
                for row in job.db.execute("SELECT g.name,g.members_json FROM groups g WHERE g.kind='ResearchProblem'"):
                    mid=json.loads(row['members_json'])[0]
                    paper=SemanticStore(graph_run_identity(root/'job'))
                    try:
                        expected=paper.db.execute('SELECT s.paper_id FROM mentions m JOIN sources s ON m.extraction_id=s.extraction_id WHERE mention_id=%s',(mid,)).fetchone()['paper_id']
                    finally:paper.close()
                    self.assertEqual(row['name'],expected)
                job.close()
            class FailingNamer:
                def generate_json(self,prompt,state,*,required_keys):
                    if state['members'][0]['paper_id']=='paper:1':raise RuntimeError('naming failed')
                    return {'group_id':state['group_id'],'name':'Other','description':'Good'}
            with patch('kg_builder.decision_api.judge_pairs',distinct),patch('kg_builder.decision_api.review_groups',groups):
                job=SnapshotGraph(source,root/'failed',client=object(),k=1,model=FakeEmbedding(),
                                  name_mode='api',namer=FailingNamer(),name_concurrency=2)
                with self.assertRaisesRegex(RuntimeError,'naming failed'):job.run()
                self.assertEqual(job.status()['name_tasks'],{'done':1,'queued':3})
                job.close()

    def test_build_names_recovery_retrieval_and_immutable_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';value=fixture(source)
            with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',groups):
                job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['phase'],'grouped')
                self.assertEqual(state['candidate_pairs']['same'],2)
                self.assertEqual(state['candidate_pairs']['different'],1)
                self.assertEqual(state['name_tasks']['queued'],1)
                task=job.claim('worker')[0]
                job.close()
                with patch('kg_builder.snapshot_graph.read_snapshot',side_effect=AssertionError('should use indexed job metadata')):
                    quick=SnapshotGraph(source,root/'job',k=1,read_input=False)
                    self.assertEqual(quick.status()['name_tasks']['leased'],1)
                    quick.close()
                job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
                value_to_submit={'group_id':task['group_id'],'name':'Long term agent recall','description':'Retain and recall context'}
                submission={'task_id':task['task_id'],'lease_token':task['lease_token'],
                            'input_hash':task['input_hash'],'result':value_to_submit}
                self.assertEqual(job.submit([submission])[0]['status'],'done')
                self.assertEqual(job.submit([submission])[0]['status'],'unchanged')
                self.assertEqual(job.run()['phase'],'complete')
                self.assertEqual(job.status()['pending_mentions'],0)
                self.assertEqual(job.status()['index_status'],'verified')
                self.assertEqual(job.status()['index_builds'],4)
                job.close()
            master=GraphStore(graph_run_identity(root/'job'))
            try:self.assertEqual(master.db.execute('SELECT abstract FROM paper_versions WHERE version_id=%s',('paper:1@v1',)).fetchone()['abstract'],'Text 1')
            finally:master.close()
            store=SemanticStore(graph_run_identity(root/'job'))
            try:
                self.assertEqual(store.db.execute('SELECT count(*) FROM entities').fetchone()['count'],4)
                self.assertEqual(store.db.execute('SELECT count(*) FROM embedding_cache').fetchone()['count'],8)
                result=store.search('Long term agent recall',kind='ResearchProblem')
                self.assertTrue(result['results'])
                self.assertEqual(len(store.get_entity(result['results'][0]['entity_id'])['mentions']),2)
                class NoEmbedding:
                    model_id='test-embed'
                    def encode(self,texts): raise AssertionError('Snapshot vectors should be reused')
                store.model=NoEmbedding()
                self.assertEqual(store._embed(['Memory problem 1\nRecall over time'])[0],[1.0,0.0])
                self.assertEqual(store.stats['embeddings_computed'],0)
            finally:store.close()
            value['papers'][0]['title']='Tampered'
            source.write_text(json.dumps(value),encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'digest mismatch'):
                SnapshotGraph(source,root/'job')

    def test_restored_snapshot_path_and_timestamp_accept_same_bytes_only(self):
        import os
        import shutil
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
            job.close()
            restored=root/'restored.json'
            shutil.copyfile(source,restored)
            stat=restored.stat()
            os.utime(restored,ns=(stat.st_atime_ns,stat.st_mtime_ns+123456789))
            job=SnapshotGraph(restored,root/'job',k=1,read_input=False)
            self.assertEqual(job.status()['phase'],'new')
            job.close()
            # Changed bytes with a valid newly computed canonical digest remain a different job input.
            value=json.loads(restored.read_text())
            value['run_id']='altered'
            value['snapshot_hash']=canonical_hash({k:v for k,v in value.items() if k!='snapshot_hash'})
            restored.write_text(json.dumps(value),encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'different immutable snapshot'):
                SnapshotGraph(restored,root/'job',k=1,read_input=False)

    def test_coherence_failure_preserves_pending_and_source_fingerprint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';value=fixture(source)
            value['papers'][0]['title']='Changed under valid snapshot envelope'
            value['snapshot_hash']=canonical_hash({k:v for k,v in value.items() if k!='snapshot_hash'})
            source.write_text(json.dumps(value),encoding='utf-8')
            job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
            with self.assertRaisesRegex(ValueError,'source fingerprint mismatch'):
                job.run()
            job.close()
            fixture(source)
            def incoherent(items,client,**kwargs):
                return [{'group_id':g['group_id'],'passed':g['kind']!='Method','issues':['members disagree'] if g['kind']=='Method' else [],'raw':{}} for g in items]
            with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',incoherent):
                job=SnapshotGraph(source,root/'fresh',client=object(),k=1,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['pending_groups'],1)
                task=job.claim('worker')[0]
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],'input_hash':task['input_hash'],
                             'result':{'name':'Recall','description':'Recall over time','group_id':task['group_id']}}])
                state=job.run()
                self.assertEqual(state['phase'],'complete')
                self.assertEqual(state['pending_mentions'],2)
                job.close()

    def test_empty_accepted_snapshot_completes_without_embedding_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json'
            value={'schema_version':'research-kg-prepared-snapshot/1','run_id':'empty','papers':[],
                   'extractions':[],'vectors':[],'counts':{'accepted':0,'excluded':2}}
            value['snapshot_hash']=canonical_hash(value)
            source.write_text(json.dumps(value),encoding='utf-8')
            job=SnapshotGraph(source,root/'job',client=object())
            state=job.run()
            self.assertEqual(state['phase'],'complete')
            self.assertEqual(state['index_status'],'verified')
            self.assertEqual(state['index_builds'],0)
            self.assertIsNotNone(state['completed_at'])
            self.assertGreaterEqual(state['completed_at'],state['started_at'])
            self.assertIn('retrieval_index_cpu',state['stage_seconds'])
            job.close()

    def test_generated_name_requires_final_jev_coherence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            def final_review(items,client,**kwargs):
                return [{'group_id':g['group_id'],'passed':g['name']!='Unrelated invented claim',
                         'issues':['display conflicts'] if g['name']=='Unrelated invented claim' else [],'raw':{'checked_name':g['name']}} for g in items]
            with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',final_review):
                job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
                job.run()
                task=job.claim('worker')[0]
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],'input_hash':task['input_hash'],
                             'result':{'name':'Unrelated invented claim','description':'Unsupported description','group_id':task['group_id']}}])
                state=job.run()
                self.assertEqual(state['phase'],'complete')
                self.assertEqual(state['pending_groups'],1)
                self.assertEqual(state['pending_mentions'],2)
                self.assertEqual(job.db.execute("SELECT name_review FROM groups WHERE group_id=%s",(task['group_id'],)).fetchone()['name_review'],'pending')
                job.close()

    def test_pending_pair_and_stale_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            def uncertain(items,client,**kwargs):
                return [{**r,'decision':'pending'} if i==0 else r for i,r in enumerate(pairs(items,client))]
            with patch('kg_builder.decision_api.judge_pairs',uncertain),patch('kg_builder.decision_api.review_groups',groups):
                job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['pending_pairs'],1)
                self.assertGreater(state['pending_mentions'],0)
                if state['name_tasks'].get('queued'):
                    task=job.claim('first')[0]
                    job.db.execute('UPDATE tasks SET expires=0 WHERE task_id=%s',(task['task_id'],));job.db.commit()
                    newer=job.claim('second')[0]
                    self.assertNotEqual(task['lease_token'],newer['lease_token'])
                    with self.assertRaisesRegex(ValueError,'Stale'):
                        job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],'input_hash':task['input_hash'],'result':{'name':'A','description':'B'}}])
                job.close()

if __name__=='__main__':unittest.main()


class DomainGraphTests(GraphPostgresCase):
    def test_unmarked_partial_member_review_requires_explicit_legacy_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            job=SnapshotGraph(source,root/'job',graph_policy='domain-v1')
            with job.db.transaction():
                job.db.execute("DELETE FROM meta WHERE key='member_review_mode'")
                job.db.execute("INSERT INTO member_reviews VALUES('old-group','old-mention',0,'{}')")
                job._set_meta('phase','judged')
            job.close()
            with self.assertRaisesRegex(ValueError,'Partially reviewed job'):
                SnapshotGraph(source,root/'job',graph_policy='domain-v1')
            with self.assertRaisesRegex(ValueError,'member_review_mode cannot be changed'):
                SnapshotGraph(source,root/'job',graph_policy='domain-v1',member_review_mode='whole-group-v1')
            job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',member_review_mode='anchor-v1')
            self.assertEqual(job.member_review_mode,'anchor-v1')
            self.assertIsNone(job._meta('member_review_mode'))
            job.close()

    def test_cli_resolution_status_and_mismatch(self):
        from contextlib import redirect_stdout
        from io import StringIO
        from scripts.build_snapshot_graph import main
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            args=['--snapshot',str(source),'--job-dir',str(root/'job')]
            with redirect_stdout(StringIO()) as output:
                main([*args,'--problem-resolution','0.3','status'])
            self.assertEqual(json.loads(output.getvalue())['problem_resolution'],0.3)
            with redirect_stdout(StringIO()) as output:
                main([*args,'status'])
            self.assertEqual(json.loads(output.getvalue())['problem_resolution'],0.3)
            with self.assertRaisesRegex(ValueError,'problem_resolution cannot be changed'):
                main([*args,'--problem-resolution','0.4','status'])

    def test_entity_same_threshold_cli_restore_and_legacy_default(self):
        from contextlib import redirect_stdout
        from io import StringIO
        from scripts.build_snapshot_graph import main
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            args=['--snapshot',str(source),'--job-dir',str(root/'job')]
            with redirect_stdout(StringIO()) as output:
                main([*args,'--entity-same-min-probability','0.65','status'])
            self.assertEqual(json.loads(output.getvalue())['entity_same_min_probability'],.65)
            with redirect_stdout(StringIO()) as output:main([*args,'status'])
            self.assertEqual(json.loads(output.getvalue())['entity_same_min_probability'],.65)
            with self.assertRaisesRegex(ValueError,'entity_same_min_probability cannot be changed'):
                main([*args,'--entity-same-min-probability','0.6','status'])
            job=SnapshotGraph(source,root/'default')
            self.assertEqual(job.status()['entity_same_min_probability'],.6)
            job.close()
            job=SnapshotGraph(source,root/'default')
            self.assertEqual(job.entity_same_min_probability,.6)
            with job.db.transaction():job.db.execute("DELETE FROM meta WHERE key='entity_same_min_probability'")
            job.close()
            job=SnapshotGraph(source,root/'default')
            self.assertEqual(job.status()['entity_same_min_probability'],.7)
            job.close()
            for invalid in (True,-.1,1.1,float('nan'),float('inf')):
                with self.subTest(invalid=invalid),self.assertRaises(ValueError):
                    SnapshotGraph(source,root/'invalid',entity_same_min_probability=invalid)

    def test_resolution_binding_and_legacy_metric_restore(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            job=SnapshotGraph(source,root/'job',problem_resolution=0.4)
            self.assertEqual(job.status()['decision_metric'],'probability')
            self.assertEqual(job.status()['context_format'],'explicit-v1')
            self.assertEqual(job.status()['problem_resolution'],0.4)
            job.close()
            with self.assertRaisesRegex(ValueError,'problem_resolution cannot be changed'):
                SnapshotGraph(source,root/'job',problem_resolution=0.2)
            with self.assertRaisesRegex(ValueError,'context_format cannot be changed'):
                SnapshotGraph(source,root/'job',context_format='registry-v1')
            job=SnapshotGraph(source,root/'job')
            self.assertEqual(job.problem_resolution,0.4)
            with job.db.transaction():
                job.db.execute("DELETE FROM meta WHERE key IN ('decision_metric','problem_resolution','context_format')")
            job.close()
            job=SnapshotGraph(source,root/'job')
            self.assertEqual((job.decision_metric,job.problem_resolution),('confidence',0.05))
            self.assertEqual(job.status()['context_format'],'registry-v1')
            self.assertTrue(job._strict_exclusion({'choice':'not_belongs','confidence':0.9,'probability':0.1}))
            with job.db.transaction():job._set_meta('context_format','invalid')
            job.close()
            with self.assertRaisesRegex(ValueError,'Invalid saved context_format'):
                SnapshotGraph(source,root/'job')
            for invalid in (0,-1,float('nan'),float('inf'),True):
                with self.assertRaises(ValueError):SnapshotGraph(source,root/'invalid',problem_resolution=invalid)

    def test_leiden_members_and_strict_exclusion_keep_source_nodes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=2)
            def judge(items,client,**kwargs):
                self.assertEqual(kwargs['context_format'],'explicit-v1')
                self.assertEqual(kwargs['same_min_probability'],.6)
                return [{'pair_id':p['pair_id'],'decision':'pending' if p['kind']=='Benchmark' else 'same','raw':{}} for p in items]
            def members(items,client,rejection_threshold=0.7,**kwargs):
                self.assertEqual(kwargs['context_format'],'explicit-v1')
                results=[]
                for item in items:
                    if item['kind']=='ResearchArea':
                        self.assertEqual(item['name'],'Agent memory research')
                        self.assertEqual(item['description'],'Problems concerning memory')
                    excluded=item['member']['name']=='Method 2'
                    borderline=item['member']['name']=='Memory problem 2'
                    low_probability=item['member']['name']=='Method 1'
                    results.append({**member_result(item,'not_belongs' if excluded or borderline or low_probability else 'belongs',
                                     0.9 if excluded else (0.7 if borderline else 0.2),
                                     0.1 if excluded else 0.95),'group_id':item['group_id']})
                return results
            with patch('kg_builder.decision_api.judge_pairs',judge),patch('kg_builder.decision_api.review_group_members',members):
                job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=object(),k=1,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['phase'],'grouped')
                self.assertEqual(state['pending_pairs'],1)
                self.assertEqual(state['excluded_memberships'],1)
                self.assertEqual(state['name_tasks'],{'queued':1})
                self.assertEqual(job.db.execute("SELECT count(*) FROM member_reviews mr JOIN groups g ON g.group_id=mr.group_id WHERE g.kind='ResearchProblem'").fetchone()['count'],0)
                proposal=json.loads(job._meta('problem_proposal'))
                actual=[json.loads(r['members_json']) for r in job.db.execute("SELECT members_json FROM groups WHERE kind='ResearchProblem'")]
                self.assertEqual(sorted(map(sorted,actual)),sorted(map(sorted,proposal['clusters'])))
                task=job.claim('worker')[0]
                self.assertIn('ResearchArea',task['instructions'])
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Agent memory research','description':'Problems concerning memory'}}])
                state=job.run()
                self.assertEqual(state['phase'],'complete')
                self.assertEqual(state['research_areas'],1)
                self.assertEqual(state['pending_mentions'],0)
                self.assertEqual(state['index_builds'],5)
                self.assertEqual(job.db.execute("SELECT count(*) FROM member_reviews mr JOIN groups g ON g.group_id=mr.group_id WHERE g.kind='ResearchProblem' AND mr.audit::jsonb->'area_revision' IS NOT NULL").fetchone()['count'],2)
                job.close()

            store=SemanticStore(graph_run_identity(root/'job'),FakeEmbedding())
            try:
                self.assertEqual(store.db.execute("SELECT count(*) FROM entities WHERE kind='ResearchProblem'").fetchone()['count'],2)
                self.assertEqual(store.db.execute("SELECT count(*) FROM entities WHERE kind='Method'").fetchone()['count'],2)
                self.assertEqual(store.db.execute('SELECT count(*) FROM research_area_members').fetchone()['count'],2)
                self.assertEqual(store.search('Agent memory research',kind='ResearchArea')['results'][0]['name'],'Agent memory research')
            finally:store.close()

    def test_saved_policy_is_immutable_and_old_job_without_policy_is_legacy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            job=SnapshotGraph(source,root/'new');self.assertEqual(job.graph_policy,'domain-v1');job.close()
            with self.assertRaisesRegex(ValueError,'policy cannot be changed'):
                SnapshotGraph(source,root/'new',graph_policy='legacy')
            job=SnapshotGraph(source,root/'old',graph_policy='legacy')
            with job.db.transaction():job.db.execute("DELETE FROM meta WHERE key='graph_policy'")
            job.close()
            job=SnapshotGraph(source,root/'old');self.assertEqual(job.graph_policy,'legacy');job.close()

    def test_conflicting_different_same_chain_continues_to_member_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=3)
            def conflicting(items,client,**kwargs):
                method=sorted((p for p in items if p['kind']=='Method'),key=lambda p:(p['left']['mention_id'],p['right']['mention_id']))
                different=method[0]['pair_id']
                return [{'pair_id':p['pair_id'],'decision':'different' if p['pair_id']==different else 'same','raw':{}}
                        for p in items]
            reviewed=[]
            def members(items,client,**kwargs):
                reviewed.extend(items)
                return [member_result(item) for item in items]
            with patch('kg_builder.decision_api.judge_pairs',conflicting),patch('kg_builder.decision_api.review_group_members',members):
                job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=object(),k=2,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['phase'],'grouped')
                self.assertEqual(len(state['identity_conflicts']),1)
                self.assertEqual(state['identity_conflict_policy'],'member_review_v1')
                self.assertEqual(len([item for item in reviewed if item['kind']=='Method']),3)
                self.assertEqual(job.db.execute("SELECT count(*) FROM candidates WHERE decision='different'").fetchone()['count'],1)
                self.assertEqual(job.db.execute("SELECT count(*) FROM member_reviews mr JOIN groups g ON g.group_id=mr.group_id WHERE g.kind='Method'").fetchone()['count'],3)
                while task_batch:=job.claim('worker'):
                    job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                                 'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                                 'name':'Memory research area','description':'Recall over time'}} for task in task_batch])
                self.assertEqual(job.run()['phase'],'complete')
                before=[dict(row) for row in job.db.execute('SELECT pair_id,decision,audit FROM candidates ORDER BY pair_id')]
                with patch('kg_builder.decision_api.judge_pairs',side_effect=AssertionError('pair replay')), \
                     patch('kg_builder.decision_api.review_group_members',side_effect=AssertionError('member replay')):
                    self.assertEqual(job.run()['phase'],'complete')
                self.assertEqual(before,[dict(row) for row in job.db.execute('SELECT pair_id,decision,audit FROM candidates ORDER BY pair_id')])
                job.close()
                from kg_builder.graph_validation import validate_graph
                audit=validate_graph(source,root/'job',database_url=self.database_url)
                self.assertTrue(audit['structural']['passed'],audit['structural'])

    def test_sparse_five_member_chain_reviews_without_all_pairs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=5)
            job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=object(),k=2,model=FakeEmbedding())
            job._prepare_store();job._candidates()
            store=job._semantic_store()
            try:
                mids=sorted(r['mention_id'] for r in store.db.execute("SELECT mention_id FROM mentions WHERE kind='Method'"))
            finally:store.close()
            edges=[(0,2,'same'),(2,3,'same'),(3,1,'same'),(1,4,'same'),(0,1,'different')]
            with job.db.transaction():
                job.db.execute('DELETE FROM candidates')
                for n,(a,b,decision) in enumerate(edges):
                    job.db.execute('INSERT INTO candidates(pair_id,kind,left_id,right_id,similarity,decision,audit) VALUES(%s,%s,%s,%s,%s,%s,%s)',
                                   (f'synthetic:{n}','Method',mids[a],mids[b],0.8,decision,json.dumps({'original':decision})))
                job._set_meta('phase','judged')
                job.db.execute("DELETE FROM meta WHERE key='member_review_mode'")
            job.close()
            job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=object(),k=2,model=FakeEmbedding())
            self.assertEqual(job.member_review_mode,'whole-group-v1')
            reviewed=[]
            def members(items,client,**kwargs):
                result=[]
                for item in items:
                    if item['kind']=='Method':
                        reviewed.append(item)
                        mid=item['member']['mention_id']
                        choice,score=({mids[0]:('not_belongs',.9),mids[1]:('insufficient',.9),
                                       mids[2]:('not_belongs',.7)}.get(mid,('belongs',.2)))
                    else:choice,score='belongs',.2
                    result.append(member_result(item,choice,score))
                return result
            with patch('kg_builder.decision_api.judge_pairs',side_effect=AssertionError('pair replay')), \
                 patch('kg_builder.decision_api.review_group_members',members):
                state=job.run()
            self.assertEqual(state['phase'],'grouped')
            self.assertEqual(state['candidate_pairs'],{'same':4,'different':1})
            self.assertEqual(len(state['identity_conflicts']),1)
            self.assertEqual(len(reviewed),5)
            self.assertEqual(state['excluded_memberships'],1)
            self.assertEqual(job.db.execute("SELECT count(*) FROM groups WHERE kind='Method'").fetchone()['count'],1)
            self.assertEqual([json.loads(r['audit'])['original'] for r in job.db.execute('SELECT audit FROM candidates ORDER BY pair_id')],
                             ['same','same','same','same','different'])
            while task_batch:=job.claim('worker'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Memory research area','description':'Recall over time'}} for task in task_batch])
            with patch('kg_builder.decision_api.judge_pairs',side_effect=AssertionError('pair replay')), \
                 patch('kg_builder.decision_api.review_group_members',members):
                self.assertEqual(job.run()['phase'],'complete')
            source_store=job._semantic_store()
            try:
                rows={r['mention_id']:r for r in source_store.db.execute("SELECT mention_id,name,entity_id FROM mentions WHERE kind='Method'")}
                retained=rows[mids[1]]['entity_id']
                self.assertEqual({rows[mid]['entity_id'] for mid in mids[1:]},{retained})
                self.assertNotEqual(rows[mids[0]]['entity_id'],retained)
                self.assertEqual(source_store.db.execute('SELECT name FROM entities WHERE entity_id=%s',(retained,)).fetchone()['name'],rows[mids[1]]['name'])
            finally:source_store.close()
            job.close()

    def test_native_whole_group_adapter_batches_multiple_groups(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=3)
            class Client:
                max_concurrency=1
                def __init__(self):self.calls=[]
                def decide(self,state,questions):
                    self.calls.append((state,questions))
                    return {'model':'jev-1.14.0','answers':{qid:{'type':'choice','choice':'belongs',
                        'probabilities':{'belongs':.9,'not_belongs':.05,'insufficient':.05},'confidence':.1}
                        for qid in questions},'attempts':1,'request_duration_s':.01}
            client=Client()
            with patch('kg_builder.decision_api.judge_pairs',pairs):
                job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=client,k=2,model=FakeEmbedding())
                state=job.run()
            self.assertEqual(state['phase'],'grouped')
            self.assertEqual(state['member_review_mode'],'whole-group-v1')
            self.assertEqual(len(client.calls),1)
            packet,questions=client.calls[0]
            self.assertEqual(len(questions),6)
            self.assertEqual(sorted(len(group['members']) for group in packet['candidate_groups']),[1,1,1,3])
            self.assertTrue(all('state.candidate_groups[' in q['instructions'] for q in questions.values()))
            self.assertTrue(all(json.loads(r['audit'])['review_mode']=='whole-group-v1'
                                for r in job.db.execute('SELECT audit FROM member_reviews')))
            job.close()

    def test_api_area_naming_and_terminal_replay_without_api(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            class Namer:
                def generate_json(self,prompt,state,*,required_keys):
                    self_outer.assertIn('thematic ResearchArea',prompt)
                    return {'group_id':state['group_id'],'name':'Memory research area','description':'A field of agent memory'}
            self_outer=self
            def reviewed(items,client,rejection_threshold=0.7,**kwargs):
                return [member_result(i) for i in items]
            with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_group_members',reviewed):
                job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=object(),k=1,
                                  model=FakeEmbedding(),name_mode='api',namer=Namer())
                self.assertEqual(job.run()['phase'],'complete')
                job.close()
            job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',k=1,name_mode='api',model=FakeEmbedding())
            self.assertEqual(job.run()['phase'],'complete')
            self.assertEqual(job.status()['research_areas'],1)
            job.close()

    def test_singleton_problems_have_no_area_or_fabricated_reviews(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=2)
            def singleton_proposal(vectors,**kwargs):
                return {'clusters':[[mid] for mid in sorted(vectors)],'edges':[],'diagnostics':{'algorithm':'leiden-cpm'}}
            def reviewed(items,client,rejection_threshold=0.7,**kwargs):
                self.assertTrue(all(i['kind']!='ResearchArea' for i in items))
                return [member_result(i) for i in items]
            with patch('kg_builder.snapshot_graph.cluster_problem_vectors',singleton_proposal),\
                 patch('kg_builder.decision_api.judge_pairs',pairs),\
                 patch('kg_builder.decision_api.review_group_members',reviewed):
                job=SnapshotGraph(source,root/'job',graph_policy='domain-v1',client=object(),k=1,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['phase'],'complete')
                self.assertEqual(state['name_tasks'],{})
                self.assertEqual(state['research_areas'],0)
                self.assertEqual(job.db.execute("SELECT count(*) FROM member_reviews mr JOIN groups g ON g.group_id=mr.group_id WHERE g.kind='ResearchProblem'").fetchone()['count'],0)
                job.close()


class ParallelPairTests(GraphPostgresCase):
    def test_public_cli_status_preflight_allows_first_neo4j_run_without_api_during_status(self):
        from contextlib import redirect_stdout
        from io import StringIO
        from scripts.build_snapshot_graph import main
        class Publisher:
            def __init__(self):self.events=[]
            def publish(self,run_id,event):self.events.append((run_id,event))
            def close(self):pass
        publisher=Publisher()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            base=['--snapshot',str(source),'--job-dir',str(root/'job'),'--embedding-provider','fastembed']
            with patch('kg_builder.embeddings.create_embedding_model',return_value=FakeEmbedding()) as embedding,\
                 patch('kg_builder.decision_api.JevClient',return_value=object()) as client,\
                 patch('kg_builder.neo4j_store.Neo4jStore.from_env',return_value=publisher) as graph,\
                 patch('kg_builder.decision_api.judge_pairs',pairs),\
                 patch('kg_builder.decision_api.review_group_members',side_effect=lambda items,*args,**kwargs:[member_result(item) for item in items]):
                with redirect_stdout(StringIO()) as output:main([*base,'status'])
                self.assertEqual(json.loads(output.getvalue())['phase'],'new')
                embedding.assert_not_called();client.assert_not_called();graph.assert_not_called()
                with redirect_stdout(StringIO()) as output:main([*base,'--neo4j','run'])
                result=json.loads(output.getvalue())
                self.assertEqual(result['phase'],'grouped')
                self.assertTrue(result['neo4j_run_id'])
                self.assertEqual(result['neo4j_projection']['pending'],0)
                self.assertGreater(result['neo4j_projection']['delivered'],0)
                self.assertTrue(publisher.events)
            self.assertEqual(list((root/'job').glob('*.sqlite')),[])

    def test_publisher_binding_rejects_processed_or_partial_local_business_state(self):
        class Publisher:
            def publish(self,*args):raise AssertionError('Rejected binding must not publish')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            for phase in ('new','imported','grouped','complete'):
                with self.subTest(phase=phase):
                    job=SnapshotGraph(source,root/phase)
                    job._prepare_store()
                    with job.db.transaction():job._set_meta('phase',phase)
                    job.close()
                    with self.assertRaisesRegex(ValueError,'Enable Neo4j only for a fresh graph job'):
                        SnapshotGraph(source,root/phase,publisher=Publisher())
            job=SnapshotGraph(source,root/'legacy',graph_policy='legacy');job.close()
            with self.assertRaisesRegex(ValueError,'Neo4j publishing requires domain-v1'):
                SnapshotGraph(source,root/'legacy',publisher=Publisher())

    def test_first_publisher_selection_rejects_active_local_driver_uncommitted_import(self):
        from concurrent.futures import ThreadPoolExecutor
        from kg_builder import snapshot_graph
        inserted=threading.Event();release=threading.Event();local=threading.local()
        original_store=snapshot_graph.GraphStore
        class Connection:
            def __init__(self,db):self.db=db
            def execute(self,query,*args):
                result=self.db.execute(query,*args)
                if getattr(local,'driver',False) and isinstance(query,str) and query.startswith('INSERT INTO papers'):
                    inserted.set()
                    if not release.wait(10):raise AssertionError('publisher opener did not progress')
                return result
            def __getattr__(self,name):return getattr(self.db,name)
        def store(*args,**kwargs):
            value=original_store(*args,**kwargs);value.db=Connection(value.db);return value
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source,n=1)
            def run_local():
                local.driver=True
                graph=SnapshotGraph(source,root/'job',client=object())
                try:
                    with self.assertRaisesRegex(RuntimeError,'stop after import'):graph.run()
                finally:graph.close()
            with patch.object(snapshot_graph,'GraphStore',store),patch.object(SnapshotGraph,'_candidates',side_effect=RuntimeError('stop after import')):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future=pool.submit(run_local)
                    try:
                        self.assertTrue(inserted.wait(10))
                        with self.assertRaisesRegex(ValueError,'graph driver is active'):
                            SnapshotGraph(source,root/'job',publisher=object())
                    finally:release.set()
                    future.result(15)
            graph=SnapshotGraph(source,root/'job',read_input=False)
            self.assertEqual(graph._meta('phase'),'imported')
            self.assertIsNone(graph._meta('neo4j_run_id'))
            self.assertEqual(graph.db.execute('SELECT count(*) FROM sources').fetchone()['count'],1)
            self.assertEqual(graph.db.execute('SELECT count(*) FROM graph_outbox').fetchone()['count'],0)
            graph.close()

    def test_concurrent_first_open_binds_snapshot_and_configuration_once(self):
        from concurrent.futures import ThreadPoolExecutor
        from kg_builder import snapshot_graph
        for variant in ('snapshot','configuration','same'):
            with self.subTest(variant=variant),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);first_source=root/'a.json';second_source=root/'b.json'
                first_value=fixture(first_source,n=1)
                if variant=='snapshot':fixture(second_source,n=2)
                else:second_source.write_bytes(first_source.read_bytes())
                run_id='binding-race-'+uuid.uuid4().hex
                first_read=threading.Event();release=threading.Event();local=threading.local()
                original_store=snapshot_graph.GraphStore
                original_meta=SnapshotGraph._meta;original_set=SnapshotGraph._set_meta
                initial_writes=[];write_lock=threading.Lock()
                class Connection:
                    def __init__(self,db):self.db=db
                    def execute(self,query,*args):
                        if getattr(local,'role',None)=='second' and isinstance(query,str) and query.startswith('SELECT pg_advisory_xact_lock'):
                            release.set()
                        return self.db.execute(query,*args)
                    def __getattr__(self,name):return getattr(self.db,name)
                def store(*args,**kwargs):
                    value=original_store(*args,**kwargs);value.db=Connection(value.db);return value
                def meta(graph,key):
                    value=original_meta(graph,key)
                    if key=='snapshot_hash' and not getattr(local,'read',False):
                        local.read=True
                        if local.role=='first':
                            first_read.set()
                            if not release.wait(10):raise AssertionError('second constructor did not progress')
                        else:release.set()  # Baseline reaches a second empty read.
                    return value
                def set_meta(graph,key,value):
                    if key in ('snapshot_hash','phase','started_at'):
                        with write_lock:initial_writes.append((key,value))
                    return original_set(graph,key,value)
                def opening(role):
                    local.role=role
                    try:
                        graph=SnapshotGraph(first_source if role=='first' else second_source,root/role,
                            run_id=run_id,k=2 if role=='second' and variant=='configuration' else 1)
                        return graph
                    except ValueError as exc:return exc
                with patch.object(snapshot_graph,'GraphStore',store),patch.object(SnapshotGraph,'_meta',meta),patch.object(SnapshotGraph,'_set_meta',set_meta):
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        first=pool.submit(opening,'first')
                        try:
                            self.assertTrue(first_read.wait(10))
                            second=pool.submit(opening,'second')
                            results=[first.result(15),second.result(15)]
                        finally:release.set()
                graphs=[result for result in results if isinstance(result,SnapshotGraph)]
                errors=[result for result in results if isinstance(result,ValueError)]
                self.assertEqual(len(graphs),2 if variant=='same' else 1)
                self.assertEqual(len(errors),0 if variant=='same' else 1)
                if variant=='snapshot':self.assertIn('different immutable snapshot',str(errors[0]))
                if variant=='configuration':self.assertIn('configuration changed',str(errors[0]))
                self.assertEqual([key for key,value in initial_writes].count('snapshot_hash'),1)
                self.assertEqual([key for key,value in initial_writes].count('phase'),1)
                self.assertEqual([key for key,value in initial_writes].count('started_at'),1)
                winner=graphs[0]
                self.assertEqual(winner._meta('snapshot_hash'),first_value['snapshot_hash'])
                self.assertEqual(winner._meta('k'),'1')
                self.assertEqual(winner._meta('phase'),'new')
                winner._prepare_store()
                self.assertEqual(winner.db.execute('SELECT count(*) FROM sources').fetchone()['count'],1)
                for graph in graphs:graph.close()

    def test_constructor_failure_closes_connection_and_graph_state_has_no_local_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            from kg_builder import snapshot_graph
            stores=[]
            original=snapshot_graph.GraphStore
            def opened(*args,**kwargs):
                store=original(*args,**kwargs);stores.append(store);return store
            with patch.object(snapshot_graph,'GraphStore',opened),self.assertRaisesRegex(ValueError,'Positive k'):
                SnapshotGraph(source,root/'invalid',k=0)
            self.assertTrue(stores[0].db.closed)
            job,_=self.job(root,object(),count=1,batch_size=1)
            self.assertEqual(list((root/'job').glob('*.sqlite')),[])
            self.assertTrue((root/'job'/'graph-job.json').exists())
            job.close()

    def test_concurrent_claims_duplicate_submission_stale_lease_and_driver_lock(self):
        from concurrent.futures import ThreadPoolExecutor
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);job,_=self.job(root,object(),count=1,batch_size=1)
            mids=[r['mention_id'] for r in job.db.execute('SELECT mention_id FROM mentions ORDER BY mention_id LIMIT 2')]
            with job.db.transaction():
                for n in range(4):
                    job.db.execute("INSERT INTO groups(group_id,kind,members_json) VALUES(%s,'ResearchProblem',%s)",(f'g{n}',json.dumps(mids)))
                    job.db.execute("INSERT INTO tasks(task_id,group_id,status,queued_at) VALUES(%s,%s,'queued',0)",(f't{n}',f'g{n}'))
            barrier=threading.Barrier(2)
            def claim(worker):
                other=SnapshotGraph(root/'snapshot.json',root/'job',read_input=False)
                try:
                    barrier.wait(10)
                    return other.claim(worker,limit=4)
                finally:other.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                batches=list(pool.map(claim,('first','second')))
            tasks=[task for batch in batches for task in batch]
            self.assertEqual(len(tasks),4)
            self.assertEqual(len({task['task_id'] for task in tasks}),4)
            task=tasks[0]
            submission={'task_id':task['task_id'],'lease_token':task['lease_token'],'input_hash':task['input_hash'],
                'result':{'name':'Grounded name','description':'Grounded description','group_id':task['group_id']}}
            barrier=threading.Barrier(2)
            def submit(_):
                other=SnapshotGraph(root/'snapshot.json',root/'job',read_input=False)
                try:
                    barrier.wait(10)
                    return other.submit([submission])[0]['status']
                finally:other.close()
            with ThreadPoolExecutor(max_workers=2) as pool:statuses=list(pool.map(submit,range(2)))
            self.assertEqual(sorted(statuses),['done','unchanged'])
            stale=tasks[1]
            job.db.execute('UPDATE tasks SET expires=0 WHERE task_id=%s',(stale['task_id'],))
            newer=job.claim('replacement',limit=1)[0]
            self.assertNotEqual(newer['lease_token'],stale['lease_token'])
            with self.assertRaisesRegex(ValueError,'Stale name lease'):
                job.submit([{'task_id':stale['task_id'],'lease_token':stale['lease_token'],
                    'input_hash':stale['input_hash'],'result':{'name':'Old','description':'Old'}}])
            lock_id=int.from_bytes(hashlib.sha256(('graph-driver:'+job.run_id).encode()).digest()[:8],'big',signed=True)
            other=job.graph_store.connect()
            try:
                other.execute('SELECT pg_advisory_lock(%s)',(lock_id,))
                with self.assertRaisesRegex(RuntimeError,'active driver'):job.run()
            finally:other.close()
            job.close()

    def test_pair_queue_native_index_plan_ignores_unrelated_judged_rows(self):
        class Client:max_concurrency=1
        with tempfile.TemporaryDirectory() as tmp:
            job,_=self.job(Path(tmp),Client(),count=2,batch_size=12)
            plans=[]
            for count in (1000,20000):
                job.db.execute("INSERT INTO candidates(pair_id,kind,left_id,right_id,similarity,decision,audit) SELECT 'unrelated:'||n,'Method','left','right',0,'different',repeat('x',2000) FROM generate_series(%s::integer,%s::integer) n ON CONFLICT DO NOTHING",(1,count))
                job.db.execute('ANALYZE candidates')
                plan=job.db.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM candidates WHERE decision='unjudged' AND pair_id>%s ORDER BY pair_id LIMIT %s",('pair:00010',5)).fetchone()['QUERY PLAN'][0]['Plan']
                scan=plan['Plans'][0]
                self.assertEqual(scan['Index Name'],'candidate_judgment_queue')
                self.assertEqual(scan['Actual Rows'],5)
                self.assertLessEqual(plan['Shared Hit Blocks']+plan['Shared Read Blocks'],20)
                plans.append({'unrelated_rows':count,'plan':plan})
            evidence=Path('docs/reviews/full-preparation-20261102/graph-postgres8/logs/graph-pair-locality.json')
            evidence.parent.mkdir(parents=True,exist_ok=True);evidence.write_text(json.dumps(plans,indent=2))
            job.close()

    def job(self, root, client, *, count=12, batch_size=32):
        source=root/'snapshot.json';fixture(source,n=8)
        job=SnapshotGraph(source,root/'job',client=client,batch_size=batch_size)
        job._prepare_store()
        mentions=job.db.execute('SELECT mention_id,name FROM mentions ORDER BY mention_id').fetchall()
        with job.db.transaction():
            for n in range(count*batch_size):
                job.db.execute('INSERT INTO candidates(pair_id,kind,left_id,right_id,similarity) VALUES(%s,%s,%s,%s,%s)',
                    (f'pair:{n:05d}','Method',mentions[n//batch_size]['mention_id'],mentions[-1]['mention_id'],1))
            job._set_meta('phase','candidates')
        return job,mentions[0]['name']

    def test_real_adapter_shared_http8_replenishes_behind_slow_first_owner_thread_db(self):
        import io
        from contextlib import contextmanager
        from kg_builder.decision_api import JevClient,judge_pairs
        client=JevClient(max_concurrency=8,max_retries=0)
        eight=threading.Event();refilled=threading.Event();persisted=threading.Event()
        lock=threading.Lock();active=peak=started=dispatched=0
        owner=threading.get_ident()
        class OwnerDB:
            def __init__(self,db):self.db=db;self.updated=False
            def execute(self,query,*args):
                if threading.get_ident()!=owner:raise AssertionError('worker accessed PostgreSQL')
                if isinstance(query,str) and query.startswith('UPDATE candidates'):self.updated=True
                return self.db.execute(query,*args)
            @contextmanager
            def transaction(self):
                if threading.get_ident()!=owner:raise AssertionError('worker transaction')
                with self.db.transaction():yield
                if self.updated:persisted.set()
            def __getattr__(self,name):return getattr(self.db,name)
        with tempfile.TemporaryDirectory() as tmp:
            job,slow_name=self.job(Path(tmp),client)
            job.db=OwnerDB(job.db)
            original_store=job._semantic_store
            def semantic(*args):
                store=original_store(*args);store.db=OwnerDB(store.db);return store
            def forward(inputs,*args,**kwargs):
                nonlocal dispatched
                with lock:
                    dispatched+=1
                    if dispatched>8:refilled.set()
                return judge_pairs(inputs,*args,**kwargs)
            def open_response(request,**kwargs):
                nonlocal active,peak,started
                payload=json.loads(request.data)
                with lock:
                    active+=1;started+=1;peak=max(peak,active)
                    if active==8:eight.set()
                try:
                    if not eight.wait(10):raise AssertionError('HTTP cap8 never reached')
                    if payload['state']['entities'][0]['name']==slow_name:
                        if not persisted.wait(10):raise AssertionError('completed batch was not persisted promptly')
                        if not refilled.wait(10):raise AssertionError('scheduler waited for slow first batch')
                    answers={qid:{'type':'choice','choice':'different','confidence':.9,
                        'probabilities':{'same':.05,'different':.9,'insufficient':.05}} for qid in payload['questions']}
                    return io.BytesIO(json.dumps({'model':'jev-1.14.0','answers':answers}).encode())
                finally:
                    with lock:active-=1
            with patch.dict(os.environ,{'TYPESAFE_API_KEY':'offline-test-only'}),\
                 patch('kg_builder.decision_api.urlopen',open_response),\
                 patch('kg_builder.decision_api.judge_pairs',forward),\
                 patch.object(job,'_semantic_store',semantic):
                job._judge()
            self.assertEqual(peak,8)
            self.assertEqual(client.concurrency_stats['peak_active'],8)
            self.assertEqual(active,0)
            self.assertGreater(started,12)  # Real adapter splits the bounded pair batches.
            self.assertEqual(dispatched,12)
            self.assertEqual(job.status()['candidate_pairs'],{'different':384})
            self.assertEqual(job._meta('phase'),'judged')
            job.close()

    def test_failure_drains_successes_and_reopen_only_dispatches_unjudged(self):
        class Client:max_concurrency=3
        barrier=threading.Barrier(3);seen=[];lock=threading.Lock();failure_seen=threading.Event()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);job,_=self.job(root,Client(),count=8,batch_size=1)
            def judge(inputs,*args,**kwargs):
                pid=inputs[0]['pair_id']
                with lock:seen.append(pid)
                barrier.wait(10)
                if pid=='pair:00000':
                    exc=RuntimeError('offline failure')
                    exc.request_audit={'request_id':'failed','attempts':1}
                    raise exc
                if not failure_seen.wait(10):raise AssertionError('owner did not observe failure')
                return [{'pair_id':pid,'decision':'pending','raw':{'requests':[{'request_id':pid,'attempts':1}]}}]
            record=job._record_failure
            def observed(exc):
                record(exc);failure_seen.set()
            with patch('kg_builder.decision_api.judge_pairs',judge),patch.object(job,'_record_failure',observed),self.assertRaisesRegex(RuntimeError,'offline failure'):
                job._judge()
            self.assertEqual(len(seen),3)
            self.assertEqual(job.status()['candidate_pairs'],{'pending':2,'unjudged':6})
            self.assertEqual(job.status()['jev_request_count'],3)
            self.assertEqual(job._meta('phase'),'candidates')
            job.close()
            job=SnapshotGraph(root/'snapshot.json',root/'job',client=Client(),batch_size=1)
            resumed=[]
            def success(inputs,*args,**kwargs):
                resumed.extend(p['pair_id'] for p in inputs)
                return [{'pair_id':p['pair_id'],'decision':'different','raw':{}} for p in reversed(inputs)]
            with patch('kg_builder.decision_api.judge_pairs',success):job._judge()
            self.assertEqual(len(resumed),6)
            self.assertNotIn('pair:00001',resumed);self.assertNotIn('pair:00002',resumed)
            self.assertEqual(job.status()['candidate_pairs'],{'different':6,'pending':2})
            job.close()

    def test_malformed_batches_roll_back_all_candidates_and_preserve_http_audit(self):
        class Client:max_concurrency=1
        variants=('missing','duplicate','unexpected','invalid')
        for variant in variants:
            with self.subTest(variant=variant),tempfile.TemporaryDirectory() as tmp:
                job,_=self.job(Path(tmp),Client(),count=1,batch_size=2)
                def malformed(inputs,*args,**kwargs):
                    results=[{'pair_id':p['pair_id'],'decision':'different','raw':{'requests':[{'request_id':'audit','attempts':1}]}} for p in inputs]
                    if variant=='missing':results.pop()
                    elif variant=='duplicate':results[1]=results[0]
                    elif variant=='unexpected':results[1]['pair_id']='other'
                    else:results[1]['decision']='unsupported'
                    return results
                with patch('kg_builder.decision_api.judge_pairs',malformed),self.assertRaises(ValueError):job._judge()
                self.assertEqual(job.status()['candidate_pairs'],{'unjudged':2})
                self.assertEqual(job._meta('phase'),'candidates')
                self.assertEqual(job.status()['jev_request_count'],1)
                job.close()

    def test_concurrency_one_arbitrary_response_order(self):
        class Client:max_concurrency=1
        active=peak=0
        with tempfile.TemporaryDirectory() as tmp:
            job,_=self.job(Path(tmp),Client(),count=4,batch_size=3)
            def judge(inputs,*args,**kwargs):
                nonlocal active,peak
                active+=1;peak=max(peak,active)
                try:return [{'pair_id':p['pair_id'],'decision':'same' if int(p['pair_id'].split(':')[1])%2 else 'different','raw':{}} for p in reversed(inputs)]
                finally:active-=1
            with patch('kg_builder.decision_api.judge_pairs',judge):job._judge()
            self.assertEqual(peak,1)
            for row in job.db.execute('SELECT pair_id,decision FROM candidates'):
                self.assertEqual(row['decision'],'same' if int(row['pair_id'].split(':')[1])%2 else 'different')
            job.close()
