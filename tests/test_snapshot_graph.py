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


def pairs(items,client):
    return [{'pair_id':p['pair_id'],'decision':'different' if p['kind']=='Benchmark' else 'same','raw':{'test':True}} for p in items]


def groups(items,client):
    return [{'group_id':g['group_id'],'passed':True,'issues':[],'raw':{'test':True}} for g in items]


class FakeEmbedding:
    model_id='test-embed'
    def encode(self,texts): return [[1.0,0.0] for _ in texts]


class SnapshotGraphTests(unittest.TestCase):
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
            with job.db:job._record_requests(raw);job._record_requests(raw)
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
            def distinct(items,client):
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
                    paper=SemanticStore(root/'job'/'semantic.sqlite')
                    try:
                        expected=paper.db.execute('SELECT s.paper_id FROM mentions m JOIN sources s ON m.extraction_id=s.extraction_id WHERE mention_id=?',(mid,)).fetchone()[0]
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
            with __import__('sqlite3').connect(root/'job'/'snapshot-master.sqlite') as master:
                self.assertEqual(master.execute('SELECT abstract FROM paper_versions WHERE version_id=?',('paper:1@v1',)).fetchone()[0],'Text 1')
            store=SemanticStore(root/'job'/'semantic.sqlite')
            try:
                self.assertEqual(store.db.execute('SELECT count(*) FROM entities').fetchone()[0],4)
                self.assertEqual(store.db.execute('SELECT count(*) FROM embedding_cache').fetchone()[0],8)
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
            def incoherent(items,client):
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
            def final_review(items,client):
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
                self.assertEqual(job.db.execute("SELECT name_review FROM groups WHERE group_id=?",(task['group_id'],)).fetchone()[0],'pending')
                job.close()

    def test_pending_pair_and_stale_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            def uncertain(items,client):
                return [{**r,'decision':'pending'} if i==0 else r for i,r in enumerate(pairs(items,client))]
            with patch('kg_builder.decision_api.judge_pairs',uncertain),patch('kg_builder.decision_api.review_groups',groups):
                job=SnapshotGraph(source,root/'job',client=object(),k=1,model=FakeEmbedding())
                state=job.run()
                self.assertEqual(state['pending_pairs'],1)
                self.assertGreater(state['pending_mentions'],0)
                if state['name_tasks'].get('queued'):
                    task=job.claim('first')[0]
                    job.db.execute('UPDATE tasks SET expires=0 WHERE task_id=?',(task['task_id'],));job.db.commit()
                    newer=job.claim('second')[0]
                    self.assertNotEqual(task['lease_token'],newer['lease_token'])
                    with self.assertRaisesRegex(ValueError,'Stale'):
                        job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],'input_hash':task['input_hash'],'result':{'name':'A','description':'B'}}])
                job.close()

if __name__=='__main__':unittest.main()
