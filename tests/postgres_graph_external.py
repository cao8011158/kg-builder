"""Real PostgreSQL external handoff, native lease and crash replay checks."""
import json
import tempfile
import subprocess
import sys
import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from psycopg import sql
from postgres_graph_cli_fixtures import NativeGraphCase
from postgres_snapshot_graph import fixture, pairs, groups, FakeEmbedding, member_result
from kg_builder.graph_external import ExternalGraphQueue, HelperGraph, SummaryMonitor, preflight_summary, claim_to_file, run_controller


class ExternalGraphTests(NativeGraphCase):
    def setUp(self):
        super().setUp()
        temporary_root=Path(__file__).resolve().parents[1]/'.test-tmp'/'external-graph'
        temporary_root.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=temporary_root);self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        source=self.root/'snapshot.json';fixture(source,4)
        self.job=self.make_job(source,self.root/'job',graph_policy='legacy',client=object(),model=FakeEmbedding())
        for target,value in [('judge_pairs',pairs),('review_groups',groups)]:
            p=patch('kg_builder.decision_api.'+target,side_effect=value);p.start();self.addCleanup(p.stop)
        self.job.run()
        self.queue=ExternalGraphQueue(self.job,initialize=True)
        self._schemas.add(self.queue.schema);self.addCleanup(self.queue.close)

    def item(self, worker='worker1'):
        claim=self.queue.claim(worker)['tasks'][0]
        return {k:claim[k] for k in ('task_id','lease_token','input_hash')}|{'result':{'group_id':claim['group_id'],'name':'Memory systems','description':'Persistent memory and retrieval'}}

    def test_duplicate_conflict_pins_and_shape_native_lease(self):
        item=self.item()
        self.assertEqual(self.queue.claim('worker1')['state'],'waiting')
        with self.assertRaisesRegex(ValueError,'fingerprint'):
            self.queue.enqueue(item|{'input_hash':'wrong'})
        with self.assertRaisesRegex(ValueError,'Group pin'):
            self.queue.enqueue(item|{'result':item['result']|{'group_id':'wrong'}})
        with self.assertRaisesRegex(ValueError,'requires'):
            self.queue.enqueue(item|{'result':{'name':''}})
        self.assertEqual(self.queue.enqueue(item)['status'],'ready')
        self.assertTrue(self.queue.enqueue(item)['duplicate'])
        with self.assertRaisesRegex(ValueError,'Conflicting'):
            self.queue.enqueue(item|{'result':item['result']|{'name':'Other'}})
        self.queue.acquire_owner();self.assertTrue(self.queue.process_one())
        self.assertEqual(self.queue.enqueue(item)['status'],'accepted')
        self.assertEqual(self.queue.claim('worker1')['state'],'idle')

    def test_expired_lease_rejected(self):
        item=self.item()
        self.job.db.execute('UPDATE tasks SET expires=0 WHERE task_id=%s',(item['task_id'],))
        with self.assertRaisesRegex(ValueError,'Stale'):
            self.queue.enqueue(item)
        replacement=self.queue.claim('worker1')['tasks'][0]
        self.assertNotEqual(item['lease_token'],replacement['lease_token'])

    def test_output_preflight_before_claim_and_no_http_clients(self):
        output=self.root/'claims'/'claim.json'
        with patch('kg_builder.decision_api.JevClient',side_effect=AssertionError('HTTP client constructed')), patch('kg_builder.embeddings.create_embedding_model',side_effect=AssertionError('model constructed')):
            result=claim_to_file(self.queue,'worker1',output,wait_seconds=0)
            self.assertEqual(result['task_count'],1)
        before=self.job.db.execute('SELECT sum(attempts) AS n FROM tasks').fetchone()['n']
        with self.assertRaises(FileExistsError):claim_to_file(self.queue,'worker2',output,wait_seconds=0)
        self.assertEqual(before,self.job.db.execute('SELECT sum(attempts) AS n FROM tasks').fetchone()['n'])

    def test_controller_crash_after_native_submit_before_ack(self):
        item=self.item();self.queue.enqueue(item);self.queue.acquire_owner()
        def crash(item):raise RuntimeError('simulated process exit')
        with self.assertRaisesRegex(RuntimeError,'process exit'):self.queue.process_one(after_submit=crash)
        self.assertEqual(self.job.db.execute('SELECT status FROM tasks WHERE task_id=%s',(item['task_id'],)).fetchone()['status'],'done')
        self.assertEqual(self.queue.summary()['queue']['ready'],1)
        other=ExternalGraphQueue(self.job);self.addCleanup(other.close)
        with self.assertRaisesRegex(RuntimeError,'already active'):other.acquire_owner()
        self.queue.close();other.acquire_owner()
        self.assertTrue(other.process_one())
        self.assertEqual(other.summary()['queue']['accepted'],1)

    def test_continuous_callback_handoff_and_one_shared_client(self):
        client=self.job.client;observed=[]
        def external_callback(state):
            observed.append(self.job.client)
            if state['phase']=='complete':return
            claim=self.queue.claim('external-agent')
            for task in claim['tasks']:
                self.queue.enqueue({k:task[k] for k in ('task_id','lease_token','input_hash')}|{'result':{'name':'Memory systems','description':'Source-grounded memory research'}})
        with patch.object(self.job,'run',wraps=self.job.run) as run:
            result=run_controller(self.queue,poll_seconds=0,on_summary=external_callback)
            self.assertEqual(result['phase'],'complete')
            self.assertEqual(run.call_count,2)
        self.assertTrue(observed);self.assertTrue(all(value is client for value in observed))

    def add_tasks(self, count):
        row=self.job.db.execute('SELECT kind,members_json FROM groups WHERE group_id IN (SELECT group_id FROM tasks) LIMIT 1').fetchone()
        for i in range(count):
            gid=f'extra-group-{i}'
            self.job.db.execute('INSERT INTO groups(group_id,kind,members_json) VALUES(%s,%s,%s)',(gid,row['kind'],row['members_json']))
            self.job.db.execute("INSERT INTO tasks(task_id,group_id,status,queued_at) VALUES(%s,%s,'queued',%s)",(f'extra-task-{i}',gid,time.time()))

    def test_enqueue_allows_next_claim_with_bounded_backpressure(self):
        self.add_tasks(4)
        first=self.item('continuous');self.queue.enqueue(first)
        second=self.item('continuous');self.queue.enqueue(second)
        self.assertEqual(self.queue.claim('continuous')['state'],'waiting')
        self.queue.acquire_owner();self.queue.process_one()
        self.assertEqual(self.queue.claim('continuous')['state'],'claimed')
        summary=self.queue.summary()
        self.assertIsNone(summary['timing']['pure_model_seconds'])
        for name in ('ready_to_claim_seconds','claim_to_enqueue_seconds','enqueue_to_processing_seconds'):
            self.assertGreaterEqual(summary['timing'][name],0)

    def test_six_active_workers_release_on_lease_expiry(self):
        self.add_tasks(8)
        claimed=[self.queue.claim(f'worker-{i}')['tasks'][0] for i in range(6)]
        self.assertEqual(self.queue.claim('replacement')['state'],'waiting')
        self.job.db.execute('UPDATE tasks SET expires=0 WHERE task_id=%s',(claimed[0]['task_id'],))
        self.assertEqual(self.queue.claim('replacement')['state'],'claimed')

    def test_expired_enqueued_result_is_retired_for_native_reclaim(self):
        item=self.item();self.queue.enqueue(item)
        self.job.db.execute('UPDATE tasks SET expires=0 WHERE task_id=%s',(item['task_id'],))
        self.queue.acquire_owner();self.assertTrue(self.queue.process_one())
        self.assertEqual(self.queue.summary()['queue']['expired'],1)
        self.assertEqual(self.queue.claim('replacement')['state'],'claimed')

    def test_helper_never_loads_snapshot_or_constructs_remote_clients(self):
        with patch('kg_builder.snapshot_graph.read_snapshot',side_effect=AssertionError('full snapshot read')):
            helper=HelperGraph(self.job.snapshot_path,self.job.job_dir)
            self.addCleanup(helper.close)
            other=ExternalGraphQueue(helper);self.addCleanup(other.close)
            task=other.claim('metadata-helper')['tasks'][0]
            self.assertTrue(task['members'])
            self.assertTrue(all({'description','source','title','name'}<=set(m) for m in task['members']))
            with self.assertRaisesRegex(ValueError,'verify snapshot metadata'):
                wrong=self.root/'wrong.json';wrong.write_text('{}')
                HelperGraph(wrong,self.job.job_dir)
        with self.assertRaisesRegex(ValueError,'Controller must create'):
            HelperGraph(self.job.snapshot_path,self.root/'missing-job')

    def test_idle_wait_is_bounded_and_complete_exits(self):
        self.item()
        output=self.root/'waiting.json'
        before=time.monotonic()
        result=claim_to_file(self.queue,'worker1',output,wait_seconds=.02,poll_seconds=.005)
        self.assertEqual(result['state'],'waiting')
        self.assertLess(time.monotonic()-before,2)
        with self.assertRaises(ValueError):claim_to_file(self.queue,'worker1',self.root/'too-long.json',wait_seconds=56)
        self.job.db.execute("UPDATE tasks SET status='done'")
        self.job._set_meta('phase','complete')
        result=claim_to_file(self.queue,'worker2',self.root/'complete.json',wait_seconds=55)
        self.assertEqual(result['state'],'complete')

    def test_ready_processing_does_not_wait_for_other_worker(self):
        self.add_tasks(2)
        first=self.item('early');self.item('slow')
        self.queue.enqueue(first);self.queue.acquire_owner()
        self.assertTrue(self.queue.process_one())
        self.assertEqual(self.job.db.execute('SELECT status FROM tasks WHERE task_id=%s',(first['task_id'],)).fetchone()['status'],'done')
        self.assertEqual(self.queue.summary()['queue']['claimed'],1)
        self.assertEqual(self.queue.summary()['queue']['accepted'],1)

    def test_summary_and_queue_lookup_do_not_fetch_historical_payloads(self):
        item=self.item();self.queue.enqueue(item)
        class CountingConnection:
            def __init__(self,db):self.db=db;self.queries=[]
            def execute(self,query,*args):
                self.queries.append(query.as_string(self.db) if hasattr(query,'as_string') else query)
                return self.db.execute(query,*args)
        def summary_queries():
            original=self.queue.db;counted=CountingConnection(original)
            self.queue.db=counted
            try:
                result=self.queue.summary()
                self.assertFalse(any('payload' in query.lower() for query in counted.queries))
                return len(json.dumps(result)),len(counted.queries)
            finally:self.queue.db=original
        cold=summary_queries();hot=summary_queries();self.assertEqual(cold,hot)
        self.queue.db.execute(sql.SQL("""INSERT INTO {}(task_id,token,worker,input_hash,payload,status,created)
            SELECT 'historical-'||i,'old','history','hash',repeat('x',4000),'accepted',0
            FROM generate_series(1,2000) i""").format(self.queue.table))
        self.queue.db.execute(sql.SQL('ANALYZE {}').format(self.queue.table))
        large=summary_queries();self.assertEqual(large[1],cold[1]);self.assertLess(large[0]-cold[0],400)
        plan=self.queue.db.execute(sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT task_id,token,payload FROM {} WHERE status='ready' ORDER BY created,task_id LIMIT 1").format(self.queue.table)).fetchone()['QUERY PLAN'][0]
        native=self.job.db.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT status,token,expires FROM tasks WHERE task_id=%s',(item['task_id'],)).fetchone()['QUERY PLAN'][0]
        self.assertEqual(plan['Plan']['Actual Rows'],1)
        self.assertIn('Index',json.dumps(plan))
        print(json.dumps({'external_locality':{'history_rows':2000,'history_payload_bytes':8000000,'cold_summary_bytes_queries':cold,'hot_summary_bytes_queries':hot,'expanded_summary_bytes_queries':large,'ready_lookup_plan':plan,'native_id_lookup_plan':native}},sort_keys=True))

    def test_summary_preflight_protects_inputs_and_hardlinks(self):
        source=self.job.snapshot_path
        original=source.read_bytes()
        with self.assertRaisesRegex(ValueError,'protected'):
            preflight_summary(source,self.job.job_dir,source)
        descriptor=self.job.job_dir/'graph-job.json'
        with self.assertRaisesRegex(ValueError,'protected'):
            preflight_summary(source,self.job.job_dir,descriptor)
        proposal=self.job.job_dir/'proposal.json';proposal.write_text('{}')
        with self.assertRaisesRegex(ValueError,'protected'):
            preflight_summary(source,self.job.job_dir,proposal)
        import os
        for index,path in enumerate((source,descriptor,proposal)):
            alias=self.root/f'alias-{index}.json';os.link(path,alias)
            with self.assertRaisesRegex(ValueError,'hardlink'):
                preflight_summary(source,self.job.job_dir,alias)
        self.assertEqual(source.read_bytes(),original)
        output=self.root/'progress'/'summary.json'
        self.assertEqual(preflight_summary(source,self.job.job_dir,output),output)

    def test_monitor_reports_while_main_driver_is_busy_without_snapshot_read(self):
        output=preflight_summary(self.job.snapshot_path,self.job.job_dir,self.root/'monitor.json')
        monitor=SummaryMonitor(self.job.snapshot_path,self.job.job_dir,output,run_id=self.job.run_id,
                               metrics=lambda:{'jev':{'peak_active':8},'voyage':{'requests':3}},interval=.01)
        self.addCleanup(monitor.close)
        with patch('kg_builder.snapshot_graph.read_snapshot',side_effect=AssertionError('monitor snapshot read')):
            monitor.start()
            self.assertEqual(json.loads(output.read_text())['controller_state'],'loading')
            monitor.state('running')
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                state=json.loads(output.read_text())
                if state.get('controller_state')=='running' and state.get('phase')=='grouped':break
                time.sleep(.01)
            else:self.fail(f'monitor failed to publish while driver thread was busy: {state}')
            self.assertEqual(state['jev']['peak_active'],8)
            self.assertEqual(state['voyage']['requests'],3)
            monitor.state('failed',failure_type='SyntheticError');monitor.close()
            self.assertEqual(json.loads(output.read_text())['failure_type'],'SyntheticError')

    def test_monitor_recovers_after_transient_windows_replace_failure(self):
        output=preflight_summary(self.job.snapshot_path,self.job.job_dir,self.root/'monitor-retry.json')
        monitor=SummaryMonitor(self.job.snapshot_path,self.job.job_dir,output,run_id=self.job.run_id,interval=.01)
        self.addCleanup(monitor.close)
        original=Path.replace;injected=threading.Event()
        def replace(path,target):
            if threading.current_thread().name=='graph-metadata-monitor' and not injected.is_set():
                injected.set();raise PermissionError('simulated Windows reader holds summary')
            return original(path,target)
        with patch.object(Path,'replace',new=replace):
            monitor.start();monitor.state('running')
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                state=json.loads(output.read_text())
                if state.get('controller_state')=='running' and state.get('phase')=='grouped':break
                time.sleep(.02)
            else:self.fail(f'monitor did not recover transient summary replacement: {state}')
            self.assertTrue(injected.is_set())
            monitor.state('complete');monitor.close()
            self.assertEqual(json.loads(output.read_text())['controller_state'],'complete')

    def test_monitor_terminal_state_without_waiting_for_database_sample(self):
        output=preflight_summary(self.job.snapshot_path,self.job.job_dir,self.root/'monitor-terminal.json')
        entered=threading.Event();release=threading.Event()
        def metrics():
            entered.set();release.wait(10)
            return {'sample':'delayed'}
        monitor=SummaryMonitor(self.job.snapshot_path,self.job.job_dir,output,run_id=self.job.run_id,metrics=metrics,interval=.01)
        self.addCleanup(monitor.close);self.addCleanup(release.set)
        monitor.start()
        self.assertTrue(entered.wait(10))
        monitor.state('failed',failure_type='SyntheticError')
        monitor.close()
        self.assertEqual(json.loads(output.read_text())['controller_state'],'failed')
        release.set();monitor._thread.join(timeout=5)
        self.assertEqual(json.loads(output.read_text())['controller_state'],'failed')

    def test_real_hnsw_duplicate_vectors_respect_k_after_self_filter(self):
        from kg_builder.vector_index import HNSWIndex
        for count,k in ((30,20),(2,20)):
            source=self.root/f'k-bound-{count}.json';fixture(source,count)
            job=self.make_job(source,self.root/f'k-job-{count}',graph_policy='domain-v1',k=k,model=FakeEmbedding())
            job._prepare_store()
            observed=[]
            class RecordingIndex(HNSWIndex):
                def __init__(self,data,*args,**kwargs):
                    super().__init__(data,*args,**kwargs)
                    self.order=iter(data)
                def search(self,vector,limit):
                    mid=next(self.order)
                    results=super().search(vector,limit)
                    observed.append((mid,results))
                    return results
            with patch('kg_builder.snapshot_graph.HNSWIndex',RecordingIndex):job._candidates()
            expected=set()
            for mid,neighbors in observed:
                for neighbor in [r for r in neighbors if r['id']!=mid][:k]:
                    expected.add(tuple(sorted((mid,neighbor['id']))))
            actual={(r['left_id'],r['right_id']) for r in job.db.execute('SELECT left_id,right_id FROM candidates')}
            self.assertEqual(actual,expected)
            if count==30:
                self.assertTrue(any(mid not in {r['id'] for r in rows} and len(rows)==21 for mid,rows in observed))
            else:
                self.assertTrue(all(mid in {r['id'] for r in rows} for mid,rows in observed))
                self.assertTrue(actual)

    def test_six_helper_processes_no_api_clients_domain_native_controller(self):
        source=self.root/'six.json';fixture(source,12)
        class Publisher:
            def __init__(self):self.events=[]
            def publish(self,run_id,event):self.events.append(event)
        publisher=Publisher()
        from kg_builder.decision_api import JevClient
        client=JevClient(max_concurrency=8)
        job=self.make_job(source,self.root/'six-job',graph_policy='domain-v1',client=client,model=FakeEmbedding(),publisher=publisher)
        def proposal(vectors,**kwargs):
            mids=sorted(vectors)
            return {'clusters':[mids[i:i+2] for i in range(0,len(mids),2)],'edges':[],'diagnostics':{'algorithm':'leiden-cpm'}}
        clients=[]
        def members(items,actual_client,**kwargs):
            clients.append(actual_client)
            return [member_result(item) for item in items]
        queue=ExternalGraphQueue(job,initialize=True);self._schemas.add(queue.schema);self.addCleanup(queue.close)
        entry=str(Path(__file__).resolve().parents[1]/'scripts'/'run_external_graph.py')
        # In EACH child, prohibit API/model/publisher construction explicitly.
        wrapper="import sys,runpy;from unittest.mock import patch;from contextlib import ExitStack;" + "\nwith ExitStack() as stack:\n for name in ['kg_builder.decision_api.JevClient','kg_builder.embeddings.create_embedding_model','kg_builder.neo4j_store.Neo4jStore.from_env','kg_builder.snapshot_graph.read_snapshot','kg_builder.snapshot_graph.SnapshotGraph.__init__']:\n  stack.enter_context(patch(name,side_effect=AssertionError('helper constructed remote client')))\n sys.argv=sys.argv[1:]\n runpy.run_path(sys.argv[0],run_name='__main__')"
        base=[sys.executable,'-c',wrapper,entry,'--snapshot',str(source),'--job-dir',str(self.root/'six-job')]
        def invoke(args):
            proc=subprocess.run(base+args,text=True,capture_output=True,timeout=55)
            self.assertEqual(proc.returncode,0,proc.stderr)
            return json.loads(proc.stdout)
        delivered=[False]
        def agents(state):
            if delivered[0] or state['phase']=='complete':return
            with ThreadPoolExecutor(max_workers=6) as pool:
                results=list(pool.map(lambda i:invoke(['claim','--worker',f'agent-{i}','--output',str(self.root/f'claim-{i}.json'),'--wait-seconds','0']),range(6)))
                self.assertTrue(all(r['task_count']==1 for r in results))
                items=[]
                for i in range(6):
                    task=json.loads((self.root/f'claim-{i}.json').read_text())['tasks'][0]
                    item={k:task[k] for k in ('task_id','lease_token','input_hash')}|{'result':{'group_id':task['group_id'],'name':'Source memory research','description':'A source-grounded research theme'}}
                    path=self.root/f'result-{i}.json';path.write_text(json.dumps(item));items.append(path)
                list(pool.map(lambda path:invoke(['enqueue','--input',str(path)]),items))
            self.assertEqual(queue.summary()['queue']['ready'],6)
            delivered[0]=True
        with patch('kg_builder.snapshot_graph.cluster_problem_vectors',side_effect=proposal), patch('kg_builder.decision_api.review_group_members',side_effect=members):
            result=run_controller(queue,poll_seconds=0,on_summary=agents)
        self.assertEqual(result['phase'],'complete')
        self.assertEqual(result['queue']['accepted'],6)
        self.assertTrue(clients);self.assertTrue(all(c is client for c in clients))
        self.assertEqual(client.max_concurrency,8)
        # Exercise the actual shared HTTP semaphore without network/paid calls.
        gate=threading.Event();mutex=threading.Lock();started=[0]
        def fake_http(*args,**kwargs):
            with mutex:
                started[0]+=1
                if started[0]>=8:gate.set()
            if not gate.wait(10):raise RuntimeError('shared client did not admit eight requests')
            return io.StringIO('{}')
        with patch('kg_builder.decision_api.urlopen',side_effect=fake_http):
            with ThreadPoolExecutor(max_workers=16) as pool:list(pool.map(client._open,range(16)))
        self.assertEqual(client.concurrency_stats['peak_active'],8)
        self.assertEqual(client.concurrency_stats['started'],16)
        self.assertTrue(publisher.events)


if __name__=='__main__':
    import unittest
    unittest.main()
