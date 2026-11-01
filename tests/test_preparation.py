import asyncio
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from kg_builder.preparation import PreparationWorkflow, snapshot_digest
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION


def source(n):
    return dict(title=f'Agent memory paper {n}',abstract=f'We introduce memory system {n} for an agent.',
                year=2026,published_at='2026-01-01',url=f'https://arxiv.org/abs/2601.{n:05d}v1',
                arxiv_id=f'2601.{n:05d}',version_label='v1',source_id=f'https://arxiv.org/abs/2601.{n:05d}v1')

def draft(task):
    return dict(schema_version=RETRIEVAL_SCHEMA_VERSION,paper_id=task['paper_id'],version_id=task['version_id'],
       paper_version=task['paper_version'],extractor_type='external_unreviewed',extractor_version='fake-model-1',
       source_scope='supplied_paper_text',status='draft_requires_review',
       contributions=[dict(key=task['version_id']+'#c1',description='Introduces a memory system')],
       research_problems=[],methods=[],benchmarks=[])

class Embedding:
    model_id='fake:local'
    def encode(self,texts):return [[1.,0.] for _ in texts]

def scope(items,client,min_confidence):
    return [dict(paper_id=i['paper_id'],decision='include',topics=['agent_memory'],reason='model yes',raw={'choices':'yes'}) for i in items]

def review(items,client,min_confidence):
    return [dict(paper_id=i['paper_id'],passed=True,issues=[],raw={'items':'supported'}) for i in items]

class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.w=PreparationWorkflow(self.tmp.name,target=2,client=object(),scope_fn=scope,review_fn=review,embedding=Embedding(),lease_seconds=1,review_policy='legacy-paper')
    def add(self,n):self.w.add_source(source(n),'arxiv','agent memory','agent_memory')

    def test_terminal_replay_preserves_first_completion_and_work(self):
        self.add(12);self.w._scope_batch()
        task=self.w.claim('extract',1,'worker')[0]
        self.w.submit([dict(task_id=task['task_id'],lease_token=task['lease_token'],
                            input_hash=task['input_hash'],result=draft(task))])
        self.w._review_batch()
        db=self.w._db()
        with db:db.execute("UPDATE run SET collection_state='sealed' WHERE id=1")
        db.close()
        first=asyncio.run(self.w.run(collect=False))
        db=self.w._db()
        before=(db.execute('SELECT completed FROM run WHERE id=1').fetchone()[0],
                db.execute('SELECT count(*) FROM tasks').fetchone()[0],
                db.execute('SELECT count(*) FROM decision_audits').fetchone()[0])
        db.close()
        second=asyncio.run(self.w.run(collect=False))
        db=self.w._db()
        after=(db.execute('SELECT completed FROM run WHERE id=1').fetchone()[0],
               db.execute('SELECT count(*) FROM tasks').fetchone()[0],
               db.execute('SELECT count(*) FROM decision_audits').fetchone()[0])
        db.close()
        self.assertEqual(before,after)
        self.assertEqual(first['wall_seconds'],second['wall_seconds'])

    def test_claim_locality_ignores_completed_same_kind_tasks(self):
        self.add(9);self.w._scope_batch()
        db=self.w._db()
        original_db=self.w._db
        ticks=[0]
        def counted_db():
            con=original_db()
            def progress():ticks[0]+=1;return 0
            con.set_progress_handler(progress,1)
            return con
        self.w._db=counted_db
        def measured():
            db.execute("UPDATE tasks SET state='queued',lease_until=NULL WHERE attempt=0")
            db.commit()
            ticks[0]=0
            result=self.w.claim('extract',1,'locality')
            self.assertEqual(len(result),1)
            return ticks[0]
        baseline=measured()
        paper=db.execute('SELECT paper_id,fingerprint FROM papers LIMIT 1').fetchone()
        now=time.time()
        db.executemany('''INSERT INTO tasks(task_id,paper_id,kind,state,fingerprint,attempt,created,result_json)
            VALUES(?,?, 'extract','done',?,?,?,?)''',
            [(f'done-{i}',paper['paper_id'],paper['fingerprint'],i+1,now+i,'x'*10000) for i in range(1000)])
        db.commit()
        inflated=measured()
        db.close()
        self.assertGreater(baseline,0)
        self.assertLess(inflated-baseline,200,(baseline,inflated))
    def test_claim_duplicate_stale_source_and_repair(self):
        self.add(1);self.w._scope_batch()
        a,b=[],[]
        t1=threading.Thread(target=lambda:a.extend(self.w.claim('extract',1,'a')))
        t2=threading.Thread(target=lambda:b.extend(self.w.claim('extract',1,'b')))
        t1.start();t2.start();t1.join();t2.join()
        self.assertEqual(len(a)+len(b),1)
        task=(a+b)[0]
        bad=dict(task_id=task['task_id'],lease_token='wrong',input_hash=task['input_hash'],result=draft(task))
        with self.assertRaisesRegex(ValueError,'lease'):self.w.submit([bad])
        good={**bad,'lease_token':task['lease_token']}
        self.assertEqual(self.w.submit([good])[0]['status'],'accepted')
        self.assertEqual(self.w.submit([good])[0]['status'],'duplicate')
        with self.assertRaisesRegex(ValueError,'Conflicting'):self.w.submit([{**good,'result':{**good['result'],'extractor_version':'other'}}])
        self.w._review_batch()
        self.assertEqual(self.w.status()['review']['ready'],1)
        self.add(2);self.w._scope_batch()
        task=self.w.claim('extract',1,'a')[0]
        changed=source(2);changed['abstract']='Changed primary text'
        self.w.add_source(changed,'arxiv','agent memory','agent_memory')
        with self.assertRaises(ValueError):self.w.submit([dict(task_id=task['task_id'],input_hash=task['input_hash'],lease_token=task['lease_token'],result=draft(task))])
        self.assertEqual(self.w.status()['papers']['pending'],1)

    def test_streaming_snapshot_and_restart(self):
        # The first task is claimable before the producer completes the second source.
        started=threading.Event();release=threading.Event()
        def collecting(query,limit,since):
            yield source(3)
            started.set();release.wait(4)
            yield source(4)
        w=PreparationWorkflow(self.tmp.name,target=2,client=object(),scope_fn=scope,review_fn=review,
             embedding=Embedding(),collectors={'openalex':lambda *a:[],'arxiv':collecting},lease_seconds=1,review_policy='legacy-paper')
        async def exercise():
            task=asyncio.create_task(w.run(poll_seconds=.02))
            for _ in range(100):
                if w.status()['tasks'].get('extract/queued'):break
                await asyncio.sleep(.02)
            first=w.claim('extract',1,'worker')
            self.assertEqual(len(first),1)
            self.assertEqual(sum(w.status()['papers'].values()),1)
            w.submit([dict(task_id=first[0]['task_id'],lease_token=first[0]['lease_token'],input_hash=first[0]['input_hash'],result=draft(first[0]))])
            for _ in range(100):
                if w.status()['review'].get('ready'):break
                await asyncio.sleep(.02)
            self.assertEqual(w.status()['review'].get('ready'),1)
            self.assertEqual(w.status()['collection_state'],'open')
            release.set()
            for _ in range(400):
                second=w.claim('extract',1,'worker')
                if second:break
                await asyncio.sleep(.02)
            self.assertEqual(len(second),1)
            w.submit([dict(task_id=second[0]['task_id'],lease_token=second[0]['lease_token'],input_hash=second[0]['input_hash'],result=draft(second[0]))])
            return await asyncio.wait_for(task,5)
        status=asyncio.run(exercise());self.assertEqual(status['review']['ready'],2)
        reopened=PreparationWorkflow(self.tmp.name,embedding=Embedding())
        path=Path(self.tmp.name)/'snapshot.json'
        snap=reopened.snapshot(path)
        self.assertEqual(snap['snapshot_hash'],snapshot_digest(snap))
        before=path.stat().st_mtime_ns
        reopened.snapshot(path)
        self.assertEqual(before,path.stat().st_mtime_ns)
        self.assertEqual(len(snap['vectors']),2)
        self.assertEqual(snap['counts']['ready'],2)

    def test_balanced_sources_topics_and_resume_without_refetch(self):
        calls=[]
        from kg_builder.preparation import QUERIES
        def alex(query,limit,since,until):
            calls.append(('openalex',query))
            i=QUERIES.index(query)
            return [source(100+i*20+j) for j in range(8)]
        def arxiv(query,limit,since):
            calls.append(('arxiv',query))
            i=QUERIES.index(query)
            return [source(200+i*20+j) for j in range(8)]
        w=PreparationWorkflow(Path(self.tmp.name)/'balanced',target=12,client=object(),scope_fn=scope,review_fn=review,
             collectors={'openalex':alex,'arxiv':arxiv},embedding=Embedding(),review_policy='legacy-paper')
        asyncio.run(w.collect())
        self.assertEqual(len(calls),6)
        self.assertEqual(sum(w.status()['papers'].values()),12)
        con=__import__('kg_builder.storage',fromlist=['connect']).connect(w.master)
        self.assertEqual(con.execute('SELECT count(DISTINCT source_name||query) FROM discovery_hits').fetchone()[0],6)
        con.close()
        asyncio.run(w.collect())
        self.assertEqual(len(calls),6)

    def test_append_only_audit_and_recovered_transient_error(self):
        self.add(7)
        class Client:
            last_request_audit={'attempts':2,'http_status':503}
        self.w.client=Client()
        def failed(items,client,min_confidence):raise RuntimeError('temporary')
        self.w.scope_fn=failed
        with self.assertRaises(RuntimeError):self.w._scope_batch()
        self.assertEqual(self.w.status()['error'],'RuntimeError')
        self.w.scope_fn=scope;self.w._scope_batch()
        self.assertIsNone(self.w.status()['error'])
        task=self.w.claim('extract',1,'worker')[0]
        self.w.submit([dict(task_id=task['task_id'],lease_token=task['lease_token'],input_hash=task['input_hash'],result=draft(task))])
        self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=i['paper_id'],passed=False,issues=['missing'],raw={'usage':1}) for i in items]
        self.w._review_batch()
        repair=self.w.claim('repair_extraction',1,'worker')[0]
        self.w.submit([dict(task_id=repair['task_id'],lease_token=repair['lease_token'],input_hash=repair['input_hash'],result=draft(repair))])
        self.w.review_fn=review;self.w._review_batch()
        db=self.w._db()
        audits=[tuple(r) for r in db.execute('SELECT stage,attempt FROM decision_audits ORDER BY id')]
        db.close()
        self.assertEqual(audits,[('scope_error',0),('scope',0),('field_review',1),('field_review',2)])

    def test_shortfall_and_stop_before_loop(self):
        w=PreparationWorkflow(Path(self.tmp.name)/'short',target=60,client=object(),scope_fn=scope,
             review_fn=review,collectors={'openalex':lambda *a:[], 'arxiv':lambda *a:[]},embedding=Embedding())
        done=asyncio.run(w.run(poll_seconds=.01))
        self.assertEqual(done['error'],'collection_shortfall:0/60')
        with self.assertRaises(ValueError):w.snapshot(Path(self.tmp.name)/'invalid.json')
        stopped=threading.Event();stopped.set()
        w2=PreparationWorkflow(Path(self.tmp.name)/'stop',client=object(),scope_fn=scope,review_fn=review)
        self.assertEqual(asyncio.run(w2.run(collect=False,stop_event=stopped))['collection_state'],'open')

    def test_repair_limit_and_expired_lease(self):
        self.add(5);self.w._scope_batch()
        t=self.w.claim('extract',1,'a')[0]
        time.sleep(1.02)
        newer=self.w.claim('extract',1,'b')[0]
        with self.assertRaisesRegex(ValueError,'lease'):
            self.w.submit([dict(task_id=t['task_id'],lease_token=t['lease_token'],input_hash=t['input_hash'],result=draft(t))])
        self.w.submit([dict(task_id=newer['task_id'],lease_token=newer['lease_token'],input_hash=newer['input_hash'],result=draft(newer))])
        self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=i['paper_id'],passed=False,issues=['unsupported'],raw={}) for i in items]
        self.w._review_batch();self.assertEqual(self.w.status()['tasks']['repair_extraction/queued'],1)
        for attempt in range(2):
            t=self.w.claim('repair_extraction',1,'b')[0]
            self.w.submit([dict(task_id=t['task_id'],lease_token=t['lease_token'],input_hash=t['input_hash'],result=draft(t))])
            self.w._review_batch()
        self.assertEqual(self.w.status()['review']['pending'],1)
        self.assertNotIn('repair_extraction/queued',self.w.status()['tasks'])

class ItemPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.results=[]
        def review_items(items,client,min_confidence):
            self.results.append([x['key'] for x in items])
            return [dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],
                passed=not x['key'].endswith('#bad') or len(self.results)>1,
                confidence=.2,probability=.9,decision_metric='probability',issues=['unsupported'] if x['key'].endswith('#bad') and len(self.results)==1 else [],raw={}) for x in items]
        self.w=PreparationWorkflow(self.tmp.name,target=1,client=object(),scope_fn=scope,review_fn=review_items,embedding=Embedding())
        self.w.add_source(source(50),'arxiv','agent memory','agent_memory');self.w._scope_batch()
    def submit(self,task,payload):
        return self.w.submit([dict(task_id=task['task_id'],lease_token=task['lease_token'],input_hash=task['input_hash'],result=payload)])
    def test_keyed_repair_freezes_approved_and_partial_snapshot(self):
        task=self.w.claim()[0];payload=draft(task)
        payload['contributions'].append(dict(key=task['version_id']+'#bad',description='unsupported draft'))
        self.submit(task,payload);self.w._review_batch()
        repair=self.w.claim('repair_extraction')[0]
        self.assertEqual([x['key'] for x in repair['requested_items']],[task['version_id']+'#bad'])
        changed=draft(repair);changed['contributions']=[dict(key=task['version_id']+'#bad',description='repaired')]
        self.submit(repair,changed);self.w._review_batch()
        self.assertEqual(self.results[1],[task['version_id']+'#bad'])
        db=self.w._db();row=db.execute('SELECT extraction_json,review_json FROM papers').fetchone();db.close()
        extraction=json.loads(row[0]);self.assertEqual(len(extraction['contributions']),2)
        self.assertIn(payload['contributions'][0],extraction['contributions'])
        self.assertEqual(self.w.status()['review']['ready'],1)
        db=self.w._db()
        with db:db.execute("UPDATE run SET collection_state='sealed',completed=? WHERE id=1",(time.time(),))
        db.close();snap=self.w.snapshot(Path(self.tmp.name)/'item.json')
        self.assertEqual(snap['counts']['ready'],1)
        self.assertEqual(len(snap['review_metadata'][task['paper_id']]['items']),2)
        self.assertEqual(snap['review_metadata'][task['paper_id']]['decision_metric'],'probability')
        by_key={x['key']:x for x in snap['review_metadata'][task['paper_id']]['items']}
        self.assertEqual(by_key[task['version_id']+'#c1']['probability'],.9)

    def test_empty_is_terminal(self):
        task=self.w.claim()[0];payload=draft(task);payload['contributions']=[]
        self.submit(task,payload);self.w._review_batch()
        self.assertEqual(self.w.status()['review']['no_usable_items'],1)
        self.assertFalse(self.w.claim('repair_extraction'))
        db=self.w._db()
        with db:db.execute("UPDATE run SET collection_state='sealed',completed=? WHERE id=1",(time.time(),))
        db.close();snapshot=self.w.snapshot(Path(self.tmp.name)/'empty.json')
        self.assertEqual(snapshot['counts'],dict(collected=1,ready=0,excluded=0,pending=0,no_usable_items=1))

    def test_wrong_review_hash_does_not_approve(self):
        task=self.w.claim()[0];self.submit(task,draft(task))
        self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],
            input_hash='wrong',passed=True,confidence=.99,probability=.99,decision_metric='probability',issues=[],raw={}) for x in items]
        with self.assertRaisesRegex(ValueError,'hash'):self.w._review_batch()
        self.assertEqual(self.w.status()['review']['queued'],1)
        db=self.w._db();self.assertEqual(db.execute("SELECT count(*) FROM item_reviews WHERE state='approved'").fetchone()[0],0);db.close()

    def test_source_change_revokes_approval(self):
        task=self.w.claim()[0];self.submit(task,draft(task));self.w._review_batch()
        changed=source(50);changed['abstract']='Changed primary source'
        self.w.add_source(changed,'arxiv','agent memory','agent_memory')
        self.assertEqual(self.w.status()['review']['pending'],1)
        db=self.w._db();row=db.execute('SELECT state,approved_json FROM item_reviews').fetchone();db.close()
        self.assertEqual(row['state'],'revoked');self.assertIsNone(row['approved_json'])

    def test_jev_stats_survive_reopen_and_terminal_replay(self):
        task=self.w.claim()[0];self.submit(task,draft(task));self.w._review_batch()
        class Measured:
            concurrency_stats=dict(active=0,peak_active=4,started=9,finished=9,queued=0,
                peak_queued=2,cumulative_request_duration_s=4.5,request_wall_union_s=1.7)
        self.w.client=Measured()
        db=self.w._db()
        with db:db.execute("UPDATE run SET collection_state='sealed' WHERE id=1")
        db.close()
        self.assertEqual(asyncio.run(self.w.run(collect=False))['jev_concurrency']['peak_active'],4)
        reopened=PreparationWorkflow(self.tmp.name)
        self.assertEqual(reopened.status()['jev_concurrency']['peak_active'],4)
        self.assertEqual(asyncio.run(reopened.run(collect=False))['jev_concurrency']['peak_active'],4)
        db=reopened._db();self.assertEqual(json.loads(db.execute('SELECT jev_stats_json FROM run').fetchone()[0])['started'],9);db.close()

    def test_parallel_scope_claims_overlap_without_duplicates(self):
        self.w.add_source(source(51),'arxiv','agent memory','agent_memory')
        barrier=threading.Barrier(2,timeout=3);seen=[]
        def concurrent(items,client,min_confidence):
            seen.extend(x['paper_id'] for x in items)
            barrier.wait()
            return scope(items,client,min_confidence)
        self.w.scope_fn=concurrent;self.w.config['batch_size']=1
        # First source was scoped during setUp; queue it again for this claim race.
        db=self.w._db()
        with db:db.execute("UPDATE papers SET scope_state='queued' WHERE paper_id=(SELECT paper_id FROM papers ORDER BY discovered LIMIT 1)")
        db.close()
        errors=[]
        def work():
            try:self.w._scope_batch()
            except BaseException as exc:errors.append(exc)
        threads=[threading.Thread(target=work) for _ in range(2)]
        for t in threads:t.start()
        for t in threads:t.join(5)
        self.assertFalse(errors,errors);self.assertEqual(len(seen),2)
        self.assertEqual(len(set(seen)),2)

    def test_parallel_api_extractors_share_claim_validator(self):
        self.w.add_source(source(51),'arxiv','agent memory','agent_memory');self.w._scope_batch()
        barrier=threading.Barrier(2,timeout=3);seen=[]
        class Extractor:
            model='fake'
            def extract_draft(self,pid,vid,text,paper_version):
                seen.append(pid);barrier.wait()
                return draft(dict(paper_id=pid,version_id=vid,paper_version=paper_version))
        self.w.extractor=Extractor();self.w.config['batch_size']=1
        errors=[]
        def work():
            try:self.w._api_extract()
            except BaseException as exc:errors.append(exc)
        workers=[threading.Thread(target=work) for _ in range(2)]
        for t in workers:t.start()
        for t in workers:t.join(5)
        self.assertFalse(errors,errors)
        self.assertEqual(len(set(seen)),2)
        self.assertEqual(self.w.status()['tasks']['extract/done'],2)

    def test_concurrent_identical_submission_is_accepted_then_duplicate(self):
        task=self.w.claim()[0]
        entry=dict(task_id=task['task_id'],lease_token=task['lease_token'],input_hash=task['input_hash'],result=draft(task))
        barrier=threading.Barrier(2,timeout=3);results=[];errors=[]
        def submit():
            try:
                barrier.wait();results.append(self.w.submit([entry])[0]['status'])
            except BaseException as exc:errors.append(exc)
        workers=[threading.Thread(target=submit) for _ in range(2)]
        for t in workers:t.start()
        for t in workers:t.join(5)
        self.assertFalse(errors,errors)
        self.assertEqual(sorted(results),['accepted','duplicate'])

    def test_plural_error_audits_and_diagnostic_failure_are_retained(self):
        task=self.w.claim()[0];self.submit(task,draft(task))
        class APIError(RuntimeError):pass
        def fail(items,client,min_confidence):
            exc=APIError('temporary');exc.request_audits=[{'request_id':'a','request_duration_s':.1},{'request_id':'b','request_duration_s':.2}]
            raise exc
        self.w.review_fn=fail
        with self.assertRaises(APIError):self.w._review_batch()
        db=self.w._db();error=json.loads(db.execute("SELECT payload_json FROM decision_audits WHERE stage='item_review_error'").fetchone()[0]);db.close()
        self.assertEqual([x['request_id'] for x in error['requests']],['a','b'])
        self.w.review_fn=None
        def success(items,client,min_confidence,*,decision_metric):
            self.assertEqual(decision_metric,'probability')
            return [dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],passed=True,confidence=.2,probability=.9,decision_metric='probability',issues=[],raw={}) for x in items]
        with patch('kg_builder.decision_api.review_items',side_effect=success),patch('kg_builder.decision_api.review_completeness',side_effect=lambda items,client,min_confidence,*,decision_metric:fail(items,client,min_confidence)):
            self.w._review_batch()
        db=self.w._db();diagnostic=json.loads(db.execute("SELECT payload_json FROM decision_audits WHERE stage='completeness'").fetchone()[0]);db.close()
        self.assertEqual([x['request_id'] for x in diagnostic['requests']],['a','b'])
        self.assertIsNone(diagnostic['completeness']['methods']['passed'])

    def test_failed_second_review_drops_only_item(self):
        self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],
              passed=not x['key'].endswith('#bad'),confidence=.2,probability=.9,decision_metric='probability',issues=['unsupported'],raw={}) for x in items]
        task=self.w.claim()[0];payload=draft(task)
        payload['contributions'].append(dict(key=task['version_id']+'#bad',description='unsupported'))
        self.submit(task,payload);self.w._review_batch()
        repair=self.w.claim('repair_extraction')[0]
        changed=draft(repair);changed['contributions']=[payload['contributions'][-1]]
        self.submit(repair,changed);self.w._review_batch()
        self.assertEqual(self.w.status()['review']['ready'],1)
        db=self.w._db();row=db.execute('SELECT extraction_json,review_json FROM papers').fetchone();db.close()
        self.assertEqual([x['key'] for x in json.loads(row[0])['contributions']],[task['version_id']+'#c1'])
        self.assertEqual({x['key']:x['state'] for x in json.loads(row[1])['items']}[task['version_id']+'#bad'],'dropped')

    def test_probability_gate_ignores_low_confidence_and_blocks_low_probability(self):
        task=self.w.claim()[0];payload=draft(task)
        payload['contributions'].append(dict(key=task['version_id']+'#bad',description='another claim'))
        self.submit(task,payload)
        def outcomes(items,client,min_confidence):
            return [dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],
                passed=True,confidence=.05 if x['key'].endswith('#c1') else .99,
                probability=.92 if x['key'].endswith('#c1') else .15,
                decision_metric='probability',issues=[],raw={}) for x in items]
        self.w.review_fn=outcomes;self.w._review_batch()
        db=self.w._db();rows=db.execute('SELECT item_key,state FROM item_reviews ORDER BY item_key').fetchall()
        review=json.loads(db.execute('SELECT review_json FROM papers').fetchone()[0]);db.close()
        self.assertEqual({x['item_key'].split('#')[-1]:x['state'] for x in rows},{'bad':'repair_queued','c1':'approved'})
        self.assertEqual((review['decision_metric'],review['decision_threshold']),('probability',.7))
        self.assertEqual({x['key'].split('#')[-1]:x['probability'] for x in review['items']},{'bad':.15,'c1':.92})
        self.assertEqual(self.w.status()['decision_metric'],'probability')

    def test_missing_probability_fails_closed_and_reopen_keeps_new_metric(self):
        task=self.w.claim()[0];self.submit(task,draft(task))
        self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=x['paper_id'],field=x['field'],
            key=x['key'],input_hash=x['input_hash'],passed=True,confidence=.99,issues=[],raw={}) for x in items]
        with self.assertRaisesRegex(ValueError,'probability'):self.w._review_batch()
        self.assertEqual(self.w.status()['review']['queued'],1)
        db=self.w._db();attempts=db.execute('SELECT attempts FROM item_reviews').fetchone()[0];db.close()
        self.assertEqual(attempts,0)
        reopened=PreparationWorkflow(self.tmp.name,client=object(),review_fn=self.w.review_fn)
        self.assertEqual(reopened.decision_metric,'probability')
        self.assertEqual(reopened.config['decision_metric'],'probability')

    def test_invalid_probability_does_not_approve_injected_callback(self):
        task=self.w.claim()[0];self.submit(task,draft(task))
        for value in (2,-.1,True,False,float('nan'),float('inf'),'0.95'):
            with self.subTest(value=value):
                self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=x['paper_id'],field=x['field'],
                    key=x['key'],input_hash=x['input_hash'],passed=True,confidence=.99,
                    probability=value,decision_metric='probability',issues=[],raw={}) for x in items]
                with self.assertRaisesRegex(ValueError,'probability'):self.w._review_batch()
                self.assertEqual(self.w.status()['review']['queued'],1)
                db=self.w._db();row=db.execute('SELECT state,attempts,approved_json FROM item_reviews').fetchone();db.close()
                self.assertEqual((row['state'],row['attempts'],row['approved_json']),('queued',0,None))
                self.assertFalse(self.w._item_passes(dict(passed=True,probability=value,confidence=.99)))

    def test_declared_metric_mismatch_is_technical_error(self):
        task=self.w.claim()[0];self.submit(task,draft(task))
        self.w.review_fn=lambda items,client,min_confidence:[dict(paper_id=x['paper_id'],field=x['field'],
            key=x['key'],input_hash=x['input_hash'],passed=True,confidence=.99,
            probability=.95,decision_metric='confidence',issues=[],raw={}) for x in items]
        with self.assertRaisesRegex(ValueError,'metric'):self.w._review_batch()
        db=self.w._db();row=db.execute('SELECT state,attempts FROM item_reviews').fetchone();db.close()
        self.assertEqual(tuple(row),('queued',0))

    def test_boolean_decision_threshold_is_invalid(self):
        with self.assertRaisesRegex(ValueError,'threshold'):
            PreparationWorkflow(Path(self.tmp.name)/'bool-threshold',client=object(),min_confidence=True)

    def test_native_adapter_receives_probability_metric_and_completeness_is_diagnostic(self):
        task=self.w.claim()[0];self.submit(task,draft(task));self.w.review_fn=None
        def native(items,client,min_confidence,*,decision_metric):
            self.assertEqual((decision_metric,min_confidence),('probability',.7))
            return [dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],
                passed=True,confidence=.01,probability=.95,decision_metric=decision_metric,issues=[],raw={}) for x in items]
        def diagnostic(items,client,min_confidence,*,decision_metric):
            self.assertEqual(decision_metric,'probability')
            return [dict(paper_id=x['paper_id'],completeness={'contributions':dict(passed=False)}) for x in items]
        with patch('kg_builder.decision_api.review_items',side_effect=native),patch('kg_builder.decision_api.review_completeness',side_effect=diagnostic):
            self.w._review_batch()
        self.assertEqual(self.w.status()['review']['ready'],1)
        db=self.w._db();review=json.loads(db.execute('SELECT review_json FROM papers').fetchone()[0]);db.close()
        self.assertFalse(review['completeness']['contributions']['passed'])

    def test_legacy_config_without_metric_restores_confidence_gate(self):
        db=self.w._db()
        with db:
            old=dict(self.w.config);old.pop('decision_metric')
            db.execute('UPDATE run SET config_json=? WHERE id=1',(json.dumps(old),))
        db.close()
        old=PreparationWorkflow(self.tmp.name,client=object(),scope_fn=scope,embedding=Embedding())
        self.assertEqual(old.decision_metric,'confidence')
        self.assertNotIn('decision_metric',old.status()['config'])
        task=old.claim()[0];old.submit([dict(task_id=task['task_id'],lease_token=task['lease_token'],
            input_hash=task['input_hash'],result=draft(task))])
        def legacy(items,client,min_confidence):
            return [dict(paper_id=x['paper_id'],field=x['field'],key=x['key'],input_hash=x['input_hash'],
                passed=True,confidence=.9,issues=[],raw={}) for x in items]
        old.review_fn=legacy;old._review_batch()
        self.assertEqual(old.status()['review']['ready'],1)
        db=old._db();review=json.loads(db.execute('SELECT review_json FROM papers').fetchone()[0]);db.close()
        self.assertNotIn('decision_metric',review)

    def test_native_scope_adapter_reads_persisted_metric(self):
        self.w.add_source(source(51),'arxiv','agent memory','agent_memory')
        db=self.w._db()
        with db:
            db.execute("UPDATE papers SET scope_state='queued' WHERE paper_id=(SELECT paper_id FROM papers ORDER BY discovered LIMIT 1)")
        db.close()
        self.w.scope_fn=None
        def classify(items,client,min_confidence,*,decision_metric):
            self.assertEqual((decision_metric,min_confidence),('probability',.7))
            return scope(items,client,min_confidence)
        with patch('kg_builder.decision_api.classify_scope',side_effect=classify):
            self.w._scope_batch()
        self.assertEqual(self.w.status()['papers']['include'],2)

if __name__=='__main__':unittest.main()
