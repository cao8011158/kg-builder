import asyncio
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
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
        self.w=PreparationWorkflow(self.tmp.name,target=2,client=object(),scope_fn=scope,review_fn=review,embedding=Embedding(),lease_seconds=1)
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
             embedding=Embedding(),collectors={'openalex':lambda *a:[],'arxiv':collecting},lease_seconds=1)
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
             collectors={'openalex':alex,'arxiv':arxiv},embedding=Embedding())
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

if __name__=='__main__':unittest.main()
