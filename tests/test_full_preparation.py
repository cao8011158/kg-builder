import asyncio
import json
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path
from xml.etree import ElementTree as ET

from kg_builder.full_preparation import FullPreparationWorkflow, parse_page
from tests.test_preparation import draft, Embedding


def paper(n, abstract=True):
    return {'id':f'https://openalex.org/W{n}','doi':None,'title':f'Agent memory {n}',
            'publication_year':2026,'publication_date':'2026-01-02','authorships':[],
            'abstract_inverted_index':{'Agent':[0],'memory':[1]} if abstract else None,
            'ids':{},'primary_location':None}

def page(rows, count, following=None):
    return json.dumps({'results':rows,'meta':{'count':count,'next_cursor':following}}).encode()

def atom(rows, total):
    ns='http://www.w3.org/2005/Atom'
    o='http://a9.com/-/spec/opensearch/1.1/'
    root=ET.Element(f'{{{ns}}}feed')
    ET.SubElement(root,f'{{{o}}}totalResults').text=str(total)
    for n in rows:
        e=ET.SubElement(root,f'{{{ns}}}entry')
        for tag,val in [('id',f'https://arxiv.org/abs/2601.{n:05d}v1'),('published','2026-01-02T00:00:00Z'),
                        ('updated','2026-01-02T00:00:00Z'),('title',f'Agent memory {n}'),('summary',f'Memory system {n}')]:
            ET.SubElement(e,f'{{{ns}}}{tag}').text=val
    return ET.tostring(root)

class FullPreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.calls=[]
    def fetch(self,source,plan,window,position):
        self.calls.append((source,window['since'],position,time.monotonic()))
        if source=='arxiv':return atom([],0)
        if window['since']!='2026-01-01':return page([],0)
        if position=='*':return page([paper(i) for i in range(100)],101,'next')
        if position=='next':return page([paper(100)],101)
        raise AssertionError(position)
    def wf(self,fetch=None,**kwargs):
        return FullPreparationWorkflow(self.tmp.name,fetcher=fetch or self.fetch,client=object(),**kwargs)
    def test_complete_pages_and_resume_without_refetch(self):
        w=self.wf()
        asyncio.run(w.collect())
        self.assertEqual(sum(x['received'] for x in w.status()['full_streams']),101)
        self.assertEqual(sum(w.status()['papers'].values()),101)
        db=w._db()
        self.assertEqual(db.execute("SELECT count(*) FROM full_pages WHERE state='applied'").fetchone()[0],10)
        self.assertEqual(db.execute('SELECT count(*) FROM full_pages WHERE length(raw)>0').fetchone()[0],10)
        db.close()
        before=len(self.calls)
        asyncio.run(w.collect())
        self.assertEqual(len(self.calls),before)
        with self.assertRaisesRegex(ValueError,'plan differs'):
            self.wf(plan={**w.plan,'openalex_query':'conflicting'})
        self.assertEqual(w.config['jev_concurrency'],4)  # Direct workflow defaults are persisted.
    def test_custom_plan_reopens_without_override(self):
        from kg_builder.full_preparation import PLAN
        custom={**json.loads(PLAN.read_text()),'openalex_query':'LLM AND agent AND memory'}
        w=self.wf(plan=custom,jev_concurrency=8,scope_workers=4,review_workers=4,generation_concurrency=4)
        reopened=FullPreparationWorkflow(self.tmp.name,client=object())
        self.assertEqual(reopened.plan,custom)
        self.assertEqual(reopened.config['jev_concurrency'],8)
        with self.assertRaisesRegex(ValueError,'plan differs'):
            self.wf(plan={**custom,'openalex_query':'other'})
    def test_replay_fetched_page_and_deferred_abstract(self):
        calls=[]
        def fetch(source,plan,window,pos):
            calls.append((source,window['since'],pos))
            if source=='arxiv':return atom([],0)
            if window['since']=='2026-01-01':return page([paper(1,False)],1)
            if window['since']=='2026-04-01':return page([paper(1,True)],1)
            return page([],0)
        w=self.wf(fetch=fetch)
        db=w._db()
        with db:db.execute('INSERT INTO full_pages(stream_id,window_index,position,raw) VALUES(?,?,?,?)',
                           ('openalex-0',1,'*',page([paper(1,False)],1)))
        db.close()
        asyncio.run(w.collect())
        self.assertNotIn(('openalex','2026-01-01','*'),calls)
        db=w._db();r=db.execute('SELECT text FROM papers').fetchone();db.close()
        self.assertEqual(r[0],'Agent memory')
        self.assertEqual(sum(w.status()['papers'].values()),1)
    def test_second_window_uses_local_count_and_replays_fetched_page(self):
        calls=[]
        def fetch(source,plan,window,pos):
            calls.append((window['since'],pos))
            return page([paper(500)],1)
        w=self.wf(fetch=fetch)
        db=w._db()
        with db:db.execute("UPDATE full_streams SET window_index=1,received=1000,position='*' WHERE stream_id='openalex-0'")
        db.close()
        original=w._admit_full
        w._admit_full=lambda *args: (_ for _ in ()).throw(RuntimeError('interrupted admission'))
        with self.assertRaisesRegex(RuntimeError,'interrupted'):
            w._consume('openalex-0')
        w._admit_full=original
        self.assertFalse(w._consume('openalex-0'))
        self.assertEqual(calls,[('2026-01-01','*')])
        db=w._db()
        stream=db.execute("SELECT window_index,window_received,received,state FROM full_streams WHERE stream_id='openalex-0'").fetchone()
        self.assertEqual(tuple(stream),(2,0,1001,'done'))
        self.assertEqual(db.execute("SELECT state FROM full_pages WHERE stream_id='openalex-0' AND window_index=1").fetchone()[0],'applied')
        db.close()
    def test_empty_second_window_does_not_inherit_prior_count(self):
        w=self.wf(fetch=lambda *a:page([],10))
        db=w._db()
        with db:db.execute("UPDATE full_streams SET window_index=1,received=1000,window_received=0,position='cursor2' WHERE stream_id='openalex-0'")
        db.close()
        with self.assertRaisesRegex(ValueError,'Incomplete OpenAlex'):
            w._consume('openalex-0')
        self.assertEqual(w.status()['full_streams'][-1]['state'],'open')  # Other lanes are unaffected.
        db=w._db()
        self.assertEqual(db.execute("SELECT state FROM full_streams WHERE stream_id='openalex-0'").fetchone()[0],'failed')
        db.close()
    def test_earlier_publication_evidence_revokes_and_is_retained(self):
        w=self.wf()
        first={'title':'Agent memory','abstract':'Paper evidence','published_at':'2025-06-01',
               'doi':'10.1234/window-test','url':'https://openalex.org/W100','source_id':'https://openalex.org/W100'}
        older={**first,'published_at':'2024-12-31','source_id':'https://openalex.org/W101',
               'url':'https://openalex.org/W101'}
        self.assertEqual(w._admit_full(first,'openalex'),1)
        db=w._db()
        with db:
            db.execute("UPDATE papers SET scope_state='include',review_state='ready'")
            pid,fp=db.execute('SELECT paper_id,fingerprint FROM papers').fetchone()
            db.execute("INSERT INTO item_reviews(paper_id,field,item_key,fingerprint,input_hash,state,attempts,payload_json,approved_json) VALUES(?,?,?,?,?,'approved',1,'{}','{}')",
                       (pid,'methods','m1',fp,'hash'))
            db.execute("INSERT INTO tasks(task_id,paper_id,kind,state,fingerprint,attempt,created,lease_token) VALUES('leased-older',?,'extract','leased',?,0,?,'lease')",(pid,fp,time.time()))
        db.close()
        self.assertEqual(w._admit_full(older,'openalex'),0)
        db=w._db();self.assertEqual(tuple(db.execute('SELECT scope_state,review_state,reason FROM papers').fetchone()),
                                    ('pending','pending','before_window'))
        self.assertEqual(tuple(db.execute('SELECT state,approved_json FROM item_reviews').fetchone()),('revoked',None))
        self.assertEqual(tuple(db.execute('SELECT state,reason,lease_token FROM tasks').fetchone()),('failed','before_window',None))
        db.close()
        from kg_builder.storage import connect
        master=connect(w.master)
        self.assertEqual(master.execute('SELECT first_published FROM papers').fetchone()[0],'2024-12-31')
        self.assertEqual(master.execute('SELECT count(*) FROM source_records').fetchone()[0],2)
        master.close()
        empty_dir=Path(self.tmp.name)/'older-first'
        other=FullPreparationWorkflow(empty_dir,client=object())
        self.assertEqual(other._admit_full(older,'openalex'),0)
        self.assertEqual(sum(other.status()['papers'].values()),0)
        master=connect(other.master)
        self.assertEqual(master.execute('SELECT first_published FROM papers').fetchone()[0],'2024-12-31')
        master.close()
    def test_failure_does_not_seal_or_spin(self):
        def failing(source,plan,window,pos):
            if source=='openalex' and window['since']=='2026-01-01':raise RuntimeError('source unavailable https://example.test/?api_key=private')
            return self.fetch(source,plan,window,pos)
        w=self.wf(fetch=failing)
        with self.assertRaisesRegex(RuntimeError,'source unavailable'):
            asyncio.run(asyncio.wait_for(w.run(poll_seconds=.01),5))
        self.assertEqual(w.status()['collection_state'],'open')
        self.assertTrue(w.status()['error'].startswith('full_collection_failed:'))
        self.assertNotIn('private',json.dumps(w.status()['full_streams']))
        recovered=self.wf()
        asyncio.run(recovered.collect())
        self.assertEqual(recovered.status()['collection_state'],'sealed')
    def test_streaming_scope_before_last_page(self):
        gate=threading.Event();entered=threading.Event()
        def delayed(source,plan,window,pos):
            if source=='openalex' and window['since']=='2026-01-01' and pos=='next':
                entered.set();gate.wait(5)
            return self.fetch(source,plan,window,pos)
        def scope(items,client,min_confidence):
            return [dict(paper_id=i['paper_id'],decision='exclude',topics=[],reason='test',raw={}) for i in items]
        w=self.wf(fetch=delayed,scope_fn=scope)
        async def exercise():
            running=asyncio.create_task(w.run(poll_seconds=.01))
            for _ in range(400):
                if entered.is_set() and w.status()['papers'].get('exclude',0):break
                await asyncio.sleep(.01)
            self.assertTrue(entered.is_set())
            self.assertGreater(w.status()['papers'].get('exclude',0),0)
            self.assertEqual(w.status()['collection_state'],'open')
            gate.set()
            return await asyncio.wait_for(running,5)
        result=asyncio.run(exercise())
        self.assertEqual(result['collection_state'],'sealed')
        self.assertEqual(result['papers']['exclude'],101)
        snapshot=w.snapshot(Path(self.tmp.name)/'prepared.json')
        self.assertEqual(snapshot['counts']['excluded'],101)
        self.assertEqual(snapshot['counts']['ready'],0)
    def test_single_arxiv_lane_paces_and_drains_multiple_pages(self):
        arxiv_calls=[]
        def fetch(source,plan,window,pos):
            if source=='openalex':return page([],0)
            arxiv_calls.append(int(pos))
            return atom(range(100) if pos=='0' else [100],101)
        w=self.wf(fetch=fetch)
        sleeps=[]
        with patch('kg_builder.full_preparation.time.sleep',side_effect=lambda delay:sleeps.append(delay)):
            w._stream('arxiv-0')
        self.assertEqual(arxiv_calls,[0,100])
        self.assertEqual(len(sleeps),1)
        # Admission consumes part of the three-second interval.
        self.assertGreater(sleeps[0],0)
        self.assertEqual(w.status()['full_streams'][0]['received'],101)
    def test_checkpoint_and_poll_queries_use_indexes(self):
        w=self.wf()
        db=w._db()
        checkpoint=db.execute('EXPLAIN QUERY PLAN SELECT raw FROM full_pages WHERE stream_id=? AND window_index=? AND position=?',
                              ('openalex-0',0,'*')).fetchall()
        scope=db.execute("EXPLAIN QUERY PLAN SELECT 1 FROM papers WHERE scope_state IN ('queued','processing') LIMIT 1").fetchall()
        tasks=db.execute("EXPLAIN QUERY PLAN SELECT 1 FROM tasks WHERE state IN ('queued','leased') LIMIT 1").fetchall()
        self.assertIn('sqlite_autoindex_full_pages_1',str([tuple(r) for r in checkpoint]))
        self.assertIn('idx_papers_scope',str([tuple(r) for r in scope]))
        self.assertIn('idx_tasks_outstanding',str([tuple(r) for r in tasks]))
        db.close()
    def test_external_item_review_drain_to_prepared_snapshot(self):
        def one(source,plan,window,pos):
            if source=='openalex' and window['since']=='2026-01-01':return page([paper(42)],1)
            return atom([],0) if source=='arxiv' else page([],0)
        def scope(items,client,min_confidence):
            return [dict(paper_id=i['paper_id'],decision='include',topics=['agent_memory'],reason='test',raw={}) for i in items]
        def review(items,client,min_confidence):
            return [dict(paper_id=i['paper_id'],field=i['field'],key=i['key'],input_hash=i['input_hash'],
                         passed=True,probability=.95,decision_metric='probability',issues=[],raw={}) for i in items]
        w=self.wf(fetch=one,scope_fn=scope,review_fn=review,embedding=Embedding())
        async def exercise():
            running=asyncio.create_task(w.run(poll_seconds=.01))
            task=[]
            for _ in range(300):
                task=w.claim('extract',1,'external-agent')
                if task:break
                await asyncio.sleep(.01)
            self.assertEqual(len(task),1)
            t=task[0]
            w.submit([dict(task_id=t['task_id'],lease_token=t['lease_token'],input_hash=t['input_hash'],result=draft(t))])
            return await asyncio.wait_for(running,5)
        state=asyncio.run(exercise())
        self.assertEqual(state['review']['ready'],1)
        snap=w.snapshot(Path(self.tmp.name)/'prepared.json')
        self.assertEqual(snap['counts']['ready'],1)
        self.assertEqual(len(snap['extractions']),1)
        self.assertEqual(len(snap['vectors']),1)

if __name__=='__main__':unittest.main()
