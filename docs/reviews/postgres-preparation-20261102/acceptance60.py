"""Independent fresh PostgreSQL preparation acceptance. All paper/model data are synthetic."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid
from unittest.mock import patch

from psycopg import sql
from kg_builder.preparation import PreparationWorkflow, ITEM_FIELDS, snapshot_digest
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION


class FixtureEmbedding:
    model_id = 'fixture-only:deterministic-vector/1'
    def encode(self, texts):
        return [[1.0, float(len(text) % 17) / 17.0, 0.5] for text in texts]


def source(n):
    aid = f'2601.{n+80000:05d}'
    return dict(source_id=f'https://arxiv.org/abs/{aid}v1', arxiv_id=aid,
                doi=f'10.1234/pg-fixture-{n}', version_label='v1',
                published_at='2026-01-02', year=2026,
                title=f'Synthetic PostgreSQL fixture {n}',
                abstract=f'Fixture {n} proposes an agent memory procedure and evaluates recall.',
                url=f'https://arxiv.org/abs/{aid}v1')


def number(paper_id):
    return int(paper_id.split(':')[-1].split('.')[-1]) - 80000


def classify(items, client, min_confidence):
    return [dict(paper_id=i['paper_id'],decision='exclude' if number(i['paper_id']) < 5 else 'pending' if number(i['paper_id']) < 8 else 'include',
                 topics=['agent_memory'],reason='SYNTHETIC persistence fixture decision',probability=.9) for i in items]


def review(items, client, min_confidence):
    result=[]
    for item in items:
        n=number(item['paper_id'])
        # n8 loses all entries after final review; n9 retains only contribution;
        # other methods fail initial review and pass after an item-only repair.
        supported=(n!=8 and (item['field']=='contributions' or (n!=9 and item['item']['description'].endswith('repaired'))))
        result.append(dict(paper_id=item['paper_id'],field=item['field'],key=item['key'],
                           input_hash=item['input_hash'],passed=supported,probability=.9 if supported else .2,
                           confidence=.1 if supported else .99,decision_metric='probability',
                           issues=[] if supported else ['SYNTHETIC request: repair this item only']))
    return result


def draft(task):
    return dict(schema_version=RETRIEVAL_SCHEMA_VERSION,paper_id=task['paper_id'],version_id=task['version_id'],
                paper_version=task['paper_version'],extractor_type='external_unreviewed',extractor_version='synthetic-fixture/1',
                source_scope='supplied_paper_text',status='draft_requires_review',
                contributions=[dict(key=task['version_id']+'#c1',description='Fixture proposes a memory procedure')],
                methods=[dict(key='fixture-method',name='Fixture procedure',description='Fixture procedure initial',role='proposed')],
                research_problems=[],benchmarks=[])


def submission(task, result):
    return dict(task_id=task['task_id'],input_hash=task['input_hash'],lease_token=task['lease_token'],result=result)


def expect_rejected(fn):
    try:fn()
    except ValueError:return
    raise AssertionError('Expected invalid submission to reject')


async def exercise(wf, stop_after=None):
    stop=threading.Event()
    runner=asyncio.create_task(wf.run(collect=False,poll_seconds=.01,stop_event=stop))
    accepted=0
    duplicate_checked=False
    freeze_checked=False
    rounds=0
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            while not runner.done():
                rounds+=1
                if rounds>3000:raise AssertionError('Workflow did not drain')
                for kind in ('extract','repair_extraction'):
                    chunks=await asyncio.gather(*(asyncio.get_running_loop().run_in_executor(pool,wf.claim,kind,5,f'external-{i}') for i in range(4)))
                    tasks=[task for chunk in chunks for task in chunk]
                    assert len({t['task_id'] for t in tasks})==len(tasks), 'Duplicate concurrent claim'
                    entries=[]
                    for task in tasks:
                        if kind=='extract':payload=draft(task)
                        else:
                            payload=deepcopy(task['previous_extraction'])
                            for field in ITEM_FIELDS:
                                for item in payload[field]:
                                    if any(r['field']==field and r['key']==item['key'] for r in task['requested_items']):
                                        item['description']='Fixture procedure repaired'
                            if not freeze_checked and number(task['paper_id'])!=8:
                                bad=deepcopy(payload)
                                bad['contributions'][0]['description']='tampered approved content'
                                expect_rejected(lambda:wf.submit([submission(task,bad)]))
                                bad=deepcopy(payload);bad['methods'].append(dict(key='new-key',name='new',description='new',role='used'))
                                expect_rejected(lambda:wf.submit([submission(task,bad)]))
                                freeze_checked=True
                        entries.append(submission(task,payload))
                    if entries:
                        results=wf.submit(entries)
                        assert all(r['status']=='accepted' for r in results)
                        accepted+=len(entries)
                        if not duplicate_checked:
                            assert wf.submit([entries[0]])[0]['status']=='duplicate'
                            bad=deepcopy(entries[0]);bad['result']['extractor_version']='different'
                            expect_rejected(lambda:wf.submit([bad]))
                            duplicate_checked=True
                if stop_after and accepted>=stop_after:
                    stop.set()
                await asyncio.sleep(.01)
        state=await asyncio.wait_for(runner,20)
        return state,accepted,duplicate_checked,freeze_checked
    finally:
        stop.set()
        if not runner.done():
            await asyncio.wait_for(runner,20)


def main():
    run_id='acceptance60-'+uuid.uuid4().hex
    url=os.environ['KG_TEST_DATABASE_URL']
    out=Path(os.environ.get('KG_TEST_EVIDENCE_DIR','/tmp'))
    out.mkdir(parents=True,exist_ok=True)
    started=time.monotonic()
    stores=[]
    with tempfile.TemporaryDirectory() as work:
        def open_workflow(path):
            wf=PreparationWorkflow(path,run_id=run_id,database_url=url,target=60,llm_mode='external',
                until='2026-11-02',jev_concurrency=8,generation_concurrency=4,scope_workers=4,review_workers=4,
                batch_size=5,lease_seconds=1800,scope_fn=classify,review_fn=review,client=object(),embedding=FixtureEmbedding())
            stores.append(wf.store)
            return wf
        wf=open_workflow(Path(work)/'initial')
        try:
            # A SQLite connect anywhere in the actual preparation path is a hard failure.
            with patch('sqlite3.connect',side_effect=AssertionError('Preparation touched SQLite')):
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures=[pool.submit(wf.add_source,source(n),'arxiv','fixture','agent_memory') for n in range(60)]
                    pids=[f.result() for f in futures]
                    assert len(set(pids))==60
                    # Twenty cross-source reliable-ID duplicates; no old records imported.
                    duplicates=[]
                    for n in range(20):
                        r=source(n);r['source_id']=f'https://openalex.org/W-FIXTURE-{n}'
                        duplicates.append(pool.submit(wf.add_source,r,'openalex','fixture','agent_memory'))
                    assert set(f.result() for f in duplicates)<=set(pids)
                with wf.store.transaction() as db:
                    assert db.execute('SELECT count(*) AS n FROM papers').fetchone()['n']==60
                    assert db.execute(sql.SQL('SELECT count(*) AS n FROM {}').format(wf.store.source_table('source_records'))).fetchone()['n']==80
                    db.execute("UPDATE run SET collection_state='sealed' WHERE id=1")
                expect_rejected(lambda:wf.snapshot(Path(work)/'premature.json'))
                state,accepted,dup1,freeze1=asyncio.run(exercise(wf,stop_after=10))
                assert state['tasks'].get('extract/queued',0)+state['tasks'].get('extract/leased',0)>0, 'Stop must preserve outstanding work'
                # Expire a claimed task; stale worker must reject after takeover.
                held=wf.claim('extract',1,'held-for-restart')[0]
                assert 1790 < held['lease_until']-time.time() <=1800
                with wf.store.transaction() as db:
                    db.execute('UPDATE tasks SET lease_until=%s WHERE task_id=%s',(time.time()-1,held['task_id']))
                wf.close()
                wf=open_workflow(Path(work)/'different-directory')
                renewed=wf.claim('extract',100,'after-restart')
                replacement=next(t for t in renewed if t['task_id']==held['task_id'])
                assert replacement['lease_token']!=held['lease_token']
                expect_rejected(lambda:wf.submit([submission(held,draft(held))]))
                wf.submit([submission(t,draft(t)) for t in renewed])
                final,more,dup2,freeze2=asyncio.run(exercise(wf))
                assert final['review']['ready']==51, final
                assert final['review']['no_usable_items']==1, final
                assert dup1 or dup2
                assert freeze1 or freeze2
                assert final['config']['llm_mode']=='external'
                prepared=wf.snapshot(Path(work)/'prepared.json')
                assert prepared['counts']==dict(collected=60,ready=51,excluded=5,pending=3,no_usable_items=1), prepared['counts']
                assert prepared['snapshot_hash']==snapshot_digest(prepared)
                assert len(prepared['vectors'])==101
                assert all(v['model_id']==FixtureEmbedding.model_id for v in prepared['vectors'])
                assert not any(e['paper_id'].endswith('80008') for e in prepared['extractions'])
                for extraction in prepared['extractions']:
                    assert set(ITEM_FIELDS)<=extraction.keys()
                    assert 'findings' not in extraction and 'limitations' not in extraction
                    assert extraction['contributions'][0]['description']=='Fixture proposes a memory procedure'
                    if number(extraction['paper_id'])==9:assert extraction['methods']==[]
                with wf.store.transaction() as db:
                    completed=db.execute('SELECT completed FROM run WHERE id=1').fetchone()['completed']
                    audit_count=db.execute('SELECT count(*) AS n FROM decision_audits').fetchone()['n']
                asyncio.run(wf.run(collect=False,poll_seconds=.01))
                replay=wf.snapshot(Path(work)/'prepared.json')
                assert replay['snapshot_hash']==prepared['snapshot_hash']
                with wf.store.transaction() as db:
                    assert db.execute('SELECT completed FROM run WHERE id=1').fetchone()['completed']==completed
                    assert db.execute('SELECT count(*) AS n FROM decision_audits').fetchone()['n']==audit_count
                expect_rejected(lambda:wf.add_source(source(61),'arxiv','fixture','agent_memory'))
            report=dict(result='pass',synthetic_papers=True,semantic_decisions='controlled fixtures; no Jev calls',
                vectors='deterministic fixture vectors; not BGE embeddings',database_engine='PostgreSQL',run_id=run_id,
                source_records=80,distinct_papers=60,counts=prepared['counts'],snapshot_hash=prepared['snapshot_hash'],
                vectors_count=len(prepared['vectors']),decision_audits=audit_count,stop_resume=True,
                cross_directory_reopen=True,concurrent_workers=4,lease_seconds=1800,stale_rejected=True,
                item_freeze_and_new_key_rejected=True,sqlite_accesses=0,elapsed_seconds=time.monotonic()-started)
            (out/'acceptance60.json').write_text(json.dumps(report,indent=2))
            print(json.dumps(report,indent=2))
        finally:
            with wf.store.transaction() as db:
                db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(wf.store.workflow_schema)))
                db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(wf.store.source_schema)))
            for store in stores:store.close()

if __name__=='__main__':main()
