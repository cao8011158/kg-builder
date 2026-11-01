"""Durable, streaming source preparation. All semantic decisions use model adapters."""
from __future__ import annotations
import asyncio
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from .collectors import openalex_search, arxiv_search
from .postgres import PreparationStore
from psycopg import sql
from .postgres_source import PostgresSourceRepository
from .extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction

QUERIES = ('agent long term memory', 'agent context compression', 'agent skill learning evolution')
TOPICS = ('agent_memory', 'context_management', 'skill_evolution')
ITEM_FIELDS = ('contributions','research_problems','methods','benchmarks')
CANON = lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
SHA = lambda x: hashlib.sha256(CANON(x).encode()).hexdigest()

def snapshot_digest(snapshot):
    return SHA({k:v for k,v in snapshot.items() if k!='snapshot_hash'})

def _failure_audits(exc, client):
    audits=getattr(exc,'request_audits',None)
    if audits is not None:return audits
    audit=getattr(exc,'request_audit',None) or getattr(client,'last_request_audit',None)
    return [audit] if audit else []

class PreparationWorkflow:
    def __init__(self, run_dir, *, run_id, database_url=None, target=60, llm_mode='external', jev_model='jev-1.14.0',
                 batch_size=5, per_query=100, since='2025-01-01', until=None,
                 lease_seconds=1800, repair_limit=2, min_confidence=.7, decision_metric='probability', context_format='explicit-v1',
                 review_policy='item-v1', item_max_reviews=2, jev_concurrency=4,
                 generation_concurrency=3, scope_workers=2, review_workers=2,
                 client=None, extractor=None, scope_fn=None, review_fn=None,
                 collectors=None, embedding=None):
        if review_policy not in ('item-v1','legacy-paper') or item_max_reviews<1:
            raise ValueError('Invalid review policy or item review budget')
        if decision_metric not in ('probability','confidence'):
            raise ValueError('Invalid decision metric')
        if context_format not in ('registry-v1','explicit-v1'):
            raise ValueError('Invalid context format')
        if (not isinstance(min_confidence,(int,float)) or isinstance(min_confidence,bool)
                or not math.isfinite(min_confidence) or not 0<=min_confidence<=1):
            raise ValueError('Decision threshold must be finite and between 0 and 1')
        if min(jev_concurrency,generation_concurrency,scope_workers,review_workers)<1:
            raise ValueError('Concurrency budgets must be positive')
        self.run_dir=Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.store=PreparationStore(run_id,database_url=database_url)
        self.source=PostgresSourceRepository(self.store)
        self.config=dict(target=target,llm_mode=llm_mode,jev_model=jev_model,batch_size=batch_size,
                         per_query=per_query,since=since,until=until or datetime.now(timezone.utc).date().isoformat(),
                         lease_seconds=lease_seconds,repair_limit=repair_limit,min_confidence=min_confidence,
                         decision_metric=decision_metric,context_format=context_format,
                         review_policy=review_policy,item_max_reviews=item_max_reviews,
                         jev_concurrency=jev_concurrency,generation_concurrency=generation_concurrency,
                         scope_workers=scope_workers,review_workers=review_workers)
        self.client=client; self.extractor=extractor; self.scope_fn=scope_fn; self.review_fn=review_fn
        self.collectors=collectors or {'openalex':openalex_search,'arxiv':arxiv_search}
        self.embedding=embedding
        self._init()
        if self.client is None:
            from .decision_api import JevClient
            self.client=JevClient(model=self.config['jev_model'],max_concurrency=self.config.get('jev_concurrency',4))

    @property
    def item_policy(self):return self.config.get('review_policy','legacy-paper')=='item-v1'

    @property
    def decision_metric(self):return self.config.get('decision_metric','confidence')

    @property
    def context_format(self):return self.config.get('context_format','registry-v1')

    def _native_decision(self, fn, *args):
        return fn(*args,min_confidence=self.config['min_confidence'],decision_metric=self.decision_metric,
                  context_format=self.context_format)

    def _item_passes(self, outcome):
        if not outcome.get('passed'):return False
        value=outcome.get(self.decision_metric)
        # In particular, an absent P(supported) cannot inherit Jev's confidence.
        return (isinstance(value,(int,float)) and not isinstance(value,bool)
                and math.isfinite(value) and 0<=value<=1
                and value>=self.config['min_confidence'])

    def _db(self):
        db=self.store.pool.getconn()
        try:
            db.autocommit=True
            db.execute(sql.SQL('SET search_path TO {}, pg_catalog').format(sql.Identifier(self.store.workflow_schema)))
            return db
        except BaseException:
            self.store.pool.putconn(db)
            raise

    def _release(self, db):
        if db.info.transaction_status.name != 'IDLE':
            db.rollback()
        self.store.pool.putconn(db)

    def close(self):
        self.store.close()

    def _init(self):
        self.store.initialize()
        db=self._db()
        try:
            with db.transaction():
                db.execute('''INSERT INTO run(id,run_id,config_json,collection_state,started,completed,error)
                    VALUES(1,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING''',
                    (self.store.run_id,CANON(self.config),'open',time.time(),None,None))
                row=db.execute('SELECT config_json FROM run WHERE id=1').fetchone()
                old=json.loads(row['config_json'])
                if old.get('context_format','registry-v1') not in ('registry-v1','explicit-v1'):
                    raise ValueError('Invalid saved context format')
                if old!=self.config:self.config=old
        finally:self._release(db)

    def _metric(self, db, stage, *, active=0, api=0, service=0, queued=0):
        db.execute('''INSERT INTO metrics VALUES(%s,1,%s,%s,%s,%s) ON CONFLICT(stage) DO UPDATE SET
            calls=metrics.calls+1,active_seconds=metrics.active_seconds+excluded.active_seconds,
            api_seconds=metrics.api_seconds+excluded.api_seconds,service_seconds=metrics.service_seconds+excluded.service_seconds,
            queued_seconds=metrics.queued_seconds+excluded.queued_seconds''',(stage,active,api,service,queued))

    def _freeze_source(self, db, row, source, sid, pid, vid):
        ver=db.execute(sql.SQL('SELECT version_label,source_url FROM {} WHERE version_id=%s').format(
            self.store.source_table('paper_versions')),(vid,)).fetchone()
        title=row.get('title') or pid; abstract=row.get('abstract') or ''
        url=ver['source_url'] or row.get('url')
        pins=dict(paper_id=pid,version_id=vid,title=title,text=abstract,
                  version_label=ver['version_label'],source_url=url)
        fingerprint=SHA(pins)
        prior=db.execute('SELECT fingerprint,origin_source,origin_id FROM papers WHERE paper_id=%s FOR UPDATE',(pid,)).fetchone()
        if not prior:
            db.execute('INSERT INTO papers(paper_id,version_id,title,text,version_label,source_url,fingerprint,origin_source,origin_id,discovered) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (pid,vid,title,abstract,ver['version_label'],url,fingerprint,source,sid,time.time()))
        elif prior['fingerprint']!=fingerprint and (prior['origin_source'],prior['origin_id'])==(source,sid):
            db.execute("UPDATE papers SET scope_state='pending',review_state='pending',reason='source_changed',scope_token=NULL,review_token=NULL WHERE paper_id=%s",(pid,))
            db.execute("UPDATE item_reviews SET state='revoked',approved_json=NULL WHERE paper_id=%s",(pid,))
            db.execute("UPDATE tasks SET state='failed',reason='source_changed',lease_token=NULL WHERE paper_id=%s AND state IN ('queued','leased')",(pid,))
        self._assert_mutable(db)
        return pid

    @staticmethod
    def _assert_mutable(db):
        if db.execute('SELECT completed FROM run WHERE id=1 FOR SHARE').fetchone()['completed'] is not None:
            raise ValueError('Completed preparation run is immutable')

    def add_source(self, record, source, query, topic):
        row=dict(record);sid=row.pop('source_id');row['topics']=[topic]
        row['topic_basis']='discovery_query_candidate'
        db=self._db()
        try:
            with db.transaction():
                pid,vid=self.source.upsert_paper(db,row,source,sid,query)
                return self._freeze_source(db,row,source,sid,pid,vid)
        finally:self._release(db)

    def _admit(self, source, topic, query, row):
        pub=row.get('published_at') or ''
        if pub and not self.config['since']<=pub[:10]<=self.config['until']:return
        db=self._db();count=db.execute('SELECT count(*) AS n FROM papers').fetchone()['n'];self._release(db)
        if count<self.config['target']:self.add_source(row,source,query,topic)

    def _collect_query(self, source, topic, query):
        db=self._db()
        prior=db.execute('SELECT state FROM query_progress WHERE source=%s AND topic=%s AND query=%s',(source,topic,query)).fetchone()
        if prior and prior['state']=='done':self._release(db);return
        with db.transaction(): db.execute("""INSERT INTO query_progress(source,topic,query,state,error,received)
            VALUES(%s,%s,%s,'running',NULL,0) ON CONFLICT(source,topic,query)
            DO UPDATE SET state='running',error=NULL,received=0""",(source,topic,query))
        self._release(db)
        received=0;error=None;started=time.time()
        try:
            f=self.collectors[source]
            if source=='openalex':rows=f(query,self.config['per_query'],self.config['since'],self.config['until'])
            else:rows=f(query,self.config['per_query'],self.config['since'])
            iterator=iter(rows)
            while received<self.config['per_query']:
                request_start=time.time()
                try:row=next(iterator)
                except StopIteration:break
                request_elapsed=time.time()-request_start
                db=self._db()
                with db.transaction():self._metric(db,'collection_request',api=request_elapsed)
                self._release(db)
                normalize_start=time.time()
                db=self._db()
                with db.transaction():db.execute('''INSERT INTO candidates VALUES(%s,%s,%s,%s,%s)
                    ON CONFLICT(source,topic,query,position) DO UPDATE SET payload_json=excluded.payload_json''',
                                   (source,topic,query,received,CANON(row)))
                self._release(db)
                received+=1
                # Admit a balanced first share immediately, allowing scope/extraction to start.
                if received<=max(1,(self.config['target']+5)//6):self._admit(source,topic,query,row)
                db=self._db()
                with db.transaction():self._metric(db,'collection_normalize',active=time.time()-normalize_start)
                self._release(db)
        except Exception as exc:error=type(exc).__name__
        db=self._db()
        with db.transaction():
            db.execute('UPDATE query_progress SET state=%s,error=%s,received=%s WHERE source=%s AND topic=%s AND query=%s',
                       ('failed' if error else 'done',error,received,source,topic,query))
            self._metric(db,'collection',active=time.time()-started)
        self._release(db)

    def _topup(self):
        quota=max(1,(self.config['target']+5)//6)
        for position in range(quota,self.config['per_query']):
            for topic,query in zip(TOPICS,QUERIES):
                for source in ('openalex','arxiv'):
                    db=self._db()
                    count=db.execute('SELECT count(*) AS n FROM papers').fetchone()['n']
                    row=db.execute('SELECT payload_json FROM candidates WHERE source=%s AND topic=%s AND query=%s AND position=%s',
                                   (source,topic,query,position)).fetchone()
                    self._release(db)
                    if count>=self.config['target']:return
                    if row:self._admit(source,topic,query,json.loads(row['payload_json']))

    async def collect(self):
        for topic,query in zip(TOPICS,QUERIES):
            for source in ('openalex','arxiv'):
                await asyncio.to_thread(self._collect_query,source,topic,query)
        await asyncio.to_thread(self._topup)
        db=self._db()
        with db.transaction():
            count=db.execute('SELECT count(*) AS n FROM papers').fetchone()['n']
            db.execute("UPDATE run SET collection_state='sealed',error=%s WHERE id=1",
                       (f'collection_shortfall:{count}/{self.config["target"]}' if count<self.config['target'] else None,))
        self._release(db)

    def _scope_batch(self):
        from .decision_api import JevClient, classify_scope
        fn=self.scope_fn or classify_scope
        db=self._db(); now=time.time(); token=secrets.token_urlsafe(24)
        try:
            with db.transaction():
                rows=db.execute("""UPDATE papers p SET scope_state='processing',scope_started=%s,
                scope_token=%s,scope_lease_until=%s FROM
                (SELECT paper_id FROM papers WHERE scope_state='queued' ORDER BY discovered
                 LIMIT %s FOR UPDATE SKIP LOCKED) picked
                WHERE p.paper_id=picked.paper_id RETURNING p.*""",
                    (now,token,now+self.config['lease_seconds'],self.config['batch_size'])).fetchall()
        finally:self._release(db)
        if not rows:return 0
        items=[dict(paper_id=r['paper_id'],title=r['title'],text=r['text']) for r in rows if r['text'] and r['source_url']]
        outcomes={}
        if items:
            try:
                client=self.client or JevClient(model=self.config['jev_model'])
                results=(fn(items,client,min_confidence=self.config['min_confidence']) if self.scope_fn
                         else self._native_decision(fn,items,client))
                outcomes={o['paper_id']:o for o in results}
            except Exception as exc:
                # Transient API failure leaves the items restartable.
                db=self._db()
                try:
                    with db.transaction():
                        for r in rows:db.execute("UPDATE papers SET scope_state='queued',scope_token=NULL,scope_lease_until=NULL WHERE paper_id=%s AND scope_state='processing' AND scope_token=%s",(r['paper_id'],token))
                        db.execute('UPDATE run SET error=%s WHERE id=1',(type(exc).__name__,))
                        audits=_failure_audits(exc,locals().get('client'))
                        if audits:
                            for r in rows:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                                (r['paper_id'],'scope_error',0,CANON(dict(error=type(exc).__name__,requests=audits)),time.time()))
                finally:self._release(db)
                raise
        now2=time.time();db=self._db()
        try:
            with db.transaction():
                for r in rows:
                    current=db.execute('SELECT scope_state,scope_token,scope_lease_until FROM papers WHERE paper_id=%s FOR UPDATE',(r['paper_id'],)).fetchone()
                    if not current or current['scope_state']!='processing' or current['scope_token']!=token:continue
                    if current['scope_lease_until']<now2:
                        db.execute("UPDATE papers SET scope_state='queued',scope_token=NULL,scope_lease_until=NULL WHERE paper_id=%s",(r['paper_id'],))
                        continue
                    o=outcomes.get(r['paper_id'])
                    decision=o['decision'] if o else 'pending'
                    if decision not in ('include','exclude','pending'):decision='pending'
                    db.execute('''UPDATE papers SET scope_state=%s,scope_json=%s,scoped=%s,reason=%s,
                        scope_token=NULL,scope_lease_until=NULL
                        WHERE paper_id=%s AND scope_state='processing' AND scope_token=%s ''',
                        (decision,CANON(o) if o else None,now2,
                         'missing_source_content' if not o else o.get('reason'),r['paper_id'],token))
                    if o:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                        (r['paper_id'],'scope',0,CANON(o),now2))
                    if decision=='include':self._queue_task(db,r['paper_id'],'extract',r['fingerprint'],0)
                    self._metric(db,'scope',active=(now2-now)/len(rows),
                        queued=now-r['discovered'])
                self._metric(db,'scope_api',api=self._unique_api_seconds(outcomes))
                db.execute("UPDATE run SET error=NULL WHERE id=1 AND error NOT LIKE 'collection_shortfall:%'")
            return len(rows)
        finally:self._release(db)

    @staticmethod
    def _unique_api_seconds(outcomes):
        seen=set();total=0.0
        for outcome in outcomes.values():
            raw=outcome.get('raw') or {}
            for request in raw.get('requests',[]):
                key=request.get('request_id') or id(request)
                if key not in seen:
                    seen.add(key);total+=request.get('request_duration_s') or 0.0
        return total

    def _queue_task(self,db,pid,kind,fingerprint,attempt):
        db.execute('''INSERT INTO tasks(task_id,paper_id,kind,state,fingerprint,attempt,created)
          VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(paper_id,kind,attempt) DO NOTHING''',
          (uuid.uuid4().hex,pid,kind,'queued',fingerprint,attempt,time.time()))

    def claim(self, kind='extract', limit=5, worker='external'):
        if kind not in ('extract','repair_extraction') or limit<1 or limit>100:raise ValueError('Invalid claim kind/limit')
        db=self._db();now=time.time();out=[]
        try:
            with db.transaction():

                # Each branch uses its state-specific index. Completed tasks never enter either walk.
                queued=db.execute('''SELECT t.task_id,t.created FROM tasks t
                    JOIN papers p ON p.paper_id=t.paper_id AND p.fingerprint=t.fingerprint
                    WHERE t.kind=%s AND t.state='queued' ORDER BY t.created LIMIT %s
                    FOR UPDATE OF t SKIP LOCKED''',
                    (kind,limit)).fetchall()
                expired=db.execute('''SELECT t.task_id,t.created FROM tasks t
                    JOIN papers p ON p.paper_id=t.paper_id AND p.fingerprint=t.fingerprint
                    WHERE t.kind=%s AND t.state='leased' AND t.lease_until<%s
                    ORDER BY t.created LIMIT %s FOR UPDATE OF t SKIP LOCKED''',(kind,now,limit)).fetchall()
                selected=sorted([*queued,*expired],key=lambda row:row['created'])[:limit]
                for candidate in selected:
                    r=db.execute('''SELECT t.*,p.title,p.text,p.version_id,p.version_label,p.source_url,
                        p.review_json,p.extraction_json FROM tasks t JOIN papers p
                        ON p.paper_id=t.paper_id AND p.fingerprint=t.fingerprint
                        WHERE t.task_id=%s''',(candidate['task_id'],)).fetchone()
                    if r is None:continue
                    token=secrets.token_urlsafe(32)
                    cur=db.execute('''UPDATE tasks SET state='leased',lease_token=%s,lease_until=%s,worker=%s,claimed=%s
                      WHERE task_id=%s AND (state='queued' OR (state='leased' AND lease_until<%s))''',
                      (token,now+self.config['lease_seconds'],worker,now,r['task_id'],now))
                    if cur.rowcount:
                        out.append(dict(task_id=r['task_id'],kind=kind,paper_id=r['paper_id'],
                           version_id=r['version_id'],title=r['title'],text=r['text'],
                           paper_version=dict(version_label=r['version_label'],source_url=r['source_url']),
                           input_hash=r['fingerprint'],lease_token=token,lease_until=now+self.config['lease_seconds'],
                           attempt=r['attempt']))
                        if kind=='repair_extraction':
                            out[-1]['previous_extraction']=json.loads(r['extraction_json']) if r['extraction_json'] else None
                            out[-1]['review_issues']=json.loads(r['review_json']).get('issues',[]) if r['review_json'] else []
                            if self.item_policy:
                                failed=db.execute("SELECT field,item_key,payload_json,issues_json FROM item_reviews WHERE paper_id=%s AND state='repair_queued' ORDER BY field,item_key",(r['paper_id'],)).fetchall()
                                out[-1]['requested_items']=[dict(field=x['field'],key=x['item_key'],previous_item=json.loads(x['payload_json']),issues=json.loads(x['issues_json'])) for x in failed]
                                out[-1]['review_issues']=[dict(field=x['field'],key=x['item_key'],issues=json.loads(x['issues_json'])) for x in failed]
                        self._metric(db,'extract_queue',queued=now-r['created'])
        finally:self._release(db)
        return out

    def submit(self, entries):
        if not isinstance(entries,list):raise ValueError('Submission must be a list')
        outputs=[]
        for entry in entries:outputs.append(self._submit_one(entry))
        return outputs

    def _submit_one(self,e):
        db=self._db();now=time.time()
        try:
            with db.transaction():
                owner=db.execute('SELECT paper_id FROM tasks WHERE task_id=%s',(e['task_id'],)).fetchone()
                if not owner:raise ValueError('Unknown task')
                db.execute('SELECT paper_id FROM papers WHERE paper_id=%s FOR UPDATE',(owner['paper_id'],))
                r=db.execute('''SELECT t.*,p.version_id,p.version_label,p.source_url,p.fingerprint AS current_hash,
                    p.scope_state,p.title,p.text FROM tasks t JOIN papers p ON p.paper_id=t.paper_id
                    WHERE t.task_id=%s FOR UPDATE OF t''',(e['task_id'],)).fetchone()
                if not r:raise ValueError('Unknown task')
                h=SHA(e['result'])
                if r['state']=='done':
                    if r['result_hash']==h and e.get('input_hash')==r['fingerprint'] and e.get('lease_token')==r['lease_token']:
                        return dict(task_id=e['task_id'],status='duplicate')
                    raise ValueError('Conflicting submission')
                if r['state']!='leased' or r['lease_token']!=e.get('lease_token') or r['lease_until']<now:
                    raise ValueError('Stale or invalid lease')
                if e.get('input_hash')!=r['fingerprint'] or r['current_hash']!=r['fingerprint'] or r['scope_state']!='include':
                    raise ValueError('Changed source or input hash')
                draft=e['result']
                if not isinstance(draft,dict):raise ValueError('Extraction must be an object')
                pins=dict(version_label=r['version_label'],source_url=r['source_url'])
                if draft.get('paper_id')!=r['paper_id'] or draft.get('version_id')!=r['version_id'] or draft.get('paper_version')!=pins:
                    raise ValueError('Extraction metadata pins mismatch')
                if draft.get('schema_version')!=RETRIEVAL_SCHEMA_VERSION:raise ValueError('Expected v0.3 extraction')
                if draft.get('status')!='draft_requires_review':raise ValueError('Expected unreviewed draft status')
                validate_extraction(draft)
                if self.item_policy and r['kind']=='repair_extraction':
                    prior=db.execute("SELECT field,item_key,state,approved_json FROM item_reviews WHERE paper_id=%s AND state IN ('approved','repair_queued')",(r['paper_id'],)).fetchall()
                    expected={(x['field'],x['item_key']):x for x in prior}
                    supplied={(field,item['key']):item for field in ITEM_FIELDS for item in draft[field]}
                    if not any(x['state']=='repair_queued' for x in prior):raise ValueError('No requested repair items')
                    if set(supplied)-set(expected):raise ValueError('Repair introduced an item key')
                    if any((x['field'],x['item_key']) not in supplied for x in prior if x['state']=='repair_queued'):
                        raise ValueError('Repair omitted a requested item')
                    if any(CANON(supplied[(x['field'],x['item_key'])])!=x['approved_json'] for x in prior
                           if x['state']=='approved' and (x['field'],x['item_key']) in supplied):
                        raise ValueError('Repair changed an approved item')
                db.execute("UPDATE tasks SET state='done',result_hash=%s,result_json=%s,finished=%s WHERE task_id=%s",
                           (h,CANON(draft),now,e['task_id']))
                db.execute("UPDATE papers SET review_state='queued',extraction_json=%s WHERE paper_id=%s",
                           (CANON(draft),r['paper_id']))
                if self.item_policy:
                    if r['kind']=='extract':
                        for field in ITEM_FIELDS:
                            for item in draft[field]:
                                db.execute('''INSERT INTO item_reviews(paper_id,field,item_key,fingerprint,input_hash,state,payload_json)
                                    VALUES(%s,%s,%s,%s,%s,'queued',%s)''',(r['paper_id'],field,item['key'],r['fingerprint'],SHA(item),CANON(item)))
                    else:
                        for field in ITEM_FIELDS:
                            for item in draft[field]:
                                db.execute('''UPDATE item_reviews SET input_hash=%s,state='queued',payload_json=%s
                                  WHERE paper_id=%s AND field=%s AND item_key=%s AND state='repair_queued' ''',
                                  (SHA(item),CANON(item),r['paper_id'],field,item['key']))
                self._metric(db,'external_extraction',service=now-r['claimed'])
                return dict(task_id=e['task_id'],status='accepted')
        finally:self._release(db)

    def _review_batch(self):
        if self.item_policy:return self._review_item_batch()
        from .decision_api import JevClient, review_extractions
        fn=self.review_fn or review_extractions
        db=self._db(); claim_token=secrets.token_urlsafe(24); claimed_at=time.time()
        try:
            with db.transaction():
                rows=db.execute("""UPDATE papers p SET review_state='processing',review_started=%s,
                    review_token=%s,review_lease_until=%s FROM
                    (SELECT paper_id FROM papers WHERE review_state='queued' AND scope_state='include'
                     ORDER BY discovered LIMIT %s FOR UPDATE SKIP LOCKED) picked
                    WHERE p.paper_id=picked.paper_id RETURNING p.*""",
                    (claimed_at,claim_token,claimed_at+self.config['lease_seconds'],self.config['batch_size'])).fetchall()
        finally:self._release(db)
        if not rows:return 0
        start=time.time()
        try:
            client=self.client or JevClient(model=self.config['jev_model'])
            inputs=[dict(paper_id=r['paper_id'],text=r['text'],extraction=json.loads(r['extraction_json'])) for r in rows]
            results=(fn(inputs,client,min_confidence=self.config['min_confidence']) if self.review_fn
                     else self._native_decision(fn,inputs,client))
            outcomes={o['paper_id']:o for o in results}
        except Exception as exc:
            db=self._db()
            with db.transaction():
                for r in rows:db.execute("UPDATE papers SET review_state='queued',review_token=NULL,review_lease_until=NULL WHERE paper_id=%s AND review_state='processing' AND review_token=%s",(r['paper_id'],claim_token))
                db.execute('UPDATE run SET error=%s WHERE id=1',(type(exc).__name__,))
                audits=_failure_audits(exc,locals().get('client'))
                if audits:
                    for r in rows:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                        (r['paper_id'],'field_review_error',r['attempts']+1,CANON(dict(error=type(exc).__name__,requests=audits)),time.time()))
            self._release(db);raise
        db=self._db(); now=time.time()
        try:
            with db.transaction():
                for r in rows:
                    db.execute(sql.SQL('SELECT paper_id FROM {} WHERE paper_id=%s FOR SHARE').format(
                        self.store.source_table('papers')),(r['paper_id'],)).fetchone()
                    db.execute(sql.SQL('SELECT version_id FROM {} WHERE version_id=%s FOR SHARE').format(
                        self.store.source_table('paper_versions')),(r['version_id'],)).fetchone()
                    current=db.execute('SELECT review_state,review_token,review_lease_until FROM papers WHERE paper_id=%s FOR UPDATE',(r['paper_id'],)).fetchone()
                    if not current or current['review_state']!='processing' or current['review_token']!=claim_token:continue
                    if current['review_lease_until']<now:
                        db.execute("UPDATE papers SET review_state='queued',review_token=NULL,review_lease_until=NULL WHERE paper_id=%s",(r['paper_id'],))
                        continue
                    o=outcomes.get(r['paper_id']) or dict(passed=False,issues=['missing review response'])
                    payload=json.loads(r['extraction_json'])
                    nonempty=any(payload[k] for k in ('contributions','research_problems','methods','benchmarks'))
                    passed=bool(o.get('passed')) and nonempty
                    attempts=r['attempts']+1
                    db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                        (r['paper_id'],'field_review',attempts,CANON(o),now))
                    if passed:
                        # The source store validates the exact owner pins as well as schema.
                        payload['status']='reviewed'
                        ver=db.execute(sql.SQL('SELECT source_url,abstract,version_label FROM {} WHERE version_id=%s FOR SHARE').format(
                            self.store.source_table('paper_versions')),(r['version_id'],)).fetchone()
                        if not ver or (ver['source_url'],ver['abstract'],ver['version_label'])!=(r['source_url'],r['text'],r['version_label']):
                            passed=False;o=dict(passed=False,issues=['source_changed'],raw=o)
                        else:self.source.ingest_extraction(db,payload)
                    state='ready' if passed else ('pending' if attempts>self.config['repair_limit'] else 'repair_queued')
                    db.execute('''UPDATE papers SET review_state=%s,review_json=%s,extraction_json=%s,attempts=%s,reviewed=%s,reason=%s,
                        review_token=NULL,review_lease_until=NULL
                        WHERE paper_id=%s AND review_state='processing' AND review_token=%s ''',
                        (state,CANON(o),CANON(payload),attempts,now,None if passed else '; '.join(map(str,o.get('issues',[])))[:1000],r['paper_id'],claim_token))
                    if state=='repair_queued':self._queue_task(db,r['paper_id'],'repair_extraction',r['fingerprint'],attempts)
                    self._metric(db,'field_review',active=(now-start)/len(rows))
                self._metric(db,'field_review_api',api=self._unique_api_seconds(outcomes))
                db.execute("UPDATE run SET error=NULL WHERE id=1 AND error NOT LIKE 'collection_shortfall:%'")
            return len(rows)
        finally:self._release(db)

    def _review_item_batch(self):
        from .decision_api import review_items
        db=self._db();start=time.time(); claim_token=secrets.token_urlsafe(24)
        try:
            with db.transaction():
                rows=db.execute("""UPDATE papers p SET review_state='processing',review_started=%s,
                    review_token=%s,review_lease_until=%s FROM
                    (SELECT paper_id FROM papers WHERE review_state='queued' AND scope_state='include'
                     ORDER BY discovered LIMIT %s FOR UPDATE SKIP LOCKED) picked
                    WHERE p.paper_id=picked.paper_id RETURNING p.*""",
                    (start,claim_token,start+self.config['lease_seconds'],self.config['batch_size'])).fetchall()
                work={r['paper_id']:db.execute("SELECT * FROM item_reviews WHERE paper_id=%s AND state='queued' ORDER BY field,item_key",(r['paper_id'],)).fetchall() for r in rows}
        finally:self._release(db)
        if not rows:return 0
        inputs=[dict(paper_id=r['paper_id'],field=x['field'],key=x['item_key'],text=r['text'],
                     item=json.loads(x['payload_json']),input_hash=x['input_hash']) for r in rows for x in work[r['paper_id']]]
        expected_keys={(i['paper_id'],i['field'],i['key']) for i in inputs}
        try:
            outcomes=(self.review_fn(inputs,self.client,min_confidence=self.config['min_confidence']) if self.review_fn
                      else self._native_decision(review_items,inputs,self.client)) if inputs else []
            keyed={}
            for o in outcomes:
                k=(o['paper_id'],o['field'],o['key'])
                if k in keyed or k not in expected_keys:
                    raise ValueError('Duplicate or unknown review item key')
                if 'decision_metric' in o and o['decision_metric']!=self.decision_metric:
                    raise ValueError('Review decision metric does not match run configuration')
                if self.decision_metric=='probability':
                    probability=o.get('probability')
                    if (not isinstance(probability,(int,float)) or isinstance(probability,bool)
                            or not math.isfinite(probability) or not 0<=probability<=1):
                        raise ValueError('Missing or malformed review probability')
                keyed[k]=o
            if len(keyed)!=len(inputs) or any(keyed[(i['paper_id'],i['field'],i['key'])].get('input_hash')!=i['input_hash'] for i in inputs):
                raise ValueError('Missing item review or wrong review input hash')
        except Exception as exc:
            db=self._db()
            with db.transaction():
                for r in rows:db.execute("UPDATE papers SET review_state='queued',review_token=NULL,review_lease_until=NULL WHERE paper_id=%s AND review_state='processing' AND review_token=%s",(r['paper_id'],claim_token))
                db.execute('UPDATE run SET error=%s WHERE id=1',(type(exc).__name__,))
                audits=_failure_audits(exc,self.client)
                if audits:
                    for r in rows:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                        (r['paper_id'],'item_review_error',0,CANON(dict(error=type(exc).__name__,requests=audits)),time.time()))
            self._release(db);raise
        diagnostics={}
        if self.review_fn is None:
            from .decision_api import review_completeness
            fresh=[dict(paper_id=r['paper_id'],text=r['text'],extraction=json.loads(r['extraction_json'])) for r in rows if not r['attempts']]
            if fresh:
                try:diagnostics={o['paper_id']:o for o in self._native_decision(review_completeness,fresh,self.client)}
                except Exception as exc:
                    diagnostics={x['paper_id']:dict(completeness={field:dict(passed=None,error=type(exc).__name__) for field in ITEM_FIELDS},
                        error=type(exc).__name__,requests=_failure_audits(exc,self.client)) for x in fresh}
        db=self._db();now=time.time()
        try:
            with db.transaction():
                for r in rows:
                    db.execute(sql.SQL('SELECT paper_id FROM {} WHERE paper_id=%s FOR SHARE').format(
                        self.store.source_table('papers')),(r['paper_id'],)).fetchone()
                    db.execute(sql.SQL('SELECT version_id FROM {} WHERE version_id=%s FOR SHARE').format(
                        self.store.source_table('paper_versions')),(r['version_id'],)).fetchone()
                    current=db.execute('SELECT review_state,review_token,review_lease_until FROM papers WHERE paper_id=%s FOR UPDATE',(r['paper_id'],)).fetchone()
                    if not current or current['review_state']!='processing' or current['review_token']!=claim_token:continue
                    if current['review_lease_until']<now:
                        db.execute("UPDATE papers SET review_state='queued',review_token=NULL,review_lease_until=NULL WHERE paper_id=%s",(r['paper_id'],))
                        continue
                    for x in work[r['paper_id']]:
                        o=keyed[(r['paper_id'],x['field'],x['item_key'])]
                        attempts=x['attempts']+1
                        state='approved' if self._item_passes(o) else ('repair_queued' if attempts<self.config['item_max_reviews'] else 'dropped')
                        db.execute('''UPDATE item_reviews SET state=%s,attempts=%s,issues_json=%s,approved_json=%s
                          WHERE paper_id=%s AND field=%s AND item_key=%s AND input_hash=%s AND state='queued' ''',
                          (state,attempts,CANON(o.get('issues',[])),x['payload_json'] if state=='approved' else None,
                           r['paper_id'],x['field'],x['item_key'],x['input_hash']))
                        db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                            (r['paper_id'],'item_review',attempts,CANON(o),now))
                    items=db.execute('SELECT * FROM item_reviews WHERE paper_id=%s ORDER BY field,item_key',(r['paper_id'],)).fetchall()
                    repair=any(x['state']=='repair_queued' for x in items)
                    approved=any(x['state']=='approved' for x in items)
                    state='repair_queued' if repair else ('ready' if approved else 'no_usable_items')
                    raw=json.loads(r['extraction_json'])
                    previous=json.loads(r['review_json']) if r['review_json'] else {}
                    completeness=diagnostics.get(r['paper_id'],{}).get('completeness') or previous.get('completeness') or {field:dict(passed=None,diagnostic_only=True) for field in ITEM_FIELDS}
                    review=dict(policy='item-v1',items=[dict(field=x['field'],key=x['item_key'],state=x['state'],attempts=x['attempts'],input_hash=x['input_hash'],approved_payload_hash=SHA(json.loads(x['approved_json'])) if x['approved_json'] else None,issues=json.loads(x['issues_json'])) for x in items],completeness=completeness)
                    if 'decision_metric' in self.config:
                        review['decision_metric']=self.decision_metric
                        review['decision_threshold']=self.config['min_confidence']
                        previous_items={(x['field'],x['key']):x for x in previous.get('items',[])}
                        for item in review['items']:
                            outcome=keyed.get((r['paper_id'],item['field'],item['key']))
                            if outcome:
                                item['probability']=outcome.get('probability')
                                item['confidence']=outcome.get('confidence')
                            elif (item['field'],item['key']) in previous_items:
                                prior=previous_items[(item['field'],item['key'])]
                                item['probability']=prior.get('probability')
                                item['confidence']=prior.get('confidence')
                    if r['paper_id'] in diagnostics:
                        db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(%s,%s,%s,%s,%s)',
                            (r['paper_id'],'completeness',1,CANON(diagnostics[r['paper_id']]),now))
                    source_task=db.execute("SELECT task_id,result_hash FROM tasks WHERE paper_id=%s AND kind='extract' AND state='done' ORDER BY attempt LIMIT 1",(r['paper_id'],)).fetchone()
                    review['derivation']=dict(source_task_id=source_task['task_id'],source_draft_hash=source_task['result_hash']) if source_task else None
                    if state=='ready':
                        payload={**raw,'status':'reviewed','extractor_type':'item_reviewed_derivative',
                                 'extractor_version':raw['extractor_version']+':item-v1:'+SHA(raw)[:12]}
                        for field in ITEM_FIELDS:payload[field]=[json.loads(x['approved_json']) for x in items if x['field']==field and x['state']=='approved']
                        ver=db.execute(sql.SQL('SELECT source_url,abstract,version_label FROM {} WHERE version_id=%s FOR SHARE').format(
                            self.store.source_table('paper_versions')),(r['version_id'],)).fetchone()
                        if not ver or (ver['source_url'],ver['abstract'],ver['version_label'])!=(r['source_url'],r['text'],r['version_label']):
                            state='pending';review['source_changed']=True
                        else:self.source.ingest_extraction(db,payload)
                        if state=='ready':raw=payload
                        else:db.execute("UPDATE item_reviews SET state='revoked',approved_json=NULL WHERE paper_id=%s",(r['paper_id'],))
                    db.execute('''UPDATE papers SET review_state=%s,review_json=%s,extraction_json=%s,attempts=attempts+1,reviewed=%s,reason=%s,
                        review_token=NULL,review_lease_until=NULL
                        WHERE paper_id=%s AND review_state='processing' AND review_token=%s ''',
                        (state,CANON(review),CANON(raw),now,'no_usable_items' if state=='no_usable_items' else None,r['paper_id'],claim_token))
                    if state=='repair_queued':self._queue_task(db,r['paper_id'],'repair_extraction',r['fingerprint'],r['attempts']+1)
                    self._metric(db,'item_review',active=(now-start)/len(rows))
                db.execute("UPDATE run SET error=NULL WHERE id=1 AND error NOT LIKE 'collection_shortfall:%'")
            return len(rows)
        finally:self._release(db)

    def status(self):
        db=self._db()
        try:
            run=dict(db.execute('SELECT * FROM run WHERE id=1').fetchone())
            return dict(run_id=run['run_id'],collection_state=run['collection_state'],config=json.loads(run['config_json']),
               decision_metric=self.decision_metric,context_format=self.context_format,
               jev_concurrency=json.loads(run['jev_stats_json']) if run['jev_stats_json'] else getattr(self.client,'concurrency_stats',None),
               papers={r['scope_state']:r['n'] for r in db.execute('SELECT scope_state,count(*) AS n FROM papers GROUP BY scope_state')},
               review={r['review_state']:r['n'] for r in db.execute('SELECT review_state,count(*) AS n FROM papers GROUP BY review_state')},
               tasks={r['kind']+'/'+r['state']:r['n'] for r in db.execute('SELECT kind,state,count(*) AS n FROM tasks GROUP BY kind,state')},
               queries={r['state']:r['n'] for r in db.execute('SELECT state,count(*) AS n FROM query_progress GROUP BY state')},
               metrics={r['stage']:dict(calls=r['calls'],active_seconds=r['active_seconds'],api_seconds=r['api_seconds'],service_seconds=r['service_seconds'],queued_seconds=r['queued_seconds']) for r in db.execute('SELECT * FROM metrics')},
               wall_seconds=(run['completed'] or time.time())-run['started'],error=run['error'])
        finally:self._release(db)

    async def run(self, *, poll_seconds=1, collect=True, stop_event=None):
        db=self._db()
        cutoff=time.time()
        with db.transaction():db.execute("UPDATE papers SET scope_state='queued',scope_token=NULL,scope_lease_until=NULL WHERE scope_state='processing' AND scope_lease_until<%s",(cutoff,))
        with db.transaction():db.execute("UPDATE papers SET review_state='queued',review_token=NULL,review_lease_until=NULL WHERE review_state='processing' AND review_lease_until<%s",(cutoff,))
        sealed=db.execute('SELECT collection_state FROM run WHERE id=1').fetchone()['collection_state']=='sealed'
        self._release(db)
        producer=asyncio.create_task(self.collect()) if collect and not sealed else None
        state='sealed' if sealed else 'open';outstanding=1
        pool=ThreadPoolExecutor(max_workers=self.config.get('scope_workers',2)+self.config.get('review_workers',2)+self.config.get('generation_concurrency',3))
        workers=[]
        def launch(fn,count):
            while sum(1 for f,kind in workers if kind==fn and not f.done())<count:
                workers.append((asyncio.get_running_loop().run_in_executor(pool,fn),fn))
        try:
            while True:
                if stop_event and stop_event.is_set():break
                if producer and producer.done():
                    # A failed producer cannot leave an open run spinning forever or
                    # accidentally publish a terminal snapshot.
                    producer.result()
                completed=[f for f,_ in workers if f.done()]
                for f in completed:f.result()
                workers=[pair for pair in workers if not pair[0].done()]
                db=self._db()
                cutoff=time.time()
                with db.transaction():
                    db.execute("UPDATE papers SET scope_state='queued',scope_token=NULL,scope_lease_until=NULL WHERE scope_state='processing' AND scope_lease_until<%s",(cutoff,))
                    db.execute("UPDATE papers SET review_state='queued',review_token=NULL,review_lease_until=NULL WHERE review_state='processing' AND review_lease_until<%s",(cutoff,))
                state=db.execute('SELECT collection_state FROM run WHERE id=1').fetchone()['collection_state']
                outstanding=(bool(db.execute("SELECT 1 FROM papers WHERE scope_state IN ('queued','processing') LIMIT 1").fetchone())
                    or bool(db.execute("SELECT 1 FROM papers WHERE review_state IN ('queued','processing','repair_queued') LIMIT 1").fetchone())
                    or bool(db.execute("SELECT 1 FROM tasks WHERE state IN ('queued','leased') LIMIT 1").fetchone()))
                self._release(db)
                if state=='sealed' and outstanding==0:
                    if not workers:break
                    await asyncio.sleep(poll_seconds)
                    continue
                launch(self._scope_batch,self.config.get('scope_workers',2))
                launch(self._review_batch,self.config.get('review_workers',2))
                if self.config['llm_mode']=='api':
                    launch(self._api_extract,self.config.get('generation_concurrency',3))
                await asyncio.sleep(poll_seconds)
            db=self._db()
            with db.transaction():
                if state=='sealed' and outstanding==0:
                    run=db.execute('SELECT collection_state,completed FROM run WHERE id=1 FOR UPDATE').fetchone()
                    if run['collection_state']=='sealed' and not (db.execute("SELECT 1 FROM papers WHERE scope_state IN ('queued','processing') OR review_state IN ('queued','processing','repair_queued') LIMIT 1").fetchone()
                        or db.execute("SELECT 1 FROM tasks WHERE state IN ('queued','leased') LIMIT 1").fetchone()):
                        db.execute('UPDATE run SET completed=COALESCE(completed,%s) WHERE id=1',(time.time(),))
                        stats=getattr(self.client,'concurrency_stats',None)
                        if stats and stats.get('started',0):
                            db.execute('UPDATE run SET jev_stats_json=COALESCE(jev_stats_json,%s) WHERE id=1',(CANON(stats),))
            self._release(db)
        finally:
            for f,_ in workers:
                if not f.done():f.cancel()
            pool.shutdown(wait=True,cancel_futures=True)
            if producer:
                if not producer.done():producer.cancel()
                try:await producer
                except asyncio.CancelledError:pass
        return self.status()

    def _api_extract(self):
        if self.extractor is None:raise RuntimeError('API mode requires explicit extractor')
        n=0
        for kind in ('extract','repair_extraction'):
            for task in self.claim(kind,self.config['batch_size'],'api'):
                if kind=='repair_extraction':
                    from .extraction_api import EXTRACTION_INSTRUCTIONS
                    instruction=(EXTRACTION_INSTRUCTIONS+' Repair only requested failed item keys using review issues; return a complete v0.3 object with original metadata pins. Do not add keys or modify frozen approved items.'
                                 if self.item_policy else EXTRACTION_INSTRUCTIONS+' Repair the previous extraction using review issues; return complete v0.3 object with original metadata pins.')
                    result=self.extractor.generate_json(instruction,
                        {'paper_id':task['paper_id'],'version_id':task['version_id'],
                         'paper_version':task['paper_version'],'paper_text':task['text'],
                         'previous_extraction':task['previous_extraction'],'review_issues':task['review_issues'],
                         'requested_items':task.get('requested_items')},
                        required_keys=('schema_version','paper_id','version_id','paper_version'))
                    result['status']='draft_requires_review'
                    result['extractor_type']='llm_api_unreviewed'
                    result['extractor_version']=self.extractor.model
                    result['source_scope']='supplied_paper_text'
                else:
                    result=self.extractor.extract_draft(task['paper_id'],task['version_id'],task['text'],paper_version=task['paper_version'])
                self.submit([dict(task_id=task['task_id'],lease_token=task['lease_token'],input_hash=task['input_hash'],result=result)])
                n+=1
        return n

    def snapshot(self, output):
        db=self._db()
        try:
            run=db.execute('SELECT * FROM run WHERE id=1').fetchone()
            if run['collection_state']!='sealed' or run['completed'] is None or run['error']:
                raise ValueError('Collection and preparation must be terminal')
            if db.execute("SELECT 1 FROM tasks WHERE state IN ('queued','leased') LIMIT 1").fetchone():
                raise ValueError('Active tasks remain')
            rows=db.execute('SELECT * FROM papers ORDER BY paper_id').fetchall()
            ready=[r for r in rows if r['review_state']=='ready']
            con=db
            papers=[]
            for r in ready:
                metadata=con.execute(sql.SQL('SELECT first_published,year,doi,arxiv_id FROM {} WHERE paper_id=%s').format(
                    self.store.source_table('papers')),(r['paper_id'],)).fetchone()
                papers.append(dict(paper_id=r['paper_id'],version_id=r['version_id'],title=r['title'],text=r['text'],
                    paper_version=dict(version_label=r['version_label'],source_url=r['source_url']),
                    source_hash=r['fingerprint'],first_published=metadata['first_published'],year=metadata['year'],doi=metadata['doi'],
                    arxiv_id=metadata['arxiv_id'],scope=json.loads(r['scope_json'])))
            extractions=[json.loads(r['extraction_json']) for r in ready]
            vectors=[]
            entries=[]
            for extraction in extractions:
                for field,kind in (('research_problems','ResearchProblem'),('methods','Method'),('benchmarks','Benchmark'),('contributions','Contribution')):
                    for item in extraction[field]:
                        entries.append((extraction['paper_id']+'::'+item['key'],kind,((item.get('name','')+'\n') if item.get('name') else '')+item['description']))
            embed_start=time.time()
            if entries:
                if self.embedding is None:
                    from .embeddings import EmbeddingModel
                    self.embedding=EmbeddingModel(cache_dir=os.getenv('FASTEMBED_CACHE_PATH'))
                for start in range(0,len(entries),32):
                    part=entries[start:start+32]
                    encoded=self.embedding.encode([e[2] for e in part])
                    vectors.extend(dict(item_key=e[0],kind=e[1],text=e[2],model_id=self.embedding.model_id,
                                        vector=[float(v) for v in vec]) for e,vec in zip(part,encoded))
            embed_elapsed=time.time()-embed_start
            with db.transaction():self._metric(db,'snapshot_embedding',active=embed_elapsed)
            result=dict(schema_version='research-kg-prepared-snapshot/1',run_id=run['run_id'],
                counts=dict(collected=len(rows),ready=len(ready),excluded=sum(r['scope_state']=='exclude' for r in rows),
                            pending=sum(r['scope_state']=='pending' or r['review_state']=='pending' for r in rows),
                            no_usable_items=sum(r['review_state']=='no_usable_items' for r in rows)),
                config=json.loads(run['config_json']),papers=papers,extractions=extractions,vectors=vectors,
                excluded=[dict(paper_id=r['paper_id'],reason=r['reason'],scope=json.loads(r['scope_json']) if r['scope_json'] else None) for r in rows if r['scope_state']=='exclude'],
                pending=[dict(paper_id=r['paper_id'],reason=r['reason'],scope_state=r['scope_state'],review_state=r['review_state']) for r in rows if r['scope_state']=='pending' or r['review_state']=='pending'])
            if self.item_policy:
                result['review_metadata']={r['paper_id']:json.loads(r['review_json']) for r in rows if r['review_json'] and r['review_state'] in ('ready','no_usable_items')}
                result['no_usable_items']=[dict(paper_id=r['paper_id'],review=json.loads(r['review_json'])) for r in rows if r['review_state']=='no_usable_items']
            result['snapshot_hash']=snapshot_digest(result)
        finally:self._release(db)
        path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
        data=CANON(result)+'\n'
        if path.exists():
            if path.read_text()==data:return result
            raise ValueError('Snapshot output already exists with different content')
        tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
        tmp.write_text(data,encoding='utf-8');os.replace(tmp,path)
        return result
