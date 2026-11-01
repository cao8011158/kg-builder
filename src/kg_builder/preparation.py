"""Durable, streaming source preparation. All semantic decisions use model adapters."""
from __future__ import annotations
import asyncio
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time
import uuid

from .collectors import openalex_search, arxiv_search
from .storage import connect as source_connect, upsert_paper, ingest_extraction
from .extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction

QUERIES = ('agent long term memory', 'agent context compression', 'agent skill learning evolution')
TOPICS = ('agent_memory', 'context_management', 'skill_evolution')
CANON = lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
SHA = lambda x: hashlib.sha256(CANON(x).encode()).hexdigest()

def snapshot_digest(snapshot):
    return SHA({k:v for k,v in snapshot.items() if k!='snapshot_hash'})

class PreparationWorkflow:
    def __init__(self, run_dir, *, target=60, llm_mode='external', jev_model='jev-1.14.0',
                 batch_size=5, per_query=100, since='2025-01-01', until=None,
                 lease_seconds=1800, repair_limit=2, min_confidence=.7,
                 client=None, extractor=None, scope_fn=None, review_fn=None,
                 collectors=None, embedding=None):
        self.run_dir=Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.path=self.run_dir/'workflow.sqlite3'
        self.master=self.run_dir/'master.sqlite3'
        self.config=dict(target=target,llm_mode=llm_mode,jev_model=jev_model,batch_size=batch_size,
                         per_query=per_query,since=since,until=until or datetime.now(timezone.utc).date().isoformat(),
                         lease_seconds=lease_seconds,repair_limit=repair_limit,min_confidence=min_confidence)
        self.client=client; self.extractor=extractor; self.scope_fn=scope_fn; self.review_fn=review_fn
        self.collectors=collectors or {'openalex':openalex_search,'arxiv':arxiv_search}
        self.embedding=embedding
        self._init()

    def _db(self):
        db=sqlite3.connect(self.path,timeout=30)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA busy_timeout=30000')
        return db

    def _init(self):
        db=self._db()
        db.executescript('''
          CREATE TABLE IF NOT EXISTS run (id INTEGER PRIMARY KEY CHECK(id=1), run_id TEXT NOT NULL,
            config_json TEXT NOT NULL, collection_state TEXT NOT NULL, started REAL NOT NULL,
            completed REAL, error TEXT);
          CREATE TABLE IF NOT EXISTS papers (paper_id TEXT PRIMARY KEY,version_id TEXT NOT NULL,
            title TEXT NOT NULL,text TEXT NOT NULL,version_label TEXT,source_url TEXT NOT NULL,
            fingerprint TEXT NOT NULL,origin_source TEXT NOT NULL,origin_id TEXT NOT NULL,scope_state TEXT NOT NULL DEFAULT 'queued',
            scope_json TEXT, review_state TEXT NOT NULL DEFAULT 'none',review_json TEXT,
            extraction_json TEXT, reason TEXT, attempts INTEGER NOT NULL DEFAULT 0,
            discovered REAL NOT NULL, scope_started REAL, scoped REAL, reviewed REAL);
          CREATE INDEX IF NOT EXISTS idx_papers_scope ON papers(scope_state,discovered);
          CREATE INDEX IF NOT EXISTS idx_papers_review ON papers(review_state,discovered);
          CREATE TABLE IF NOT EXISTS tasks (task_id TEXT PRIMARY KEY,paper_id TEXT NOT NULL REFERENCES papers(paper_id),
            kind TEXT NOT NULL CHECK(kind IN ('extract','repair_extraction')),
            state TEXT NOT NULL CHECK(state IN ('queued','leased','done','failed')),
            fingerprint TEXT NOT NULL,attempt INTEGER NOT NULL, lease_token TEXT,lease_until REAL,
            worker TEXT,created REAL NOT NULL,claimed REAL,finished REAL,result_hash TEXT,
            result_json TEXT,reason TEXT,UNIQUE(paper_id,kind,attempt));
          CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(kind,state,lease_until,created);
          CREATE INDEX IF NOT EXISTS idx_tasks_paper ON tasks(paper_id,state);
          CREATE TABLE IF NOT EXISTS decision_audits (id INTEGER PRIMARY KEY AUTOINCREMENT,
            paper_id TEXT NOT NULL,stage TEXT NOT NULL,attempt INTEGER NOT NULL,
            payload_json TEXT NOT NULL,created REAL NOT NULL);
          CREATE INDEX IF NOT EXISTS idx_decision_audit_paper ON decision_audits(paper_id,stage,attempt);
          CREATE TABLE IF NOT EXISTS candidates (source TEXT NOT NULL,topic TEXT NOT NULL,
            query TEXT NOT NULL,position INTEGER NOT NULL,payload_json TEXT NOT NULL,
            PRIMARY KEY(source,topic,query,position));
          CREATE TABLE IF NOT EXISTS metrics (stage TEXT PRIMARY KEY, calls INTEGER NOT NULL DEFAULT 0,
            active_seconds REAL NOT NULL DEFAULT 0, api_seconds REAL NOT NULL DEFAULT 0,
            service_seconds REAL NOT NULL DEFAULT 0, queued_seconds REAL NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS query_progress (source TEXT NOT NULL,topic TEXT NOT NULL,
            query TEXT NOT NULL, state TEXT NOT NULL,error TEXT, received INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source,topic,query));
        ''')
        row=db.execute('SELECT config_json FROM run WHERE id=1').fetchone()
        if row:
            old=json.loads(row[0])
            if old!=self.config:
                # A caller can reopen without reproducing defaults; run configuration remains authoritative.
                self.config=old
        else:
            db.execute('INSERT INTO run VALUES(1,?,?,?,?,?,?)',(uuid.uuid4().hex,CANON(self.config),'open',time.time(),None,None))
        db.commit();db.close()
        source_connect(self.master).close()

    def _metric(self, db, stage, *, active=0, api=0, service=0, queued=0):
        db.execute('''INSERT INTO metrics VALUES(?,1,?,?,?,?) ON CONFLICT(stage) DO UPDATE SET
            calls=calls+1,active_seconds=active_seconds+excluded.active_seconds,
            api_seconds=api_seconds+excluded.api_seconds,service_seconds=service_seconds+excluded.service_seconds,
            queued_seconds=queued_seconds+excluded.queued_seconds''',(stage,active,api,service,queued))

    def add_source(self, record, source, query, topic):
        """Normalize and freeze first observed source text and version for this run."""
        row=dict(record); sid=row.pop('source_id');row['topics']=[topic]
        row['topic_basis']='discovery_query_candidate'
        con=source_connect(self.master)
        try:
            pid,vid=upsert_paper(con,row,source,sid,query)
            ver=con.execute('SELECT version_label,source_url FROM paper_versions WHERE version_id=?',(vid,)).fetchone()
        finally: con.close()
        title=row.get('title') or pid; abstract=row.get('abstract') or ''
        url=ver[1] or row.get('url')
        pins=dict(paper_id=pid,version_id=vid,title=title,text=abstract,
                  version_label=ver[0],source_url=url)
        fingerprint=SHA(pins)
        db=self._db()
        try:
            with db:
                prior=db.execute('SELECT fingerprint,origin_source,origin_id FROM papers WHERE paper_id=?',(pid,)).fetchone()
                if not prior:
                    db.execute('''INSERT INTO papers(paper_id,version_id,title,text,version_label,source_url,
                        fingerprint,origin_source,origin_id,discovered) VALUES(?,?,?,?,?,?,?,?,?,?)''',
                        (pid,vid,title,abstract,ver[0],url,fingerprint,source,sid,time.time()))
                elif prior[0]!=fingerprint and (prior[1],prior[2])==(source,sid):
                    db.execute("UPDATE papers SET scope_state='pending',review_state='pending',reason='source_changed' WHERE paper_id=?",(pid,))
                    db.execute("UPDATE tasks SET state='failed',reason='source_changed',lease_token=NULL WHERE paper_id=? AND state IN ('queued','leased')",(pid,))
            return pid
        finally:db.close()

    def _admit(self, source, topic, query, row):
        pub=row.get('published_at') or ''
        if pub and not self.config['since']<=pub[:10]<=self.config['until']:return
        db=self._db();count=db.execute('SELECT count(*) FROM papers').fetchone()[0];db.close()
        if count<self.config['target']:self.add_source(row,source,query,topic)

    def _collect_query(self, source, topic, query):
        db=self._db()
        prior=db.execute('SELECT state FROM query_progress WHERE source=? AND topic=? AND query=?',(source,topic,query)).fetchone()
        if prior and prior[0]=='done':db.close();return
        with db: db.execute("INSERT OR REPLACE INTO query_progress VALUES(?,?,?,'running',NULL,0)",(source,topic,query))
        db.close()
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
                with db:self._metric(db,'collection_request',api=request_elapsed)
                db.close()
                normalize_start=time.time()
                db=self._db()
                with db:db.execute('INSERT OR REPLACE INTO candidates VALUES(?,?,?,?,?)',
                                   (source,topic,query,received,CANON(row)))
                db.close()
                received+=1
                # Admit a balanced first share immediately, allowing scope/extraction to start.
                if received<=max(1,(self.config['target']+5)//6):self._admit(source,topic,query,row)
                db=self._db()
                with db:self._metric(db,'collection_normalize',active=time.time()-normalize_start)
                db.close()
        except Exception as exc:error=type(exc).__name__
        db=self._db()
        with db:
            db.execute('UPDATE query_progress SET state=?,error=?,received=? WHERE source=? AND topic=? AND query=?',
                       ('failed' if error else 'done',error,received,source,topic,query))
            self._metric(db,'collection',active=time.time()-started)
        db.close()

    def _topup(self):
        quota=max(1,(self.config['target']+5)//6)
        for position in range(quota,self.config['per_query']):
            for topic,query in zip(TOPICS,QUERIES):
                for source in ('openalex','arxiv'):
                    db=self._db()
                    count=db.execute('SELECT count(*) FROM papers').fetchone()[0]
                    row=db.execute('SELECT payload_json FROM candidates WHERE source=? AND topic=? AND query=? AND position=?',
                                   (source,topic,query,position)).fetchone()
                    db.close()
                    if count>=self.config['target']:return
                    if row:self._admit(source,topic,query,json.loads(row[0]))

    async def collect(self):
        for topic,query in zip(TOPICS,QUERIES):
            for source in ('openalex','arxiv'):
                await asyncio.to_thread(self._collect_query,source,topic,query)
        await asyncio.to_thread(self._topup)
        db=self._db()
        with db:
            count=db.execute('SELECT count(*) FROM papers').fetchone()[0]
            db.execute("UPDATE run SET collection_state='sealed',error=? WHERE id=1",
                       (f'collection_shortfall:{count}/{self.config["target"]}' if count<self.config['target'] else None,))
        db.close()

    def _scope_batch(self):
        from .decision_api import JevClient, classify_scope
        fn=self.scope_fn or classify_scope
        db=self._db(); now=time.time()
        with db:
            rows=db.execute("SELECT * FROM papers WHERE scope_state='queued' ORDER BY discovered LIMIT ?",(self.config['batch_size'],)).fetchall()
            for r in rows:db.execute("UPDATE papers SET scope_state='processing',scope_started=? WHERE paper_id=? AND scope_state='queued'",(now,r['paper_id']))
        db.close()
        if not rows:return 0
        items=[dict(paper_id=r['paper_id'],title=r['title'],text=r['text']) for r in rows if r['text'] and r['source_url']]
        outcomes={}
        if items:
            try:
                client=self.client or JevClient(model=self.config['jev_model'])
                outcomes={o['paper_id']:o for o in fn(items,client,min_confidence=self.config['min_confidence'])}
            except Exception as exc:
                # Transient API failure leaves the items restartable.
                db=self._db()
                with db:
                    for r in rows:db.execute("UPDATE papers SET scope_state='queued' WHERE paper_id=? AND scope_state='processing'",(r['paper_id'],))
                    db.execute('UPDATE run SET error=? WHERE id=1',(type(exc).__name__,))
                    audit=getattr(locals().get('client'),'last_request_audit',None)
                    if audit:
                        for r in rows:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(?,?,?,?,?)',
                            (r['paper_id'],'scope_error',0,CANON(dict(error=type(exc).__name__,request=audit)),time.time()))
                db.close();raise
        now2=time.time();db=self._db()
        with db:
            for r in rows:
                o=outcomes.get(r['paper_id'])
                decision=o['decision'] if o else 'pending'
                if decision not in ('include','exclude','pending'):decision='pending'
                db.execute('''UPDATE papers SET scope_state=?,scope_json=?,scoped=?,reason=?
                    WHERE paper_id=? AND scope_state='processing' ''',
                    (decision,CANON(o) if o else None,now2,
                     'missing_source_content' if not o else o.get('reason'),r['paper_id']))
                if o:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(?,?,?,?,?)',
                    (r['paper_id'],'scope',0,CANON(o),now2))
                if decision=='include':self._queue_task(db,r['paper_id'],'extract',r['fingerprint'],0)
                self._metric(db,'scope',active=(now2-now)/len(rows),
                    queued=now-r['discovered'])
            self._metric(db,'scope_api',api=self._unique_api_seconds(outcomes))
            db.execute("UPDATE run SET error=NULL WHERE id=1 AND error NOT LIKE 'collection_shortfall:%'")
        db.close();return len(rows)

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
        db.execute('''INSERT OR IGNORE INTO tasks(task_id,paper_id,kind,state,fingerprint,attempt,created)
          VALUES(?,?,?,?,?,?,?)''',(uuid.uuid4().hex,pid,kind,'queued',fingerprint,attempt,time.time()))

    def claim(self, kind='extract', limit=5, worker='external'):
        if kind not in ('extract','repair_extraction') or limit<1 or limit>100:raise ValueError('Invalid claim kind/limit')
        db=self._db();now=time.time();out=[]
        try:
            with db:
                rows=db.execute('''SELECT t.*,p.title,p.text,p.version_id,p.version_label,p.source_url,p.review_json,p.extraction_json
                    FROM tasks t JOIN papers p ON p.paper_id=t.paper_id AND p.fingerprint=t.fingerprint
                    WHERE t.kind=? AND (t.state='queued' OR (t.state='leased' AND t.lease_until<?))
                    ORDER BY t.created LIMIT ?''',(kind,now,limit)).fetchall()
                for r in rows:
                    token=secrets.token_urlsafe(32)
                    cur=db.execute('''UPDATE tasks SET state='leased',lease_token=?,lease_until=?,worker=?,claimed=?
                      WHERE task_id=? AND (state='queued' OR (state='leased' AND lease_until<?))''',
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
                        self._metric(db,'extract_queue',queued=now-r['created'])
        finally:db.close()
        return out

    def submit(self, entries):
        if not isinstance(entries,list):raise ValueError('Submission must be a list')
        outputs=[]
        for entry in entries:outputs.append(self._submit_one(entry))
        return outputs

    def _submit_one(self,e):
        db=self._db();now=time.time()
        try:
            with db:
                r=db.execute('''SELECT t.*,p.version_id,p.version_label,p.source_url,p.fingerprint AS current_hash,
                    p.scope_state,p.title,p.text FROM tasks t JOIN papers p ON p.paper_id=t.paper_id
                    WHERE t.task_id=?''',(e['task_id'],)).fetchone()
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
                db.execute("UPDATE tasks SET state='done',result_hash=?,result_json=?,finished=? WHERE task_id=?",
                           (h,CANON(draft),now,e['task_id']))
                db.execute("UPDATE papers SET review_state='queued',extraction_json=? WHERE paper_id=?",
                           (CANON(draft),r['paper_id']))
                self._metric(db,'external_extraction',service=now-r['claimed'])
                return dict(task_id=e['task_id'],status='accepted')
        finally:db.close()

    def _review_batch(self):
        from .decision_api import JevClient, review_extractions
        fn=self.review_fn or review_extractions
        db=self._db()
        with db:
            rows=db.execute("SELECT * FROM papers WHERE review_state='queued' AND scope_state='include' ORDER BY discovered LIMIT ?",(self.config['batch_size'],)).fetchall()
            for r in rows:db.execute("UPDATE papers SET review_state='processing' WHERE paper_id=? AND review_state='queued'",(r['paper_id'],))
        db.close()
        if not rows:return 0
        start=time.time()
        try:
            client=self.client or JevClient(model=self.config['jev_model'])
            outcomes={o['paper_id']:o for o in fn([dict(paper_id=r['paper_id'],text=r['text'],extraction=json.loads(r['extraction_json'])) for r in rows],client,min_confidence=self.config['min_confidence'])}
        except Exception as exc:
            db=self._db()
            with db:
                for r in rows:db.execute("UPDATE papers SET review_state='queued' WHERE paper_id=? AND review_state='processing'",(r['paper_id'],))
                db.execute('UPDATE run SET error=? WHERE id=1',(type(exc).__name__,))
                audit=getattr(locals().get('client'),'last_request_audit',None)
                if audit:
                    for r in rows:db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(?,?,?,?,?)',
                        (r['paper_id'],'field_review_error',r['attempts']+1,CANON(dict(error=type(exc).__name__,request=audit)),time.time()))
            db.close();raise
        db=self._db(); now=time.time()
        with db:
            for r in rows:
                o=outcomes.get(r['paper_id']) or dict(passed=False,issues=['missing review response'])
                payload=json.loads(r['extraction_json'])
                nonempty=any(payload[k] for k in ('contributions','research_problems','methods','benchmarks'))
                passed=bool(o.get('passed')) and nonempty
                attempts=r['attempts']+1
                db.execute('INSERT INTO decision_audits(paper_id,stage,attempt,payload_json,created) VALUES(?,?,?,?,?)',
                    (r['paper_id'],'field_review',attempts,CANON(o),now))
                if passed:
                    # The source store validates the exact owner pins as well as schema.
                    payload['status']='reviewed'
                    con=source_connect(self.master)
                    try:
                        # Source must still match frozen data; conflicting collector refreshes cannot approve.
                        ver=con.execute('SELECT source_url,abstract,version_label FROM paper_versions WHERE version_id=?',(r['version_id'],)).fetchone()
                        if not ver or (ver[0],ver[1],ver[2])!=(r['source_url'],r['text'],r['version_label']):
                            passed=False;o=dict(passed=False,issues=['source_changed'],raw=o)
                        else: ingest_extraction(con,payload);con.commit()
                    finally:con.close()
                state='ready' if passed else ('pending' if attempts>self.config['repair_limit'] else 'repair_queued')
                db.execute('''UPDATE papers SET review_state=?,review_json=?,extraction_json=?,attempts=?,reviewed=?,reason=?
                    WHERE paper_id=? AND review_state='processing' ''',
                    (state,CANON(o),CANON(payload),attempts,now,None if passed else '; '.join(map(str,o.get('issues',[])))[:1000],r['paper_id']))
                if state=='repair_queued':self._queue_task(db,r['paper_id'],'repair_extraction',r['fingerprint'],attempts)
                self._metric(db,'field_review',active=(now-start)/len(rows))
            self._metric(db,'field_review_api',api=self._unique_api_seconds(outcomes))
            db.execute("UPDATE run SET error=NULL WHERE id=1 AND error NOT LIKE 'collection_shortfall:%'")
        db.close();return len(rows)

    def status(self):
        db=self._db()
        try:
            run=dict(db.execute('SELECT * FROM run WHERE id=1').fetchone())
            return dict(run_id=run['run_id'],collection_state=run['collection_state'],config=json.loads(run['config_json']),
               papers={r[0]:r[1] for r in db.execute('SELECT scope_state,count(*) FROM papers GROUP BY scope_state')},
               review={r[0]:r[1] for r in db.execute('SELECT review_state,count(*) FROM papers GROUP BY review_state')},
               tasks={r[0]+'/'+r[1]:r[2] for r in db.execute('SELECT kind,state,count(*) FROM tasks GROUP BY kind,state')},
               queries={r[0]:r[1] for r in db.execute('SELECT state,count(*) FROM query_progress GROUP BY state')},
               metrics={r[0]:dict(calls=r[1],active_seconds=r[2],api_seconds=r[3],service_seconds=r[4],queued_seconds=r[5]) for r in db.execute('SELECT * FROM metrics')},
               wall_seconds=(run['completed'] or time.time())-run['started'],error=run['error'])
        finally:db.close()

    async def run(self, *, poll_seconds=1, collect=True, stop_event=None):
        db=self._db()
        with db:db.execute("UPDATE papers SET scope_state='queued' WHERE scope_state='processing'")
        with db:db.execute("UPDATE papers SET review_state='queued' WHERE review_state='processing'")
        sealed=db.execute('SELECT collection_state FROM run WHERE id=1').fetchone()[0]=='sealed'
        db.close()
        producer=asyncio.create_task(self.collect()) if collect and not sealed else None
        state='sealed' if sealed else 'open';outstanding=1
        try:
            while True:
                if stop_event and stop_event.is_set():break
                n=await asyncio.to_thread(self._scope_batch)
                n+=await asyncio.to_thread(self._review_batch)
                if self.config['llm_mode']=='api':
                    n+=await asyncio.to_thread(self._api_extract)
                db=self._db()
                state=db.execute('SELECT collection_state FROM run WHERE id=1').fetchone()[0]
                outstanding=db.execute("SELECT (SELECT count(*) FROM papers WHERE scope_state IN ('queued','processing') OR review_state IN ('queued','processing','repair_queued'))+(SELECT count(*) FROM tasks WHERE state IN ('queued','leased'))").fetchone()[0]
                db.close()
                if state=='sealed' and outstanding==0:break
                if not n:await asyncio.sleep(poll_seconds)
            db=self._db()
            with db:
                if state=='sealed' and outstanding==0:db.execute('UPDATE run SET completed=? WHERE id=1',(time.time(),))
            db.close()
        finally:
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
                    result=self.extractor.generate_json(EXTRACTION_INSTRUCTIONS+' Repair the previous extraction using review issues; return complete v0.3 object with original metadata pins.',
                        {'paper_id':task['paper_id'],'version_id':task['version_id'],
                         'paper_version':task['paper_version'],'paper_text':task['text'],
                         'previous_extraction':task['previous_extraction'],'review_issues':task['review_issues']},
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
        run=db.execute('SELECT * FROM run WHERE id=1').fetchone()
        if run['collection_state']!='sealed' or run['completed'] is None or run['error']:
            db.close();raise ValueError('Collection and preparation must be terminal')
        if db.execute("SELECT 1 FROM tasks WHERE state IN ('queued','leased') LIMIT 1").fetchone():
            db.close();raise ValueError('Active tasks remain')
        rows=db.execute('SELECT * FROM papers ORDER BY paper_id').fetchall()
        ready=[r for r in rows if r['review_state']=='ready']
        con=source_connect(self.master)
        papers=[]
        for r in ready:
            metadata=con.execute('SELECT first_published,year,doi,arxiv_id FROM papers WHERE paper_id=?',(r['paper_id'],)).fetchone()
            papers.append(dict(paper_id=r['paper_id'],version_id=r['version_id'],title=r['title'],text=r['text'],
                paper_version=dict(version_label=r['version_label'],source_url=r['source_url']),
                source_hash=r['fingerprint'],first_published=metadata[0],year=metadata[1],doi=metadata[2],
                arxiv_id=metadata[3],scope=json.loads(r['scope_json'])))
        con.close()
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
        with db:self._metric(db,'snapshot_embedding',active=embed_elapsed)
        result=dict(schema_version='research-kg-prepared-snapshot/1',run_id=run['run_id'],
            counts=dict(collected=len(rows),ready=len(ready),excluded=sum(r['scope_state']=='exclude' for r in rows),
                        pending=sum(r['scope_state']=='pending' or r['review_state']=='pending' for r in rows)),
            config=json.loads(run['config_json']),papers=papers,extractions=extractions,vectors=vectors,
            excluded=[dict(paper_id=r['paper_id'],reason=r['reason'],scope=json.loads(r['scope_json']) if r['scope_json'] else None) for r in rows if r['scope_state']=='exclude'],
            pending=[dict(paper_id=r['paper_id'],reason=r['reason'],scope_state=r['scope_state'],review_state=r['review_state']) for r in rows if r['scope_state']=='pending' or r['review_state']=='pending'])
        result['snapshot_hash']=snapshot_digest(result)
        db.close()
        path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
        data=CANON(result)+'\n'
        if path.exists():
            if path.read_text()==data:return result
            raise ValueError('Snapshot output already exists with different content')
        tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
        tmp.write_text(data,encoding='utf-8');os.replace(tmp,path)
        return result
