"""Durable external naming handoff; only the controller performs API work.

The native graph remains the authority for leases and semantic acceptance. This
separate transport schema never changes the native graph schema fingerprint.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time
import threading
import uuid

from psycopg import sql
from .snapshot_graph import canonical_hash, validate_name_result

REVISION = 'external-graph-queue/2'
DDL = '''
CREATE TABLE bootstrap(id INTEGER PRIMARY KEY CHECK(id=1),run_id TEXT NOT NULL,revision TEXT NOT NULL,fingerprint TEXT NOT NULL);
CREATE TABLE receipts(task_id TEXT NOT NULL,token TEXT NOT NULL,worker TEXT NOT NULL,input_hash TEXT NOT NULL,payload TEXT,payload_hash TEXT,status TEXT NOT NULL DEFAULT 'claimed',created DOUBLE PRECISION NOT NULL,ready_at DOUBLE PRECISION,enqueued DOUBLE PRECISION,processing DOUBLE PRECISION,finished DOUBLE PRECISION,PRIMARY KEY(task_id,token));
CREATE INDEX receipt_worker ON receipts(worker,status);
CREATE INDEX receipt_ready ON receipts(status,created,task_id);
'''


def lock_key(value):
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8],'big',signed=True)


class HelperGraph:
    """Metadata-only handle; claim uses the unchanged native implementation.

    This deliberately does not call SnapshotGraph.__init__, whose read_input
    optimization can fall back to reading the full immutable snapshot.
    """
    from .snapshot_graph import SnapshotGraph as _Native
    claim = _Native.claim
    _meta = _Native._meta
    _semantic_store = _Native._semantic_store
    close = _Native.close

    def __init__(self, snapshot, job_dir, run_id=None, *, bootstrap=False, verify_snapshot=True):
        from .postgres_graph import GraphStore, graph_run_identity
        if not bootstrap and not (Path(job_dir)/'graph-job.json').is_file():
            raise ValueError('Controller must create graph job before helpers start')
        self.run_id=graph_run_identity(job_dir,run_id)
        self.database_url=None
        self.graph_store=GraphStore(self.run_id,initialize=bootstrap)
        self.db=self.graph_store.db
        self.model=None
        self.lease_seconds=1800
        try:
            if not bootstrap and verify_snapshot:
                stat=Path(snapshot).stat()
                rows={r['key']:r['value'] for r in self.db.execute("SELECT key,value FROM meta WHERE key IN ('snapshot_hash','file_hash','snapshot_stat','graph_policy','name_mode','phase')")}
                if (rows.get('snapshot_stat')!=f'{stat.st_size}:{stat.st_mtime_ns}'
                        or not rows.get('snapshot_hash') or not rows.get('file_hash')):
                    raise ValueError('Controller must verify snapshot metadata before helpers start')
                if rows.get('name_mode')!='external':raise ValueError('External name mode required')
                self.graph_policy=rows['graph_policy']
        except BaseException:
            self.close();raise


class ExternalGraphQueue:
    def __init__(self, job, *, initialize=False):
        self.job=job
        self.db=job.graph_store.connect()
        self.schema='kg_ext_'+hashlib.sha256(job.run_id.encode()).hexdigest()[:24]
        self.table=sql.SQL('{}.receipts').format(sql.Identifier(self.schema))
        self.owner=False
        fingerprint=hashlib.sha256(DDL.encode()).hexdigest()
        try:
            with self.db.transaction():
                if initialize:
                    self.db.execute('SELECT pg_advisory_xact_lock(%s)',(lock_key('external-ddl:'+job.run_id),))
                present=self.db.execute('SELECT to_regclass(%s) AS name',(self.schema+'.bootstrap',)).fetchone()['name']
                if present is None:
                    if not initialize:raise ValueError('External graph controller has not initialized the queue')
                    self.db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
                    self.db.execute(sql.SQL('SET LOCAL search_path TO {}, pg_catalog').format(sql.Identifier(self.schema)))
                    for statement in DDL.split(';'):
                        if statement.strip():self.db.execute(statement)
                    self.db.execute('INSERT INTO bootstrap VALUES(1,%s,%s,%s)',(job.run_id,REVISION,fingerprint))
                row=self.db.execute(sql.SQL('SELECT * FROM {}.bootstrap WHERE id=1').format(sql.Identifier(self.schema))).fetchone()
                if not row or (row['run_id'],row['revision'],row['fingerprint'])!=(job.run_id,REVISION,fingerprint):
                    raise ValueError('External queue identity/version differs')
        except BaseException:
            self.db.close();raise

    def acquire_owner(self):
        if self.owner:return
        if not self.db.execute('SELECT pg_try_advisory_lock(%s) AS locked',(lock_key('external-owner:'+self.job.run_id),)).fetchone()['locked']:
            raise RuntimeError('External graph controller already active')
        self.owner=True

    def close(self):
        self.db.close()  # Session lock is released even on an exceptional exit.

    def claim(self, worker):
        if not isinstance(worker,str) or not worker.strip():raise ValueError('Worker required')
        with self.db.transaction():
            # Serialize admission across helper processes. The native task lease
            # and SKIP LOCKED claim remain the semantic authority.
            self.db.execute('SELECT pg_advisory_xact_lock(%s)',(lock_key('external-claims:'+self.job.run_id),))
            active=self.db.execute(sql.SQL("""SELECT r.worker,r.status,r.task_id,r.token,
                t.status AS native_status,t.token AS native_token,t.expires
                FROM {} r JOIN {}.tasks t ON t.task_id=r.task_id
                WHERE r.status IN ('claimed','ready')""").format(self.table,sql.Identifier(self.job.graph_store.schema))).fetchall()
            live=[]
            now=time.time()
            for row in active:
                valid=(row['native_status']=='leased' and row['native_token']==row['token'] and row['expires']>now)
                # A ready result may already have committed natively before an
                # interrupted queue acknowledgement; leave it for replay.
                if row['status']=='ready' or valid:live.append(row)
                else:
                    self.db.execute(sql.SQL("UPDATE {} SET status='expired',finished=%s WHERE task_id=%s AND token=%s").format(self.table),(now,row['task_id'],row['token']))
            own=[r for r in live if r['worker']==worker]
            # Allow enqueue -> next claim immediately, with at most two durable
            # unacknowledged results per worker (one can still be generating).
            if any(r['status']=='claimed' for r in own) or len(own)>=2:
                return {'state':'waiting','tasks':[]}
            if worker not in {r['worker'] for r in live} and len({r['worker'] for r in live})>=6:
                return {'state':'waiting','tasks':[]}
            tasks=self.job.claim(worker,1)
            for task in tasks:
                native=self.db.execute(sql.SQL('SELECT queued_at,claimed_at FROM {}.tasks WHERE task_id=%s').format(sql.Identifier(self.job.graph_store.schema)),(task['task_id'],)).fetchone()
                self.db.execute(sql.SQL('INSERT INTO {}(task_id,token,worker,input_hash,created,ready_at) VALUES(%s,%s,%s,%s,%s,%s)').format(self.table),(task['task_id'],task['lease_token'],worker,task['input_hash'],native['claimed_at'],native['queued_at']))
        return {'state':'claimed' if tasks else ('complete' if self.job._meta('phase')=='complete' else 'idle'),'tasks':tasks}

    def enqueue(self, item):
        if not isinstance(item,dict):raise ValueError("Expected one name submission object")
        # Serialization is strict and the queued payload is immutable.
        encoded=json.dumps(item,ensure_ascii=False,sort_keys=True,allow_nan=False)
        fingerprint=canonical_hash(item)
        with self.db.transaction():
            row=self.db.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s AND token=%s FOR UPDATE').format(self.table),(item.get('task_id'),item.get('lease_token'))).fetchone()
            if not row:raise ValueError('Unknown external claim receipt')
            if row['payload_hash']:
                if row['payload_hash']!=fingerprint:raise ValueError('Conflicting external result')
                return {'status':row['status'],'duplicate':True}
            native=self.db.execute(sql.SQL('SELECT * FROM {}.tasks WHERE task_id=%s FOR UPDATE').format(sql.Identifier(self.job.graph_store.schema)),(item['task_id'],)).fetchone()
            if native['status']!='leased' or native['token']!=item['lease_token'] or native['expires']<=time.time():raise ValueError('Stale name lease')
            if item.get('input_hash')!=row['input_hash']:raise ValueError('Name input fingerprint mismatch')
            validate_name_result(item.get('result'),native['group_id'])
            self.db.execute(sql.SQL("UPDATE {} SET payload=%s,payload_hash=%s,enqueued=%s,status='ready' WHERE task_id=%s AND token=%s").format(self.table),(encoded,fingerprint,time.time(),item['task_id'],item['lease_token']))
        return {'status':'ready','duplicate':False}

    def process_one(self, *, after_submit=None):
        if not self.owner:raise RuntimeError('Controller ownership required')
        row=self.db.execute(sql.SQL("SELECT task_id,token,payload FROM {} WHERE status='ready' ORDER BY created,task_id LIMIT 1").format(self.table)).fetchone()
        if row is None:return False
        native=self.db.execute(sql.SQL('SELECT status,token,expires FROM {}.tasks WHERE task_id=%s').format(sql.Identifier(self.job.graph_store.schema)),(row['task_id'],)).fetchone()
        if native['status']!='done' and (native['status']!='leased' or native['token']!=row['token'] or native['expires']<=time.time()):
            self.db.execute(sql.SQL("UPDATE {} SET status='expired',finished=%s,payload=NULL WHERE task_id=%s AND token=%s").format(self.table),(time.time(),row['task_id'],row['token']))
            return True
        self.db.execute(sql.SQL('UPDATE {} SET processing=COALESCE(processing,%s) WHERE task_id=%s AND token=%s').format(self.table),(time.time(),row['task_id'],row['token']))
        item=json.loads(row['payload'])
        # Native submit revalidates pins and resumes reviews/outbox on replay.
        self.job.submit([item])
        if after_submit is not None:after_submit(item)
        with self.db.transaction():
            self.db.execute(sql.SQL("UPDATE {} SET status='accepted',finished=%s,payload=NULL WHERE task_id=%s AND token=%s AND status='ready'").format(self.table),(time.time(),row['task_id'],row['token']))
        return True

    def summary(self):
        counts={r['status']:r['n'] for r in self.db.execute(sql.SQL('SELECT status,count(*) AS n FROM {} GROUP BY status').format(self.table))}
        tasks={r['status']:r['n'] for r in self.db.execute(sql.SQL('SELECT status,count(*) AS n FROM {}.tasks GROUP BY status').format(sql.Identifier(self.job.graph_store.schema)))}
        workers=[dict(r) for r in self.db.execute(sql.SQL('SELECT worker,status,count(*) AS n,max(created) AS last_claim FROM {} GROUP BY worker,status ORDER BY worker,status').format(self.table))]
        timing=dict(self.db.execute(sql.SQL('''SELECT
            sum(GREATEST(0,created-ready_at)) AS ready_to_claim_seconds,
            sum(GREATEST(0,enqueued-created)) AS claim_to_enqueue_seconds,
            sum(GREATEST(0,processing-enqueued)) AS enqueue_to_processing_seconds,
            sum(CASE WHEN status='accepted' THEN GREATEST(0,finished-processing) END) AS native_submit_seconds
            FROM {}''').format(self.table)).fetchone())
        timing['pure_model_seconds']=None
        timing['claim_to_enqueue_includes']='Agent reading, generation, tools and handoff'
        pairs={r['decision']:r['n'] for r in self.job.db.execute('SELECT decision,count(*) AS n FROM candidates GROUP BY decision')}
        groups={r['review']:r['n'] for r in self.job.db.execute('SELECT review,count(*) AS n FROM groups GROUP BY review')}
        stages={r['name']:r['seconds'] for r in self.job.db.execute("SELECT name,seconds FROM stage WHERE name NOT LIKE 'process_cpu:%'")}
        return {'phase':self.job._meta('phase'),'queue':counts,'tasks':tasks,'workers':workers,
                'controller_owned':self.owner,'timing':timing,'native_stage_seconds':stages,
                'candidate_pairs':pairs,'groups':groups,
                'projection':{'events':int(self.job._meta('neo4j_event_count') or 0),
                              'delivered':int(self.job._meta('neo4j_delivered_count') or 0)},
                'publication_seconds':float(self.job._meta('neo4j_delivery_wall_seconds') or 0)}


def claim_to_file(queue, worker, output, *, wait_seconds=55, poll_seconds=1):
    """Prove exclusive writable output before taking any native lease."""
    if not 0<=wait_seconds<=55:raise ValueError('wait_seconds must be within 0..55')
    if poll_seconds<=0:raise ValueError('Positive poll_seconds required')
    path=Path(output)
    path.parent.mkdir(parents=True,exist_ok=True)
    # Keep the exclusive file open throughout claim/write: existing claims cannot
    # accidentally be overwritten by a second helper invocation.
    with path.open('x',encoding='utf-8') as stream:
        stream.flush();os.fsync(stream.fileno())
        deadline=time.monotonic()+wait_seconds
        while True:
            result=queue.claim(worker)
            if result['state'] not in ('idle','waiting') or time.monotonic()>=deadline:break
            time.sleep(min(poll_seconds,max(0,deadline-time.monotonic())))
        json.dump(result,stream,ensure_ascii=False,indent=2)
        stream.flush();os.fsync(stream.fileno())
    return {'state':result['state'],'task_count':len(result['tasks']),'output':str(path.resolve())}


def run_controller(queue, *, poll_seconds=1, on_summary=None, stop=None):
    """Keep one native job/client/model alive through all external handoffs."""
    queue.acquire_owner()
    # Hold the native driver lock across external waits too. Native run uses the
    # same connection, so its balanced nested acquisition is reentrant.
    driver=lock_key('graph-driver:'+queue.job.run_id)
    if not queue.job.db.execute('SELECT pg_try_advisory_lock(%s) AS locked',(driver,)).fetchone()['locked']:
        raise RuntimeError('Graph run already has an active driver')
    try:
        queue.job.run()
        while True:
            while queue.process_one():pass
            state=queue.summary()
            if on_summary is not None:on_summary(state)
            if state['phase']=='complete':return state
            if not any(status!='done' and n for status,n in state['tasks'].items()):
                queue.job.run()
                continue
            if stop is not None and stop():return state
            time.sleep(poll_seconds)
    finally:queue.job.db.execute('SELECT pg_advisory_unlock(%s)',(driver,))


def preflight_summary(snapshot, job_dir, output):
    """Protect immutable inputs, descriptors and their hard-link aliases."""
    target=Path(output).resolve()
    directory=Path(job_dir).resolve()
    if target==Path(snapshot).resolve() or target.is_relative_to(directory):
        raise ValueError('Summary output collides with protected graph input')
    protected=[Path(snapshot),*(p for p in directory.rglob('*') if p.is_file())]
    if target.exists():
        if not target.is_file():raise ValueError('Summary output must be a file')
        if any(p.exists() and target.samefile(p) for p in protected):
            raise ValueError('Summary output hardlink collides with protected graph input')
    target.parent.mkdir(parents=True,exist_ok=True)
    probe=target.with_name(target.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with probe.open('x',encoding='utf-8') as stream:
            stream.write('{}');stream.flush();os.fsync(stream.fileno())
        if target.exists():
            with target.open('a',encoding='utf-8'):pass
    finally:probe.unlink(missing_ok=True)
    return target


class SummaryMonitor:
    """Read PG metadata on thread-owned connections; never block graph work."""
    def __init__(self,snapshot,job_dir,output,*,run_id,metrics=None,interval=2):
        self.snapshot,self.job_dir,self.output=Path(snapshot),Path(job_dir),Path(output)
        self.run_id,self.metrics,self.interval=run_id,metrics,interval
        self._state={'controller_state':'loading'}
        self._latest={}
        self._lock=threading.Lock()
        self._stop=threading.Event()
        self._thread=threading.Thread(target=self._run,name='graph-metadata-monitor',daemon=True)

    def _write(self,state):
        # Serialize the two writers (periodic thread and exit-only main thread).
        # An old DB sample must never overwrite a newer terminal process state.
        with self._lock:
            state={**state,**self._state}
            self._latest=dict(state)
            temporary=self.output.with_name(self.output.name+'.'+uuid.uuid4().hex+'.tmp')
            try:
                with temporary.open('x',encoding='utf-8') as stream:
                    json.dump(state,stream,ensure_ascii=False,indent=2)
                for attempt in range(3):
                    try:
                        temporary.replace(self.output)
                        break
                    except PermissionError:
                        # Windows readers can briefly deny atomic replacement.
                        if attempt==2:raise
                        time.sleep(.01)
            finally:temporary.unlink(missing_ok=True)

    def start(self):
        self._write({'controller_state':'loading','updated_at':time.time()})
        self._thread.start()

    def state(self,value,**details):
        with self._lock:self._state={'controller_state':value,**details}

    def _run(self):
        handle=queue=None
        try:
            while True:
                closing=self._stop.is_set()
                with self._lock:state=dict(self._state)
                try:
                    if handle is None:
                        handle=HelperGraph(self.snapshot,self.job_dir,self.run_id,verify_snapshot=False)
                        queue=ExternalGraphQueue(handle)
                    state={**queue.summary(),**state}
                    if self.metrics is not None:state.update(self.metrics())
                except Exception as exc:
                    state['monitor_error_type']=type(exc).__name__
                    if queue is not None:queue.close();queue=None
                    if handle is not None:handle.close();handle=None
                state['updated_at']=time.time()
                try:self._write(state)
                except OSError as exc:
                    with self._lock:self._state['monitor_write_error_type']=type(exc).__name__
                if closing:return
                self._stop.wait(self.interval)
        except Exception:
            # A monitor write failure must not cancel already-committed work.
            # Its last updated_at stays visible to the supervising assistant.
            return
        finally:
            if queue is not None:queue.close()
            if handle is not None:handle.close()

    def close(self):
        self._stop.set()
        if self._thread.ident is not None:self._thread.join(timeout=2)
        # No database/client calls here. A blocked monitor connection must not
        # prevent the owner from publishing its known terminal process state.
        with self._lock:state={**self._latest,**self._state,'updated_at':time.time()}
        self._write(state)
