"""Full, restartable metadata collection feeding the existing preparation workflow."""
from __future__ import annotations
import asyncio
from datetime import date
from email.utils import parsedate_to_datetime
import hashlib
import json
import logging
from pathlib import Path
import threading
import time
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from xml.etree import ElementTree as ET

from .collectors import fetch_bytes, normalize_openalex, normalize_arxiv_entry
from .preparation import CANON, PreparationWorkflow
from psycopg import sql
import secrets

PLAN = Path(__file__).resolve().parents[2] / 'config/full_preparation_plan.json'
OA_SELECT = 'id,doi,title,publication_year,publication_date,authorships,abstract_inverted_index,ids,primary_location,updated_date'
SOURCE_ATTEMPTS = 5
LOG = logging.getLogger(__name__)


def _transient_source_error(exc):
    if isinstance(exc, HTTPError):
        return exc.code in (429,500,502,503,504)
    return isinstance(exc, (URLError,TimeoutError,ConnectionError))


def _source_retry_delay(exc, attempt):
    value=exc.headers.get('Retry-After') if isinstance(exc,HTTPError) and exc.headers else None
    if value:
        try:
            if value.strip().isdigit():return float(value)
            return max(0,parsedate_to_datetime(value).timestamp()-time.time())
        except (ValueError,TypeError,OverflowError):pass
    return min(30,3.05*2**(attempt-1))


class _RetrySourcePage(Exception):
    def __init__(self, source, cause, delay):
        self.source=source
        self.cause=cause
        self.delay=delay


def fetch_page(source, plan, window, position, *, fetch=fetch_bytes):
    """Return raw response; cursor and API totals are interpreted after it is durable."""
    if source == 'openalex':
        args = {'search.title_and_abstract': plan['openalex_query'],
                'filter': 'from_publication_date:'+window['since']+',to_publication_date:'+window['until'],
                'per-page':plan['page_size'], 'cursor':position, 'select':OA_SELECT}
        import os
        if os.getenv('OPENALEX_API_KEY'): args['api_key']=os.environ['OPENALEX_API_KEY']
        return fetch('https://api.openalex.org/works?'+urlencode(args),max_retries=1)
    args={'search_query':plan['arxiv_query'],'start':position,'max_results':plan['page_size'],
          'sortBy':'submittedDate','sortOrder':'descending'}
    # The stream owns the persistent three-second gate. Internal HTTP retries
    # would bypass it; retries reacquire the stream lease and this gate.
    return fetch('https://export.arxiv.org/api/query?'+urlencode(args),
                 headers={'Accept':'application/atom+xml'},max_retries=1)


def parse_page(source, raw, position, received=0):
    if source=='openalex':
        obj=json.loads(raw)
        rows=obj['results']; meta=obj['meta']; total=meta['count']
        if not isinstance(rows,list) or not isinstance(total,int) or total<0:raise ValueError('Invalid OpenAlex page')
        following=meta.get('next_cursor')
        if rows and following==position:raise ValueError('Unchanged OpenAlex cursor')
        if rows and not following and received+len(rows)<total:raise ValueError('Missing OpenAlex next_cursor')
        if not rows and received<total:raise ValueError('Incomplete OpenAlex cursor stream')
        return [normalize_openalex(x) for x in rows], following if rows else None, total
    root=ET.fromstring(raw)
    ns={'a':'http://www.w3.org/2005/Atom','o':'http://a9.com/-/spec/opensearch/1.1/'}
    total=int(root.findtext('o:totalResults',namespaces=ns))
    entries=root.findall('a:entry',ns)
    if not entries and int(position)<total:raise ValueError('Incomplete arXiv page')
    rows=[normalize_arxiv_entry(e) for e in entries]
    following=int(position)+len(entries)
    return rows, following if following<total else None, total


class FullPreparationWorkflow(PreparationWorkflow):
    def __init__(self, run_dir, *, plan=None, fetcher=None, **kwargs):
        proposed=json.loads(PLAN.read_text(encoding='utf-8')) if plan is None else plan
        if proposed['schema']!='full-preparation-plan/1' or proposed['page_size']!=100:
            raise ValueError('Invalid full collection plan')
        self.fetcher=fetcher or fetch_page
        self._stop_collection=threading.Event()
        for field,value in (('target',0),('llm_mode','external'),('since',proposed['since']),
                            ('until',proposed['until']),('jev_concurrency',8),
                            ('generation_concurrency',4),('scope_workers',4),('review_workers',4)):
            kwargs.setdefault(field,value)
        super().__init__(run_dir,**kwargs)
        if self.config['target']!=0 or self.config['llm_mode']!='external':
            raise ValueError('Full collection requires a dedicated external-mode logical run')
        db=self._db()
        try:
            with db.transaction():
                lock_key=int.from_bytes(hashlib.sha256(('full-plan:'+self.store.workflow_schema).encode()).digest()[:8],
                                        'big',signed=True)
                db.execute('SELECT pg_advisory_xact_lock(%s)',(lock_key,))
                saved=db.execute('SELECT payload FROM full_plan WHERE id=1 FOR UPDATE').fetchone()
                self.plan=json.loads(saved['payload']) if saved and plan is None else proposed
                if saved and saved['payload']!=CANON(self.plan):
                    raise ValueError('Full collection plan differs from saved plan')
                if not saved:
                    if db.execute('SELECT 1 FROM papers LIMIT 1').fetchone() or db.execute('SELECT 1 FROM query_progress LIMIT 1').fetchone():
                        raise ValueError('Full collection requires an unused logical run')
                    windows=self.plan['openalex_windows']
                    if len(windows)<4 or windows[0]['since']!=self.plan['since'] or windows[-1]['until']!=self.plan['until']:
                        raise ValueError('Incomplete OpenAlex windows')
                    for i in range(1,len(windows)):
                        if date.fromisoformat(windows[i-1]['until']).toordinal()+1!=date.fromisoformat(windows[i]['since']).toordinal():
                            raise ValueError('OpenAlex windows overlap or leave gaps')
                    db.execute('INSERT INTO full_plan VALUES(1,%s)',(CANON(self.plan),))
                    for n in range(4):
                        db.execute('INSERT INTO full_streams(stream_id,source,windows_json,position) VALUES(%s,%s,%s,%s)',
                                   (f'openalex-{n}','openalex',CANON(windows[n::4]),'*'))
                    db.execute('INSERT INTO full_streams(stream_id,source,windows_json,position) VALUES(%s,%s,%s,%s)',
                               ('arxiv-0','arxiv',CANON([{'since':self.plan['since'],'until':self.plan['until']}]),'0'))
        finally:self._release(db)

    def _admit_full(self, record, source, db=None):
        published=record.get('published_at') or ''
        sid=record['source_id']
        query=self.plan['openalex_query'] if source=='openalex' else self.plan['arxiv_query']
        row={**record,'topics':['all_topics'],'topic_basis':'discovery_query_candidate'}
        row.pop('source_id')
        owns_connection=db is None
        if owns_connection:db=self._db()
        try:
            if owns_connection:
                with db.transaction():return self._admit_full(record,source,db)
            pid,vid=self.source.upsert_paper(db,row,source,sid,query)
            first=db.execute(sql.SQL('SELECT first_published FROM {} WHERE paper_id=%s').format(
                self.store.source_table('papers')),(pid,)).fetchone()['first_published']
            if first and first[:10]<self.plan['since']:
                db.execute("UPDATE papers SET scope_state='pending',review_state='pending',reason='before_window',scope_token=NULL,review_token=NULL WHERE paper_id=%s",(pid,))
                db.execute("UPDATE tasks SET state='failed',reason='before_window',lease_token=NULL WHERE paper_id=%s AND state IN ('queued','leased')",(pid,))
                db.execute("UPDATE item_reviews SET state='revoked',approved_json=NULL WHERE paper_id=%s",(pid,))
                self._assert_mutable(db)
                return 0
            if published and published[:10]>self.plan['until']:
                self._assert_mutable(db)
                return 0
            if record.get('abstract'):
                existed=bool(db.execute('SELECT 1 FROM papers WHERE paper_id=%s',(pid,)).fetchone())
                self._freeze_source(db,row,source,sid,pid,vid)
                return int(not existed)
            db.execute('INSERT INTO full_deferred(source,source_id,record_json) VALUES(%s,%s,%s) ON CONFLICT(source,source_id) DO NOTHING',
                       (source,sid,CANON(record)))
            self._assert_mutable(db)
            return 0
        finally:
            if owns_connection:self._release(db)

    def _consume(self, stream_id, *, attempt=SOURCE_ATTEMPTS):
        db=self._db();token=secrets.token_urlsafe(24)
        request_failure=False
        try:
            with db.transaction():
                stream=db.execute('SELECT * FROM full_streams WHERE stream_id=%s FOR UPDATE SKIP LOCKED',(stream_id,)).fetchone()
                if not stream or stream['state']=='done' or (stream['state']=='processing' and stream['lease_until'] and stream['lease_until']>time.time()):
                    return False
                if stream['state']=='failed':return False
                windows=json.loads(stream['windows_json'])
                if stream['window_index']>=len(windows):return False
                wi=stream['window_index'];position=stream['position'];source=stream['source']
                db.execute("UPDATE full_streams SET state='processing',lease_token=%s,lease_until=%s WHERE stream_id=%s",
                           (token,time.time()+self.config['lease_seconds'],stream_id))
                page=db.execute('SELECT raw,applied_rows,admitted FROM full_pages WHERE stream_id=%s AND window_index=%s AND position=%s',
                                (stream_id,wi,position)).fetchone()
                if page is None and source=='arxiv':
                    request_at=max(time.time(),stream['next_request_at'])
                    # Reserve a small scheduling margin so measured HTTP start
                    # times remain at least three seconds apart.
                    db.execute('UPDATE full_streams SET next_request_at=%s WHERE stream_id=%s',(request_at+3.05,stream_id))
                else:request_at=0
            if page is None:
                if request_at and self._stop_collection.wait(max(0,request_at-time.time())):
                    raise InterruptedError('Full collection stopped')
                if self._stop_collection.is_set():raise InterruptedError('Full collection stopped')
                try:raw=self.fetcher(source,self.plan,windows[wi],position)
                except Exception:
                    request_failure=True
                    raise
                with db.transaction():
                    current=db.execute('SELECT lease_token,lease_until FROM full_streams WHERE stream_id=%s FOR UPDATE',(stream_id,)).fetchone()
                    if current['lease_token']!=token or current['lease_until']<time.time():raise ValueError('Stale stream lease')
                    db.execute('INSERT INTO full_pages(stream_id,window_index,position,raw) VALUES(%s,%s,%s,%s) ON CONFLICT(stream_id,window_index,position) DO NOTHING',
                               (stream_id,wi,position,raw))
                    page=db.execute('SELECT raw,applied_rows,admitted FROM full_pages WHERE stream_id=%s AND window_index=%s AND position=%s',
                                    (stream_id,wi,position)).fetchone()
            raw=bytes(page['raw'])
            rows,next_position,total=parse_page(source,raw,position,stream['window_received'])
            if len(rows)>self.plan['page_size']:raise ValueError('Oversized source page')
            for index in range(page['applied_rows'],len(rows)):
                with db.transaction():
                    current=db.execute('SELECT lease_token,lease_until FROM full_streams WHERE stream_id=%s',(stream_id,)).fetchone()
                    if current['lease_token']!=token or current['lease_until']<time.time():raise ValueError('Stale stream lease')
                    admitted=self._admit_full(rows[index],source,db)
                    renewal=time.time()
                    updated=db.execute("UPDATE full_streams SET lease_until=%s WHERE stream_id=%s AND lease_token=%s AND state='processing' AND lease_until>=%s",
                                       (renewal+self.config['lease_seconds'],stream_id,token,renewal))
                    if updated.rowcount!=1:raise ValueError('Stale stream lease')
                    db.execute('UPDATE full_pages SET applied_rows=%s,admitted=admitted+%s WHERE stream_id=%s AND window_index=%s AND position=%s AND applied_rows=%s',
                               (index+1,admitted,stream_id,wi,position,index))
            new_wi=wi+1 if next_position is None else wi
            new_position=('*' if source=='openalex' else '0') if next_position is None else str(next_position)
            state='done' if new_wi==len(windows) else 'open'
            with db.transaction():
                current=db.execute('SELECT lease_token,lease_until,window_received FROM full_streams WHERE stream_id=%s FOR UPDATE',(stream_id,)).fetchone()
                if current['lease_token']!=token or current['lease_until']<time.time():raise ValueError('Stale stream lease')
                page=db.execute('SELECT state,applied_rows,admitted FROM full_pages WHERE stream_id=%s AND window_index=%s AND position=%s FOR UPDATE',
                                (stream_id,wi,position)).fetchone()
                if page['state']=='applied':return state!='done'
                if page['applied_rows']!=len(rows):raise ValueError('Incomplete page admission')
                db.execute("UPDATE full_pages SET state='applied',count=%s,total=%s WHERE stream_id=%s AND window_index=%s AND position=%s",
                           (len(rows),total,stream_id,wi,position))
                db.execute('''UPDATE full_streams SET window_index=%s,position=%s,state=%s,
                    received=received+%s,window_received=%s,admitted=admitted+%s,
                    lease_token=NULL,lease_until=NULL,error=NULL WHERE stream_id=%s AND lease_token=%s''',
                    (new_wi,new_position,state,len(rows),current['window_received']+len(rows) if new_wi==wi else 0,
                     page['admitted'],stream_id,token))
            return state!='done'
        except Exception as exc:
            retry=request_failure and _transient_source_error(exc) and attempt<SOURCE_ATTEMPTS
            with db.transaction():
                updated=db.execute('''UPDATE full_streams SET state=%s,lease_token=NULL,lease_until=NULL,error=%s
                    WHERE stream_id=%s AND lease_token=%s''',
                    ('open' if retry else 'failed',type(exc).__name__+(' HTTP '+str(exc.code) if isinstance(getattr(exc,'code',None),int) else ''),stream_id,token))
            if retry and updated.rowcount==1:
                raise _RetrySourcePage(source,exc,_source_retry_delay(exc,attempt)) from exc
            raise
        finally:self._release(db)

    def _stream(self, stream_id):
        attempt=1
        while not self._stop_collection.is_set():
            try:more=self._consume(stream_id,attempt=attempt)
            except _RetrySourcePage as retry:
                LOG.warning('Source retry source=%s attempt=%s/%s delay=%.3fs type=%s',
                            retry.source,attempt+1,SOURCE_ATTEMPTS,retry.delay,type(retry.cause).__name__)
                if self._stop_collection.wait(retry.delay):raise InterruptedError('Full collection stopped')
                attempt+=1
                continue
            if more:
                attempt=1
                continue
            db=self._db()
            try:state=db.execute('SELECT state FROM full_streams WHERE stream_id=%s',(stream_id,)).fetchone()['state']
            finally:self._release(db)
            if state=='done':return
            if state=='failed':raise ValueError('Full stream failed')
            self._stop_collection.wait(.5)
        raise InterruptedError('Full collection stopped')

    def _finish_empty(self):
        db=self._db()
        try:
            deferred=db.execute('SELECT source,source_id,record_json FROM full_deferred ORDER BY source,source_id').fetchall()
            for row in deferred:
                source,sid=row['source'],row['source_id']
                with db.transaction():
                    source_record=db.execute(sql.SQL('SELECT paper_id FROM {} WHERE source_name=%s AND source_id=%s').format(
                        self.store.source_table('source_records')),(source,sid)).fetchone()
                    pid=source_record['paper_id']
                    first=db.execute(sql.SQL('SELECT first_published FROM {} WHERE paper_id=%s').format(
                        self.store.source_table('papers')),(pid,)).fetchone()['first_published']
                    if not first or first[:10]>=self.plan['since']:
                        if not db.execute('SELECT 1 FROM papers WHERE paper_id=%s',(pid,)).fetchone():
                            record=json.loads(row['record_json'])
                            normalized={**record,'topics':['all_topics'],'topic_basis':'discovery_query_candidate'}
                            normalized.pop('source_id')
                            _,vid=self.source.upsert_paper(db,normalized,source,sid,'all_topics')
                            self._freeze_source(db,normalized,source,sid,pid,vid)
                    db.execute('DELETE FROM full_deferred WHERE source=%s AND source_id=%s',(source,sid))
        finally:self._release(db)

    async def collect(self):
        # One arXiv request lane and four independent OpenAlex cursor lanes.
        try:
            self._stop_collection.clear()
            # Explicit run resume retains its existing failed-stream recovery.
            for stream_id in ('openalex-0','openalex-1','openalex-2','openalex-3','arxiv-0'):
                db=self._db()
                with db.transaction():db.execute("UPDATE full_streams SET state='open',error=NULL WHERE stream_id=%s AND state='failed'",(stream_id,))
                self._release(db)
            await asyncio.gather(*(asyncio.to_thread(self._stream,s) for s in
                ('openalex-0','openalex-1','openalex-2','openalex-3','arxiv-0')))
            if self._stop_collection.is_set():raise InterruptedError('Full collection stopped')
            await asyncio.to_thread(self._finish_empty)
        except asyncio.CancelledError:
            self._stop_collection.set()
            raise
        except Exception as exc:
            self._stop_collection.set()
            db=self._db()
            with db.transaction():db.execute('UPDATE run SET error=%s WHERE id=1',('full_collection_failed:'+type(exc).__name__,))
            self._release(db)
            raise
        db=self._db()
        try:
            with db.transaction():
                if self._stop_collection.is_set() or db.execute("SELECT 1 FROM full_streams WHERE state<>'done' LIMIT 1").fetchone():
                    raise InterruptedError('Full collection unfinished')
                db.execute("UPDATE run SET collection_state='sealed',error=NULL WHERE id=1")
        finally:self._release(db)

    def status(self):
        result=super().status()
        db=self._db()
        try:
            result['full_streams']=[dict(r) for r in db.execute('SELECT stream_id,state,window_index,position,received,admitted,error FROM full_streams ORDER BY stream_id')]
        finally:self._release(db)
        return result
