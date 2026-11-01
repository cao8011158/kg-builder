"""Full, restartable metadata collection feeding the existing preparation workflow."""
from __future__ import annotations
import asyncio
from datetime import date
import json
from pathlib import Path
import sqlite3
import threading
import time
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

from .collectors import fetch_bytes, normalize_openalex, normalize_arxiv_entry
from .preparation import CANON, PreparationWorkflow
from .storage import connect as source_connect, upsert_paper

PLAN = Path(__file__).resolve().parents[2] / 'config/full_preparation_plan.json'
OA_SELECT = 'id,doi,title,publication_year,publication_date,authorships,abstract_inverted_index,ids,primary_location,updated_date'


def fetch_page(source, plan, window, position, *, fetch=fetch_bytes):
    """Return raw response; cursor and API totals are interpreted after it is durable."""
    if source == 'openalex':
        args = {'search.title_and_abstract': plan['openalex_query'],
                'filter': 'from_publication_date:'+window['since']+',to_publication_date:'+window['until'],
                'per-page':plan['page_size'], 'cursor':position, 'select':OA_SELECT}
        import os
        if os.getenv('OPENALEX_API_KEY'): args['api_key']=os.environ['OPENALEX_API_KEY']
        return fetch('https://api.openalex.org/works?'+urlencode(args))
    args={'search_query':plan['arxiv_query'],'start':position,'max_results':plan['page_size'],
          'sortBy':'submittedDate','sortOrder':'descending'}
    # The stream owns the persistent three-second gate. Internal HTTP retries
    # would bypass it, so a failed request is retried on run resume.
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
        if plan is None:
            saved_path=Path(run_dir)/'workflow.sqlite3'
            if saved_path.exists():
                with sqlite3.connect(saved_path) as existing:
                    table=existing.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='full_plan'").fetchone()
                    saved=existing.execute('SELECT payload FROM full_plan WHERE id=1').fetchone() if table else None
                plan=json.loads(saved[0]) if saved else None
        self.plan=json.loads(PLAN.read_text()) if plan is None else plan
        self.fetcher=fetcher or fetch_page
        self._admission_lock=threading.Lock()
        self._stop_collection=threading.Event()
        if self.plan['schema']!='full-preparation-plan/1' or self.plan['page_size']!=100:
            raise ValueError('Invalid full collection plan')
        # Explicit mode flag is persisted in the regular run configuration.
        kwargs.setdefault('target',0)
        kwargs.setdefault('since',self.plan['since'])
        kwargs.setdefault('until',self.plan['until'])
        super().__init__(run_dir,**kwargs)
        if self.config['target']!=0 or self.config['llm_mode']!='external':
            raise ValueError('Full collection requires its own external-mode run directory')
        db=self._db()
        try:
            with db:
                db.executescript('''CREATE TABLE IF NOT EXISTS full_plan (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS full_streams (stream_id TEXT PRIMARY KEY, source TEXT NOT NULL,
                        windows_json TEXT NOT NULL, window_index INTEGER NOT NULL DEFAULT 0,
                        position TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'open', received INTEGER NOT NULL DEFAULT 0,
                        window_received INTEGER NOT NULL DEFAULT 0,
                        admitted INTEGER NOT NULL DEFAULT 0, next_request_at REAL NOT NULL DEFAULT 0, error TEXT);
                    CREATE TABLE IF NOT EXISTS full_pages (stream_id TEXT NOT NULL, window_index INTEGER NOT NULL,
                        position TEXT NOT NULL, raw BLOB NOT NULL, state TEXT NOT NULL DEFAULT 'fetched',
                        count INTEGER, total INTEGER, PRIMARY KEY(stream_id,window_index,position));
                    CREATE INDEX IF NOT EXISTS idx_full_pages_state ON full_pages(stream_id,state);
                    CREATE TABLE IF NOT EXISTS full_deferred (source TEXT NOT NULL, source_id TEXT NOT NULL,
                        record_json TEXT NOT NULL, PRIMARY KEY(source,source_id));''')
                if 'window_received' not in {r[1] for r in db.execute('PRAGMA table_info(full_streams)')}:
                    db.execute('ALTER TABLE full_streams ADD COLUMN window_received INTEGER NOT NULL DEFAULT 0')
                    db.execute('''UPDATE full_streams SET window_received=COALESCE((SELECT sum(count) FROM full_pages
                        WHERE full_pages.stream_id=full_streams.stream_id AND full_pages.window_index=full_streams.window_index
                        AND full_pages.state='applied'),0)''')
                saved=db.execute('SELECT payload FROM full_plan WHERE id=1').fetchone()
                if saved and saved[0]!=CANON(self.plan):raise ValueError('Full collection plan differs from saved plan')
                if not saved:
                    if db.execute('SELECT 1 FROM papers LIMIT 1').fetchone() or db.execute('SELECT 1 FROM query_progress LIMIT 1').fetchone():
                        raise ValueError('Full collection requires an unused run directory')
                    db.execute('INSERT INTO full_plan VALUES(1,?)',(CANON(self.plan),))
                    windows=self.plan['openalex_windows']
                    if len(windows)<4 or windows[0]['since']!=self.plan['since'] or windows[-1]['until']!=self.plan['until']:
                        raise ValueError('Incomplete OpenAlex windows')
                    for i in range(1,len(windows)):
                        if date.fromisoformat(windows[i-1]['until']).toordinal()+1!=date.fromisoformat(windows[i]['since']).toordinal():
                            raise ValueError('OpenAlex windows overlap or leave gaps')
                    for n in range(4):
                        assigned=windows[n::4]
                        db.execute('INSERT INTO full_streams(stream_id,source,windows_json,position) VALUES(?,?,?,?)',
                            (f'openalex-{n}','openalex',CANON(assigned),'*'))
                    db.execute('INSERT INTO full_streams(stream_id,source,windows_json,position) VALUES(?,?,?,?)',
                        ('arxiv-0','arxiv',CANON([{'since':self.plan['since'],'until':self.plan['until']}]),'0'))
        finally:db.close()

    def _admit_full(self, record, source):
        published=record.get('published_at') or ''
        sid=record['source_id']
        master=source_connect(self.master)
        try:
            pid,_=upsert_paper(master,{**record,'topics':[]},source,sid,self.plan['openalex_query'] if source=='openalex' else self.plan['arxiv_query'])
            first=master.execute('SELECT first_published FROM papers WHERE paper_id=?',(pid,)).fetchone()[0]
        finally:master.close()
        if first and first[:10]<self.plan['since']:
            # A later-version date may not requalify a pre-window paper.
            db=self._db()
            with db:
                db.execute("UPDATE papers SET scope_state='pending',review_state='pending',reason='before_window' WHERE paper_id=?",(pid,))
                db.execute("UPDATE tasks SET state='failed',reason='before_window',lease_token=NULL WHERE paper_id=? AND state IN ('queued','leased')",(pid,))
                db.execute("UPDATE item_reviews SET state='revoked',approved_json=NULL WHERE paper_id=?",(pid,))
            db.close()
            return 0
        if published and published[:10]>self.plan['until']:
            return 0
        if record.get('abstract'):
            db=self._db()
            existed=bool(db.execute('SELECT 1 FROM papers WHERE paper_id=?',(pid,)).fetchone())
            db.close()
            self.add_source(record,source,self.plan['openalex_query'] if source=='openalex' else self.plan['arxiv_query'],'all_topics')
            return int(not existed)
        db=self._db()
        with db:db.execute('INSERT OR IGNORE INTO full_deferred VALUES(?,?,?)',(source,sid,CANON(record)))
        db.close()
        return 0

    def _consume(self, stream_id):
        db=self._db()
        try:
            stream=db.execute('SELECT * FROM full_streams WHERE stream_id=?',(stream_id,)).fetchone()
            if stream['state']=='done':return False
            windows=json.loads(stream['windows_json'])
            if stream['window_index']>=len(windows):return False
            wi=stream['window_index']; position=stream['position']; source=stream['source']
            page=db.execute('SELECT raw FROM full_pages WHERE stream_id=? AND window_index=? AND position=?',(stream_id,wi,position)).fetchone()
            if page is None:
                if source=='arxiv':
                    delay=max(0,stream['next_request_at']-time.time())
                    if delay:time.sleep(delay)
                    # Reserve pacing before HTTP, including failures and process restarts.
                    with db:db.execute('UPDATE full_streams SET next_request_at=? WHERE stream_id=?',(time.time()+3,stream_id))
                raw=self.fetcher(source,self.plan,windows[wi],position)
                with db:db.execute('INSERT OR IGNORE INTO full_pages(stream_id,window_index,position,raw) VALUES(?,?,?,?)',
                                   (stream_id,wi,position,raw))
            else:raw=page['raw']
            rows,next_position,total=parse_page(source,raw,position,stream['window_received'])
            if len(rows)>self.plan['page_size']:raise ValueError('Oversized source page')
            admitted=0
            for row in rows:
                with self._admission_lock:
                    admitted+=self._admit_full(row,source)
            if next_position is None:
                wi+=1
                next_position='*' if source=='openalex' else '0'
            state='done' if wi==len(windows) else 'open'
            with db:
                db.execute("UPDATE full_pages SET state='applied',count=?,total=? WHERE stream_id=? AND window_index=? AND position=?",
                           (len(rows),total,stream_id,stream['window_index'],position))
                db.execute('''UPDATE full_streams SET window_index=?,position=?,state=?,received=received+?,
                    window_received=?,admitted=admitted+?,error=NULL WHERE stream_id=?''',
                           (wi,str(next_position),state,len(rows),
                            stream['window_received']+len(rows) if wi==stream['window_index'] else 0,
                            admitted,stream_id))
            return state!='done'
        except Exception as exc:
            with db:db.execute('UPDATE full_streams SET state=?,error=? WHERE stream_id=?',
                               ('failed',type(exc).__name__+(' HTTP '+str(exc.code) if isinstance(getattr(exc,'code',None),int) else ''),stream_id))
            raise
        finally:db.close()

    def _stream(self, stream_id):
        while not self._stop_collection.is_set() and self._consume(stream_id):pass

    def _finish_empty(self):
        db=self._db()
        try:
            for source,sid,record_json in db.execute('SELECT source,source_id,record_json FROM full_deferred'):
                master=source_connect(self.master)
                try:
                    r=master.execute('SELECT paper_id FROM source_records WHERE source_name=? AND source_id=?',(source,sid)).fetchone()
                    pid=r[0]
                    first=master.execute('SELECT first_published FROM papers WHERE paper_id=?',(pid,)).fetchone()[0]
                finally:master.close()
                if first and first[:10]<self.plan['since']:continue
                if not db.execute('SELECT 1 FROM papers WHERE paper_id=?',(pid,)).fetchone():
                    self.add_source(json.loads(record_json),source,'all_topics','all_topics')
            with db:db.execute('DELETE FROM full_deferred')
        finally:db.close()

    async def collect(self):
        # One arXiv request lane and four independent OpenAlex cursor lanes.
        # Admission is serialized to respect SQLite's single-writer boundary.
        try:
            self._stop_collection.clear()
            for stream_id in ('openalex-0','openalex-1','openalex-2','openalex-3','arxiv-0'):
                db=self._db()
                with db:db.execute("UPDATE full_streams SET state='open',error=NULL WHERE stream_id=? AND state='failed'",(stream_id,))
                db.close()
            # Each thread has its own SQLite connections; WAL serializes writes.
            await asyncio.gather(*(asyncio.to_thread(self._stream,s) for s in
                ('openalex-0','openalex-1','openalex-2','openalex-3','arxiv-0')))
            await asyncio.to_thread(self._finish_empty)
        except asyncio.CancelledError:
            self._stop_collection.set()
            raise
        except Exception as exc:
            self._stop_collection.set()
            db=self._db()
            with db:db.execute('UPDATE run SET error=? WHERE id=1',('full_collection_failed:'+type(exc).__name__,))
            db.close()
            raise
        db=self._db()
        with db:db.execute("UPDATE run SET collection_state='sealed',error=NULL WHERE id=1")
        db.close()

    def status(self):
        result=super().status()
        db=self._db()
        try:
            result['full_streams']=[dict(r) for r in db.execute('SELECT stream_id,state,window_index,position,received,admitted,error FROM full_streams ORDER BY stream_id')]
        finally:db.close()
        return result
