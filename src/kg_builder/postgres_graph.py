"""Native PostgreSQL graph, semantic, and frozen source persistence."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import uuid
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from .postgres import SOURCE_DDL
SCHEMA_REVISION = 'graph-postgres/1'

GRAPH_DDL = '''
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS graph_outbox(event_id TEXT PRIMARY KEY,kind TEXT NOT NULL,key TEXT NOT NULL,payload_json TEXT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS graph_outbox_pending ON graph_outbox(delivered,event_id);
        CREATE TABLE IF NOT EXISTS stage(name TEXT PRIMARY KEY,seconds DOUBLE PRECISION NOT NULL);
        CREATE TABLE IF NOT EXISTS jev_requests(request_id TEXT PRIMARY KEY,request_duration DOUBLE PRECISION,total_elapsed DOUBLE PRECISION,attempts INTEGER,model TEXT,usage_json TEXT,started_at DOUBLE PRECISION,finished_at DOUBLE PRECISION,http_intervals_json TEXT);
        CREATE TABLE IF NOT EXISTS candidates(pair_id TEXT PRIMARY KEY,kind TEXT NOT NULL,left_id TEXT NOT NULL,right_id TEXT NOT NULL,similarity DOUBLE PRECISION NOT NULL,decision TEXT NOT NULL DEFAULT 'unjudged',audit TEXT);
        CREATE INDEX IF NOT EXISTS candidate_decisions ON candidates(decision,kind,pair_id);
        CREATE TABLE IF NOT EXISTS groups(group_id TEXT PRIMARY KEY,kind TEXT NOT NULL,members_json TEXT NOT NULL,name TEXT,description TEXT,review TEXT NOT NULL DEFAULT 'unreviewed',issues_json TEXT,audit TEXT,name_review TEXT NOT NULL DEFAULT 'unreviewed',name_audit TEXT);
        CREATE TABLE IF NOT EXISTS member_reviews(group_id TEXT NOT NULL,mention_id TEXT NOT NULL,excluded INTEGER NOT NULL,audit TEXT NOT NULL,PRIMARY KEY(group_id,mention_id));
        CREATE INDEX IF NOT EXISTS group_review ON groups(review,kind,group_id);
        CREATE TABLE IF NOT EXISTS tasks(task_id TEXT PRIMARY KEY,group_id TEXT UNIQUE NOT NULL, status TEXT NOT NULL,token TEXT,expires DOUBLE PRECISION,worker TEXT,attempts INTEGER NOT NULL DEFAULT 0,result_json TEXT,reason TEXT,queued_at DOUBLE PRECISION,claimed_at DOUBLE PRECISION);
        CREATE INDEX IF NOT EXISTS task_claim ON tasks(status,expires,task_id);
        CREATE INDEX IF NOT EXISTS candidate_judgment_queue ON candidates(decision,pair_id);
'''
SEMANTIC_DDL = '''
        CREATE TABLE IF NOT EXISTS sources (
          extraction_id TEXT PRIMARY KEY, paper_id TEXT NOT NULL, version_id TEXT NOT NULL,
          title TEXT NOT NULL, source_url TEXT, version_label TEXT, source_hash TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS sources_paper ON sources(paper_id,version_id);
        CREATE TABLE IF NOT EXISTS mentions (
          mention_id TEXT PRIMARY KEY, extraction_id TEXT NOT NULL REFERENCES sources(extraction_id) ON DELETE CASCADE,
          kind TEXT NOT NULL, item_key TEXT NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL,
          role_json TEXT NOT NULL, version_or_subset TEXT, source_json TEXT NOT NULL,
          mention_hash TEXT NOT NULL, entity_id TEXT, decision TEXT NOT NULL DEFAULT 'pending',
          UNIQUE(extraction_id,item_key));
        CREATE INDEX IF NOT EXISTS mentions_entity ON mentions(entity_id,mention_id);
        CREATE INDEX IF NOT EXISTS mentions_source ON mentions(extraction_id,mention_id);
        CREATE INDEX IF NOT EXISTS mentions_pending ON mentions(kind,decision,mention_id);
        CREATE TABLE IF NOT EXISTS contributions (
          contribution_id TEXT PRIMARY KEY, extraction_id TEXT NOT NULL REFERENCES sources(extraction_id) ON DELETE CASCADE,
          item_key TEXT NOT NULL, description TEXT NOT NULL, source_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS contributions_source ON contributions(extraction_id);
        CREATE TABLE IF NOT EXISTS entities (
          entity_id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL,
          name_origin TEXT NOT NULL, aliases_json TEXT NOT NULL, revision TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS entities_kind ON entities(kind,entity_id);
        CREATE INDEX IF NOT EXISTS entities_name ON entities(kind,name,entity_id);
        CREATE TABLE IF NOT EXISTS entity_surfaces (
          kind TEXT NOT NULL, normalized_name TEXT NOT NULL,
          entity_id TEXT NOT NULL REFERENCES entities(entity_id),
          PRIMARY KEY(kind,normalized_name,entity_id));
        CREATE TABLE IF NOT EXISTS reviews (review_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS embedding_cache (
          model_id TEXT NOT NULL, text_hash TEXT NOT NULL, vector_json TEXT NOT NULL,
          PRIMARY KEY(model_id,text_hash));
        CREATE TABLE IF NOT EXISTS entity_fts (entity_id TEXT PRIMARY KEY,name TEXT NOT NULL,description TEXT NOT NULL,aliases TEXT NOT NULL,search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple',name || ' ' || description || ' ' || aliases)) STORED); CREATE INDEX IF NOT EXISTS entity_fts_search ON entity_fts USING GIN(search_vector);
        CREATE TABLE IF NOT EXISTS contribution_fts (contribution_id TEXT PRIMARY KEY,description TEXT NOT NULL,search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple',description)) STORED); CREATE INDEX IF NOT EXISTS contribution_fts_search ON contribution_fts USING GIN(search_vector);
        CREATE TABLE IF NOT EXISTS research_areas (
          area_id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, revision TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS research_area_members (
          area_id TEXT NOT NULL REFERENCES research_areas(area_id) ON DELETE CASCADE,
          mention_id TEXT NOT NULL REFERENCES mentions(mention_id) ON DELETE CASCADE,
          PRIMARY KEY(area_id,mention_id));
        CREATE INDEX IF NOT EXISTS research_area_members_mention ON research_area_members(mention_id,area_id);
        CREATE TABLE IF NOT EXISTS research_area_fts (area_id TEXT PRIMARY KEY,name TEXT NOT NULL,description TEXT NOT NULL,search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple',name || ' ' || description)) STORED); CREATE INDEX IF NOT EXISTS research_area_fts_search ON research_area_fts USING GIN(search_vector);
        '''

SCHEMA_FINGERPRINT = hashlib.sha256((SCHEMA_REVISION+'\0'+SOURCE_DDL+'\0'+GRAPH_DDL+'\0'+SEMANTIC_DDL).encode()).hexdigest()


def graph_run_identity(job_dir, run_id=None):
    """Persist a location independent logical identity; no credentials enter disk."""
    directory = Path(job_dir)
    directory.mkdir(parents=True, exist_ok=True)
    descriptor = directory / 'graph-job.json'
    if run_id is not None and (not isinstance(run_id,str) or not run_id.strip()):
        raise ValueError('Nonempty graph run_id required')
    if not descriptor.exists():
        if any((directory / name).exists() for name in ('graph.sqlite','semantic.sqlite','snapshot-master.sqlite')):
            raise ValueError('SQLite graph job cannot be resumed; use a fresh PostgreSQL job directory')
        value = {'run_id':run_id or str(uuid.uuid4()), 'backend':'postgresql', 'revision':SCHEMA_REVISION}
        temporary = directory / ('.graph-job-'+uuid.uuid4().hex+'.tmp')
        try:
            with temporary.open('x',encoding='utf-8') as stream:
                json.dump(value,stream,sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            # Hard-link publishes complete bytes atomically without overwriting a winner.
            try:os.link(temporary,descriptor)
            except FileExistsError:pass
        finally:temporary.unlink(missing_ok=True)
    try:value=json.loads(descriptor.read_text(encoding='utf-8'))
    except (ValueError,OSError):raise ValueError('Invalid graph job descriptor') from None
    if not isinstance(value,dict) or value.get('backend')!='postgresql' or value.get('revision')!=SCHEMA_REVISION or not isinstance(value.get('run_id'),str) or not value['run_id'].strip():
        raise ValueError('Invalid or incompatible graph job descriptor')
    if run_id is not None and run_id!=value['run_id']:
        raise ValueError('Graph job run_id cannot be changed')
    return value['run_id']


class GraphStore:
    def __init__(self, run_id, database_url=None, *, readonly=False, initialize=True):
        if not isinstance(run_id,str) or not run_id.strip():raise ValueError('Nonempty graph run_id required')
        self.run_id=run_id
        self.schema='kg_graph_'+hashlib.sha256(run_id.encode()).hexdigest()[:24]
        self.source_schema=self.schema
        self.database_url=database_url or os.getenv('KG_GRAPH_DATABASE_URL') or os.getenv('KG_DATABASE_URL') or os.getenv('DATABASE_URL')
        if not self.database_url:raise ValueError('PostgreSQL URL required: use KG_GRAPH_DATABASE_URL/KG_DATABASE_URL/DATABASE_URL')
        self.db=self._open(False)
        try:
            if initialize and not readonly:self.initialize()
            elif not self._ready(self.db):raise ValueError('PostgreSQL graph schema is not initialized')
            self._scope(self.db,readonly)
        except psycopg.Error:
            self.db.close()
            raise RuntimeError('PostgreSQL graph initialization unavailable') from None
        except BaseException:
            self.db.close()
            raise

    def _open(self,readonly):
        try:return psycopg.connect(self.database_url,autocommit=True,row_factory=dict_row)
        except (psycopg.Error,ValueError,OSError):raise RuntimeError('PostgreSQL graph connection unavailable') from None

    def _scope(self,conn,readonly):
        conn.execute(sql.SQL('SET search_path TO {}, pg_catalog').format(sql.Identifier(self.schema)))
        if readonly:conn.execute('SET default_transaction_read_only=on')

    def connect(self,readonly=False):
        conn=self._open(readonly)
        try:self._scope(conn,readonly)
        except BaseException:
            conn.close()
            raise
        return conn

    def source_table(self,name):
        if name not in ('papers','paper_doi_aliases','paper_versions','source_records','paper_topics','discovery_hits','extraction_records'):raise ValueError('Unknown source table')
        return sql.SQL('{}.{}').format(sql.Identifier(self.schema),sql.Identifier(name))

    def _ready(self,conn):
        row=conn.execute('SELECT to_regclass(%s) AS name',(self.schema+'.schema_bootstrap',)).fetchone()
        if row['name'] is None:return False
        row=conn.execute(sql.SQL('SELECT * FROM {}.schema_bootstrap WHERE id=1').format(sql.Identifier(self.schema))).fetchone()
        if not row or row['run_id']!=self.run_id or row['revision']!=SCHEMA_REVISION or row['ddl_fingerprint']!=SCHEMA_FINGERPRINT:
            raise ValueError('PostgreSQL graph schema identity/version differs; use a fresh job or explicit migration')
        return True

    def initialize(self):
        with self.db.transaction():
            if self._ready(self.db):return
            key=int.from_bytes(hashlib.sha256(('schema:'+self.schema).encode()).digest()[:8],'big',signed=True)
            self.db.execute('SELECT pg_advisory_xact_lock(%s)',(key,))
            if self._ready(self.db):return
            exists=self.db.execute('SELECT 1 FROM pg_namespace WHERE nspname=%s',(self.schema,)).fetchone()
            if exists:raise ValueError('Unrecognized PostgreSQL graph schema; use a fresh job or explicit migration')
            self.db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
            self.db.execute(sql.SQL('SET LOCAL search_path TO {}, pg_catalog').format(sql.Identifier(self.schema)))
            for ddl in (SOURCE_DDL,GRAPH_DDL,SEMANTIC_DDL):
                for statement in ddl.split(';'):
                    if statement.strip():self.db.execute(statement)
            self.db.execute('CREATE TABLE schema_bootstrap(id INTEGER PRIMARY KEY CHECK(id=1),run_id TEXT NOT NULL,revision TEXT NOT NULL,ddl_fingerprint TEXT NOT NULL)')
            self.db.execute('INSERT INTO schema_bootstrap VALUES(1,%s,%s,%s)',(self.run_id,SCHEMA_REVISION,SCHEMA_FINGERPRINT))

    def close(self):self.db.close()
    def __enter__(self):return self
    def __exit__(self,*_):self.close()
