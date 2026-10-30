"""SQLite source store and versioned extraction store (no Neo4j writes)."""
from __future__ import annotations
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

NOW = lambda: datetime.now(timezone.utc).isoformat(timespec='seconds')

def connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute('PRAGMA foreign_keys = ON')
    con.execute('PRAGMA journal_mode = WAL')
    con.executescript('''
    CREATE TABLE IF NOT EXISTS papers (
      paper_id TEXT PRIMARY KEY, title TEXT NOT NULL, first_published TEXT,
      doi TEXT, arxiv_id TEXT, year INTEGER, current_version_id TEXT,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_doi ON papers(doi) WHERE doi IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_arxiv ON papers(arxiv_id) WHERE arxiv_id IS NOT NULL;
    CREATE TABLE IF NOT EXISTS paper_versions (
      version_id TEXT PRIMARY KEY, paper_id TEXT NOT NULL REFERENCES papers(paper_id),
      version_label TEXT, version_status TEXT NOT NULL,
      published_at TEXT, source_url TEXT, content_hash TEXT,
      abstract TEXT, abstract_available INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL, UNIQUE(paper_id, version_label, version_status)
    );
    CREATE TABLE IF NOT EXISTS source_records (
      source_name TEXT NOT NULL, source_id TEXT NOT NULL, paper_id TEXT NOT NULL REFERENCES papers(paper_id),
      url TEXT, source_updated_at TEXT, retrieved_at TEXT NOT NULL,
      payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
      PRIMARY KEY(source_name, source_id)
    );
    CREATE TABLE IF NOT EXISTS paper_topics (
      paper_id TEXT NOT NULL REFERENCES papers(paper_id), topic TEXT NOT NULL,
      annotation_basis TEXT NOT NULL, PRIMARY KEY(paper_id,topic)
    );
    CREATE TABLE IF NOT EXISTS discovery_hits (
      paper_id TEXT NOT NULL REFERENCES papers(paper_id), source_name TEXT NOT NULL,
      query TEXT NOT NULL, discovered_at TEXT NOT NULL,
      PRIMARY KEY(paper_id,source_name,query)
    );
    CREATE TABLE IF NOT EXISTS extraction_records (
      extraction_id TEXT PRIMARY KEY,
      paper_id TEXT NOT NULL REFERENCES papers(paper_id),
      version_id TEXT NOT NULL REFERENCES paper_versions(version_id),
      extractor_type TEXT NOT NULL, extractor_version TEXT NOT NULL,
      source_scope TEXT NOT NULL, status TEXT NOT NULL,
      payload_json TEXT NOT NULL, extracted_at TEXT NOT NULL,
      UNIQUE(paper_id,version_id,extractor_type,extractor_version)
    );
    CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_id TEXT PRIMARY KEY, mode TEXT NOT NULL, started_at TEXT NOT NULL,
      completed_at TEXT, records_seen INTEGER DEFAULT 0, records_upserted INTEGER DEFAULT 0,
      status TEXT NOT NULL, error TEXT
    );
    ''')
    con.commit()
    return con

def normalise_doi(doi):
    if not doi: return None
    doi = re.sub(r'^(https?://(dx\.)?doi\.org/|doi:)', '', str(doi).strip(), flags=re.I).lower()
    return doi or None

def arxiv_norm(arxiv):
    if not arxiv: return None
    x = re.sub(r'^(https?://arxiv\.org/(abs|pdf)/|arxiv:)', '', str(arxiv), flags=re.I)
    return re.sub(r'v\d+$', '', x).removesuffix('.pdf')

def upsert_paper(con, record, source, source_id, query, retrieved_at=None):
    """Strong-ID dedup only; title similarity deliberately never auto-merges papers."""
    date = retrieved_at or NOW()
    arxiv_id = arxiv_norm(record.get('arxiv_id'))
    doi = normalise_doi(record.get('doi'))
    if not arxiv_id and not doi and not (source and source_id):
        raise ValueError('A stable source identifier is mandatory')
    if arxiv_id:
        found = con.execute('SELECT paper_id FROM papers WHERE arxiv_id=?', (arxiv_id,)).fetchone()
    else: found = None
    if not found and doi:
        found = con.execute('SELECT paper_id FROM papers WHERE doi=?', (doi,)).fetchone()
    paper_id = found[0] if found else ('arxiv:' + arxiv_id if arxiv_id else ('doi:' + doi if doi else source+':'+source_id))
    title = record.get('title') or paper_id
    year = int(record['year']) if record.get('year') else None
    first = record.get('published_at')
    with con:
        con.execute('''INSERT INTO papers(paper_id,title,first_published,doi,arxiv_id,year,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(paper_id) DO UPDATE SET
            title=excluded.title, first_published=COALESCE(papers.first_published,excluded.first_published),
            doi=COALESCE(papers.doi,excluded.doi), arxiv_id=COALESCE(papers.arxiv_id,excluded.arxiv_id),
            year=COALESCE(papers.year,excluded.year), updated_at=excluded.updated_at''',
            (paper_id,title,first,doi,arxiv_id,year,date,date))
        label = record.get('version_label')
        status = 'source_confirmed' if label else 'unverified_metadata_snapshot'
        version_id = paper_id + '@' + (str(label) if label else 'unverified')
        abstract = record.get('abstract') or ''
        # Hash only a genuine abstract, never a generated summary or fabricated full text.
        h = hashlib.sha256((title+'\n'+abstract).encode('utf-8')).hexdigest() if abstract else None
        con.execute('''INSERT INTO paper_versions(version_id,paper_id,version_label,version_status,published_at,source_url,content_hash,abstract,abstract_available,created_at)
          VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(version_id) DO UPDATE SET
          abstract=CASE WHEN length(excluded.abstract)>0 THEN excluded.abstract ELSE paper_versions.abstract END,
          abstract_available=MAX(paper_versions.abstract_available,excluded.abstract_available),
          content_hash=CASE WHEN length(excluded.abstract)>0 THEN excluded.content_hash ELSE paper_versions.content_hash END,
          source_url=COALESCE(excluded.source_url,paper_versions.source_url)''',
          (version_id,paper_id,label,status,first,record.get('url'),h,abstract,int(bool(abstract)),date))
        if label:
            con.execute('UPDATE papers SET current_version_id=? WHERE paper_id=?', (version_id,paper_id))
        # Preserve the upstream API's original record when available.
        # Also retain normalized fields for reproducibility.
        payload = json.dumps({'raw': record.get('source_payload'), 'normalized': {k: v for k, v in record.items() if k != 'source_payload'}}, ensure_ascii=False, sort_keys=True)
        con.execute('''INSERT INTO source_records(source_name,source_id,paper_id,url,retrieved_at,payload_json,payload_hash)
          VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_name,source_id) DO UPDATE SET
          paper_id=excluded.paper_id,url=excluded.url,retrieved_at=excluded.retrieved_at,
          payload_json=excluded.payload_json,payload_hash=excluded.payload_hash''',
          (source,source_id,paper_id,record.get('url'),date,payload,hashlib.sha256(payload.encode()).hexdigest()))
        for tag in record.get('topics',[]):
            con.execute('INSERT OR IGNORE INTO paper_topics VALUES(?,?,?)',(paper_id,tag,record.get('topic_basis','candidate_heuristic')))
        con.execute('INSERT OR IGNORE INTO discovery_hits VALUES(?,?,?,?)',(paper_id,source,query,date))
    return paper_id, version_id

def ingest_extraction(con, payload):
    """Store independently reviewed/assistant-curated structured content; never call an LLM."""
    required = {'paper_id','version_id','summary','methods','research_problems','findings','evidence_locators'}
    missing = required - payload.keys()
    if missing: raise ValueError('Missing extraction fields: '+', '.join(sorted(missing)))
    p = con.execute('SELECT paper_id FROM paper_versions WHERE version_id=?',(payload['version_id'],)).fetchone()
    if not p or p[0] != payload['paper_id']: raise ValueError('Unknown or mismatched paper/version')
    for loc in payload['evidence_locators']:
        if not loc.get('source_url') or not loc.get('section'):
            raise ValueError('Evidence locator must contain source_url and section')
    key = '|'.join([payload['paper_id'], payload['version_id'], payload.get('extractor_type','assistant_manual'), payload.get('extractor_version','2026-10-31-v1')])
    extraction_id = 'extr:' + hashlib.sha256(key.encode('utf-8')).hexdigest()[:24]
    with con:
        con.execute('''INSERT INTO extraction_records VALUES(?,?,?,?,?,?,?,?,?)
          ON CONFLICT(paper_id,version_id,extractor_type,extractor_version) DO UPDATE SET
          payload_json=excluded.payload_json,status=excluded.status,extracted_at=excluded.extracted_at''',
          (extraction_id,payload['paper_id'],payload['version_id'],
           payload.get('extractor_type','assistant_manual'), payload.get('extractor_version','2026-10-31-v1'),
           payload.get('source_scope','abstract_only'),payload.get('status','draft_requires_review'),
           json.dumps(payload,ensure_ascii=False),NOW()))
    return extraction_id
