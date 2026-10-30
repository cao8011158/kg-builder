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
    CREATE TABLE IF NOT EXISTS paper_doi_aliases (
      doi TEXT PRIMARY KEY, paper_id TEXT NOT NULL REFERENCES papers(paper_id)
    );
    CREATE INDEX IF NOT EXISTS idx_doi_alias_paper ON paper_doi_aliases(paper_id);
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
    doi = doi.split('?', 1)[0].split('#', 1)[0]
    return doi if re.fullmatch(r'10\.\d{4,9}/[^\s<>"?#]+', doi) else None

def arxiv_norm(arxiv):
    if not arxiv: return None
    x = str(arxiv).strip()
    x = re.sub(r'^(https?://(export\.)?arxiv\.org/(abs|pdf)/|arxiv:)', '', x, flags=re.I)
    x = x.split('?', 1)[0].split('#', 1)[0]
    x = re.sub(r'\.pdf$', '', x, flags=re.I)
    x = re.sub(r'v[1-9]\d*$', '', x, flags=re.I)
    if re.fullmatch(r'\d{4}\.\d{4,5}',x):
        return x if 1 <= int(x[2:4]) <= 12 else None
    return x.lower() if re.fullmatch(r'[a-z][a-z0-9.\-]+(?:\.[A-Z]{2})?/\d{7}',x,flags=re.I) else None


def identities(record):
    """Only explicit, valid scholarly identifiers; no title or arbitrary URL guesses."""
    raw = record.get('source_payload') or {}
    external = (raw.get('externalIds') or {}) if isinstance(raw, dict) else {}
    raw_ids = (raw.get('ids') or {}) if isinstance(raw, dict) else {}
    primary = (raw.get('primary_location') or {}) if isinstance(raw, dict) else {}
    pdf = (raw.get('openAccessPdf') or {}) if isinstance(raw, dict) else {}
    urls = (record.get('url'), primary.get('landing_page_url'), pdf.get('url'))
    dois = {x for x in (normalise_doi(v) for v in
            (record.get('doi'), external.get('DOI'), raw_ids.get('doi'), *urls)) if x}
    arxivs = {x for x in (arxiv_norm(v) for v in
              (record.get('arxiv_id'), external.get('ArXiv'), raw_ids.get('arxiv'),
               record.get('source_id'), *urls)) if x}
    arxivs.update(a for a in (arxiv_norm(x.removeprefix('10.48550/arxiv.'))
                  for x in dois if x.startswith('10.48550/arxiv.')) if a)
    if len(arxivs) > 1:
        raise ValueError('Conflicting arXiv identities in one source record')
    return dois, next(iter(arxivs), None)


def preferred_metadata_version(con, paper_id):
    """Best metadata snapshot for display; it is not a confirmed current version."""
    return con.execute('''SELECT version_id FROM paper_versions WHERE paper_id=?
        ORDER BY CASE WHEN version_status='source_confirmed' THEN 0 ELSE 1 END,
        CAST(SUBSTR(version_label, 2) AS INTEGER) DESC, created_at DESC LIMIT 1''',
        (paper_id,)).fetchone()

def upsert_paper(con, record, source, source_id, query, retrieved_at=None):
    """Strong-ID dedup only; title similarity deliberately never auto-merges papers."""
    date = retrieved_at or NOW()
    dois, arxiv_id = identities(record)
    doi = normalise_doi(record.get('doi')) or (sorted(dois)[0] if dois else None)
    if not arxiv_id and not doi and not (source and source_id):
        raise ValueError('A stable source identifier is mandatory')
    with con:
        candidates = set()
        if source and source_id:
            candidates.update(r[0] for r in con.execute(
                'SELECT paper_id FROM source_records WHERE source_name=? AND source_id=?', (source,source_id)))
        if arxiv_id:
            candidates.update(r[0] for r in con.execute('SELECT paper_id FROM papers WHERE arxiv_id=?', (arxiv_id,)))
        for identity in dois:
            candidates.update(r[0] for r in con.execute('SELECT paper_id FROM papers WHERE doi=?', (identity,)))
            candidates.update(r[0] for r in con.execute('SELECT paper_id FROM paper_doi_aliases WHERE doi=?', (identity,)))
        if len(candidates) > 1:
            raise ValueError('Strong identifiers resolve to different papers; reconcile offline')
        paper_id = next(iter(candidates)) if candidates else ('arxiv:' + arxiv_id if arxiv_id else ('doi:' + doi if doi else source+':'+str(source_id)))
        existing = con.execute('SELECT arxiv_id FROM papers WHERE paper_id=?', (paper_id,)).fetchone()
        if existing and existing[0] and arxiv_id and existing[0] != arxiv_id:
            raise ValueError('Conflicting arXiv identities for paper')
        title = record.get('title') or paper_id
        year = int(record['year']) if record.get('year') else None
        first = record.get('published_at')
        if first and re.match(r'^\d{4}-',first):
            year = int(first[:4])
        con.execute('''INSERT INTO papers(paper_id,title,first_published,doi,arxiv_id,year,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(paper_id) DO UPDATE SET
            title=excluded.title, first_published=CASE
              WHEN papers.first_published IS NULL THEN excluded.first_published
              WHEN excluded.first_published IS NULL THEN papers.first_published
              ELSE MIN(papers.first_published,excluded.first_published) END,
            doi=COALESCE(papers.doi,excluded.doi), arxiv_id=COALESCE(papers.arxiv_id,excluded.arxiv_id),
            year=CASE WHEN papers.first_published IS NULL THEN excluded.year
              WHEN excluded.first_published IS NOT NULL AND excluded.first_published < papers.first_published
              THEN excluded.year ELSE COALESCE(papers.year,excluded.year) END,
            updated_at=excluded.updated_at''',
            (paper_id,title,first,doi,arxiv_id,year,date,date))
        # A publication DOI and an arXiv DOI may legitimately name the same work.
        for identity in dois:
            con.execute('INSERT OR IGNORE INTO paper_doi_aliases VALUES(?,?)', (identity,paper_id))
        if doi:
            con.execute('INSERT OR IGNORE INTO paper_doi_aliases VALUES(?,?)', (doi,paper_id))
        label = record.get('version_label')
        if label and not re.fullmatch(r'v[1-9]\d*', str(label)):
            raise ValueError('Confirmed version requires a numeric vN label')
        status = 'source_confirmed' if label else 'unverified_metadata_snapshot'
        abstract = record.get('abstract') or ''
        snapshot_key = json.dumps([source,source_id,abstract,record.get('url')],ensure_ascii=False)
        version_id = paper_id + '@' + (str(label) if label else
                     'unverified:' + hashlib.sha256(snapshot_key.encode()).hexdigest()[:20])
        # Hash only a genuine abstract, never a generated summary or fabricated full text.
        h = hashlib.sha256((title+'\n'+abstract).encode('utf-8')).hexdigest() if abstract else None
        con.execute('''INSERT INTO paper_versions(version_id,paper_id,version_label,version_status,published_at,source_url,content_hash,abstract,abstract_available,created_at)
          VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(version_id) DO UPDATE SET
          abstract=CASE WHEN length(excluded.abstract)>0 THEN excluded.abstract ELSE paper_versions.abstract END,
          abstract_available=MAX(paper_versions.abstract_available,excluded.abstract_available),
          content_hash=CASE WHEN length(excluded.abstract)>0 THEN excluded.content_hash ELSE paper_versions.content_hash END,
          source_url=COALESCE(excluded.source_url,paper_versions.source_url),
          published_at=CASE WHEN paper_versions.published_at IS NULL THEN excluded.published_at
            WHEN excluded.published_at IS NULL THEN paper_versions.published_at
            ELSE MIN(paper_versions.published_at,excluded.published_at) END''',
          (version_id,paper_id,label,status,record.get('version_published_at') or first,
           record.get('url'),h,abstract,int(bool(abstract)),date))
        if label:
            previous = con.execute('''SELECT v.version_label FROM papers p
                LEFT JOIN paper_versions v ON v.version_id=p.current_version_id
                WHERE p.paper_id=?''',(paper_id,)).fetchone()
            prior = previous[0] if previous else None
            if prior is None or (re.fullmatch(r'v[1-9]\d*',prior) and int(label[1:]) > int(prior[1:])):
                con.execute('UPDATE papers SET current_version_id=? WHERE paper_id=?',(version_id,paper_id))
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
