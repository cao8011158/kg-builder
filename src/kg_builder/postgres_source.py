"""Strong-identifier source records in PostgreSQL; caller owns the transaction."""
from __future__ import annotations

import hashlib
import json
import re

from psycopg import sql
from psycopg.pq import TransactionStatus

from .extraction_validation import validate_extraction_batch
from .storage import NOW, identities, normalise_doi
from .postgres import PreparationStore


class PostgresSourceRepository:
    def __init__(self, store: PreparationStore):
        self.store = store

    def _table(self, name):
        return self.store.source_table(name)

    def _execute(self, conn, query, tables, values=()):
        if conn.info.transaction_status == TransactionStatus.IDLE:
            raise ValueError('Source operations require a caller-owned transaction')
        return conn.execute(sql.SQL(query).format(*(self._table(name) for name in tables)), values)

    def upsert_paper(self, conn, record, source, source_id, query, retrieved_at=None):
        """Resolve only strong identifiers, locking each identity in sorted order.

        PostgreSQL transaction advisory locks serialize overlapping identity sets. Unique
        constraints remain the final arbiter, including concurrent unanticipated links.
        No savepoint or independent commit is introduced into the caller transaction.
        """
        date = retrieved_at or NOW()
        dois, arxiv_id = identities(record)
        doi = normalise_doi(record.get('doi')) or (sorted(dois)[0] if dois else None)
        if not arxiv_id and not doi and not (source and source_id):
            raise ValueError('A stable source identifier is mandatory')
        if not source or not source_id:
            raise ValueError('A stable source identifier is mandatory')
        lock_names = {f'source:{source}:{source_id}'}
        lock_names.update(f'doi:{value}' for value in dois)
        if arxiv_id:
            lock_names.add(f'arxiv:{arxiv_id}')
        for identity in sorted(lock_names):
            # Advisory locks are scoped to this transaction, shared by all writers of
            # this run. A 64-bit digest gives stable keys across processes.
            key = int.from_bytes(hashlib.sha256((self.store.source_schema + ':' + identity).encode()).digest()[:8], 'big', signed=True)
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (key,))
        candidates = set()
        existing_source = self._execute(conn,
            'SELECT paper_id FROM {} WHERE source_name=%s AND source_id=%s',
            ('source_records',), (source, source_id)).fetchone()
        if existing_source:
            candidates.add(existing_source['paper_id'])
        if arxiv_id:
            candidates.update(r['paper_id'] for r in self._execute(conn,
                'SELECT paper_id FROM {} WHERE arxiv_id=%s', ('papers',),(arxiv_id,)))
        for identity in sorted(dois):
            candidates.update(r['paper_id'] for r in self._execute(conn,
                'SELECT paper_id FROM {} WHERE doi=%s',('papers',),(identity,)))
            candidates.update(r['paper_id'] for r in self._execute(conn,
                'SELECT paper_id FROM {} WHERE doi=%s',('paper_doi_aliases',),(identity,)))
        if len(candidates)>1:
            raise ValueError('Strong identifiers resolve to different papers; reconcile offline')
        paper_id = next(iter(candidates)) if candidates else ('arxiv:'+arxiv_id if arxiv_id else ('doi:'+doi if doi else source+':'+str(source_id)))
        existing = self._execute(conn,'SELECT arxiv_id,doi FROM {} WHERE paper_id=%s',('papers',),(paper_id,)).fetchone()
        if existing and existing['arxiv_id'] and arxiv_id and existing['arxiv_id'] != arxiv_id:
            raise ValueError('Conflicting arXiv identities for paper')
        title=record.get('title') or paper_id
        year=int(record['year']) if record.get('year') else None
        first=record.get('published_at')
        if first and re.match(r'^\d{4}-',first):
            year=int(first[:4])
        self._execute(conn,'''INSERT INTO {}(paper_id,title,first_published,doi,arxiv_id,year,created_at,updated_at)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(paper_id) DO UPDATE SET
          title=excluded.title, first_published=CASE
            WHEN papers.first_published IS NULL THEN excluded.first_published
            WHEN excluded.first_published IS NULL THEN papers.first_published
            ELSE LEAST(papers.first_published,excluded.first_published) END,
          doi=COALESCE(papers.doi,excluded.doi), arxiv_id=COALESCE(papers.arxiv_id,excluded.arxiv_id),
          year=CASE WHEN papers.first_published IS NULL THEN excluded.year
            WHEN excluded.first_published IS NOT NULL AND excluded.first_published < papers.first_published
            THEN excluded.year ELSE COALESCE(papers.year,excluded.year) END,
          updated_at=excluded.updated_at''',('papers',),(paper_id,title,first,doi,arxiv_id,year,date,date))
        for identity in sorted(dois | ({doi} if doi else set())):
            result=self._execute(conn,'''INSERT INTO {}(doi,paper_id) VALUES(%s,%s)
                ON CONFLICT(doi) DO UPDATE SET paper_id=excluded.paper_id
                WHERE paper_doi_aliases.paper_id=excluded.paper_id''',('paper_doi_aliases',),(identity,paper_id))
            if result.rowcount != 1:
                raise ValueError('DOI alias already belongs to a different paper')
        label=record.get('version_label')
        if label and not re.fullmatch(r'v[1-9]\d*',str(label)):
            raise ValueError('Confirmed version requires a numeric vN label')
        status='source_confirmed' if label else 'unverified_metadata_snapshot'
        abstract=record.get('abstract') or ''
        snapshot_key=json.dumps([source,source_id,abstract,record.get('url')],ensure_ascii=False)
        version_id=paper_id+'@'+(str(label) if label else 'unverified:'+hashlib.sha256(snapshot_key.encode()).hexdigest()[:20])
        h=hashlib.sha256((title+'\n'+abstract).encode('utf-8')).hexdigest() if abstract else None
        self._execute(conn,'''INSERT INTO {}(version_id,paper_id,version_label,version_status,published_at,source_url,content_hash,abstract,abstract_available,created_at)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(version_id) DO UPDATE SET
          abstract=CASE WHEN length(excluded.abstract)>0 THEN excluded.abstract ELSE paper_versions.abstract END,
          abstract_available=GREATEST(paper_versions.abstract_available,excluded.abstract_available),
          content_hash=CASE WHEN length(excluded.abstract)>0 THEN excluded.content_hash ELSE paper_versions.content_hash END,
          source_url=COALESCE(excluded.source_url,paper_versions.source_url),
          published_at=CASE WHEN paper_versions.published_at IS NULL THEN excluded.published_at
            WHEN excluded.published_at IS NULL THEN paper_versions.published_at
            ELSE LEAST(paper_versions.published_at,excluded.published_at) END''',('paper_versions',),
            (version_id,paper_id,label,status,record.get('version_published_at') or first,
             record.get('url'),h,abstract,int(bool(abstract)),date))
        if label:
            previous=self._execute(conn,'''SELECT v.version_label FROM {} p LEFT JOIN {} v
                  ON v.version_id=p.current_version_id WHERE p.paper_id=%s''',('papers','paper_versions'),(paper_id,)).fetchone()
            prior=previous['version_label'] if previous else None
            if prior is None or (re.fullmatch(r'v[1-9]\d*',prior) and int(label[1:])>int(prior[1:])):
                self._execute(conn,'UPDATE {} SET current_version_id=%s WHERE paper_id=%s',('papers',),(version_id,paper_id))
        payload=json.dumps({'raw':record.get('source_payload'),'normalized':{k:v for k,v in record.items() if k!='source_payload'}},ensure_ascii=False,sort_keys=True)
        self._execute(conn,'''INSERT INTO {}(source_name,source_id,paper_id,url,source_updated_at,retrieved_at,payload_json,payload_hash)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source_name,source_id) DO UPDATE SET
          paper_id=excluded.paper_id,url=excluded.url,
          source_updated_at=COALESCE(excluded.source_updated_at,source_records.source_updated_at),
          retrieved_at=excluded.retrieved_at,
          payload_json=excluded.payload_json,payload_hash=excluded.payload_hash''',('source_records',),
          (source,source_id,paper_id,record.get('url'),record.get('source_updated_at') or record.get('updated_at'),
           date,payload,hashlib.sha256(payload.encode()).hexdigest()))
        for tag in record.get('topics',[]):
            self._execute(conn,'''INSERT INTO {}(paper_id,topic,annotation_basis) VALUES(%s,%s,%s)
                 ON CONFLICT(paper_id,topic) DO NOTHING''',('paper_topics',),
                 (paper_id,tag,record.get('topic_basis','candidate_heuristic')))
        self._execute(conn,'''INSERT INTO {}(paper_id,source_name,query,discovered_at)
            VALUES(%s,%s,%s,%s) ON CONFLICT(paper_id,source_name,query) DO NOTHING''',('discovery_hits',),
            (paper_id,source,query,date))
        return paper_id,version_id

    def ingest_extraction(self, conn, payload):
        return self.ingest_extractions(conn,[payload])[0]

    def ingest_extractions(self, conn, records):
        """Validate all drafts before writes; caller transaction owns atomicity."""
        validate_extraction_batch(records)
        ids=[]
        for payload in records:
            row=self._execute(conn,'''SELECT paper_id,version_label,source_url FROM {}
                WHERE version_id=%s''',('paper_versions',),(payload['version_id'],)).fetchone()
            if not row or row['paper_id'] != payload['paper_id']:
                raise ValueError('Unknown or mismatched paper/version')
            if 'schema_version' in payload:
                pinned=payload['paper_version']
                if pinned['version_label'] != row['version_label'] or pinned['source_url'] != row['source_url']:
                    raise ValueError('Pinned version label/URL differs from owning PaperVersion')
            extractor_type=payload.get('extractor_type','assistant_manual')
            extractor_version=payload.get('extractor_version','2026-10-31-v1')
            key='|'.join((payload['paper_id'],payload['version_id'],extractor_type,extractor_version))
            extraction_id='extr:'+hashlib.sha256(key.encode()).hexdigest()[:24]
            encoded=json.dumps(payload,ensure_ascii=False,allow_nan=False,sort_keys=True)
            scope=payload.get('source_scope','abstract_only')
            status=payload.get('status','draft_requires_review')
            self._execute(conn,'''INSERT INTO {}(extraction_id,paper_id,version_id,extractor_type,
                extractor_version,source_scope,status,payload_json,extracted_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(paper_id,version_id,extractor_type,extractor_version)
                DO UPDATE SET payload_json=excluded.payload_json,
                source_scope=excluded.source_scope,status=excluded.status,
                extracted_at=excluded.extracted_at
                WHERE extraction_records.payload_json IS DISTINCT FROM excluded.payload_json
                   OR extraction_records.source_scope IS DISTINCT FROM excluded.source_scope
                   OR extraction_records.status IS DISTINCT FROM excluded.status''',('extraction_records',),
                (extraction_id,payload['paper_id'],payload['version_id'],extractor_type,
                 extractor_version,scope,status,encoded,NOW()))
            ids.append(extraction_id)
        return ids
