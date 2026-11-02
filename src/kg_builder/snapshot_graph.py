"""Durable graph construction from a verified, frozen preparation snapshot."""
from __future__ import annotations

import hashlib
import json
import math
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from pathlib import Path

from .extraction_validation import validate_extraction, RETRIEVAL_SCHEMA_VERSION
from .semantic_store import SemanticStore, digest, REVIEW_VERSION, KINDS
from .postgres_graph import GraphStore, graph_run_identity
from .postgres_source import PostgresSourceRepository
from psycopg.pq import TransactionStatus
from .vector_index import HNSWIndex
from .problem_clustering import cluster_problem_vectors

SCHEMA = 'research-kg-prepared-snapshot/1'
FIELDS = {v: k for k, v in KINDS.items()}


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def read_snapshot(path):
    raw = Path(path).read_bytes()
    snapshot = json.loads(raw)
    if snapshot.get('schema_version') != SCHEMA:
        raise ValueError('Unknown prepared snapshot schema')
    claimed = snapshot.get('snapshot_hash')
    if not isinstance(claimed, str) or canonical_hash({k: v for k, v in snapshot.items() if k != 'snapshot_hash'}) != claimed:
        raise ValueError('Prepared snapshot digest mismatch')
    if not isinstance(snapshot.get('papers'), list) or not isinstance(snapshot.get('extractions'), list) or not isinstance(snapshot.get('vectors'), list):
        raise ValueError('Prepared snapshot missing source arrays')
    policy = snapshot.get('config', {}).get('review_policy', 'legacy-paper')
    if policy not in ('legacy-paper', 'item-v1'):
        raise ValueError('Unknown snapshot review policy')
    metadata = snapshot.get('review_metadata')
    if policy == 'item-v1' and not isinstance(metadata, dict):
        raise ValueError('Item-reviewed snapshot missing review metadata')
    fields = ('research_problems', 'methods', 'benchmarks', 'contributions')
    for wrapper in snapshot['extractions']:
        extraction = _extraction(wrapper)
        if extraction.get('status') != 'reviewed':
            raise ValueError('Unreviewed extraction snapshot')
        if policy == 'item-v1':
            paper_id = extraction.get('paper_id')
            review = metadata.get(paper_id)
            if not isinstance(review, dict) or review.get('policy') != 'item-v1' or not isinstance(review.get('items'), list):
                raise ValueError('Item-reviewed extraction missing provenance')
            approved = {}
            for item in review['items']:
                if not isinstance(item, dict) or item.get('field') not in fields or not isinstance(item.get('key'), str):
                    raise ValueError('Malformed item review provenance')
                if item.get('state') == 'approved':
                    identity=(item['field'], item['key'])
                    if identity in approved:raise ValueError('Duplicate approved item provenance')
                    if not isinstance(item.get('approved_payload_hash'),str):
                        raise ValueError('Approved item missing payload fingerprint')
                    approved[identity]=item['approved_payload_hash']
            actual = [(field, item.get('key')) for field in fields for item in extraction.get(field, [])]
            if len(set(actual)) != len(actual) or set(actual) != set(approved):
                raise ValueError('Snapshot extraction differs from approved items')
            for field in fields:
                for item in extraction.get(field, []):
                    expected=approved[(field,item['key'])]
                    if expected != canonical_hash(item):
                        raise ValueError('Snapshot approved item payload differs from review')
    return snapshot, hashlib.sha256(raw).hexdigest()


def _extraction(row):
    return row.get('extraction', row.get('payload', row))


def _paper(papers, extraction):
    p = papers.get(extraction['paper_id'])
    if p is None:
        raise ValueError('Missing paper source')
    version = p.get('paper_version', p)
    pinned = {key:p.get(key) for key in ('paper_id','version_id','title','text')}
    pinned['version_label'] = version.get('version_label')
    pinned['source_url'] = version.get('source_url')
    if p.get('source_hash') != canonical_hash(pinned):
        raise ValueError('Frozen source fingerprint mismatch')
    if p.get('version_id', extraction['version_id']) != extraction['version_id']:
        raise ValueError('Paper version pin mismatch')
    if version.get('version_label', extraction['paper_version']['version_label']) != extraction['paper_version']['version_label'] or version.get('source_url', extraction['paper_version']['source_url']) != extraction['paper_version']['source_url']:
        raise ValueError('Paper URL/label pin mismatch')
    return p


def domain_group_projection(group, audits, mention_rows, model_id):
    """Pure bounded projection shared by incremental graph and PostgreSQL retrieval."""
    entities=[];assignments=[];areas=[];memberships=[]
    mids=json.loads(group['members_json']);kind=group['kind']
    if kind!='ResearchProblem' or len(mids)>=2:
        if set(audits)!=set(mids):raise ValueError('Missing member review before materialization')
    def add_entity(selected):
        selected=sorted(selected);representative=mention_rows[selected[0]]
        eid='entity:'+digest([kind,selected])[:32]
        name=representative['name']
        entities.append({'entity_id':eid,'kind':kind,'name':name,'description':representative['description'],
                         'aliases':sorted({mention_rows[mid]['name'] for mid in selected}-{name}),'name_origin':'source_authored'})
        for mid in selected:
            m=mention_rows[mid]
            assignments.append({'mention_id':mid,'model_id':model_id,
                'precondition':digest([mid,m['source_hash'],m['mention_hash'],model_id]),
                'candidate_revisions':{},'decision':'new_entity','entity_id':eid})
    kept=[mid for mid in mids if not audits.get(mid,0)]
    if kind=='ResearchProblem':
        for mid in mids:add_entity([mid])
        if len(mids)>=2 and kept:
            area_id='area:'+digest([group['group_id']])[:32]
            areas.append({'area_id':area_id,'name':group['name'],'description':group['description']})
            memberships.extend({'area_id':area_id,'mention_id':mid} for mid in kept)
    else:
        if kept:add_entity(kept)
        for mid in mids:
            if audits[mid]:add_entity([mid])
    return entities,assignments,areas,memberships


class SnapshotGraph:
    def __init__(self, snapshot_path, job_dir, **options):
        try:
            self._initialize(snapshot_path, job_dir, **options)
        except BaseException:
            if hasattr(self, 'graph_store'):self.graph_store.close()
            raise

    def _initialize(self, snapshot_path, job_dir, *, client=None, k=20, batch_size=32,
                 name_mode='external', model=None, lease_seconds=1800, namer=None, name_base_url=None, name_model=None, read_input=True, embedding_cache_dir=None, name_concurrency=3, graph_policy=None, problem_resolution=None, context_format=None, entity_same_min_probability=None, publisher=None, voyage_options=None, run_id=None, database_url=None):
        self.voyage_options = voyage_options or {}
        self.publisher = publisher
        self.snapshot_path = Path(snapshot_path).resolve()
        self.job_dir = Path(job_dir).resolve()
        self.job_dir.mkdir(parents=True, exist_ok=True)
        stat = self.snapshot_path.stat()
        stamp = f'{stat.st_size}:{stat.st_mtime_ns}'
        self.run_id = graph_run_identity(self.job_dir, run_id)
        self.database_url = database_url
        self.graph_store = GraphStore(self.run_id, database_url=database_url)
        self.db = self.graph_store.db
        quick = None
        if not read_input:
            rows = {r['key']:r['value'] for r in self.db.execute("SELECT key,value FROM meta WHERE key IN ('snapshot_hash','file_hash','snapshot_stat')")}
            if rows.get('snapshot_stat') == stamp and rows.get('snapshot_hash') and rows.get('file_hash'):
                quick = rows
        if quick:
            self.snapshot={'snapshot_hash':quick['snapshot_hash']}
            self.file_hash=quick['file_hash']
        else:
            self.snapshot,self.file_hash=read_snapshot(self.snapshot_path)
        # Bind a logical run once: concurrent first-open callers must observe the
        # winner's snapshot/configuration before either constructor returns.
        binding_lock=int.from_bytes(hashlib.sha256(('graph-binding:'+self.run_id).encode()).digest()[:8],'big',signed=True)
        with self.db.transaction():
            self.db.execute('SELECT pg_advisory_xact_lock(%s)',(binding_lock,))
            if k is None:k = int(self._meta('k') or 20)
            if name_mode is None:name_mode = self._meta('name_mode') or 'external'
            self.k, self.batch_size, self.client, self.name_mode = k, batch_size, client, name_mode
            self.model = model
            self.namer = namer
            self.name_base_url = name_base_url
            self.name_model = name_model
            if not isinstance(name_concurrency, int) or isinstance(name_concurrency, bool) or name_concurrency < 1:
                raise ValueError('Positive name_concurrency required')
            self.name_concurrency = name_concurrency
            self.embedding_cache_dir = embedding_cache_dir or os.getenv('FASTEMBED_CACHE_PATH')
            self.lease_seconds = lease_seconds
            if not isinstance(k, int) or k < 1 or not isinstance(batch_size, int) or batch_size < 1:
                raise ValueError('Positive k and batch_size required')
            if name_mode not in ('external', 'api'):
                raise ValueError('name_mode must be external or api')
            saved = self._meta('snapshot_hash')
            if entity_same_min_probability is not None and (not isinstance(entity_same_min_probability,(int,float))
                    or isinstance(entity_same_min_probability,bool) or not math.isfinite(entity_same_min_probability)
                    or not 0 <= entity_same_min_probability <= 1):
                raise ValueError('entity_same_min_probability must be within [0, 1]')
            saved_same=self._meta('entity_same_min_probability')
            self.entity_same_min_probability=float(saved_same) if saved_same is not None else (0.7 if saved else 0.6)
            if entity_same_min_probability is not None:
                if saved and float(entity_same_min_probability)!=self.entity_same_min_probability:
                    raise ValueError('Graph job entity_same_min_probability cannot be changed')
                self.entity_same_min_probability=float(entity_same_min_probability)
            if not math.isfinite(self.entity_same_min_probability) or not 0 <= self.entity_same_min_probability <= 1:
                raise ValueError('Invalid saved entity_same_min_probability')
            if problem_resolution is not None and (not isinstance(problem_resolution,(int,float)) or isinstance(problem_resolution,bool)
                                                  or not math.isfinite(problem_resolution) or problem_resolution <= 0):
                raise ValueError('Positive finite problem_resolution required')
            saved_resolution=self._meta('problem_resolution')
            self.problem_resolution=float(saved_resolution) if saved_resolution is not None else 0.05
            if problem_resolution is not None:
                if saved and float(problem_resolution)!=self.problem_resolution:
                    raise ValueError('Graph job problem_resolution cannot be changed')
                self.problem_resolution=float(problem_resolution)
            if not math.isfinite(self.problem_resolution) or self.problem_resolution<=0:
                raise ValueError('Invalid saved problem_resolution')
            self.decision_metric=self._meta('decision_metric') or ('confidence' if saved else 'probability')
            if self.decision_metric not in ('probability','confidence'):
                raise ValueError('Invalid saved decision_metric')
            saved_format=self._meta('context_format')
            self.context_format=saved_format if saved_format is not None else ('registry-v1' if saved else 'explicit-v1')
            if context_format is not None and (context_format not in ('registry-v1','explicit-v1') or
                                               saved and context_format!=self.context_format):
                raise ValueError('Graph job context_format cannot be changed or is invalid')
            if context_format is not None:self.context_format=context_format
            if self.context_format not in ('registry-v1','explicit-v1'):
                raise ValueError('Invalid saved context_format')
            saved_policy = self._meta('graph_policy')
            self.graph_policy = saved_policy or ('legacy' if saved else (graph_policy or 'domain-v1'))
            if graph_policy is not None and graph_policy != self.graph_policy:
                raise ValueError('Graph job policy cannot be changed')
            if self.graph_policy not in ('legacy','domain-v1'):
                raise ValueError('Unknown graph policy')
            if saved and saved != self.snapshot['snapshot_hash']:
                raise ValueError('Graph job is bound to a different immutable snapshot')
            saved_file = self._meta('file_hash')
            if saved_file and saved_file != self.file_hash:
                raise ValueError('Frozen snapshot bytes changed')
            if not saved:
                with self.db.transaction():
                    self._set_meta('snapshot_hash', self.snapshot['snapshot_hash'])
                    self._set_meta('file_hash', self.file_hash)
                    self._set_meta('snapshot_stat', stamp)
                    self._set_meta('phase', 'new')
                    self._set_meta('started_at', str(time.time()))
                    self._set_meta('k', str(k))
                    self._set_meta('name_mode', name_mode)
                    self._set_meta('graph_policy', self.graph_policy)
                    self._set_meta('decision_metric',self.decision_metric)
                    self._set_meta('context_format',self.context_format)
                    self._set_meta('problem_resolution',str(self.problem_resolution))
                    self._set_meta('entity_same_min_probability',str(self.entity_same_min_probability))
            elif self._meta('snapshot_stat') != stamp:
                # The full file was read above and both canonical and byte digests matched.
                # A restored copy can have a new path/mtime without changing the input.
                with self.db.transaction():self._set_meta('snapshot_stat',stamp)
            if saved and (int(self._meta('k')) != k or self._meta('name_mode') != name_mode):
                raise ValueError('Graph job configuration changed')
            frozen_model = self._meta('model_id')
            if frozen_model is None and 'vectors' in self.snapshot:
                ids = {row.get('model_id') for row in self.snapshot['vectors']}
                frozen_model = next(iter(ids)) if len(ids) == 1 else None
            if self.model is not None and frozen_model and self.model.model_id != frozen_model:
                raise ValueError('Embedding model differs from snapshot')

            if self.publisher is not None:
                if self.graph_policy != 'domain-v1':raise ValueError('Neo4j publishing requires domain-v1')
                if not self._meta('neo4j_run_id'):
                    if saved:
                        # A public status/claim preflight may have bound only the
                        # snapshot metadata. It must not consume the first-run
                        # opportunity to select Neo4j publishing. Imported local
                        # business state remains ineligible for backend conversion.
                        driver_lock=int.from_bytes(hashlib.sha256(('graph-driver:'+self.run_id).encode()).digest()[:8],'big',signed=True)
                        if not self.db.execute('SELECT pg_try_advisory_xact_lock(%s) AS locked',(driver_lock,)).fetchone()['locked']:
                            raise ValueError('Cannot enable Neo4j while a graph driver is active')
                        business_tables=('papers','paper_versions','extraction_records','sources','mentions',
                            'contributions','entities','reviews','research_areas','research_area_members',
                            'embedding_cache','candidates','groups','tasks','member_reviews','graph_outbox')
                        has_business_state=self.db.execute(' UNION ALL '.join(
                            'SELECT 1 FROM '+table for table in business_tables)+' LIMIT 1').fetchone()
                        if self._meta('phase')!='new' or has_business_state:
                            raise ValueError('Enable Neo4j only for a fresh graph job')
                    with self.db.transaction():self._set_meta('neo4j_run_id','graph:'+uuid.uuid4().hex)
            elif self._meta('neo4j_run_id') and read_input:
                raise ValueError('This graph job requires its Neo4j publisher')

    def _enqueue(self, kind, key, payload):
        if self.publisher is None:return
        event_id=kind+':'+key
        payload={**payload,'event_id':event_id}
        encoded=json.dumps(payload,ensure_ascii=False,sort_keys=True)
        previous=self.db.execute('SELECT payload_json FROM graph_outbox WHERE event_id=%s',(event_id,)).fetchone()
        if previous and previous['payload_json']!=encoded:raise ValueError('Immutable graph projection event changed')
        inserted=self.db.execute('INSERT INTO graph_outbox(event_id,kind,key,payload_json) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING',(event_id,kind,key,encoded))
        if inserted.rowcount:self._set_meta('neo4j_event_count',str(int(self._meta('neo4j_event_count') or 0)+1))

    def _drain(self):
        if self.publisher is None:return
        if self.db.info.transaction_status != TransactionStatus.IDLE:raise RuntimeError('Cannot publish before PostgreSQL commit')
        while True:
            rows=self.db.execute('SELECT event_id,payload_json FROM graph_outbox WHERE delivered=0 ORDER BY event_id LIMIT %s', (self.batch_size,)).fetchall()
            if not rows:return
            for row in rows:
                started=time.time();wall=time.perf_counter()
                with self.db.transaction():self._set_meta('neo4j_delivery_attempts',str(int(self._meta('neo4j_delivery_attempts') or 0)+1))
                try:self.publisher.publish(self._meta('neo4j_run_id'),json.loads(row['payload_json']))
                finally:
                    with self.db.transaction():self._set_meta('neo4j_delivery_wall_seconds',str(float(self._meta('neo4j_delivery_wall_seconds') or 0)+time.perf_counter()-wall))
                with self.db.transaction():
                    ack=self.db.execute('UPDATE graph_outbox SET delivered=1 WHERE event_id=%s AND delivered=0',(row['event_id'],))
                    if ack.rowcount:self._set_meta('neo4j_delivered_count',str(int(self._meta('neo4j_delivered_count') or 0)+1))
                    if not self._meta('neo4j_first_delivery_at'):self._set_meta('neo4j_first_delivery_at',str(started))
                    kind=json.loads(row['payload_json'])['event_id'].split(':',1)[0]
                    if not self._meta('neo4j_first_'+kind+'_delivery_at'):self._set_meta('neo4j_first_'+kind+'_delivery_at',str(started))
                    self._set_meta('neo4j_latest_delivery_at',str(time.time()))

    def _enqueue_group(self, group_id, store):
        if self.publisher is None:return
        group=self.db.execute('SELECT * FROM groups WHERE group_id=%s',(group_id,)).fetchone()
        mids=json.loads(group['members_json'])
        audits={r['mention_id']:r['excluded'] for r in self.db.execute('SELECT mention_id,excluded FROM member_reviews WHERE group_id=%s',(group_id,))}
        if (group['kind']!='ResearchProblem' or len(mids)>=2) and set(audits)!=set(mids):return
        if group['kind']=='ResearchProblem' and len(mids)>=2 and not self.db.execute("SELECT 1 FROM tasks WHERE group_id=%s AND status='done'",(group_id,)).fetchone():return
        mention_rows={mid:dict(store.db.execute('SELECT m.*,s.source_hash FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.mention_id=%s',(mid,)).fetchone()) for mid in mids}
        entities,assignments,areas,memberships=domain_group_projection(group,audits,mention_rows,store.model.model_id if store.model else None)
        from .neo4j_store import node,edge
        nodes=[node(e['entity_id'],e['kind'],name=e['name'],description=e['description'],aliases=e['aliases'],name_origin=e['name_origin']) for e in entities]
        nodes.extend(node(a['area_id'],'ResearchArea',name=a['name'],description=a['description']) for a in areas)
        edges=[edge(a['mention_id'],a['entity_id'],'RESOLVES_TO') for a in assignments]
        # Areas contain distinct RP entities, never merge them.
        resolved={a['mention_id']:a['entity_id'] for a in assignments}
        edges.extend(edge(resolved[m['mention_id']],m['area_id'],'MEMBER_OF') for m in memberships)
        self._enqueue('group',group_id,{'nodes':nodes,'edges':edges})

    def _enqueue_source(self, store, extraction_id, prepared_paper):
        from .neo4j_store import node,edge
        source=dict(store.db.execute('SELECT * FROM sources WHERE extraction_id=%s',(extraction_id,)).fetchone())
        pid,vid=source['paper_id'],source['version_id']
        sid='source:'+extraction_id
        nodes=[node(pid,'Paper',title=source['title']),node(vid,'PaperVersion',version_label=source['version_label'],source_url=source['source_url'],text=prepared_paper['text'],prepared_source_hash=prepared_paper['source_hash']),node(sid,'Source',**source,snapshot_hash=self.snapshot['snapshot_hash'],review_metadata=self.snapshot.get('review_metadata',{}).get(pid,{}))]
        edges=[edge(pid,vid,'HAS_VERSION'),edge(vid,sid,'HAS_SOURCE')]
        for row in store.db.execute('SELECT * FROM mentions WHERE extraction_id=%s ORDER BY mention_id',(extraction_id,)):
            nodes.append(node(row['mention_id'],'SourceItem',kind=row['kind'],name=row['name'],description=row['description'],roles=json.loads(row['role_json']),version_or_subset=row['version_or_subset'],source_json=row['source_json'],mention_hash=row['mention_hash']))
            edges.append(edge(sid,row['mention_id'],'HAS_ITEM'))
            if row['kind']=='ResearchProblem':
                mention=dict(row);mention['source_hash']=source['source_hash']
                singleton={'members_json':json.dumps([row['mention_id']]),'kind':'ResearchProblem'}
                entities,assignments,_,_=domain_group_projection(singleton,{}, {row['mention_id']:mention},None)
                e=entities[0]
                nodes.append(node(e['entity_id'],'ResearchProblem',name=e['name'],description=e['description'],aliases=e['aliases'],name_origin=e['name_origin']))
                edges.append(edge(row['mention_id'],e['entity_id'],'RESOLVES_TO'))
        for row in store.db.execute('SELECT * FROM contributions WHERE extraction_id=%s ORDER BY contribution_id',(extraction_id,)):
            nodes.append(node(row['contribution_id'],'Contribution',description=row['description'],source_json=row['source_json']))
            edges.append(edge(sid,row['contribution_id'],'HAS_ITEM'))
        self._enqueue('source',extraction_id,{'nodes':nodes,'edges':edges})

    def close(self):
        self.graph_store.close()

    def _semantic_store(self, model=None):
        return SemanticStore(self.run_id, model, database_url=self.database_url)

    def _meta(self, key):
        row = self.db.execute('SELECT value FROM meta WHERE key=%s', (key,)).fetchone()
        return row['value'] if row else None

    def _set_meta(self, key, value):
        self.db.execute('INSERT INTO meta VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))

    def _timed(self, name, fn):
        start = time.perf_counter()
        cpu = time.process_time()
        result = fn()
        with self.db.transaction():
            self.db.execute('INSERT INTO stage VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET seconds=stage.seconds+excluded.seconds',
                            (name, time.perf_counter()-start))
            self.db.execute('INSERT INTO stage VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET seconds=stage.seconds+excluded.seconds',('process_cpu:'+name,time.process_time()-cpu))
        return result

    def _record_requests(self, result):
        raw=result.get('raw') or {}
        for req in raw.get('requests',[]):
            rid=req.get('request_id')
            if not rid:continue
            cursor=self.db.execute('INSERT INTO jev_requests VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                        (rid,req.get('request_duration_s'),req.get('total_elapsed_s'),req.get('attempts'),
                         req.get('model'),json.dumps(req.get('usage')),req.get('started_at'),req.get('finished_at'),
                         json.dumps(req.get('http_intervals')) if req.get('http_intervals') is not None else None))
            if cursor.rowcount:
                for stage,seconds in (('jev_api_request_seconds',req.get('request_duration_s') or 0),
                                      ('jev_api_elapsed_seconds',req.get('total_elapsed_s') or 0),
                                      ('jev_retry_count',max(0,(req.get('attempts') or 1)-1))):
                    self.db.execute('INSERT INTO stage VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET seconds=stage.seconds+excluded.seconds',(stage,seconds))

    def _record_failure(self, exc):
        audits=getattr(exc,'request_audits',None)
        if not isinstance(audits,list):audits=[getattr(exc,'request_audit',None)]
        audits=[audit for audit in audits if isinstance(audit,dict) and audit.get('request_id')]
        if audits:
            with self.db.transaction():self._record_requests({'raw':{'requests':audits}})

    def _prepare_store(self):
        """Import pinned sources idempotently into this graph's PostgreSQL schema."""
        if self._meta('phase') != 'new':
            return self.run_id
        ids = []
        papers = {}
        for paper in self.snapshot['papers']:
            if paper['paper_id'] in papers:
                raise ValueError('Duplicated paper source')
            papers[paper['paper_id']] = paper
        repository = PostgresSourceRepository(self.graph_store)
        # Interrupted retries replay immutable inputs, never delete durable state.
        with self.db.transaction():
            for wrapper in self.snapshot['extractions']:
                x = _extraction(wrapper)
                if validate_extraction(x) != RETRIEVAL_SCHEMA_VERSION:
                    raise ValueError('Snapshot contains non-v0.3 extraction')
                p = _paper(papers, x)
                version = x['paper_version']
                self.db.execute('INSERT INTO papers(paper_id,title,first_published,year,doi,arxiv_id,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                    (x['paper_id'],p['title'],p.get('first_published'),p.get('year'),p.get('doi'),p.get('arxiv_id'),'snapshot','snapshot'))
                self.db.execute('INSERT INTO paper_versions(version_id,paper_id,version_label,version_status,source_url,abstract,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                    (x['version_id'],x['paper_id'],version['version_label'],'source_confirmed' if version['version_label'] else 'unverified_metadata_snapshot',version['source_url'],p['text'],'snapshot'))
                eid = repository.ingest_extractions(self.db,[x])[0]
                if wrapper.get('extraction_id') and wrapper['extraction_id'] != eid:
                    raise ValueError('Snapshot extraction ID differs from validated import')
                ids.append(eid)
        store = self._semantic_store(self.model)
        try:
            if ids:
                store.ingest(self.run_id, ids)
            # Cache uses exactly SemanticStore's model/text digest, so retrieval can reuse it.
            known = {r['paper_id']+'::'+r['item_key']: (r['kind'],r['name']+'\n'+r['description']) for r in store.db.execute('SELECT s.paper_id,m.item_key,m.name,m.description,m.kind FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id')}
            known.update({r['paper_id']+'::'+r['item_key']: ('Contribution',r['description']) for r in store.db.execute('SELECT s.paper_id,c.item_key,c.description FROM contributions c JOIN sources s ON s.extraction_id=c.extraction_id')})
            seen = set()
            model_ids = set()
            dimension = None
            model_dimensions = {}
            with store.db.transaction():
                for row in self.snapshot['vectors']:
                    key = row['item_key']
                    if key in seen or key not in known or row['kind'] != known[key][0]:
                        raise ValueError('Duplicate, unknown, or mismatched snapshot vector')
                    seen.add(key)
                    expected = known[key][1]
                    if row['text'] != expected or not isinstance(row.get('model_id'), str) or not row['model_id']:
                        raise ValueError('Snapshot vector text/model mismatch')
                    vector = row['vector']
                    from .vector_index import _unit_vector
                    _unit_vector(vector, dimension)
                    dimension = len(vector)
                    if row['model_id'].startswith('voyageai:'):
                        from .embeddings import embedding_model_from_id
                        if row['model_id'] not in model_dimensions:
                            model_dimensions[row['model_id']] = embedding_model_from_id(row['model_id']).dimension
                        expected_dimension = model_dimensions[row['model_id']]
                        if dimension != expected_dimension:
                            raise ValueError('Snapshot vector dimension differs from model identity')
                    model_ids.add(row['model_id'])
                    store.db.execute('INSERT INTO embedding_cache VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                                     (row['model_id'], digest(expected), json.dumps(vector)))
            if not {key for key,(kind,_) in known.items() if kind!='Contribution'} <= seen or len(model_ids) > 1:
                raise ValueError('Every semantic mention requires one compatible vector/model')
            if self.model and model_ids and model_ids != {self.model.model_id}:
                raise ValueError('Embedding model differs from snapshot')
            if self.publisher is not None:
                for eid in ids:
                    with self.db.transaction():
                        paper_id=store.db.execute('SELECT paper_id FROM sources WHERE extraction_id=%s',(eid,)).fetchone()['paper_id']
                        self._enqueue_source(store,eid,papers[paper_id])
                        self._set_meta('source_published:'+eid,'queued')
                    self._drain()
            with self.db.transaction():
                self._set_meta('model_id', next(iter(model_ids), ''))
                self._set_meta('phase', 'imported')
        finally:
            store.close()
        return self.run_id

    def _candidates(self):
        store = self._semantic_store()
        try:
            rows = store.db.execute('SELECT m.mention_id,m.item_key,m.kind,s.paper_id FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id ORDER BY m.kind,m.mention_id').fetchall()
            vectors = {r['item_key']: (r['model_id'], r['vector']) for r in self.snapshot['vectors']}
            bykind = {kind: {} for kind in KINDS.values()}
            for r in rows:
                bykind[r['kind']][r['mention_id']] = vectors[r['paper_id']+'::'+r['item_key']][1]
            pairs = {}
            for kind, data in bykind.items():
                if self.graph_policy == 'domain-v1' and kind == 'ResearchProblem':continue
                index = HNSWIndex(data, self._meta('model_id') or 'empty-snapshot')
                for mid, vector in data.items():
                    for neighbor in index.search(vector, min(len(data), self.k+1)):
                        other = neighbor['id']
                        if mid == other: continue
                        a,b = sorted((mid,other))
                        pairs[(kind,a,b)] = max(pairs.get((kind,a,b),-1),neighbor['similarity'])
            proposal = cluster_problem_vectors(bykind['ResearchProblem'],k=self.k,resolution=self.problem_resolution)
            with self.db.transaction():
                for (kind,a,b),similarity in sorted(pairs.items()):
                    pid = 'pair:' + digest([kind,a,b])[:32]
                    self.db.execute('INSERT INTO candidates(pair_id,kind,left_id,right_id,similarity) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                                    (pid,kind,a,b,similarity))
                self._set_meta('problem_proposal',json.dumps(proposal,sort_keys=True))
                self._set_meta('phase','candidates')
            return len(pairs)
        finally: store.close()

    def _judge(self):
        from .decision_api import judge_pairs
        if self.client is None:
            raise ValueError('Jev client required for unjudged candidate pairs')
        store = self._semantic_store()
        try:
            # Only this owning thread touches either PostgreSQL connection. Each worker
            # receives a detached, bounded payload and shares the client's HTTP cap.
            concurrency = getattr(self.client, 'max_concurrency', 1)
            last_pair = None
            exhausted = False
            failure = None
            pending = {}

            def next_batch():
                nonlocal last_pair, exhausted
                if last_pair is None:
                    rows = self.db.execute("SELECT * FROM candidates WHERE decision='unjudged' ORDER BY pair_id LIMIT %s",(self.batch_size,)).fetchall()
                else:
                    rows = self.db.execute("SELECT * FROM candidates WHERE decision='unjudged' AND pair_id>%s ORDER BY pair_id LIMIT %s",(last_pair,self.batch_size)).fetchall()
                if not rows:
                    exhausted = True
                    return None
                last_pair = rows[-1]['pair_id']
                ids = sorted({mid for r in rows for mid in (r['left_id'],r['right_id'])})
                mentions = {}
                for mid in ids:
                    r = store.db.execute('''SELECT m.*,s.paper_id,s.version_id,s.title,s.source_url,s.source_hash FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id WHERE mention_id=%s''',(mid,)).fetchone()
                    mentions[mid] = {'mention_id':mid,'name':r['name'],'description':r['description'],'item_key':r['item_key'],
                                     'paper_id':r['paper_id'],'version_id':r['version_id'],'title':r['title'],
                                     'source_url':r['source_url'],'source_hash':r['source_hash'],'source':json.loads(r['source_json'])}
                inputs = [{'pair_id':r['pair_id'],'kind':r['kind'],'left':mentions[r['left_id']], 'right':mentions[r['right_id']]} for r in rows]
                return inputs

            def persist(inputs, results):
                if not isinstance(results, (list, tuple)) or any(not isinstance(r, dict) for r in results):
                    raise ValueError('Malformed pair judgments')
                mapping = {r.get('pair_id'):r for r in results}
                if set(mapping) != {r['pair_id'] for r in inputs} or len(results)!=len(mapping):
                    raise ValueError('Jev returned incomplete or duplicate pair judgments')
                if any(r.get('decision') not in ('same','different','pending') for r in results):
                    raise ValueError('Invalid pair judgment')
                # Validate serialization for the entire batch before any changes.
                serialized = {pid:json.dumps(r,sort_keys=True) for pid,r in mapping.items()}
                with self.db.transaction():
                    for pid, result in mapping.items():
                        self._record_requests(result)
                        self.db.execute('UPDATE candidates SET decision=%s,audit=%s WHERE pair_id=%s AND decision=%s',
                                        (result['decision'],serialized[pid],pid,'unjudged'))

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                while pending or not exhausted and failure is None:
                    # Refill only free slots, without waiting for a whole wave.
                    while len(pending) < concurrency and not exhausted and failure is None:
                        try:
                            inputs = next_batch()
                            if inputs is not None:
                                future = executor.submit(judge_pairs,inputs,self.client,
                                    decision_metric=self.decision_metric,context_format=self.context_format,
                                    same_min_probability=self.entity_same_min_probability if self.decision_metric=='probability' else None)
                                pending[future] = inputs
                        except Exception as exc:
                            failure = exc
                    if not pending:
                        break
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    # Observe every completed failure before dispatching more work.
                    for future in done:
                        inputs = pending.pop(future)
                        results = None
                        try:
                            results = future.result()
                            persist(inputs, results)
                        except Exception as exc:
                            self._record_failure(exc)
                            # Keep HTTP evidence even when returned batch coverage
                            # or decisions are invalid; no candidate is committed.
                            if isinstance(results, (list, tuple)):
                                with self.db.transaction():
                                    for result in results:
                                        if isinstance(result, dict):self._record_requests(result)
                            if failure is None:failure = exc
                if failure is not None:
                    raise failure
            with self.db.transaction():self._set_meta('phase','judged')
        finally: store.close()

    def _groups(self):
        from .decision_api import review_groups
        if self.client is None:raise ValueError('Jev client required for group coherence review')
        store = self._semantic_store()
        try:
            members = {r['mention_id']:dict(r) for r in store.db.execute('SELECT * FROM mentions')}
            parent = {mid:mid for mid in members}
            def root(mid):
                while parent[mid]!=mid:
                    parent[mid]=parent[parent[mid]];mid=parent[mid]
                return mid
            for r in self.db.execute("SELECT left_id,right_id FROM candidates WHERE decision='same'"):
                a,b=root(r['left_id']),root(r['right_id'])
                parent[b]=a
            components={}
            for mid in sorted(members):components.setdefault(root(mid),[]).append(mid)
            decisions=list(self.db.execute("SELECT left_id,right_id,decision FROM candidates WHERE decision IN ('different','pending')"))
            blocked=set()
            for r in decisions:
                if r['decision']=='pending':
                    blocked.update((root(r['left_id']),root(r['right_id'])))
                elif root(r['left_id'])==root(r['right_id']):
                    blocked.add(root(r['left_id']))
            groups=[]
            for mids in components.values():
                first=members[mids[0]]
                gid='group:'+digest(mids)[:32]
                variants=[members[mid]['name'] for mid in mids]
                group={'group_id':gid,'kind':first['kind'],'name':variants[0],
                       'description':first['description'],
                       'members':[{'mention_id':mid,'name':members[mid]['name'],'description':members[mid]['description'],
                                   'source':json.loads(members[mid]['source_json'])} for mid in mids]}
                groups.append(group)
            for start in range(0,len(groups),self.batch_size):
                batch=groups[start:start+self.batch_size]
                try:results=review_groups(batch,self.client,decision_metric=self.decision_metric,context_format=self.context_format)
                except Exception as exc:
                    self._record_failure(exc)
                    raise
                mapping={r['group_id']:r for r in results}
                if set(mapping)!={g['group_id'] for g in batch} or len(mapping)!=len(results):raise ValueError('Incomplete group review')
                with self.db.transaction():
                    for group in batch:
                        r=mapping[group['group_id']]
                        self._record_requests(r)
                        mids=[m['mention_id'] for m in group['members']]
                        ok=bool(r['passed']) and root(mids[0]) not in blocked
                        self.db.execute('INSERT INTO groups(group_id,kind,members_json,name,description,review,issues_json,audit) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(group_id) DO UPDATE SET kind=excluded.kind,members_json=excluded.members_json,name=excluded.name,description=excluded.description,review=excluded.review,issues_json=excluded.issues_json,audit=excluded.audit',
                                        (group['group_id'],group['kind'],json.dumps(mids),group['name'],group['description'],
                                         'accepted' if ok else 'pending',json.dumps(r.get('issues',[])),json.dumps(r,sort_keys=True)))
                        if ok and group['kind']=='ResearchProblem':
                            self.db.execute("INSERT INTO tasks(task_id,group_id,status,queued_at) VALUES(%s,%s,'queued',%s) ON CONFLICT DO NOTHING",
                                            ('name:'+group['group_id'],group['group_id'],time.time()))
            with self.db.transaction():self._set_meta('phase','grouped')
        finally:store.close()

    def _groups_domain(self):
        """Review each candidate membership; neither uncertainty nor pending edges freeze a group."""
        from .decision_api import review_group_members
        if self.client is None:raise ValueError('Jev client required for member review')
        store=self._semantic_store()
        try:
            members={r['mention_id']:dict(r) for r in store.db.execute('SELECT * FROM mentions')}
            parent={mid:mid for mid,r in members.items() if r['kind']!='ResearchProblem'}
            def root(mid):
                while parent[mid]!=mid:
                    parent[mid]=parent[parent[mid]];mid=parent[mid]
                return mid
            for row in self.db.execute("SELECT left_id,right_id FROM candidates WHERE decision='same'"):
                a,b=root(row['left_id']),root(row['right_id']);parent[b]=a
            conflicts=[dict(r) for r in self.db.execute("SELECT pair_id,kind,left_id,right_id FROM candidates WHERE decision='different'")
                       if root(r['left_id'])==root(r['right_id'])]
            if conflicts:
                with self.db.transaction():self._set_meta('identity_conflicts',json.dumps(conflicts,sort_keys=True))
                raise ValueError('Explicit different judgment conflicts with a same identity chain; user decision required: '+json.dumps(conflicts,sort_keys=True))
            components={}
            for mid in sorted(parent):components.setdefault(root(mid),[]).append(mid)
            proposal=json.loads(self._meta('problem_proposal'))
            clusters=proposal['clusters']
            rp={mid for mid,r in members.items() if r['kind']=='ResearchProblem'}
            if len({mid for cluster in clusters for mid in cluster})!=sum(map(len,clusters)) or {mid for cluster in clusters for mid in cluster}!=rp:
                raise ValueError('Leiden proposal does not partition ResearchProblem mentions')
            all_groups=list(components.values())+[sorted(cluster) for cluster in clusters]
            groups=[]
            for mids in all_groups:
                mids=sorted(mids);first=members[mids[0]]
                gid='group:'+digest(mids)[:32]
                groups.append({'group_id':gid,'kind':first['kind'],'members':mids,
                               'name':first['name'],'description':first['description']})
            with self.db.transaction():
                for group in groups:
                    self.db.execute('''INSERT INTO groups(group_id,kind,members_json,name,description,review)
                                       VALUES(%s,%s,%s,%s,%s,'unreviewed') ON CONFLICT DO NOTHING''',
                                    (group['group_id'],group['kind'],json.dumps(group['members']),group['name'],group['description']))
            inputs=[]
            for g in groups:
                if g['kind']=='ResearchProblem':continue
                for mid in g['members']:
                    if self.db.execute('SELECT 1 FROM member_reviews WHERE group_id=%s AND mention_id=%s',(g['group_id'],mid)).fetchone():continue
                    m=members[mid]
                    inputs.append({'membership_id':'membership:'+digest([g['group_id'],mid])[:32],
                                   'group_id':g['group_id'],'kind':'ResearchArea' if g['kind']=='ResearchProblem' else g['kind'],
                                   'name':g['name'],'description':g['description'],
                                   'member':{'mention_id':mid,'name':m['name'],'description':m['description'],
                                             'source':json.loads(m['source_json'])}})
            for start in range(0,len(inputs),self.batch_size):
                batch=inputs[start:start+self.batch_size]
                try:results=review_group_members(batch,self.client,decision_metric=self.decision_metric,context_format=self.context_format)
                except Exception as exc:
                    self._record_failure(exc);raise
                mapping={r['membership_id']:r for r in results}
                if len(mapping)!=len(results) or set(mapping)!={x['membership_id'] for x in batch}:
                    raise ValueError('Incomplete or duplicate member review')
                with self.db.transaction():
                    for item in batch:
                        result=mapping[item['membership_id']]
                        self._record_requests(result)
                        if result.get('excluded') != self._strict_exclusion(result):
                            raise ValueError('Member exclusion disagrees with strict policy')
                        self.db.execute('INSERT INTO member_reviews VALUES(%s,%s,%s,%s)',
                                        (item['group_id'],item['member']['mention_id'],int(result['excluded']),json.dumps(result,sort_keys=True)))
                    for gid in {item['group_id'] for item in batch}:self._enqueue_group(gid,store)
                self._drain()
            with self.db.transaction():
                for g in groups:
                    self.db.execute("UPDATE groups SET review='accepted' WHERE group_id=%s",(g['group_id'],))
                    if g['kind']=='ResearchProblem' and len(g['members'])>=2:
                        self.db.execute("INSERT INTO tasks(task_id,group_id,status,queued_at) VALUES(%s,%s,'queued',%s) ON CONFLICT DO NOTHING",
                                        ('name:'+g['group_id'],g['group_id'],time.time()))
                self._set_meta('phase','grouped')
                for g in groups:self._enqueue_group(g['group_id'],store)
            self._drain()
        finally:store.close()

    def _strict_exclusion(self, result):
        metric=result.get('decision_metric',self.decision_metric)
        value=result.get(self.decision_metric)
        if metric!=self.decision_metric or not isinstance(value,(int,float)) or isinstance(value,bool) or not math.isfinite(value) or not 0<=value<=1:
            raise ValueError('Member review has invalid decision metric or score')
        if self.decision_metric=='probability':
            raw=result.get('raw')
            answer=raw.get('answers',{}).get('membership') if isinstance(raw,dict) and isinstance(raw.get('answers'),dict) else None
            probabilities=answer.get('probabilities') if isinstance(answer,dict) else None
            if (not isinstance(probabilities,dict) or set(probabilities)!={'belongs','not_belongs','insufficient'}
                or any(not isinstance(v,(int,float)) or isinstance(v,bool) or not math.isfinite(v) or not 0<=v<=1
                       for v in probabilities.values()) or abs(sum(probabilities.values())-1)>0.02
                or value!=probabilities['not_belongs'] or result.get('confidence')!=answer.get('confidence')
                or result.get('choice')!=answer.get('choice') or result.get('decision_metric')!='probability'):
                raise ValueError('Member probability disagrees with raw Jev answer')
        return result.get('choice')=='not_belongs' and value>0.7

    def _review_area_members(self, group_id=None):
        """Judge area membership only against the actual generated area summary."""
        from .decision_api import review_group_members
        groups=self.db.execute("""SELECT g.* FROM groups g JOIN tasks t ON t.group_id=g.group_id
            WHERE g.kind='ResearchProblem' AND t.status='done'""" +
            (' AND g.group_id=%s' if group_id is not None else '')+' ORDER BY g.group_id',
            (group_id,) if group_id is not None else ()).fetchall()
        if not groups:return
        store=self._semantic_store()
        try:
            inputs=[]
            for group in groups:
                for mid in json.loads(group['members_json']):
                    if self.db.execute('SELECT 1 FROM member_reviews WHERE group_id=%s AND mention_id=%s',(group['group_id'],mid)).fetchone():continue
                    row=store.db.execute('SELECT name,description,source_json FROM mentions WHERE mention_id=%s',(mid,)).fetchone()
                    inputs.append({'membership_id':'membership:'+digest([group['group_id'],mid])[:32],
                                   'group_id':group['group_id'],'kind':'ResearchArea','name':group['name'],
                                   'description':group['description'],'member':{'mention_id':mid,'name':row['name'],
                                   'description':row['description'],'source':json.loads(row['source_json'])}})
            if inputs and self.client is None:raise ValueError('Jev client required for named area member review')
            for start in range(0,len(inputs),self.batch_size):
                batch=inputs[start:start+self.batch_size]
                try:results=review_group_members(batch,self.client,decision_metric=self.decision_metric,context_format=self.context_format)
                except Exception as exc:self._record_failure(exc);raise
                mapping={r['membership_id']:r for r in results}
                if len(mapping)!=len(results) or set(mapping)!={i['membership_id'] for i in batch}:
                    raise ValueError('Incomplete or duplicate area member review')
                with self.db.transaction():
                    for item in batch:
                        result=mapping[item['membership_id']]
                        self._record_requests(result)
                        if result.get('excluded') != self._strict_exclusion(result):
                            raise ValueError('Area member exclusion disagrees with strict policy')
                        result={**result,'area_revision':digest([item['name'],item['description']])}
                        self.db.execute('INSERT INTO member_reviews VALUES(%s,%s,%s,%s)',
                                        (item['group_id'],item['member']['mention_id'],int(result['excluded']),json.dumps(result,sort_keys=True)))
                    for gid in {item['group_id'] for item in batch}:self._enqueue_group(gid,store)
                self._drain()
        finally:store.close()

    def claim(self, worker, limit=5):
        if not worker or not 1<=limit<=100:raise ValueError('Worker and limit 1..100 required')
        now=time.time();out=[]
        self.db.execute('BEGIN')
        try:
            rows=self.db.execute("SELECT t.*,g.members_json FROM tasks t JOIN groups g ON g.group_id=t.group_id WHERE t.status='queued' OR (t.status='leased' AND t.expires<%s) ORDER BY t.task_id LIMIT %s FOR UPDATE OF t SKIP LOCKED",(now,limit)).fetchall()
            store=self._semantic_store()
            try:
                for row in rows:
                    token=uuid.uuid4().hex
                    self.db.execute("UPDATE tasks SET status='leased',token=%s,expires=%s,worker=%s,claimed_at=%s,attempts=attempts+1 WHERE task_id=%s",
                                    (token,now+self.lease_seconds,worker,now,row['task_id']))
                    self.db.execute('INSERT INTO stage VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET seconds=stage.seconds+excluded.seconds',('name_queued_wait',max(0,now-(row['queued_at'] or now))))
                    mids=json.loads(row['members_json'])
                    details=[]
                    for mid in mids:
                        m=store.db.execute('''SELECT m.name,m.description,m.source_json,s.paper_id,s.title,s.source_url FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.mention_id=%s''',(mid,)).fetchone()
                        details.append({'mention_id':mid,'name':m['name'],'description':m['description'],'paper_id':m['paper_id'],'title':m['title'],'source_url':m['source_url'],'source':json.loads(m['source_json'])})
                    out.append({'task_id':row['task_id'],'group_id':row['group_id'],'kind':'name_group',
                                'lease_token':token,'input_hash':digest(details),'members':details,
                                'instructions':('Summarize a thematic ResearchArea covering the related but distinct research problems. Provide a concise area name and description grounded in the source members.' if self.graph_policy=='domain-v1' else 'Provide a concise canonical ResearchProblem name and description grounded in the source members.')})
            finally:store.close()
            self.db.commit()
        except BaseException:self.db.rollback();raise
        return out

    def submit(self, submissions):
        if self._meta('neo4j_run_id') and self.publisher is None:raise ValueError('Neo4j publisher required to submit results')
        self._drain()
        if not isinstance(submissions,list):raise ValueError('Expected list of name submissions')
        results=[]
        for item in submissions:
            self.db.execute('BEGIN')
            try:
                row=self.db.execute('SELECT * FROM tasks WHERE task_id=%s FOR UPDATE',(item.get('task_id'),)).fetchone()
                if not row:raise ValueError('Unknown task')
                value=item.get('result')
                if row['status']=='done':
                    if row['token']==item.get('lease_token') and json.loads(row['result_json'])==value:
                        results.append({'task_id':row['task_id'],'status':'unchanged'});self.db.commit();continue
                    raise ValueError('Conflicting name submission')
                if row['status']!='leased' or row['expires']<=time.time() or row['token']!=item.get('lease_token'):
                    raise ValueError('Stale name lease')
                if not isinstance(value,dict) or not isinstance(value.get('name'),str) or not value['name'].strip() or not isinstance(value.get('description'),str) or not value['description'].strip():
                    raise ValueError('Name result requires name and description')
                if value.get('group_id',row['group_id'])!=row['group_id']:
                    raise ValueError('Group pin mismatch')
                mids=json.loads(self.db.execute('SELECT members_json FROM groups WHERE group_id=%s',(row['group_id'],)).fetchone()['members_json'])
                store=self._semantic_store()
                try:
                    details=[]
                    for mid in mids:
                        m=store.db.execute('''SELECT m.name,m.description,m.source_json,s.paper_id,s.title,s.source_url FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.mention_id=%s''',(mid,)).fetchone()
                        details.append({'mention_id':mid,'name':m['name'],'description':m['description'],'paper_id':m['paper_id'],'title':m['title'],'source_url':m['source_url'],'source':json.loads(m['source_json'])})
                finally:store.close()
                if item.get('input_hash') != digest(details):raise ValueError('Name input fingerprint mismatch')
                self.db.execute('INSERT INTO stage VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET seconds=stage.seconds+excluded.seconds',('name_external_service',max(0,time.time()-(row['claimed_at'] or time.time()))))
                self.db.execute("UPDATE tasks SET status='done',result_json=%s WHERE task_id=%s",(json.dumps(value,sort_keys=True),row['task_id']))
                self.db.execute('UPDATE groups SET name=%s,description=%s WHERE group_id=%s',(value['name'],value['description'],row['group_id']))
                self.db.commit();results.append({'task_id':row['task_id'],'status':'done'})
            except BaseException:self.db.rollback();raise
        if self.graph_policy=='domain-v1' and self.publisher is not None:
            for submission in submissions:
                row=self.db.execute('SELECT group_id FROM tasks WHERE task_id=%s',(submission['task_id'],)).fetchone()
                group_id=row['group_id']
                self._timed('jev_area_member_wall',lambda group_id=group_id:self._review_area_members(group_id))
        return results

    def _api_names(self):
        from .extraction_api import OptionalLLMExtractor
        if self.namer is None and not (self.name_base_url and self.name_model):
            raise ValueError('API name mode needs name_base_url and name_model')
        namer = self.namer or OptionalLLMExtractor(self.name_base_url,self.name_model,enabled=True)
        prompt = ('Summarize a thematic ResearchArea from related, potentially distinct research problems. Use supplied source evidence. Return JSON with name, description and group_id exactly matching the input.' if self.graph_policy=='domain-v1' else 'Name this coherent ResearchProblem group using supplied source evidence. Return JSON with name, description and group_id exactly matching the input.')
        # Claim and submit use the owning SQLite thread. Workers only call the HTTP namer.
        with ThreadPoolExecutor(max_workers=self.name_concurrency) as pool:
            while True:
                tasks=self.claim('api',min(self.batch_size,self.name_concurrency))
                if not tasks:break
                futures={pool.submit(namer.generate_json,prompt,
                    {'group_id':task['group_id'],'members':task['members']},
                    required_keys=('name','description','group_id')):task for task in tasks}
                failure=None
                for future in as_completed(futures):
                    task=futures[future]
                    try:
                        result=future.result()
                        self.submit([{**{key:task[key] for key in ('task_id','lease_token','input_hash')},'result':result}])

                    except BaseException as exc:
                        if failure is None:failure=exc
                        # Release only this call's own lease, so recovery does not wait
                        # for the deadline and unrelated external leases are untouched.
                        with self.db.transaction():
                            self.db.execute("UPDATE tasks SET status='queued',token=NULL,expires=NULL,worker=NULL WHERE task_id=%s AND status='leased' AND token=%s AND worker='api'",
                                            (task['task_id'],task['lease_token']))
                if failure is not None:raise failure

    def _materialize(self):
        if self.db.execute("SELECT 1 FROM tasks WHERE status!='done' LIMIT 1").fetchone():return
        store=self._semantic_store(self.model)
        try:
            # export establishes the model fingerprint; imported review validates every source.
            store.export_candidates(limit=1)
            groups=self.db.execute("SELECT * FROM groups WHERE review='accepted' ORDER BY group_id").fetchall()
            entities=[];assignments=[]
            for group in groups:
                mids=json.loads(group['members_json'])
                names=[store.db.execute('SELECT name FROM mentions WHERE mention_id=%s',(mid,)).fetchone()['name'] for mid in mids]
                eid='entity:'+digest([group['kind'],mids])[:32]
                entities.append({'entity_id':eid,'kind':group['kind'],'name':group['name'],'description':group['description'],
                                 'aliases':sorted(set(names)-{group['name']}),
                                 'name_origin':'reviewer_generated' if group['kind']=='ResearchProblem' else 'source_authored'})
                for mid in mids:
                    m=store.db.execute('''SELECT m.mention_hash,s.source_hash FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.mention_id=%s''',(mid,)).fetchone()
                    assignments.append({'mention_id':mid,'model_id':store.model.model_id if store.model else None,
                                        'precondition':digest([mid,m['source_hash'],m['mention_hash'],store.model.model_id if store.model else None]),
                                        'candidate_revisions':{},'decision':'new_entity','entity_id':eid})
            review={'schema_version':REVIEW_VERSION,'review_id':'snapshot:'+self.snapshot['snapshot_hash'],
                    'entities':entities,'assignments':assignments}
            result=store.import_review(review)
            with self.db.transaction():self._set_meta('phase','complete');self._set_meta('review_result',json.dumps(result))
        finally:store.close()

    def _materialize_domain(self):
        if self.db.execute("SELECT 1 FROM tasks WHERE status!='done' LIMIT 1").fetchone():return
        store=self._semantic_store(self.model)
        try:
            store.export_candidates(limit=1)
            groups=self.db.execute("SELECT * FROM groups WHERE review='accepted' ORDER BY group_id").fetchall()
            entities=[];assignments=[];areas=[];memberships=[]
            mention_rows={r['mention_id']:dict(r) for r in store.db.execute('''SELECT m.mention_id,m.name,m.description,m.mention_hash,
                m.kind,s.source_hash FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id''')}
            covered=set()
            for group in groups:
                mids=json.loads(group['members_json'])
                audits={r['mention_id']:r['excluded'] for r in self.db.execute('SELECT mention_id,excluded FROM member_reviews WHERE group_id=%s',(group['group_id'],))}
                if covered.intersection(mids):raise ValueError('Mention appears in more than one group')
                covered.update(mids)
                projection=domain_group_projection(group,audits,mention_rows,store.model.model_id if store.model else None)
                entities.extend(projection[0]);assignments.extend(projection[1]);areas.extend(projection[2]);memberships.extend(projection[3])
            if covered!=set(mention_rows):raise ValueError('Graph groups do not cover all source mentions')
            review={'schema_version':REVIEW_VERSION,'review_id':'snapshot:'+self.snapshot['snapshot_hash'],
                    'entities':entities,'assignments':assignments}
            result=store.import_review(review)
            area_result=store.import_research_areas(areas,memberships)
            with self.db.transaction():
                self._set_meta('phase','complete');self._set_meta('review_result',json.dumps(result))
                self._set_meta('area_result',json.dumps(area_result))
        finally:store.close()

    def _final_name_review(self):
        """Review the actual generated display claim after external/API naming."""
        from .decision_api import review_groups
        rows=self.db.execute("SELECT * FROM groups WHERE kind='ResearchProblem' AND review='accepted' AND name_review='unreviewed' AND group_id IN (SELECT group_id FROM tasks WHERE status='done') ORDER BY group_id").fetchall()
        if not rows:return
        if self.client is None:raise ValueError('Jev client required for final ResearchProblem naming review')
        store=self._semantic_store()
        try:
            for start in range(0,len(rows),self.batch_size):
                batch=rows[start:start+self.batch_size]
                inputs=[]
                for g in batch:
                    members=[]
                    for mid in json.loads(g['members_json']):
                        r=store.db.execute('SELECT name,description,source_json FROM mentions WHERE mention_id=%s',(mid,)).fetchone()
                        members.append({'mention_id':mid,'name':r['name'],'description':r['description'],'source':json.loads(r['source_json'])})
                    inputs.append({'group_id':g['group_id'],'kind':g['kind'],'name':g['name'],'description':g['description'],'members':members})
                try:results=review_groups(inputs,self.client,decision_metric=self.decision_metric,context_format=self.context_format)
                except Exception as exc:
                    self._record_failure(exc)
                    raise
                mapping={r['group_id']:r for r in results}
                if len(mapping)!=len(results) or set(mapping)!={g['group_id'] for g in batch}:raise ValueError('Incomplete final name review')
                with self.db.transaction():
                    for g in batch:
                        result=mapping[g['group_id']]
                        self._record_requests(result)
                        self.db.execute('UPDATE groups SET name_review=%s,name_audit=%s,review=CASE WHEN %s THEN review ELSE %s END,issues_json=CASE WHEN %s THEN issues_json ELSE %s END WHERE group_id=%s',
                                        ('accepted' if result['passed'] else 'pending',json.dumps(result,sort_keys=True),bool(result['passed']),'pending',bool(result['passed']),json.dumps(result.get('issues',[])),g['group_id']))
        finally:store.close()

    def _compatible_model(self):
        model_id=self._meta('model_id')
        if self.model is not None:
            model = self.model
        else:
            from .embeddings import embedding_model_from_id
            model=embedding_model_from_id(model_id,cache_dir=self.embedding_cache_dir, **self.voyage_options)
        if model.model_id!=model_id:
            raise ValueError('Provide an embedding model matching the frozen snapshot for retrieval indexes')
        self.model=model
        return model

    def _build_indexes(self):
        if self._meta('index_status')=='verified':return
        empty=not self.db.execute('SELECT 1 FROM mentions LIMIT 1').fetchone() and not self.db.execute('SELECT 1 FROM contributions LIMIT 1').fetchone()
        if empty:
            with self.db.transaction():
                self._set_meta('index_status','verified')
                self._set_meta('index_model_id','')
                self._set_meta('index_builds','0')
                self._set_meta('index_cache_hits','0')
                self._set_meta('index_embeddings_computed','0')
            return
        model=self._compatible_model()
        store=self._semantic_store(model)
        try:
            for kind in (*KINDS.values(),'Contribution',*(['ResearchArea'] if self.graph_policy=='domain-v1' else [])):
                index=store._index_for(kind)
                if index._vectors:
                    first=next(iter(index._vectors.values()))
                    if not index.search(first,1):raise RuntimeError('Dense index query failed')
            with self.db.transaction():
                self._set_meta('index_status','verified')
                self._set_meta('index_model_id',model.model_id)
                self._set_meta('index_builds',str(store.stats['dense_index_builds']))
                self._set_meta('index_cache_hits',str(store.stats['embedding_cache_hits']))
                self._set_meta('index_embeddings_computed',str(store.stats['embeddings_computed']))
        finally:store.close()

    def run(self):
        # One graph driver per logical run; external naming workers use independent
        # row leases and may continue claiming/submitting on their own connections.
        lock_id=int.from_bytes(hashlib.sha256(('graph-driver:'+self.run_id).encode()).digest()[:8],'big',signed=True)
        if not self.db.execute('SELECT pg_try_advisory_lock(%s) AS locked',(lock_id,)).fetchone()['locked']:
            raise RuntimeError('Graph run already has an active driver')
        try:return self._run_locked()
        finally:self.db.execute('SELECT pg_advisory_unlock(%s)',(lock_id,))

    def _run_locked(self):
        if self._meta('neo4j_run_id') and self.publisher is None:raise ValueError('Neo4j publisher required to run this job')
        self._drain()
        phase=self._meta('phase')
        if phase=='new':self._timed('snapshot_import_cpu',self._prepare_store);phase='imported'
        if phase=='imported':self._timed('candidate_graph_cpu',self._candidates);phase='candidates'
        if phase=='candidates':self._timed('jev_pair_wall',self._judge);phase='judged'
        if phase=='judged':
            self._timed('jev_member_wall' if self.graph_policy=='domain-v1' else 'jev_group_wall',
                        self._groups_domain if self.graph_policy=='domain-v1' else self._groups)
            phase='grouped'
        if phase=='grouped':
            if self.name_mode=='api':self._timed('name_api_wall',self._api_names)
            if self.graph_policy=='domain-v1':self._timed('jev_area_member_wall',self._review_area_members)
            elif not self.db.execute("SELECT 1 FROM tasks WHERE status!='done' LIMIT 1").fetchone():
                self._timed('jev_final_name_wall',self._final_name_review)
            self._timed('materialize_cpu',self._materialize_domain if self.graph_policy=='domain-v1' else self._materialize)
        if self._meta('phase')=='complete' and self._meta('index_status')!='verified':
            self._timed('retrieval_index_cpu',self._build_indexes)
        self._drain()
        if self._meta('phase')=='complete' and self._meta('index_status')=='verified' and not self._meta('completed_at'):
            with self.db.transaction():self._set_meta('completed_at',str(time.time()))
        return self.status()

    def status(self):
        counts={r['decision']:r['n'] for r in self.db.execute('SELECT decision,count(*) n FROM candidates GROUP BY decision')}
        groups={r['review']:r['n'] for r in self.db.execute('SELECT review,count(*) n FROM groups GROUP BY review')}
        tasks={r['status']:r['n'] for r in self.db.execute('SELECT status,count(*) n FROM tasks GROUP BY status')}
        mention_counts={}
        if self._meta('phase')!='new':
            store=self._semantic_store()
            try:mention_counts={r['decision']:r['n'] for r in store.db.execute('SELECT decision,count(*) n FROM mentions GROUP BY decision')}
            finally:store.close()
        intervals=[]
        logical_intervals=[]
        for row in self.db.execute('SELECT started_at,finished_at,http_intervals_json FROM jev_requests'):
            if row['started_at'] is not None and row['finished_at'] is not None and row['finished_at']>=row['started_at']:
                logical_intervals.append((row['started_at'],row['finished_at']))
            if row['http_intervals_json']:
                for item in json.loads(row['http_intervals_json']):
                    a,b=item.get('started_at'),item.get('finished_at')
                    if isinstance(a,(int,float)) and isinstance(b,(int,float)) and b>=a:
                        intervals.append((a,b))
        intervals.sort()
        events=sorted([(a,1) for a,b in intervals]+[(b,-1) for a,b in intervals])
        active=peak=0
        union=0.0
        previous=None
        for instant,delta in events:
            if previous is not None and active:union+=instant-previous
            active+=delta;peak=max(peak,active);previous=instant
        stats=getattr(self.client,'concurrency_stats',None)
        memberships={r['excluded']:r['n'] for r in self.db.execute('SELECT excluded,count(*) n FROM member_reviews GROUP BY excluded')}
        area_count=0
        if self.graph_policy=='domain-v1' and self._meta('phase')=='complete':
            area_count=self.db.execute('SELECT count(*) FROM research_areas').fetchone()['count']
        delivered=int(self._meta('neo4j_delivered_count') or 0)
        projection={1:delivered,0:int(self._meta('neo4j_event_count') or 0)-delivered}
        return {'neo4j_run_id':self._meta('neo4j_run_id'),'neo4j_projection':{'pending':projection.get(0,0),'delivered':projection.get(1,0),
            'attempts':int(self._meta('neo4j_delivery_attempts') or 0),'delivery_wall_seconds':float(self._meta('neo4j_delivery_wall_seconds') or 0),
            **{key:float(self._meta('neo4j_'+key)) if self._meta('neo4j_'+key) else None for key in ('first_delivery_at','first_source_delivery_at','first_group_delivery_at','latest_delivery_at')}},'phase':self._meta('phase'),'graph_policy':self.graph_policy,'decision_metric':self.decision_metric,'context_format':self.context_format,
                'problem_resolution':self.problem_resolution,'entity_same_min_probability':self.entity_same_min_probability,
                'snapshot_hash':self._meta('snapshot_hash'),
                'research_areas':area_count,'retained_memberships':memberships.get(0,0),
                'excluded_memberships':memberships.get(1,0),'identity_conflicts':json.loads(self._meta('identity_conflicts') or '[]'),
                'candidate_pairs':counts,'groups':groups,'name_tasks':tasks,'mentions':mention_counts,
                'stage_seconds':{r['name']:r['seconds'] for r in self.db.execute("SELECT * FROM stage WHERE name NOT LIKE 'process_cpu:%'")},
                'stage_process_cpu_seconds':{r['name'][12:]:r['seconds'] for r in self.db.execute("SELECT * FROM stage WHERE name LIKE 'process_cpu:%'")},
                'pending_pairs':counts.get('pending',0),'pending_groups':groups.get('pending',0),
                'pending_mentions':mention_counts.get('pending',0),
                'started_at':float(self._meta('started_at')) if self._meta('started_at') else None,
                'completed_at':float(self._meta('completed_at')) if self._meta('completed_at') else None,
                'elapsed_wall_seconds':(float(self._meta('completed_at') or time.time())-float(self._meta('started_at'))) if self._meta('started_at') else None,
                'jev_request_count':self.db.execute('SELECT count(*) FROM jev_requests').fetchone()['count'],
                'jev_request_peak_concurrency':peak,
                'jev_request_wall_union_seconds':union,
                'jev_logical_request_intervals':len(logical_intervals),
                'jev_http_attempt_intervals':len(intervals),
                'jev_client_concurrency':stats if isinstance(stats,dict) else None,
                'index_status':self._meta('index_status'),
                'index_builds':int(self._meta('index_builds') or 0),'index_cache_hits':int(self._meta('index_cache_hits') or 0),
                'index_embeddings_computed':int(self._meta('index_embeddings_computed') or 0)}
