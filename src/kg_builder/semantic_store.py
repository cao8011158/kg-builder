"""Incremental, human reviewed semantic registry in a separate SQLite sidecar.

The metadata/extraction master is opened read-only. No similarity score changes identity.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from .extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction

KINDS = {'research_problems': 'ResearchProblem', 'methods': 'Method', 'benchmarks': 'Benchmark'}
PACKET_VERSION = 'research-kg-semantic-packet/1'
REVIEW_VERSION = 'research-kg-semantic-review/1'


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':')).encode()).hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


class SemanticStore:
    def __init__(self, path, model=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.executescript('''
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
        CREATE TABLE IF NOT EXISTS reviews (review_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS embedding_cache (
          model_id TEXT NOT NULL, text_hash TEXT NOT NULL, vector_json TEXT NOT NULL,
          PRIMARY KEY(model_id,text_hash));
        CREATE VIRTUAL TABLE IF NOT EXISTS entity_fts USING fts5(entity_id UNINDEXED,name,description,aliases);
        CREATE VIRTUAL TABLE IF NOT EXISTS contribution_fts USING fts5(contribution_id UNINDEXED,description);
        ''')
        self.db.commit()
        self.model = model
        self._indexes = {}
        self._index_model = None
        self.stats = {'embedding_cache_hits': 0, 'embeddings_computed': 0,
                      'dense_index_builds': 0, 'detail_rows': 0}

    def close(self):
        self.db.close()

    def _model_id(self):
        if self.model is None:
            raise RuntimeError('Dense operations require an embedding model')
        return self.model.model_id

    def _embed(self, texts):
        model_id = self._model_id()
        result = [None] * len(texts)
        missing = {}
        for i, value in enumerate(texts):
            key = digest(value)
            row = self.db.execute('SELECT vector_json FROM embedding_cache WHERE model_id=? AND text_hash=?',
                                  (model_id, key)).fetchone()
            if row:
                result[i] = json.loads(row[0]); self.stats['embedding_cache_hits'] += 1
            else:
                missing.setdefault(key, (value, []) )[1].append(i)
        if missing:
            keys = list(missing)
            vectors = self.model.encode([missing[k][0] for k in keys])
            for key, vector in zip(keys, vectors):
                values = [float(x) for x in vector]
                self.db.execute('INSERT OR IGNORE INTO embedding_cache VALUES(?,?,?)',
                                (model_id, key, _json(values)))
                for i in missing[key][1]: result[i] = values
            self.db.commit()
            self.stats['embeddings_computed'] += len(keys)
        return result

    def _index_for(self, kind):
        from .vector_index import HNSWIndex
        model_id = self._model_id()
        if self._index_model != model_id:
            self._indexes.clear()
            self._index_model = model_id
        if kind not in self._indexes:
            rows = self.db.execute('SELECT entity_id,name,description FROM entities WHERE kind=? ORDER BY entity_id',
                                   (kind,)).fetchall()
            vectors = self._embed([r['name'] + '\n' + r['description'] for r in rows])
            self._indexes[kind] = HNSWIndex(dict(zip([r['entity_id'] for r in rows], vectors)), model_id=model_id)
            self.stats['dense_index_builds'] += 1
        return self._indexes[kind]

    def _sparse_entities(self, query, kind, limit=100):
        import re
        terms=re.findall(r'[^\W_]+',query,flags=re.UNICODE)
        if not terms:return []
        expression=' OR '.join('"'+term.replace('"','')+'"' for term in terms[:16])
        return [r['entity_id'] for r in self.db.execute('''SELECT f.entity_id FROM entity_fts f
            JOIN entities e ON e.entity_id=f.entity_id WHERE entity_fts MATCH ? AND e.kind=?
            ORDER BY bm25(entity_fts) LIMIT ?''',(expression,kind,limit))]

    def _rank_entities(self, query, kind, limit):
        exact=[r['entity_id'] for r in self.db.execute('''SELECT entity_id FROM entities
            WHERE kind=? AND name=? ORDER BY entity_id LIMIT ?''',(kind,query.strip(),limit))]
        sparse=self._sparse_entities(query,kind,max(100,limit))
        dense=[]
        if self.model:
            dense=[h['id'] for h in self._index_for(kind).search(self._embed([query])[0],k=max(100,limit))]
        scores={}
        for ranking,weight in ((exact,3),(sparse,1),(dense,1)):
            for i,eid in enumerate(ranking):scores[eid]=scores.get(eid,0)+weight/(60+i+1)
        return sorted(scores,key=lambda eid:(-scores[eid],eid))[:limit],scores

    def ingest(self, master_path, extraction_ids):
        """Import only explicitly chosen extractions, replacing changed sources atomically."""
        ids = list(extraction_ids)
        if not ids or len(ids) != len(set(ids)):
            raise ValueError('Provide distinct explicit extraction IDs')
        master = sqlite3.connect(f'file:{Path(master_path).resolve().as_posix()}?mode=ro', uri=True)
        master.row_factory = sqlite3.Row
        imported = []
        try:
            records = []
            for eid in ids:
                row = master.execute('''SELECT x.*,p.title,v.source_url,v.version_label FROM extraction_records x
                    JOIN papers p ON p.paper_id=x.paper_id JOIN paper_versions v ON v.version_id=x.version_id
                    WHERE x.extraction_id=?''', (eid,)).fetchone()
                if row is None: raise ValueError(f'Unknown extraction ID: {eid}')
                data = json.loads(row['payload_json'])
                if validate_extraction(data) != RETRIEVAL_SCHEMA_VERSION:
                    raise ValueError(f'Extraction {eid} is not a retrieval extraction')
                if data['paper_id'] != row['paper_id'] or data['version_id'] != row['version_id']:
                    raise ValueError(f'Extraction {eid} identity mismatch')
                if data['paper_version'] != {'source_url': row['source_url'], 'version_label': row['version_label']}:
                    raise ValueError(f'Extraction {eid} version pin mismatch')
                source_hash = digest({'payload': data, 'status': row['status'], 'source_scope': row['source_scope'],
                                      'title': row['title'], 'source_url': row['source_url'],
                                      'version_label': row['version_label']})
                records.append((row, data, source_hash))
            with self.db:
                for row, data, source_hash in records:
                    eid = row['extraction_id']
                    previous = self.db.execute('SELECT source_hash FROM sources WHERE extraction_id=?', (eid,)).fetchone()
                    if previous and previous[0] == source_hash:
                        imported.append({'extraction_id': eid, 'status': 'unchanged'}); continue
                    # The source is replaced in one transaction; old decisions become invisible.
                    if previous:
                        self.db.execute('DELETE FROM mentions WHERE extraction_id=?', (eid,))
                        self.db.execute('DELETE FROM contribution_fts WHERE contribution_id IN '
                                        '(SELECT contribution_id FROM contributions WHERE extraction_id=?)', (eid,))
                        self.db.execute('DELETE FROM contributions WHERE extraction_id=?', (eid,))
                        self.db.execute('DELETE FROM sources WHERE extraction_id=?', (eid,))
                    self.db.execute('INSERT INTO sources VALUES(?,?,?,?,?,?,?)',
                                    (eid,row['paper_id'],row['version_id'],row['title'],row['source_url'],
                                     row['version_label'],source_hash))
                    for field, kind in KINDS.items():
                        for item in data[field]:
                            mid = 'mention:' + digest([eid, field, item['key']])[:32]
                            mh = digest(item)
                            name = item['name']
                            roles = item.get('roles', [item['role']] if 'role' in item else [])
                            self.db.execute('INSERT INTO mentions VALUES(?,?,?,?,?,?,?,?,?,?,NULL,?)',
                                (mid,eid,kind,item['key'],name,item['description'],_json(roles),
                                 item.get('version_or_subset'),_json(item),mh,'pending'))
                    for item in data['contributions']:
                        cid = 'contribution:' + digest([eid,item['key']])[:32]
                        self.db.execute('INSERT INTO contributions VALUES(?,?,?,?,?)',
                                        (cid,eid,item['key'],item['description'],_json(item)))
                        self.db.execute('INSERT INTO contribution_fts VALUES(?,?)', (cid,item['description']))
                    imported.append({'extraction_id': eid, 'status': 'replaced' if previous else 'added',
                                     'source_hash': source_hash})
        finally:
            master.close()
        return imported

    def _pending(self, kind=None, limit=100, offset=0):
        sql = '''SELECT m.*,s.source_hash,s.paper_id,s.version_id,s.title,s.source_url FROM mentions m
                 JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.decision='pending' '''
        args=[]
        if kind: sql += ' AND m.kind=?'; args.append(kind)
        sql += ' ORDER BY m.mention_id LIMIT ? OFFSET ?'; args += [limit,offset]
        return self.db.execute(sql,args).fetchall()

    def _candidate(self, row, k=8):
        query = row['name']+'\n'+row['description']
        ids,scores = self._rank_entities(query,row['kind'],k)
        found = []
        for eid in ids:
            entity = self.db.execute('SELECT entity_id,kind,name,description,revision FROM entities WHERE entity_id=?',
                                     (eid,)).fetchone()
            found.append({**dict(entity),'score':scores[eid]})
        return found

    def export_candidates(self, kind=None, limit=100, offset=0, k=8):
        if kind and kind not in KINDS.values(): raise ValueError('Unknown kind')
        self._bound(limit,offset)
        if not isinstance(k,int) or not 1<=k<=100:raise ValueError('k must be 1..100')
        active_model=self.model.model_id if self.model else None
        with self.db:
            self.db.execute('INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                            ('review_model_id',_json(active_model)))
        rows=self._pending(kind,limit+1,offset)
        packets=[]
        for row in rows[:limit]:
            candidates=self._candidate(row,k)
            precondition=digest([row['mention_id'],row['source_hash'],row['mention_hash'],
                                 self.model.model_id if self.model else None])
            packets.append({'mention_id':row['mention_id'],'kind':row['kind'],'item_key':row['item_key'],
                'name':row['name'],'description':row['description'],'roles':json.loads(row['role_json']),
                'version_or_subset':row['version_or_subset'],'source':{'extraction_id':row['extraction_id'],
                'paper_id':row['paper_id'],'version_id':row['version_id'],'title':row['title'],
                'source_url':row['source_url'],'source_hash':row['source_hash'],
                'item':json.loads(row['source_json'])},'mention_hash':row['mention_hash'],
                'precondition':precondition,'candidate_revisions':{c['entity_id']:c['revision'] for c in candidates},
                'candidates':candidates})
        return {'schema_version':PACKET_VERSION,'model_id':self.model.model_id if self.model else None,
                'mentions':packets,'next_offset':offset+limit if len(rows)>limit else None}

    def cluster_problems(self, k=20, min_similarity=.65, resolution=.05, seed=42):
        from .problem_clustering import cluster_problem_vectors
        rows=self.db.execute('''SELECT m.*,s.source_hash FROM mentions m JOIN sources s
            ON s.extraction_id=m.extraction_id WHERE m.decision='pending' AND m.kind='ResearchProblem'
            ORDER BY m.mention_id''').fetchall()
        vectors=self._embed([r['name']+'\n'+r['description'] for r in rows]) if rows else []
        result=cluster_problem_vectors(dict(zip([r['mention_id'] for r in rows],vectors)),
              k=k,min_similarity=min_similarity,resolution=resolution,seed=seed)
        # This is a proposed partition only. No identity assignment occurs.
        result['model_id']=self.model.model_id
        result['source_fingerprints']={r['mention_id']:{'source_hash':r['source_hash'],
            'mention_hash':r['mention_hash']} for r in rows}
        return result

    def import_review(self, review):
        if review.get('schema_version') != REVIEW_VERSION: raise ValueError('Unknown review schema')
        review_id=review.get('review_id')
        if not isinstance(review_id,str) or not review_id: raise ValueError('Missing review_id')
        entities=review.get('entities',[]); assignments=review.get('assignments',[])
        if not isinstance(entities,list) or not isinstance(assignments,list): raise ValueError('Invalid review lists')
        if len({e.get('entity_id') for e in entities}) != len(entities): raise ValueError('Duplicate entity ID')
        if len({a.get('mention_id') for a in assignments}) != len(assignments): raise ValueError('Duplicate mention ID')
        review_hash=digest(review)
        previous=self.db.execute('SELECT payload_hash FROM reviews WHERE review_id=?',(review_id,)).fetchone()
        if previous:
            if previous[0] == review_hash: return {'status':'unchanged','review_id':review_id}
            raise ValueError('Review ID conflict')
        active=self.db.execute('SELECT value FROM metadata WHERE key=?',('review_model_id',)).fetchone()
        if not active:raise ValueError('Export candidates before importing a review')
        active_model=json.loads(active[0])
        declared={}
        for e in entities:
            eid=e.get('entity_id');kind=e.get('kind')
            if not isinstance(eid,str) or not eid or kind not in KINDS.values(): raise ValueError('Invalid entity')
            if not isinstance(e.get('name'),str) or not e['name'].strip() or not isinstance(e.get('description'),str):
                raise ValueError('Invalid entity text')
            if not isinstance(e.get('aliases',[]),list) or any(not isinstance(x,str) for x in e.get('aliases',[])):
                raise ValueError('Invalid aliases')
            origin=e.get('name_origin','source_authored')
            if origin not in ('source_authored','reviewer_generated') or (kind!='ResearchProblem' and origin!='source_authored'):
                raise ValueError('Invalid name origin')
            if self.db.execute('SELECT 1 FROM entities WHERE entity_id=?',(eid,)).fetchone():
                raise ValueError(f'Entity ID already exists: {eid}')
            declared[eid]=e
        with self.db:
            new_entity_names={eid:[] for eid in declared}
            for a in assignments:
                if a.get('model_id')!=active_model:raise ValueError('Stale review model')
                mid=a.get('mention_id');row=self.db.execute('''SELECT m.*,s.source_hash FROM mentions m
                    JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.mention_id=?''',(mid,)).fetchone()
                if not row or row['decision']!='pending': raise ValueError(f'Unknown or decided mention: {mid}')
                expected=digest([mid,row['source_hash'],row['mention_hash'],a.get('model_id')])
                if a.get('precondition')!=expected: raise ValueError(f'Stale mention/model precondition: {mid}')
                revisions=a.get('candidate_revisions',{})
                if not isinstance(revisions,dict): raise ValueError('Invalid candidate revisions')
                for candidate,rev in revisions.items():
                    target=self.db.execute('SELECT revision FROM entities WHERE entity_id=?',(candidate,)).fetchone()
                    if not target or target[0]!=rev: raise ValueError(f'Stale candidate: {candidate}')
                decision=a.get('decision');eid=a.get('entity_id')
                if decision=='pending':
                    if eid is not None: raise ValueError('Pending decision must have null entity_id')
                elif decision=='new_entity':
                    if eid not in declared or declared[eid]['kind']!=row['kind']:
                        raise ValueError('New entity must be declared with matching kind')
                    new_entity_names[eid].append(row['name'])
                elif decision=='match':
                    target=self.db.execute('SELECT kind,revision FROM entities WHERE entity_id=?',(eid,)).fetchone()
                    if not target or target['kind']!=row['kind']: raise ValueError('Unknown or mismatched target entity')
                    if a.get('target_revision')!=target['revision']: raise ValueError('Stale target revision')
                else: raise ValueError('Invalid assignment decision')
            used={a['entity_id'] for a in assignments if a.get('decision')=='new_entity'}
            if used!=set(declared): raise ValueError('Every new entity must have an assignment')
            for eid,e in declared.items():
                if e.get('name_origin','source_authored')=='source_authored':
                    names=new_entity_names[eid]
                    if e['name'] not in names or any(name!=e['name'] and name not in e.get('aliases',[])
                                                     for name in names):
                        raise ValueError('Source-authored name needs a matching mention; variant names need aliases')
            for eid,e in declared.items():
                revision=digest([e['kind'],e['name'],e['description'],e.get('aliases',[]),e.get('name_origin','source_authored')])
                self.db.execute('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',
                    (eid,e['kind'],e['name'],e['description'],e.get('name_origin','source_authored'),
                     _json(e.get('aliases',[])),revision))
                self.db.execute('INSERT INTO entity_fts VALUES(?,?,?,?)',
                    (eid,e['name'],e['description'],' '.join(e.get('aliases',[]))))
            for a in assignments:
                if a['decision']!='pending':
                    self.db.execute('UPDATE mentions SET decision=?,entity_id=? WHERE mention_id=?',
                                    (a['decision'],a['entity_id'],a['mention_id']))
            self.db.execute('INSERT INTO reviews VALUES(?,?)',(review_id,review_hash))
        index_status='current'
        if declared and self.model and self._index_model == self.model.model_id:
            for kind,index in list(self._indexes.items()):
                new_ids=[eid for eid in declared if declared[eid]['kind']==kind]
                if new_ids:
                    try:
                        vectors=self._embed([declared[eid]['name']+'\n'+declared[eid]['description'] for eid in new_ids])
                        for eid,vector in zip(new_ids,vectors):index.upsert(eid,vector)
                    except Exception:
                        # SQLite review is authoritative; never retain a partial in-memory index.
                        self._indexes.pop(kind,None)
                        index_status='rebuild_required'
        return {'status':'imported','review_id':review_id,'entities':len(declared),
                'assignments':sum(a['decision']!='pending' for a in assignments),
                'index_status':index_status}

    @staticmethod
    def _bound(limit,offset):
        if not isinstance(limit,int) or not 1<=limit<=100 or not isinstance(offset,int) or offset<0:
            raise ValueError('limit must be 1..100 and offset nonnegative')

    def get_entity(self,entity_id,limit=20,offset=0):
        self._bound(limit,offset)
        entity=self.db.execute('SELECT * FROM entities WHERE entity_id=?',(entity_id,)).fetchone()
        if not entity:return None
        rows=self.db.execute('''SELECT m.mention_id,m.name,m.description,m.role_json,m.version_or_subset,
            s.paper_id,s.version_id,s.title,s.source_url FROM mentions m JOIN sources s
            ON s.extraction_id=m.extraction_id WHERE m.entity_id=? ORDER BY m.mention_id LIMIT ? OFFSET ?''',
            (entity_id,limit+1,offset)).fetchall()
        self.stats['detail_rows']+=1+len(rows)
        members=[{**dict(r),'roles':json.loads(r['role_json'])} for r in rows[:limit]]
        for member in members:del member['role_json']
        return {**dict(entity),'aliases':json.loads(entity['aliases_json']),
                'mentions':members,'next_offset':offset+limit if len(rows)>limit else None}

    def get_paper(self,paper_id,limit=20,offset=0):
        self._bound(limit,offset)
        row=self.db.execute('SELECT paper_id,title FROM sources WHERE paper_id=? LIMIT 1',(paper_id,)).fetchone()
        if not row:return None
        members=self.db.execute('''SELECT m.mention_id,m.kind,m.name,m.entity_id,m.role_json,m.version_or_subset,
            s.version_id,s.source_url FROM sources s JOIN mentions m ON m.extraction_id=s.extraction_id
            WHERE s.paper_id=? ORDER BY m.mention_id LIMIT ? OFFSET ?''',(paper_id,limit+1,offset)).fetchall()
        contributions=self.db.execute('''SELECT c.contribution_id,c.item_key,c.description,s.version_id,s.source_url
            FROM sources s JOIN contributions c ON c.extraction_id=s.extraction_id
            WHERE s.paper_id=? ORDER BY c.contribution_id LIMIT ? OFFSET ?''',
            (paper_id,limit+1,offset)).fetchall()
        self.stats['detail_rows']+=1+len(members)+len(contributions)
        values=[{**dict(m),'roles':json.loads(m['role_json'])} for m in members[:limit]]
        for v in values:del v['role_json']
        return {**dict(row),'mentions':values,'next_offset':offset+limit if len(members)>limit else None,
                'contributions':[dict(c) for c in contributions[:limit]],
                'contributions_next_offset':offset+limit if len(contributions)>limit else None}

    def search(self,query,limit=10,offset=0,kind=None,expand_limit=3):
        self._bound(limit,offset)
        if not isinstance(query,str) or not query.strip(): raise ValueError('Nonempty query required')
        if kind and kind not in (*KINDS.values(),'Contribution'): raise ValueError('Unknown kind')
        if not isinstance(expand_limit,int) or not 0<=expand_limit<=20: raise ValueError('Invalid expand_limit')
        scores={}
        if kind!='Contribution':
            for entity_kind in ([kind] if kind else list(KINDS.values())):
                ids,ranking=self._rank_entities(query,entity_kind,100)
                for eid in ids:scores[('entity',eid)]=ranking[eid]
        if kind in (None,'Contribution'):
            import re
            terms=re.findall(r'[^\W_]+',query,flags=re.UNICODE)
            if terms:
                expression=' OR '.join('"'+term.replace('"','')+'"' for term in terms[:16])
                for rank,row in enumerate(self.db.execute('''SELECT contribution_id FROM contribution_fts
                    WHERE contribution_fts MATCH ? ORDER BY bm25(contribution_fts) LIMIT 100''',(expression,))):
                    scores[('contribution',row['contribution_id'])]=1/(60+rank+1)
        found=[]
        for (typ,key),score in sorted(scores.items(),key=lambda pair:(-pair[1],pair[0])):
            if typ=='entity':
                row=self.db.execute('SELECT entity_id,kind,name,description,aliases_json FROM entities WHERE entity_id=?',(key,)).fetchone()
                if not row or (kind and row['kind']!=kind):continue
                found.append(('entity',key,score))
            else:found.append(('contribution',key,score))
            if len(found)>=offset+limit+1:break
        results=[]
        for typ,key,score in found[offset:offset+limit]:
            if typ=='entity':
                row=self.db.execute('SELECT entity_id,kind,name,description FROM entities WHERE entity_id=?',(key,)).fetchone()
                papers=self.db.execute('''SELECT DISTINCT s.paper_id,s.title,s.version_id,s.source_url FROM mentions m
                    JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.entity_id=? ORDER BY s.paper_id LIMIT ?''',
                    (key,expand_limit+1)).fetchall()
                results.append({**dict(row),'score':score,'papers':[dict(x) for x in papers[:expand_limit]],
                                'papers_truncated':len(papers)>expand_limit})
            else:
                row=self.db.execute('''SELECT c.contribution_id,c.description,s.paper_id,s.version_id,s.title,s.source_url
                    FROM contributions c JOIN sources s ON s.extraction_id=c.extraction_id WHERE c.contribution_id=?''',(key,)).fetchone()
                if row:results.append({**dict(row),'kind':'Contribution','score':score})
        return {'results':results,'next_offset':offset+limit if len(found)>offset+limit else None}
