"""Human reviewed PostgreSQL semantic registry.

Native tsvector lexical ranks join dense and exact-name ranks through RRF;
these are not BM25 scores. Similarity never changes identity.
"""
from __future__ import annotations

import hashlib
import json

from .extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction

KINDS = {'research_problems': 'ResearchProblem', 'methods': 'Method', 'benchmarks': 'Benchmark'}
PACKET_VERSION = 'research-kg-semantic-packet/1'
REVIEW_VERSION = 'research-kg-semantic-review/1'


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':')).encode()).hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _surface(value):
    return ' '.join(value.split()).casefold()


class SemanticStore:
    def __init__(self, run_id, model=None, *, database_url=None, readonly=False, initialize=True):
        from .postgres_graph import GraphStore
        self.store = GraphStore(run_id, database_url=database_url, readonly=readonly, initialize=initialize)
        self.run_id = run_id
        self.readonly = readonly
        self.db = self.store.db
        self.model = model
        self._indexes = {}
        self._index_model = None
        self._readonly_embeddings = {}
        self.stats = {'embedding_cache_hits': 0, 'embeddings_computed': 0,
                      'dense_index_builds': 0, 'detail_rows': 0}

    def close(self):
        self.store.close()

    def _insert_surfaces(self,eid,kind,name,aliases):
        for surface in {name,*aliases}:
            self.db.execute('INSERT INTO entity_surfaces VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                            (kind,_surface(surface),eid))

    def _model_id(self):
        if self.model is None:
            raise RuntimeError('Dense operations require an embedding model')
        return self.model.model_id

    def _embed(self, texts, *, query=False):
        document_id = self._model_id()
        model_id = getattr(self.model, 'query_model_id', document_id) if query else document_id
        result = [None] * len(texts)
        missing = {}
        for i, value in enumerate(texts):
            key = digest(value)
            cached = self._readonly_embeddings.get((model_id,key)) if self.readonly else None
            if cached is not None:
                result[i] = cached
                self.stats['embedding_cache_hits'] += 1
                continue
            row = self.db.execute('SELECT vector_json FROM embedding_cache WHERE model_id=%s AND text_hash=%s',
                                  (model_id, key)).fetchone()
            if row:
                result[i] = json.loads(row['vector_json']); self.stats['embedding_cache_hits'] += 1
            else:
                missing.setdefault(key, (value, []) )[1].append(i)
        if missing:
            keys = list(missing)
            encode = getattr(self.model, 'encode_query', self.model.encode) if query else self.model.encode
            vectors = encode([missing[k][0] for k in keys])
            for key, vector in zip(keys, vectors):
                values = [float(x) for x in vector]
                if not self.readonly: self.db.execute('INSERT INTO embedding_cache VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                                (model_id, key, _json(values)))
                if self.readonly:self._readonly_embeddings[(model_id,key)] = values
                for i in missing[key][1]: result[i] = values
            self.stats['embeddings_computed'] += len(keys)
        return result

    def _embed_query(self, texts):
        return self._embed(texts, query=True)

    def _index_for(self, kind):
        from .vector_index import HNSWIndex
        model_id = self._model_id()
        if self._index_model != model_id:
            self._indexes.clear()
            self._index_model = model_id
        if kind not in self._indexes:
            if kind=='Contribution':
                rows=self.db.execute('SELECT contribution_id,description FROM contributions ORDER BY contribution_id').fetchall()
                ids=[r['contribution_id'] for r in rows]
                texts=[r['description'] for r in rows]
            elif kind=='ResearchArea':
                rows=self.db.execute('SELECT area_id,name,description FROM research_areas ORDER BY area_id').fetchall()
                ids=[r['area_id'] for r in rows]
                texts=[r['name'] + '\n' + r['description'] for r in rows]
            elif kind=='Paper':
                from .paper_dense import _papers
                rows=_papers(self.db)
                ids=[r['node_id'] for r in rows]
                texts=[r['title'] + '\n' + (r['abstract'] or '') for r in rows]
            else:
                rows = self.db.execute('SELECT entity_id,name,description FROM entities WHERE kind=%s ORDER BY entity_id',
                                       (kind,)).fetchall()
                ids=[r['entity_id'] for r in rows]
                texts=[r['name'] + '\n' + r['description'] for r in rows]
            vectors = self._embed(texts)
            self._indexes[kind] = HNSWIndex(dict(zip(ids, vectors)), model_id=model_id)
            self.stats['dense_index_builds'] += 1
        return self._indexes[kind]

    def _sparse_entities(self, query, kind, limit=100):
        import re
        terms=re.findall(r'[^\W_]+',query,flags=re.UNICODE)
        if not terms:return []
        expression=' | '.join("'"+term.replace("'","''")+"'" for term in terms[:16])
        return [r['entity_id'] for r in self.db.execute('''SELECT f.entity_id FROM entity_fts f
            JOIN entities e ON e.entity_id=f.entity_id WHERE f.search_vector @@ to_tsquery('simple',%s) AND e.kind=%s
            ORDER BY ts_rank_cd(f.search_vector,to_tsquery('simple',%s)) DESC,f.entity_id LIMIT %s''',(expression,kind,expression,limit))]

    def _rank_entities(self, query, kind, limit, surface_name=None):
        exact=[r['entity_id'] for r in self.db.execute('''SELECT entity_id FROM entity_surfaces
            WHERE kind=%s AND normalized_name=%s ORDER BY entity_id LIMIT %s''',
            (kind,_surface(surface_name if surface_name is not None else query),limit))]
        sparse=self._sparse_entities(query,kind,max(100,limit))
        dense=[]
        if self.model:
            dense=[h['id'] for h in self._index_for(kind).search(self._embed_query([query])[0],k=max(100,limit))]
        scores={}
        for ranking,weight in ((exact,3),(sparse,1),(dense,1)):
            for i,eid in enumerate(ranking):scores[eid]=scores.get(eid,0)+weight/(60+i+1)
        return sorted(scores,key=lambda eid:(-scores[eid],eid))[:limit],scores

    def _rank_contributions(self, query, limit=100):
        import re
        terms=re.findall(r'[^\W_]+',query,flags=re.UNICODE)
        sparse=[]
        if terms:
            expression=' | '.join("'"+term.replace("'","''")+"'" for term in terms[:16])
            sparse=[r['contribution_id'] for r in self.db.execute('''SELECT contribution_id FROM contribution_fts
                WHERE search_vector @@ to_tsquery('simple',%s) ORDER BY ts_rank_cd(search_vector,to_tsquery('simple',%s)) DESC,contribution_id LIMIT %s''',(expression,expression,limit))]
        dense=[]
        if self.model:
            dense=[h['id'] for h in self._index_for('Contribution').search(self._embed_query([query])[0],k=limit)]
        scores={}
        for ranking in (sparse,dense):
            for i,cid in enumerate(ranking):scores[cid]=scores.get(cid,0)+1/(60+i+1)
        return sorted(scores,key=lambda cid:(-scores[cid],cid))[:limit],scores

    def _rank_areas(self, query, limit=100):
        if not self.db.execute('SELECT 1 FROM research_areas LIMIT 1').fetchone():return [],{}
        import re
        terms=re.findall(r'[^\W_]+',query,flags=re.UNICODE)
        sparse=[]
        if terms:
            expression=' | '.join("'"+term.replace("'","''")+"'" for term in terms[:16])
            sparse=[r['area_id'] for r in self.db.execute('''SELECT area_id FROM research_area_fts
                WHERE search_vector @@ to_tsquery('simple',%s) ORDER BY ts_rank_cd(search_vector,to_tsquery('simple',%s)) DESC,area_id LIMIT %s''',(expression,expression,limit))]
        dense=[]
        if self.model:
            dense=[h['id'] for h in self._index_for('ResearchArea').search(self._embed_query([query])[0],k=limit)]
        scores={}
        for ranking in (sparse,dense):
            for i,area_id in enumerate(ranking):scores[area_id]=scores.get(area_id,0)+1/(60+i+1)
        return sorted(scores,key=lambda area_id:(-scores[area_id],area_id))[:limit],scores

    def import_research_areas(self, areas, memberships):
        """Import area summaries and retained source problem members atomically."""
        if not isinstance(areas,list) or not isinstance(memberships,list) or len(areas)>1000 or len(memberships)>10000:
            raise ValueError('Bounded area and membership lists required')
        area_ids=[]; member_keys=[]
        for area in areas:
            if not isinstance(area,dict) or not isinstance(area.get('area_id'),str) or not area['area_id'].startswith('area:') or not isinstance(area.get('name'),str) or not area['name'].strip() or not isinstance(area.get('description'),str) or not area['description'].strip():
                raise ValueError('Invalid research area')
            area_ids.append(area['area_id'])
        for member in memberships:
            if not isinstance(member,dict) or not isinstance(member.get('area_id'),str) or not isinstance(member.get('mention_id'),str):
                raise ValueError('Invalid research area member')
            member_keys.append((member['area_id'],member['mention_id']))
        if len(set(area_ids))!=len(area_ids) or len(set(member_keys))!=len(member_keys):
            raise ValueError('Duplicate research area or member')
        declared_ids=set(area_ids)
        if any(area_id not in declared_ids for area_id,_ in member_keys):
            raise ValueError('Membership requires an area in this import')
        inserted=[]; added_members=0
        with self.db.transaction():
            for area in areas:
                area_id=area['area_id']
                revision=digest([area['name'],area['description']])
                existing=self.db.execute('SELECT name,description,revision FROM research_areas WHERE area_id=%s',(area_id,)).fetchone()
                if existing:
                    if existing['revision']!=revision or existing['name']!=area['name'] or existing['description']!=area['description']:
                        raise ValueError(f'Research area ID conflict: {area_id}')
                    continue
                self.db.execute('INSERT INTO research_areas VALUES(%s,%s,%s,%s)',
                                (area_id,area['name'],area['description'],revision))
                self.db.execute('INSERT INTO research_area_fts(area_id,name,description) VALUES(%s,%s,%s)',
                                (area_id,area['name'],area['description']))
                inserted.append(area)
            for area_id,mid in member_keys:
                row=self.db.execute('''SELECT m.kind,m.entity_id,m.decision,m.name,e.kind AS entity_kind,
                    e.name AS entity_name,e.name_origin FROM mentions m
                    LEFT JOIN entities e ON e.entity_id=m.entity_id WHERE m.mention_id=%s''',(mid,)).fetchone()
                if not row or row['kind']!='ResearchProblem' or row['decision']=='pending' or row['entity_kind']!='ResearchProblem' or row['name_origin']!='source_authored' or row['entity_name']!=row['name']:
                    raise ValueError(f'Area member must be a decided source ResearchProblem: {mid}')
                owners=self.db.execute('SELECT mention_id FROM mentions WHERE entity_id=%s LIMIT 2',
                                       (row['entity_id'],)).fetchall()
                if len(owners)!=1 or owners[0]['mention_id']!=mid:
                    raise ValueError(f'Area member must retain an individual ResearchProblem: {mid}')
                if not self.db.execute('SELECT 1 FROM research_area_members WHERE area_id=%s AND mention_id=%s',
                                       (area_id,mid)).fetchone():
                    self.db.execute('INSERT INTO research_area_members VALUES(%s,%s)',(area_id,mid))
                    added_members+=1
        index_status='current'
        if inserted and self.model and self._index_model==self.model.model_id and 'ResearchArea' in self._indexes:
            try:
                vectors=self._embed([a['name']+'\n'+a['description'] for a in inserted])
                for area,vector in zip(inserted,vectors):self._indexes['ResearchArea'].upsert(area['area_id'],vector)
            except Exception:
                self._indexes.pop('ResearchArea',None)
                index_status='rebuild_required'
        return {'status':'imported' if inserted or added_members else 'unchanged',
                'areas':len(inserted),'memberships':added_members,'index_status':index_status}

    def ingest(self, source_run_id, extraction_ids):
        """Import only explicitly chosen extractions, replacing changed sources atomically."""
        ids = list(extraction_ids)
        if not ids or len(ids) != len(set(ids)):
            raise ValueError('Provide distinct explicit extraction IDs')
        from .postgres_graph import GraphStore
        source = GraphStore(source_run_id, database_url=self.store.database_url, readonly=True, initialize=False)
        master = source.db
        imported = []
        contribution_deltas=[]
        try:
            records = []
            for eid in ids:
                row = master.execute('''SELECT x.*,p.title,v.source_url,v.version_label FROM extraction_records x
                    JOIN papers p ON p.paper_id=x.paper_id JOIN paper_versions v ON v.version_id=x.version_id
                    WHERE x.extraction_id=%s''', (eid,)).fetchone()
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
            with self.db.transaction():
                self.db.execute('SELECT extraction_id FROM sources WHERE extraction_id=ANY(%s) '
                                'ORDER BY extraction_id FOR UPDATE',(sorted(ids),)).fetchall()
                for row, data, source_hash in records:
                    eid = row['extraction_id']
                    previous = self.db.execute('SELECT source_hash FROM sources WHERE extraction_id=%s', (eid,)).fetchone()
                    if previous and previous['source_hash'] == source_hash:
                        imported.append({'extraction_id': eid, 'status': 'unchanged'}); continue
                    # The source is replaced in one transaction; old decisions become invisible.
                    if previous:
                        old_ids=[r['contribution_id'] for r in self.db.execute(
                            'SELECT contribution_id FROM contributions WHERE extraction_id=%s',(eid,))]
                        self.db.execute('DELETE FROM mentions WHERE extraction_id=%s', (eid,))
                        self.db.execute('DELETE FROM contribution_fts WHERE contribution_id IN '
                                        '(SELECT contribution_id FROM contributions WHERE extraction_id=%s)', (eid,))
                        self.db.execute('DELETE FROM contributions WHERE extraction_id=%s', (eid,))
                        self.db.execute('DELETE FROM sources WHERE extraction_id=%s', (eid,))
                    self.db.execute('INSERT INTO sources VALUES(%s,%s,%s,%s,%s,%s,%s)',
                                    (eid,row['paper_id'],row['version_id'],row['title'],row['source_url'],
                                     row['version_label'],source_hash))
                    for field, kind in KINDS.items():
                        for item in data[field]:
                            mid = 'mention:' + digest([eid, field, item['key']])[:32]
                            mh = digest(item)
                            name = item['name']
                            roles = item.get('roles', [item['role']] if 'role' in item else [])
                            self.db.execute('INSERT INTO mentions VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s)',
                                (mid,eid,kind,item['key'],name,item['description'],_json(roles),
                                 item.get('version_or_subset'),_json(item),mh,'pending'))
                    for item in data['contributions']:
                        cid = 'contribution:' + digest([eid,item['key']])[:32]
                        self.db.execute('INSERT INTO contributions VALUES(%s,%s,%s,%s,%s)',
                                        (cid,eid,item['key'],item['description'],_json(item)))
                        self.db.execute('INSERT INTO contribution_fts(contribution_id,description) VALUES(%s,%s)', (cid,item['description']))
                        contribution_deltas.append((cid,item['description']))
                    if previous:
                        contribution_deltas.append((old_ids,None))
                    imported.append({'extraction_id': eid, 'status': 'replaced' if previous else 'added',
                                     'source_hash': source_hash})
        finally:
            source.close()
        if self.model and self._index_model != self.model.model_id:
            self._indexes.clear()
        if contribution_deltas and 'Contribution' in self._indexes:
            try:
                index=self._indexes['Contribution']
                for ids,description in contribution_deltas:
                    if description is None:
                        for cid in ids:index.delete(cid)
                new=[(cid,description) for cid,description in contribution_deltas if description is not None]
                vectors=self._embed([description for _,description in new])
                for (cid,_),vector in zip(new,vectors):index.upsert(cid,vector)
            except Exception:
                self._indexes.pop('Contribution',None)
                for result in imported:
                    if result['status']!='unchanged':result['index_status']='rebuild_required'
        return imported

    def _pending(self, kind=None, limit=100, offset=0):
        sql = '''SELECT m.*,s.source_hash,s.paper_id,s.version_id,s.title,s.source_url FROM mentions m
                 JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.decision='pending' '''
        args=[]
        if kind: sql += ' AND m.kind=%s'; args.append(kind)
        sql += ' ORDER BY m.mention_id LIMIT %s OFFSET %s'; args += [limit,offset]
        return self.db.execute(sql,args).fetchall()

    def _candidate(self, row, k=8):
        query = row['name']+'\n'+row['description']
        ids,scores = self._rank_entities(query,row['kind'],k,surface_name=row['name'])
        found = []
        for eid in ids:
            entity = self.db.execute('SELECT entity_id,kind,name,description,revision FROM entities WHERE entity_id=%s',
                                     (eid,)).fetchone()
            found.append({**dict(entity),'score':scores[eid]})
        return found

    def export_candidates(self, kind=None, limit=100, offset=0, k=8):
        if kind and kind not in KINDS.values(): raise ValueError('Unknown kind')
        self._bound(limit,offset)
        if not isinstance(k,int) or not 1<=k<=100:raise ValueError('k must be 1..100')
        active_model=self.model.model_id if self.model else None
        with self.db.transaction():
            self.db.execute('INSERT INTO metadata VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
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
            declared[eid]=e
        with self.db.transaction():
            # Serialize review identity even when its row does not exist yet.
            key=int.from_bytes(hashlib.sha256((self.store.schema+':review:'+review_id).encode()).digest()[:8],
                               'big',signed=True)
            self.db.execute('SELECT pg_advisory_xact_lock(%s)',(key,))
            previous=self.db.execute('SELECT payload_hash FROM reviews WHERE review_id=%s',(review_id,)).fetchone()
            if previous:
                if previous['payload_hash']==review_hash:return {'status':'unchanged','review_id':review_id}
                raise ValueError('Review ID conflict')
            active=self.db.execute('SELECT value FROM metadata WHERE key=%s FOR SHARE',('review_model_id',)).fetchone()
            if not active:raise ValueError('Export candidates before importing a review')
            active_model=json.loads(active['value'])
            mids=sorted(a.get('mention_id') for a in assignments if isinstance(a.get('mention_id'),str))
            # Source replacement takes these locks before deleting its mentions.
            self.db.execute('SELECT extraction_id FROM sources WHERE extraction_id IN '
                '(SELECT extraction_id FROM mentions WHERE mention_id=ANY(%s)) '
                'ORDER BY extraction_id FOR SHARE',(mids,)).fetchall()
            self.db.execute('SELECT mention_id FROM mentions WHERE mention_id=ANY(%s) '
                            'ORDER BY mention_id FOR UPDATE',(mids,)).fetchall()
            targets=set()
            for assignment in assignments:
                revisions=assignment.get('candidate_revisions',{})
                if not isinstance(revisions,dict):raise ValueError('Invalid candidate revisions')
                targets.update(revisions)
                if assignment.get('decision')=='match':targets.add(assignment.get('entity_id'))
            if any(not isinstance(target,str) for target in targets):raise ValueError('Invalid target entity ID')
            self.db.execute('SELECT entity_id FROM entities WHERE entity_id=ANY(%s) '
                            'ORDER BY entity_id FOR SHARE',(sorted(targets),)).fetchall()
            for eid in declared:
                if self.db.execute('SELECT 1 FROM entities WHERE entity_id=%s',(eid,)).fetchone():
                    raise ValueError(f'Entity ID already exists: {eid}')
            new_entity_names={eid:[] for eid in declared}
            for a in assignments:
                if a.get('model_id')!=active_model:raise ValueError('Stale review model')
                mid=a.get('mention_id');row=self.db.execute('''SELECT m.*,s.source_hash FROM mentions m
                    JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.mention_id=%s''',(mid,)).fetchone()
                if not row or row['decision']!='pending': raise ValueError(f'Unknown or decided mention: {mid}')
                expected=digest([mid,row['source_hash'],row['mention_hash'],a.get('model_id')])
                if a.get('precondition')!=expected: raise ValueError(f'Stale mention/model precondition: {mid}')
                revisions=a.get('candidate_revisions',{})
                if not isinstance(revisions,dict): raise ValueError('Invalid candidate revisions')
                for candidate,rev in revisions.items():
                    target=self.db.execute('SELECT revision FROM entities WHERE entity_id=%s',(candidate,)).fetchone()
                    if not target or target['revision']!=rev: raise ValueError(f'Stale candidate: {candidate}')
                decision=a.get('decision');eid=a.get('entity_id')
                if decision=='pending':
                    if eid is not None: raise ValueError('Pending decision must have null entity_id')
                elif decision=='new_entity':
                    if eid not in declared or declared[eid]['kind']!=row['kind']:
                        raise ValueError('New entity must be declared with matching kind')
                    new_entity_names[eid].append(row['name'])
                elif decision=='match':
                    target=self.db.execute('SELECT kind,revision FROM entities WHERE entity_id=%s',(eid,)).fetchone()
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
                self.db.execute('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (eid,e['kind'],e['name'],e['description'],e.get('name_origin','source_authored'),
                     _json(e.get('aliases',[])),revision))
                self.db.execute('INSERT INTO entity_fts(entity_id,name,description,aliases) VALUES(%s,%s,%s,%s)',
                    (eid,e['name'],e['description'],' '.join(e.get('aliases',[]))))
                self._insert_surfaces(eid,e['kind'],e['name'],e.get('aliases',[]))
            for a in assignments:
                if a['decision']!='pending':
                    self.db.execute('UPDATE mentions SET decision=%s,entity_id=%s WHERE mention_id=%s',
                                    (a['decision'],a['entity_id'],a['mention_id']))
            self.db.execute('INSERT INTO reviews VALUES(%s,%s)',(review_id,review_hash))
        index_status='current'
        if declared and self.model and self._index_model == self.model.model_id:
            for kind,index in list(self._indexes.items()):
                new_ids=[eid for eid in declared if declared[eid]['kind']==kind]
                if new_ids:
                    try:
                        vectors=self._embed([declared[eid]['name']+'\n'+declared[eid]['description'] for eid in new_ids])
                        for eid,vector in zip(new_ids,vectors):index.upsert(eid,vector)
                    except Exception:
                        # PostgreSQL review is authoritative; never retain a partial in-memory index.
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
        entity=self.db.execute('SELECT * FROM entities WHERE entity_id=%s',(entity_id,)).fetchone()
        if not entity:
            area=self.db.execute('SELECT * FROM research_areas WHERE area_id=%s',(entity_id,)).fetchone()
            if not area:return None
            rows=self.db.execute('''SELECT m.mention_id,m.entity_id AS problem_entity_id,m.name,m.description,
                m.role_json,m.version_or_subset,s.paper_id,s.version_id,s.title,s.source_url
                FROM research_area_members a JOIN mentions m ON m.mention_id=a.mention_id
                JOIN sources s ON s.extraction_id=m.extraction_id
                WHERE a.area_id=%s ORDER BY a.mention_id LIMIT %s OFFSET %s''',
                (entity_id,limit+1,offset)).fetchall()
            self.stats['detail_rows']+=1+len(rows)
            members=[{**dict(r),'roles':json.loads(r['role_json'])} for r in rows[:limit]]
            for member in members:del member['role_json']
            return {**dict(area),'entity_id':entity_id,'kind':'ResearchArea','mentions':members,
                    'next_offset':offset+limit if len(rows)>limit else None}
        rows=self.db.execute('''SELECT m.mention_id,m.name,m.description,m.role_json,m.version_or_subset,
            s.paper_id,s.version_id,s.title,s.source_url FROM mentions m JOIN sources s
            ON s.extraction_id=m.extraction_id WHERE m.entity_id=%s ORDER BY m.mention_id LIMIT %s OFFSET %s''',
            (entity_id,limit+1,offset)).fetchall()
        self.stats['detail_rows']+=1+len(rows)
        members=[{**dict(r),'roles':json.loads(r['role_json'])} for r in rows[:limit]]
        for member in members:del member['role_json']
        return {**dict(entity),'aliases':json.loads(entity['aliases_json']),
                'mentions':members,'next_offset':offset+limit if len(rows)>limit else None}

    def get_paper(self,paper_id,limit=20,offset=0):
        self._bound(limit,offset)
        row=self.db.execute('SELECT paper_id,title FROM sources WHERE paper_id=%s LIMIT 1',(paper_id,)).fetchone()
        if not row:return None
        members=self.db.execute('''SELECT m.mention_id,m.kind,m.name,m.entity_id,m.role_json,m.version_or_subset,
            s.version_id,s.source_url FROM sources s JOIN mentions m ON m.extraction_id=s.extraction_id
            WHERE s.paper_id=%s ORDER BY m.mention_id LIMIT %s OFFSET %s''',(paper_id,limit+1,offset)).fetchall()
        contributions=self.db.execute('''SELECT c.contribution_id,c.item_key,c.description,s.version_id,s.source_url
            FROM sources s JOIN contributions c ON c.extraction_id=s.extraction_id
            WHERE s.paper_id=%s ORDER BY c.contribution_id LIMIT %s OFFSET %s''',
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
        if kind and kind not in (*KINDS.values(),'Contribution','ResearchArea'): raise ValueError('Unknown kind')
        if not isinstance(expand_limit,int) or not 0<=expand_limit<=20: raise ValueError('Invalid expand_limit')
        scores={}
        if kind not in ('Contribution','ResearchArea'):
            for entity_kind in ([kind] if kind else list(KINDS.values())):
                ids,ranking=self._rank_entities(query,entity_kind,100)
                for eid in ids:scores[('entity',eid)]=ranking[eid]
        if kind in (None,'Contribution'):
            ids,ranking=self._rank_contributions(query,100)
            for cid in ids:scores[('contribution',cid)]=ranking[cid]
        if kind in (None,'ResearchArea'):
            ids,ranking=self._rank_areas(query,100)
            for area_id in ids:scores[('area',area_id)]=ranking[area_id]
        found=[]
        for (typ,key),score in sorted(scores.items(),key=lambda pair:(-pair[1],pair[0])):
            if typ=='entity':
                row=self.db.execute('SELECT entity_id,kind,name,description,aliases_json FROM entities WHERE entity_id=%s',(key,)).fetchone()
                if not row or (kind and row['kind']!=kind):continue
                found.append(('entity',key,score))
            elif typ=='area':
                if self.db.execute('SELECT 1 FROM research_areas WHERE area_id=%s',(key,)).fetchone():
                    found.append(('area',key,score))
            else:found.append(('contribution',key,score))
            if len(found)>=offset+limit+1:break
        results=[]
        for typ,key,score in found[offset:offset+limit]:
            if typ=='entity':
                row=self.db.execute('SELECT entity_id,kind,name,description FROM entities WHERE entity_id=%s',(key,)).fetchone()
                papers=self.db.execute('''SELECT DISTINCT s.paper_id,s.title,s.version_id,s.source_url FROM mentions m
                    JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.entity_id=%s ORDER BY s.paper_id LIMIT %s''',
                    (key,expand_limit+1)).fetchall()
                results.append({**dict(row),'score':score,'papers':[dict(x) for x in papers[:expand_limit]],
                                'papers_truncated':len(papers)>expand_limit})
            elif typ=='area':
                row=self.db.execute('SELECT area_id,name,description FROM research_areas WHERE area_id=%s',(key,)).fetchone()
                papers=self.db.execute('''SELECT DISTINCT s.paper_id,s.title,s.version_id,s.source_url
                    FROM research_area_members a JOIN mentions m ON m.mention_id=a.mention_id
                    JOIN sources s ON s.extraction_id=m.extraction_id WHERE a.area_id=%s
                    ORDER BY s.paper_id LIMIT %s''',(key,expand_limit+1)).fetchall()
                results.append({**dict(row),'entity_id':key,'kind':'ResearchArea','score':score,
                                'papers':[dict(x) for x in papers[:expand_limit]],
                                'papers_truncated':len(papers)>expand_limit})
            else:
                row=self.db.execute('''SELECT c.contribution_id,c.description,s.paper_id,s.version_id,s.title,s.source_url
                    FROM contributions c JOIN sources s ON s.extraction_id=c.extraction_id WHERE c.contribution_id=%s''',(key,)).fetchone()
                if row:results.append({**dict(row),'kind':'Contribution','score':score})
        return {'results':results,'next_offset':offset+limit if len(found)>offset+limit else None}
