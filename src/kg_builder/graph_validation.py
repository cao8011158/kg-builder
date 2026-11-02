"""Read-only, full structural audit and optional labeled graph acceptance cases."""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

from .semantic_store import KINDS, SemanticStore, digest
from .postgres_graph import SCHEMA_REVISION
from .snapshot_graph import canonical_hash, read_snapshot

FIELDS = {**KINDS, 'contributions': 'Contribution'}
DETAIL_LIMIT = 20


def validate_graph(snapshot_path, job_dir, cases=None, model=None, *, run_id=None, database_url=None):
    """Audit one PostgreSQL REPEATABLE READ, READ ONLY snapshot, including queries.

    The descriptor is read without creating or modifying job artifacts. Query
    embeddings missing from the persisted cache are transient in readonly mode.
    """
    job_dir = Path(job_dir)
    try:
        descriptor = json.loads((job_dir / 'graph-job.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        raise ValueError('A PostgreSQL graph-job.json descriptor is required') from None
    if (not isinstance(descriptor, dict) or descriptor.get('backend') != 'postgresql'
            or descriptor.get('revision') != SCHEMA_REVISION
            or not isinstance(descriptor.get('run_id'), str) or not descriptor['run_id'].strip()):
        raise ValueError('Invalid PostgreSQL graph job descriptor')
    if run_id is not None and run_id != descriptor['run_id']:
        raise ValueError('Graph job run_id cannot be changed')
    search = SemanticStore(descriptor['run_id'], model=model, database_url=database_url,
                           readonly=True, initialize=False)
    try:
        with search.db.transaction():
            search.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            return _validate_graph(snapshot_path, job_dir, cases, model, search)
    finally:
        search.close()


def _validate_graph(snapshot_path, job_dir, cases, model, search):
    snapshot, file_hash = read_snapshot(Path(snapshot_path))
    graph = store = master = search.db
    failures = Counter()
    details = []
    def fail(code, **context):
        failures[code] += 1
        if len(details) < DETAIL_LIMIT:
            details.append({'code': code, **context})
    meta = {row['key']:row['value'] for row in graph.execute('SELECT key,value FROM meta')}
    metric=meta.get('decision_metric','confidence')
    if metric not in ('probability','confidence'):fail('invalid_decision_metric',metric=metric)
    try:
        resolution=float(meta.get('problem_resolution',0.05))
        if not math.isfinite(resolution) or resolution<=0:raise ValueError('invalid resolution')
    except (ValueError,TypeError):
        fail('invalid_problem_resolution');resolution=0.05
    if meta.get('snapshot_hash') != snapshot['snapshot_hash'] or meta.get('file_hash') != file_hash:
        fail('snapshot_binding')
    if meta.get('phase') != 'complete':fail('incomplete_graph_phase',phase=meta.get('phase'))
    if meta.get('index_status') != 'verified':fail('unverified_retrieval_index',status=meta.get('index_status'))
    papers = {p['paper_id']: p for p in snapshot['papers']}
    if len(papers) != len(snapshot['papers']): fail('duplicate_snapshot_paper')
    expected_sources = {}
    expected_extractions = {}
    expected_items = {}
    expected_contributions = {}
    approved = set()
    review_metadata = snapshot.get('review_metadata', {})
    policy = snapshot.get('config', {}).get('review_policy', 'legacy-paper')
    for wrapper in snapshot['extractions']:
        x = wrapper.get('extraction', wrapper.get('payload', wrapper))
        pid = x['paper_id']; vid = x['version_id']
        paper = papers.get(pid)
        if not paper:
            fail('missing_snapshot_paper', paper_id=pid)
            continue
        pin = paper.get('paper_version', paper)
        source_pin = {k:paper.get(k) for k in ('paper_id','version_id','title','text')}
        source_pin.update({k:pin.get(k) for k in ('version_label','source_url')})
        if paper.get('source_hash') != canonical_hash(source_pin) or paper.get('version_id') != vid or x.get('paper_version') != {k:pin.get(k) for k in ('version_label','source_url')}:
            fail('source_pin', paper_id=pid)
        eid = 'extr:' + hashlib.sha256('|'.join((pid, vid, x.get('extractor_type','assistant_manual'), x.get('extractor_version','2026-10-31-v1'))).encode()).hexdigest()[:24]
        if wrapper.get('extraction_id') and wrapper['extraction_id'] != eid: fail('extraction_id', paper_id=pid)
        if eid in expected_sources: fail('duplicate_extraction', extraction_id=eid)
        expected_sources[eid] = (pid,vid,paper['title'],pin.get('source_url'),pin.get('version_label'), digest({'payload':x,'status':x['status'],'source_scope':x['source_scope'],'title':paper['title'],'source_url':pin.get('source_url'),'version_label':pin.get('version_label')}))
        expected_extractions[eid] = x
        if x.get('status') != 'reviewed': fail('unreviewed_extraction', paper_id=pid)
        metadata = review_metadata.get(pid, {})
        review_items=metadata.get('items',[]) if isinstance(metadata,dict) else []
        reviewed = {(r.get('field'),r.get('key')) for r in review_items if r.get('state') == 'approved'}
        if policy == 'item-v1' and (not isinstance(review_items,list) or len({(r.get('field'),r.get('key')) for r in review_items})!=len(review_items)):
            fail('invalid_review_metadata',paper_id=pid)
        approved_hashes={(r.get('field'),r.get('key')):r.get('approved_payload_hash') for r in review_items if r.get('state')=='approved'}
        actual = set()
        for field,kind in FIELDS.items():
            for item in x[field]:
                key=(field,item['key']); actual.add(key)
                if policy == 'item-v1':
                    if key not in reviewed:fail('unapproved_snapshot_item',paper_id=pid,field=field,key=item['key'])
                    elif approved_hashes.get(key)!=canonical_hash(item):fail('approved_payload_hash',paper_id=pid,field=field,key=item['key'])
                identity=(eid,item['key'])
                if identity in expected_items or identity in expected_contributions: fail('duplicate_item_key',paper_id=pid,key=item['key'])
                if field == 'contributions':expected_contributions[identity] = item
                else:expected_items[identity] = (kind,item)
                approved.add((pid,field,item['key']))
        if policy == 'item-v1':
            for field,key in reviewed - actual:fail('approved_item_missing_from_snapshot',paper_id=pid,field=field,key=key)
    # The job's private master must retain the exact source text and approved extraction.
    master_papers={r['paper_id']:r for r in master.execute('SELECT paper_id,title FROM papers')}
    master_versions={r['version_id']:r for r in master.execute('SELECT version_id,paper_id,version_label,source_url,abstract FROM paper_versions')}
    master_extractions={r['extraction_id']:r for r in master.execute('SELECT extraction_id,paper_id,version_id,extractor_type,extractor_version,source_scope,status,payload_json FROM extraction_records')}
    expected_paper_ids={x['paper_id'] for x in expected_extractions.values()}
    for pid in expected_paper_ids:
        paper=papers[pid]
        row=master_papers.get(pid)
        if not row or row['title']!=paper['title']:fail('master_paper',paper_id=pid)
    for pid in master_papers.keys()-expected_paper_ids:fail('extra_master_paper',paper_id=pid)
    for eid,x in expected_extractions.items():
        pid=x['paper_id'];vid=x['version_id'];paper=papers[pid]
        pin=paper.get('paper_version',paper)
        version=master_versions.get(vid)
        if not version or tuple(version[k] for k in ('paper_id','version_label','source_url','abstract'))!=(pid,pin.get('version_label'),pin.get('source_url'),paper['text']):
            fail('master_paper_version',paper_id=pid,version_id=vid)
        extraction=master_extractions.get(eid)
        if not extraction or tuple(extraction[k] for k in ('paper_id','version_id','extractor_type','extractor_version','source_scope','status'))!=(pid,vid,x.get('extractor_type','assistant_manual'),x.get('extractor_version','2026-10-31-v1'),x['source_scope'],x['status']) or json.loads(extraction['payload_json'])!=x:
            fail('master_extraction',extraction_id=eid)
    expected_versions={x['version_id'] for x in expected_extractions.values()}
    for vid in master_versions.keys()-expected_versions:fail('extra_master_version',version_id=vid)
    for eid in master_extractions.keys()-expected_extractions.keys():fail('extra_master_extraction',extraction_id=eid)
    sources = {r['extraction_id']:r for r in store.execute('SELECT * FROM sources')}
    for eid,pin in expected_sources.items():
        row=sources.get(eid)
        if row is None:fail('missing_source',extraction_id=eid)
        elif tuple(row[k] for k in ('paper_id','version_id','title','source_url','version_label','source_hash')) != pin:
            fail('source_metadata',extraction_id=eid)
    for eid in sources.keys() - expected_sources.keys():fail('extra_source',extraction_id=eid)
    mentions = {r['mention_id']:r for r in store.execute('SELECT * FROM mentions')}
    mention_keys = set()
    for mid,row in mentions.items():
        identity=(row['extraction_id'],row['item_key']); mention_keys.add(identity)
        expected=expected_items.get(identity)
        if not expected:
            fail('extra_mention',mention_id=mid)
            continue
        kind,item=expected
        field=next(k for k,v in KINDS.items() if v == kind)
        expected_id='mention:'+digest([row['extraction_id'],field,item['key']])[:32]
        roles=item.get('roles',[item['role']] if 'role' in item else [])
        if (row['kind'] != kind or mid != expected_id or row['name'] != item['name'] or row['description'] != item['description'] or
            json.loads(row['role_json']) != roles or row['version_or_subset'] != item.get('version_or_subset') or
            json.loads(row['source_json']) != item or row['mention_hash'] != digest(item)):
            fail('mention_payload',mention_id=mid)
    for eid,key in expected_items.keys() - mention_keys:fail('missing_mention',extraction_id=eid,key=key)
    contributions = {r['contribution_id']:r for r in store.execute('SELECT * FROM contributions')}
    contribution_keys=set()
    for cid,row in contributions.items():
        identity=(row['extraction_id'],row['item_key']);contribution_keys.add(identity)
        item=expected_contributions.get(identity)
        if not item:fail('extra_contribution',contribution_id=cid);continue
        if cid != 'contribution:'+digest([row['extraction_id'],item['key']])[:32] or row['description'] != item['description'] or json.loads(row['source_json']) != item:
            fail('contribution_payload',contribution_id=cid)
    for eid,key in expected_contributions.keys()-contribution_keys:fail('missing_contribution',extraction_id=eid,key=key)
    entities={r['entity_id']:r for r in store.execute('SELECT * FROM entities')}
    groups={r['group_id']:r for r in graph.execute('SELECT * FROM groups')}
    for table, identity, expression in (
        ('entity_fts','entity_id',"name || ' ' || description || ' ' || aliases"),
        ('contribution_fts','contribution_id','description'),
        ('research_area_fts','area_id',"name || ' ' || description"),
    ):
        for row in store.execute(f"SELECT {identity} FROM {table} WHERE search_vector IS DISTINCT FROM to_tsvector('simple', {expression})"):
            fail('invalid_search_vector', table=table, item_id=row[identity])
    for row in graph.execute('SELECT event_id,payload_json,delivered FROM graph_outbox'):
        try:
            payload = json.loads(row['payload_json'])
            if (not isinstance(payload,dict) or payload.get('event_id') != row['event_id']
                    or not isinstance(payload.get('nodes'),list) or not isinstance(payload.get('edges'),list)
                    or row['delivered'] not in (0,1)):
                raise ValueError('Invalid outbox payload')
        except (ValueError,TypeError):
            fail('invalid_graph_outbox', event_id=row['event_id'])
    if meta.get('graph_policy') == 'domain-v1':
        return _domain_validation(snapshot,job_dir,cases,model,graph,store,meta,mentions,
                                  contributions,entities,groups,approved,sources,fail,failures,details,search)
    accepted_members=defaultdict(set)
    for gid,group in groups.items():
        mids=json.loads(group['members_json'])
        if len(mids)!=len(set(mids)) or not mids or gid != 'group:'+digest(mids)[:32]:fail('invalid_group_members',group_id=gid)
        for mid in mids:
            row=mentions.get(mid)
            if not row:fail('dangling_group_member',group_id=gid,mention_id=mid);continue
            if row['kind'] != group['kind']:fail('cross_kind_group',group_id=gid,mention_id=mid)
            if group['review']=='accepted':accepted_members[mid].add(gid)
            elif row['decision']!='pending' or row['entity_id'] is not None:fail('pending_group_resolved',group_id=gid,mention_id=mid)
        if group['review']=='accepted':
            expected_eid='entity:'+digest([group['kind'],mids])[:32]
            entity=entities.get(expected_eid)
            if not entity or entity['kind']!=group['kind'] or entity['name']!=group['name'] or entity['description']!=group['description']:
                fail('accepted_group_entity',group_id=gid)
            for mid in mids:
                row=mentions.get(mid)
                if row and (row['entity_id']!=expected_eid or row['decision']=='pending'):
                    fail('accepted_group_assignment',group_id=gid,mention_id=mid)
    group_member_counts=Counter(mid for group in groups.values() for mid in json.loads(group['members_json']))
    if meta.get('phase') in ('grouped','complete'):
        for mid in mentions:
            if group_member_counts[mid]!=1:fail('group_membership_count',mention_id=mid,count=group_member_counts[mid])
    for mid,row in mentions.items():
        eid=row['entity_id'];entity=entities.get(eid) if eid else None
        if eid and (not entity or entity['kind']!=row['kind']):fail('dangling_or_cross_kind_entity',mention_id=mid)
        if (row['decision']=='pending') != (eid is None):fail('pending_assignment',mention_id=mid)
        if eid and not accepted_members[mid]:fail('assignment_without_accepted_group',mention_id=mid)
        if not eid and accepted_members[mid]:fail('accepted_member_pending',mention_id=mid)
        if len(accepted_members[mid])>1:fail('multiple_accepted_groups',mention_id=mid)
    assigned_entities={r['entity_id'] for r in mentions.values() if r['entity_id']}
    for eid in entities.keys()-assigned_entities:fail('orphan_entity',entity_id=eid)
    pending_pairs=0
    for row in graph.execute('SELECT pair_id,kind,left_id,right_id,decision FROM candidates'):
        left=mentions.get(row['left_id']);right=mentions.get(row['right_id'])
        if not left or not right:fail('dangling_pair',pair_id=row['pair_id']);continue
        if left['kind']!=row['kind'] or right['kind']!=row['kind']:fail('cross_kind_pair',pair_id=row['pair_id'])
        le=left['entity_id'];re=right['entity_id']
        if row['decision']=='different' and le and le==re:fail('different_pair_merged',pair_id=row['pair_id'])
        if row['decision']=='same' and (not le or le!=re) and not (left['decision']=='pending' or right['decision']=='pending'):
            fail('same_pair_split',pair_id=row['pair_id'])
        if row['decision'] in ('pending','unjudged'):
            pending_pairs+=1
            if left['decision']!='pending' or right['decision']!='pending':fail('pending_pair_resolved',pair_id=row['pair_id'])
    # FTS and exact alias directory are audited as stored, without issuing matching writes.
    entity_fts={r['entity_id']:r for r in store.execute('SELECT entity_id,name,description,aliases FROM entity_fts')}
    surfaces=defaultdict(set)
    for r in store.execute('SELECT kind,normalized_name,entity_id FROM entity_surfaces'):
        surfaces[r['entity_id']].add((r['kind'],r['normalized_name']))
    names_by_entity=defaultdict(set)
    for mention in mentions.values():
        if mention['entity_id']:names_by_entity[mention['entity_id']].add(mention['name'])
    for eid,row in entities.items():
        aliases=json.loads(row['aliases_json']);fts=entity_fts.get(eid)
        if not fts or (fts['name'],fts['description'],fts['aliases'])!=(row['name'],row['description'],' '.join(aliases)):
            fail('entity_fts',entity_id=eid)
        required={(row['kind'],' '.join(name.split()).casefold()) for name in [row['name'],*aliases]}
        if not required.issubset(surfaces[eid]):fail('entity_surfaces',entity_id=eid)
        if row['kind'] in ('Method','Benchmark'):
            names=names_by_entity[eid]
            if row['name_origin']!='source_authored' or row['name'] not in names or not names.issubset(set(aliases)|{row['name']}):
                fail('source_authored_name',entity_id=eid)
    for r in store.execute('SELECT entity_id,count(*) n FROM entity_fts GROUP BY entity_id HAVING count(*)>1'):
        fail('duplicate_entity_fts',entity_id=r['entity_id'],count=r['n'])
    for eid in entity_fts.keys()-entities.keys():fail('extra_entity_fts',entity_id=eid)
    for eid in surfaces.keys()-entities.keys():fail('extra_entity_surfaces',entity_id=eid)
    cfts={r['contribution_id']:r['description'] for r in store.execute('SELECT contribution_id,description FROM contribution_fts')}
    for cid,row in contributions.items():
        if cfts.get(cid)!=row['description']:fail('contribution_fts',contribution_id=cid)
    for r in store.execute('SELECT contribution_id,count(*) n FROM contribution_fts GROUP BY contribution_id HAVING count(*)>1'):
        fail('duplicate_contribution_fts',contribution_id=r['contribution_id'],count=r['n'])
    for cid in cfts.keys()-contributions.keys():fail('extra_contribution_fts',contribution_id=cid)
    quality=_quality(cases,store,job_dir,model,mentions,contributions,fail,search=search) if cases is not None else {'status':'untested','reason':'no labeled cases'}
    counts={'approved_items':len(approved),'sources':len(sources),'mentions':len(mentions),'contributions':len(contributions),
            'entities':len(entities),'accepted_groups':sum(g['review']=='accepted' for g in groups.values()),
            'pending_groups':sum(g['review']!='accepted' for g in groups.values()),'pending_pairs':pending_pairs,
            'pending_mentions':sum(r['decision']=='pending' for r in mentions.values())}
    return {'schema_version':'research-kg-graph-acceptance/1','snapshot_hash':snapshot['snapshot_hash'],
            'structural':{'passed':not failures,'failure_counts':dict(sorted(failures.items())),'failure_total':sum(failures.values()),
                          'details':details,'details_truncated':sum(failures.values())>len(details),'coverage':counts,
                          'complete_resolution':counts['pending_pairs']==counts['pending_groups']==counts['pending_mentions']==0},
            'quality':quality}

def _domain_validation(snapshot, job_dir, cases, model, graph, store, meta, mentions,
                       contributions, entities, groups, approved, sources, fail, failures, details, search):
    """Audit the domain policy's immutable proposals and final retained relationships."""
    metric=meta.get('decision_metric','confidence')
    try:resolution=float(meta.get('problem_resolution',0.05))
    except (ValueError,TypeError):resolution=0.05
    group_members = Counter()
    expected_entities = {}
    expected_assignment = {}
    expected_areas = {}
    expected_memberships = set()
    excluded_count = 0
    reviews = {}
    parsed_groups = {}
    for row in graph.execute('SELECT * FROM member_reviews'):
        key = row['group_id'], row['mention_id']
        if key in reviews: fail('duplicate_member_review', group_id=key[0], mention_id=key[1])
        reviews[key] = row
    for gid, group in groups.items():
        try: mids = json.loads(group['members_json'])
        except (ValueError, TypeError):
            fail('invalid_group_members',group_id=gid);continue
        if not isinstance(mids,list) or not mids or not all(isinstance(mid,str) for mid in mids):
            fail('invalid_group_members',group_id=gid);continue
        parsed_groups[gid]=mids
        if mids!=sorted(set(mids)) or gid!='group:'+digest(mids)[:32]:
            fail('invalid_group_members',group_id=gid)
        if group['kind'] not in KINDS.values() or group['review']!='accepted':
            fail('invalid_group_status',group_id=gid,review=group['review'])
        kept=[]
        for mid in mids:
            group_members[mid]+=1
            mention=mentions.get(mid)
            if mention is None or mention['kind']!=group['kind']:
                fail('dangling_or_cross_kind_group_member',group_id=gid,mention_id=mid);continue
            if group['kind']=='ResearchProblem' and len(mids)==1:
                if (gid,mid) in reviews:fail('unexpected_singleton_area_review',group_id=gid,mention_id=mid)
                kept.append(mid)
                continue
            row=reviews.get((gid,mid))
            if row is None:
                fail('missing_member_review',group_id=gid,mention_id=mid);continue
            try: audit=json.loads(row['audit'])
            except (ValueError,TypeError):
                fail('invalid_member_audit',group_id=gid,mention_id=mid);continue
            if not isinstance(audit,dict):
                fail('invalid_member_audit',group_id=gid,mention_id=mid);continue
            confidence=audit.get('confidence')
            score=audit.get(metric)
            choice=audit.get('choice')
            valid_score=(isinstance(score,(int,float)) and not isinstance(score,bool)
                         and math.isfinite(score) and 0<=score<=1)
            expected_excluded=choice=='not_belongs' and valid_score and score>0.7
            raw=audit.get('raw')
            raw_answer=raw.get('answers',{}).get('membership') if isinstance(raw,dict) and isinstance(raw.get('answers'),dict) else None
            probabilities=raw_answer.get('probabilities') if isinstance(raw_answer,dict) else None
            raw_probability_valid=(metric!='probability' or
                (isinstance(probabilities,dict) and set(probabilities)=={'belongs','not_belongs','insufficient'}
                 and all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and 0<=v<=1
                         for v in probabilities.values()) and abs(sum(probabilities.values())-1)<=0.02
                 and score==probabilities['not_belongs'] and raw_answer.get('confidence')==confidence
                 and raw_answer.get('choice')==choice))
            if (audit.get('membership_id')!='membership:'+digest([gid,mid])[:32]
                or audit.get('excluded') is not expected_excluded or row['excluded'] not in (0,1)
                or bool(row['excluded'])!=expected_excluded or choice not in ('belongs','not_belongs','insufficient')
                or not valid_score or audit.get('decision_metric','confidence')!=metric or not raw_probability_valid
                or not isinstance(confidence,(int,float)) or isinstance(confidence,bool) or not math.isfinite(confidence) or not 0<=confidence<=1
                or (group['kind']=='ResearchProblem' and audit.get('area_revision')!=digest([group['name'],group['description']]))
                or not isinstance(audit.get('raw'),dict)):
                fail('member_exclusion_evidence',group_id=gid,mention_id=mid)
            if row['excluded']:excluded_count+=1
            else:kept.append(mid)
        if group['kind']=='ResearchProblem':
            if len(mids)>1:
                task=graph.execute('SELECT status,result_json FROM tasks WHERE group_id=%s',(gid,)).fetchone()
                try:result=json.loads(task['result_json']) if task and task['status']=='done' else None
                except (ValueError,TypeError):result=None
                if not isinstance(result,dict) or result.get('name')!=group['name'] or result.get('description')!=group['description'] or result.get('group_id',gid)!=gid:
                    fail('area_name_task',group_id=gid)
            if len(mids)>1 and kept:
                area_id='area:'+digest([gid])[:32]
                expected_areas[area_id]=group
                expected_memberships.update((area_id,mid) for mid in kept)
            continue
        if kept:
            eid='entity:'+digest([group['kind'],sorted(kept)])[:32]
            if eid in expected_entities:fail('duplicate_expected_entity',entity_id=eid)
            expected_entities[eid]=(group['kind'],sorted(kept))
            for mid in kept:
                if mid in expected_assignment:fail('duplicate_expected_assignment',mention_id=mid)
                expected_assignment[mid]=eid
        for mid in set(mids)-set(kept):
            eid='entity:'+digest([group['kind'],[mid]])[:32]
            if eid in expected_entities:fail('duplicate_expected_entity',entity_id=eid)
            expected_entities[eid]=(group['kind'],[mid])
            if mid in expected_assignment:fail('duplicate_expected_assignment',mention_id=mid)
            expected_assignment[mid]=eid
    for key in reviews.keys()-{(gid,mid) for gid,mids in parsed_groups.items() for mid in mids}:
        fail('extra_member_review',group_id=key[0],mention_id=key[1])
    for mid,row in mentions.items():
        if group_members[mid]!=1:fail('group_membership_count',mention_id=mid,count=group_members[mid])
        if row['kind']=='ResearchProblem':
            eid='entity:'+digest(['ResearchProblem',[mid]])[:32]
            expected_entities[eid]=('ResearchProblem',[mid])
            expected_assignment[mid]=eid
        else:
            eid=expected_assignment.get(mid)
        if row['entity_id']!=eid or row['decision']!='new_entity':
            fail('mention_entity_assignment',mention_id=mid,entity_id=row['entity_id'],expected=eid)
        if row['extraction_id'] not in sources:
            fail('dangling_mention_source',mention_id=mid)
    if entities.keys()!=expected_entities.keys():
        for eid in expected_entities.keys()-entities.keys():fail('missing_entity',entity_id=eid)
        for eid in entities.keys()-expected_entities.keys():fail('extra_entity',entity_id=eid)
    fts=defaultdict(list)
    for row in store.execute('SELECT entity_id,name,description,aliases FROM entity_fts'):
        fts[row['entity_id']].append(row)
    surfaces=defaultdict(set)
    for row in store.execute('SELECT kind,normalized_name,entity_id FROM entity_surfaces'):
        surfaces[row['entity_id']].add((row['kind'],row['normalized_name']))
    for eid,row in entities.items():
        kind,mids=expected_entities.get(eid,(None,[]))
        names={mentions[mid]['name'] for mid in mids if mid in mentions}
        aliases=json.loads(row['aliases_json'])
        if (row['kind']!=kind or row['name_origin']!='source_authored' or row['name'] not in names
            or not names.issubset(set(aliases)|{row['name']}) or row['revision']!=digest([row['kind'],row['name'],row['description'],aliases,row['name_origin']])):
            fail('entity_source_identity',entity_id=eid)
        if mids and (row['name']!=mentions[mids[0]]['name'] or row['description']!=mentions[mids[0]]['description']):
            fail('entity_source_payload',entity_id=eid)
        if len(fts[eid])!=1 or (fts[eid][0]['name'],fts[eid][0]['description'],fts[eid][0]['aliases'])!=(row['name'],row['description'],' '.join(aliases)):
            fail('entity_fts',entity_id=eid)
        required={(row['kind'],' '.join(name.split()).casefold()) for name in (row['name'],*aliases)}
        if not required.issubset(surfaces[eid]):fail('entity_surfaces',entity_id=eid)
    for eid in (fts.keys()|surfaces.keys())-entities.keys():fail('extra_entity_index',entity_id=eid)
    cfts=defaultdict(list)
    for row in store.execute('SELECT contribution_id,description FROM contribution_fts'):
        cfts[row['contribution_id']].append(row['description'])
    for cid,row in contributions.items():
        if cfts[cid]!=[row['description']]:fail('contribution_fts',contribution_id=cid)
        if row['extraction_id'] not in sources:fail('dangling_contribution_source',contribution_id=cid)
    for cid in cfts.keys()-contributions.keys():fail('extra_contribution_fts',contribution_id=cid)
    areas={r['area_id']:r for r in store.execute('SELECT * FROM research_areas')}
    memberships=Counter((r['area_id'],r['mention_id']) for r in store.execute('SELECT area_id,mention_id FROM research_area_members'))
    area_fts=defaultdict(list)
    for row in store.execute('SELECT area_id,name,description FROM research_area_fts'):
        area_fts[row['area_id']].append(row)
    for aid,group in expected_areas.items():
        area=areas.get(aid)
        if not area:fail('missing_research_area',area_id=aid);continue
        if (area['name'],area['description'],area['revision'])!=(group['name'],group['description'],digest([group['name'],group['description']])):
            fail('research_area_payload',area_id=aid)
    for aid in areas.keys()-expected_areas.keys():fail('extra_research_area',area_id=aid)
    for key in expected_memberships:
        if memberships[key]!=1:fail('research_area_membership',area_id=key[0],mention_id=key[1],count=memberships[key])
    for aid,mid in memberships.keys()-expected_memberships:
        fail('extra_or_dangling_area_member',area_id=aid,mention_id=mid)
    for aid,area in areas.items():
        if len(area_fts[aid])!=1 or (area_fts[aid][0]['name'],area_fts[aid][0]['description'])!=(area['name'],area['description']):
            fail('research_area_fts',area_id=aid)
    for aid in area_fts.keys()-areas.keys():fail('extra_research_area_fts',area_id=aid)
    # The proposal is an artifact of the actual clustering run; verify it also
    # matches the frozen vectors, so replacing the artifact cannot mask a wrong partition.
    try:
        proposal=json.loads(meta.get('problem_proposal','null'))
        if not isinstance(proposal,dict):raise ValueError('Missing PostgreSQL problem proposal')
        vectors={r['item_key']:r['vector'] for r in snapshot['vectors'] if r['kind']=='ResearchProblem'}
        by_paper={(r['paper_id'],r['item_key']):r['mention_id'] for r in store.execute('SELECT m.mention_id,m.item_key,s.paper_id FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.kind=%s',('ResearchProblem',))}
        rp_vectors={mid:vectors[pid+'::'+key] for (pid,key),mid in by_paper.items()}
        from .problem_clustering import cluster_problem_vectors
        recalculated=cluster_problem_vectors(rp_vectors,k=int(meta['k']),resolution=resolution)
        actual=sorted(tuple(parsed_groups[gid]) for gid,g in groups.items() if g['kind']=='ResearchProblem' and gid in parsed_groups)
        if proposal['clusters']!=recalculated['clusters'] or actual!=sorted(tuple(c) for c in recalculated['clusters']):
            fail('leiden_partition_mismatch')
    except (OSError,ValueError,KeyError,TypeError) as exc:
        fail('missing_or_invalid_leiden_proposal',error=str(exc)[:200])
    pending_pairs=0
    parent={mid:mid for mid,r in mentions.items() if r['kind'] in ('Method','Benchmark')}
    def root(mid):
        while parent[mid]!=mid:
            parent[mid]=parent[parent[mid]];mid=parent[mid]
        return mid
    for pair in graph.execute('SELECT * FROM candidates'):
        left=mentions.get(pair['left_id']);right=mentions.get(pair['right_id'])
        if not left or not right or pair['kind'] not in ('Method','Benchmark') or left['kind']!=pair['kind'] or right['kind']!=pair['kind']:
            fail('invalid_identity_candidate',pair_id=pair['pair_id']);continue
        if pair['decision']=='different' and left['entity_id']==right['entity_id']:
            fail('different_pair_merged',pair_id=pair['pair_id'])
        if pair['decision']=='same':parent[root(pair['right_id'])]=root(pair['left_id'])
        if pair['decision'] in ('pending','unjudged'):pending_pairs+=1
    components=defaultdict(list)
    for mid in sorted(parent):components[root(mid)].append(mid)
    actual_mb={tuple(parsed_groups[gid]) for gid,g in groups.items() if g['kind'] in ('Method','Benchmark') and gid in parsed_groups}
    if actual_mb!={tuple(mids) for mids in components.values()}:fail('identity_components_mismatch')
    quality=_quality(cases,store,job_dir,model,mentions,contributions,fail,domain=True,search=search) if cases is not None else {'status':'untested','reason':'no labeled cases'}
    counts={'approved_items':len(approved),'sources':len(sources),'mentions':len(mentions),'contributions':len(contributions),
            'entities':len(entities),'research_areas':len(areas),'area_memberships':sum(memberships.values()),
            'excluded_memberships':excluded_count,'member_reviews':len(reviews),'accepted_groups':sum(g['review']=='accepted' for g in groups.values()),
            'pending_groups':sum(g['review']!='accepted' for g in groups.values()),'pending_pairs':pending_pairs,
            'pending_mentions':sum(r['decision']=='pending' for r in mentions.values()),
            'all_source_nodes_available':all(r['entity_id'] in entities and r['decision']=='new_entity' for r in mentions.values())}
    return {'schema_version':'research-kg-graph-acceptance/1','graph_policy':'domain-v1','snapshot_hash':snapshot['snapshot_hash'],
            'structural':{'passed':not failures,'failure_counts':dict(sorted(failures.items())),'failure_total':sum(failures.values()),
                          'details':details,'details_truncated':sum(failures.values())>len(details),'coverage':counts,
                          'complete_resolution':counts['pending_pairs']==counts['pending_groups']==counts['pending_mentions']==0},'quality':quality}


def _quality(cases, source_db, job_dir, model, mentions, contributions, fail, *, domain=False, search):
    if not isinstance(cases,dict):raise ValueError('Cases must be a JSON object')
    queries=cases.get('queries',[]);pairs=cases.get('identity_pairs',[])
    if not isinstance(queries,list) or not isinstance(pairs,list):raise ValueError('Invalid case arrays')
    by_paper=defaultdict(list)
    for r in source_db.execute('SELECT m.mention_id,m.item_key,s.paper_id FROM mentions m JOIN sources s ON s.extraction_id=m.extraction_id'):
        by_paper[(r['paper_id'],r['item_key'])].append(r['mention_id'])
    seen=set();resolved=wrong=pending=unknown=not_applicable=0
    by_kind=defaultdict(Counter)
    for pair in pairs:
        cid=pair['id']
        if cid in seen:raise ValueError('Duplicate case ID')
        seen.add(cid)
        expected=pair['expected']
        if expected not in ('same','different','unknown'):raise ValueError('Invalid identity label')
        left=by_paper.get((pair['left']['paper_id'],pair['left']['key']),[])
        right=by_paper.get((pair['right']['paper_id'],pair['right']['key']),[])
        if len(left)!=1 or len(right)!=1:fail('case_identity_lookup',case_id=cid);continue
        a=mentions[left[0]];b=mentions[right[0]]
        kind=a['kind'] if a['kind']==b['kind'] else 'cross_kind'
        if expected=='unknown':
            unknown+=1
            if domain:by_kind[kind]['unknown_unscored']+=1
            continue
        if domain and a['kind']=='ResearchProblem' and b['kind']=='ResearchProblem':
            not_applicable+=1;continue
        if a['decision']=='pending' or b['decision']=='pending':
            pending+=1
            if domain:by_kind[kind]['pending']+=1
        elif (a['entity_id']==b['entity_id'])==(expected=='same'):
            resolved+=1
            if domain:by_kind[kind]['resolved']+=1
        else:
            wrong+=1
            if domain:by_kind[kind]['wrong']+=1
    labeled=[q for q in queries if q.get('expected_paper_ids')]
    if any(q.get('id') in seen for q in queries) or len({q.get('id') for q in queries})!=len(queries):raise ValueError('Duplicate case ID')
    hits=[];reciprocals=[];area_hits=[];area_reciprocals=[]
    if labeled:
        for case in labeled:
            expected=set(case['expected_paper_ids'])
            if case['kind'] not in (*KINDS.values(),'Contribution',*(['ResearchArea'] if domain else [])):raise ValueError('Invalid query kind')
            result=search.search(case['query'],kind=case['kind'],limit=5,expand_limit=20)['results']
            paper_sets=[{p['paper_id'] for p in search.db.execute('SELECT DISTINCT s.paper_id FROM research_area_members am JOIN mentions m ON am.mention_id=m.mention_id JOIN sources s ON m.extraction_id=s.extraction_id WHERE am.area_id=%s',(r['entity_id'],))} if domain and r.get('kind')=='ResearchArea' else {p['paper_id'] for p in search.db.execute('SELECT DISTINCT s.paper_id FROM mentions m JOIN sources s ON m.extraction_id=s.extraction_id WHERE m.entity_id=%s',(r['entity_id'],))} if 'entity_id' in r else {r['paper_id']} for r in result]
            ranks=[i for i,pids in enumerate(paper_sets,1) if expected & pids]
            rank=min(ranks) if ranks else None
            target_hits,target_reciprocals=(area_hits,area_reciprocals) if domain and case['kind']=='ResearchArea' else (hits,reciprocals)
            target_hits.append(int(rank is not None));target_reciprocals.append(1/rank if rank else 0)
    return {'status':'evaluated' if labeled or resolved or wrong or pending else 'untested','query_cases':len(queries) if not domain else len(queries)-sum(q.get('kind')=='ResearchArea' for q in queries),
            'labeled_positive_queries':len(hits) if domain else len(labeled),'hit_at_5':sum(hits)/len(hits) if hits else None,
            'mrr':sum(reciprocals)/len(reciprocals) if reciprocals else None,
            'identity':{'resolved':resolved,'wrong':wrong,'pending':pending,'unknown_unscored':unknown,
                        **({'not_applicable_new_semantics':not_applicable,
                            'by_kind':{kind:{key:by_kind[kind][key] for key in ('resolved','wrong','pending','unknown_unscored')}
                                       for kind in ('Method','Benchmark')}} if domain else {})},
            **({'research_area_queries':{'query_cases':sum(q.get('kind')=='ResearchArea' for q in queries),
                                         'labeled_positive_queries':len(area_hits),
                                         'hit_at_5':sum(area_hits)/len(area_hits) if area_hits else None,
                                         'mrr':sum(area_reciprocals)/len(area_reciprocals) if area_reciprocals else None}}
               if domain else {})}
