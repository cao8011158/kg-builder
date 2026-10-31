"""Explicit manual identity decisions over immutable extraction snapshots."""
from __future__ import annotations
import hashlib
from importlib.resources import files
import json
import re
import sqlite3

from jsonschema import Draft202012Validator

SCHEMA_VERSION='research-kg-entity-resolution/0.1'
SCHEMA_VERSION_V2='research-kg-entity-resolution/0.2'
VALIDATORS={version:Draft202012Validator(json.loads(files('kg_builder.schemas').joinpath(name).read_text(encoding='utf-8')))
            for version,name in ((SCHEMA_VERSION,'entity_resolution_v0_1.json'),
                                 (SCHEMA_VERSION_V2,'entity_resolution_v0_2.json'))}
FIELD_KIND={'methods':'Method','research_problems':'ResearchProblem','artifacts':'Artifact'}


def _load_json(raw):
    return json.loads(raw,parse_constant=lambda value: (_ for _ in ()).throw(
        ValueError(f'Nonfinite JSON: {value}')))


def validate_resolution_manifest(manifest):
    """Validate the complete declared profile, without reading or writing SQLite."""
    if (not isinstance(manifest,dict) or
        not isinstance(manifest.get('schema_version'),str) or
        manifest['schema_version'] not in VALIDATORS):
        raise ValueError('Unknown entity resolution schema_version')
    errors=sorted(VALIDATORS[manifest['schema_version']].iter_errors(manifest),key=lambda e:list(map(str,e.path)))
    if errors:
        err=errors[0]
        raise ValueError(f'Entity resolution schema at {list(err.path)}: {err.message}')
    # JSON Schema treats floats as numbers; strict JSON forbids NaN/Infinity everywhere.
    json.dumps(manifest,allow_nan=False)
    return manifest


def _source(con, extraction_id, cache):
    if extraction_id not in cache:
        row=con.execute('''SELECT version_id,payload_json FROM extraction_records
                           WHERE extraction_id=?''',(extraction_id,)).fetchone()
        if not row:
            raise ValueError(f'Unknown extraction_id: {extraction_id}')
        payload=_load_json(row[1])
        if not isinstance(payload,dict):
            raise ValueError(f'Extraction payload must be an object: {extraction_id}')
        digest=hashlib.sha256(row[1].encode('utf-8')).hexdigest()
        cache[extraction_id]=(row[0],payload,digest)
    return cache[extraction_id]


def _check_evidence(source, refs, paths):
    locators=source.get('evidence_locators') or []
    known={loc.get('evidence_id') for loc in locators if isinstance(loc,dict)}
    for ref in refs:
        if ref not in known:
            raise ValueError(f'Unknown evidence_ref: {ref}')
    for path in paths:
        index=int(path.rsplit('/',1)[1])
        if index >= len(locators) or not isinstance(locators[index],dict):
            raise ValueError(f'Invalid evidence_locator_path: {path}')


def _validate_problem_hierarchy(entities):
    """Full manifest admission: validate a reviewer taxonomy in O(nodes + edges)."""
    parents={}
    for eid,entity in entities.items():
        links=entity.get('parent_problem_ids',[])
        if links and (entity['kind']!='ResearchProblem' or
                      entity.get('relation_basis')!='reviewer_taxonomy'):
            raise ValueError(f'Parent links require ResearchProblem reviewer_taxonomy: {eid}')
        if entity['kind']!='ResearchProblem' and (
            'parent_problem_ids' in entity or 'relation_basis' in entity):
            raise ValueError(f'Parent links only belong to ResearchProblem: {eid}')
        for parent in links:
            if parent==eid or parent not in entities or entities[parent]['kind']!='ResearchProblem':
                raise ValueError(f'Invalid parent ResearchProblem: {eid} -> {parent}')
        parents[eid]=links
    state={}
    for root in parents:
        if state.get(root)==2:continue
        state[root]=1
        stack=[(root,iter(parents[root]))]
        while stack:
            current,edges=stack[-1]
            parent=next(edges,None)
            if parent is None:
                state[current]=2
                stack.pop()
            elif state.get(parent)==1:
                raise ValueError(f'Cycle in ResearchProblem taxonomy: {parent}')
            elif state.get(parent)!=2:
                state[parent]=1
                stack.append((parent,iter(parents[parent])))


def _validate_sources(con,manifest):
    entities={}
    cache={}
    for entity in manifest['entities']:
        eid=entity['entity_id']
        if eid in entities:
            raise ValueError(f'Duplicate entity_id: {eid}')
        entities[eid]=entity
        local=entity['scope']=='paper_version_local'
        version=entity['scope_version_id']
        if local != (version is not None):
            raise ValueError(f'Entity scope_version_id mismatch: {eid}')
        if entity['kind']=='Artifact' and 'artifact_kind' not in entity:
            raise ValueError(f'Artifact requires artifact_kind: {eid}')
        if local and not con.execute('SELECT 1 FROM paper_versions WHERE version_id=?',(version,)).fetchone():
            raise ValueError(f'Unknown scope_version_id: {version}')
        for support in entity['support']:
            source_version,source,digest=_source(con,support['extraction_id'],cache)
            if digest != support['source_payload_sha256'].lower():
                raise ValueError(f'Support payload hash mismatch: {eid}')
            if local and source_version != version:
                raise ValueError(f'Local entity support belongs to another version: {eid}')
            _check_evidence(source,[support['evidence_ref']] if 'evidence_ref' in support else [],
                            [support['evidence_locator_path']] if 'evidence_locator_path' in support else [])
    if manifest['schema_version']==SCHEMA_VERSION_V2:
        _validate_problem_hierarchy(entities)
    seen=set()
    for mention in manifest['mentions']:
        key=(mention['extraction_id'],mention['mention_path'])
        if key in seen:
            raise ValueError(f'Duplicate mention: {key}')
        seen.add(key)
        version,source,digest=_source(con,mention['extraction_id'],cache)
        if digest != mention['source_payload_sha256'].lower():
            raise ValueError(f'Mention payload hash mismatch: {key}')
        path=re.fullmatch(r'/(methods|research_problems|artifacts)/(0|[1-9]\d*)',mention['mention_path'])
        field,index=path.group(1),int(path.group(2))
        if mention['kind'] != FIELD_KIND[field]:
            raise ValueError(f'Mention kind and path mismatch: {key}')
        sources=source.get(field) or []
        if index >= len(sources) or not isinstance(sources[index],dict):
            raise ValueError(f'Mention path outside extraction: {key}')
        observed=sources[index]
        if observed.get('name') != mention['surface_name'] or observed.get('key') != mention['source_key']:
            raise ValueError(f'Mention surface/key differs from extraction: {key}')
        _check_evidence(source,mention['evidence_refs'],mention['evidence_locator_paths'])
        target=mention['entity_id']
        if manifest['schema_version']==SCHEMA_VERSION_V2:
            outcome=mention['review_outcome']
            components=mention['component_entity_ids']
            if outcome=='mapped':
                if mention['decision']!='resolved' or target is None or components:
                    raise ValueError(f'Inconsistent mapped outcome: {key}')
            elif outcome=='composite_label':
                if (mention['decision']!='pending' or target is not None or len(components)<2 or
                    mention['kind']!='Method'):
                    raise ValueError(f'Inconsistent composite_label outcome: {key}')
                for component_id in components:
                    component=entities.get(component_id)
                    if not component or component['kind'] not in ('Method','Artifact'):
                        raise ValueError(f'Invalid composite component: {component_id}')
                    if (component['scope']=='paper_version_local' and
                        component['scope_version_id']!=version):
                        raise ValueError(f'Cross-version local component: {component_id}')
            else:
                if mention['decision']!='pending' or target is not None or components:
                    raise ValueError(f'Inconsistent {outcome} outcome: {key}')
                if outcome=='not_a_method' and mention['kind']!='Method':
                    raise ValueError(f'not_a_method requires a Method source: {key}')
            if outcome in ('composite_label','not_a_method') and not (
                mention['evidence_refs'] or mention['evidence_locator_paths']):
                raise ValueError(f'{outcome} needs source evidence: {key}')
        if mention['decision']=='pending':
            if target is not None: raise ValueError(f'Pending mention has entity_id: {key}')
            continue
        if target is None or target not in entities:
            raise ValueError(f'Resolved mention has unknown entity_id: {key}')
        if not mention['evidence_refs'] and not mention['evidence_locator_paths']:
            raise ValueError(f'Resolved mention needs source evidence: {key}')
        entity=entities[target]
        if entity['kind'] != mention['kind']:
            raise ValueError(f'Cross-kind resolution: {key}')
        if mention['kind']=='Artifact' and observed.get('artifact_kind') != entity['artifact_kind']:
            raise ValueError(f'Artifact subtype mismatch: {key}')
        if entity['scope']=='paper_version_local' and entity['scope_version_id'] != version:
            raise ValueError(f'Cross-version local resolution: {key}')
        if mention['kind']=='Artifact' and entity['scope']=='named_concept' and (
            observed.get('identity_scope')=='paper_version_local_configuration' or
            observed.get('name_origin')=='annotator_descriptive'):
            raise ValueError(f'Local/descriptive Artifact cannot resolve globally: {key}')


def import_resolution_manifest(con,manifest,raw_json=None):
    """All-or-nothing immutable batch import; a caller transaction remains caller-owned."""
    validate_resolution_manifest(manifest)
    if raw_json is None:
        raw_json=json.dumps(manifest,ensure_ascii=False,sort_keys=True,allow_nan=False)
    elif _load_json(raw_json) != manifest:
        raise ValueError('Raw manifest bytes do not match supplied document')
    canonical=json.dumps(manifest,ensure_ascii=False,sort_keys=True,allow_nan=False)
    digest=hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    con.execute('SAVEPOINT entity_resolution_import')
    try:
        existing=con.execute('SELECT manifest_sha256 FROM entity_resolution_batches WHERE batch_id=?',
                             (manifest['batch_id'],)).fetchone()
        if existing:
            if existing[0] != digest:
                raise ValueError('Immutable batch_id already has a different manifest')
        else:
            _validate_sources(con,manifest)
            con.execute('INSERT INTO entity_resolution_batches VALUES(?,?,?,?,?,?)',
                (manifest['batch_id'],manifest['schema_version'],manifest['reviewer'],manifest['reviewed_at'],digest,raw_json))
            for entity in manifest['entities']:
                con.execute('INSERT INTO entity_resolution_entities VALUES(?,?,?,?,?,?)',
                    (manifest['batch_id'],entity['entity_id'],entity['kind'],entity['scope'],
                     entity['scope_version_id'],json.dumps(entity,ensure_ascii=False,allow_nan=False)))
            for mention in manifest['mentions']:
                con.execute('INSERT INTO entity_resolution_mentions VALUES(?,?,?,?,?,?,?,?)',
                    (manifest['batch_id'],mention['extraction_id'],mention['kind'],mention['mention_path'],
                     mention['source_payload_sha256'],mention['decision'],mention['entity_id'],
                     json.dumps(mention,ensure_ascii=False,allow_nan=False)))
        con.execute('RELEASE SAVEPOINT entity_resolution_import')
    except BaseException:
        con.execute('ROLLBACK TO SAVEPOINT entity_resolution_import')
        con.execute('RELEASE SAVEPOINT entity_resolution_import')
        raise
    return manifest['batch_id']


def get_resolution_mentions(con,batch_id,extraction_id):
    """Indexed, explicit batch/extraction lookup; stale support hides active resolution."""
    con.execute('SAVEPOINT entity_resolution_read')
    try:
        result=_get_resolution_mentions_snapshot(con,batch_id,extraction_id)
        con.execute('RELEASE SAVEPOINT entity_resolution_read')
        return result
    except BaseException:
        con.execute('ROLLBACK TO SAVEPOINT entity_resolution_read')
        con.execute('RELEASE SAVEPOINT entity_resolution_read')
        raise


def _entity_payload(con,batch_id,entity_id,cache):
    if entity_id not in cache:
        row=con.execute('''SELECT payload_json FROM entity_resolution_entities
            WHERE batch_id=? AND entity_id=?''',(batch_id,entity_id)).fetchone()
        cache[entity_id]=_load_json(row[0]) if row else None
    return cache[entity_id]


def _current_entity(con,batch_id,entity_id,entities,validity,hashes):
    """Check only this entity's support and actual parent closure, with shared caching."""
    if entity_id in validity:return validity[entity_id]
    active=set()
    own_valid={}
    stack=[(entity_id,False)]
    while stack:
        eid,finished=stack.pop()
        if finished:
            active.remove(eid)
            entity=entities[eid]
            validity[eid]=own_valid[eid] and all(
                validity.get(parent,False) for parent in entity.get('parent_problem_ids',[]))
            continue
        if eid in validity:continue
        entity=_entity_payload(con,batch_id,eid,entities)
        if entity is None:
            validity[eid]=False
            continue
        active.add(eid)
        current=True
        for support in entity['support']:
            sid=support['extraction_id']
            if sid not in hashes:
                row=con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                                (sid,)).fetchone()
                hashes[sid]=hashlib.sha256(row[0].encode('utf-8')).hexdigest() if row else None
            if hashes[sid] != support['source_payload_sha256'].lower():
                current=False
        own_valid[eid]=current
        stack.append((eid,True))
        for parent in reversed(entity.get('parent_problem_ids',[])):
            if parent in active:
                own_valid[eid]=False  # Defensive for a damaged historical cycle.
            elif parent not in validity:
                stack.append((parent,False))
    return validity[entity_id]


def get_resolution_entity(con,batch_id,entity_id):
    """Explicit indexed entity detail with current support and parent closure."""
    con.execute('SAVEPOINT entity_resolution_read')
    try:
        entities={};validity={};hashes={}
        current=_current_entity(con,batch_id,entity_id,entities,validity,hashes)
        result={'entity_id':entity_id,'stale':not current,
                'entity':entities.get(entity_id) if current else None}
        con.execute('RELEASE SAVEPOINT entity_resolution_read')
        return result
    except BaseException:
        con.execute('ROLLBACK TO SAVEPOINT entity_resolution_read')
        con.execute('RELEASE SAVEPOINT entity_resolution_read')
        raise


def _get_resolution_mentions_snapshot(con,batch_id,extraction_id):
    """All source hashes are read from the same SQLite snapshot."""
    mention_rows=con.execute('''SELECT mention_path,decision,entity_id,source_payload_sha256,payload_json
        FROM entity_resolution_mentions WHERE batch_id=? AND extraction_id=? ORDER BY mention_path''',
        (batch_id,extraction_id)).fetchall()
    if not mention_rows:
        return []
    source=con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                       (extraction_id,)).fetchone()
    current_hash=hashlib.sha256(source[0].encode('utf-8')).hexdigest() if source else None
    entities={};validity={};support_hashes={extraction_id:current_hash}
    result=[]
    for path,decision,entity_id,pinned,raw in mention_rows:
        stale=(current_hash != pinned.lower())
        resolved=None
        mention=_load_json(raw)
        if decision=='resolved' and not stale:
            stale=not _current_entity(con,batch_id,entity_id,entities,validity,support_hashes)
            if not stale:resolved=entities[entity_id]
        active_components=[]
        if 'review_outcome' in mention and mention['review_outcome']=='composite_label' and not stale:
            for component_id in mention['component_entity_ids']:
                if not _current_entity(con,batch_id,component_id,entities,validity,support_hashes):
                    stale=True
            if not stale:
                active_components=[entities[cid] for cid in mention['component_entity_ids']]
        item={'batch_id':batch_id,'extraction_id':extraction_id,'mention_path':path,
                       'decision':decision,'entity_id':entity_id,'stale':stale,
                       'entity':resolved,'mention':mention}
        if 'review_outcome' in mention:
            item.update(review_outcome=mention['review_outcome'],
                        component_entity_ids=mention['component_entity_ids'],
                        component_entities=active_components)
        result.append(item)
    return result
