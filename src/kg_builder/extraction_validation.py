"""Executable extraction schemas and local, version-aware semantic checks."""
from importlib.resources import files
import json
import math

from jsonschema import Draft202012Validator

SCHEMA_VERSION = 'research-kg-extraction/0.2-draft-pilot'
SCHEMAS = {
    None: 'legacy.json',
    SCHEMA_VERSION: 'v0_2_draft.json',
}
KINDS = {
    'research_problems': 'problem', 'methods': 'method', 'contributions': 'contribution',
    'artifacts': 'artifact', 'benchmarks': 'benchmark', 'metrics': 'metric',
    'evaluations': 'evaluation', 'findings': 'finding',
}
RELATIONS = {
    'HAS_CONTRIBUTION': ('paper_version', 'contribution'),
    'PROPOSES_METHOD': ('contribution', 'method'),
    'INTRODUCES_ARTIFACT': ('contribution', 'artifact'),
    'ADDRESSES': ('method', 'problem'),
    'USES_ARTIFACT': ('method', 'artifact'),
    'PRODUCES_ARTIFACT': ('method', 'artifact'),
    'USES_METHOD': ('method', 'method'),
    'REPORTS_EVALUATION': ('paper_version', 'evaluation'),
    'EVALUATES_ARTIFACT': ('evaluation', 'artifact'),
    'USES_BENCHMARK': ('evaluation', 'benchmark'),
    'MEASURES': ('evaluation', 'metric'),
}
VALIDATORS = {version: Draft202012Validator(json.loads(
    files('kg_builder.schemas').joinpath(name).read_text(encoding='utf-8')))
    for version, name in SCHEMAS.items()}


def _finite(value):
    if isinstance(value,float) and not math.isfinite(value):
        raise ValueError('Nonfinite numeric value in extraction')
    if isinstance(value,dict):
        for item in value.values(): _finite(item)
    elif isinstance(value,list):
        for item in value: _finite(item)


def validate_extraction(payload):
    _finite(payload)
    if not isinstance(payload,dict):
        raise ValueError('Extraction must be a JSON object')
    version = payload['schema_version'] if 'schema_version' in payload else None
    if 'schema_version' in payload and version != SCHEMA_VERSION:
        raise ValueError(f'Unknown extraction schema_version: {version!r}')
    if version not in SCHEMAS:
        raise ValueError(f'Unknown extraction schema_version: {version!r}')
    errors = sorted(VALIDATORS[version].iter_errors(payload),key=lambda e:list(map(str,e.path)))
    if errors:
        error=errors[0]
        raise ValueError(f'Extraction schema at {list(error.path)}: {error.message}')
    if version is None:
        return
    ids = {payload['version_id']:'paper_version'}
    entities={}
    for field,kind in KINDS.items():
        for entity in payload[field]:
            key=entity['key']
            if key in ids:
                raise ValueError(f'Duplicate entity key: {key}')
            if kind in ('contribution','evaluation') and not key.startswith(payload['version_id']+'#'):
                raise ValueError(f'{kind} must be scoped to the owning paper version: {key}')
            ids[key]=kind
            entities[key]=entity
    evidence={}
    for locator in payload['evidence_locators']:
        eid=locator['evidence_id']
        if eid in evidence:
            raise ValueError(f'Duplicate evidence_id: {eid}')
        if locator['source_url'].split('#',1)[0] != payload['paper_version']['source_url'].split('#',1)[0]:
            raise ValueError(f'Evidence URL differs from pinned paper version: {eid}')
        capture=payload['paper_version'].get('retrieved_html_sha256')
        if capture and locator.get('html_sha256') != capture:
            raise ValueError(f'Evidence capture hash differs from pinned version: {eid}')
        evidence[eid]=locator
    def check_refs(refs):
        for ref in refs:
            if ref not in evidence:
                raise ValueError(f'Unresolved evidence reference: {ref}')
    for entity in entities.values():
        check_refs(entity['evidence_refs'])
    edges=set()
    for relation in payload['relations']:
        source,target,predicate=(relation[k] for k in ('source','target','predicate'))
        expected=RELATIONS.get(predicate)
        if expected is None:
            raise ValueError(f'Unknown relation predicate: {predicate}')
        if (ids.get(source),ids.get(target)) != expected:
            raise ValueError(f'Wrong relation endpoints for {predicate}: {source} -> {target}')
        check_refs(relation['evidence_refs'])
        edges.add((source,predicate,target))
    for evaluation in payload['evaluations']:
        key=evaluation['key']
        for field,kind,predicate in (
            ('target_artifact','artifact','EVALUATES_ARTIFACT'),
            ('benchmark','benchmark','USES_BENCHMARK'),
            ('metric','metric','MEASURES')):
            target=evaluation[field]
            if ids.get(target) != kind:
                raise ValueError(f'Evaluation {field} references wrong or missing {kind}: {target}')
            # If the graph declares an edge of this kind, it must agree with the field.
            if any(s==key and p==predicate and t!=target for s,p,t in edges):
                raise ValueError(f'Evaluation {field} conflicts with {predicate} relation')
        if evaluation['unit'] != entities[evaluation['metric']].get('unit'):
            raise ValueError(f'Evaluation unit conflicts with metric {evaluation["metric"]}')
    return version


def validate_extraction_batch(records):
    if not isinstance(records,list):
        raise ValueError('Extraction input must be a JSON array')
    seen={}
    for index,record in enumerate(records):
        try:
            validate_extraction(record)
        except ValueError as exc:
            raise ValueError(f'Extraction item {index}: {exc}') from exc
        key=(record['paper_id'],record['version_id'],record.get('extractor_type','assistant_manual'),
             record.get('extractor_version','2026-10-31-v1'))
        canonical=json.dumps(record,sort_keys=True,ensure_ascii=False,allow_nan=False)
        if key in seen and seen[key] != canonical:
            raise ValueError(f'Conflicting extraction entries at item {index}')
        seen[key]=canonical
    return records
