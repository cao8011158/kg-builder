"""Pure, immutable composition of approved prepared snapshots."""
from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

from .preparation import CANON, snapshot_digest
from .snapshot_graph import _extraction, _paper, read_snapshot
from .storage import arxiv_norm, normalise_doi


def write_immutable_json(path, value):
    """Publish complete canonical JSON bytes once; identical replays are harmless."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (CANON(value) + '\n').encode('utf-8')
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('xb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            if os.name == 'nt':
                os.rename(temporary, path)
            else:
                os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise ValueError('Immutable JSON output already exists with different content')
    finally:
        temporary.unlink(missing_ok=True)


def _approved(snapshot):
    """Index the prepared payload once and reject ambiguous per-paper records."""
    papers = {}
    for paper in snapshot['papers']:
        pid = paper.get('paper_id') if isinstance(paper, dict) else None
        if not isinstance(pid, str) or not pid or pid in papers:
            raise ValueError('Missing or duplicate paper identity')
        papers[pid] = paper
    extractions = {}
    item_keys = {}
    for wrapper in snapshot['extractions']:
        extraction = _extraction(wrapper)
        pid = extraction.get('paper_id') if isinstance(extraction, dict) else None
        if pid not in papers or pid in extractions:
            raise ValueError('Missing or duplicate approved extraction')
        _paper(papers, extraction)
        extractions[pid] = wrapper
        keys = set()
        for field in ('research_problems', 'methods', 'benchmarks', 'contributions'):
            for item in extraction.get(field, []):
                key = pid + '::' + item['key']
                if key in keys:
                    raise ValueError('Duplicate extraction item identity')
                keys.add(key)
        item_keys[pid] = keys
    if papers.keys() != extractions.keys():
        raise ValueError('Every approved paper needs one extraction')
    vectors = {pid: [] for pid in papers}
    owner = {key: pid for pid, keys in item_keys.items() for key in keys}
    seen = set()
    models = set()
    for row in snapshot['vectors']:
        key = row.get('item_key') if isinstance(row, dict) else None
        pid = owner.get(key)
        if pid is None or key in seen:
            raise ValueError('Unknown or duplicate snapshot vector')
        seen.add(key)
        vectors[pid].append(row)
        model = row.get('model_id')
        if not isinstance(model, str) or not model:
            raise ValueError('Missing vector model identity')
        models.add(model)
    if len(models) > 1:
        raise ValueError('Mixed embedding models in prepared snapshot')
    for pid, keys in item_keys.items():
        required = {pid + '::' + item['key'] for field in ('research_problems', 'methods', 'benchmarks')
                    for item in _extraction(extractions[pid]).get(field, [])}
        if not required <= seen:
            raise ValueError('Approved semantic item missing vector')
    return papers, extractions, vectors, models


def _check_version(previous, replacement):
    old = previous['paper_version'].get('version_label')
    new = replacement['paper_version'].get('version_label')
    version = re.compile(r'v([1-9]\d*)\Z')
    old_match = version.fullmatch(old) if isinstance(old, str) else None
    new_match = version.fullmatch(new) if isinstance(new, str) else None
    if old_match and (not new_match or int(new_match[1]) < int(old_match[1])):
        raise ValueError('Approved replacement downgrades a confirmed paper version')
    if old_match and new_match and old_match[1] == new_match[1] and previous['source_hash'] != replacement['source_hash']:
        raise ValueError('Conflicting payload for the same confirmed paper version')


def _check_identity(papers):
    identities = {}
    for pid, paper in papers.items():
        for kind, value in (('doi', normalise_doi(paper.get('doi'))),
                            ('arxiv', arxiv_norm(paper.get('arxiv_id')))):
            if value:
                key = (kind, value)
                if key in identities and identities[key] != pid:
                    raise ValueError('Strong scholarly identity belongs to different paper IDs')
                identities[key] = pid


def prepare_incremental_snapshot(base_path, delta_path, output_path, *, remove_paper_ids=()) -> dict:
    """Merge approved paper versions by ID without changing either input snapshot."""
    base_path, delta_path, output_path = map(lambda p: Path(p).resolve(), (base_path, delta_path, output_path))
    if len({base_path, delta_path, output_path}) != 3:
        raise ValueError('Base, delta and output paths must be distinct')
    if output_path.exists() and any(os.path.samefile(output_path, p) for p in (base_path, delta_path)):
        raise ValueError('Output aliases an input snapshot')
    base, _ = read_snapshot(base_path)
    delta, _ = read_snapshot(delta_path)
    if base.get('config', {}).get('review_policy', 'legacy-paper') != delta.get('config', {}).get('review_policy', 'legacy-paper'):
        raise ValueError('Mixed prepared review policies')
    old_papers, old_extractions, old_vectors, old_models = _approved(base)
    new_papers, new_extractions, new_vectors, new_models = _approved(delta)
    if old_models and new_models and old_models != new_models:
        raise ValueError('Mixed embedding models')
    removed = list(remove_paper_ids)
    if len(set(removed)) != len(removed) or any(not isinstance(pid, str) or not pid for pid in removed):
        raise ValueError('Removal IDs must be unique nonempty strings')
    if set(removed) & new_papers.keys():
        raise ValueError('A paper cannot be removed and replaced in one delta')
    if not set(removed) <= old_papers.keys():
        raise ValueError('Removal references unknown paper')
    for pid in new_papers.keys() & old_papers.keys():
        _check_version(old_papers[pid], new_papers[pid])
    selected = {pid: paper for pid, paper in old_papers.items() if pid not in removed}
    selected.update(new_papers)
    _check_identity(selected)
    result = {key: value for key, value in base.items() if key not in
              ('snapshot_hash', 'papers', 'extractions', 'vectors', 'review_metadata', 'incremental', 'counts', 'excluded', 'pending', 'no_usable_items')}
    result['papers'] = [selected[pid] for pid in sorted(selected)]
    result['extractions'] = [new_extractions.get(pid, old_extractions.get(pid)) for pid in sorted(selected)]
    result['vectors'] = [row for pid in sorted(selected) for row in
                         (new_vectors[pid] if pid in new_papers else old_vectors[pid])]
    policy = base.get('config', {}).get('review_policy', 'legacy-paper')
    if policy == 'item-v1':
        base_reviews = base['review_metadata']
        delta_reviews = delta['review_metadata']
        result['review_metadata'] = {pid: (delta_reviews if pid in new_papers else base_reviews)[pid]
                                     for pid in sorted(selected)}
    result['counts'] = {'collected': len(selected), 'ready': len(selected),
                        'excluded': 0, 'pending': 0, 'no_usable_items': 0}
    result['excluded'] = []
    result['pending'] = []
    if policy == 'item-v1':
        result['no_usable_items'] = []
    result['incremental'] = {'base_snapshot_hash': base['snapshot_hash'],
                             'delta_snapshot_hash': delta['snapshot_hash'],
                             'removed_paper_ids': sorted(removed)}
    result['snapshot_hash'] = snapshot_digest(result)
    write_immutable_json(output_path, result)
    return result
