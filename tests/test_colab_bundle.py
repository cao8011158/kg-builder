"""Bundle integrity and target safety without touching the live services."""
import json
from pathlib import Path

import pytest

from kg_builder import colab_bundle as bundle


def _fixture(root: Path):
    run_id, ns, snap = 'run-1', 'graph:one', 'hash-1'
    for name, counts_key, format_id in (('bm25', 'counts', 'kg-bm25/1'),
                                        ('dense', 'kind_counts', 'kg-persistent-dense/1')):
        directory = root / name
        directory.mkdir()
        (directory / 'manifest.json').write_text(json.dumps({
            'format': format_id, 'run_id': run_id, 'graph_namespace': ns,
            'snapshot_hash': snap, counts_key: {kind: 1 for kind in bundle.KINDS}}))
    graph = root / 'neo4j'
    graph.mkdir()
    (graph / 'nodes.jsonl').write_bytes(bundle._json({'id': 'paper:1', 'labels': ['Paper'],
                                                       'properties': {'run_id': ns, 'node_id': 'paper:1'}}))
    (graph / 'edges.jsonl').write_bytes(b'')
    (root / 'postgres.dump').write_bytes(b'native-dump-placeholder')
    manifest = {'format': bundle.FORMAT, 'run_id': run_id, 'graph_namespace': ns,
                'snapshot_hash': snap,
                'graph': {'nodes_sha256': bundle._sha(graph / 'nodes.jsonl'),
                          'edges_sha256': bundle._sha(graph / 'edges.jsonl')},
                'files': bundle._files(root)}
    (root / 'manifest.json').write_bytes(bundle._json(manifest))
    return manifest


def test_verify_detects_tampering(tmp_path):
    expected = _fixture(tmp_path)
    assert bundle.verify_bundle(tmp_path) == expected
    with (tmp_path / 'neo4j/nodes.jsonl').open('ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='Bundle file differs'):
        bundle.verify_bundle(tmp_path)


def test_verify_rejects_manifest_escape(tmp_path):
    manifest = _fixture(tmp_path)
    manifest['files']['../outside'] = {'sha256': '0', 'bytes': 0}
    (tmp_path / 'manifest.json').write_bytes(bundle._json(manifest))
    with pytest.raises(ValueError, match='Bundle files differ|Unsafe bundle'):
        bundle.verify_bundle(tmp_path)


def test_export_refuses_existing_target_before_source_access(tmp_path):
    target = tmp_path / 'existing'
    target.mkdir()
    with pytest.raises(FileExistsError):
        bundle.export_bundle('run-1', target, tmp_path / 'bm25', tmp_path / 'dense')


def test_pg_restore_requires_complete_runtime_environment(monkeypatch, tmp_path):
    _fixture(tmp_path)
    for key in ('PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD'):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValueError, match='PGHOST'):
        bundle.restore_postgres(tmp_path)
