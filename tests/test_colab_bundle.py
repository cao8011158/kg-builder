"""Bundle integrity and target safety without touching the live services."""
import ast
import json
from pathlib import Path
from contextlib import contextmanager
import zipfile

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
    hnsw = root / 'dense' / 'hnsw'
    hnsw.mkdir()
    (hnsw / 'manifest.json').write_text('{"format":"kg-hnsw/1"}')
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


@pytest.mark.parametrize('relative', ('bm25/manifest.json', 'dense/manifest.json',
                                      'dense/hnsw/manifest.json'))
def test_zip_roundtrip_and_nested_manifest_tamper(tmp_path, relative):
    original = tmp_path / 'original'
    original.mkdir()
    _fixture(original)
    archive_path = tmp_path / 'bundle.zip'
    with zipfile.ZipFile(archive_path, 'w') as archive:
        for path in original.rglob('*'):
            if path.is_file():
                archive.write(path, path.relative_to(original).as_posix())
    restored = tmp_path / 'restored'
    bundle.extract_zip(archive_path, restored)
    assert set(bundle.verify_bundle(restored)['files']) == {
        p.relative_to(original).as_posix() for p in original.rglob('*') if p.is_file() and p != original / 'manifest.json'}
    with (restored / relative).open('a') as stream:
        stream.write('tampered')
    with pytest.raises(ValueError, match='Bundle file differs'):
        bundle.verify_bundle(restored)


@pytest.mark.parametrize('partial', (False, True))
def test_neo4j_retry_discards_attempt_counts_and_files(monkeypatch, tmp_path, partial):
    ns = 'graph:retry'

    class Tx:
        def __init__(self, ids, fail_after_first=False):
            self.ids = ids
            self.fail_after_first = fail_after_first

        def run(self, query, **kwargs):
            if 'RETURN n.node_id' in query:
                def rows():
                    for node_id in self.ids:
                        yield {'id': node_id, 'labels': ['KGNode', 'Paper'],
                               'properties': {'run_id': ns, 'node_id': node_id}}
                        if self.fail_after_first:
                            raise RuntimeError('simulated transient read failure')
                return rows()
            return []

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute_read(self, callback):
            try:
                callback(Tx(['first', 'extra'], fail_after_first=partial))
            except RuntimeError:
                pass
            return callback(Tx(['final']))

    class Driver:
        def session(self, **kwargs):
            return Session()

    @contextmanager
    def driver():
        yield Driver()

    monkeypatch.setattr(bundle, '_neo_driver', driver)
    graph = bundle._graph_export(tmp_path / 'neo4j', ns)
    assert graph['node_counts']['Paper'] == 1
    assert graph['edge_count'] == 0
    assert json.loads((tmp_path / 'neo4j/nodes.jsonl').read_text().strip())['id'] == 'final'


def test_colab_python_cells_parse():
    document = (Path(__file__).resolve().parents[1] / 'docs/COLAB_KG_RUNTIME.md').read_text()
    for cell in document.split('```python\n')[1:]:
        ast.parse(cell.split('```', 1)[0])


def test_colab_meta_and_official_mcp_contract():
    document = (Path(__file__).resolve().parents[1] / 'docs/COLAB_KG_RUNTIME.md').read_text()
    configuration = document.index('dbms.security.procedures.unrestricted=apoc.meta.*')
    startup = document.index('runuser -u kgneo -- "$NEO_HOME/bin/neo4j" start')
    restore = document.index("'restore-neo4j', str(KG_BUNDLE)")
    meta_probe = document.index('CALL apoc.meta.schema({sample: 100})')
    handshake = document.index('async with Neo4jMCPTools() as tools:')
    assert configuration < startup < restore < meta_probe < handshake
    assert "'apoc.meta.schema' in procedures" in document
    assert "'neo4j-mcp-server==1.6.1', 'mcp>=1.30,<2'" in document
    assert "'neo4j_get_schema', 'neo4j_read_cypher'" in document
    assert "'NEO4J_MCP_READ_ONLY'] = 'true'" in document
    assert "'NEO4J_MCP_TOOLS'] = 'get-schema,read-cypher'" in document
    assert '/content/kg-builder[neo4j,retrieval,voyage]' in document
    assert "os.environ['KG_GRAPH_DATABASE_URL'] = make_conninfo(" in document
    assert "verify_source(manifest['run_id'], KG_BUNDLE / 'dense')" in document
