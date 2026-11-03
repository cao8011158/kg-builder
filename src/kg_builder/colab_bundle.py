"""Portable native PostgreSQL backup and published Neo4j graph for Colab.

All connection secrets come from the process environment.  The bundle contains
database data and prebuilt indexes, never database credentials or model keys.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid
import zipfile

from .neo4j_store import LABELS, RELATIONS

FORMAT = 'kg-colab-native-bundle/1'
KINDS = ('Paper', 'ResearchProblem', 'Method', 'Benchmark', 'Contribution', 'ResearchArea')


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _json(value) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode('utf-8')


def _files(root: Path) -> dict[str, dict]:
    if any(p.is_symlink() for p in root.rglob('*')):
        raise ValueError('Bundle contains a symbolic link')
    return {p.relative_to(root).as_posix(): {'sha256': _sha(p), 'bytes': p.stat().st_size}
            for p in sorted(root.rglob('*')) if p.is_file() and p != root / 'manifest.json'}


def _safe_index(directory: Path, *, kind: str, run_id: str, namespace: str, snapshot_hash: str) -> dict:
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError(f'{kind} index directory missing or linked')
    manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
    if (manifest.get('run_id'), manifest.get('graph_namespace'), manifest.get('snapshot_hash')) != (run_id, namespace, snapshot_hash):
        raise ValueError(f'{kind} index identity differs from PostgreSQL graph')
    if kind == 'bm25' and (manifest.get('format') != 'kg-bm25/1' or set(manifest.get('counts', {})) != set(KINDS)):
        raise ValueError('BM25 manifest is incomplete')
    if kind == 'dense' and (manifest.get('format') != 'kg-persistent-dense/1' or set(manifest.get('kind_counts', {})) != set(KINDS)):
        raise ValueError('Persistent dense manifest is incomplete')
    for path in directory.rglob('*'):
        if path.is_symlink():
            raise ValueError(f'{kind} index contains a symbolic link')
    return manifest


def _pg_env(*, source: bool) -> dict[str, str]:
    env = os.environ.copy()
    if source:
        password = env.get('KG_PG_APP_PASSWORD')
        if not password:
            raise ValueError('KG_PG_APP_PASSWORD required')
        env.update(PGHOST='127.0.0.1', PGPORT='55432', PGDATABASE='kg_graph_full_20261103_990ffcd6',
                   PGUSER='kg_preparation', PGPASSWORD=password)
    elif not all(env.get(k) for k in ('PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD')):
        raise ValueError('PGHOST, PGPORT, PGDATABASE, PGUSER and PGPASSWORD required for restore')
    return env


def _pg_run(args: list[str], env: dict[str, str]) -> None:
    # Do not include connection URLs or passwords in arguments or exception text.
    result = subprocess.run(args, env=env, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f'{Path(args[0]).name} failed (exit {result.returncode}); inspect service logs locally')


def _pg_identity(run_id: str, env: dict[str, str]) -> dict:
    import psycopg
    schema_name = 'kg_graph_' + hashlib.sha256(run_id.encode()).hexdigest()[:24]
    with psycopg.connect(host=env['PGHOST'], port=env['PGPORT'], dbname=env['PGDATABASE'],
                         user=env['PGUSER'], password=env['PGPASSWORD'], autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            from psycopg import sql
            cur.execute(sql.SQL('SELECT key,value FROM {}.meta WHERE key IN (%s,%s,%s)').format(sql.Identifier(schema_name)),
                        ('neo4j_run_id', 'snapshot_hash', 'phase'))
            meta = dict(cur.fetchall())
    if not meta.get('neo4j_run_id') or not meta.get('snapshot_hash') or meta.get('phase') != 'complete':
        raise ValueError('PostgreSQL graph is not a complete published snapshot')
    return {'run_id': run_id, 'source_schema': schema_name, 'graph_namespace': meta['neo4j_run_id'],
            'snapshot_hash': meta['snapshot_hash'], 'phase': meta.get('phase')}


@contextmanager
def _neo_driver():
    required = ('NEO4J_URI', 'NEO4J_USERNAME', 'NEO4J_PASSWORD')
    if any(not os.environ.get(k) for k in required):
        raise ValueError('NEO4J_URI, NEO4J_USERNAME and NEO4J_PASSWORD required')
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(os.environ['NEO4J_URI'],
                                  auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD']))
    try:
        yield driver
    finally:
        driver.close()


def _graph_export(directory: Path, namespace: str) -> dict:
    directory.mkdir()
    with _neo_driver() as driver, driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
        def read(tx):
            # execute_read may retry this callback. Each attempt overwrites both
            # streams and owns its own counts; only the successful attempt wins.
            counts = Counter()
            with (directory / 'nodes.jsonl').open('wb') as out:
                rows = tx.run('MATCH (n:KGNode {run_id:$ns}) RETURN n.node_id AS id, labels(n) AS labels, properties(n) AS properties ORDER BY id', ns=namespace)
                for record in rows:
                    labels = sorted(set(record['labels']) - {'KGNode'})
                    if len(labels) != 1 or labels[0] not in LABELS:
                        raise ValueError('Unexpected graph node labels')
                    properties = dict(record['properties'])
                    if properties.get('run_id') != namespace or properties.get('node_id') != record['id']:
                        raise ValueError('Graph node identity mismatch')
                    out.write(_json({'id': record['id'], 'labels': labels, 'properties': properties}))
                    counts[labels[0]] += 1
            with (directory / 'edges.jsonl').open('wb') as out:
                rows = tx.run('MATCH (a:KGNode {run_id:$ns})-[r]->(b:KGNode {run_id:$ns}) '
                              'RETURN a.node_id AS source, b.node_id AS target, type(r) AS type, properties(r) AS properties '
                              'ORDER BY source,type,target', ns=namespace)
                for record in rows:
                    if record['type'] not in RELATIONS:
                        raise ValueError('Unexpected graph relation')
                    out.write(_json(dict(record)))
                    counts['edges'] += 1
            return counts
        counts = session.execute_read(read)
    return {'node_counts': {k: counts[k] for k in sorted(LABELS)}, 'edge_count': counts['edges'],
            'nodes_sha256': _sha(directory / 'nodes.jsonl'), 'edges_sha256': _sha(directory / 'edges.jsonl'),
            'excluded': 'KGEvent audit nodes; PostgreSQL graph_outbox remains in the native backup'}


def export_bundle(run_id: str, target: str | Path, bm25_dir: str | Path, dense_dir: str | Path,
                  *, pg_dump: str = 'pg_dump', pg_restore: str = 'pg_restore') -> dict:
    """Create a new immutable directory and ZIP from live native services."""
    target = Path(target).absolute()
    if target.exists() or target.with_suffix('.zip').exists():
        raise FileExistsError('Bundle target or ZIP already exists')
    env = _pg_env(source=True)
    identity = _pg_identity(run_id, env)
    bm25 = _safe_index(Path(bm25_dir), kind='bm25', run_id=run_id,
                       namespace=identity['graph_namespace'], snapshot_hash=identity['snapshot_hash'])
    dense = _safe_index(Path(dense_dir), kind='dense', run_id=run_id,
                        namespace=identity['graph_namespace'], snapshot_hash=identity['snapshot_hash'])
    if bm25['counts'] != dense['kind_counts']:
        raise ValueError('BM25 and dense index six-kind counts differ')
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / ('.' + target.name + '-' + uuid.uuid4().hex + '.tmp')
    staging.mkdir()
    try:
        _pg_run([pg_dump, '--format=custom', '--no-owner', '--no-privileges', '--file', str(staging / 'postgres.dump')], env)
        _pg_run([pg_restore, '--list', str(staging / 'postgres.dump')], env)
        graph = _graph_export(staging / 'neo4j', identity['graph_namespace'])
        if any(graph['node_counts'][kind] != bm25['counts'][kind] for kind in KINDS):
            raise ValueError('Published graph and search-index six-kind counts differ')
        if _pg_identity(run_id, env) != identity:
            raise ValueError('PostgreSQL graph identity changed during export')
        shutil.copytree(bm25_dir, staging / 'bm25')
        shutil.copytree(dense_dir, staging / 'dense')
        manifest = {'format': FORMAT, **identity, 'source_database': env['PGDATABASE'],
                    'graph': graph, 'bm25_counts': bm25['counts'], 'dense_counts': dense['kind_counts'],
                    'dense_model_id': dense['model_id'], 'dense_query_model_id': dense.get('query_model_id'),
                    'files': _files(staging)}
        (staging / 'manifest.json').write_bytes(_json(manifest))
        verify_bundle(staging)
        staging.rename(target)
        zip_temp = target.parent / ('.' + target.name + '-' + uuid.uuid4().hex + '.zip.tmp')
        try:
            with zipfile.ZipFile(zip_temp, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for relative in sorted(manifest['files']):
                    archive.write(target / relative, relative)
                archive.write(target / 'manifest.json', 'manifest.json')
            zip_temp.rename(target.with_suffix('.zip'))
        finally:
            zip_temp.unlink(missing_ok=True)
        return {**manifest, 'archive': {'path': str(target.with_suffix('.zip')),
                                         'bytes': target.with_suffix('.zip').stat().st_size,
                                         'sha256': _sha(target.with_suffix('.zip'))}}
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def verify_bundle(root: str | Path) -> dict:
    root = Path(root)
    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('format') != FORMAT or not manifest.get('files'):
        raise ValueError('Unsupported or empty bundle')
    if set(_files(root)) != set(manifest['files']):
        raise ValueError('Bundle files differ from manifest')
    for relative, expected in manifest['files'].items():
        parts = Path(relative).parts
        if Path(relative).is_absolute() or '..' in parts or not parts or any((root / Path(*parts[:i])).is_symlink() for i in range(1, len(parts))):
            raise ValueError('Unsafe bundle file path')
        path = root / relative
        if path.is_symlink() or path.stat().st_size != expected['bytes'] or _sha(path) != expected['sha256']:
            raise ValueError(f'Bundle file differs: {relative}')
    for kind in ('bm25', 'dense'):
        _safe_index(root / kind, kind=kind, run_id=manifest['run_id'],
                    namespace=manifest['graph_namespace'], snapshot_hash=manifest['snapshot_hash'])
    graph = manifest['graph']
    if (_sha(root / 'neo4j/nodes.jsonl'), _sha(root / 'neo4j/edges.jsonl')) != (graph['nodes_sha256'], graph['edges_sha256']):
        raise ValueError('Graph fingerprint differs')
    return manifest


def extract_zip(archive_path: str | Path, target: str | Path) -> dict:
    target = Path(target).absolute()
    if target.exists():
        raise FileExistsError(target)
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            path = Path(member.filename)
            if path.is_absolute() or '..' in path.parts or member.is_dir():
                raise ValueError('Unsafe ZIP member')
        target.mkdir(parents=True)
        try:
            archive.extractall(target)
            return verify_bundle(target)
        except BaseException:
            shutil.rmtree(target)
            raise


def restore_postgres(root: str | Path, *, pg_restore: str = 'pg_restore') -> None:
    """Restore into a separately created empty database named by PGDATABASE."""
    verify_bundle(root)
    env = _pg_env(source=False)
    import psycopg
    with psycopg.connect(host=env['PGHOST'], port=env['PGPORT'], dbname=env['PGDATABASE'],
                         user=env['PGUSER'], password=env['PGPASSWORD']) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')")
            if cur.fetchone()[0]:
                raise ValueError('PostgreSQL target database is not empty')
    _pg_run([pg_restore, '--exit-on-error', '--no-owner', '--no-privileges', '--dbname', env['PGDATABASE'],
             str(Path(root) / 'postgres.dump')], env)


def restore_neo4j(root: str | Path, *, batch_size: int = 500) -> dict:
    """Restore published query graph into an empty dedicated Neo4j database."""
    manifest = verify_bundle(root)
    root = Path(root)
    if batch_size < 1 or batch_size > 5000:
        raise ValueError('batch_size outside 1..5000')
    with _neo_driver() as driver, driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
        if session.run('MATCH (n) RETURN count(n) AS count').single()['count']:
            raise ValueError('Neo4j target database is not empty')
        session.run('CREATE CONSTRAINT kg_node_identity IF NOT EXISTS FOR (n:KGNode) REQUIRE (n.run_id,n.node_id) IS UNIQUE').consume()
        for filename, labels_or_types in (('nodes.jsonl', LABELS), ('edges.jsonl', RELATIONS)):
            groups = {kind: [] for kind in labels_or_types}
            with (root / 'neo4j' / filename).open('r', encoding='utf-8') as stream:
                for line in stream:
                    item = json.loads(line)
                    kind = item['labels'][0] if filename == 'nodes.jsonl' else item['type']
                    if kind not in groups:
                        raise ValueError('Unexpected graph label or relation')
                    if filename == 'nodes.jsonl' and (item['properties']['run_id'] != manifest['graph_namespace'] or
                                                       item['properties']['node_id'] != item['id']):
                        raise ValueError('Graph node identity mismatch')
                    groups[kind].append(item)
                    if len(groups[kind]) == batch_size:
                        _restore_group(session, kind, groups[kind], filename, manifest['graph_namespace'])
                        groups[kind].clear()
            for kind, items in groups.items():
                if items:
                    _restore_group(session, kind, items, filename, manifest['graph_namespace'])
        counts = {r['label']: r['count'] for r in session.run('MATCH (n:KGNode {run_id:$ns}) WITH labels(n) AS ls, count(*) AS count UNWIND ls AS label RETURN label,count', ns=manifest['graph_namespace']) if r['label'] != 'KGNode'}
        edge_count = session.run('MATCH (:KGNode {run_id:$ns})-[r]->(:KGNode {run_id:$ns}) RETURN count(r) AS count', ns=manifest['graph_namespace']).single()['count']
        if counts != {k: v for k, v in manifest['graph']['node_counts'].items() if v} or edge_count != manifest['graph']['edge_count']:
            raise ValueError('Restored graph counts differ')
    with tempfile.TemporaryDirectory(prefix='kg-restore-fingerprint-') as temp:
        observed = _graph_export(Path(temp) / 'neo4j', manifest['graph_namespace'])
        if observed != manifest['graph']:
            raise ValueError('Restored graph fingerprint differs')
    return {'node_counts': counts, 'edge_count': edge_count}


def _restore_group(session, kind: str, items: list[dict], filename: str, namespace: str) -> None:
    if filename == 'nodes.jsonl':
        session.execute_write(lambda tx: tx.run(
            'UNWIND $items AS item CREATE (n:KGNode:' + kind + ') SET n = item.properties', items=items).consume())
    else:
        result = session.execute_write(lambda tx: tx.run(
            'UNWIND $items AS item MATCH (a:KGNode {run_id:$ns,node_id:item.source}) '
            'MATCH (b:KGNode {run_id:$ns,node_id:item.target}) CREATE (a)-[r:' + kind + ']->(b) '
            'SET r = item.properties RETURN count(r) AS count', items=items, ns=namespace).single()['count'])
        if result != len(items):
            raise ValueError('Graph edge endpoint missing')
