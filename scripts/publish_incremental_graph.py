#!/usr/bin/env python3
"""Build and publish a replayable graph revision from an approved delta."""
import argparse
from contextlib import redirect_stdout
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))

from kg_builder.incremental_update import prepare_incremental_snapshot, write_immutable_json
from kg_builder.snapshot_graph import canonical_hash
from scripts import build_snapshot_graph


def _bm25_manifest(run_id, index_dir):
    with redirect_stdout(sys.stderr):
        from kg_builder.bm25_retrieval import BM25Index, build

        if not index_dir.exists():
            try:
                build(run_id, index_dir)
            except FileExistsError:
                # Another invocation may have completed this immutable index.
                if not index_dir.is_dir():
                    raise
        with BM25Index(index_dir, expected_run_id=run_id) as index:
            return index.manifest


def _publish(args):
    release_dir = args.release_dir.resolve()
    snapshot_path = release_dir / 'prepared.json'
    snapshot = prepare_incremental_snapshot(
        args.base_snapshot, args.delta_snapshot, snapshot_path,
        remove_paper_ids=args.remove_paper_id,
    )
    snapshot_hash = snapshot['snapshot_hash']
    run_id = 'incremental-' + canonical_hash([args.base_run_id, snapshot_hash])
    expected = {
        'format': 'kg-incremental-release/1',
        'base_run_id': args.base_run_id,
        'run_id': run_id,
        'snapshot_hash': snapshot_hash,
        'graph_job_dir': 'graph',
        'bm25_index_dir': 'bm25',
    }
    if args.prepare_only:
        return {'state': 'prepared', 'run_id': run_id, 'snapshot_hash': snapshot_hash}

    release_path = release_dir / 'release.json'
    if release_path.exists():
        release = json.loads(release_path.read_text(encoding='utf-8'))
        if not isinstance(release, dict) or any(release.get(k) != v for k, v in expected.items()):
            raise ValueError('Release manifest conflicts with this graph revision')
        if release.get('state') != 'published' or not release.get('graph_namespace'):
            raise ValueError('Invalid published graph release manifest')
        return release

    graph_dir = release_dir / 'graph'
    graph_args = [
        '--snapshot', str(snapshot_path), '--job-dir', str(graph_dir),
        '--run-id', run_id, '--base-run-id', args.base_run_id, '--neo4j',
    ]
    for option, value in (
        ('--name-mode', args.name_mode), ('--name-base-url', args.name_base_url),
        ('--name-model', args.name_model), ('--embedding-cache-dir', args.embedding_cache_dir),
    ):
        if value is not None:
            graph_args.extend((option, str(value)))
    status = build_snapshot_graph.main([*graph_args, 'run', '--jev-model', args.jev_model], emit=False)
    if status['phase'] == 'grouped' and status['name_tasks'].get('queued', 0) + status['name_tasks'].get('leased', 0):
        return {
            'state': 'waiting_for_names', 'run_id': run_id, 'snapshot_hash': snapshot_hash,
            'job_dir': str(graph_dir), 'snapshot': str(snapshot_path),
            'name_tasks': status['name_tasks'],
        }
    namespace = status['neo4j_run_id']
    if (status['phase'] != 'complete' or status['index_status'] != 'verified'
            or not status['completed_at'] or not namespace
            or status['neo4j_projection']['pending'] != 0
            or status['snapshot_hash'] != snapshot_hash):
        raise RuntimeError('Graph revision is incomplete or has undelivered projection events')

    from kg_builder.graph_validation import validate_graph

    audit = validate_graph(snapshot_path, graph_dir, run_id=run_id)
    if not audit['structural']['passed']:
        failures = audit['structural']['failure_counts']
        raise RuntimeError(f'Graph revision failed structural validation: {failures}')
    index = _bm25_manifest(run_id, release_dir / 'bm25')
    if index['graph_namespace'] != namespace or index['snapshot_hash'] != snapshot_hash:
        raise ValueError('BM25 index differs from the graph revision')
    release = {**expected, 'state': 'published', 'graph_namespace': namespace}
    write_immutable_json(release_path, release)
    return release


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-run-id', required=True)
    parser.add_argument('--base-snapshot', type=Path, required=True)
    parser.add_argument('--delta-snapshot', type=Path, required=True)
    parser.add_argument('--release-dir', type=Path, required=True)
    parser.add_argument('--remove-paper-id', action='append', default=[])
    parser.add_argument('--prepare-only', action='store_true', help='Write the merged snapshot without opening graph services')
    parser.add_argument('--name-mode', choices=['external', 'api'])
    parser.add_argument('--name-base-url')
    parser.add_argument('--name-model')
    parser.add_argument('--embedding-cache-dir', type=Path)
    parser.add_argument('--jev-model', default='jev-1.14.0')
    args = parser.parse_args(argv)
    if not args.base_run_id.strip():
        parser.error('--base-run-id must be nonempty')
    result = _publish(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == '__main__':
    main()
