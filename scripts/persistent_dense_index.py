"""Explicit offline builder and vector-only search for the frozen six-kind HNSW."""
import argparse
import json

from kg_builder.persistent_dense import PersistentDenseIndex, build, verify_source


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    make = sub.add_parser('build')
    make.add_argument('--run-id', required=True)
    make.add_argument('--index-dir', required=True)
    make.add_argument('--model-id', required=True)
    make.add_argument('--query-model-id')
    make.add_argument('--database-url')
    verify = sub.add_parser('verify-source')
    verify.add_argument('--run-id', required=True)
    verify.add_argument('--index-dir', required=True)
    verify.add_argument('--database-url')
    lookup = sub.add_parser('search')
    lookup.add_argument('--index-dir', required=True)
    lookup.add_argument('--vector-json', required=True)
    lookup.add_argument('--kind', action='append')
    lookup.add_argument('--top-k', type=int, default=10)
    lookup.add_argument('--expected-run-id')
    lookup.add_argument('--expected-graph-namespace')
    lookup.add_argument('--expected-model-id')
    args = parser.parse_args()
    if args.command == 'build':
        result = build(args.run_id, args.index_dir, args.model_id,
                       query_model_id=args.query_model_id, database_url=args.database_url)
    elif args.command == 'verify-source':
        result = verify_source(args.run_id, args.index_dir, database_url=args.database_url)
    else:
        index = PersistentDenseIndex(args.index_dir, expected_run_id=args.expected_run_id,
            expected_graph_namespace=args.expected_graph_namespace,
            expected_model_id=args.expected_model_id)
        result = index.search(json.loads(args.vector_json), types=args.kind, top_k=args.top_k)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
