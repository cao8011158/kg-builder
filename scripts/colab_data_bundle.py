"""Export, verify, extract, or restore the native Colab knowledge-graph bundle.

Credentials are read only from process environment. Never pass a connection URL
or password on this command line.
"""
from __future__ import annotations

import argparse
import json

from kg_builder.colab_bundle import (export_bundle, extract_zip, restore_neo4j,
                                      restore_postgres, verify_bundle)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    export = commands.add_parser('export')
    export.add_argument('--run-id', required=True)
    export.add_argument('--target', required=True)
    export.add_argument('--bm25-dir', required=True)
    export.add_argument('--dense-dir', required=True)
    export.add_argument('--pg-dump', default='pg_dump')
    export.add_argument('--pg-restore', default='pg_restore')
    verify = commands.add_parser('verify')
    verify.add_argument('directory')
    extract = commands.add_parser('extract')
    extract.add_argument('archive')
    extract.add_argument('directory')
    postgres = commands.add_parser('restore-postgres')
    postgres.add_argument('directory')
    postgres.add_argument('--pg-restore', default='pg_restore')
    neo4j = commands.add_parser('restore-neo4j')
    neo4j.add_argument('directory')
    neo4j.add_argument('--batch-size', type=int, default=500)
    args = parser.parse_args(argv)
    if args.command == 'export':
        result = export_bundle(args.run_id, args.target, args.bm25_dir, args.dense_dir,
                               pg_dump=args.pg_dump, pg_restore=args.pg_restore)
    elif args.command == 'verify':
        result = verify_bundle(args.directory)
    elif args.command == 'extract':
        result = extract_zip(args.archive, args.directory)
    elif args.command == 'restore-postgres':
        restore_postgres(args.directory, pg_restore=args.pg_restore)
        result = {'restored': 'postgresql'}
    else:
        result = restore_neo4j(args.directory, batch_size=args.batch_size)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
