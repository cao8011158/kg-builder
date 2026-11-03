"""Build and query a frozen PostgreSQL graph BM25 index."""
import argparse
import json
from kg_builder.bm25_retrieval import BM25Index, TYPES, build


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    make=commands.add_parser('build')
    make.add_argument('--run-id',required=True)
    make.add_argument('--index-dir',required=True)
    search=commands.add_parser('search')
    search.add_argument('--index-dir',required=True)
    search.add_argument('query')
    search.add_argument('--type',choices=TYPES,action='append',dest='types')
    search.add_argument('--limit',type=int,default=10)
    info=commands.add_parser('info')
    info.add_argument('--index-dir',required=True)
    args=parser.parse_args(argv)
    if args.command=='build': result=build(args.run_id,args.index_dir)
    else:
        with BM25Index(args.index_dir) as index:
            result=index.manifest if args.command=='info' else index.search(args.query,types=args.types,top_k=args.limit)
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
