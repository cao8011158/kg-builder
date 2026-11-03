"""Prepare frozen Paper vectors or run dense-only Paper search."""
import argparse
import json

from .embeddings import create_embedding_model
from .paper_dense import PaperDenseIndex, build


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--index-dir',required=True)
    parser.add_argument('--voyage-concurrency',type=int,default=2)
    parser.add_argument('--voyage-rpm',type=int,default=2000)
    parser.add_argument('--voyage-tpm',type=int,default=3000000)
    commands=parser.add_subparsers(dest='command',required=True)
    prepare=commands.add_parser('prepare')
    prepare.add_argument('--batch-size',type=int,default=64)
    search=commands.add_parser('search')
    search.add_argument('query')
    search.add_argument('--top-k',type=int,default=10)
    args=parser.parse_args(argv)
    model=create_embedding_model(provider='voyage',dimension=1024,
        max_concurrency=args.voyage_concurrency,requests_per_minute=args.voyage_rpm,
        tokens_per_minute=args.voyage_tpm)
    if args.command=='prepare':
        result=build(args.run_id,args.index_dir,model,batch_size=args.batch_size)
    else:
        with PaperDenseIndex(args.run_id,args.index_dir,model) as index:
            result=index.search(args.query,top_k=args.top_k)
    print(json.dumps({'result':result,'usage':model.usage},ensure_ascii=False,indent=2))


if __name__=='__main__':main()
