"""Command line entry point for the reviewed semantic sidecar."""
import argparse
import json
from pathlib import Path
from .semantic_store import SemanticStore


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sidecar',required=True)
    parser.add_argument('--model','--embedding-model',dest='embedding_model',help='Embedding model (provider default when omitted)')
    parser.add_argument('--embedding-provider',choices=['fastembed','voyage'],default='fastembed')
    parser.add_argument('--embedding-dimension',type=int,default=1024)
    parser.add_argument('--cache-dir',help='Local model cache directory')
    parser.add_argument('--voyage-concurrency',type=int,default=8)
    parser.add_argument('--voyage-rpm',type=int,default=2000)
    parser.add_argument('--voyage-tpm',type=int,default=3000000)
    sub=parser.add_subparsers(dest='command',required=True)
    ingest=sub.add_parser('ingest');ingest.add_argument('--master',required=True)
    ingest.add_argument('--extraction-id',action='append',required=True)
    cluster=sub.add_parser('cluster-problems');cluster.add_argument('--output',required=True)
    cluster.add_argument('--k',type=int,default=20)
    cluster.add_argument('--min-similarity',type=float,default=.65)
    cluster.add_argument('--resolution',type=float,default=.05)
    cluster.add_argument('--seed',type=int,default=42)
    export=sub.add_parser('export-candidates');export.add_argument('--output',required=True)
    export.add_argument('--kind',choices=['ResearchProblem','Method','Benchmark'])
    export.add_argument('--limit',type=int,default=100);export.add_argument('--offset',type=int,default=0)
    export.add_argument('--k',type=int,default=8)
    review=sub.add_parser('import-review');review.add_argument('--input',required=True)
    search=sub.add_parser('search');search.add_argument('query');search.add_argument('--limit',type=int,default=10)
    search.add_argument('--offset',type=int,default=0)
    search.add_argument('--kind',choices=['ResearchProblem','Method','Benchmark','Contribution'])
    entity=sub.add_parser('entity');entity.add_argument('entity_id');entity.add_argument('--limit',type=int,default=20)
    entity.add_argument('--offset',type=int,default=0)
    paper=sub.add_parser('paper');paper.add_argument('paper_id');paper.add_argument('--limit',type=int,default=20)
    paper.add_argument('--offset',type=int,default=0)
    args=parser.parse_args(argv)
    model=None
    if args.command in ('cluster-problems','export-candidates','search'):
        from .embeddings import create_embedding_model
        model=create_embedding_model(provider=args.embedding_provider,model_name=args.embedding_model,
            dimension=args.embedding_dimension,cache_dir=args.cache_dir,max_concurrency=args.voyage_concurrency,
            requests_per_minute=args.voyage_rpm,tokens_per_minute=args.voyage_tpm)
    store=None
    try:
        store=SemanticStore(args.sidecar,model=model)
        if args.command=='ingest': result=store.ingest(args.master,args.extraction_id)
        elif args.command=='cluster-problems':result=store.cluster_problems(
            k=args.k,min_similarity=args.min_similarity,resolution=args.resolution,seed=args.seed)
        elif args.command=='export-candidates':result=store.export_candidates(args.kind,args.limit,args.offset,args.k)
        elif args.command=='import-review':result=store.import_review(json.loads(Path(args.input).read_text()))
        elif args.command=='search':result=store.search(args.query,args.limit,args.offset,args.kind)
        elif args.command=='entity':result=store.get_entity(args.entity_id,args.limit,args.offset)
        else:result=store.get_paper(args.paper_id,args.limit,args.offset)
        output=json.dumps(result,ensure_ascii=False,indent=2)
        if args.command in ('cluster-problems','export-candidates'):
            Path(args.output).write_text(output+'\n',encoding='utf-8')
            print(args.output)
        else:print(output)
    finally:
        try:
            if store is not None:store.close()
        finally:
            if model is not None and hasattr(model,'close'):model.close()

if __name__=='__main__':main()
