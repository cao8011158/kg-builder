#!/usr/bin/env python3
"""Build a separate graph job from an immutable preparation snapshot."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1] / 'src'))
from kg_builder.snapshot_graph import SnapshotGraph


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',required=True,type=Path)
    parser.add_argument('--job-dir',required=True,type=Path)
    parser.add_argument('--run-id',help='Explicit PostgreSQL graph run identity; otherwise reuse/create graph-job.json')
    parser.add_argument('--k',type=int)
    parser.add_argument('--name-mode',choices=['external','api'])
    parser.add_argument('--graph-policy',choices=['legacy','domain-v1'])
    parser.add_argument('--problem-resolution',type=float)
    parser.add_argument('--entity-same-min-probability',type=float)
    parser.add_argument('--name-base-url')
    parser.add_argument('--name-model')
    parser.add_argument('--embedding-cache-dir',type=Path)
    parser.add_argument('--embedding-provider',choices=['fastembed','voyage'])
    parser.add_argument('--embedding-model')
    parser.add_argument('--embedding-dimension',type=int,default=1024)
    parser.add_argument('--voyage-concurrency',type=int,default=8)
    parser.add_argument('--voyage-rpm',type=int,default=2000)
    parser.add_argument('--voyage-tpm',type=int,default=3000000)
    parser.add_argument('--neo4j',action='store_true',help='Publish approved graph events using NEO4J_* environment variables')
    sub=parser.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run');run.add_argument('--jev-model',default='jev-1.14.0');run.add_argument('--batch-size',type=int,default=32)
    run.add_argument('--jev-concurrency',type=int,default=8,help='Shared Jev HTTP concurrency limit (default: 8); also bounds in-flight pair batches')
    run.add_argument('--name-concurrency',type=int,default=3)
    claim=sub.add_parser('claim');claim.add_argument('--worker',required=True);claim.add_argument('--limit',type=int,default=5);claim.add_argument('--output',required=True,type=Path)
    submit=sub.add_parser('submit');submit.add_argument('--input',required=True,type=Path)
    submit.add_argument('--jev-model',default='jev-1.14.0')
    submit.add_argument('--jev-concurrency',type=int,default=8,help='Shared Jev HTTP concurrency limit (default: 8); also bounds in-flight pair batches')
    sub.add_parser('status')
    args=parser.parse_args(argv)
    client=model=publisher=job=None
    voyage_options=dict(max_concurrency=args.voyage_concurrency,
        requests_per_minute=args.voyage_rpm,tokens_per_minute=args.voyage_tpm)
    try:
        if args.neo4j and args.command in ('run','submit'):
            from kg_builder.neo4j_store import Neo4jStore
            publisher=Neo4jStore.from_env()
        if args.command=='run' or (args.command=='submit' and args.neo4j):
            from kg_builder.decision_api import JevClient
            client=JevClient(model=args.jev_model,max_concurrency=args.jev_concurrency)
        if args.command in ('run','submit') and (args.embedding_provider or args.embedding_model):
            from kg_builder.embeddings import create_embedding_model
            model=create_embedding_model(provider=args.embedding_provider or 'fastembed',model_name=args.embedding_model,
                dimension=args.embedding_dimension,cache_dir=args.embedding_cache_dir,**voyage_options)
        job=SnapshotGraph(args.snapshot,args.job_dir,client=client,model=model,publisher=publisher,
            voyage_options=voyage_options,k=args.k,name_mode=args.name_mode,run_id=args.run_id,
            batch_size=getattr(args,'batch_size',32),name_concurrency=getattr(args,'name_concurrency',3),
            name_base_url=args.name_base_url,name_model=args.name_model,read_input=args.command=='run',
            embedding_cache_dir=args.embedding_cache_dir,graph_policy=args.graph_policy,
            problem_resolution=args.problem_resolution,entity_same_min_probability=args.entity_same_min_probability)
        if args.command=='run':result=job.run()
        elif args.command=='claim':
            result=job.claim(args.worker,args.limit)
            args.output.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
        elif args.command=='submit':result=job.submit(json.loads(args.input.read_text(encoding='utf-8')))
        else:result=job.status()
        print(json.dumps(result,indent=2,ensure_ascii=False))
    finally:
        try:
            if job is not None:
                model=job.model
                job.close()
        finally:
            try:
                if model is not None and hasattr(model,'close'):model.close()
            finally:
                try:
                    if client is not None and hasattr(client,'close'):client.close()
                finally:
                    if publisher is not None:publisher.close()

if __name__=='__main__':main()
