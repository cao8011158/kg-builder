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
    sub=parser.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run');run.add_argument('--jev-model',default='jev-1.14.0');run.add_argument('--batch-size',type=int,default=32)
    run.add_argument('--jev-concurrency',type=int,default=4)
    run.add_argument('--name-concurrency',type=int,default=3)
    claim=sub.add_parser('claim');claim.add_argument('--worker',required=True);claim.add_argument('--limit',type=int,default=5);claim.add_argument('--output',required=True,type=Path)
    submit=sub.add_parser('submit');submit.add_argument('--input',required=True,type=Path)
    sub.add_parser('status')
    args=parser.parse_args(argv)
    client=None
    if args.command=='run':
        from kg_builder.decision_api import JevClient
        client=JevClient(model=args.jev_model,max_concurrency=args.jev_concurrency)
    model=None
    if args.embedding_provider or args.embedding_model:
        from kg_builder.embeddings import create_embedding_model
        model=create_embedding_model(provider=args.embedding_provider or 'fastembed',model_name=args.embedding_model,
            dimension=args.embedding_dimension,cache_dir=args.embedding_cache_dir)
    k=args.k
    name_mode=args.name_mode
    if args.command!='run' and (args.job_dir/'graph.sqlite').exists():
        import sqlite3
        with sqlite3.connect(args.job_dir/'graph.sqlite') as db:
            settings=dict(db.execute("SELECT key,value FROM meta WHERE key IN ('k','name_mode')"))
        k=k if k is not None else int(settings['k'])
        name_mode=name_mode or settings['name_mode']
    job=SnapshotGraph(args.snapshot,args.job_dir,client=client,model=model,k=k or 20,name_mode=name_mode or 'external',batch_size=getattr(args,'batch_size',32),name_concurrency=getattr(args,'name_concurrency',3),name_base_url=args.name_base_url,name_model=args.name_model,read_input=args.command=='run',embedding_cache_dir=args.embedding_cache_dir,graph_policy=args.graph_policy,problem_resolution=args.problem_resolution,entity_same_min_probability=args.entity_same_min_probability)
    try:
        if args.command=='run':result=job.run()
        elif args.command=='claim':
            result=job.claim(args.worker,args.limit)
            args.output.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
        elif args.command=='submit':result=job.submit(json.loads(args.input.read_text(encoding='utf-8')))
        else:result=job.status()
        print(json.dumps(result,indent=2,ensure_ascii=False))
    finally:job.close()

if __name__=='__main__':main()
