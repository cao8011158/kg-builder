#!/usr/bin/env python3
"""Run one graph controller, or exchange durable external Agent name results."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.graph_external import ExternalGraphQueue, HelperGraph, SummaryMonitor, preflight_summary, claim_to_file, run_controller
from kg_builder.snapshot_graph import SnapshotGraph


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--job-dir',type=Path,required=True)
    parser.add_argument('--run-id')
    sub=parser.add_subparsers(dest='command',required=True)
    controller=sub.add_parser('controller')
    controller.add_argument('--neo4j',action='store_true')
    controller.add_argument('--embedding-provider',choices=['fastembed','voyage'])
    controller.add_argument('--embedding-model')
    controller.add_argument('--embedding-dimension',type=int,default=1024)
    controller.add_argument('--embedding-cache-dir',type=Path)
    controller.add_argument('--k',type=int,default=20)
    controller.add_argument('--batch-size',type=int,default=32)
    controller.add_argument('--problem-resolution',type=float,default=.05)
    controller.add_argument('--entity-same-min-probability',type=float,default=.6)
    controller.add_argument('--summary',type=Path,required=True)
    claim=sub.add_parser('claim')
    claim.add_argument('--worker',required=True)
    claim.add_argument('--output',type=Path,required=True)
    claim.add_argument('--wait-seconds',type=float,default=55)
    enqueue=sub.add_parser('enqueue')
    enqueue.add_argument('--input',type=Path,required=True)
    sub.add_parser('status')
    args=parser.parse_args(argv)
    job=queue=client=model=publisher=monitor=None
    try:
        # Acquire the durable owner lock before constructing any remote client
        # or loading the single full snapshot in the controller process.
        job=HelperGraph(args.snapshot,args.job_dir,run_id=args.run_id,bootstrap=args.command=='controller')
        queue=ExternalGraphQueue(job,initialize=args.command=='controller')
        if args.command=='controller':
            queue.acquire_owner()
            output=preflight_summary(args.snapshot,args.job_dir,args.summary)
            def metrics():
                active_model=model or getattr(job,'model',None)
                return {'jev':client.concurrency_stats if client else None,
                        'voyage':getattr(active_model,'usage',None)}
            monitor=SummaryMonitor(args.snapshot,args.job_dir,output,run_id=job.run_id,metrics=metrics)
            monitor.start()
            from kg_builder.decision_api import JevClient
            client=JevClient(max_concurrency=8)
            if args.embedding_provider or args.embedding_model:
                from kg_builder.embeddings import create_embedding_model
                model=create_embedding_model(provider=args.embedding_provider or 'fastembed',model_name=args.embedding_model,
                    dimension=args.embedding_dimension,cache_dir=args.embedding_cache_dir,max_concurrency=8,
                    requests_per_minute=2000,tokens_per_minute=3000000)
            if args.neo4j:
                from kg_builder.neo4j_store import Neo4jStore
                publisher=Neo4jStore.from_env()
            job.close()
            job=SnapshotGraph(args.snapshot,args.job_dir,run_id=args.run_id,
                client=client,model=model,publisher=publisher,k=args.k,batch_size=args.batch_size,
                name_mode='external',graph_policy='domain-v1',lease_seconds=1800,
                problem_resolution=args.problem_resolution,entity_same_min_probability=args.entity_same_min_probability,
                context_format='explicit-v1')
            queue.job=job
            monitor.state('running')
            result=run_controller(queue)
            monitor.state('complete')
        elif args.command=='claim':result=claim_to_file(queue,args.worker,args.output,wait_seconds=args.wait_seconds)
        elif args.command=='enqueue':result=queue.enqueue(json.loads(args.input.read_text(encoding='utf-8')))
        else:result=queue.summary()
        print(json.dumps(result,ensure_ascii=False,indent=2))
    except BaseException as exc:
        if monitor is not None:monitor.state('failed',failure_type=type(exc).__name__)
        raise
    finally:
        if monitor is not None:monitor.close()
        if queue is not None:queue.close()
        if job is not None:
            model=job.model or model
            job.close()
        for resource in (model,client,publisher):
            if resource is not None and hasattr(resource,'close'):resource.close()


if __name__=='__main__':
    try:main()
    except Exception as exc:
        # Driver/database exception strings can contain connection details.
        print(json.dumps({'error_type':type(exc).__name__,'error':'External graph command failed; inspect credential-free job state'}),file=sys.stderr)
        raise SystemExit(1)
