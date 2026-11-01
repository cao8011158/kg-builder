"""Full paginated preparation. External extraction workers use claim/submit."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.full_preparation import FullPreparationWorkflow


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--run-dir',type=Path,required=True)
    sub=p.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run')
    run.add_argument('--plan',type=Path)
    run.add_argument('--batch-size',type=int,default=5)
    run.add_argument('--jev-concurrency',type=int,default=8)
    run.add_argument('--generation-concurrency',type=int,default=4)
    run.add_argument('--scope-workers',type=int,default=4)
    run.add_argument('--review-workers',type=int,default=4)
    run.add_argument('--lease-seconds',type=int,default=1800)
    run.add_argument('--repair-limit',type=int,default=2)
    run.add_argument('--item-max-reviews',type=int,default=2)
    run.add_argument('--min-probability',type=float,default=.7)
    run.add_argument('--jev-model',default='jev-1.14.0')
    claim=sub.add_parser('claim');claim.add_argument('--kind',choices=['extract','repair_extraction'],default='extract')
    claim.add_argument('--limit',type=int,default=5);claim.add_argument('--worker',required=True);claim.add_argument('--output',type=Path,required=True)
    submit=sub.add_parser('submit');submit.add_argument('--input',type=Path,required=True)
    sub.add_parser('status')
    snap=sub.add_parser('snapshot');snap.add_argument('--output',type=Path,required=True);snap.add_argument('--cache-dir',type=Path)
    a=p.parse_args(argv)
    kw={}
    if a.command=='run':
        if not os.getenv('TYPESAFE_API_KEY'):p.error('TYPESAFE_API_KEY is required for Jev decisions')
        kw={k:getattr(a,k) for k in ('batch_size','jev_concurrency','generation_concurrency','scope_workers',
              'review_workers','lease_seconds','repair_limit','item_max_reviews','jev_model')}
        kw['min_confidence']=a.min_probability
        if a.plan:kw['plan']=json.loads(a.plan.read_text())
    wf=FullPreparationWorkflow(a.run_dir,**kw)
    if a.command=='run':result=asyncio.run(wf.run())
    elif a.command=='claim':
        result=wf.claim(a.kind,a.limit,a.worker)
        a.output.parent.mkdir(parents=True,exist_ok=True)
        a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    elif a.command=='submit':result=wf.submit(json.loads(a.input.read_text()))
    elif a.command=='snapshot':
        if a.cache_dir:
            from kg_builder.embeddings import EmbeddingModel
            wf.embedding=EmbeddingModel(cache_dir=a.cache_dir)
        result=wf.snapshot(a.output)
        result=dict(snapshot_hash=result['snapshot_hash'],counts=result['counts'],output=str(a.output))
    else:result=wf.status()
    print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
