"""Streaming preparation CLI. Run and external workers operate concurrently."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.preparation import PreparationWorkflow

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--run-dir',type=Path,required=True)
    p.add_argument('--run-id',required=True,help='Stable logical PostgreSQL run identifier')
    sub=p.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run')
    run.add_argument('--target',type=int,default=60);run.add_argument('--llm-mode',choices=['external','api'],default='external')
    run.add_argument('--jev-model',default='jev-1.14.0');run.add_argument('--batch-size',type=int,default=5)
    run.add_argument('--per-query',type=int,default=100);run.add_argument('--since',default='2025-01-01')
    run.add_argument('--until');run.add_argument('--repair-limit',type=int,default=2)
    run.add_argument('--review-policy',choices=['item-v1','legacy-paper'],default='item-v1')
    run.add_argument('--item-max-reviews',type=int,default=2)
    run.add_argument('--min-probability',type=float,default=.7,help='Minimum probability for new decisions (legacy confidence runs retain saved threshold)')
    run.add_argument('--jev-concurrency',type=int,default=4)
    run.add_argument('--generation-concurrency',type=int,default=3)
    run.add_argument('--scope-workers',type=int,default=2)
    run.add_argument('--review-workers',type=int,default=2)
    run.add_argument('--lease-seconds',type=int,default=1800)
    run.add_argument('--llm-base-url');run.add_argument('--llm-model')
    claim=sub.add_parser('claim');claim.add_argument('--kind',choices=['extract','repair_extraction'],default='extract')
    claim.add_argument('--limit',type=int,default=5);claim.add_argument('--worker',required=True);claim.add_argument('--output',type=Path,required=True)
    submit=sub.add_parser('submit');submit.add_argument('--input',type=Path,required=True)
    sub.add_parser('status')
    snap=sub.add_parser('snapshot');snap.add_argument('--output',type=Path,required=True);snap.add_argument('--cache-dir',type=Path)
    a=p.parse_args(argv)
    kw={k:getattr(a,k) for k in ('target','llm_mode','jev_model','batch_size','per_query','since','until','repair_limit','lease_seconds','review_policy','item_max_reviews','jev_concurrency','generation_concurrency','scope_workers','review_workers')} if a.command=='run' else {}
    if a.command=='run':kw['min_confidence']=a.min_probability
    if a.command=='run' and a.llm_mode=='api':
        if not a.llm_base_url or not a.llm_model:p.error('API mode needs --llm-base-url and --llm-model')
        from kg_builder.extraction_api import OptionalLLMExtractor
        kw['extractor']=OptionalLLMExtractor(a.llm_base_url,a.llm_model,enabled=True)
    if a.command=='run' and not os.getenv('TYPESAFE_API_KEY'):
        p.error('TYPESAFE_API_KEY is required for Jev scope and field review')
    if a.command=='run' and a.llm_mode=='api' and not os.getenv('LLM_API_KEY'):
        p.error('LLM_API_KEY is required for API extraction')
    wf=PreparationWorkflow(a.run_dir,run_id=a.run_id,**kw)
    if a.command=='run': result=asyncio.run(wf.run())
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
    else:result=wf.status()
    print(json.dumps(result if a.command!='snapshot' else dict(snapshot_hash=result['snapshot_hash'],counts=result['counts'],output=str(a.output)),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
