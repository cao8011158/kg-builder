#!/usr/bin/env python3
"""Audit a frozen snapshot graph without changing its job databases."""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.graph_validation import validate_graph


def _check_output_path(output, snapshot, job_dir, cases):
    protected=[snapshot, *(job_dir/name for name in ('graph.sqlite','semantic.sqlite','snapshot-master.sqlite'))]
    if cases is not None:protected.append(cases)
    target=output.resolve()
    for source in protected:
        if target==source.resolve() or (output.exists() and source.exists() and os.path.samefile(output,source)):
            raise ValueError(f'Output path collides with protected input: {source}')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--job-dir',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--cases',type=Path)
    parser.add_argument('--cache-dir',type=Path)
    args=parser.parse_args(argv)
    _check_output_path(args.output,args.snapshot,args.job_dir,args.cases)
    cases=json.loads(args.cases.read_text(encoding='utf-8')) if args.cases else None
    model=None
    if args.cache_dir and cases and any(q.get('expected_paper_ids') for q in cases.get('queries',[])):
        from kg_builder.embeddings import EmbeddingModel
        model=EmbeddingModel(cache_dir=args.cache_dir)
    result=validate_graph(args.snapshot,args.job_dir,cases,model)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(args.output),'structural':result['structural']['passed'],'quality':result['quality']['status']}))
    return 0 if result['structural']['passed'] else 1

if __name__=='__main__':sys.exit(main())
