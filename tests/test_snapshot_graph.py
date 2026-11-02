import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION
from kg_builder.semantic_store import SemanticStore


def fixture(path, n=2):
    papers=[];extractions=[];vectors=[]
    for n in range(1,n+1):
        pid=f'paper:{n}';vid=f'{pid}@v1';url=f'https://example.org/{n}'
        papers.append({'paper_id':pid,'title':f'Title {n}','version_id':vid,
                       'paper_version':{'version_label':'v1','source_url':url},'text':f'Text {n}', 'source_hash':None})
        papers[-1]['source_hash']=canonical_hash({**{k:papers[-1][k] for k in ('paper_id','version_id','title','text')},'version_label':'v1','source_url':url})
        items={
            'research_problems':[{'key':'problem_1','name':f'Memory problem {n}','description':'Recall over time'}],
            'methods':[{'key':'method_1','name':f'Method {n}','description':'Persistent memory','role':'proposed'}],
            'benchmarks':[{'key':'benchmark_1','name':f'Benchmark {n}','description':'Memory task','roles':['used'],'version_or_subset':None}],
            'contributions':[{'key':f'{vid}#c','description':'A contribution'}]}
        extractions.append({'schema_version':RETRIEVAL_SCHEMA_VERSION,'paper_id':pid,'version_id':vid,
                            'paper_version':papers[-1]['paper_version'],'extractor_type':'model','extractor_version':'v1',
                            'source_scope':'abstract_only','status':'reviewed',**items})
        for kind,field in [('ResearchProblem','research_problems'),('Method','methods'),('Benchmark','benchmarks'),('Contribution','contributions')]:
            item=items[field][0]
            vectors.append({'item_key':pid+'::'+item['key'],'kind':kind,'text':item['name']+'\n'+item['description'] if kind!='Contribution' else item['description'],
                            'model_id':'test-embed','vector':[1.0,0.0] if kind=='ResearchProblem' else ([0.0,1.0] if kind=='Method' else [0.70710678,0.70710678])})
    value={'schema_version':'research-kg-prepared-snapshot/1','run_id':'test','papers':papers,
           'extractions':extractions,'vectors':vectors,'counts':{'accepted':n}}
    value['snapshot_hash']=canonical_hash(value)
    path.write_text(json.dumps(value),encoding='utf-8')
    return value


def pairs(items,client,**kwargs):
    return [{'pair_id':p['pair_id'],'decision':'different' if p['kind']=='Benchmark' else 'same','raw':{'test':True}} for p in items]


def groups(items,client,**kwargs):
    return [{'group_id':g['group_id'],'passed':True,'issues':[],'raw':{'test':True}} for g in items]


def member_result(item,choice='belongs',probability=0.2,confidence=0.9):
    return {'membership_id':item['membership_id'],'choice':choice,'probability':probability,
            'confidence':confidence,'decision_metric':'probability',
            'excluded':choice=='not_belongs' and probability>0.7,
            'raw':{'answers':{'membership':{'choice':choice,'confidence':confidence,
                    'probabilities':{'belongs':1-probability,'not_belongs':probability,'insufficient':0}}}}}


class FakeEmbedding:
    model_id='test-embed'
    def encode(self,texts): return [[1.0,0.0] for _ in texts]
