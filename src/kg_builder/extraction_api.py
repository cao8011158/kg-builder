"""Optional OpenAI-compatible extraction adapter; disabled by default."""
from __future__ import annotations
import json
import os
from .extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction
from urllib.request import Request, urlopen

EXTRACTION_INSTRUCTIONS = '''Return only a JSON object for research-kg-extraction/0.3-retrieval.
Include schema_version, paper_id, version_id, paper_version with version_label
(vN or null for an unverified snapshot) and source_url, and four arrays:
contributions [{key,description}], research_problems [{key,name,description}],
methods [{key,name,description,role}], benchmarks
[{key,name,description,roles,version_or_subset}]. A contribution is an
author-claimed contribution, and its key starts with version_id followed by #.
Method role is proposed, used, or involved. Benchmark roles are introduced
and/or used; version_or_subset is a string or null. Include optional section
and source_url on individual entries where known. Use empty arrays for absent
information, do not invent details, and do not include other fields.'''

class OptionalLLMExtractor:
    def __init__(self, base_url, model, enabled=False):
        self.base_url=base_url.rstrip('/')
        self.model=model
        self.enabled=enabled

    def extract_draft(self, paper_id, version_id, text):
        if not self.enabled: raise RuntimeError('LLM extraction is disabled by default')
        key=os.getenv('LLM_API_KEY')
        if not key: raise RuntimeError('LLM_API_KEY not set')
        payload=json.dumps({'model':self.model,'messages':[
          {'role':'system','content':EXTRACTION_INSTRUCTIONS},
          {'role':'user','content':json.dumps({'paper_id':paper_id,'version_id':version_id,'paper_text':text})}]
        }).encode('utf-8')
        req=Request(self.base_url+'/chat/completions',data=payload,method='POST',headers={
          'Authorization':'Bearer '+key,'Content-Type':'application/json'})
        with urlopen(req,timeout=120) as response: body=json.load(response)
        draft=json.loads(body['choices'][0]['message']['content'])
        if not isinstance(draft,dict):
            raise ValueError('LLM extraction must be a JSON object')
        if draft.get('schema_version') != RETRIEVAL_SCHEMA_VERSION:
            raise ValueError('LLM extraction schema_version differs from requested contract')
        if draft.get('paper_id') != paper_id or draft.get('version_id') != version_id:
            raise ValueError('LLM extraction paper/version differs from supplied IDs')
        draft['status']='draft_requires_review'
        draft['extractor_type']='llm_api_unreviewed'
        draft['extractor_version']=self.model
        draft['source_scope']='supplied_paper_text'
        validate_extraction(draft)
        return draft
