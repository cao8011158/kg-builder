"""Optional OpenAI-compatible extraction adapter. NOT EXECUTED, NOT TESTED.

The API response is only a *draft* and requires schema/evidence review.
"""
from __future__ import annotations
import json
import os
from .collectors import fetch_bytes
from urllib.parse import urljoin
from urllib.request import Request, urlopen

EXTRACTION_INSTRUCTIONS = '''Return a JSON object with paper_id, version_id,
summary, methods (list), research_problems (list), findings (list),
evaluations (list), artifacts (list), evidence_locators (list).
For every assertion, include exact source locator and distinguish author claims
from verified facts. If the abstract does not specify a metric or benchmark,
leave the field empty. Do not invent. This output must be reviewed before import.'''

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
        draft['status']='draft_requires_review'
        draft['extractor_type']='llm_api_unreviewed'
        return draft
