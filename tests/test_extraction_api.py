import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.extraction_api import OptionalLLMExtractor
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION

class FakeResponse:
    def __enter__(self): return self
    def __exit__(self,*args): pass

class ExtractionAPITests(unittest.TestCase):
    def setUp(self):
        self.adapter=OptionalLLMExtractor('https://fake.test','test-model',enabled=True)
        self.pins={'version_label':'v1','source_url':'https://arxiv.org/abs/2501.12345v1'}
        self.draft={'schema_version':RETRIEVAL_SCHEMA_VERSION,'paper_id':'p','version_id':'v',
                    'paper_version':self.pins,'contributions':[],'research_problems':[],'methods':[],'benchmarks':[]}
    def test_pins_and_generic_json(self):
        def load(_): return {'choices':[{'message':{'content':json.dumps(self.draft)}}]}
        with patch.dict(os.environ,{'LLM_API_KEY':'secret'}),patch('kg_builder.extraction_api.urlopen',return_value=FakeResponse()),patch('kg_builder.extraction_api.json.load',load):
            result=self.adapter.extract_draft('p','v','actual text',paper_version=self.pins)
            self.assertEqual(result['paper_version'],self.pins)
            self.assertEqual(self.adapter.generate_json('Return JSON',{'task':'name_group'},required_keys=['paper_id'])['paper_id'],'p')
            self.draft['paper_version']={'version_label':'v2','source_url':self.pins['source_url']}
            with self.assertRaisesRegex(ValueError,'pins'):
                self.adapter.extract_draft('p','v','actual text',paper_version=self.pins)
    def test_requires_pins_and_disabled(self):
        with self.assertRaisesRegex(ValueError,'pins'):
            self.adapter.extract_draft('p','v','text')
        with self.assertRaisesRegex(RuntimeError,'disabled'):
            OptionalLLMExtractor('https://fake.test','model').extract_draft('p','v','text')

if __name__=='__main__': unittest.main()
