import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction
from kg_builder.extraction_api import OptionalLLMExtractor
from kg_builder.storage import connect, upsert_paper, ingest_extractions
from reconcile_sqlite import reconcile


def payload(paper, version, url, label):
    return {'schema_version':RETRIEVAL_SCHEMA_VERSION,'paper_id':paper,'version_id':version,
            'paper_version':{'version_label':label,'source_url':url},
            'extractor_type':'assistant_manual','extractor_version':'2026-10-31-v1',
            'source_scope':'abstract_only','status':'draft_requires_review',
            'contributions':[{'key':version+'#c1','description':'Authors claim a memory system',
                              'section':'abstract'}],
            'research_problems':[{'key':'problem:forgetting','name':'Forgetting',
                                  'description':'Agents lose context'}],
            'methods':[{'key':'method:retrieval','name':'Retrieval',
                        'description':'Retrieve relevant memory','role':'proposed'}],
            'benchmarks':[{'key':'benchmark:one','name':'One','description':'Task suite',
                           'roles':['introduced','used'],'version_or_subset':None}]}


class RetrievalTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.db=connect(self.root/'original.db');self.addCleanup(self.db.close)
        self.url='https://arxiv.org/abs/2501.12345v1'
        self.paper,self.version=upsert_paper(self.db,{'title':'Example','arxiv_id':'2501.12345',
            'version_label':'v1','url':self.url},'arxiv','2501.12345','q')
        self.data=payload(self.paper,self.version,self.url,'v1')

    def test_minimal_contract_and_rejections(self):
        self.assertEqual(validate_extraction(self.data),RETRIEVAL_SCHEMA_VERSION)
        for change,pattern in (
            (lambda x:x.update(summary='old'),'schema'),
            (lambda x:x['methods'][0].update(evidence_refs=[]),'schema'),
            (lambda x:x['research_problems'][0].pop('description'),'schema'),
            (lambda x:x['methods'][0].update(role='invented'),'schema'),
            (lambda x:x['benchmarks'][0].update(roles=['used','used']),'schema'),
            (lambda x:x['benchmarks'][0].update(roles=[]),'schema'),
            (lambda x:x['contributions'][0].update(key='another@v1#c1'),'scoped'),
            (lambda x:x['methods'][0].update(key='problem:forgetting'),'Duplicate'),
            (lambda x:x['paper_version'].update(extra='no'),'schema'),
            (lambda x:x['benchmarks'][0].update(extra=float('nan')),'Nonfinite'),
        ):
            invalid=copy.deepcopy(self.data);change(invalid)
            with self.subTest(pattern=pattern),self.assertRaisesRegex(ValueError,pattern):
                validate_extraction(invalid)
        empty=copy.deepcopy(self.data)
        for field in ('contributions','research_problems','methods','benchmarks'):
            empty[field]=[]
        validate_extraction(empty)

    def test_pinned_versions_and_atomic_mixed_batch(self):
        self.assertEqual(len(ingest_extractions(self.db,[self.data])),1)
        self.assertEqual(len(ingest_extractions(self.db,[self.data])),1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM extraction_records').fetchone()[0],1)
        snapshot_url='https://example.org/snapshot'
        p,v=upsert_paper(self.db,{'title':'Snapshot','url':snapshot_url},'source','snapshot','q')
        snapshot=payload(p,v,snapshot_url,None)
        snapshot['extractor_version']='snapshot-v1'
        self.assertEqual(len(ingest_extractions(self.db,[snapshot])),1)
        mismatched=copy.deepcopy(snapshot);mismatched['paper_version']['version_label']='v1'
        with self.assertRaisesRegex(ValueError,'Pinned version'):
            ingest_extractions(self.db,[mismatched])
        second=copy.deepcopy(self.data);second['extractor_version']='different'
        bad=copy.deepcopy(snapshot);bad['benchmarks'][0]['roles']=['unknown']
        with self.assertRaisesRegex(ValueError,'item 1'):
            ingest_extractions(self.db,[second,bad])
        self.assertEqual(self.db.execute('SELECT count(*) FROM extraction_records').fetchone()[0],2)
        bad=copy.deepcopy(snapshot);bad['extractor_version']='different'
        self.db.execute("CREATE TRIGGER reject_snapshot BEFORE INSERT ON extraction_records WHEN NEW.extractor_version='different' BEGIN SELECT RAISE(ABORT,'blocked'); END")
        self.db.commit()
        with self.assertRaisesRegex(sqlite3.DatabaseError,'blocked'):
            ingest_extractions(self.db,[second,bad])
        self.assertEqual(self.db.execute('SELECT count(*) FROM extraction_records').fetchone()[0],2)

    def test_same_paper_versions_remain_independent(self):
        first=ingest_extractions(self.db,[self.data])[0]
        url_v2='https://arxiv.org/abs/2501.12345v2'
        paper,version_v2=upsert_paper(self.db,{'title':'Example','arxiv_id':'2501.12345',
            'version_label':'v2','url':url_v2},'arxiv','2501.12345v2','q')
        self.assertEqual(paper,self.paper)
        second_data=payload(paper,version_v2,url_v2,'v2')
        second=ingest_extractions(self.db,[second_data])[0]
        self.assertNotEqual(first,second)
        rows=self.db.execute('SELECT version_id,payload_json FROM extraction_records ORDER BY version_id').fetchall()
        self.assertEqual(len(rows),2)
        for version,raw in rows:
            self.assertEqual(json.loads(raw)['contributions'][0]['key'],version+'#c1')

    def test_reconcile_remaps_local_contribution_and_retains_prose(self):
        other=connect(self.root/'other.db')
        old_p,old_v=upsert_paper(other,{'title':'Example','url':self.url,'version_label':'v1'},
                                  'source','alternate','q')
        other.execute('UPDATE papers SET arxiv_id=? WHERE paper_id=?',('2501.12345',old_p))
        other.commit()
        data=payload(old_p,old_v,self.url,'v1')
        data['contributions'][0]['description']=old_v+' is literal prose'
        ingest_extractions(other,[data]);other.close();self.db.close()
        reconcile([self.root/'original.db',self.root/'other.db'],
                  self.root/'merged.db',self.root/'report.json')
        con=sqlite3.connect(self.root/'merged.db')
        stored=json.loads(con.execute('SELECT payload_json FROM extraction_records').fetchone()[0])
        con.close()
        self.assertEqual(stored['version_id'],self.version)
        self.assertEqual(stored['contributions'][0]['key'],self.version+'#c1')
        self.assertEqual(stored['contributions'][0]['description'],old_v+' is literal prose')
        validate_extraction(stored)

    def test_optional_api_disabled(self):
        with self.assertRaisesRegex(RuntimeError,'disabled'):
            OptionalLLMExtractor('https://example.org','model').extract_draft(self.paper,self.version,'text')


if __name__=='__main__': unittest.main()
