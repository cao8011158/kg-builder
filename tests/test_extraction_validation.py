import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from kg_builder.extraction_validation import validate_extraction,validate_extraction_batch
from kg_builder.storage import connect,upsert_paper,ingest_extractions,ingest_extraction
from import_reviewed import load_batch,main as import_main
from reconcile_sqlite import reconcile


def draft(paper,version,url):
    return {
      'schema_version':'research-kg-extraction/0.2-draft-pilot',
      'paper_id':paper,'version_id':version,'summary':'draft','status':'draft_requires_review',
      'paper_version':{'version_label':'v1','source_url':url,'retrieved_html_sha256':'a'*64},
      'research_problems':[{'key':'problem:p','name':'Question','identity_scope':'candidate_shared_concept','evidence_refs':['s']}],
      'methods':[{'key':'method:m','name':'Method','description':'Procedure','role':'proposed','evidence_refs':['s']}],
      'contributions':[{'key':version+'#c1','identity_scope':'paper_version','description':'Claim',
                        'claim_status':'author_reported','evidence_refs':['s']}],
      'artifacts':[{'key':'artifact:a','name':'System','artifact_kind':'AGENT_SYSTEM',
                    'name_origin':'source_name','identity_scope':'candidate_named_artifact','evidence_refs':['s']}],
      'benchmarks':[{'key':'benchmark:b','name':'Benchmark','protocol':'test','evidence_refs':['s']}],
      'metrics':[{'key':'metric:m','name':'Accuracy','definition':'Fraction','unit':'percent',
                  'higher_is_better':True,'evidence_refs':['s']}],
      'evaluations':[{'key':version+'#e1','target_artifact':'artifact:a',
          'benchmark':'benchmark:b','metric':'metric:m','value':1.0,'unit':'percent',
          'identity_scope':'paper_version_reported_measurement','setup':{},
          'result_status':'source_checked_author_reported_not_reproduced','evidence_refs':['s']}],
      'findings':[{'key':'f1','description':'Observed','claim_status':'author_reported','evidence_refs':['s']}],
      'evidence_locators':[{'evidence_id':'s','source_url':url+'#S1','section':'1',
                            'scope':'primary_paper_v1_html','html_sha256':'a'*64}],
      'relations':[{'source':version,'predicate':'HAS_CONTRIBUTION',
                    'target':version+'#c1','evidence_refs':['s'],'assertion_status':'draft_author_attributed'},
                   {'source':version,'predicate':'REPORTS_EVALUATION',
                    'target':version+'#e1','evidence_refs':['s'],'assertion_status':'draft_author_attributed'},
                   {'source':version+'#e1','predicate':'MEASURES',
                    'target':'metric:m','evidence_refs':['s'],'assertion_status':'draft_author_attributed'}]
    }


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.db=connect(self.root/'one.db'); self.addCleanup(self.db.close)
        self.paper,self.version=upsert_paper(self.db,{'title':'A','arxiv_id':'2501.12345',
            'version_label':'v1','url':'https://arxiv.org/html/2501.12345v1'},'arxiv','A','q')
        self.url='https://arxiv.org/html/2501.12345v1'
        self.draft=draft(self.paper,self.version,self.url)

    def test_valid_and_semantic_failures(self):
        self.assertEqual(validate_extraction(self.draft),self.draft['schema_version'])
        for change,match in (
            (lambda x:x.update(schema_version='unknown'),'Unknown'),
            (lambda x:x['evidence_locators'].append(copy.deepcopy(x['evidence_locators'][0])),'Duplicate evidence'),
            (lambda x:x['contributions'][0].update(evidence_refs=['missing']),'Unresolved evidence'),
            (lambda x:x['relations'][0].update(target='metric:m'),'Wrong relation'),
            (lambda x:x['evaluations'][0].update(metric='benchmark:b'),'wrong or missing metric'),
            (lambda x:x['evaluations'][0].update(unit='count'),'unit conflicts'),
            (lambda x:x['evaluations'][0].update(value=float('nan')),'Nonfinite'),
            (lambda x:x['evaluations'][0].update(value=True),'schema'),
            (lambda x:x['evidence_locators'][0].update(html_sha256='b'*64),'capture hash'),
            (lambda x:x['contributions'][0].update(key='other@v1#c1'),'scoped'),
            (lambda x:x['relations'][2].update(target='metric:other'),'Wrong relation'),
        ):
            invalid=copy.deepcopy(self.draft);change(invalid)
            with self.subTest(match=match),self.assertRaisesRegex(ValueError,match):
                validate_extraction(invalid)
        conflict=copy.deepcopy(self.draft)
        conflict['relations'][2]['target']='metric:m'
        conflict['metrics'].append({'key':'metric:n','name':'Other','definition':'Other','unit':'percent',
                                    'higher_is_better':True,'evidence_refs':['s']})
        conflict['relations'].append({'source':self.version+'#e1','predicate':'MEASURES',
                                     'target':'metric:n','evidence_refs':['s'],
                                     'assertion_status':'draft_author_attributed'})
        # A declaration of two metrics contradicts the single evaluation metric.
        with self.assertRaisesRegex(ValueError,'conflicts'):
            validate_extraction(conflict)

    def test_legacy_and_unknown_version(self):
        legacy={'paper_id':self.paper,'version_id':self.version,'summary':'',
                'methods':[],'research_problems':[],'findings':[],
                'evidence_locators':[{'section':'abstract','source_url':self.url}]}
        validate_extraction(legacy)
        bad=dict(legacy,schema_version=None)
        with self.assertRaisesRegex(ValueError,'Unknown'): validate_extraction(bad)
        for value in ([],{},'future'):
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'Unknown'):
                validate_extraction(dict(legacy,schema_version=value))

    def test_idempotence_existing_id_and_outer_transaction(self):
        payload=copy.deepcopy(self.draft)
        legacy_id='old:preserved'
        self.db.execute('INSERT INTO extraction_records VALUES(?,?,?,?,?,?,?,?,?)',
            (legacy_id,self.paper,self.version,'assistant_manual','2026-10-31-v1',
             'abstract_only','draft_requires_review',json.dumps(payload,indent=2),'historic'))
        self.db.commit()
        self.db.execute('BEGIN')
        ids=ingest_extractions(self.db,[payload])
        self.assertEqual(ids,[legacy_id]);self.assertTrue(self.db.in_transaction)
        self.assertEqual(self.db.execute('SELECT extracted_at,payload_json FROM extraction_records').fetchone(),
                         ('historic',json.dumps(payload,indent=2)))
        self.db.rollback()
        self.db.execute('BEGIN')
        changed=copy.deepcopy(payload);changed['summary']='changed'
        self.assertEqual(ingest_extraction(self.db,changed),legacy_id)
        self.assertTrue(self.db.in_transaction)
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT payload_json FROM extraction_records').fetchone()[0],
                         json.dumps(payload,indent=2))

    def test_invalid_second_and_sql_failure_atomic(self):
        second=copy.deepcopy(self.draft)
        second['extractor_version']='other'
        second['schema_version']='future'
        with self.assertRaisesRegex(ValueError,'item 1'):
            ingest_extractions(self.db,[self.draft,second])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM extraction_records').fetchone()[0],0)
        second['schema_version']=self.draft['schema_version']
        self.db.execute('''CREATE TRIGGER reject_second BEFORE INSERT ON extraction_records
                         WHEN NEW.extractor_version='other' BEGIN SELECT RAISE(ABORT,'blocked'); END''')
        self.db.commit()
        with self.assertRaisesRegex(sqlite3.DatabaseError,'blocked'):
            ingest_extractions(self.db,[self.draft,second])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM extraction_records').fetchone()[0],0)

    def test_cli_validates_before_db_creation(self):
        input_path=self.root/'input.json';db=self.root/'missing.db'
        input_path.write_text(json.dumps([self.draft,dict(self.draft,schema_version='future')]))
        with self.assertRaises(ValueError): import_main(['--input',str(input_path),'--db',str(db)])
        self.assertFalse(db.exists())
        input_path.write_text('[{"value": NaN}]')
        with self.assertRaises(ValueError): load_batch(input_path)
        self.assertFalse(db.exists())

    def test_review_reconcile_and_unknown_table(self):
        self.db.execute('INSERT INTO relevance_reviews VALUES(?,?,?,?,?,?,?,?,?)',
                        ('review:a',self.paper,'include','draft','abstract','["memory"]',
                         'reason','{"key": 1}', 'now'))
        self.db.commit();self.db.close()
        source=self.root/'one.db'
        result=reconcile([source],self.root/'reconciled.db',self.root/'report.json')
        self.assertEqual(result['counts']['relevance_reviews'],1)
        out=sqlite3.connect(self.root/'reconciled.db')
        self.assertEqual(out.execute('SELECT payload_json FROM relevance_reviews').fetchone()[0],'{"key": 1}')
        self.assertEqual(out.execute("SELECT COUNT(*) FROM reconciliation_audit WHERE table_name='relevance_reviews'").fetchone()[0],1)
        self.assertEqual(out.execute('PRAGMA foreign_key_check').fetchall(),[])
        out.close()
        extra=sqlite3.connect(source);extra.execute('CREATE TABLE unknown_notes (x TEXT)');extra.commit()
        with self.assertRaisesRegex(ValueError,'Unsupported input tables'):
            reconcile([source],self.root/'should_not_exist.db',self.root/'should_not_exist.json')
        self.assertFalse((self.root/'should_not_exist.db').exists())
        extra.execute('DROP TABLE unknown_notes')
        extra.execute('ALTER TABLE relevance_reviews ADD COLUMN unknown_annotation TEXT')
        extra.commit();extra.close()
        with self.assertRaisesRegex(ValueError,'Unsupported input columns'):
            reconcile([source],self.root/'should_not_exist.db',self.root/'should_not_exist.json')

    def test_reconcile_remaps_version_scoped_payload_refs(self):
        alternate=self.root/'alternate.db'
        other=connect(alternate)
        old_paper,old_version=upsert_paper(other,{'title':'Same','version_label':'v1','url':self.url},
                                            'source','alternate','q')
        other.execute('UPDATE papers SET arxiv_id=? WHERE paper_id=?',('2501.12345',old_paper))
        other.commit()
        original=draft(old_paper,old_version,self.url)
        ingest_extraction(other,original)
        other.close();self.db.close()
        result=reconcile([self.root/'one.db',alternate],self.root/'combined.db',self.root/'combined.json')
        self.assertEqual(result['counts']['papers'],1)
        output=sqlite3.connect(self.root/'combined.db')
        pid,vid,raw=output.execute('SELECT paper_id,version_id,payload_json FROM extraction_records').fetchone()
        transformed=json.loads(raw)
        self.assertEqual((pid,vid),(self.paper,self.version))
        self.assertEqual(transformed['contributions'][0]['key'],self.version+'#c1')
        self.assertEqual(transformed['relations'][0]['target'],self.version+'#c1')
        self.assertEqual(transformed['evaluations'][0]['key'],self.version+'#e1')
        validate_extraction(transformed)
        self.assertEqual(output.execute("SELECT COUNT(*) FROM reconciliation_audit WHERE table_name='extraction_records'").fetchone()[0],1)
        output.close()

if __name__=='__main__': unittest.main()
