import json
from pathlib import Path
import sqlite3
import shutil
import tempfile
import unittest
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from kg_builder.storage import connect, upsert_paper, normalise_doi, arxiv_norm, preferred_metadata_version
from reconcile_sqlite import reconcile
import collect_papers


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir=Path(self.tmp.name)
        self.db=connect(self.dir/'one.db')
        self.addCleanup(self.db.close)

    def test_identifiers_and_source_enrichment(self):
        self.assertEqual(normalise_doi('https://doi.org/10.48550/arXiv.2501.12345'), '10.48550/arxiv.2501.12345')
        self.assertIsNone(normalise_doi('this is not a DOI'))
        self.assertEqual(arxiv_norm('https://arxiv.org/pdf/2501.12345v10.pdf'), '2501.12345')
        self.assertIsNone(arxiv_norm('https://example.com/not-an-arxiv'))
        p,_=upsert_paper(self.db,{'title':'First'},'openalex','W1','q')
        enriched,_=upsert_paper(self.db,{'title':'First','doi':'10.48550/arXiv.2501.12345'},'openalex','W1','q')
        self.assertEqual(enriched,p)
        via_arxiv,_=upsert_paper(self.db,{'title':'First','arxiv_id':'2501.12345','doi':'10.1234/publication'},'s2','S1','q')
        self.assertEqual(via_arxiv,p)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM papers').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM paper_doi_aliases').fetchone()[0],2)
        upsert_paper(self.db,{'title':'First','arxiv_id':'2501.12345','published_at':'2026-03-01'},'x','late','q')
        upsert_paper(self.db,{'title':'First','arxiv_id':'2501.12345','published_at':'2025-01-02'},'x','early','q')
        self.assertEqual(self.db.execute('SELECT first_published,year FROM papers WHERE paper_id=?',(p,)).fetchone(),
                         ('2025-01-02',2025))

    def test_conflict_rollback_and_version_order(self):
        a,_=upsert_paper(self.db,{'title':'A','arxiv_id':'2501.00001'},'arxiv','A','q')
        b,_=upsert_paper(self.db,{'title':'B','doi':'10.1234/b'},'s2','B','q')
        before=self.db.total_changes
        with self.assertRaises(ValueError):
            upsert_paper(self.db,{'title':'Conflict','arxiv_id':'2501.00001','doi':'10.1234/b'},'s3','C','q')
        self.assertEqual(self.db.total_changes,before)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM source_records').fetchone()[0],2)
        for v in ('v2','v10','v1'):
            upsert_paper(self.db,{'title':'A','arxiv_id':'2501.00001','version_label':v},'arxiv','A'+v,'q')
        current=self.db.execute('SELECT current_version_id FROM papers WHERE paper_id=?',(a,)).fetchone()[0]
        self.assertEqual(current,a+'@v10')
        upsert_paper(self.db,{'title':'A','arxiv_id':'2501.00001','published_at':'2025-01-01'},'s4','D','q')
        self.assertEqual(self.db.execute('SELECT current_version_id FROM papers WHERE paper_id=?',(a,)).fetchone()[0],current)
        self.assertIsNone(self.db.execute('SELECT current_version_id FROM papers WHERE paper_id=?',(b,)).fetchone()[0])
        old_snapshot=preferred_metadata_version(self.db,b)[0]
        self.assertTrue(old_snapshot.startswith(b+'@unverified:'))
        self.assertIsNone(arxiv_norm('9999.12345'))
        _,snapshot=upsert_paper(self.db,{'title':'B','doi':'10.1234/b','abstract':'New abstract'},'s2','B','q')
        self.assertNotEqual(snapshot,old_snapshot)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM paper_versions WHERE paper_id=?',(b,)).fetchone()[0],2)

    def test_reconciliation_preserves_extractions_and_is_deterministic(self):
        a=self.dir/'candidate.db'; seed=self.dir/'seed.db'
        for path, source, paper in ((a,'arxiv','2501.11111'),(seed,'s2','2501.11111')):
            db=connect(path)
            pid,vid=upsert_paper(db,{'title':'Same','arxiv_id':paper,'version_label':'v1',
                              'doi':'10.1234/journal' if source=='s2' else '10.48550/arxiv.2501.11111',
                              'source_payload':{'opaque':source}},source,source,'q')
            db.execute('INSERT INTO extraction_records VALUES(?,?,?,?,?,?,?,?,?)',
                       (source,pid,vid,'manual',source,'abstract_only','draft',json.dumps({'paper_id':pid,'version_id':vid}), 'now'))
            db.commit(); db.close()
        result=reconcile([a,seed],self.dir/'out.db',self.dir/'report.json')
        out=sqlite3.connect(self.dir/'out.db')
        self.assertEqual(result['counts']['papers'],1)
        self.assertEqual(result['counts']['source_records'],2)
        self.assertEqual(result['counts']['extraction_records'],2)
        self.assertEqual(result['counts']['paper_doi_aliases'] if 'paper_doi_aliases' in result['counts'] else out.execute('SELECT COUNT(*) FROM paper_doi_aliases').fetchone()[0],2)
        self.assertEqual(out.execute('SELECT COUNT(*) FROM reconciliation_audit WHERE table_name="extraction_records"').fetchone()[0],2)
        for pid,vid,payload in out.execute('SELECT paper_id,version_id,payload_json FROM extraction_records'):
            self.assertEqual((json.loads(payload)['paper_id'],json.loads(payload)['version_id']),(pid,vid))
        self.assertEqual(out.execute('PRAGMA foreign_key_check').fetchall(),[])
        out.close()
        second=reconcile([a,seed],self.dir/'out2.db',self.dir/'report2.json')
        self.assertEqual(result['counts'],second['counts'])
        self.assertEqual(result['paper_mapping'],second['paper_mapping'])
        self.assertEqual(sqlite3.connect(a).execute('SELECT COUNT(*) FROM papers').fetchone()[0],1)
        third=reconcile([self.dir/'out.db'],self.dir/'out3.db',self.dir/'report3.json')
        self.assertGreaterEqual(third['counts']['reconciliation_audit'],result['counts']['reconciliation_audit'])
        repeated=reconcile([self.dir/'out.db',a],self.dir/'repeated.db',self.dir/'repeated.json')
        for table in ('papers','paper_versions','source_records','extraction_records'):
            self.assertEqual(repeated['counts'][table],result['counts'][table])
        original_audit=sqlite3.connect(self.dir/'out.db').execute(
            'SELECT COUNT(*) FROM reconciliation_audit').fetchone()[0]
        self.assertGreaterEqual(repeated['counts']['reconciliation_audit'],original_audit)
        duplicate=self.dir/'candidate_copy.db'
        shutil.copyfile(a,duplicate)
        self.assertEqual(a.read_bytes(),duplicate.read_bytes())
        same=reconcile([a,duplicate],self.dir/'same_bytes.db',self.dir/'same_bytes.json')
        self.assertEqual(same['counts']['papers'],1)
        self.assertEqual(same['counts']['source_records'],1)
        self.assertEqual(same['counts']['reconciliation_audit'],6)
        self.assertEqual(a.read_bytes(),duplicate.read_bytes())
        corrupted=self.dir/'altered_audit.db'
        shutil.copyfile(self.dir/'out.db',corrupted)
        tampered=sqlite3.connect(corrupted)
        candidate_hash=__import__('hashlib').sha256(a.read_bytes()).hexdigest()
        tampered.execute('''UPDATE reconciliation_audit SET original_json=?
            WHERE input_sha256=? AND table_name='papers' AND ordinal=0''',
            ('{"changed":true}',candidate_hash))
        tampered.commit(); tampered.close()
        with self.assertRaisesRegex(ValueError,'Conflicting provenance'):
            reconcile([corrupted,a],self.dir/'reject.db',self.dir/'reject.json')
        self.assertFalse((self.dir/'reject.db').exists())

    def test_output_paths_and_source_history(self):
        path=self.dir/'input.db'
        db=connect(path)
        upsert_paper(db,{'title':'Old','source_payload':{'old':True}},'s','same','q')
        db.close()
        with self.assertRaises(ValueError): reconcile([path],self.dir/'same.db',self.dir/'same.db')
        with self.assertRaises(ValueError): reconcile([path],self.dir/'new.db',path)
        self.assertFalse((self.dir/'same.db').exists())

    def test_historical_collector_cli_is_retired_without_creating_sqlite(self):
        path=self.dir/'collection.db'
        with self.assertRaisesRegex(SystemExit,'prepare_pipeline.py.*KG_DATABASE_URL'):
            collect_papers.main(['--db',str(path)])
        self.assertFalse(path.exists())

    def test_locality_index_plan_and_unrelated_growth(self):
        upsert_paper(self.db,{'title':'Target','doi':'10.1234/target'},'s','target','q')
        sql='SELECT paper_id FROM papers WHERE doi=?'
        plan=self.db.execute('EXPLAIN QUERY PLAN '+sql,('10.1234/target',)).fetchall()
        self.assertTrue(any('idx_paper_doi' in p[3] for p in plan),plan)
        for i in range(200):
            upsert_paper(self.db,{'title':str(i),'doi':f'10.1234/unrelated{i}'},'s',str(i),'q')
        self.assertEqual(self.db.execute(sql,('10.1234/target',)).fetchall(),[('doi:10.1234/target',)])
        grown=self.db.execute('EXPLAIN QUERY PLAN '+sql,('10.1234/target',)).fetchall()
        self.assertTrue(any('idx_paper_doi' in p[3] for p in grown),grown)

if __name__=='__main__': unittest.main()
