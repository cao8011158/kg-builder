import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kg_builder.graph_validation import validate_graph
from scripts.validate_snapshot_graph import main as validate_cli
from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash
from test_snapshot_graph import FakeEmbedding, fixture, groups, pairs


class GraphValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.snapshot=self.root/'snapshot.json';fixture(self.snapshot)
        self.job_dir=self.root/'job'
        with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',groups):
            job=SnapshotGraph(self.snapshot,self.job_dir,client=object(),k=1,model=FakeEmbedding())
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Long term memory','description':'Memory over time'}}])
            job.run();job.close()

    def audit(self,cases=None):return validate_graph(self.snapshot,self.job_dir,cases,model=FakeEmbedding())

    def test_complete_graph_and_labeled_quality_and_read_only_job(self):
        before={n:(self.job_dir/n).read_bytes() for n in ('graph.sqlite','semantic.sqlite')}
        result=self.audit({'queries':[{'id':'q1','kind':'ResearchProblem','query':'Long term memory',
                                       'expected_paper_ids':['paper:1']}],
                           'identity_pairs':[{'id':'s','left':{'paper_id':'paper:1','key':'problem_1'},
                                              'right':{'paper_id':'paper:2','key':'problem_1'},'expected':'same'},
                                             {'id':'u','left':{'paper_id':'paper:1','key':'method_1'},
                                              'right':{'paper_id':'paper:2','key':'method_1'},'expected':'unknown'}]})
        self.assertTrue(result['structural']['passed'],result['structural'])
        self.assertEqual(result['quality']['hit_at_5'],1)
        self.assertEqual(result['quality']['mrr'],1)
        self.assertEqual(result['quality']['identity'],{'resolved':1,'wrong':0,'pending':0,'unknown_unscored':1})
        self.assertEqual(before,{n:(self.job_dir/n).read_bytes() for n in before})
        self.assertEqual(self.audit()['quality']['status'],'untested')

    def test_cli_rejects_output_collisions_before_writing(self):
        cases=self.root/'cases.json';cases.write_text('{}')
        inputs=[self.snapshot,cases,*(self.job_dir/name for name in
                 ('graph.sqlite','semantic.sqlite','snapshot-master.sqlite'))]
        original={path:path.read_bytes() for path in inputs}
        base=['--snapshot',str(self.snapshot),'--job-dir',str(self.job_dir),'--cases',str(cases)]
        for source in inputs:
            with self.subTest(path=source):
                with self.assertRaisesRegex(ValueError,'collides'):
                    validate_cli([*base,'--output',str(source)])
        alias=self.root/'output-alias.json';alias.symlink_to(self.snapshot)
        with self.assertRaisesRegex(ValueError,'collides'):
            validate_cli([*base,'--output',str(alias)])
        hardlink=self.root/'output-hardlink.json';os.link(self.snapshot,hardlink)
        with self.assertRaisesRegex(ValueError,'collides'):
            validate_cli([*base,'--output',str(hardlink)])
        self.assertEqual(original,{path:path.read_bytes() for path in inputs})

    def test_master_preserves_source_text_pins_and_extraction(self):
        master=self.job_dir/'snapshot-master.sqlite'
        with sqlite3.connect(master) as db:
            db.execute("UPDATE paper_versions SET abstract='Altered' WHERE paper_id='paper:1'")
            db.execute("UPDATE paper_versions SET source_url='https://wrong.example' WHERE paper_id='paper:2'")
            db.execute("UPDATE extraction_records SET payload_json='{}' WHERE paper_id='paper:1'")
        result=self.audit()
        counts=result['structural']['failure_counts']
        self.assertEqual(counts['master_paper_version'],2)
        self.assertEqual(counts['master_extraction'],1)

    def test_faults_missing_source_wrong_kind_merge_and_fts(self):
        path=self.job_dir/'semantic.sqlite'
        with sqlite3.connect(path) as db:
            db.execute("DELETE FROM sources WHERE paper_id='paper:2'")
            db.execute("UPDATE mentions SET kind='Benchmark' WHERE item_key='method_1' AND extraction_id IN (SELECT extraction_id FROM sources WHERE paper_id='paper:1')")
            db.execute('DELETE FROM entity_fts')
        result=self.audit()
        codes=result['structural']['failure_counts']
        self.assertIn('missing_source',codes)
        self.assertIn('mention_payload',codes)
        self.assertIn('entity_fts',codes)
        self.assertFalse(result['structural']['passed'])
        self.assertLessEqual(len(result['structural']['details']),20)

    def test_item_review_hash_prevents_payload_substitution(self):
        snapshot=json.loads(self.snapshot.read_text())
        snapshot['config']={'review_policy':'item-v1'}
        snapshot['review_metadata']={x['paper_id']:{'policy':'item-v1','items':[
            {'field':field,'key':item['key'],'state':'approved',
             'approved_payload_hash':canonical_hash(item)}
            for field in ('research_problems','methods','benchmarks','contributions') for item in x[field]]}
            for x in snapshot['extractions']}
        snapshot['review_metadata']['paper:1']['items'][0]['approved_payload_hash']='wrong'
        snapshot['snapshot_hash']=canonical_hash({k:v for k,v in snapshot.items() if k!='snapshot_hash'})
        self.snapshot.write_text(json.dumps(snapshot))
        with self.assertRaisesRegex(ValueError,'approved item payload'):
            self.audit()

    def test_different_pair_shared_entity_rejected(self):
        with sqlite3.connect(self.job_dir/'graph.sqlite') as db:
            row=db.execute("SELECT pair_id FROM candidates WHERE decision='same' LIMIT 1").fetchone()
            db.execute("UPDATE candidates SET decision='different' WHERE pair_id=?",row)
        self.assertIn('different_pair_merged',self.audit()['structural']['failure_counts'])

    def test_coherent_all_pending_has_no_quality_claim(self):
        other=self.root/'pending-job'
        def pending_pairs(items,client):
            return [{'pair_id':p['pair_id'],'decision':'pending','raw':{}} for p in items]
        def pending_groups(items,client):
            return [{'group_id':g['group_id'],'passed':False,'issues':[],'raw':{}} for g in items]
        with patch('kg_builder.decision_api.judge_pairs',pending_pairs),patch('kg_builder.decision_api.review_groups',pending_groups):
            job=SnapshotGraph(self.snapshot,other,client=object(),k=1,model=FakeEmbedding())
            job.run();job.close()
        result=validate_graph(self.snapshot,other)
        self.assertTrue(result['structural']['passed'],result['structural'])
        self.assertFalse(result['structural']['complete_resolution'])
        self.assertGreater(result['structural']['coverage']['pending_mentions'],0)
        self.assertEqual(result['quality']['status'],'untested')

    def test_pending_is_reported_without_quality_pass_claim(self):
        with sqlite3.connect(self.job_dir/'semantic.sqlite') as db:
            mid=db.execute("SELECT mention_id FROM mentions WHERE kind='Benchmark' LIMIT 1").fetchone()[0]
            db.execute("UPDATE mentions SET decision='pending',entity_id=NULL WHERE mention_id=?",(mid,))
        result=self.audit({'identity_pairs':[{'id':'pending','left':{'paper_id':'paper:1','key':'benchmark_1'},
                    'right':{'paper_id':'paper:2','key':'benchmark_1'},'expected':'different'}]})
        self.assertFalse(result['structural']['passed'])
        self.assertEqual(result['quality']['identity']['pending'],1)
        self.assertEqual(result['structural']['coverage']['pending_mentions'],1)
        self.assertIsNone(result['quality']['hit_at_5'])


if __name__=='__main__':unittest.main()
