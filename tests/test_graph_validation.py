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
from test_snapshot_graph import FakeEmbedding, fixture, groups, pairs, member_result


class GraphValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.snapshot=self.root/'snapshot.json';fixture(self.snapshot)
        self.job_dir=self.root/'job'
        with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',groups):
            job=SnapshotGraph(self.snapshot,self.job_dir,client=object(),k=1,model=FakeEmbedding(),graph_policy='legacy')
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
        def pending_pairs(items,client,**kwargs):
            return [{'pair_id':p['pair_id'],'decision':'pending','raw':{}} for p in items]
        def pending_groups(items,client,**kwargs):
            return [{'group_id':g['group_id'],'passed':False,'issues':[],'raw':{}} for g in items]
        with patch('kg_builder.decision_api.judge_pairs',pending_pairs),patch('kg_builder.decision_api.review_groups',pending_groups):
            job=SnapshotGraph(self.snapshot,other,client=object(),k=1,model=FakeEmbedding(),graph_policy='legacy')
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


class DomainValidationTests(unittest.TestCase):
    def test_old_domain_job_uses_saved_confidence_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            path=root/'old';job=SnapshotGraph(source,path,k=1,graph_policy='domain-v1')
            with job.db:job.db.execute("DELETE FROM meta WHERE key IN ('decision_metric','problem_resolution')")
            job.close()
            def old_members(items,client,**kwargs):
                self.assertEqual(kwargs['decision_metric'],'confidence')
                return [{'membership_id':i['membership_id'],'choice':'not_belongs','confidence':0.9,
                         'excluded':True,'raw':{}} for i in items]
            with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_group_members',old_members):
                job=SnapshotGraph(source,path,client=object(),k=1,model=FakeEmbedding())
                self.assertEqual(job.decision_metric,'confidence')
                job.run()
                for task in job.claim('test'):
                    job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                                 'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                                 'name':'Memory research area','description':'Temporal recall problems'}}])
                job.run();job.close()
            self.assertTrue(validate_graph(source,path)['structural']['passed'])

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.snapshot=self.root/'snapshot.json';fixture(self.snapshot)
        self.job_dir=self.root/'job'
        def member_review(items,client,**kwargs):
            results=[]
            for item in items:
                kind=item['kind'];name=item['member']['name']
                choice='not_belongs' if (kind=='ResearchArea' and name.endswith('2')) or kind=='Method' else 'insufficient'
                confidence=0.8 if kind=='ResearchArea' else 0.7
                results.append(member_result(item,choice,confidence,0.1 if kind=='ResearchArea' else 0.95))
            return results
        with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_group_members',member_review):
            job=SnapshotGraph(self.snapshot,self.job_dir,client=object(),k=1,model=FakeEmbedding(),graph_policy='domain-v1')
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Memory research area','description':'Temporal recall problems'}}])
            job.run();job.close()

    def audit(self,cases=None):return validate_graph(self.snapshot,self.job_dir,cases,model=FakeEmbedding())

    def test_retains_every_original_and_separates_area_quality(self):
        result=self.audit({'queries':[{'id':'q','kind':'ResearchProblem','query':'Memory problem 2','expected_paper_ids':['paper:2']},
                                      {'id':'area','kind':'ResearchArea','query':'Memory research area','expected_paper_ids':['paper:1']}],
                           'identity_pairs':[{'id':'rp','left':{'paper_id':'paper:1','key':'problem_1'},
                                              'right':{'paper_id':'paper:2','key':'problem_1'},'expected':'same'},
                                             {'id':'mb','left':{'paper_id':'paper:1','key':'method_1'},
                                              'right':{'paper_id':'paper:2','key':'method_1'},'expected':'same'}]})
        self.assertTrue(result['structural']['passed'],result['structural'])
        self.assertEqual(result['structural']['coverage']['mentions'],6)
        self.assertEqual(result['structural']['coverage']['excluded_memberships'],1)
        self.assertEqual(result['structural']['coverage']['area_memberships'],1)
        self.assertEqual(result['quality']['identity']['not_applicable_new_semantics'],1)
        self.assertEqual(result['quality']['identity']['resolved'],1)
        self.assertEqual(result['quality']['hit_at_5'],1)
        self.assertEqual(result['quality']['query_cases'],1)
        self.assertEqual(result['quality']['research_area_queries']['hit_at_5'],1)

    def test_missing_review_and_wrong_boundary_exclusion(self):
        with sqlite3.connect(self.job_dir/'graph.sqlite') as db:
            row=db.execute("SELECT group_id,mention_id FROM member_reviews WHERE audit LIKE '%0.7%' LIMIT 1").fetchone()
            db.execute('UPDATE member_reviews SET excluded=1 WHERE group_id=? AND mention_id=?',row)
            db.execute("DELETE FROM member_reviews WHERE rowid IN (SELECT rowid FROM member_reviews WHERE NOT (group_id=? AND mention_id=?) LIMIT 1)",row)
        codes=self.audit()['structural']['failure_counts']
        self.assertIn('member_exclusion_evidence',codes)
        self.assertIn('missing_member_review',codes)

    def test_probability_audit_checks_raw_distribution_and_saved_resolution(self):
        from kg_builder.problem_clustering import cluster_problem_vectors
        captured=[]
        def recalculate(vectors,**kwargs):
            captured.append(kwargs['resolution'])
            return cluster_problem_vectors(vectors,**kwargs)
        with patch('kg_builder.problem_clustering.cluster_problem_vectors',side_effect=recalculate):
            self.assertTrue(self.audit()['structural']['passed'])
        self.assertEqual(captured,[0.05])
        with sqlite3.connect(self.job_dir/'graph.sqlite') as db:
            row=db.execute('SELECT group_id,mention_id,audit FROM member_reviews LIMIT 1').fetchone()
            audit=json.loads(row[2]);audit['probability']=0.99 if audit['probability']!=0.99 else 0.01
            db.execute('UPDATE member_reviews SET audit=? WHERE group_id=? AND mention_id=?',
                       (json.dumps(audit),row[0],row[1]))
        self.assertIn('member_exclusion_evidence',self.audit()['structural']['failure_counts'])

    def test_nondefault_resolution_recomputed_by_validator(self):
        from kg_builder.problem_clustering import cluster_problem_vectors
        path=self.root/'fine';seen=[]
        def capture(vectors,**kwargs):
            seen.append(kwargs['resolution'])
            return cluster_problem_vectors(vectors,**kwargs)
        def keep(items,client,**kwargs):return [member_result(i) for i in items]
        with patch('kg_builder.snapshot_graph.cluster_problem_vectors',side_effect=capture),\
             patch('kg_builder.decision_api.judge_pairs',pairs),\
             patch('kg_builder.decision_api.review_group_members',keep):
            job=SnapshotGraph(self.snapshot,path,client=object(),k=1,model=FakeEmbedding(),
                              graph_policy='domain-v1',problem_resolution=0.3)
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Area','description':'Problem area'}}])
            job.run();job.close()
        with patch('kg_builder.problem_clustering.cluster_problem_vectors',side_effect=capture):
            self.assertTrue(validate_graph(self.snapshot,path)['structural']['passed'])
        self.assertEqual(seen,[0.3,0.3])

    def test_missing_node_area_link_and_proposal(self):
        with sqlite3.connect(self.job_dir/'semantic.sqlite') as db:
            db.execute("DELETE FROM research_area_members")
            db.execute("DELETE FROM entity_fts WHERE entity_id IN (SELECT entity_id FROM entities WHERE kind='ResearchProblem')")
        (self.job_dir/'problem-proposal.json').write_text('{"clusters": []}')
        codes=self.audit()['structural']['failure_counts']
        self.assertIn('research_area_membership',codes)
        self.assertIn('entity_fts',codes)
        self.assertIn('leiden_partition_mismatch',codes)

    def test_missing_original_mention_is_detected(self):
        with sqlite3.connect(self.job_dir/'semantic.sqlite') as db:
            db.execute("DELETE FROM mentions WHERE kind='ResearchProblem' AND name='Memory problem 2'")
        codes=self.audit()['structural']['failure_counts']
        self.assertIn('missing_mention',codes)
        self.assertIn('dangling_or_cross_kind_group_member',codes)

    def test_pending_pair_does_not_freeze_approved_members(self):
        other=self.root/'pending'
        def uncertain(items,client,**kwargs):
            return [{**r,'decision':'pending' if item['kind']=='Method' else 'different'}
                    for item,r in zip(items,pairs(items,client))]
        def retain(items,client,**kwargs):
            return [member_result(item,'insufficient',0.1,0.9) for item in items]
        with patch('kg_builder.decision_api.judge_pairs',uncertain),patch('kg_builder.decision_api.review_group_members',retain):
            job=SnapshotGraph(self.snapshot,other,client=object(),k=1,model=FakeEmbedding(),graph_policy='domain-v1')
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Memory area','description':'Source grounded'}}])
            job.run();job.close()
        result=validate_graph(self.snapshot,other)
        self.assertTrue(result['structural']['passed'],result['structural'])
        self.assertEqual(result['structural']['coverage']['pending_pairs'],1)
        self.assertTrue(result['structural']['coverage']['all_source_nodes_available'])
        self.assertFalse(result['structural']['complete_resolution'])

    def test_malformed_audit_and_group_payload_fail_bounded_audit(self):
        with sqlite3.connect(self.job_dir/'graph.sqlite') as db:
            db.execute("UPDATE member_reviews SET audit='{}' WHERE rowid=(SELECT rowid FROM member_reviews LIMIT 1)")
        codes=self.audit()['structural']['failure_counts']
        self.assertIn('member_exclusion_evidence',codes)
        with sqlite3.connect(self.job_dir/'graph.sqlite') as db:
            db.execute("UPDATE groups SET members_json='not-json' WHERE rowid=(SELECT rowid FROM groups LIMIT 1)")
        self.assertIn('invalid_group_members',self.audit()['structural']['failure_counts'])

    def test_legacy_job_without_policy_marker_still_uses_old_audit(self):
        legacy=self.root/'legacy'
        with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',groups):
            job=SnapshotGraph(self.snapshot,legacy,client=object(),k=1,model=FakeEmbedding(),graph_policy='legacy')
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Canonical problem','description':'Memory over time'}}])
            job.run();job.close()
        with sqlite3.connect(legacy/'graph.sqlite') as db:db.execute("DELETE FROM meta WHERE key='graph_policy'")
        self.assertTrue(validate_graph(self.snapshot,legacy)['structural']['passed'])


if __name__=='__main__':unittest.main()
