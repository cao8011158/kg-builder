import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kg_builder.graph_validation import validate_graph
from scripts.validate_snapshot_graph import main as validate_cli
from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash
from tests.test_snapshot_graph import FakeEmbedding, fixture, groups, pairs, member_result


from tests.postgres_graph_cli_fixtures import NativeGraphCase


class GraphValidationTests(NativeGraphCase):
    def setUp(self):
        super().setUp()
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.snapshot=self.root/'snapshot.json';fixture(self.snapshot)
        self.job_dir=self.root/'job'
        with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',groups):
            job=self.make_job(self.snapshot,self.job_dir,client=object(),k=1,model=FakeEmbedding(),graph_policy='legacy')
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Long term memory','description':'Memory over time'}}])
            job.run();job.close()

    def audit(self,cases=None):return validate_graph(self.snapshot,self.job_dir,cases,model=FakeEmbedding())

    def test_complete_graph_and_labeled_quality_and_read_only_job(self):
        with self.connection() as db:
            before = {table:db.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall() for table in ('embedding_cache','metadata','entities','graph_outbox','meta')}
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
        with self.connection() as db:
            self.assertEqual(before,{table:db.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall() for table in before})
        self.assertEqual(self.audit()['quality']['status'],'untested')

    def test_audit_uses_one_repeatable_read_readonly_snapshot(self):
        from kg_builder import graph_validation
        with self.connection() as db:
            db.execute("INSERT INTO meta(key,value) VALUES('readonly_probe','before')")
        original = graph_validation._validate_graph
        observed = []
        def inspect(snapshot, directory, cases, model, search):
            db = search.db
            self.assertEqual(db.execute('SHOW transaction_read_only').fetchone()['transaction_read_only'],'on')
            self.assertEqual(db.execute('SHOW transaction_isolation').fetchone()['transaction_isolation'],'repeatable read')
            observed.append(db.execute("SELECT value FROM meta WHERE key='readonly_probe'").fetchone()['value'])
            with self.connection() as other:
                other.execute("UPDATE meta SET value='after' WHERE key='readonly_probe'")
            observed.append(db.execute("SELECT value FROM meta WHERE key='readonly_probe'").fetchone()['value'])
            return original(snapshot,directory,cases,model,search)
        with patch.object(graph_validation,'_validate_graph',side_effect=inspect):
            self.assertTrue(self.audit()['structural']['passed'])
        self.assertEqual(observed,['before','before'])
        with self.connection() as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='readonly_probe'").fetchone()['value'],'after')

    def test_cli_rejects_output_collisions_before_writing(self):
        cases=self.root/'cases.json';cases.write_text('{}')
        inputs=[self.snapshot,cases,*(self.job_dir/name for name in
                 ('graph-job.json',))]
        original={path:path.read_bytes() for path in inputs}
        base=['--snapshot',str(self.snapshot),'--job-dir',str(self.job_dir),'--cases',str(cases)]
        for source in inputs:
            with self.subTest(path=source):
                with self.assertRaisesRegex(ValueError,'collides'):
                    validate_cli([*base,'--output',str(source)])
        alias=self.root/'output-alias.json'
        try:
            alias.symlink_to(self.snapshot)
        except OSError as exc:
            if getattr(exc,'winerror',None)!=1314:raise
            # Windows user has no symlink privilege; hard-link collision still runs below.
        else:
            with self.assertRaisesRegex(ValueError,'collides'):
                validate_cli([*base,'--output',str(alias)])
        hardlink=self.root/'output-hardlink.json';os.link(self.snapshot,hardlink)
        with self.assertRaisesRegex(ValueError,'collides'):
            validate_cli([*base,'--output',str(hardlink)])
        self.assertEqual(original,{path:path.read_bytes() for path in inputs})

    def test_master_preserves_source_text_pins_and_extraction(self):
        with self.connection() as db:
            db.execute("UPDATE paper_versions SET abstract='Altered' WHERE paper_id='paper:1'")
            db.execute("UPDATE paper_versions SET source_url='https://wrong.example' WHERE paper_id='paper:2'")
            db.execute("UPDATE extraction_records SET payload_json='{}' WHERE paper_id='paper:1'")
        result=self.audit()
        counts=result['structural']['failure_counts']
        self.assertEqual(counts['master_paper_version'],2)
        self.assertEqual(counts['master_extraction'],1)

    def test_faults_missing_source_wrong_kind_merge_and_fts(self):
        with self.connection() as db:
            db.execute("DELETE FROM sources WHERE paper_id='paper:2'")
            db.execute("UPDATE mentions SET kind='Benchmark' WHERE item_key='method_1' AND extraction_id IN (SELECT extraction_id FROM sources WHERE paper_id='paper:1')")
            db.execute('DELETE FROM entity_fts')
            db.execute("INSERT INTO graph_outbox(event_id,kind,key,payload_json) VALUES('bad:event','bad','event','{}')")
        result=self.audit()
        codes=result['structural']['failure_counts']
        self.assertIn('missing_source',codes)
        self.assertIn('mention_payload',codes)
        self.assertIn('entity_fts',codes)
        self.assertIn('invalid_graph_outbox',codes)
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
        with self.connection() as db:
            row=db.execute("SELECT pair_id FROM candidates WHERE decision='same' LIMIT 1").fetchone()
            db.execute("UPDATE candidates SET decision='different' WHERE pair_id=%s",(row['pair_id'],))
        self.assertIn('different_pair_merged',self.audit()['structural']['failure_counts'])

    def test_coherent_all_pending_has_no_quality_claim(self):
        other=self.root/'pending-job'
        def pending_pairs(items,client,**kwargs):
            return [{'pair_id':p['pair_id'],'decision':'pending','raw':{}} for p in items]
        def pending_groups(items,client,**kwargs):
            return [{'group_id':g['group_id'],'passed':False,'issues':[],'raw':{}} for g in items]
        with patch('kg_builder.decision_api.judge_pairs',pending_pairs),patch('kg_builder.decision_api.review_groups',pending_groups):
            job=self.make_job(self.snapshot,other,client=object(),k=1,model=FakeEmbedding(),graph_policy='legacy')
            job.run();job.close()
        result=validate_graph(self.snapshot,other)
        self.assertTrue(result['structural']['passed'],result['structural'])
        self.assertFalse(result['structural']['complete_resolution'])
        self.assertGreater(result['structural']['coverage']['pending_mentions'],0)
        self.assertEqual(result['quality']['status'],'untested')

    def test_pending_is_reported_without_quality_pass_claim(self):
        with self.connection() as db:
            mid=db.execute("SELECT mention_id FROM mentions WHERE kind='Benchmark' LIMIT 1").fetchone()['mention_id']
            db.execute("UPDATE mentions SET decision='pending',entity_id=NULL WHERE mention_id=%s",(mid,))
        result=self.audit({'identity_pairs':[{'id':'pending','left':{'paper_id':'paper:1','key':'benchmark_1'},
                    'right':{'paper_id':'paper:2','key':'benchmark_1'},'expected':'different'}]})
        self.assertFalse(result['structural']['passed'])
        self.assertEqual(result['quality']['identity']['pending'],1)
        self.assertEqual(result['structural']['coverage']['pending_mentions'],1)
        self.assertIsNone(result['quality']['hit_at_5'])


class DomainValidationTests(NativeGraphCase):
    def test_old_domain_job_uses_saved_confidence_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'snapshot.json';fixture(source)
            path=root/'old';job=self.make_job(source,path,k=1,graph_policy='domain-v1')
            with job.db.transaction():job.db.execute("DELETE FROM meta WHERE key IN ('decision_metric','problem_resolution')")
            job.close()
            def old_members(items,client,**kwargs):
                self.assertEqual(kwargs['decision_metric'],'confidence')
                return [{'membership_id':i['membership_id'],'choice':'not_belongs','confidence':0.9,
                         'excluded':True,'raw':{}} for i in items]
            def old_pairs(items,client,**kwargs):
                self.assertIsNone(kwargs['same_min_probability'])
                return pairs(items,client,**kwargs)
            with patch('kg_builder.decision_api.judge_pairs',old_pairs),patch('kg_builder.decision_api.review_group_members',old_members):
                job=self.make_job(source,path,client=object(),k=1,model=FakeEmbedding())
                self.assertEqual(job.decision_metric,'confidence')
                job.run()
                for task in job.claim('test'):
                    job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                                 'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                                 'name':'Memory research area','description':'Temporal recall problems'}}])
                job.run();job.close()
            self.assertTrue(validate_graph(source,path)['structural']['passed'])

    def setUp(self):
        super().setUp()
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
            job=self.make_job(self.snapshot,self.job_dir,client=object(),k=1,model=FakeEmbedding(),graph_policy='domain-v1')
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
        with self.connection() as db:
            row=db.execute("SELECT group_id,mention_id FROM member_reviews WHERE audit LIKE '%0.7%' LIMIT 1").fetchone()
            db.execute('UPDATE member_reviews SET excluded=1 WHERE group_id=%s AND mention_id=%s',(row['group_id'],row['mention_id']))
            db.execute("DELETE FROM member_reviews WHERE (group_id,mention_id) IN (SELECT group_id,mention_id FROM member_reviews WHERE NOT (group_id=%s AND mention_id=%s) LIMIT 1)",(row['group_id'],row['mention_id']))
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
        with self.connection() as db:
            row=db.execute('SELECT group_id,mention_id,audit FROM member_reviews LIMIT 1').fetchone()
            audit=json.loads(row['audit']);audit['probability']=0.99 if audit['probability']!=0.99 else 0.01
            db.execute('UPDATE member_reviews SET audit=%s WHERE group_id=%s AND mention_id=%s',
                       (json.dumps(audit),row['group_id'],row['mention_id']))
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
            job=self.make_job(self.snapshot,path,client=object(),k=1,model=FakeEmbedding(),
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
        with self.connection() as db:
            db.execute("DELETE FROM research_area_members")
            db.execute("DELETE FROM entity_fts WHERE entity_id IN (SELECT entity_id FROM entities WHERE kind='ResearchProblem')")
        with self.connection() as db:
            db.execute("UPDATE meta SET value=%s WHERE key='problem_proposal'", ('{"clusters": []}',))
        codes=self.audit()['structural']['failure_counts']
        self.assertIn('research_area_membership',codes)
        self.assertIn('entity_fts',codes)
        self.assertIn('leiden_partition_mismatch',codes)

    def test_missing_original_mention_is_detected(self):
        with self.connection() as db:
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
            job=self.make_job(self.snapshot,other,client=object(),k=1,model=FakeEmbedding(),graph_policy='domain-v1')
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
        with self.connection() as db:
            db.execute("UPDATE member_reviews SET audit='{}' WHERE (group_id,mention_id)=(SELECT group_id,mention_id FROM member_reviews LIMIT 1)")
        codes=self.audit()['structural']['failure_counts']
        self.assertIn('member_exclusion_evidence',codes)
        with self.connection() as db:
            db.execute("UPDATE groups SET members_json='not-json' WHERE group_id=(SELECT group_id FROM groups LIMIT 1)")
        self.assertIn('invalid_group_members',self.audit()['structural']['failure_counts'])

    def test_legacy_job_without_policy_marker_still_uses_old_audit(self):
        legacy=self.root/'legacy'
        with patch('kg_builder.decision_api.judge_pairs',pairs),patch('kg_builder.decision_api.review_groups',groups):
            job=self.make_job(self.snapshot,legacy,client=object(),k=1,model=FakeEmbedding(),graph_policy='legacy')
            job.run()
            for task in job.claim('test'):
                job.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                             'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                             'name':'Canonical problem','description':'Memory over time'}}])
            job.run();job.close()
        with self.connection(legacy) as db:db.execute("DELETE FROM meta WHERE key='graph_policy'")
        self.assertTrue(validate_graph(self.snapshot,legacy)['structural']['passed'])


if __name__=='__main__':unittest.main()
