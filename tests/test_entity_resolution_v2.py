import copy
import json
import sqlite3
import sys
import unittest
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'src'),str(Path(__file__).resolve().parents[1]/'scripts')]
import test_entity_resolution as legacy_tests
from test_entity_resolution import digest
from kg_builder.storage import ingest_extraction
from kg_builder.entity_resolution import (validate_resolution_manifest,import_resolution_manifest,
                                           get_resolution_mentions,get_resolution_entity)


class ResolutionV2Tests(unittest.TestCase):
    def setUp(self):
        legacy_tests.ResolutionTests.setUp(self)

    def v2(self):
        manifest=copy.deepcopy(self.manifest)
        manifest['schema_version']='research-kg-entity-resolution/0.2'
        manifest['batch_id']='batch:v2'
        for mention in manifest['mentions']:
            mention['review_outcome']='mapped' if mention['decision']=='resolved' else 'insufficient_evidence'
            mention['component_entity_ids']=[]
        return manifest

    def add_entity(self,manifest,eid,kind,scope='named_concept',parents=None):
        entity={'entity_id':eid,'kind':kind,'name':eid,'scope':scope,
                'scope_version_id':self.version if scope=='paper_version_local' else None,
                'aliases':[],'identity_definition':'Reviewer-defined identity',
                'support':[{'extraction_id':self.eid,'source_payload_sha256':self.hash,
                            'evidence_locator_path':'/evidence_locators/0'}]}
        if kind=='Artifact':entity['artifact_kind']='AGENT_SYSTEM'
        if parents is not None:
            entity['parent_problem_ids']=parents
            if parents:entity['relation_basis']='reviewer_taxonomy'
        manifest['entities'].append(entity)
        return entity

    def test_compound_cross_kind_and_non_method(self):
        manifest=self.v2()
        self.add_entity(manifest,'artifact:local','Artifact','paper_version_local')
        mention=manifest['mentions'][0]
        mention.update(decision='pending',entity_id=None,review_outcome='composite_label',
                       component_entity_ids=['concept:grpo','artifact:local'])
        validate_resolution_manifest(manifest)
        import_resolution_manifest(self.con,manifest)
        result=get_resolution_mentions(self.con,manifest['batch_id'],self.eid)
        self.assertEqual(result[0]['review_outcome'],'composite_label')
        self.assertEqual(result[0]['decision'],'pending')
        self.assertIsNone(result[0]['entity'])
        self.assertEqual([e['entity_id'] for e in result[0]['component_entities']],
                         ['concept:grpo','artifact:local'])
        self.assertEqual(result[0]['component_entity_ids'],['concept:grpo','artifact:local'])
        self.assertEqual(result[1]['review_outcome'],'insufficient_evidence')
        self.assertEqual(result[1]['component_entities'],[])
        component_source=copy.deepcopy(self.extraction)
        component_source['extractor_version']='component-source'
        component_id=ingest_extraction(self.con,component_source)
        component_raw=self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                                       (component_id,)).fetchone()[0]
        # Historical first batch remains immutable; a second batch can pin separate component evidence.
        separate=self.v2();separate['batch_id']='batch:component-stale'
        artifact=self.add_entity(separate,'artifact:local','Artifact','paper_version_local')
        artifact['support']=[{'extraction_id':component_id,'source_payload_sha256':digest(component_raw),
                              'evidence_locator_path':'/evidence_locators/0'}]
        separate['mentions'][0].update(decision='pending',entity_id=None,review_outcome='composite_label',
                                       component_entity_ids=['concept:grpo','artifact:local'])
        import_resolution_manifest(self.con,separate)
        self.con.execute('UPDATE extraction_records SET payload_json=? WHERE extraction_id=?',
                         (component_raw+' ',component_id));self.con.commit()
        stale=get_resolution_mentions(self.con,separate['batch_id'],self.eid)[0]
        self.assertTrue(stale['stale']);self.assertIsNone(stale['entity'])
        self.assertEqual(stale['component_entities'],[])
        self.assertEqual(stale['review_outcome'],'composite_label')
        # A separate immutable batch can conclude that the method-shaped source label is taxonomy only.
        other=self.v2();other['batch_id']='batch:not-method'
        other['mentions'][0].update(decision='pending',entity_id=None,
                                   review_outcome='not_a_method',component_entity_ids=[])
        import_resolution_manifest(self.con,other)
        self.assertEqual(get_resolution_mentions(self.con,other['batch_id'],self.eid)[0]['review_outcome'],
                         'not_a_method')
        self.assertEqual(self.con.execute('SELECT schema_version FROM entity_resolution_batches WHERE batch_id=?',
                                          (other['batch_id'],)).fetchone()[0],other['schema_version'])

    def test_outcome_and_components_negative(self):
        baseline=self.v2()
        self.add_entity(baseline,'artifact:local','Artifact','paper_version_local')
        self.add_entity(baseline,'problem:parent','ResearchProblem')
        cases=[
            ('Inconsistent mapped',lambda x:x['mentions'][0].update(decision='pending',entity_id=None)),
            ('Inconsistent composite',lambda x:x['mentions'][0].update(review_outcome='composite_label',
                  decision='pending',entity_id=None,component_entity_ids=['concept:grpo'])),
            ('Invalid composite component',lambda x:x['mentions'][0].update(review_outcome='composite_label',
                  decision='pending',entity_id=None,component_entity_ids=['concept:grpo','problem:parent'])),
            ('not_a_method requires',lambda x:x['mentions'][1].update(review_outcome='not_a_method')),
            ('needs source evidence',lambda x:x['mentions'][0].update(review_outcome='not_a_method',
                  decision='pending',entity_id=None,evidence_locator_paths=[])),
        ]
        for message,change in cases:
            manifest=copy.deepcopy(baseline);change(manifest)
            with self.subTest(message=message),self.assertRaisesRegex(ValueError,message):
                import_resolution_manifest(self.con,manifest)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM entity_resolution_batches').fetchone()[0],0)
        bad=copy.deepcopy(baseline)
        bad['mentions'][0].update(review_outcome='composite_label',decision='pending',entity_id=None,
                                  component_entity_ids=['concept:grpo','concept:grpo'])
        with self.assertRaisesRegex(ValueError,'schema'):
            validate_resolution_manifest(bad)

    def test_problem_hierarchy_diamond_and_stale_parent(self):
        manifest=self.v2()
        self.add_entity(manifest,'problem:root','ResearchProblem',parents=[])
        self.add_entity(manifest,'problem:left','ResearchProblem',parents=['problem:root'])
        self.add_entity(manifest,'problem:right','ResearchProblem',parents=['problem:root'])
        self.add_entity(manifest,'problem:leaf','ResearchProblem',parents=['problem:left','problem:right'])
        parent_source=copy.deepcopy(self.extraction)
        parent_source['extractor_version']='parent-source'
        parent_eid=ingest_extraction(self.con,parent_source)
        parent_raw=self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                                    (parent_eid,)).fetchone()[0]
        manifest['entities'][1]['support']=[{'extraction_id':parent_eid,
            'source_payload_sha256':digest(parent_raw),'evidence_locator_path':'/evidence_locators/0'}]
        mention=manifest['mentions'][1]
        mention.update(decision='resolved',entity_id='problem:leaf',review_outcome='mapped',
                       evidence_locator_paths=['/evidence_locators/0'])
        import_resolution_manifest(self.con,manifest)
        self.assertEqual(get_resolution_entity(self.con,manifest['batch_id'],'problem:leaf')['entity']['entity_id'],
                         'problem:leaf')
        self.assertFalse(get_resolution_mentions(self.con,manifest['batch_id'],self.eid)[1]['stale'])
        self.assertEqual(get_resolution_entity(self.con,manifest['batch_id'],'missing'),
                         {'entity_id':'missing','stale':True,'entity':None})
        self.con.execute('BEGIN')
        self.assertFalse(get_resolution_entity(self.con,manifest['batch_id'],'problem:leaf')['stale'])
        self.assertTrue(self.con.in_transaction)
        self.con.rollback()
        self.con.execute('UPDATE extraction_records SET payload_json=? WHERE extraction_id=?',
                         (parent_raw+' ',parent_eid));self.con.commit()
        self.assertTrue(get_resolution_entity(self.con,manifest['batch_id'],'problem:leaf')['stale'])
        result=get_resolution_mentions(self.con,manifest['batch_id'],self.eid)[1]
        self.assertTrue(result['stale']);self.assertIsNone(result['entity'])
        self.assertEqual(result['review_outcome'],'mapped')

    def test_hierarchy_rejections(self):
        baseline=self.v2()
        self.add_entity(baseline,'problem:a','ResearchProblem',parents=[])
        self.add_entity(baseline,'problem:b','ResearchProblem',parents=[])
        self.add_entity(baseline,'artifact:a','Artifact')
        cases=[
            ('Invalid parent',lambda x:x['entities'][1].update(parent_problem_ids=['unknown'],relation_basis='reviewer_taxonomy')),
            ('Invalid parent',lambda x:x['entities'][1].update(parent_problem_ids=['problem:a'],relation_basis='reviewer_taxonomy')),
            ('Invalid parent',lambda x:x['entities'][1].update(parent_problem_ids=['artifact:a'],relation_basis='reviewer_taxonomy')),
            ('Parent links require',lambda x:x['entities'][1].update(parent_problem_ids=['problem:b'])),
            ('Cycle',lambda x:(x['entities'][1].update(parent_problem_ids=['problem:b'],relation_basis='reviewer_taxonomy'),
                               x['entities'][2].update(parent_problem_ids=['problem:a'],relation_basis='reviewer_taxonomy'))),
            ('Parent links only',lambda x:x['entities'][0].update(parent_problem_ids=[])),
        ]
        for message,change in cases:
            manifest=copy.deepcopy(baseline);change(manifest)
            with self.subTest(message=message),self.assertRaisesRegex(ValueError,message):
                import_resolution_manifest(self.con,manifest)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM entity_resolution_batches').fetchone()[0],0)

    def test_v1_shape_and_v2_reconciliation_retention(self):
        import_resolution_manifest(self.con,self.manifest)
        legacy=get_resolution_mentions(self.con,self.manifest['batch_id'],self.eid)[0]
        self.assertNotIn('review_outcome',legacy)
        manifest=self.v2();self.add_entity(manifest,'artifact:local','Artifact','paper_version_local')
        manifest['mentions'][0].update(review_outcome='composite_label',decision='pending',entity_id=None,
                                       component_entity_ids=['concept:grpo','artifact:local'])
        self.con.execute('BEGIN')
        import_resolution_manifest(self.con,manifest)
        self.assertTrue(self.con.in_transaction)
        self.con.commit()
        before=self.con.execute('SELECT payload_json FROM entity_resolution_batches WHERE batch_id=?',
                                (manifest['batch_id'],)).fetchone()[0]
        import_resolution_manifest(self.con,manifest,raw_json=json.dumps(manifest,indent=2))
        self.assertEqual(self.con.execute('SELECT payload_json FROM entity_resolution_batches WHERE batch_id=?',
                                          (manifest['batch_id'],)).fetchone()[0],before)
        changed=copy.deepcopy(manifest);changed['notes']='changed review'
        with self.assertRaisesRegex(ValueError,'Immutable batch_id'):
            import_resolution_manifest(self.con,changed)
        self.con.close()
        from reconcile_sqlite import reconcile
        result=reconcile([self.path],self.root/'copy.db',self.root/'report.json')
        self.assertEqual(result['counts']['entity_resolution_batches'],2)
        out=sqlite3.connect(self.root/'copy.db')
        self.assertEqual(get_resolution_mentions(out,manifest['batch_id'],self.eid)[0]['review_outcome'],
                         'composite_label')
        out.close()
        again=reconcile([self.root/'copy.db'],self.root/'again.db',self.root/'again.json')
        self.assertEqual(again['counts']['entity_resolution_batches'],2)

    def test_explicit_entity_index_and_unrelated_growth(self):
        manifest=self.v2()
        self.add_entity(manifest,'problem:root','ResearchProblem',parents=[])
        self.add_entity(manifest,'problem:child','ResearchProblem',parents=['problem:root'])
        import_resolution_manifest(self.con,manifest)
        plan=self.con.execute('''EXPLAIN QUERY PLAN SELECT payload_json FROM entity_resolution_entities
             WHERE batch_id=? AND entity_id=?''',(manifest['batch_id'],'problem:child')).fetchall()
        self.assertTrue(any('sqlite_autoindex_entity_resolution_entities_1' in row[3] for row in plan),plan)
        trace=[];self.con.set_trace_callback(trace.append)
        try:
            missing=get_resolution_entity(self.con,manifest['batch_id'],'absent')
        finally:self.con.set_trace_callback(None)
        self.assertTrue(missing['stale']);self.assertIsNone(missing['entity'])
        self.assertEqual(len(trace),3)
        self.assertTrue(all('extraction_records' not in sql for sql in trace))
        def measure():
            steps=[0]
            self.con.set_progress_handler(lambda:steps.__setitem__(0,steps[0]+1),1)
            try:result=get_resolution_entity(self.con,manifest['batch_id'],'problem:child')
            finally:self.con.set_progress_handler(None,0)
            return steps[0],len(json.dumps(result))
        first_steps,first_bytes=measure()
        for n in range(500):
            entity=copy.deepcopy(manifest['entities'][-1]);entity['entity_id']=f'unrelated:{n}'
            entity['parent_problem_ids']=[]
            self.con.execute('INSERT INTO entity_resolution_entities VALUES(?,?,?,?,?,?)',
                (manifest['batch_id'],entity['entity_id'],entity['kind'],entity['scope'],
                 entity['scope_version_id'],json.dumps(entity)))
        self.con.commit()
        later_steps,later_bytes=measure()
        self.assertLessEqual(later_steps,first_steps+3)
        self.assertEqual(later_bytes,first_bytes)

    def test_deep_parent_chain_is_bounded_by_actual_closure(self):
        manifest=self.v2()
        for index in range(1050):
            parents=[f'problem:{index-1}'] if index else []
            self.add_entity(manifest,f'problem:{index}','ResearchProblem',parents=parents)
        import_resolution_manifest(self.con,manifest)
        result=get_resolution_entity(self.con,manifest['batch_id'],'problem:1049')
        self.assertFalse(result['stale'])
        self.assertEqual(result['entity']['entity_id'],'problem:1049')

if __name__=='__main__':unittest.main()
