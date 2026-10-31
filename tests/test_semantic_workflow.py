import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from kg_builder.semantic_store import SemanticStore, REVIEW_VERSION
from kg_builder.storage import connect,upsert_paper,ingest_extraction
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION

class FakeModel:
    model_id='fake:1'
    def __init__(self):self.calls=[];self.fail=False
    def encode(self,texts):
        if self.fail:raise RuntimeError('injected encoder failure')
        self.calls.extend(texts)
        return [[1.,0.] if any(word in t.lower() for word in ('memory','remember','earlier'))
                else [0.,1.] for t in texts]

class SemanticTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name);self.master=self.root/'master.db'
        self.db=connect(self.master);self.addCleanup(self.db.close)
        self.paper,self.version=upsert_paper(self.db,{'title':'Memory Paper','arxiv_id':'2501.12345',
            'version_label':'v1','url':'https://arxiv.org/abs/2501.12345v1'},'arxiv','2501.12345','q')
        self.payload={'schema_version':RETRIEVAL_SCHEMA_VERSION,'paper_id':self.paper,
            'version_id':self.version,'paper_version':{'version_label':'v1','source_url':'https://arxiv.org/abs/2501.12345v1'},
            'extractor_type':'assistant_manual','extractor_version':'2026-10-31-v1',
            'source_scope':'abstract_only','status':'draft_requires_review',
            'contributions':[{'key':self.version+'#c1','description':'A memory method'}],
            'research_problems':[{'key':'p1','name':'Memory decay','description':'Agents forget memory'}],
            'methods':[{'key':'m1','name':'Memory retrieval','description':'Retrieve memory','role':'proposed'}],
            'benchmarks':[{'key':'b1','name':'Memory benchmark','description':'Tests recall',
                           'roles':['used'],'version_or_subset':'subset A'}]}
        self.eid=ingest_extraction(self.db,self.payload)
        self.model=FakeModel();self.store=SemanticStore(self.root/'semantic.db',self.model)
        self.addCleanup(self.store.close)

    def packet(self,kind='ResearchProblem'):
        return self.store.export_candidates(kind)['mentions'][0]

    def assignment(self,packet,decision,eid=None):
        return {'mention_id':packet['mention_id'],'model_id':self.model.model_id,
            'precondition':packet['precondition'],'candidate_revisions':packet['candidate_revisions'],
            'decision':decision,'entity_id':eid}

    def test_ingest_review_search_and_incremental_matching(self):
        self.assertEqual(self.store.ingest(self.master,[self.eid])[0]['status'],'added')
        self.assertEqual(self.store.ingest(self.master,[self.eid])[0]['status'],'unchanged')
        packet=self.packet()
        clustering=self.store.cluster_problems()
        self.assertEqual(clustering['clusters'],[[packet['mention_id']]])
        review={'schema_version':REVIEW_VERSION,'review_id':'r1','entities':[{
            'entity_id':'rp:memory','kind':'ResearchProblem','name':'Memory retention',
            'description':'Remember past context','aliases':['Memory decay'],
            'name_origin':'reviewer_generated'}],
            'assignments':[self.assignment(packet,'new_entity','rp:memory')]}
        builds=self.store.stats['dense_index_builds']
        self.assertEqual(self.store.import_review(review)['status'],'imported')
        self.assertEqual(self.store.stats['dense_index_builds'],builds)
        self.assertEqual(self.store.import_review(review)['status'],'unchanged')
        self.assertEqual(self.store.get_entity('rp:memory')['mentions'][0]['mention_id'],packet['mention_id'])
        self.assertTrue(self.store.search('memory')['results'])
        method=self.packet('Method')
        self.assertEqual(method['candidates'],[])
        pending_review={'schema_version':REVIEW_VERSION,'review_id':'r2','entities':[],
                        'assignments':[self.assignment(method,'pending')]}
        self.store.import_review(pending_review)
        self.assertEqual(len(self.store.export_candidates('Method')['mentions']),1)
        other=copy.deepcopy(self.payload);other['extractor_version']='second'
        other['research_problems'][0]['key']='p2';other['methods']=[];other['benchmarks']=[];other['contributions']=[]
        second=ingest_extraction(self.db,other)
        self.store.ingest(self.master,[second])
        candidate=self.store.export_candidates('ResearchProblem')['mentions'][0]
        self.assertEqual(candidate['candidates'][0]['entity_id'],'rp:memory')
        match=self.assignment(candidate,'match','rp:memory')
        match['target_revision']=candidate['candidates'][0]['revision']
        self.store.import_review({'schema_version':REVIEW_VERSION,'review_id':'r3','entities':[],
                                  'assignments':[match]})
        self.assertEqual(len(self.store.get_entity('rp:memory')['mentions']),2)
        self.assertEqual(self.store.stats['dense_index_builds'],builds+3)

    def test_kind_first_candidate_and_alias_sparse(self):
        self.store.ingest(self.master,[self.eid])
        # The method representation must survive a larger wrong-kind population.
        with self.store.db:
            for i in range(40):
                eid=f'rp:wrong:{i}'
                self.store.db.execute('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',
                    (eid,'ResearchProblem','Memory retrieval','Retrieve memory',
                     'reviewer_generated','[]',f'rev:{i}'))
            self.store.db.execute('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',
                ('method:alias','Method','Archival lookup','Retrieve memory',
                 'source_authored','["Memory retrieval"]','rev:method'))
            self.store.db.execute('INSERT INTO entity_fts VALUES(?,?,?,?)',
                ('method:alias','Archival lookup','Retrieve memory','Memory retrieval'))
            self.store.db.execute('INSERT INTO entity_surfaces VALUES(?,?,?)',
                ('Method','memory retrieval','method:alias'))
        packet=self.packet('Method')
        self.assertIn('method:alias',[c['entity_id'] for c in packet['candidates']])
        self.assertTrue(all(c['kind']=='Method' for c in packet['candidates']))
        self.assertIn('method:alias',[r['entity_id'] for r in
            self.store.search('Memory retrieval',kind='Method')['results']])
        self.assertEqual(self.store.search('Memory retrieval',kind='ResearchProblem')['results'][0]['kind'],
                         'ResearchProblem')
        with self.assertRaises(ValueError):self.store.export_candidates(limit=101)

    def test_contribution_dense_paraphrase_and_local_source_refresh(self):
        self.store.ingest(self.master,[self.eid])
        first=self.store.search('remember earlier conversations',kind='Contribution')
        self.assertEqual(first['results'][0]['description'],'A memory method')
        builds=self.store.stats['dense_index_builds']
        other=copy.deepcopy(self.payload);other['extractor_version']='independent'
        other['contributions'][0]['key']=self.version+'#c2'
        other['contributions'][0]['description']='Unrelated benchmark claim'
        other['research_problems']=[];other['methods']=[];other['benchmarks']=[]
        second=ingest_extraction(self.db,other)
        self.store.ingest(self.master,[second])
        self.assertEqual(self.store.stats['dense_index_builds'],builds)
        modified=copy.deepcopy(self.payload)
        modified['contributions'][0]['description']='A completely different finding'
        ingest_extraction(self.db,modified)
        self.store.ingest(self.master,[self.eid])
        self.assertEqual(self.store.stats['dense_index_builds'],builds)
        self.assertNotIn('A memory method',[r['description'] for r in
            self.store.search('remember earlier conversations',kind='Contribution')['results']])
        self.assertIn('Unrelated benchmark claim',[r['description'] for r in
            self.store.search('benchmark',kind='Contribution')['results']])

    def test_exact_casefold_alias_beats_dense_distractor(self):
        self.store.ingest(self.master,[self.eid])
        method=self.packet('Method')
        first={'schema_version':REVIEW_VERSION,'review_id':'exact-primary',
               'entities':[{'entity_id':'method:primary','kind':'Method','name':'Memory retrieval',
                            'description':'Retrieve memory','aliases':['LoCoMo']}],
               'assignments':[self.assignment(method,'new_entity','method:primary')]}
        self.store.import_review(first)
        with self.store.db:
            for i in range(20):
                eid=f'method:distractor:{i}'
                self.store.db.execute('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',
                    (eid,'Method','Memory pattern','Memory retrieval text','source_authored','[]',f'rev:{i}'))
                self.store.db.execute('INSERT INTO entity_fts VALUES(?,?,?,?)',
                    (eid,'Memory pattern','Memory retrieval text',''))
                self.store.db.execute('INSERT INTO entity_surfaces VALUES(?,?,?)',
                    ('Method','memory pattern',eid))
        # Test the cold index after adding independent distractors in this test fixture.
        self.store._indexes.pop('Method',None)
        other=copy.deepcopy(self.payload);other['extractor_version']='exact-variant'
        other['methods'][0]['name']='locomo';other['methods'][0]['key']='variant'
        other['research_problems']=[];other['benchmarks']=[];other['contributions']=[]
        eid=ingest_extraction(self.db,other);self.store.ingest(self.master,[eid])
        candidate=self.store.export_candidates('Method')['mentions'][0]
        self.assertEqual(candidate['candidates'][0]['entity_id'],'method:primary')
        plan=self.store.db.execute('EXPLAIN QUERY PLAN SELECT entity_id FROM entity_surfaces '
            'WHERE kind=? AND normalized_name=? ORDER BY entity_id LIMIT ?',
            ('Method','locomo',8)).fetchall()
        self.assertTrue(any('entity_surfaces' in str(tuple(r)) and 'INDEX' in str(tuple(r)) for r in plan))

    def test_changed_source_revokes_old_membership_and_stale_packet(self):
        self.store.ingest(self.master,[self.eid]);packet=self.packet()
        updated=copy.deepcopy(self.payload)
        updated['research_problems'][0]['description']='Now a changed problem description'
        ingest_extraction(self.db,updated)
        self.assertEqual(self.store.ingest(self.master,[self.eid])[0]['status'],'replaced')
        review={'schema_version':REVIEW_VERSION,'review_id':'stale','entities':[],
            'assignments':[self.assignment(packet,'pending')]}
        with self.assertRaisesRegex(ValueError,'Stale'):self.store.import_review(review)
        self.assertEqual(len(self.store.get_paper(self.paper)['mentions']),3)
        self.assertEqual(len(self.store.get_paper(self.paper)['contributions']),1)

    def test_model_change_invalidates_old_review_packet(self):
        self.store.ingest(self.master,[self.eid]);old=self.packet()
        self.model.model_id='fake:2'
        self.packet()
        review={'schema_version':REVIEW_VERSION,'review_id':'old-model','entities':[],
                'assignments':[self.assignment(old,'pending')]}
        review['assignments'][0]['model_id']='fake:1'
        with self.assertRaisesRegex(ValueError,'model'):self.store.import_review(review)

    def test_source_authored_primary_and_explicit_alias(self):
        other=copy.deepcopy(self.payload);other['extractor_version']='variant'
        other['methods'][0]['name']='LOCOMO';other['methods'][0]['key']='m2'
        other['research_problems']=[];other['benchmarks']=[];other['contributions']=[]
        second=ingest_extraction(self.db,other)
        self.store.ingest(self.master,[self.eid,second])
        packets=self.store.export_candidates('Method')['mentions']
        review={'schema_version':REVIEW_VERSION,'review_id':'aliases',
                'entities':[{'entity_id':'method:locomo','kind':'Method','name':'Memory retrieval',
                             'description':'Method identity','aliases':['LOCOMO']}],
                'assignments':[self.assignment(p,'new_entity','method:locomo') for p in packets]}
        self.assertEqual(self.store.import_review(review)['assignments'],2)
        self.assertEqual(len(self.store.get_entity('method:locomo')['mentions']),2)

    def test_postcommit_index_failure_invalidates_warm_cache(self):
        self.store.ingest(self.master,[self.eid]);packet=self.packet('Method')
        self.assertIn('Method',self.store._indexes)
        review={'schema_version':REVIEW_VERSION,'review_id':'warm-fail',
                'entities':[{'entity_id':'method:memory','kind':'Method','name':'Memory retrieval',
                             'description':'Unique new representative','aliases':[]}],
                'assignments':[self.assignment(packet,'new_entity','method:memory')]}
        self.model.fail=True
        result=self.store.import_review(review)
        self.assertEqual(result['index_status'],'rebuild_required')
        self.assertNotIn('Method',self.store._indexes)
        self.assertIsNotNone(self.store.get_entity('method:memory'))
        self.assertEqual(self.store.import_review(review)['status'],'unchanged')
        self.model.fail=False
        self.assertEqual(self.store.search('Memory retrieval',kind='Method')['results'][0]['entity_id'],
                         'method:memory')

    def test_atomic_conflicts_and_locality(self):
        self.store.ingest(self.master,[self.eid]);packet=self.packet()
        entity={'entity_id':'rp:x','kind':'ResearchProblem','name':'Memory decay',
                'description':'Agents forget memory','aliases':[]}
        good=self.assignment(packet,'new_entity','rp:x')
        bad=copy.deepcopy(good);bad['mention_id']='unknown'
        review={'schema_version':REVIEW_VERSION,'review_id':'fail','entities':[entity],
                'assignments':[good,bad]}
        with self.assertRaises(ValueError):self.store.import_review(review)
        self.assertIsNone(self.store.get_entity('rp:x'))
        review['assignments']=[good];self.store.import_review(review)
        with self.assertRaisesRegex(ValueError,'conflict'):
            altered=copy.deepcopy(review);altered['entities'][0]['description']='changed'
            self.store.import_review(altered)
        with self.assertRaises(ValueError):self.store.ingest(self.master,[self.eid,self.eid])
        # Large unrelated content stays outside the ID detail query and its response.
        self.store.db.executemany('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',
             [(f'unrelated:{i}','Method','x','z'*10000,'source_authored','[]',f'r{i}') for i in range(500)])
        self.store.db.commit()
        before=self.store.stats['detail_rows']
        detail=self.store.get_entity('rp:x',limit=1)
        self.assertEqual(self.store.stats['detail_rows']-before,2)
        self.assertLess(len(json.dumps(detail)),2000)
        plan=self.store.db.execute('''EXPLAIN QUERY PLAN SELECT m.mention_id,s.paper_id FROM mentions m
            JOIN sources s ON s.extraction_id=m.extraction_id WHERE m.entity_id=?
            ORDER BY m.mention_id LIMIT ? OFFSET ?''',('rp:x',2,0)).fetchall()
        self.assertTrue(any('mentions_entity' in str(tuple(x)) for x in plan))

if __name__=='__main__':unittest.main()
