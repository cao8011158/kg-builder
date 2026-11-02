import concurrent.futures
import time
import threading
import copy
import json
from pathlib import Path
import os
import uuid
import psycopg
from psycopg import sql
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from kg_builder.semantic_store import SemanticStore, REVIEW_VERSION
from kg_builder.postgres_graph import GraphStore
from kg_builder.postgres_source import PostgresSourceRepository

_REPOS={}
def ingest_extraction(conn,payload):
    with conn.transaction():return _REPOS[id(conn)].ingest_extraction(conn,payload)
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
    def test_research_area_retains_original_problem_and_local_expansion(self):
        self.store.ingest(self.master,[self.eid])
        problem=self.packet()
        self.store.import_review({'schema_version':REVIEW_VERSION,'review_id':'source-rp',
            'entities':[{'entity_id':'rp:source','kind':'ResearchProblem','name':'Memory decay',
                         'description':'Agents forget memory','aliases':[]}],
            'assignments':[self.assignment(problem,'new_entity','rp:source')]})
        area={'area_id':'area:memory','name':'Agent memory','description':'Memory retention in agents'}
        member={'area_id':'area:memory','mention_id':problem['mention_id']}
        self.assertEqual(self.store.import_research_areas([area],[member])['memberships'],1)
        self.assertEqual(self.store.import_research_areas([area],[member])['status'],'unchanged')
        detail=self.store.get_entity('area:memory')
        self.assertEqual(detail['entity_id'],'area:memory')
        self.assertEqual(detail['mentions'][0]['problem_entity_id'],'rp:source')
        self.assertEqual(detail['mentions'][0]['name'],'Memory decay')
        self.assertEqual(self.store.get_entity('rp:source')['mentions'][0]['mention_id'],problem['mention_id'])
        hit=self.store.search('retention',kind='ResearchArea')['results'][0]
        self.assertEqual(hit['papers'][0]['paper_id'],self.paper)
        self.assertEqual(hit['entity_id'],'area:memory')
        self.assertTrue(any(r['kind']=='ResearchArea' for r in self.store.search('retention')['results']))
        self.assertTrue(all(r['kind']=='ResearchProblem' for r in
                            self.store.search('memory',kind='ResearchProblem')['results']))
        with self.assertRaisesRegex(ValueError,'conflict'):
            self.store.import_research_areas([dict(area,name='Wrong')],[member])
        self.assertEqual(self.store.get_entity('area:memory')['name'],'Agent memory')
        with self.assertRaisesRegex(ValueError,'ResearchProblem'):
            self.store.import_research_areas([{'area_id':'area:bad','name':'Bad','description':'Bad'}],
                [{'area_id':'area:bad','mention_id':self.packet('Method')['mention_id']}])
        self.assertIsNone(self.store.get_entity('area:bad'))
        # Native PostgreSQL plans and bounded response work replace SQLite VM counters.
        def measured_detail():
            before=self.store.stats['detail_rows']
            value=self.store.get_entity('area:memory',limit=1)
            return self.store.stats['detail_rows']-before,len(json.dumps(value))
        baseline=measured_detail()
        with self.store.db.transaction():
            with self.store.db.cursor() as cursor:
                cursor.executemany('INSERT INTO research_areas VALUES(%s,%s,%s,%s)',
                    [(f'area:unrelated:{i}','Elsewhere','Z'*10000,f'rev:{i}') for i in range(1000)])
        self.assertEqual(measured_detail(),baseline)
        self.assertEqual(self.store.search('retention',kind='ResearchArea')['results'][0]['entity_id'],'area:memory')
        self.assert_index('SELECT m.mention_id,s.paper_id FROM research_area_members a '
            'JOIN mentions m ON m.mention_id=a.mention_id JOIN sources s ON s.extraction_id=m.extraction_id '
            'WHERE a.area_id=%s ORDER BY a.mention_id LIMIT %s',('area:memory',2),
            'research_area_members_pkey')
        self.assert_index("SELECT area_id FROM research_area_fts WHERE search_vector @@ to_tsquery('simple',%s)",
                          ('retention',),'research_area_fts_search')
        changed=copy.deepcopy(self.payload)
        changed['research_problems'][0]['description']='Changed source problem'
        ingest_extraction(self.db,changed)
        self.store.ingest(self.master,[self.eid])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM research_area_members WHERE area_id=%s',
                                               ('area:memory',)).fetchone()['count'],0)

    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name);self.master='semantic-test-'+uuid.uuid4().hex
        self.database_url=os.environ['KG_TEST_DATABASE_URL']
        self.graph=GraphStore(self.master,self.database_url)
        self.db=self.graph.db;repo=PostgresSourceRepository(self.graph);_REPOS[id(self.db)]=repo
        self.addCleanup(self.cleanup_database)
        with self.db.transaction():self.paper,self.version=repo.upsert_paper(self.db,{'title':'Memory Paper','arxiv_id':'2501.12345',
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
        self.model=FakeModel();self.store=SemanticStore(self.master,self.model,database_url=self.database_url)
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
        with self.store.db.transaction():
            for i in range(40):
                eid=f'rp:wrong:{i}'
                self.store.db.execute('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (eid,'ResearchProblem','Memory retrieval','Retrieve memory',
                     'reviewer_generated','[]',f'rev:{i}'))
            self.store.db.execute('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
                ('method:alias','Method','Archival lookup','Retrieve memory',
                 'source_authored','["Memory retrieval"]','rev:method'))
            self.store.db.execute('INSERT INTO entity_fts(entity_id,name,description,aliases) VALUES(%s,%s,%s,%s)',
                ('method:alias','Archival lookup','Retrieve memory','Memory retrieval'))
            self.store.db.execute('INSERT INTO entity_surfaces VALUES(%s,%s,%s)',
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
        with self.store.db.transaction():
            for i in range(20):
                eid=f'method:distractor:{i}'
                self.store.db.execute('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (eid,'Method','Memory pattern','Memory retrieval text','source_authored','[]',f'rev:{i}'))
                self.store.db.execute('INSERT INTO entity_fts(entity_id,name,description,aliases) VALUES(%s,%s,%s,%s)',
                    (eid,'Memory pattern','Memory retrieval text',''))
                self.store.db.execute('INSERT INTO entity_surfaces VALUES(%s,%s,%s)',
                    ('Method','memory pattern',eid))
        # Test the cold index after adding independent distractors in this test fixture.
        self.store._indexes.pop('Method',None)
        other=copy.deepcopy(self.payload);other['extractor_version']='exact-variant'
        other['methods'][0]['name']='locomo';other['methods'][0]['key']='variant'
        other['research_problems']=[];other['benchmarks']=[];other['contributions']=[]
        eid=ingest_extraction(self.db,other);self.store.ingest(self.master,[eid])
        candidate=self.store.export_candidates('Method')['mentions'][0]
        self.assertEqual(candidate['candidates'][0]['entity_id'],'method:primary')
        self.assert_index('SELECT entity_id FROM entity_surfaces WHERE kind=%s AND normalized_name=%s '
                          'ORDER BY entity_id LIMIT %s',('Method','locomo',8),'entity_surfaces_pkey')

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
        self.store.db.cursor().executemany('INSERT INTO entities VALUES(%s,%s,%s,%s,%s,%s,%s)',
             [(f'unrelated:{i}','Method','x','z'*10000,'source_authored','[]',f'r{i}') for i in range(500)])
        self.store.db.commit()
        before=self.store.stats['detail_rows']
        detail=self.store.get_entity('rp:x',limit=1)
        self.assertEqual(self.store.stats['detail_rows']-before,2)
        self.assertLess(len(json.dumps(detail)),2000)
        self.assert_index('SELECT m.mention_id,s.paper_id FROM mentions m JOIN sources s '
            'ON s.extraction_id=m.extraction_id WHERE m.entity_id=%s ORDER BY m.mention_id LIMIT %s OFFSET %s',
            ('rp:x',2,0),'mentions_entity')

    def concurrent_imports(self,reviews,mids):
        def worker(index):
            store=SemanticStore(self.master,FakeModel(),database_url=self.database_url)
            try:
                store.db.execute("SELECT set_config('application_name',%s,false)",
                                 (self.master+':race:'+str(index),))
                try:return store.import_review(reviews[index])
                except ValueError as error:return {'status':'rejected','reason':str(error)}
            finally:store.close()
        # Hold the common mention rows until both real backends are blocked.
        # Old code reaches its unguarded UPDATE and then both imports succeed;
        # guarded code waits before validating the pending state.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            with self.db.transaction():
                self.db.execute('SELECT mention_id FROM mentions WHERE mention_id=ANY(%s) '
                                'ORDER BY mention_id FOR UPDATE',(sorted(mids),)).fetchall()
                futures=[pool.submit(worker,index) for index in range(2)]
                deadline=time.monotonic()+15
                while time.monotonic()<deadline:
                    self.db.execute('SELECT pg_stat_clear_snapshot()')
                    blocked=self.db.execute("SELECT count(*) AS n FROM pg_stat_activity WHERE "
                        "application_name LIKE %s AND cardinality(pg_blocking_pids(pid))>0",
                        (self.master+':race:%',)).fetchone()['n']
                    if blocked==2:break
                    time.sleep(.01)
                else:raise AssertionError('Concurrent import backends did not reach native lock waits')
            return [future.result(timeout=20) for future in futures]

    def make_review(self,review_id,packets,prefix):
        entities=[];assignments=[]
        for index,packet in enumerate(packets):
            eid=prefix+str(index)
            entities.append({'entity_id':eid,'kind':packet['kind'],'name':packet['name'],
                             'description':'Concurrent reviewed entity','aliases':[]})
            assignments.append(self.assignment(packet,'new_entity',eid))
        return {'schema_version':REVIEW_VERSION,'review_id':review_id,
                'entities':entities,'assignments':assignments}

    def test_concurrent_reviews_exclude_stale_assignment_and_orphans(self):
        self.store.ingest(self.master,[self.eid]);packet=self.packet()
        reviews=[self.make_review('race:'+str(i),[packet],'rp:race:'+str(i)+':') for i in range(2)]
        outcomes=self.concurrent_imports(reviews,[packet['mention_id']])
        self.assertEqual(sorted(x['status'] for x in outcomes),['imported','rejected'])
        for table in ('entities','entity_fts','entity_surfaces','reviews'):
            self.assertEqual(self.db.execute('SELECT count(*) AS n FROM '+table).fetchone()['n'],1)
        self.assertEqual(self.db.execute('SELECT count(*) AS n FROM entities e WHERE NOT EXISTS '
            '(SELECT 1 FROM mentions m WHERE m.entity_id=e.entity_id)').fetchone()['n'],0)

    def test_concurrent_identical_review_is_idempotent(self):
        self.store.ingest(self.master,[self.eid]);packet=self.packet()
        review=self.make_review('same-review',[packet],'rp:identical:')
        outcomes=self.concurrent_imports([review,copy.deepcopy(review)],[packet['mention_id']])
        self.assertEqual(sorted(x['status'] for x in outcomes),['imported','unchanged'])
        self.assertEqual(self.db.execute('SELECT count(*) AS n FROM reviews').fetchone()['n'],1)

    def test_reversed_multi_mention_reviews_use_canonical_locks(self):
        self.store.ingest(self.master,[self.eid]);packets=[self.packet(),self.packet('Method')]
        reviews=[self.make_review('multi:0',packets,'entity:multi:0:'),
                 self.make_review('multi:1',list(reversed(packets)),'entity:multi:1:')]
        outcomes=self.concurrent_imports(reviews,[p['mention_id'] for p in packets])
        self.assertEqual(sorted(x['status'] for x in outcomes),['imported','rejected'])
        self.assertEqual(self.db.execute('SELECT count(*) AS n FROM entities').fetchone()['n'],2)
        self.assertEqual(self.db.execute('SELECT count(*) AS n FROM reviews').fetchone()['n'],1)

    def test_source_replacement_excludes_stale_concurrent_review(self):
        self.store.ingest(self.master,[self.eid]);packet=self.packet()
        changed=copy.deepcopy(self.payload)
        changed['research_problems'][0]['description']='Replaced source problem'
        ingest_extraction(self.db,changed)
        locked=threading.Event();release=threading.Event()
        class HeldSourceConnection:
            def __init__(self,db):self.db=db
            def __getattr__(self,name):return getattr(self.db,name)
            def execute(self,query,args=None):
                cursor=self.db.execute(query,args)
                if 'SELECT extraction_id FROM sources WHERE extraction_id=ANY' in str(query):
                    locked.set()
                    if not release.wait(15):raise AssertionError('Source lock release was not signaled')
                return cursor
        def replace():
            store=SemanticStore(self.master,FakeModel(),database_url=self.database_url)
            try:
                store.db=HeldSourceConnection(store.db)
                return store.ingest(self.master,[self.eid])
            finally:store.close()
        def review():
            store=SemanticStore(self.master,FakeModel(),database_url=self.database_url)
            try:
                store.db.execute("SELECT set_config('application_name',%s,false)",(self.master+':source-review',))
                try:return store.import_review(self.make_review('source-race',[packet],'rp:stale:'))
                except ValueError as error:return {'status':'rejected','reason':str(error)}
            finally:store.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            replacement=pool.submit(replace)
            self.assertTrue(locked.wait(15))
            reviewing=pool.submit(review)
            try:
                deadline=time.monotonic()+15
                while time.monotonic()<deadline:
                    blocked=self.db.execute("SELECT cardinality(pg_blocking_pids(pid)) AS n FROM pg_stat_activity "
                        "WHERE application_name=%s",(self.master+':source-review',)).fetchone()
                    if blocked and blocked['n']>0:break
                    time.sleep(.01)
                else:raise AssertionError('Review did not wait for native source replacement lock')
            finally:release.set()
            self.assertEqual(replacement.result(timeout=20)[0]['status'],'replaced')
            self.assertEqual(reviewing.result(timeout=20)['status'],'rejected')
        for table in ('entities','entity_fts','entity_surfaces','reviews'):
            self.assertEqual(self.db.execute('SELECT count(*) AS n FROM '+table).fetchone()['n'],0)

    def assert_index(self,query,args,index):
        with self.store.db.transaction():
            self.store.db.execute('SET LOCAL enable_seqscan=off')
            plan=self.store.db.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query,args).fetchone()['QUERY PLAN']
        self.assertIn(index,json.dumps(plan))

    def cleanup_database(self):
        _REPOS.pop(id(self.db),None)
        self.graph.close()
        with psycopg.connect(self.database_url,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.graph.schema)))

if __name__=='__main__':unittest.main()
