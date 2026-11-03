"""Isolated PostgreSQL revision fixture; requires explicit KG_TEST_DATABASE_URL."""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from postgres_snapshot_graph import GraphPostgresCase, FakeEmbedding, fixture, member_result, pairs
from kg_builder.graph_validation import validate_graph
from kg_builder.incremental_update import prepare_incremental_snapshot
from kg_builder.snapshot_graph import SnapshotGraph, canonical_hash


class RevisionPostgresTests(GraphPostgresCase):
    def test_base_delta_resume_reuses_area_and_publishes_effective_ids(self):
        class Publisher:
            def __init__(self): self.events=[]
            def publish(self, run_id, event): self.events.append((run_id,event))

        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); base_file=root/'base.json';delta_file=root/'delta.json';merged_file=root/'merged.json'
            fixture(base_file,n=2)
            delta=fixture(delta_file,n=3)
            delta['papers']=delta['papers'][2:]
            delta['extractions']=delta['extractions'][2:]
            delta['vectors']=[v for v in delta['vectors'] if v['item_key'].startswith('paper:3::')]
            for vector in delta['vectors']:
                if vector['kind']=='ResearchProblem':vector['vector']=[-1.0,0.0]
            delta['snapshot_hash']=canonical_hash({k:v for k,v in delta.items() if k!='snapshot_hash'})
            delta_file.write_text(json.dumps(delta),encoding='utf-8')
            merged=prepare_incremental_snapshot(base_file,delta_file,merged_file)
            publisher=Publisher()
            review_calls=[];pair_calls=[]
            def review(items,client,**kwargs):
                review_calls.extend(items)
                return [member_result(item) for item in items]
            def judge(items,client,**kwargs):
                pair_calls.extend(items)
                return pairs(items,client,**kwargs)
            with patch('kg_builder.decision_api.judge_pairs',side_effect=judge), \
                 patch('kg_builder.decision_api.review_group_members',side_effect=review):
                base=SnapshotGraph(base_file,root/'base-job',
                                   client=object(),k=2,model=FakeEmbedding())
                self.assertEqual(base.run()['phase'],'grouped')
                while tasks:=base.claim('fixture'):
                    base.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                                  'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                                  'name':'Memory area','description':'Memory research'}} for task in tasks])
                self.assertEqual(base.run()['phase'],'complete')
                self.assertEqual(base.status()['index_status'],'verified')
                old_areas={r['area_id'] for r in base.db.execute('SELECT area_id FROM research_areas')}
                old_methods={r['entity_id'] for r in base.db.execute("SELECT entity_id FROM entities WHERE kind='Method'")}
                old_pair_ids={p['pair_id'] for p in pair_calls}
                base_pair_count=len(pair_calls);old_reviews=len(review_calls)
                child=SnapshotGraph(merged_file,root/'child-job',
                                    base_run_id=base.run_id,client=object(),model=FakeEmbedding(),
                                    publisher=publisher)
                self.assertEqual(child.k,base.k)
                state=child.run()
                self.assertEqual(state['phase'],'complete')
                self.assertEqual(state['index_status'],'verified')
                self.assertEqual({r['area_id'] for r in child.db.execute('SELECT area_id FROM research_areas')},old_areas)
                self.assertTrue(child.db.execute("SELECT 1 FROM tasks WHERE status='done' LIMIT 1").fetchone())
                self.assertFalse(old_pair_ids.intersection(p['pair_id'] for p in pair_calls[base_pair_count:]))
                self.assertFalse([item for item in review_calls[old_reviews:] if item['kind']=='ResearchArea'])
                self.assertEqual(child.claim('fixture'),[])
                ids=json.loads(child._meta('revision_node_ids'))
                self.assertTrue(old_methods <= {r['entity_id'] for r in child.db.execute("SELECT entity_id FROM entities WHERE kind='Method'")})
                self.assertEqual(child._meta('delta_snapshot_hash'),delta['snapshot_hash'])
                self.assertEqual(child._meta('base_snapshot_hash'),merged['incremental']['base_snapshot_hash'])
                self.assertTrue(validate_graph(merged_file,root/'child-job',model=FakeEmbedding(),
                                               run_id=child.run_id,database_url=self.database_url)['structural']['passed'])
                group_events=[event for _,event in publisher.events if event['event_id'].startswith('group:')]
                self.assertTrue(group_events)
                published={node['id'] for event in group_events for node in event['nodes']}
                self.assertTrue(old_areas <= published)
                self.assertTrue(old_methods <= published)
                self.assertTrue(all(mapped in published for mapped in ids.values()
                                    if mapped.startswith('area:')))
                child.close()
                resumed=SnapshotGraph(merged_file,root/'child-job',run_id=child.run_id,
                                      base_run_id=base.run_id,client=object(),model=FakeEmbedding(),
                                      publisher=publisher,read_input=False)
                events=len(publisher.events)
                self.assertEqual(resumed.run()['phase'],'complete')
                self.assertEqual(len(publisher.events),events)
                resumed.close()

                # A second revision replaces one area member. Its group hash
                # changes, while unchanged paper:1 provides one-to-one lineage.
                fourth_file=root/'fourth.json';second_file=root/'second.json'
                fourth=fixture(fourth_file,n=4)
                fourth['papers']=fourth['papers'][3:]
                fourth['extractions']=fourth['extractions'][3:]
                fourth['vectors']=[v for v in fourth['vectors'] if v['item_key'].startswith('paper:4::')]
                fourth['snapshot_hash']=canonical_hash({k:v for k,v in fourth.items() if k!='snapshot_hash'})
                fourth_file.write_text(json.dumps(fourth),encoding='utf-8')
                prepare_incremental_snapshot(merged_file,fourth_file,second_file,
                                             remove_paper_ids=['paper:2'])
                second=SnapshotGraph(second_file,root/'second-job',
                                     base_run_id=child.run_id,client=object(),model=FakeEmbedding(),
                                     publisher=publisher)
                self.assertEqual(second.run()['phase'],'grouped')
                while tasks:=second.claim('fixture'):
                    second.submit([{'task_id':task['task_id'],'lease_token':task['lease_token'],
                                    'input_hash':task['input_hash'],'result':{'group_id':task['group_id'],
                                    'name':'Updated memory area','description':'Updated memory research'}}
                                   for task in tasks])
                self.assertEqual(second.run()['phase'],'complete')
                second_ids=json.loads(second._meta('revision_node_ids'))
                self.assertIn(next(iter(old_areas)),second_ids.values())
                self.assertEqual({r['area_id'] for r in second.db.execute('SELECT area_id FROM research_areas')},old_areas)
                self.assertTrue(validate_graph(second_file,root/'second-job',model=FakeEmbedding(),
                                               run_id=second.run_id,database_url=self.database_url)['structural']['passed'])
                second_events=[event for run_id,event in publisher.events
                               if run_id==second._meta('neo4j_run_id') and event['event_id'].startswith('group:')]
                self.assertTrue(old_areas <= {node['id'] for event in second_events for node in event['nodes']})
                second.close();child.close();base.close()
