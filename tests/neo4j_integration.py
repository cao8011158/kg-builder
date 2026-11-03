"""Opt-in official Bolt integration; use only the fresh disposable test server."""
import json
import os
import unittest
import uuid
from kg_builder.neo4j_store import Neo4jStore,node,edge,READ_NODE_QUERY

@unittest.skipUnless(os.getenv('NEO4J_TEST_URI'),'NEO4J_TEST_URI not provided')
class Neo4jIntegration(unittest.TestCase):
    def test_bolt_incremental_replay_rollback_namespace_and_index_locality(self):
        from neo4j import GraphDatabase
        driver=GraphDatabase.driver(os.environ['NEO4J_TEST_URI'],auth=(os.environ['NEO4J_TEST_USERNAME'],os.environ['NEO4J_TEST_PASSWORD']))
        store=Neo4jStore(driver,database=os.getenv('NEO4J_TEST_DATABASE','neo4j'))
        run='integration:'+uuid.uuid4().hex;other=run+':unrelated'
        payload={'event_id':'approved:paper','nodes':[node('paper','Paper',title='Approved'),node('version','PaperVersion',source_url='https://example.org/v1')],'edges':[edge('paper','version','HAS_VERSION')]}
        evidence=[]
        try:
            store.publish(run,payload);store.publish(run,payload)
            self.assertEqual(store.get_node(run,'paper')['properties']['title'],'Approved')
            self.assertIsNone(store.get_node(other,'paper'))
            query='PROFILE '+READ_NODE_QUERY
            def profile():
                with driver.session(database=store.database) as session:
                    result=session.run(query,run_id=run,node_id='paper');rows=list(result);summary=result.consume()
                    self.assertEqual(len(rows),1)
                    def flatten(p):return [{'operator':p['operatorType'],'rows':p.get('rows'),'db_hits':p.get('dbHits')},*[v for c in p.get('children',[]) for v in flatten(c)]]
                    operators=flatten(summary.profile)
                    self.assertTrue(any('Index' in o['operator'] for o in operators),operators)
                    self.assertFalse(any('Scan' in o['operator'] for o in operators),operators)
                    evidence.append(operators)
            profile();profile()
            unrelated={'event_id':'unrelated:1000','nodes':[node('unrelated:'+str(i),'Paper',title='x'*1000) for i in range(1000)],'edges':[]}
            store.publish(other,unrelated);profile()
            self.assertEqual(sum(o['db_hits'] or 0 for o in evidence[0]),sum(o['db_hits'] or 0 for o in evidence[-1]))
            invalid={'event_id':'bad:missing','nodes':[node('rolled-back','Paper')],'edges':[edge('rolled-back','missing','HAS_VERSION')]}
            with self.assertRaisesRegex(ValueError,'endpoint'):store.publish(run,invalid)
            self.assertIsNone(store.get_node(run,'rolled-back'))
            changed={**payload,'nodes':[node('paper','Paper',title='Forbidden')]}
            with self.assertRaisesRegex(ValueError,'payload changed'):store.publish(run,changed)
            self.assertEqual(store.get_node(run,'paper')['properties']['title'],'Approved')
            self.assertIsNotNone(store.get_node(other,'unrelated:0'))
            with driver.session(database=store.database) as session:
                count=session.run('MATCH (a:KGNode {run_id:$run_id,node_id:$node_id})-[r:HAS_VERSION]->(b:KGNode {run_id:$run_id}) RETURN count(r) AS n',run_id=run,node_id='paper').single()['n']
                self.assertEqual(count,1)
            print('NEO4J_PROFILE_EVIDENCE '+json.dumps(evidence))
        finally:
            # Exact disposable namespaces only; never global deletion.
            with driver.session(database=store.database) as session:
                for namespace in (run,other):
                    session.run('MATCH (n:KGNode {run_id:$run_id}) DETACH DELETE n',run_id=namespace).consume()
                    session.run('MATCH (e:KGEvent {run_id:$run_id}) DELETE e',run_id=namespace).consume()
            driver.close()

if __name__=='__main__':unittest.main()
