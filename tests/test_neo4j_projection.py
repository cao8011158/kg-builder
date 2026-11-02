"""Offline official Neo4j driver adapter atomicity and identity contract."""
import copy
import unittest
from kg_builder.neo4j_store import Neo4jStore, node, edge

class Result:
    def __init__(self, record=None):self.record=record
    def consume(self):return None
    def single(self):return self.record

class FakeDriver:
    def __init__(self):self.nodes={};self.edges=set();self.events={};self.queries=[];self.fail=False
    def session(self, **kwargs):return self
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def execute_write(self,fn,*args):
        before=copy.deepcopy((self.nodes,self.edges,self.events))
        try:
            result=fn(self,*args)
            if self.fail:raise OSError('unavailable')
            return result
        except BaseException:
            self.nodes,self.edges,self.events=before
            raise
    def run(self, query, **p):
        self.queries.append((query,p))
        if query.startswith('CREATE CONSTRAINT'):return Result()
        run=p['run_id']
        if 'KGEvent' in query:
            key=(run,p['event_id']);self.events.setdefault(key,p['payload_hash'])
            return Result({'payload_hash':self.events[key]})
        if query.startswith('MERGE (n:KGNode'):
            key=(run,p['node_id']);self.nodes.setdefault(key,{}).update(p['properties']);return Result()
        a,b=(run,p['source']),(run,p['target'])
        if a not in self.nodes or b not in self.nodes:return Result({'written':0})
        relation=query.split('[r:')[1].split(']')[0]
        self.edges.add((a,relation,b));return Result({'written':1})
    def close(self):pass

class DriverProjectionTests(unittest.TestCase):
    def setUp(self):
        self.driver=FakeDriver()
        self.publisher=Neo4jStore(self.driver)

    def test_driver_idempotence_namespace_payload_conflict_and_rollback(self):
        payload={'event_id':'test:1','nodes':[node('a','Paper',title='A'),node('b','PaperVersion')],'edges':[edge('a','b','HAS_VERSION')]}
        self.publisher.publish('one',payload);self.publisher.publish('one',payload)
        self.publisher.publish('two',payload)
        self.assertEqual(len(self.driver.edges),2)
        changed=copy.deepcopy(payload);changed['nodes'][0]['properties']['title']='changed'
        with self.assertRaisesRegex(ValueError,'payload changed'):self.publisher.publish('one',changed)
        bad={'event_id':'test:bad','nodes':[node('c','Paper')],'edges':[edge('c','absent','HAS_VERSION')]}
        with self.assertRaisesRegex(ValueError,'endpoint'):self.publisher.publish('one',bad)
        self.assertNotIn(('one','c'),self.driver.nodes)
        self.assertNotIn(('one','test:bad'),self.driver.events)
        for q,p in self.driver.queries:
            if not q.startswith('CREATE'):self.assertIn('run_id',p)
        with self.assertRaisesRegex(ValueError,'label'):self.publisher.publish('one',{'event_id':'bad','nodes':[node('x','Bad` label')],'edges':[]})
