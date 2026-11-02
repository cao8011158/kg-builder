"""Official-driver, idempotent projection of approved domain graph events."""
from __future__ import annotations
import hashlib
import json
import os

LABELS = frozenset(('Paper','PaperVersion','Source','Contribution','SourceItem','ResearchProblem','Method','Benchmark','ResearchArea'))
READ_NODE_QUERY = 'MATCH (n:KGNode {run_id:$run_id,node_id:$node_id}) RETURN properties(n) AS properties,labels(n) AS labels'
RELATIONS = frozenset(('HAS_VERSION','HAS_SOURCE','HAS_ITEM','RESOLVES_TO','MEMBER_OF'))

def node(node_id, label, **properties):
    return {'id':node_id,'label':label,'properties':properties}

def edge(source, target, relation):
    return {'source':source,'target':target,'type':relation}

class Neo4jStore:
    def __init__(self, driver, *, database='neo4j'):
        self.driver, self.database = driver, database
        self._ready = False

    @classmethod
    def from_env(cls):
        required = ['NEO4J_URI','NEO4J_USERNAME','NEO4J_PASSWORD']
        if any(not os.getenv(key) for key in required):
            raise ValueError('NEO4J_URI, NEO4J_USERNAME and NEO4J_PASSWORD are required')
        from neo4j import GraphDatabase
        return cls(GraphDatabase.driver(os.environ['NEO4J_URI'], auth=(os.environ['NEO4J_USERNAME'],os.environ['NEO4J_PASSWORD']),connection_timeout=10,max_transaction_retry_time=30),database=os.getenv('NEO4J_DATABASE','neo4j'))

    def close(self):
        self.driver.close()

    def get_node(self, run_id, node_id):
        """Read a single published graph node using the composite identity index."""
        with self.driver.session(database=self.database) as session:
            return session.execute_read(lambda tx: self._read_node(tx,run_id,node_id))

    @staticmethod
    def _read_node(tx, run_id, node_id):
        record=tx.run(READ_NODE_QUERY,run_id=run_id,node_id=node_id).single()
        return dict(record) if record is not None else None

    def publish(self, run_id, payload):
        # Validate before opening a transaction; labels/types never come from source text.
        for item in payload['nodes']:
            if item['label'] not in LABELS:raise ValueError('Unknown graph label')
            if 'run_id' in item['properties'] or 'node_id' in item['properties']:raise ValueError('Reserved graph identity')
        for item in payload['edges']:
            if item['type'] not in RELATIONS:raise ValueError('Unknown graph relation')
        with self.driver.session(database=self.database) as session:
            if not self._ready:
                session.execute_write(lambda tx: tx.run('CREATE CONSTRAINT kg_node_identity IF NOT EXISTS FOR (n:KGNode) REQUIRE (n.run_id,n.node_id) IS UNIQUE').consume())
                session.execute_write(lambda tx: tx.run('CREATE CONSTRAINT kg_event_identity IF NOT EXISTS FOR (e:KGEvent) REQUIRE (e.run_id,e.event_id) IS UNIQUE').consume())
                self._ready=True
            session.execute_write(self._write,run_id,payload)

    @staticmethod
    def _write(tx, run_id, payload):
        encoded=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'))
        event_hash=hashlib.sha256(encoded.encode()).hexdigest()
        record=tx.run('MERGE (e:KGEvent {run_id:$run_id,event_id:$event_id}) ON CREATE SET e.payload_hash=$payload_hash RETURN e.payload_hash AS payload_hash',run_id=run_id,event_id=payload['event_id'],payload_hash=event_hash).single()
        if record['payload_hash']!=event_hash:raise ValueError('Published event payload changed')
        for item in payload['nodes']:
            properties={k:(json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,dict) or isinstance(v,list) and any(isinstance(x,(dict,list)) for x in v) else v) for k,v in item['properties'].items()}
            tx.run('MERGE (n:KGNode {run_id:$run_id,node_id:$node_id}) SET n:'+item['label']+' SET n += $properties',run_id=run_id,node_id=item['id'],properties=properties).consume()
        for item in payload['edges']:
            result=tx.run('MATCH (a:KGNode {run_id:$run_id,node_id:$source}), (b:KGNode {run_id:$run_id,node_id:$target}) MERGE (a)-[r:'+item['type']+']->(b) RETURN count(r) AS written',run_id=run_id,source=item['source'],target=item['target']).single()
            if result['written'] != 1:raise ValueError('Projection edge endpoint missing')
