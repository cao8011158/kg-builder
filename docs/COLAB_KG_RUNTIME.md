# Restore the native knowledge graph in a Colab runtime

Run the cells in order on a fresh Ubuntu Colab VM. The data ZIP lives in Drive; PostgreSQL, Neo4j, BM25, and HNSW run from local `/content` storage. The restored services listen on loopback only. A Colab reset removes the local databases, so repeat the restore after a reset. This procedure makes no embedding, reranker, or LLM calls.

The bundle must have been produced by `scripts/colab_data_bundle.py export` after the six-kind HNSW build. It contains a fresh PostgreSQL custom-format backup, published Neo4j graph JSONL, both indexes, and SHA-256 metadata. The runtime code comes from the reviewed `codex/colab-kg` and `codex/kg-harness` branches after they are published, not from the data ZIP. For repeatable runs, set the two optional commit pins below.

## 1. Install runtime dependencies and get code

```python
from google.colab import drive
drive.mount('/content/drive')
```

```bash
%%bash
set -euo pipefail
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl ca-certificates gnupg postgresql-common openjdk-21-jre-headless
/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-16
test -x /usr/lib/postgresql/16/bin/initdb
java -version
git clone --branch codex/colab-kg --single-branch https://github.com/cao8011158/kg-builder.git /content/kg-builder
git clone --branch codex/kg-harness --single-branch https://github.com/cao8011158/research-agent.git /content/research-agent
```

```python
import os, subprocess
# Optional: pin the reviewed published integration commit in the environment.
KG_RUNTIME_REF = os.environ.get('KG_RUNTIME_REF')
if KG_RUNTIME_REF:
    assert len(KG_RUNTIME_REF) == 40 and all(c in '0123456789abcdef' for c in KG_RUNTIME_REF)
    subprocess.run(['git', '-C', '/content/kg-builder', 'checkout', '--detach', KG_RUNTIME_REF], check=True)
RA_RUNTIME_REF = os.environ.get('RA_RUNTIME_REF')
if RA_RUNTIME_REF:
    assert len(RA_RUNTIME_REF) == 40 and all(c in '0123456789abcdef' for c in RA_RUNTIME_REF)
    subprocess.run(['git', '-C', '/content/research-agent', 'checkout', '--detach', RA_RUNTIME_REF], check=True)
assert os.path.isfile('/content/kg-builder/scripts/colab_data_bundle.py')
print('Runtime commit:', subprocess.check_output(['git', '-C', '/content/kg-builder',
                                                   'rev-parse', 'HEAD'], text=True).strip())
print('Harness commit:', subprocess.check_output(['git', '-C', '/content/research-agent',
                                                   'rev-parse', 'HEAD'], text=True).strip())
subprocess.run(['python', '-m', 'pip', 'install', '-q', '-e', '/content/kg-builder[neo4j,retrieval,voyage]'], check=True)
subprocess.run(['python', '-m', 'pip', 'install', '-q', '-e', '/content/research-agent[harness]',
                'neo4j-mcp-server==1.6.1', 'mcp>=1.30,<2'], check=True)
os.environ['PYTHONPATH'] = '/content/kg-builder/src'
assert os.path.isfile('/content/research-agent/src/research_agent/mcp_tools.py')
```

The PostgreSQL apt repository command follows the [PostgreSQL Ubuntu instructions](https://www.postgresql.org/download/linux/ubuntu/). Neo4j 5.26 supports Java 21 according to its [system requirements](https://neo4j.com/docs/operations-manual/5/installation/requirements/).

## 2. Verify and extract the Drive bundle

```python
from pathlib import Path
import json, subprocess, os
KG_BUNDLE_ZIP = Path('/content/drive/MyDrive/research-agent-colab-kg/colab-bundle-v1.zip')
assert KG_BUNDLE_ZIP.is_file(), KG_BUNDLE_ZIP
KG_BUNDLE = Path('/content/kg-bundle')
subprocess.run(['python', '/content/kg-builder/scripts/colab_data_bundle.py',
                'extract', str(KG_BUNDLE_ZIP), str(KG_BUNDLE)], check=True,
               env=os.environ.copy(), stdout=subprocess.DEVNULL)
manifest = json.loads((KG_BUNDLE / 'manifest.json').read_text())
assert manifest['format'] == 'kg-colab-native-bundle/1'
assert manifest['phase'] == 'complete'
assert manifest['run_id'] == 'full-graph-20261103-990ffcd6'
assert manifest['graph_namespace'] == 'graph:a9a3ad6409e5409baa97f84394656936'
assert manifest['snapshot_hash'] == '3f259e18bc2ce2285abb8a5d734ae11a9d5b6c9ed4606071c367a4a0fdbce9fc'
print('Bundle verified:', manifest['run_id'], manifest['snapshot_hash'],
      manifest['bm25_counts'], manifest['dense_counts'])
```

`extract` refuses an existing target and checks every bundled file, including the three nested index manifests. If interrupted, use a fresh target path after removing only the incomplete `/content/kg-bundle` created by this cell.

## 3. Start an isolated PostgreSQL 16 cluster

```bash
%%bash
set -euo pipefail
install -d -m 0755 -o postgres -g postgres /content/kg-runtime
install -d -m 0700 -o postgres -g postgres /content/kg-runtime/pgdata
install -d -m 0770 -o postgres -g postgres /content/kg-runtime/pgsocket
runuser -u postgres -- /usr/lib/postgresql/16/bin/initdb -D /content/kg-runtime/pgdata --auth-local=trust --auth-host=scram-sha-256 --no-instructions >/dev/null
runuser -u postgres -- /usr/lib/postgresql/16/bin/pg_ctl -D /content/kg-runtime/pgdata \
  -o '-h 127.0.0.1 -p 55432 -k /content/kg-runtime/pgsocket -c shared_buffers=256MB' \
  -l /content/kg-runtime/postgres.log -w start >/dev/null
```

```python
import os, secrets, psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
os.environ['PGHOST'] = '127.0.0.1'
os.environ['PGPORT'] = '55432'
os.environ['PGDATABASE'] = 'kg_colab'
os.environ['PGUSER'] = 'kg_colab'
os.environ['PGPASSWORD'] = secrets.token_urlsafe(32)
os.environ['KG_GRAPH_DATABASE_URL'] = make_conninfo(
    host=os.environ['PGHOST'], port=os.environ['PGPORT'],
    dbname=os.environ['PGDATABASE'], user=os.environ['PGUSER'],
    password=os.environ['PGPASSWORD'])
with psycopg.connect(host='/content/kg-runtime/pgsocket', port=55432,
                     dbname='postgres', user='postgres', autocommit=True) as conn:
    conn.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(
        sql.Identifier('kg_colab'), sql.Literal(os.environ['PGPASSWORD'])))
    conn.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(
        sql.Identifier('kg_colab'), sql.Identifier('kg_colab')))
```

The role password stays in the notebook process environment; it is not passed to a shell command, stored in the bundle, or printed. `initdb` runs as the `postgres` OS user because it refuses to run as root. The cluster has its own data directory and port.

```python
import subprocess, os
subprocess.run(['python', '/content/kg-builder/scripts/colab_data_bundle.py',
                'restore-postgres', str(KG_BUNDLE)], check=True,
               env=os.environ.copy(), stdout=subprocess.DEVNULL)
with psycopg.connect(host='127.0.0.1', port=55432, dbname='kg_colab',
                     user='kg_colab', password=os.environ['PGPASSWORD']) as conn:
    row = conn.execute(sql.SQL('SELECT key,value FROM {}.meta WHERE key IN (%s,%s)').format(
        sql.Identifier(manifest['source_schema'])),
        ('neo4j_run_id', 'snapshot_hash')).fetchall()
assert dict(row) == {'neo4j_run_id': manifest['graph_namespace'],
                     'snapshot_hash': manifest['snapshot_hash']}
print('PostgreSQL restored and identity verified')
```

## 4. Start Neo4j Community 5.26.32 with bundled APOC Core

```bash
%%bash
set -euo pipefail
curl -fsSL https://dist.neo4j.org/neo4j-community-5.26.32-unix.tar.gz \
  -o /content/kg-runtime/neo4j-community-5.26.32-unix.tar.gz
tar -xzf /content/kg-runtime/neo4j-community-5.26.32-unix.tar.gz -C /content/kg-runtime
NEO_HOME=/content/kg-runtime/neo4j-community-5.26.32
test -x "$NEO_HOME/bin/neo4j"
cp "$NEO_HOME"/labs/apoc-*-core.jar "$NEO_HOME/plugins/"
cat >> "$NEO_HOME/conf/neo4j.conf" <<'CONF'
server.default_listen_address=127.0.0.1
server.bolt.listen_address=:17687
server.http.enabled=false
server.memory.heap.initial_size=2G
server.memory.heap.max_size=2G
server.memory.pagecache.size=1G
dbms.security.procedures.unrestricted=apoc.meta.*
CONF
useradd --system --home-dir "$NEO_HOME" --shell /usr/sbin/nologin kgneo
chown -R kgneo:kgneo "$NEO_HOME"
runuser -u kgneo -- "$NEO_HOME/bin/neo4j" start >/dev/null
```

The pinned tarball URL is used by the [official Neo4j 5.26.32 container build](https://hub.docker.com/layers/library/neo4j/5.26.32/images/sha256-ecc8dd9c875bb77aa57ac50bdcd4145720eb0f118255d00b04122b7f2db692ca). The [Neo4j tarball guide](https://neo4j.com/docs/operations-manual/5/installation/linux/tarball/) describes service startup; [APOC Core installation](https://neo4j.com/docs/apoc/current/installation/) uses the matching jar bundled in `labs`. The [APOC meta schema procedure](https://neo4j.com/docs/apoc/current/overview/apoc.meta/apoc.meta.schema/) requires the narrow `apoc.meta.*` unrestriction on a self-hosted service. The default procedure allowlist remains in effect, so the procedure is loaded without broadening other extensions. Heap and page cache are bounded to leave most Colab RAM available to models.

```python
import os, secrets, time
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
os.environ['NEO4J_URI'] = 'bolt://127.0.0.1:17687'
os.environ['NEO4J_USERNAME'] = 'neo4j'
os.environ['NEO4J_PASSWORD'] = secrets.token_urlsafe(32)
os.environ['NEO4J_DATABASE'] = 'neo4j'
for attempt in range(60):
    try:
        with GraphDatabase.driver(os.environ['NEO4J_URI'], auth=('neo4j', 'neo4j')) as driver:
            with driver.session(database='system') as session:
                session.run('ALTER CURRENT USER SET PASSWORD FROM $old TO $new',
                            old='neo4j', new=os.environ['NEO4J_PASSWORD']).consume()
        break
    except (OSError, ServiceUnavailable):
        if attempt == 59:
            raise
        time.sleep(2)
with GraphDatabase.driver(os.environ['NEO4J_URI'],
                          auth=('neo4j', os.environ['NEO4J_PASSWORD'])) as driver:
    driver.verify_connectivity()
    with driver.session(database='neo4j') as session:
        assert session.run('MATCH (n) RETURN count(n) AS count').single()['count'] == 0
        print('APOC Core:', session.run('RETURN apoc.version() AS version').single()['version'])
```

The first login changes Neo4j's factory password with a parameterized Cypher query. No password enters a command argument or config file. Neo4j documents [changing the current user's password with parameters](https://neo4j.com/docs/operations-manual/current/authentication-authorization/manage-users/).

```python
import subprocess, os
subprocess.run(['python', '/content/kg-builder/scripts/colab_data_bundle.py',
                'restore-neo4j', str(KG_BUNDLE)], check=True,
               env=os.environ.copy(), stdout=subprocess.DEVNULL)
print('Neo4j restored; counts and graph fingerprint verified')
```

After restore, query the required APOC meta component against the actual graph. A successful `apoc.version()` call alone does not establish MCP readiness.

```python
from neo4j import GraphDatabase
with GraphDatabase.driver(os.environ['NEO4J_URI'],
                          auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])) as driver:
    with driver.session(database=os.environ['NEO4J_DATABASE']) as session:
        procedures = {row['name'] for row in session.run(
            "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.meta' RETURN name")}
        assert 'apoc.meta.schema' in procedures
        topology = session.run('CALL apoc.meta.schema({sample: 100}) YIELD value RETURN value').single()['value']
        assert 'Paper' in topology and 'ResearchProblem' in topology
        count = session.run('MATCH (n:KGNode {run_id:$ns}) RETURN count(n) AS count',
                            ns=manifest['graph_namespace']).single()['count']
        assert count == sum(manifest['graph']['node_counts'].values())
print('APOC meta and restored graph ready')
```

## 5. Verify both search indexes load

```python
from kg_builder.bm25_retrieval import BM25Index
from kg_builder.persistent_dense import PersistentDenseIndex, verify_source
with BM25Index(KG_BUNDLE / 'bm25', expected_run_id=manifest['run_id']) as index:
    assert index.manifest['counts'] == manifest['bm25_counts']
index = PersistentDenseIndex(KG_BUNDLE / 'dense', expected_run_id=manifest['run_id'],
                             expected_graph_namespace=manifest['graph_namespace'],
                             expected_model_id=manifest['dense_model_id'])
assert index.metadata['kind_counts'] == manifest['dense_counts']
source_check = verify_source(manifest['run_id'], KG_BUNDLE / 'dense')
assert source_check['source_fingerprint'] == index.metadata['source_fingerprint']
print('BM25 and persistent HNSW loaded; all six-kind counts match')
```

## 6. Verify the official Neo4j MCP server handshake and read tools

The reviewed research-agent harness is installed from `codex/kg-harness` above. Its `Neo4jMCPTools` adapter starts the pinned PyPI `neo4j-mcp-server` console entry point over stdio. The separate manually installed binary uses the name `neo4j-mcp`; this recipe uses the PyPI entry point. The adapter enforces `NEO4J_MCP_READ_ONLY=true` and `NEO4J_MCP_TOOLS=get-schema,read-cypher`, then checks that only those two tools are advertised. [Neo4j MCP requirement checks](https://neo4j.com/docs/mcp/current/requirement-checks/) include APOC meta; [Neo4j MCP tools](https://neo4j.com/docs/mcp/current/tools/) document the read-only filters.

```python
import asyncio, os, shutil
from research_agent.mcp_tools import Neo4jMCPTools
assert shutil.which('neo4j-mcp-server')
for suffix in ('URI', 'USERNAME', 'PASSWORD', 'DATABASE'):
    os.environ['NEO4J_MCP_' + suffix] = os.environ['NEO4J_' + suffix]
os.environ['NEO4J_MCP_READ_ONLY'] = 'true'
os.environ['NEO4J_MCP_TOOLS'] = 'get-schema,read-cypher'

async def verify_mcp():
    async with Neo4jMCPTools() as tools:
        assert {item['function']['name'] for item in tools.tools} == {
            'neo4j_get_schema', 'neo4j_read_cypher'}
        schema = await tools.call('neo4j_get_schema', {})
        assert not schema['is_error'] and (schema['structured_content'] or schema['content'])
        rows = await tools.call('neo4j_read_cypher', {
            'query': 'MATCH (n:KGNode {run_id:$ns}) RETURN count(n) AS count',
            'params': {'ns': manifest['graph_namespace']}})
        assert not rows['is_error'] and (rows['structured_content'] or rows['content'])
    print('Official Neo4j MCP handshake, schema, and read query passed')

await verify_mcp()
```

Keep the two database services alive for the entire Colab session. Re-run this procedure after Colab resets the VM; the Drive ZIP is the durable artifact.
