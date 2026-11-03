# Colab native data bundle — development stage

## Scope and baseline

- Base: `29f4f4907295caeabf2cc972fc417dc5df6fea98` (`codex/paper-dense-code`).
- Worktree: `.worktrees/colab-data-bundle`; branch `codex/colab-data-bundle`.
- This stage implements native PostgreSQL backup, published Neo4j graph export and restore, plus packaging of existing six-kind BM25 and persistent HNSW indexes. It does not build the indexes or install the Colab services.
- Source PostgreSQL database: `kg_graph_full_20261103_990ffcd6` at `127.0.0.1:55432`, user `kg_preparation`; source credentials only from `KG_PG_APP_PASSWORD`. Source Neo4j uses `NEO4J_*` process variables.

## Data contract

- `pg_dump -Fc --no-owner --no-privileges` creates a fresh native backup; `pg_restore --list` checks the archive. The PostgreSQL graph must be in `complete` phase and retain the same graph namespace and snapshot hash after export.
- Neo4j exports only nodes with `KGNode` plus one of the nine business labels and edges among those nodes of the five published relation types. The source export uses one read transaction and ordered JSONL streams. `KGEvent` publication audit nodes are intentionally excluded from the query graph; PostgreSQL `graph_outbox` remains in the native backup.
- The six searchable kinds must have matching BM25, persistent dense, and published Neo4j counts. The bundle manifest pins run ID, PostgreSQL schema/database, graph namespace, snapshot hash, model IDs, counts, and every file's SHA-256 and byte count. Export produces a directory and a ZIP without credentials, weights, or model calls.
- Restore requires a separately created empty PostgreSQL database and an empty dedicated Neo4j database. It does not stop local services, erase any graph, or initialize a source database. The Neo4j restore checks labels/types and endpoint identities and then recomputes graph counts and file fingerprints.

## CLI

`python scripts/colab_data_bundle.py export --run-id full-graph-20261103-990ffcd6 --target data/full-graph-20261103/colab-bundle-v1 --bm25-dir data/full-graph-20261103/bm25-v1 --dense-dir data/full-graph-20261103/hnsw-v1`

`python scripts/colab_data_bundle.py verify data/full-graph-20261103/colab-bundle-v1`

`python scripts/colab_data_bundle.py extract /content/drive/MyDrive/colab-bundle-v1.zip /content/kg-bundle`

`python scripts/colab_data_bundle.py restore-postgres /content/kg-bundle`

`python scripts/colab_data_bundle.py restore-neo4j /content/kg-bundle`

For restore, set `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` for the newly created database and `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and optionally `NEO4J_DATABASE` for the empty Neo4j store. Set values through Colab runtime secrets or environment, never CLI arguments. Provision PostgreSQL 16 and Neo4j Community 5.26.32 separately before restore. Neo4j MCP deployment requires compatible APOC installation and belongs to the runtime integration stage.

## Validation

- Syntax compilation: passed using repository's Python environment.
- Targeted synthetic bundle integrity/safety tests: `4 passed in 0.89s` (repository Python, isolated pytest temp directory). Cases cover intact bundle, file tampering, manifest path escape, existing export target, and missing restore credentials.
- Fresh full source export, isolated native restore drill, Drive transfer, Colab service bootstrap: pending integration and independent review.
- Locality: export and restore are explicitly full-data operations. Targeted graph reads after restore use the existing `(run_id,node_id)` identity constraint; no query retrieval path is modified here.

## Known operational limits

- PostgreSQL and Neo4j snapshots are taken separately. The complete graph is expected to be frozen; phase and identity checks detect a changed source identity, while published graph count comparison detects index drift. A full native restore drill is still required to establish operational fidelity on the real corpus.
- The package ZIP uses no compression because HNSW binary and native dump are already encoded; ZIP64 supports large files.
