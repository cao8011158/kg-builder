# Colab native data bundle — development stage

Current status: independent formal review round **2/3 needs changes** at `061e671dadda0d4fb1f4e5ff44341f8fcc6e1666`. BUNDLE-01 and BUNDLE-02 are closed; new BUNDLE-03 is open. No real export/restore/push/merge performed. Detailed review: [REVIEW.md](REVIEW.md).

## Scope and baseline

- Base: `29f4f4907295caeabf2cc972fc417dc5df6fea98` (`codex/paper-dense-code`).
- Worktree: `.worktrees/colab-data-bundle`; branch `codex/colab-data-bundle`.
- This stage implements native PostgreSQL backup, published Neo4j graph export and restore, plus packaging of existing six-kind BM25 and persistent HNSW indexes. It does not build the indexes. The Colab setup and restore cells are in `docs/COLAB_KG_RUNTIME.md`.
- Source PostgreSQL database: `kg_graph_full_20261103_990ffcd6` at `127.0.0.1:55432`, user `kg_preparation`; source credentials only from `KG_PG_APP_PASSWORD`. Source Neo4j uses `NEO4J_*` process variables.

## Data contract

- `pg_dump -Fc --no-owner --no-privileges` creates a fresh native backup; `pg_restore --list` checks the archive. The PostgreSQL graph must be in `complete` phase and retain the same graph namespace and snapshot hash after export.
- Neo4j exports only nodes with `KGNode` plus one of the nine business labels and edges among those nodes of the five published relation types. The source export uses one read transaction and ordered JSONL streams. `KGEvent` publication audit nodes are intentionally excluded from the query graph; PostgreSQL `graph_outbox` remains in the native backup.
- The six searchable kinds must have matching BM25, persistent dense, and published Neo4j counts. The bundle manifest pins run ID, PostgreSQL schema/database, graph namespace, snapshot hash, model IDs, counts, and every file's SHA-256 and byte count. Export produces a directory and a ZIP without credentials, weights, or model calls.
- Restore requires a separately created empty PostgreSQL database and an empty dedicated Neo4j database. It does not stop local services, erase any graph, or initialize a source database. The Neo4j restore checks labels/types and endpoint identities and then recomputes graph counts and file fingerprints.

## CLI

`python scripts/colab_data_bundle.py export --run-id full-graph-20261103-990ffcd6 --target data/full-graph-20261103/colab-bundle-v1 --bm25-dir data/full-graph-20261103/bm25-v1 --dense-dir data/full-graph-20261103/hnsw-v1`

`python scripts/colab_data_bundle.py verify data/full-graph-20261103/colab-bundle-v1`

`python scripts/colab_data_bundle.py extract /content/drive/MyDrive/research-agent-colab-kg/colab-bundle-v1.zip /content/kg-bundle`

`python scripts/colab_data_bundle.py restore-postgres /content/kg-bundle`

`python scripts/colab_data_bundle.py restore-neo4j /content/kg-bundle`

For restore, set `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` for the newly created database and `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and optionally `NEO4J_DATABASE` for the empty Neo4j store. Set values through Colab runtime secrets or environment, never CLI arguments. The Colab runtime guide provisions PostgreSQL 16, Neo4j Community 5.26.32, bundled APOC Core, and the official read-only Neo4j MCP server.

## Validation

- Syntax compilation: passed using repository's Python environment.
- Initial targeted synthetic bundle integrity/safety tests: `4 passed in 0.89s` (repository Python, isolated pytest temp directory). Cases cover intact bundle, file tampering, manifest path escape, existing export target, and missing restore credentials.
- First independent review (`REVIEW.md`) found BUNDLE-01 and BUNDLE-02, both P2; formal counter is 1/3. Repair excludes only the outer bundle manifest, hashes all nested BM25/dense/HNSW manifests, and scopes Neo4j stream counters to each managed transaction attempt. ZIP extraction/tamper, full/partial retry, and Colab Python-cell syntax regressions added. Repaired targeted tests: `10 passed in 5.63s`. A sandboxed full regression stalled on network source retries and was interrupted. The ordinary-permission full regression ran 128 tests in 62.031s: 126 passed, 1 skipped, 1 error. The sole error is the same pre-existing Windows SQLite `candidate.db` temporary-directory cleanup `WinError 32` recorded in independent round 1; the full suite is **not green**.
- Added executable Colab cells for PostgreSQL 16 installation and isolated non-root cluster, Neo4j Community 5.26.32/Java 21/APOC Core installation and localhost service, environment-only generated passwords, native restore, and both index loaders. No service install occurred in this Windows development environment.
- Fresh full source export, isolated native restore drill, Drive transfer, and execution of Colab service bootstrap: pending integration and independent review.
- Locality: export and restore are explicitly full-data operations. Targeted graph reads after restore use the existing `(run_id,node_id)` identity constraint; no query retrieval path is modified here.

## Known operational limits

- PostgreSQL and Neo4j snapshots are taken separately. The complete graph is expected to be frozen; phase and identity checks detect a changed source identity, while published graph count comparison detects index drift. A full native restore drill is still required to establish operational fidelity on the real corpus.
- The package ZIP uses no compression because HNSW binary and native dump are already encoded; ZIP64 supports large files.

## Independent review stage — round 1/3 — 2026-11-03

Reviewed the entire `29f4f4907295caeabf2cc972fc417dc5df6fea98..c75e4a091d7bfa8984235217b294afcba6117cfe` four-file batch independently. Remote verification failed DNS; conclusions apply to the fixed local SHA, not a verified remote head.

- **BUNDLE-01 (P2, open):** `_files` excludes all nested `manifest.json` files. Normal ZIPs therefore omit essential BM25/dense/HNSW manifests and fail extraction verification; directory verification also accepts tampering with those manifests. Fix exclusion to apply only to the outer manifest and add full ZIP round-trip plus inner-manifest hash regressions.
- **BUNDLE-02 (P2, open):** graph counts live outside the retryable `execute_read` callback. A retry truncates JSONL but accumulates previous counts; independent reproduction yields two counted Paper nodes for one file line. Return per-attempt counts from the successful callback and cover partial-attempt retries.
- Independent targeted tests: **4 passed in 2.35s**, synthetic only. Full ordinary regression: **128 in 47.144s; 126 passed, 1 skipped, 1 existing ingestion SQLite cleanup WinError 32 error; exit 1, not green**. Sandbox failures were separately classified as environment blockage and rerun with working permissions.
- Native read-only identity/type audit succeeded: graph complete, 36,502 nodes, 38,125 relationships across all five types; current property values are JSON-compatible strings/lists. No source writes, export, restore, provider calls, push or merge.
- Native restore/full corpus/Colab service bootstrap/Drive transfer and persistent HNSW integration remain unverified. The runtime integration stage must supply Java 21, Neo4j 5.26.32 and compatible bundled APOC, and PostgreSQL 16 under a service user. Four synthetic tests are not native or full acceptance.
- Result: **needs changes**. Completed review count now **1/3**. Apply the two scoped fixes, preserve baseline-error evidence and rerun review against the repaired SHA. Raw local logs and full evidence/repair instructions are listed in [REVIEW.md](REVIEW.md).

## Independent review stage — round 2/3 — 2026-11-04

Reviewed the complete five-file batch at `061e671dadda0d4fb1f4e5ff44341f8fcc6e1666`, including new runtime service documentation, against unchanged base `29f4f4907295caeabf2cc972fc417dc5df6fea98`. Remote head remains unverified; local fixed-commit review only.

- **BUNDLE-01 closed:** only the outer manifest is excluded; BM25/dense/HNSW manifests are hashed and archived. Independent actual `export_bundle` → ZIP → `extract_zip` synthetic orchestration confirms all three manifests survive and inventories agree.
- **BUNDLE-02 closed:** counts now belong to one managed transaction attempt and only the successful callback result is used. Independent full/partial retry tests pass.
- **BUNDLE-03 P2 open:** new Colab Neo4j setup copies APOC but omits meta-procedure unrestriction and checks only `apoc.version()`. The official self-hosted APOC meta contract requires unrestriction; MCP requires the meta component. Add `dbms.security.procedures.unrestricted=apoc.meta.*`, appropriate allowlist, an actual post-restore meta query/procedure check, and the precise pinned MCP setup/owning harness instructions. This is a current-step bootstrap integration defect, not an upstream defect. Full evidence and official links are in REVIEW.md.
- Independent target tests **10 passed in 6.01s**; ordinary regression **128 in 31.236s: 126 passed, 1 skipped, 1 existing SQLite cleanup WinError 32 error, exit 1 — not green**. Python-cell AST checks and patch sanity passed. Colab services were not installed or run.
- No real export, restore, Drive transfer, source mutation, paid API call, push or merge. Real HNSW/native recovery and actual MCP/Colab acceptance remain pending after implementation approval.
- Result **needs changes**; completed count **2/3**. One final repair/review round remains. Preserve both earlier review history and baseline-error evidence.

## Repair after independent review round 2

- Fixed BUNDLE-03 in `docs/COLAB_KG_RUNTIME.md`: fresh Neo4j config now unrestricts only `apoc.meta.*` before startup. After restoring the published graph, the notebook checks `apoc.meta.schema` presence and runs `CALL apoc.meta.schema({sample: 100})` against actual data. It then starts the official `neo4j-mcp-server==1.6.1` through the research-agent harness and MCP SDK `>=1.30,<2`, checks the exact two read tools, and calls both schema and parameterized read-query tools.
- The recipe clones the intended public `codex/colab-kg` and `codex/kg-harness` branches after their publication. It keeps `NEO4J_MCP_*` credentials in environment variables and does not use CLI password arguments.
- The document contract regression now checks config-before-start, meta-after-restore, server/SDK pins, environment/read-only flags, and the exact harness tool names. Final focused rerun: `11 passed in 13.78s`; Colab Python cells parse. The implementation module and CLI did not change in this repair, so the prior full ordinary regression and its one known Windows SQLite cleanup error remain the latest general regression evidence. Native Linux/Colab service execution remains unverified.
- Formal review counter remains **2/3**; final independent review against the new commit will be round **3/3**. No source database/service changes, full export, Drive transfer, or merge occurred here.
