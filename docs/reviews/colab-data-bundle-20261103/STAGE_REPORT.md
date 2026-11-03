# Colab native data bundle — development stage

Current status: the earlier native-bundle implementation passed formal review round **3/3** at `1c21c421352e64dc8ec2ee111423debd6fe77a31`. The later PostgreSQL-free query-bundle scope correction below is a local development candidate awaiting its separately authorized independent review. BUNDLE-01/02/03 remain closed. Native recovery, Colab execution and Drive migration acceptance remain pending. Detailed prior review: [REVIEW.md](REVIEW.md).

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

## Independent review stage — round 3/3 — 2026-11-04

Reviewed final complete batch `29f4f4907295caeabf2cc972fc417dc5df6fea98..1c21c421352e64dc8ec2ee111423debd6fe77a31` (five files, 848 additions), including repair `719880e7fbbb1968772c5d1279681ef23e62e767` and root's final integration wiring. Local fixed-commit review; remote remains unverified.

- **BUNDLE-01/02 remain closed:** runtime code unchanged since round 2 and all final nested-manifest/retry regressions pass.
- **BUNDLE-03 closed:** APOC meta unrestriction precedes first start; post-restore real meta query and graph count assertions precede official MCP initialization/schema/read verification. Correct pinned PyPI server/SDK, entry point, connection variables and two-tool read-only discovery are documented. Restored PostgreSQL conninfo and persistent-index `verify_source` connect the search artifact to the restored native data.
- Independent final target tests **11 passed in 9.05s**, exit 0; patch sanity passed. Exact Git diff proves `src`, `scripts` and `pyproject.toml` unchanged from round 2. Thus ordinary suite was not repeated for these production-document/test-only changes: applicable independent round-2 evidence remains **128 tests / 126 passed / 1 skipped / 1 known SQLite cleanup WinError 32, exit 1 — not green**. No claim that ordinary regression ran at the final SHA.
- Result **implementation PASS**, completed formal counter **3/3**, no confirmed blocking finding. No actual native restore/Colab install/MCP handshake/Drive transfer/source write/model call/push/merge performed by reviewer.
- Next: root communicates this stage, obtains accepted real HNSW artifact, and executes the previously authorized fresh bundle and isolated native recovery/cardinality/property/fingerprint/source-index acceptance before Drive migration. This source approval does not itself establish native/full workflow PASS.

## Repair after independent review round 2

- Fixed BUNDLE-03 in `docs/COLAB_KG_RUNTIME.md`: fresh Neo4j config now unrestricts only `apoc.meta.*` before startup. After restoring the published graph, the notebook checks `apoc.meta.schema` presence and runs `CALL apoc.meta.schema({sample: 100})` against actual data. It then starts the official `neo4j-mcp-server==1.6.1` through the research-agent harness and MCP SDK `>=1.30,<2`, checks the exact two read tools, and calls both schema and parameterized read-query tools.
- The recipe clones the intended public `codex/colab-kg` and `codex/kg-harness` branches after their publication. It keeps `NEO4J_MCP_*` credentials in environment variables and does not use CLI password arguments.
- The document contract regression now checks config-before-start, meta-after-restore, server/SDK pins, environment/read-only flags, and the exact harness tool names. Final focused rerun: `11 passed in 13.78s`; Colab Python cells parse. The implementation module and CLI did not change in this repair, so the prior full ordinary regression and its one known Windows SQLite cleanup error remain the latest general regression evidence. Native Linux/Colab service execution remains unverified.
- Formal review counter remains **2/3**; final independent review against the new commit will be round **3/3**. No source database/service changes, full export, Drive transfer, or merge occurred here.
- Pre-final inspection aligned the Colab KG installation cell with the integrated `retrieval` and `voyage` extras (`neo4j,retrieval,voyage`). The former `bm25,semantic` combination omitted Voyage query support and pulled unrelated semantic build dependencies. This is documentation-only; it does not change the bundle artifact or production code.
- The same inspection wired the generated PostgreSQL credentials into `KG_GRAPH_DATABASE_URL` using `psycopg.conninfo.make_conninfo` in process memory, which the existing `GraphStore` and semantic APIs read. After native restore, the notebook now calls `persistent_dense.verify_source` to compare the saved HNSW source fingerprint with the restored PostgreSQL data; it does not encode or call Voyage. Final focused rerun: **11 passed in 3.68s**.

## PostgreSQL-free query-bundle scope correction — local development candidate — 2026-11-04

This is a new scope correction after the historical native-bundle review PASS, not a rewrite of rounds 1–3. Branch `codex/colab-data-bundle`, base `1c21c421352e64dc8ec2ee111423debd6fe77a31`; the correction is locally developed and has not undergone its authorized independent review at the time of this summary. The previously reviewed native v1 backup and its source directory remain intact.

- Added format `kg-colab-query-bundle/2` with `export-query` (published Neo4j plus existing BM25/dense artifacts) and `repack-query` (verified native v1 directory to v2 without graph reexport or document encoding). Both create a new immutable directory and ZIP. v1 export/verify/PostgreSQL restore remain available for local legacy recovery; the v2 notebook path requires only Neo4j, BM25 text/IDs/metadata, saved dense/HNSW data, and version/source/checksum manifests.
- Package verification hashes every file and nested manifest, checks the saved dense source fingerprint and HNSW files, and performs one full typed-ID membership pass across graph, BM25 text records, dense records, and HNSW mapping. This work runs on export/extract/verification, not on each query. BM25 text and dense `text_hash` use different builder representations; the verifier checks complete text presence and immutable artifact/source hashes without claiming their text byte strings are equal.
- Colab instructions now require full release commit SHA pins for KG and harness plus the bundle ZIP SHA-256, copy Drive ZIP to `/content`, verify its checksum, then extract and restore the Neo4j graph. The PostgreSQL server install, backup upload/restore, source verification, and PostgreSQL connection environment are absent from the query runtime. The KG Python install avoids forced core PostgreSQL dependencies with `--no-deps` and explicit query libraries. `BM25Index` moves its `GraphStore` import into the offline build operation; an import hook that raises for `psycopg` and `kg_builder.postgres_graph` confirmed the query loader imports without them. MCP smoke calls use required monotonic deadlines.
- Preserved legacy restore behavior when descriptive graph exclusion notes changed: post-restore fingerprint compares node counts, edge count, node SHA and edge SHA, and still rejects changed graph data. Synthetic regression covers both cases.
- Focused command: `PYTHONPATH=src D:/Projects/kg-builder/.venv/Scripts/python.exe -m pytest tests/test_colab_bundle.py -q --basetemp .../.test-tmp-bundle-scope-final2` → **13 passed in 16.78s, exit 0**, [raw log](raw/scope-targeted-final-20261104.txt). The real saved v1 bundle's 19,827 six-kind records passed full graph/BM25/dense/HNSW typed-ID membership verification in 2.28s, read only. Real `repack-query` completed in 16.97s, exit 0, producing `D:/Projects/kg-builder/data/full-graph-20261103/colab-query-bundle-v2` and `.zip`: **235,338,612 bytes**, SHA-256 `f76b2170b982dbcff400d05b4b7bb2550a8e50a2c70b8f60b17e2b94e56a4628`, 51 listed files, no PostgreSQL dump; [raw manifest/output](raw/scope-repack-20261104.txt). No source service was changed.
- An unrestricted `pytest -q` run entered environment-dependent tests and produced repeated failures/errors by 32%; it was interrupted rather than treated as a fast regression ([partial log](raw/scope-regression-20261104.txt)). The recorded ordinary command `python -m unittest discover -s tests -p 'test_*.py' -v` also produced early errors in BM25/legacy database tests and long expected OpenAlex retry sleeps; it was bounded and interrupted after the retry test completed ([partial log](raw/scope-ordinary-20261104.txt)). Neither run is green or a completed 128-test result. The earlier review's 126 passed / 1 skipped / 1 known Windows SQLite cleanup error is historical evidence at a previous commit, not validation of this candidate.
- No model/API calls, graph rebuild, source write, service start/stop, native restore, Drive upload, Colab execution, push or merge. The real v2 ZIP has not yet been independently restored or opened by BM25/HNSW loaders in Colab. Next: independent fixed-commit review of this scope correction, then isolated runtime acceptance and release pinning.
