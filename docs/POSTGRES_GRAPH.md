# PostgreSQL graph operations

New graph jobs use native PostgreSQL through psycopg. PostgreSQL owns the frozen paper/version/extraction source rows, semantic sources and mentions, entities and research areas, review results, candidate pairs, naming leases, clustering proposals, embedding cache, timing and request audits, and durable Neo4j outbox. The official Neo4j driver remains the projection publisher. PostgreSQL commits approved graph events before delivery; interrupted acknowledgement replays the same immutable event identity.

Set `KG_GRAPH_DATABASE_URL`, or fall back to `KG_DATABASE_URL` then `DATABASE_URL`. Supply connection credentials only through the process environment. CLI options do not accept a DSN or password. PostgreSQL and Neo4j services must already be available; these commands do not provision services.

```bash
python scripts/build_snapshot_graph.py --snapshot prepared.json --job-dir graph-job --run-id graph-fresh --neo4j run
python scripts/build_snapshot_graph.py --snapshot prepared.json --job-dir graph-job status
python scripts/build_snapshot_graph.py --snapshot prepared.json --job-dir graph-job claim --worker naming-worker --output tasks.json
python scripts/build_snapshot_graph.py --snapshot prepared.json --job-dir graph-job --neo4j submit --input names.json
```

`--run-id` is optional for graph commands. A fresh directory receives a UUID identity in `graph-job.json`; later commands reuse it. An explicit identity must match an existing descriptor. Preserve this credential-free descriptor and the immutable input snapshot when moving a job. Reconnecting to the same database and identity restores PostgreSQL state; local proposal JSON is diagnostic output, not state authority. Existing SQLite graph directories cannot be resumed or migrated by these commands: use a fresh PostgreSQL job directory.

## Incremental graph publishing

Use a completed, indexed `domain-v1` graph with `whole-group-v1` review and its exact prepared snapshot as the base. Export the next approved batch through the existing preparation workflow with the same review policy and embedding model. The batch contains new or replacement records keyed by the canonical paper ID. Supply `--remove-paper-id` for each intentional removal. Use a dedicated release directory per batch.

```bash
python scripts/publish_incremental_graph.py --base-run-id graph-current --base-snapshot current/prepared.json --delta-snapshot approved-batch.json --release-dir next --prepare-only
python scripts/publish_incremental_graph.py --base-run-id graph-current --base-snapshot current/prepared.json --delta-snapshot approved-batch.json --release-dir next
```

`--prepare-only` writes the merged snapshot without opening graph services. Publishing requires PostgreSQL, Neo4j, the configured graph review and embedding providers, and the BM25 optional dependency. The revision has a deterministic PostgreSQL run ID and its own Neo4j namespace. Method and Benchmark review follows changes to source items and reuses compatible prior evidence. ResearchProblem clustering runs on the merged corpus. Unambiguous continuing entities and areas retain their IDs, and the revision records lineage for membership changes.

If the result is `waiting_for_names`, use `build_snapshot_graph.py` with the reported snapshot and job directory to claim and submit area names. Submit with `--neo4j` and the same Jev model. Run the publishing command again with identical inputs. Reruns resume the same revision, including pending outbox delivery. A conflicting batch requires its own release directory.

`next/release.json` appears after structural validation, outbox delivery and a matching BM25 build. It identifies the new graph run, Neo4j namespace and snapshot hash. Use that run ID for native semantic retrieval. Consumers can select the new release using the manifest, and the previous release remains available under its own namespace.

An external scheduler can invoke `scripts/scheduled_refresh.py` with the same arguments and `ENABLE_SCHEDULED_KG_REFRESH=true`. The entrypoint expects an already approved batch. Configure any collection, extraction, review and batch selection separately.

Graph Jev concurrency defaults to **8** for `run` and Neo4j `submit`; `--jev-concurrency` changes the shared HTTP limit and bounds in-flight pair batches. A batch contains **32** pairs by default, configured by `run --batch-size`. Naming concurrency defaults to 3. Status and claim do not construct embedding, Jev, or Neo4j API clients, including cold reopening. Neo4j publication requires `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and optionally `NEO4J_DATABASE`.

Semantic commands also identify a PostgreSQL run:

```bash
python scripts/semantic_pipeline.py --run-id semantic-reviewed ingest --source-run-id graph-fresh --extraction-id extr:SELECTED_ID
python scripts/semantic_pipeline.py --run-id graph-fresh search "agent memory" --kind ResearchProblem
python scripts/semantic_pipeline.py --run-id graph-fresh paper paper:SELECTED_ID
```

Ingest reads the selected native PostgreSQL source run and validates reviewed extraction payloads. No SQLite master or sidecar is opened. Search uses PostgreSQL full-text search with generated `tsvector` columns plus the existing semantic retrieval/ranking logic. Lexical tokenization follows PostgreSQL's `simple` configuration, rather than historical SQLite FTS5 behavior.

```bash
python scripts/validate_snapshot_graph.py --snapshot prepared.json --job-dir graph-job --output acceptance.json
python scripts/validate_snapshot_graph.py --snapshot prepared.json --job-dir graph-job --cases labeled.json --output acceptance-labeled.json
```

Validation opens one PostgreSQL `REPEATABLE READ, READ ONLY` transaction for original source pins, semantic payloads, identity/domain reviews, stored proposal partition, search index contents/vectors and outbox structure. Labeled retrieval uses the same transaction; missing query embeddings are computed transiently without inserting cache rows. Structural validation needs no API client. Labeled queries require a compatible embedding model and can invoke its configured provider. The report labels quality without labeled cases as untested. Full audits intentionally scan the complete run; they are separate from metadata-only status/claim and bounded entity/paper retrieval.

Native persistence tests are in `tests/postgres_graph_validation.py`, `tests/postgres_neo4j_projection.py`, `tests/postgres_neo4j_cli.py`, and the other `postgres*.py` suites. They require `KG_TEST_DATABASE_URL` and allocate isolated schemas. Pure CLI/driver doubles remain in ordinary `test_*.py` suites. No test requires paid APIs or a Neo4j service.

## Retired corpus batch entry point

`scripts/apply_corpus_batch.py` no longer validates or applies SQLite corpus batches. Its CLI exits with native preparation guidance, and imported `run(...)` raises `RuntimeError` before reading inputs, opening storage or writing files. This is retirement, with no SQLite migration or compatibility path.

The former `tests/test_corpus_batch.py` contained six tests: four protecting the old SQLite dry-run/apply/rollback/sidecar-retry mechanism, one historical manifest metadata test, and one CLI refusal test. Those six are replaced with four focused retirement tests covering imported dry/apply refusal, refusal before input/path validation, existing-file preservation with no new files, and CLI refusal with zero SQLite connections. The retired mechanism is no longer claimed to work.

Preserved native responsibilities are covered by `tests/postgres_integration.py` (`test_schema_isolation_and_transaction_rollback`, `test_alias_versions_and_idempotent_source`, `test_conflicting_identifiers_rejected`, `test_extraction_idempotency_and_concurrent_natural_key`), `tests/postgres_preparation.py` (`test_terminal_replay_preserves_first_completion_and_work`, `test_source_change_revokes_approval`), and `tests/postgres_neo4j_projection.py` (`test_atomic_outbox_and_source_immutable_pins`, `test_bolt_commit_before_ack_crash_reopens_and_replays_once`, `test_group_delivery_failure_resume_keeps_committed_member_reviews`). These protect PostgreSQL transactions, source pinning, idempotence and recovery; they do not reproduce the retired SQLite master/sidecar mechanism.
