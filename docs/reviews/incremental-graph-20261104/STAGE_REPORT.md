# Incremental graph publishing

The work is local on `codex/bm25-retrieval`, based on `cb6f0e493bc544fab4aa351d4026681454cc72ad`. The authorized scope covers code and isolated synthetic tests. Publication, scheduling and graph activation are separate operations. Existing workspace changes outside this batch are excluded.

## Stage 1 · Approved batch and command integration

- The snapshot helper composes approved paper-level replacements and explicit removals. The output binds the base and delta snapshot digests and reuses retained payloads and vectors.
- The manual command stages an immutable release with a deterministic run ID. It publishes its manifest after graph completion, outbox delivery, structural validation and BM25 verification. The default-disabled scheduler entrypoint invokes the same command on an approved batch.
- Initial Windows sandbox runs failed during temporary file access and hard-link creation. The test run moved its temporary directory under the workspace. Atomic Windows publication uses non-replacing rename and the POSIX path uses hard links. Eight snapshot and command tests then passed.
- A disposable PostgreSQL server started successfully, but the sandbox blocked its loopback client connection with Windows socket error 10013. The server was stopped. The next run uses the same isolation model with local connection access.

## Stage 2 · Revision identity and structural validation

- The child graph reuses still-valid pair judgments and whole-group reviews. New or changed Method and Benchmark mentions query the same-kind candidate index. ResearchProblem vectors feed the existing Leiden partition for the merged corpus.
- The revision carries prior IDs across unambiguous continuing entities and areas. It records overlap edges for split and merge cases. The read-only validator checks the effective IDs against the parent and the stored evidence.
- The synthetic PostgreSQL instance completed the existing `postgres_snapshot_graph.py` suite, 31 tests in 57.365 seconds, and shut down. The then-current offline incremental suite passed 21 tests in 0.93 seconds. Further revisions to the identity edge cases require a targeted rerun.

## Stage 3 · Independent review and isolated acceptance

- An independent local reviewer checked the manual and scheduler entrypoints, the snapshot merger, the Windows publication primitive and the release gate. The reviewer found no actionable defect in those paths. The core review corrected the baseline configuration and naming reuse checks, reserved old IDs during splits and merges, and removed repeated parsing of the full ID map during projection.
- The final offline targeted command ran `test_incremental_update.py`, `test_incremental_publish.py`, `test_graph_revision_identity.py` and `test_graph_revision_validation.py` with the workspace-local temporary directory. All 25 tests passed.
- Disposable PostgreSQL suites passed for the existing snapshot graph (31), structural validator (19), outbox projection (5), and the new base-to-delta-to-replay case (1). The revision fixture uses fake Jev, embedding and Neo4j endpoints. It checks effective IDs against the published events and confirms that completed replay emits no extra events. A final isolated rerun extended that fixture with a second revision that changes ResearchArea membership and retains the area ID. The test passed in 7.173 seconds.
- The broad `test_*.py` run reported 146 passing tests and one SQLite temporary-file cleanup error on Windows. The same test raised WinError 32 on an archive of the unmodified baseline. This is recorded separately from the incremental acceptance.
- Raw PostgreSQL suite logs are in `.test-tmp/incremental-pg-25049f92432d42e3a9953b2079bd3299/`. The final revision log is in `.test-tmp/incremental-final-4322fa9df72b41aeb3e097a1d6a5b6b6/revision.log`. The isolated database servers shut down after their runs. Production data, production Neo4j, paid APIs, graph activation and real scheduler execution were outside the run.

## Acceptance criteria

- Same-batch replay resumes the same run, and a completed release skips graph work.
- Method and Benchmark reviews reuse valid evidence. A continuing entity keeps its ID.
- ResearchArea uses the updated Leiden partition. A continuing area keeps its ID, and splits and merges have explicit lineage.
- Outbox payloads and PostgreSQL agree on effective IDs. Pending publication or structural failures withhold the release manifest.
- Synthetic tests use a separate database and fake review, embedding and Neo4j endpoints. Live data and the external scheduler stay outside this validation.
