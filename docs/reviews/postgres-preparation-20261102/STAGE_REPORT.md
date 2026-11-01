# PostgreSQL preparation refactor — 2026-11-02

Status: Batch 1 implementation in progress; no production run, no main merge.

## Scope and authority

- Repository: cao8011158/kg-builder.
- Work branch: work/retrieval-pilot-20261031.
- Baseline: b070b2b5937948d4f5c0cc17e9148e0779b06906 (remote verified via GitHub PR metadata and clone).
- PR #1 open/draft, not merged. Base main fc114851d5017aed7c8d5477b58b2ff8dac6bbfa; merge is explicitly prohibited.
- User request: replace preparation persistence with PostgreSQL and use a fresh ~60-record test; no migration/recovery/import of historical test databases; no full collection, graph, retrieval or harness work.
- Preserve frozen config/full_preparation_plan.json, title/abstract evidence, Jev probability semantics and shared request budget, external generation, item repair/freeze rules, immutable prepared snapshot contract.
- Replaced: preparation SQLite storage, single-writer locking, workflow/page/task/lease persistence. Retained: independent graph storage outside this step; pure normalizers/validators and API adapters.
- Decision owners: PostgreSQL owns persistent facts and atomic task/state transitions; Jev owns semantic scope/support; external/API LLM owns extraction/repair; code owns normalization, reliable-ID dedup, structure/pins, dispatch and export.
- Implementation actor: pg_developer, GPT-6 SOL, medium; independent reviewer: root (no production implementation planned).

## Batches and acceptance

1. Native psycopg3 schemas/connections/transactions/source repository. Check unique IDs, concurrent source admission, rollback and indexed identity reads.
2. Wire public preparation and paginated streams to PostgreSQL; remove preparation's actual SQLite path; preserve formal 4/4/4 workers, shared Jev8, batch5, arXiv3-second gate, OpenAlex4 streams and frozen plan.
3. Fresh ~60-record acceptance with concurrent claims, expiry/stale rejection, idempotency, item-only repair/freeze/drop, stop/reopen, page replay and immutable vectors/snapshot. Distinguish synthetic persistence testing from live source/Jev/model execution.

Every batch: developer targeted tests + fast regression; remote fixed-SHA independent review, at most three rounds. Locality proof uses real PostgreSQL EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and unrelated-history growth. No old databases are read or imported.

## 0. Baseline / environment

- Checkout matches specified commit and branch; no AGENTS.md found in repository or checked parent directories.
- Runtime initially lacks PostgreSQL, pytest and semantic optional dependencies. A local virtualenv is being prepared; the first baseline test attempt exposes missing optional semantic dependencies and is retained as environment evidence, not a code failure.
- Workspace filesystem is overlay; /dev/shm is tmpfs. Neither is claimed durable production storage.
- No database URL or TYPESAFE_API_KEY is present in environment. Live semantic API acceptance is not currently configured.
- Test PostgreSQL provisioning and the reliable deployment/runbook remain in progress. Changing the database program alone does not establish storage reliability.

## 1. Test infrastructure development (uncommitted)

Implementation actor: pg_test_infra, GPT-6 SOL, medium. Added `.github/workflows/postgres-preparation.yml` (PostgreSQL16 disposable service, full and explicit PG suites, sanitized evidence artifacts) and `docs/POSTGRES_PREPARATION.md` (test commands and durable deployment requirements). Developer YAML parsing passed; root inspected workflow and runbook. No Actions job has run yet. Fresh60 is explicitly pending and not claimed by this foundation workflow.

Local native-server provisioning is blocked: runtime maps only UID0; PostgreSQL refuses root; user mapping/ptrace mechanisms are unavailable. No upstream program or runtime restriction was modified. Remote Actions service selected for real-server tests. Production storage remains unprovisioned; preferred later location is a user-provisioned managed PostgreSQL service or dedicated ext4/XFS host volume with backup/restore verification, outside overlay/Drive mounts.

Frozen baseline regression: `PYTHONPATH=<baseline-checkout>/src .venv/bin/python -m unittest discover -s <baseline-checkout>/tests -v`, exact b070b2b baseline archive, **163 passed in22.412s**, exit0. See `logs/baseline-frozen.log`. An earlier in-flight check included a newly created PG test before it was moved to the explicit suite; its missing-URL error is environment/test-discovery evidence, not a baseline implementation failure (`logs/baseline-ready.log`). Dependencies were installed in a task-local virtualenv; hnswlib built successfully with GCC/G++ after unavailable default clang failed.

## 2. Batch 1 storage development (uncommitted candidate)

Developer pg_developer delivered native PostgreSQL source/workflow schemas, bounded psycopg3 pool and caller-owned transactions. Source and workflow schemas share the same connection for atomic admission/approval in Batch2. Source upserts use normalized strong IDs, sorted transaction advisory locks and unique constraints; no title merges. Schema initialization is serialized per logical run. Source/version pins, source update time and extraction idempotency are retained. `pyproject.toml` adds psycopg binary and pool dependencies. The explicit `tests/postgres_integration.py` covers rollback, aliases/versions/conflicts, concurrent same-identity writes and concurrent first initialization. Existing preparation SQLite entrypoints are not yet wired: overall replacement is not complete.

Developer verification: `.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -q` with PYTHONPATH=src, **163 passed in40.415s**, exit0; compileall and git diff --check exit0. `logs/batch1-developer-fast.log` preserves actual output. Native PostgreSQL suite was not run locally (missing service URL/environment restriction), not counted as passed. No live source/Jev/generative/embedding calls.

Root added an independent native source-locality measurement script. Actions will run it after explicit PostgreSQL integration and preserve actual EXPLAIN ANALYZE/BUFFERS JSON under2000 unrelated source rows. Local compile is syntax evidence only. Next: commit/push candidate to authorized work branch, verify fixed remote head, run native service and complete independent review round1. No main merge.

## 3. Batch 1 independent review — round 1/3, passed with stated limits

Reviewed full baseline..`cb6d5d92d163dfd3c1bbc93fbaac258cf4da31ea` diff; remote branch verified. No unresolved blocking storage defect found. No main merge. Published through authenticated GitHub Git Data tools after ordinary git push failed for missing local credentials; local branch synchronized only after staging-tree equality was verified.

Real PostgreSQL16.15 service: [Actions run41360931157](https://github.com/cao8011158/kg-builder/actions/runs/41360931157), job125010921183, success. Full regression **163 passed in20.331s**; native PG integration **6 passed in2.657s**. Root inspected original job output and downloaded artifact13105361601 (ZIP SHA2565aa87a66c81844e34566ef8b3ce21ff678ad4ad05a476022fb9adf3cc6ad2869); extracted evidence in `logs/batch1-ci/`. Native tests prove cross-schema rollback, reliable-ID alias/version preservation/conflict rejection, concurrent dedup, concurrent schema initialization and extraction idempotency/concurrency.

The PR job checked virtual merge ab99dfd9ba661fc21a9c36b40983b2677c6fa5c4. It did not merge main. Root fetched that object and verified its complete Git tree equals the reviewed head tree, both `a182d887705269963e3704f5e54c64c44366eb9b`. Batch2 will explicitly check out PR head SHA so future evidence requires no merge-tree equivalence inference.

Root local full-suite rerun: **162 passed/1 failed in56.580s**; historical `test_single_arxiv_lane_paces_and_drains_multiple_pages` assumed exactly one sleep even when admission itself exceeds3seconds. Preparation/full modules and this test are byte-unchanged from baseline; native source foundation is not in that path. CI full suite passed. This timing-dependent assertion is preserved in `logs/batch1-reviewer-fast.log` and will be replaced with actual request-start interval evidence when Batch2 ports the preparation test, rather than weakening pacing semantics. This is not claimed as a clean local pass.

| Public operation | Data | Actual native plan/work | Result |
| --- | --- | --- | --- |
| Source upsert strong-ID resolution | target1, unrelated0→2000, unrelated titles4096bytes |3 index scans/lookup3→3; source_records PK actual1row/2buffer hits; DOI index actual1row/2→3hits; alias PK actual1row/2hits; shared physical reads0 | Same paper identity; no unrelated row scan. Full plans in source-locality.json. |
| Shared source/workflow transaction | fresh isolated schemas | induced exception rolls back both stores | Verified by native integration. |
| Concurrent identity/upsert/init/extraction | multiple threads and independent connections | PostgreSQL locks/constraints | One identity/extraction, no conflicting commit; all6 tests pass. |

Locality scope: actual planner/rows/buffer data are available for source identity reads. No application cache, no claimed disk-cold measurement, full pipeline queue/page/public-status locality still belongs to Batch2/3. Overall preparation replacement is not yet complete. Next: Batch2 native entrypoint/stream wiring and ported PostgreSQL workflow tests; then fresh60 acceptance. Production durable storage, live Jev and real embedding remain unverified.

## 4. Batch 2 test/CI development (uncommitted, native runtime pending)

Developer pg_test_infra ported the existing preparation/full workflow cases into `tests/postgres_preparation.py` and `tests/postgres_full_preparation.py`, with a fresh-run PostgreSQL fixture. The40 behavior methods are retained (29 preparation,11 full); native `%s` queries and actual EXPLAIN ANALYZE/BUFFERS replace SQLite-specific SQL/VM assumptions. The pacing case now observes fetch-start spacing rather than asserting an arbitrary sleep count. No claim of a passing native run yet. Python compilation and YAML parsing passed; production wiring remains in development.

CI now pins/asserts the exact PR head, requires all native suite files and rejects zero native tests, and records database durability settings plus actual PGDATA host mount facts through the root-authored read-only storage_environment.py. The runbook documents explicit logical run IDs, environment-only connection URL and PostgreSQL test commands. Root has also drafted a separate synthetic fresh60 acceptance script for later execution; it has only been compiled and is not acceptance evidence yet.

## 5. Batch 2 production development (uncommitted candidate)

Developer pg_developer completed native PG wiring for preparation/full workflow and both `--run-id` CLIs. Actual source admission, source pin check/extraction insert/ready transition now use one transaction. Indexed SKIP LOCKED claims, persisted tokens/expiry and stale result fencing cover stage/task/stream work. Full collection stores raw BYTEA pages and transactional per-row applied_rows/admitted progress, then advances the stream checkpoint exactly once. Formal FullPreparationWorkflow defaults are Jev8, scope4/review4/external generation4, batch5, target0/external. Frozen plan is byte-unchanged. Both source refresh and approval use source→version→workflow lock order; run initialization uses ON CONFLICT and terminal runs reject new admissions.

Three obsolete SQLite preparation CLIs are retired with PG guidance; their historical internal helpers/graph consumers remain outside the formal preparation path. Three corresponding old CLI tests now verify retirement/no database creation. The default unit suite is125 tests and the explicit PG suite is46 (38 ported behaviors +2 new lease/takeover cases +6 source cases), total171. Correction to section4's preliminary developer count: the actual original preparation/full case count is27+11=38, not40. No required cases were dropped; the codebase adds8 native cases over baseline163.

Developer command `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py' -q`: **125 passed in8.576s**, exit0 (`logs/batch2-developer-final-fast.log`). Test-port developer independently repeated125 passing (25.0s unittest time); an intermediate125/3-error run during CLI contract changes is retained, and its three obsolete CLI assertions were explicitly replaced (`logs/batch2-dev-default-before-cli-fixture-update.log`). Compilation and git diff --check pass. Real46 PG runtime remains pending remote candidate. Root's synthetic60 script is not yet wired/executed. User has now supplied Jev credentials; they are stored only in a private temporary runtime file outside the repository, with no secret text in these records.
