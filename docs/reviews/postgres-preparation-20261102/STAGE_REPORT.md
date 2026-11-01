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
