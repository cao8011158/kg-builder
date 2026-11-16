# PostgreSQL preparation: testing and deployment

Preparation persistence uses PostgreSQL 16. Keep source and workflow data in the same database so source admission and task transitions can commit atomically. Each fresh run needs its own run identifier; do not import historical SQLite test stores into this refactor's acceptance run. The independent graph job retains its own storage contract.

## Disposable integration tests

The [GitHub Actions workflow](../.github/workflows/postgres-preparation.yml) starts a PostgreSQL 16 service on an ephemeral runner. Its `kg_test` database and role use a throwaway credential in the workflow. The role can create and drop test schemas. The service and all data disappear after the job. CI installs the package with semantic extras, runs the full unittest suite, then explicitly runs all `postgres*.py` suites with `KG_TEST_DATABASE_URL`. It requires the source, preparation and full preparation suites to exist and execute tests. A database failure fails the job. The workflow records the checked-out commit and actual test storage/durability settings. Uploaded artifacts contain sanitized test logs and synthetic SQL plan JSON, never a database dump or run inputs.

For an equivalent local integration run, provision a disposable PostgreSQL 16 instance and a role permitted to create/drop schemas in its test database. Set `KG_TEST_DATABASE_URL` to that database, install `python -m pip install -e '.[semantic]' pytest`, then run:

```bash
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python -m unittest discover -s tests -p 'postgres*.py' -v
```

The PostgreSQL tests are excluded from standard test discovery and require the explicit URL. They create isolated schemas and clean them up. Use only disposable credentials and data. Do not use a historical database or production connection string for tests.

## Preparation command shape

Both `scripts/prepare_pipeline.py` (bounded target mode) and `scripts/prepare_full_pipeline.py` (frozen paginated plan) require global `--run-id` and `--run-dir` on every subcommand. The stable logical run ID selects the PostgreSQL schemas; reuse it for `status`, `claim`, `submit`, `snapshot`, and process restart of the same run. Use a new ID for a fresh run. The run directory holds local artifacts such as claim output and snapshots; it is not database persistence. Set `KG_DATABASE_URL` in the runtime environment from a secret store before invoking commands (the code also recognizes `DATABASE_URL`). For example, once a database is provisioned:

```bash
python scripts/prepare_full_pipeline.py --run-dir /srv/kg/runs/example --run-id example-001 status
python scripts/prepare_full_pipeline.py --run-dir /srv/kg/runs/example --run-id example-001 claim --kind extract --worker worker-1 --output /srv/kg/runs/example/claims.json
python scripts/prepare_full_pipeline.py --run-dir /srv/kg/runs/example --run-id example-001 submit --input /srv/kg/runs/example/results.json
python scripts/prepare_full_pipeline.py --run-dir /srv/kg/runs/example --run-id example-001 snapshot --output /srv/kg/runs/example/prepared.json
```

The `run` subcommand calls the actual collectors and Jev decision service and requires `TYPESAFE_API_KEY`; only invoke it when those services and a real run are intended. External extraction workers must submit genuine, validated results. The example paths and run ID are illustrative and have not been deployed.

### Full-run screening rules

The full preparation `run --plan path/to/plan.json` option accepts the existing `full-preparation-plan/1` JSON. Copy `config/full_preparation_plan.json` and adjust its discovery queries, date windows, and, optionally, add a top-level `screening_policy`:

```json
{"screening_policy": {
  "schema": "scope-rules/1",
  "inclusion_rules": [
    {"name": "battery_aging", "question": "Does the paper present original research on grid battery degradation?"}
  ],
  "exclusion_rules": [
    {"name": "survey", "question": "Is the paper a survey or literature review?"}
  ]
}}
```

This optional policy **replaces** the original LLM-agent/topic screening gate for that full run. An omitted `screening_policy` retains the original Phase 2 behavior. Each rule asks Jev for `yes`, `no`, or `insufficient` using only the supplied title and text. A confident `yes` on any exclusion rule excludes the paper. Otherwise, a confident `no` on every inclusion rule excludes it. A paper is included only if at least one inclusion rule is confidently `yes` and every exclusion rule is confidently `no`; all other outcomes remain pending. The saved run's probability threshold applies separately to each answer. An empty exclusion list is allowed and is decisively clear by definition. The inclusion list must be nonempty. Rule names must be unique lowercase identifiers starting with a letter; each rule needs a nonblank question. At most 24 rules fit one Jev question batch per paper. Names and questions are bounded to 64 and 500 characters, respectively.

The screening policy is validated before a new run changes storage. The whole plan is saved with its discovery settings and loaded again on worker restart. Passing a changed plan to the same run ID is rejected. A `claim`, `submit`, `status`, or `snapshot` command loads the saved plan without needing `--plan`; `--plan` is available on `run` only. Screening decisions and individual rule answers remain in the paper scope audit. Discovery query terms are candidate retrieval criteria, not screening decisions. Extraction and later review still operate on the supplied paper text and keep their existing contracts; custom screening does not change extraction prompts or make a claim about unsupplied full text.

The disposable local PostgreSQL fixture and the Actions service are reproducible integration checks, not durable production stores. A passing result applies to its tested revision and configuration only.

## Durable deployment requirements

Before a real preparation run, provision a separate durable PostgreSQL service. Use a managed PostgreSQL instance with documented backup/restore support or a dedicated PostgreSQL host backed by an ext4/XFS volume outside the Work workspace overlay and outside a synchronized Drive mount. The CI service and this workspace are not durable production stores. Do not infer reliability from successful SQL tests alone.

For a self-managed host, initialize the cluster on the dedicated volume and retain PostgreSQL durability defaults: `fsync=on`, `full_page_writes=on`, and `synchronous_commit=on`. Monitor disk capacity, WAL retention, database health and backups. Restrict network access to application hosts, require authenticated connections, use TLS where traffic crosses a network boundary, and grant the application role only the database/schema privileges it needs. Supply the connection URL through the runtime secret environment (`KG_DATABASE_URL`); never commit a production password, `.env` file, or DSN. Ensure the deployment has a deliberate database lifecycle and run identifier before starting workers.

Back up the PostgreSQL database with a consistent `pg_dump` (or the managed provider's backup mechanism) to independent storage. Periodically restore that backup into a separate test instance and verify schema/data counts plus a representative read, claim and snapshot path. Record backup age, restoration duration and failures. A backup file without a successful restore drill is not recovery evidence. For larger databases, use provider-supported physical backups and point-in-time WAL recovery as appropriate to the required recovery objectives.

No durable PostgreSQL deployment, backup/restore drill, or live source/Jev/model run has been completed by this document or by the CI workflow. A fresh roughly 60-record acceptance run still needs its own evidence, including stop/reopen, concurrent claims, stale lease rejection, replay and snapshot stability. Run external decisions only when their real credentials and source access have been deliberately configured; synthetic integration fixtures do not prove live API behavior.
