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

The current Work environment cannot run a real local PostgreSQL server: it has no usable nonroot execution identity, and PostgreSQL refuses to run as root. This is an environment limit, not an integration test pass. The Actions service is the reproducible real-server check. Its passing result, once observed on a fixed commit, is evidence for that commit only.

## Durable deployment requirements

Before a real preparation run, provision a separate durable PostgreSQL service. Use a managed PostgreSQL instance with documented backup/restore support or a dedicated PostgreSQL host backed by an ext4/XFS volume outside the Work workspace overlay and outside a synchronized Drive mount. The CI service and this workspace are not durable production stores. Do not infer reliability from successful SQL tests alone.

For a self-managed host, initialize the cluster on the dedicated volume and retain PostgreSQL durability defaults: `fsync=on`, `full_page_writes=on`, and `synchronous_commit=on`. Monitor disk capacity, WAL retention, database health and backups. Restrict network access to application hosts, require authenticated connections, use TLS where traffic crosses a network boundary, and grant the application role only the database/schema privileges it needs. Supply the connection URL through the runtime secret environment (`KG_DATABASE_URL`); never commit a production password, `.env` file, or DSN. Ensure the deployment has a deliberate database lifecycle and run identifier before starting workers.

Back up the PostgreSQL database with a consistent `pg_dump` (or the managed provider's backup mechanism) to independent storage. Periodically restore that backup into a separate test instance and verify schema/data counts plus a representative read, claim and snapshot path. Record backup age, restoration duration and failures. A backup file without a successful restore drill is not recovery evidence. For larger databases, use provider-supported physical backups and point-in-time WAL recovery as appropriate to the required recovery objectives.

No durable PostgreSQL deployment, backup/restore drill, or live source/Jev/model run has been completed by this document or by the CI workflow. A fresh roughly 60-record acceptance run still needs its own evidence, including stop/reopen, concurrent claims, stale lease rejection, replay and snapshot stability. Run external decisions only when their real credentials and source access have been deliberately configured; synthetic integration fixtures do not prove live API behavior.
