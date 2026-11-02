# External graph naming workers

One controller owns the native SnapshotGraph, one JevClient with HTTP concurrency 8, one embedding model and the optional Neo4j publisher. Start at most six actual external Agents. The repository does not launch Agents or call a generative naming API. Each Agent reads every member in its claimed task and writes a grounded name and description.

PostgreSQL native task leases, immutable input fingerprints, `submit()` validation and projection outbox remain authoritative. The separate `kg_ext_<run hash>` schema is only durable transport; its version is `external-graph-queue/2`. It does not alter GraphStore DDL or its fingerprint. Queue v1 drafts are incompatible; use a fresh run instead of changing an existing schema manually.

Set database and API credentials through environment variables only. `KG_GRAPH_DATABASE_URL` selects the graph database. Helpers need that variable but no API keys. The controller additionally needs `TYPESAFE_API_KEY`, `VOYAGE_API_KEY` and, when publishing, `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` and optionally `NEO4J_DATABASE`. Never put credentials in command arguments or artifacts.

Start the controller once, before helper claims. Use the exact immutable Voyage snapshot and a fresh job directory for a new graph:

```powershell
.venv/Scripts/python.exe scripts/run_external_graph.py --snapshot data/full-graph-20261103/prepared.json --job-dir data/full-graph-20261103/graph-job --run-id YOUR_GRAPH_RUN controller --neo4j --embedding-provider voyage --embedding-model voyage-4-large --embedding-dimension 1024 --embedding-cache-dir data/embedding-model-cache --summary data/full-graph-20261103/controller-summary.json
```

The controller holds session advisory locks across naming waits. A second controller fails before loading the full snapshot or constructing remote clients. It reads the full snapshot once per controller invocation. Helpers open metadata and native PostgreSQL source members only; they never invoke the SnapshotGraph constructor or read snapshot arrays. An absent descriptor or changed snapshot stat fails before claim; let the controller verify restored snapshot bytes first. Keep the input immutable while running.

Each Agent uses a stable distinct worker ID and repeats this sequence independently. Choose a new output file for every invocation; existing output files are deliberately refused before taking a lease:

```powershell
.venv/Scripts/python.exe scripts/run_external_graph.py --snapshot data/full-graph-20261103/prepared.json --job-dir data/full-graph-20261103/graph-job claim --worker naming-1 --output data/full-graph-20261103/workers/naming-1/claim-0001.json --wait-seconds 55
```

The command prints `{state, task_count, output}`. Open the output JSON, which contains `{state, tasks}`. `claimed` has exactly one task, including all source members, `task_id`, `group_id`, `lease_token`, `input_hash` and instructions. Read every member's name, description, title and source evidence before generating. Do not truncate a large task or infer unread members.

Write one UTF-8 JSON object (not a list) to a result file:

```json
{
  "task_id": "EXACT CLAIM VALUE",
  "lease_token": "EXACT CLAIM VALUE",
  "input_hash": "EXACT CLAIM VALUE",
  "result": {
    "group_id": "EXACT CLAIM VALUE",
    "name": "Concise source-grounded research area",
    "description": "A description covering the related but distinct source problems."
  }
}
```

```powershell
.venv/Scripts/python.exe scripts/run_external_graph.py --snapshot data/full-graph-20261103/prepared.json --job-dir data/full-graph-20261103/graph-job enqueue --input data/full-graph-20261103/workers/naming-1/result-0001.json
```

After successful enqueue, immediately claim again with a new output path. Do not wait for other Agents or queue acceptance. The controller processes each ready result automatically, calls native submit (including Jev membership review and publication), then acknowledges it. A worker can hold at most two unacknowledged receipts and at most one generating lease. Six active worker identities bound the queue to twelve active receipts. This provides durable backpressure without keeping large queued results in controller memory; the controller reads one ready payload at a time and clears accepted payload text. Completed or expired receipts release worker admission, so replacement worker IDs do not exhaust permanent slots.

For `idle` or `waiting`, claim waited at most 55 seconds while polling; repeat with a new output path. `waiting` means worker/global backpressure; it is not a generation task. For `complete`, exit. The 55-second bound applies to application waiting, excluding process startup and a blocked database operation. Do not run an external sleep longer than a minute. Leases last 1800 seconds. If a helper dies, allow the native lease to expire; stale enqueue is rejected and native claim may issue a replacement token. Results that expire after enqueue are retired without bypassing native lease rules.

An identical enqueue is idempotent even after acknowledgement; a changed result for the same claim conflicts. If the controller exits after native submit but before queue acknowledgement, restart the same command, job and database. Native submit replays the same result and resumes missing reviews/publication. The queue never invents an alternative semantic acceptance rule. A malformed/conflicting result or review failure stops the controller with its durable ready receipt preserved. Fix the underlying issue; do not delete leases, change thresholds or force queue acceptance. CLI errors print a sanitized error type without database connection details.

```powershell
.venv/Scripts/python.exe scripts/run_external_graph.py --snapshot data/full-graph-20261103/prepared.json --job-dir data/full-graph-20261103/graph-job status
```

Status and the atomically replaced controller summary contain counts, numeric timing aggregates, worker summaries, native stage totals and publication wall time. They do not fetch historical result payloads or Jev request JSON. Timings are cumulative across attempts and parallel intervals must not be summed as elapsed runtime:

- `ready_to_claim_seconds`: native task creation to native lease acquisition.
- `claim_to_enqueue_seconds`: lease acquisition to enqueue; includes Agent reading, generation, tools and handoff.
- `enqueue_to_processing_seconds`: durable enqueue to first processing attempt.
- `native_submit_seconds`: first processing to accepted queue acknowledgement, including crash/restart gaps if present.
- `native_stage_seconds`: native Jev review and materialization/index timings.
- `publication_seconds`: native Neo4j delivery wall time.
- `pure_model_seconds`: `null` (unknown); helper wall clocks cannot isolate model inference.

Validation uses `KG_TEST_DATABASE_URL` only, disposable native and auxiliary schemas, no real paid API and synthetic snapshots. Run `python -m unittest discover -s tests -p 'postgres_graph_external.py' -v`. Native hard-link descriptors require normal Windows filesystem permission; sandbox denial is an environment failure, not a passing test.

The controller writes `controller_state=loading` before loading the full snapshot. A separate monitor thread owns its PostgreSQL handle and queue connections and refreshes the summary every two seconds during import, candidate review and external handoff. It reports native phase, candidate/group/task counts, projection progress, shared Jev concurrency statistics and thread-safe Voyage usage. Neither the monitor nor its helpers load another snapshot. A temporarily unavailable monitor connection is retried; errors expose only their type. `updated_at` lets a supervisor detect an unhealthy/stale monitor.

On Windows, an open reader can briefly prevent atomic file replacement. The writer retries a `PermissionError` three times with 10ms spacing; continued filesystem errors are recorded and the next periodic cycle retries. On exit the controller waits at most two seconds for the monitor, then writes its known terminal process state using only cached metadata and no database/client operations. Periodic writes merge the latest process state so an older sample cannot overwrite `complete`/`failed`. Filesystem failure can still prevent reporting and must be investigated through a stale `updated_at` or sanitized CLI error; committed graph work remains durable.

The summary must be outside the graph job directory. Preflight rejects the snapshot, graph descriptor, proposal/artifact files in the job directory, and hard-link aliases of those protected files before paid work begins. Use a sibling path such as the example's `controller-summary.json`.
