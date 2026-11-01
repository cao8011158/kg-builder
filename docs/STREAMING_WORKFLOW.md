# Streaming preparation and separate graph build

Use a fresh private run directory for each test. Credentials are supplied through `OPENALEX_API_KEY` and `TYPESAFE_API_KEY`; never include them in command arguments or exported data. Source collectors retrieve metadata and abstracts, not full papers. The 60 target counts unique candidates before semantic scope filtering.

```sh
python scripts/prepare_pipeline.py --run-dir /private/run60 run --target 60 --llm-mode external --batch-size 5
python scripts/prepare_pipeline.py --run-dir /private/run60 claim --kind extract --limit 5 --worker reader1 --output /private/tasks.json
python scripts/prepare_pipeline.py --run-dir /private/run60 submit --input /private/results.json
python scripts/prepare_pipeline.py --run-dir /private/run60 status
```

The run command keeps progressing while external workers read and submit tasks. Each submission is a list item containing `task_id`, `lease_token`, `input_hash`, and `result`. The result is a complete v0.3 extraction matching the exact paper/version/URL/label pins, with status `draft_requires_review`. Use the supplied source only; preserve author-claimed wording, actual method/benchmark names and their roles. Mark `source_scope` as `abstract_only` when the task contains only an abstract. Use version-prefixed keys for every item to avoid cross-paper key collisions. Every result must include all four arrays; empty arrays are appropriate when the source provides no evidence. Do not reuse prior run extraction outputs.

Jev performs scope once and verifies submitted fields. Failed field review creates `repair_extraction` tasks, which include the previous extraction and review issues. Claim and submit them through the same interface. Leases expire; stale or conflicting submissions fail. Stop and rerun the run command to resume the same persisted configuration. Pending is an explicit terminal outcome and does not become accepted automatically.

After the run is terminal, freeze the reviewed records and local vectors:

```sh
python scripts/prepare_pipeline.py --run-dir /private/run60 snapshot --output /private/run60/prepared.json
python scripts/build_snapshot_graph.py --snapshot /private/run60/prepared.json --job-dir /private/graph60 run
python scripts/build_snapshot_graph.py --snapshot /private/run60/prepared.json --job-dir /private/graph60 claim --worker namer1 --limit 5 --output /private/names.json
python scripts/build_snapshot_graph.py --snapshot /private/run60/prepared.json --job-dir /private/graph60 submit --input /private/name-results.json
python scripts/build_snapshot_graph.py --snapshot /private/run60/prepared.json --job-dir /private/graph60 run
```

Graph naming submissions use the same task/lease/input hash envelope, with `result` containing `name`, `description` and the supplied `group_id`. The graph job uses all snapshot vectors for within-kind neighbor candidates and a ResearchProblem Leiden proposal before identity decisions. Jev determines identity/coherence; similarity alone never merges records. ResearchProblem naming is external in the measured run. Method and benchmark names remain source-authored. Preparation never creates shared entities or runs Leiden/HNSW. The graph job creates its own master and semantic databases and never updates the preparation snapshot or prior corpus.

Keep preparation and graph timings separate. Report actual total wall time, API request/retry elapsed, external claim-to-submit time, queue waits, and local processing. Parallel task-duration sums are not wall time. A completed run may retain pending records; always report their counts. The optional generative API path is preserved but is not exercised by an external-mode run.

Architecture and exact schemas: [contract](STREAMING_WORKFLOW_CONTRACT.md). Current development and acceptance evidence: [stage report](reviews/streaming-workflow-20261101/STAGE_REPORT.md).
