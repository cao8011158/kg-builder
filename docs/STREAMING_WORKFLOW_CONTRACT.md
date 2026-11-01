# Streaming preparation and snapshot graph jobs

This contract fixes the implementation boundary for the 2026-11-01 workflow step.

## Preparation

Use only existing OpenAlex/arXiv collectors and storage normalizers. A run directory owns a fresh master SQLite file and a durable workflow SQLite file. Target counts distinct papers, not hits. Scope and extraction run while collection continues. External mode is the measured execution path; no generative LLM API is called in that mode. Only accepted, field-reviewed extractions become ready. No canonical entities, neighbor graphs, Leiden or HNSW construction occurs during preparation; local per-text embeddings may be cached for the snapshot.

Persist bounded indexed task queues with atomic claims, lease token/expiry, input fingerprint, attempts, terminal pending/failed reasons, and idempotent result submission. Interrupted tasks resume; stale leases and conflicting submissions reject. Tasks are `extract` or `repair_extraction` in preparation, and `name_group` in the separate graph job. External callers read genuine source content and submit structured model results through the same validator and state transitions as API mode. No old extraction replay in a fresh run.

Collection may use first-page bounded requests round-robin across the three query topics. API keys use environment variables only. Missing source content is pending, not fabricated. Freeze the original title/abstract/version/source URL and fingerprint for each item; recheck source validity before approving. Model-returned paper/version/URL/label must equal supplied pins. Date and topic query tags are candidate metadata, never semantic relevance decisions.

Scope: Jev makes four independent choice questions per paper: substantive LLM-agent/conversational-agent focus, agent memory lifecycle/evaluation, long-horizon context management, and reusable procedural skill lifecycle/evaluation. Each choice is yes/no/insufficient. Generic tool use, RL posttraining, model KV-cache acceleration and generic RSI are excluded unless substantively intersecting one of the three domains. Derive include/exclude/pending from typed answers and explicit configurable confidence policy; save full audit results. No second full LLM scope rescreen.

Field review: Jev compares every extracted source claim/name/role with actual supplied text. Return supported/contradicted/insufficient per item plus adequacy/omission checks; all-empty extraction cannot pass. A failed review creates a bounded repair task, then pending after the configured repair limit. Roles and benchmark subsets remain per item. Contributions stay paper-local. Preserve v0.3 extraction schema.

## Shared adapter contracts (owned by API developer)

`kg_builder.decision_api.JevClient(model='jev-1.14.0', key_env='TYPESAFE_API_KEY', timeout=..., max_retries=...)`

- `decide(state, questions) -> dict`: authenticated official `https://api.typesafe.ai/v1/systemone`; validates exact question IDs/choice labels/probabilities/confidence, logs resolved model and usage with request duration/retry metadata, never logs headers/keys. Synchronous; callers can use `asyncio.to_thread`.
- `classify_scope(papers, client, min_confidence=0.7) -> list[dict]`: each input has `paper_id,title,text`; result `paper_id,decision,topics,reason,raw`.
- `review_extractions(items, client, min_confidence=0.7) -> list[dict]`: inputs `paper_id,text,extraction`; result `paper_id,passed,issues,raw`.
- `judge_pairs(pairs, client, min_confidence=0.7) -> list[dict]`: inputs `pair_id,kind,left,right` with source-authored name,description and source metadata; result `pair_id,decision` (`same`,`different`,`pending`),`raw`.
- `review_groups(groups, client, min_confidence=0.7) -> list[dict]`: inputs `group_id,kind,name,description,members`; result `group_id,passed,issues,raw`.

Batch API questions over bounded state; no fixed vendor 20-question claim. Use actual documented Choice answers (`choice,probabilities,confidence`). Pin model and store resolved model. Source text is data, not instructions. No semantic keyword/threshold replacement for model judgment; confidence routing alone is deterministic.

Extend `OptionalLLMExtractor.extract_draft(paper_id,version_id,text,paper_version=...)` to supply and validate exact metadata pins. Add a generic bounded JSON completion function for `name_group`/repairs. Keep generative API disabled unless explicitly selected. Do not fetch URLs supplied by model results.

## Preparation public contract (owned by workflow developer)

`PreparationWorkflow(run_dir, ...)` plus CLI `scripts/prepare_pipeline.py` subcommands `run`, `claim`, `submit`, `status`, `snapshot`.

- `run --target 60 --llm-mode external --jev-model ... --batch-size ...`: collectors and Jev progress concurrently with external tasks; keep polling until terminal, or explicit stop/resume. Configuration stores batch/concurrency/timeouts/repair policy.
- `claim --kind extract --limit 5 --worker ID --output FILE`: persist claims and write full source/task input plus lease tokens, not old model output.
- `submit --input FILE`: accepts list with `task_id,lease_token,result` and exact input hashes; preserve extraction validation.
- `status`: cheap indexed counts, not full source payload scans.
- `snapshot --output FILE`: only after no active work and collection sealed; immutable JSON with schema `research-kg-prepared-snapshot/1`, `run_id`, counts/config, `papers` (accepted source metadata), `extractions` (reviewed v0.3), `vectors` list (`item_key,kind,text,model_id,vector`), excluded/pending summaries, and canonical SHA256 digest (excluding `snapshot_hash`). Atomic write. No timestamp rewrite on identical snapshot replay.
- Metrics per stage distinguish queued wait, task service elapsed, API request time, active code and wall; sums of overlapping task duration are not wall. Logs and full decisions remain private run data.

Exact accepted paper shape: `{paper_id,title,first_published,year,doi,arxiv_id,version_id,paper_version:{version_label,source_url},text,source_hash}`. `extractions` contains raw v0.3 objects, not wrappers. `vectors.item_key` is `paper_id + "::" + original_item_key`, so repeated paper-local keys cannot collide; its kind is ResearchProblem/Method/Benchmark/Contribution. Snapshot digest uses UTF-8 canonical JSON: sort_keys=True, ensure_ascii=False, separators=(",",":"), allow_nan=False, excluding only snapshot_hash. Extra fields are included in the digest. Export a reusable snapshot_digest helper.

Workflow module owns `kg_builder.preparation`, `scripts/prepare_pipeline.py` and its tests; API module ownership above is exclusive.

## Separate graph job (owned by graph developer)

`kg_builder.snapshot_graph` and `scripts/build_snapshot_graph.py`. Input prepared snapshot is immutable and verified. It uses its own durable task/state path; it never mutates preparation DBs or the authoritative old corpus.

Build full within-kind sparse candidate neighborhoods from ALL snapshot vectors before identity decisions; run existing ResearchProblem Leiden proposal once. Never mix kinds in the problem graph; never allocate an N-by-N matrix. Jev judges bounded candidate pairs; record uncertainty. Group naming for ResearchProblem uses external `name_group` tasks or generative API mode. Method/Benchmark canonical display names must be source-authored member names, aliases preserve variants. Check resulting group coherence with Jev; incoherent components remain pending rather than silently forced together. Existing `SemanticStore.import_review` validates source fingerprints and materializes the reviewed nodes and links. Uncertain mentions can remain explicitly pending; completion counts must not hide them.

Provide `run`, `claim`, `submit`, `status` behavior for external names; reuse adapter APIs. Persist sparse graph/group outputs and per-stage timings. Existing semantic store remains the source of truth for retrieval; populate/correctly consume snapshot vector cache where compatible. Measure pure graph/index CPU separately from Jev decisions and external naming time. No second independent LLM identity rereview.

## Tests and boundaries

Meaningful tests: concurrent/stale claims, restart/replay, source changes, malformed or conflicting model IDs/pins, real immutable snapshot hashing, fake-adapter integration streaming (first extraction available before scope of all inputs completes), pending/repair paths, exclusion of graph effects during preparation, graph snapshot mutation rejection, and indexed queue/detail/status access under unrelated large data. Full unittest regression must retain all prior tests. Real paid API calls only by root at explicit runtime; developers test offline. Do not put credentials or genuine run inputs into public Git. No scheduling service, Neo4j writer, main merge or production dataset overwrite.
