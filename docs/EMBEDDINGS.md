# Embedding providers and HNSW consumers

Paper preparation stores source and review state in PostgreSQL and exports immutable `prepared.json` snapshots. Each approved contribution, research problem, method and benchmark has a vector. The input representation is the name followed by a newline and description, or just the description for an unnamed item. Vector generation does not repeat collection, scope screening or item review.

## Provider selection

Install the optional dependencies into the project environment:

```powershell
python -m pip install -e '.[semantic,voyage]'
```

The original local provider remains `fastembed`, defaulting to `BAAI/bge-small-en-v1.5`. Explicitly select `voyage` for the new API path. The supported Voyage model is `voyage-4-large`, with 1024-dimensional float vectors by default; 256, 512 and 2048 are also supported. Set `VOYAGE_API_KEY` in the invoking process from the local environment or secret store. Credentials are never written to snapshots or model identities. SDK construction and tokenization occur only when uncached, nonempty texts actually need encoding; status and claim operations do not request embeddings.

Example for an existing completed full preparation run, using a **new** output file:

```powershell
python scripts/prepare_full_pipeline.py --run-dir <existing-run-directory> --run-id <existing-run-id> snapshot --output <new-voyage-snapshot.json> --embedding-provider voyage --embedding-model voyage-4-large --embedding-dimension 1024 --voyage-concurrency 8 --voyage-rpm 2000 --voyage-tpm 3000000
```

The DSN is read from `KG_DATABASE_URL` (or the existing supported `DATABASE_URL`). Do not change the frozen collection plan, import previous test data, or reuse the BGE snapshot filename for a different vector representation. The snapshot exporter enforces immutable output bytes. If a new API export fails before publication, its output is not a complete snapshot; the native exporter currently recomputes vectors when restarted and has no persisted per-batch API resume cache. Account for repeated requests when retrying a large export.

Voyage encoding uses up to eight concurrent requests by default, with shared per-model-instance admission budgets of 2,000 requests and 3,000,000 input tokens per rolling minute. Configure these with `--voyage-concurrency`, `--voyage-rpm` and `--voyage-tpm` on the snapshot, semantic, graph-build and validation CLIs. Concurrency does not change model identity or frozen vector bytes. The snapshot exporter supplies a bounded window of up to concurrency times batch size (256 inputs by default), enabling concurrent request batches; local encoders retain their original 32-input windows. Admission reserves the configured SDK retry allowance for request and token budgets before each request, so actual first attempts can be below these ceilings. Small inputs, cache hits and rate limits can yield fewer active requests.

The official SDK handles bounded retries and timeouts. Its model tokenizer enforces the documented 32,000-token input and 120,000-token request budgets; batching also bounds the number of inputs. `truncation=False` prevents silently dropping text. Invalid cardinality, dimensions, nonfinite values or nonunit vectors fail export. Provider failure details and credentials are not included in the adapter error text. Voyage model names are service identifiers, not immutable provider weight revisions; retained vector bytes and hashes fix the actual output used by a job.

## Identity and downstream consumers

Model identities include provider, model, output dimension, document role, float output, text representation and normalization. Existing FastEmbed identities remain unchanged. Snapshot import rejects mixed models, mixed dimensions, changed vector text, and Voyage dimensions inconsistent with their identity. A supplied downstream model must match the frozen snapshot, including after restart. Without an explicitly supplied model, the graph consumer restores the supported encoder from that identity; it does not default a Voyage snapshot to BGE.

Corpus vectors use Voyage `input_type=document`; free-text search uses `input_type=query`. Query and document cache identities differ, so identical text cannot incorrectly reuse the other role's embedding. Existing generic encoders without a query method retain their original single-role behavior. The semantic CLI accepts `--embedding-provider`, `--embedding-model` (`--model` remains an alias), and `--embedding-dimension`; select the provider matching the sidecar's intended vector representation.

HNSW is the nearest-neighbor algorithm, implemented by the existing `hnswlib` package. It accepts the input vector dimension and therefore needs no algorithm replacement for Voyage. It returns candidate neighbors; semantic identity still requires the existing Jev decision. Each point searches its own top-k neighbors; a batch is not converted to an all-pairs clique. Research-problem clustering consumes those vectors through the existing sparse HNSW graph and Leiden implementation. Initial index construction is O(N); later queries reuse the index and encode/cache only query inputs. This change does not claim constant-time ANN internals or modify the existing semantic-store database design.

Changing the embedding model changes cosine distributions. Existing compatibility defaults (including clustering similarity 0.65 and resolution 0.05) have not been calibrated for Voyage. They must not be described as selected optimal settings. Jev probability thresholds, member-removal rules, external generation mode and frozen preparation scope are unchanged. No production graph is started by configuration or snapshot export.

## Incremental Neo4j publication

Install the optional official driver with `python -m pip install -e '.[neo4j]'`. Provide `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` and optionally `NEO4J_DATABASE` through the process environment or a secret store. Never put credentials in command arguments, snapshots or repository files.

```powershell
python scripts/build_snapshot_graph.py --snapshot <new-voyage-snapshot.json> --job-dir <new-graph-job> --neo4j --voyage-concurrency 8 --voyage-rpm 2000 --voyage-tpm 3000000 run
python scripts/build_snapshot_graph.py --snapshot <new-voyage-snapshot.json> --job-dir <new-graph-job> status
```

The graph job restores the encoder from the frozen snapshot identity, including Voyage, without requiring a provider override. Status and claim are metadata operations and construct no embedding, Jev or Neo4j client, even if provider or `--neo4j` options are present. External naming workers continue to use claim/submit; pass `--neo4j` on submit for a job with publication enabled. That submit creates a bounded Jev client for immediate named-area membership review (`submit --jev-model jev-1.14.0 --jev-concurrency 4` by default), so it also needs the existing `TYPESAFE_API_KEY` environment credential. Replaying a completed name whose membership audit is already saved does not repeat semantic requests.

Neo4j is the published domain graph target. PostgreSQL preparation, HNSW candidate retrieval, Leiden research-area clustering and Jev semantic decisions keep their existing responsibilities. SQLite retains durable job state, the publication outbox, audit data and the semantic embedding cache. Omitting `--neo4j` preserves the existing local graph workflow. An enabled job requires delivery of its durable outbox before terminal completion.

Publication follows each node or relation's own approval and state commit. It does not publish every nearest-neighbor candidate, and it does not wait for a global naming/review barrier. Source/provenance nodes can precede canonical entities; approved entity names and memberships appear as their decisions commit. Retrying the outbox uses stable identities and idempotent writes. Failure after remote commit but before local acknowledgement can replay the same event safely.

Measure service wall time, admission queue time, network/retry time, CPU graph/index work, Neo4j delivery and overlap separately. Cumulative request durations can exceed elapsed wall time under concurrency; do not sum overlapping active-stage durations and call that total wall time. Increasing the cap to eight does not establish that every graph embedding operation ran eight requests simultaneously.

## Evidence boundaries

Offline tests use synthetic 1024-dimensional snapshots and real HNSW/Leiden, plus fake SDK responses for failures and role isolation. A real two-text API smoke confirms connectivity, not retrieval quality. See [the cumulative preparation report](reviews/full-preparation-20261102/STAGE_REPORT.md) for actual executed checks, fixed commits, full-corpus BGE acceptance and any later Voyage paper-vector acceptance. No claim of full-text reading or retrieval accuracy follows from API success.

Official references: [Voyage embedding API](https://docs.voyageai.com/docs/embeddings), [input roles and similarity](https://docs.voyageai.com/docs/faq), [current pricing](https://docs.voyageai.com/docs/pricing). At the 2026-11-02 check, the published standard price for `voyage-4-large` is $0.12 per million input tokens; account credits and rate limits are account-specific.
