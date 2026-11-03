# Research KG Builder

Research paper discovery, versioned extraction and reviewed semantic retrieval for a scholarly Knowledge Graph. Approved snapshot graph nodes and edges can be published incrementally to Neo4j with the official driver and an explicit `--neo4j` flag.

## Research scope

First publication on/after **2025-01-01**, across three non-exclusive domains:
Agent Memory; Context Management & Compression; and Skill / Self-improving Skill.

See [the current scope](docs/CURRENT_RESEARCH_SCOPE.md) for inclusion boundaries. General Tool Use, RL Post-training and Recursive Self-improvement are no longer standalone discovery domains.

The corpus is a **candidate metadata pool** until a relevance review is performed.

## Current code

- New preparation uses PostgreSQL. Set `KG_DATABASE_URL`, then run
  `python scripts/prepare_pipeline.py --run-id fresh60 --run-dir /private/run60 run`.
  Reuse that logical ID for claim, submit, status and snapshot. Full pagination
  uses `scripts/prepare_full_pipeline.py`. See [PostgreSQL preparation](docs/POSTGRES_PREPARATION.md).
- `scripts/build_snapshot_graph.py --snapshot <prepared.json> --job-dir <graph-job> --neo4j run` publishes approved domain graph events to Neo4j. Configure `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` and optional `NEO4J_DATABASE` in the process environment. PostgreSQL owns graph source pins, queue, audit, proposals, semantic registry, embedding cache and Neo4j outbox. Set `KG_GRAPH_DATABASE_URL` (or `KG_DATABASE_URL` / `DATABASE_URL`); the job directory holds a credential-free `graph-job.json` run identity. See [PostgreSQL graph operations](docs/POSTGRES_GRAPH.md).
- The SQLite corpus commands below are historical explicit-path workflows; they
  do not feed a new PostgreSQL preparation run.
- `scripts/collect_papers.py`: source search from OpenAlex, Semantic Scholar, and arXiv; candidate date gate, topic tagging and SQLite upsert. This CLI does **not** invoke any LLM.
- `src/kg_builder/collectors.py`: real HTTP API collectors, retry/backoff, source record capture. Network API calls have **not** been executed or tested in the original restricted environment.
- `src/kg_builder/storage.py`: SQLite Paper / PaperVersion / source record / extraction record / discovery hit / topic tables. Strong-ID dedup is partial; nonmatching IDs are not blindly merged by title.
- `scripts/import_reviewed.py`: accepts JSON extraction records produced and reviewed independently; does not invoke LLM.
- `scripts/apply_corpus_batch.py` is retired: both its CLI and imported `run` refuse before opening storage or writing files. Use native PostgreSQL preparation and snapshot graph commands; [the corpus batch workflow](docs/CORPUS_BATCH_WORKFLOW.md) is historical.
- `src/kg_builder/extraction_api.py`: optional, **disabled by default; live API not exercised**, OpenAI-compatible draft extractor. Its v0.3 output must match the requested paper/version and schema; collection does not trigger it.
- `scripts/semantic_pipeline.py`: explicit selected-source import between native PostgreSQL runs, candidate problem clustering, review export/import and hybrid retrieval. See [PostgreSQL graph operations](docs/POSTGRES_GRAPH.md).
- `scripts/publish_incremental_graph.py` replays an approved delta against a completed graph and writes a separate release. `scripts/scheduled_refresh.py` calls the same workflow when explicitly enabled. See [incremental graph publishing](docs/POSTGRES_GRAPH.md#incremental-graph-publishing).
- `config/discovery_queries.json`: current three-domain queries; each source has different search syntax and must be validated in a network-enabled runtime.

## Setup

```bash
python -m pip install -e .
python scripts/collect_papers.py --db data/research_knowledge.sqlite3 --source all --per-query 10 --since 2025-01-01
```

`--until` defaults to today's UTC date. `--source all` queries OpenAlex and arXiv; Semantic Scholar is selected explicitly. For smaller initial runs use one source such as `--source openalex --per-query 5`.

Optional environment variables: `OPENALEX_API_KEY`, `SEMANTIC_SCHOLAR_API_KEY`, `HTTP_USER_AGENT` (supply a contact string required by providers). Never commit credentials.

Import a ChatGPT-assisted, reviewed JSON batch:

```bash
python scripts/import_reviewed.py --db data/research_knowledge.sqlite3 --input /path/to/reviewed_extractions.json
```

New extraction uses [v0.3 retrieval](docs/RETRIEVAL_EXTRACTION_V0_3.md): `paper_id`, `version_id`, pinned `paper_version`, provenance/status fields and four arrays: `contributions`, `research_problems`, `methods`, `benchmarks`. It does not require summary, findings, limitations, artifacts, metrics or evaluations. Existing legacy and v0.2 records remain supported and are not destructively migrated. Reading an author's claim does not establish or reproduce it.

For local CPU embeddings, HNSW and Leiden:

```bash
python -m pip install -e '.[semantic]'
python scripts/semantic_pipeline.py --run-id semantic-reviewed ingest --source-run-id prepared-graph --extraction-id extr:SELECTED_ID
```

Initial problem clustering proposes groups; reviewed decisions alone establish canonical identities. New descriptions retrieve existing group/entity representatives. Method and benchmark identity is never merged merely because vectors are close.

## Persistence and data governance

The SQLite database and extraction drafts are intentionally **not tracked in this public Git repository**. The current 365-candidate review and its accepted extraction/semantic data are saved in the user's existing connected project, with per-batch snapshots and audit packets. The first bootstrap sample (20 paper records, 20 abstract-level drafts) resides in the user's connected Google Drive project folder, not in the repository. That sample was sourced from publicly accessible paper pages; it was **not** obtained by the three live API collectors and is **not** an exhaustive review.

Write to a local SQLite copy and sync it back to Drive only after consistent backup and integrity checking. Keep a snapshot and avoid concurrent writers. Use the current scope and extraction/workflow guides above; older handoff and six-domain pilot documents are historical. The PostgreSQL semantic registry contains explicitly ingested source records; re-ingest changed extraction IDs before reviewing their new candidates.

## Streaming preparation and snapshot graph test

New runs use independently keyed item review and bounded concurrency. A failed item gets one repair, then is dropped from downstream after a second failed review; other approved items remain usable. Completeness diagnostics no longer gate whole papers. Graph Jev requests share a configurable budget of eight, while generative API preparation/naming use three workers. Old run configurations preserve their original policy. See [the current contract](docs/PARALLEL_ITEM_WORKFLOW_CONTRACT.md) and [the fresh parallel60 report](docs/reviews/parallel60-20261101/STAGE_REPORT.md). The following timing paragraph describes the earlier paper-gate test.

The [streaming workflow](docs/STREAMING_WORKFLOW.md) now owns source collection/cleaning, one Jev scope decision, external/API draft tasks, Jev field checks and bounded repairs, durable leases and an immutable prepared vector snapshot. A separate graph job consumes that whole snapshot and measures candidate construction, identity/coherence decisions, naming and retrieval index construction. Preparation never builds shared entities or neighbor graphs. OpenAlex and arXiv are the only sources in the new run.

The fresh 60-candidate test used real Jev decisions and external assistant workers for generation; the optional generative API was not called. All 60 candidates were processed: 47 passed scope, 5 were excluded and 8 remained scope-pending. After 47 initial extractions and 81 repairs, 11 passed field review and 36 remained field-pending. The strict 0.7 completeness-confidence rule caused repeated repairs and is an efficiency/quality-policy limitation, not evidence that the remaining papers are irrelevant. Keep all pending drafts and review histories. Preparation ran for 718.6 seconds, followed by 3.2 seconds for 81 vectors and snapshot sealing; timings overlap across workers and API stages. The independent graph timing uses only the 11 accepted papers. That job took 295.6 seconds, including 257.6 seconds of Jev requests and 1.68 seconds of measured local graph/import/index work; it retains eight pending mentions. Uploaded/downloaded recovery verified five SQLite stores and cold hybrid queries without replaying API calls. See the [cumulative report](docs/reviews/streaming-workflow-20261101/STAGE_REPORT.md) for exact scope, fixed commits, review findings, live results and restore evidence.

## Implementation status

The fixed 365-candidate pool has been screened in three batches (122 / 122 / 121): **171 include, 192 exclude, 2 pending**. There are 165 new four-array extractions and six reused reviewed pilot records. Most extraction is abstract-level; selected primary-source sections and supplementary primary abstracts are labeled separately. One admitted record has only a stored 2025 year plus a related verified 2026 publication, so its exact first-public date and source-version correspondence remain explicitly unverified. Pending records have no fabricated extraction. Original metadata, versions and 46 previous extraction payloads are retained. [The corpus stage report](docs/reviews/corpus-preprocessing-20261101/STAGE_REPORT.md) records development, independent scope/field/identity reviews, test logs and persistence checks. The API adapter remains disabled by default.

Historical pilot: On 2026-10-31 the v0.3 workflow passed 56 repository tests and independent real-data checks. A five-paper initial batch plus MemSkill increment produced 7 reviewed problem groups, 8 methods, 11 benchmarks, 37 source-description associations and 6 paper-local contributions. Real local 384d embeddings, HNSW, Leiden and hybrid queries were exercised. [The cumulative report](docs/reviews/retrieval-pilot-20261031/STAGE_REPORT.md) retains the first review's defects, fixes, limits and validation evidence. This is a small functionality pilot, not a corpus-wide quality or Agent benchmark. LLM API calls, scheduled refresh and Neo4j deployment were not run.
