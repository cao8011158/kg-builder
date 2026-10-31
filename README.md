# Research KG Builder

Research paper discovery, versioned extraction and reviewed semantic retrieval for a scholarly Knowledge Graph. **The Neo4j graph writer is not implemented yet.**

## Research scope

First publication on/after **2025-01-01**, across three non-exclusive domains:
Agent Memory; Context Management & Compression; and Skill / Self-improving Skill.

See [the current scope](docs/CURRENT_RESEARCH_SCOPE.md) for inclusion boundaries. General Tool Use, RL Post-training and Recursive Self-improvement are no longer standalone discovery domains.

The corpus is a **candidate metadata pool** until a relevance review is performed.

## Current code

- `scripts/collect_papers.py`: source search from OpenAlex, Semantic Scholar, and arXiv; candidate date gate, topic tagging and SQLite upsert. This CLI does **not** invoke any LLM.
- `src/kg_builder/collectors.py`: real HTTP API collectors, retry/backoff, source record capture. Network API calls have **not** been executed or tested in the original restricted environment.
- `src/kg_builder/storage.py`: SQLite Paper / PaperVersion / source record / extraction record / discovery hit / topic tables. Strong-ID dedup is partial; nonmatching IDs are not blindly merged by title.
- `scripts/import_reviewed.py`: accepts JSON extraction records produced and reviewed independently; does not invoke LLM.
- `scripts/apply_corpus_batch.py`: validates complete fixed manifests, relevance reviews and version-pinned v0.3 extractions before optional atomic master writes and retryable sidecar import. See [the corpus batch workflow](docs/CORPUS_BATCH_WORKFLOW.md).
- `src/kg_builder/extraction_api.py`: optional, **disabled by default; live API not exercised**, OpenAI-compatible draft extractor. Its v0.3 output must match the requested paper/version and schema; collection does not trigger it.
- `scripts/semantic_pipeline.py`: explicit selected-source import into a separate SQLite semantic sidecar, candidate problem clustering, review export/import and hybrid retrieval. See [the semantic workflow](docs/SEMANTIC_PIPELINE.md).
- `scripts/scheduled_refresh.py` and `src/kg_builder/incremental_update.py`: **disabled/unfinished** scheduled ingestion placeholders. Do not report them as operating background jobs.
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
python scripts/semantic_pipeline.py --sidecar data/semantic.sqlite3 ingest --master data/research_knowledge.sqlite3 --extraction-id extr:SELECTED_ID
```

Initial problem clustering proposes groups; reviewed decisions alone establish canonical identities. New descriptions retrieve existing group/entity representatives. Method and benchmark identity is never merged merely because vectors are close.

## Persistence and data governance

The SQLite database and extraction drafts are intentionally **not tracked in this public Git repository**. The current 365-candidate review and its accepted extraction/semantic data are saved in the user's existing connected project, with per-batch snapshots and audit packets. The first bootstrap sample (20 paper records, 20 abstract-level drafts) resides in the user's connected Google Drive project folder, not in the repository. That sample was sourced from publicly accessible paper pages; it was **not** obtained by the three live API collectors and is **not** an exhaustive review.

Write to a local SQLite copy and sync it back to Drive only after consistent backup and integrity checking. Keep a snapshot and avoid concurrent writers. Use the current scope and extraction/workflow guides above; older handoff and six-domain pilot documents are historical. The semantic sidecar is a snapshot of explicitly ingested source records; re-ingest changed extraction IDs before reviewing their new candidates.

## Implementation status

The fixed 365-candidate pool has been screened in three batches (122 / 122 / 121): **171 include, 192 exclude, 2 pending**. There are 165 new four-array extractions and six reused reviewed pilot records. Most extraction is abstract-level; selected primary-source sections and supplementary primary abstracts are labeled separately. One admitted record has only a stored 2025 year plus a related verified 2026 publication, so its exact first-public date and source-version correspondence remain explicitly unverified. Pending records have no fabricated extraction. Original metadata, versions and 46 previous extraction payloads are retained. [The corpus stage report](docs/reviews/corpus-preprocessing-20261101/STAGE_REPORT.md) records development, independent scope/field/identity reviews, test logs and persistence checks. The API adapter remains disabled by default.

Historical pilot: On 2026-10-31 the v0.3 workflow passed 56 repository tests and independent real-data checks. A five-paper initial batch plus MemSkill increment produced 7 reviewed problem groups, 8 methods, 11 benchmarks, 37 source-description associations and 6 paper-local contributions. Real local 384d embeddings, HNSW, Leiden and hybrid queries were exercised. [The cumulative report](docs/reviews/retrieval-pilot-20261031/STAGE_REPORT.md) retains the first review's defects, fixes, limits and validation evidence. This is a small functionality pilot, not a corpus-wide quality or Agent benchmark. LLM API calls, scheduled refresh and Neo4j deployment were not run.
