# Research KG Builder

Research paper discovery and storage layer for a scholarly Knowledge Graph. **The graph writer is not implemented yet.**

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
- `src/kg_builder/extraction_api.py`: optional, **disabled, untested** OpenAI-compatible Chat Completions draft extractor for assignment integration. Drafts require human/schema/evidence review; not triggered by collection.
- `scripts/scheduled_refresh.py` and `src/kg_builder/incremental_update.py`: **disabled/unfinished** scheduled ingestion placeholders. Do not report them as operating background jobs.
- `config/discovery_queries.json`: current three-domain queries; each source has different search syntax and must be validated in a network-enabled runtime.

## Setup

```bash
python -m pip install -e .
python scripts/collect_papers.py --db data/research_knowledge.sqlite3 --source all --per-query 10 --since 2025-01-01
```

`--until` defaults to today's UTC date. By default this queries three live public APIs. For smaller initial runs use one source such as `--source openalex --per-query 5`.

Optional environment variables: `OPENALEX_API_KEY`, `SEMANTIC_SCHOLAR_API_KEY`, `HTTP_USER_AGENT` (supply a contact string required by providers). Never commit credentials.

Import a ChatGPT-assisted, reviewed JSON batch:

```bash
python scripts/import_reviewed.py --db data/research_knowledge.sqlite3 --input /path/to/reviewed_extractions.json
```

A valid input is a JSON array of objects containing `paper_id`, `version_id`, `summary`, `methods`, `research_problems`, `findings` and `evidence_locators`. Each evidence locator requires `source_url` and `section`. `status` should be `draft_requires_review` until evidence is confirmed. See repository review documentation for pending ontology fields.

## Persistence and data governance

The SQLite database and extraction drafts are intentionally **not tracked in this public Git repository**. The first bootstrap sample (20 paper records, 20 abstract-level drafts) resides in the user's connected Google Drive project folder, not in the repository. That sample was sourced from publicly accessible paper pages; it was **not** obtained by the three live API collectors and is **not** an exhaustive review.

Write to a local SQLite copy and sync it back to Drive only after clean closure and `PRAGMA quick_check`. Keep a snapshot and avoid concurrent writers. Read `docs/WORK_SESSION_HANDOFF_2026-10-31.md` before beginning the Work session.

## Implementation status

Offline SQLite idempotency/source/extraction smoke checks passed on 2026-10-31. No actual public API crawl, LLM API call, scheduled refresh, Neo4j writer, entity resolution, embedding or retrieval benchmark was run in this submission. See handoff doc for remaining tasks.
