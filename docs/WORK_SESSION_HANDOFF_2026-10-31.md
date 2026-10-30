# Work Session Handoff — Research Knowledge Graph (2026-10-31)

## Mission

Create a **research knowledge base** that pre-extracts paper contributions, methods, relationships, important evaluations and findings so that a local LLM Agent can retrieve/compare knowledge without repeatedly reading every paper. The KG links to original paper versions/sections for drill-down. Later compare this against BM25 and dense text retrieval; **do not start the Agent evaluation phase yet**.

## Repositories

GitHub owner `cao8011158`, all public:

- Main: https://github.com/cao8011158/research-knowledge-system
- KG Builder (current work): https://github.com/cao8011158/kg-builder
- Agent: https://github.com/cao8011158/research-agent

Main repository includes two Git submodules pinned to exact commits; after pushing KG Builder updates, advance the main repository's submodule gitlink. `git clone --recurse-submodules` reproduces the project.

## Domain and time window

Papers first publicly released on or after **2025-01-01**; six overlapping topics:

1. Agent memory (induction, retrieval, maintenance, forgetting, memory structure)
2. Context management and compression (compaction, long horizon context lifecycle)
3. Tool use (selection, execution, learning, reliability)
4. RL post-training (RLHF, PPO, GRPO, reasoning RL, reward/verifiers; DPO as adjacent preference optimization)
5. Recursive self-improvement (agents improving workflows, tools, code, policies)
6. Skill / self-improving skill (skill generation, retrieval, verification, evolution, deduplication)

Use **multi-label classification**. General RAG or unrelated ML literature is excluded unless substantively connected. Metadata from OpenAlex/Semantic Scholar/arXiv plus bounded citation expansion, conservative dedup and relevance filtering. No uncontrolled recursive citation crawl.

## Ontology direction (provisional, not frozen)

Nine node kinds:

- `Paper`: stable scholarly work
- `PaperVersion`: versioned textual source; `Paper-[:HAS_VERSION]->PaperVersion`, `Paper-[:CURRENT_VERSION]->PaperVersion`
- `Contribution`: paper-version-scoped contribution, mechanisms, important claims/findings
- `Method`: transferable algorithm/procedure/strategy
- `ResearchProblem`: normalized research question/problem
- `Artifact`: concrete model, tool, agent, skill, code, prompt, playbook, skill library etc, with `artifact_kind`
- `Benchmark`: benchmark/task/protocol
- `Metric`: measurement definition
- `Evaluation`: paper-version-scoped reported evaluation, optional numerical score, method/artifact, benchmark/metric/setup, evidence locator

Core edges to refine: `HAS_CONTRIBUTION`, `PROPOSES_METHOD`, `INTRODUCES_ARTIFACT`, `ADDRESSES`, `USES_ARTIFACT`, `PRODUCES_ARTIFACT`, `DERIVED_FROM` (only observed lineage), `REPORTS_EVALUATION`, `EVALUATES_METHOD`, `EVALUATES_ARTIFACT`, `USES_BENCHMARK`, `MEASURES`, `CITES`.

Do **not** treat extracted author claims as universal facts, and do not merge version-specific evaluation numbers. `Result`, `Finding`, `Evidence` are not separate v0.2 node kinds; their structured properties and source locators are retained. All KG nodes should expose a derived semantic `search_text` used for both sparse and dense indexing (excluding irrelevant internals). Do not create separate index per node instance. Indexed search and Neo4j graph writer are not yet implemented.

## Data sources, persistence and status

Existing Drive folder: https://drive.google.com/drive/folders/1dJHLKlcTVIfKulVA56fi0Y2A6tVCu6ap

- `database/research_knowledge.sqlite3`: https://drive.google.com/file/d/1hO1VlJN_NOL1mDTyTTxDnopf331-62Q7/view
- `extractions/curated_abstracts.json`: https://drive.google.com/file/d/11x_TVpT5wBwuPyLmfwfpp6jPKbsrSEda/view
- `snapshots/seed_web_discovery.json`: https://drive.google.com/file/d/14Equidq_oRlww_i7Jvmf-18CNg0mK_bp/view
- `snapshots/bootstrap_pre_version_fix.sqlite3`: earlier state, **not** current authoritative database.

The first SQLite bootstrap contains **20 manually curated paper identities and 20 draft abstract-level extraction records** covering six topic labels. Full original abstracts were not fetched through live APIs; drafts were produced from source-page descriptions, and not validated from full paper text. The current authoritative database is the first link above. Keep raw database and artifacts **out of public GitHub**.

## Code committed to KG Builder

- `src/kg_builder/collectors.py`: OpenAlex, Semantic Scholar, arXiv source adapters, original payload preservation, retries
- `src/kg_builder/storage.py`: idempotent SQLite Paper/PaperVersion/source/extraction upsert; source hash and identity rules
- `scripts/collect_papers.py`: API metadata-only collection, six-domain query set, publication cutoff; does NOT call the LLM
- `scripts/import_reviewed.py`: import vetted structured JSON into SQLite; no LLM request
- `src/kg_builder/extraction_api.py`: optional OpenAI-compatible extraction adapter, **disabled, not tested**
- `src/kg_builder/incremental_update.py` and `scripts/scheduled_refresh.py`: disabled/unimplemented scheduling scaffolds
- `config/discovery_queries.json`: editable source query terms

Offline SQLite smoke check passed. Python syntax compiled. **Network collectors have never succeeded in the restricted Python execution environment; Work session must verify them with live API calls.** Live API pagination/completeness and rate limits have not been validated. It is acceptable to improve collector implementation when issues are found.

## Next tasks in Work (ordered)

1. **Verify Work cloud computer network access** to `api.openalex.org`, `api.semanticscholar.org`, and `export.arxiv.org`. Run 1–3 small queries per source and log status, errors, endpoint behavior, metadata coverage, licensing and rate limits. Don't infer support from browser access alone.
2. **Recover/backup the Drive SQLite database** and work on a local copy. `PRAGMA integrity_check` before and after. Do not overwrite existing Drive data without a recoverable snapshot.
3. **Run only metadata collection** initially, using the repository's `scripts/collect_papers.py`; **do not call LLM API** and do not enable the scheduled updater. Start with small batches, compare counts by source and domain, inspect duplicate IDs/version metadata. Extend API pagination/date queries as necessary; log run manifest.
4. Clean and filter collected candidates against six-domain scope, metadata ID resolution, title/abstract and source validity. Some records may be preprints; avoid collapsing distinct works just from title similarity. Respect the first-release date rule.
5. **Use ChatGPT to parse and summarize selected real papers**: review actual abstracts/fulltext, collect verifiable contribution/method/problem/artifact/benchmark/evaluation/finding/evidence locator records; store JSON into SQLite via reviewed import. Avoid asserting full-text verification for abstract-only cases. The local LLM API adapter remains disabled/untested by request.
6. Persist updated SQLite and source manifests to Drive, with backup and verification. If necessary, upload a new snapshot and update which Drive file is authoritative; do not produce duplicate anonymous versions.
7. Finalize `ONTOLOGY_V0_2.md` including detailed attributes, edge constraints, stable IDs, provenance/version semantics. Then implement Neo4j writer, normalization and dense/sparse indexes. This comes **after** data ingestion verification.
8. At a later project phase, deploy the local LLM Agent with natural-language sparse/dense node grounding + graph traversal + document drilldown, then benchmark BM25, dense, KG sparse/dense/hybrid under identical query sets. **Not now**.

## Architecture assumptions and caveats

- SQLite is the persistent source/extraction store. Neo4j is the future structured graph. Drive stores backups and database; GitHub holds code/docs, not bulk binary files.
- User requested API extraction and scheduled incremental ETL code **to exist but not be run or tested**. Do not label it implemented end-to-end; describe it as disabled interface/scaffold. Real user-facing extraction is ChatGPT-assisted, with records explicitly reviewed/imported.
- The nine-node ontology is provisional; source-grounded schema validation across six domains remains open.
- Authenticated Google Drive connector and GitHub are connected in ChatGPT. Work might have independent permissions. Verify before writes.
- A scheduled ChatGPT task that sends a report does not itself write a live SQLite DB. Persisted automated updates require an actual executable workflow and authenticated write-back.

## Expected outputs of next Work session

A small authenticated live collection report (source, query, date filters, counts, exceptions), up-to-date local SQLite copy, validated backups in Drive, exact paper/abstract coverage and draft status; GitHub commits only for code/doc changes. Refrain from claiming Neo4j or Agent experiments have been completed.
