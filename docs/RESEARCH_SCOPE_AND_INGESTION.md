# Research scope & incremental ingestion design (draft v0.1)

Status: requirements specification; **not** a validated ingestion system.

## Literature scope

- Date: **2025-01-01 onward** (inclusive; date refers to first public publication/version when known).
- Topic umbrella: **Research on LLM and AI-agent memory, context, tool use, RL post-training, recursive self-improvement, and skill management/evolution**.
- Controlled subdomains:
  1. Agent memory: formation, storage, retrieval, update, consolidation, skill/procedural memory.
  2. Context management: context compaction, compression, selection, long-horizon context lifecycle and cache-aware context management where relevant.
  3. Tool use: planning, tool calling, tool selection, tool learning, execution reliability and evaluation.
  4. RL post-training: RLHF/PPO/GRPO/reasoning RL, reward design, verifiers and post-training evaluation. DPO and related preference optimization are included as adjacent comparators, not mislabeled as RL.
  5. Recursive self-improvement: agents that improve prompts, skills, policies, tools or code through iterative evaluation, feedback or self-revision.
  6. Skill and self-improving skill: reusable procedural skills, natural-language or code skills, acquisition/generation, retrieval/triggering, composition, maintenance, verification, self-revision and skill library evolution.
- Relevance gate: a paper must make a substantive methodological, empirical, or benchmark contribution to at least one subdomain. Apply multiple topic tags as appropriate.
- Exclude general LLM announcements, unrelated retrieval systems, generic optimization and unrelated RL without a meaningful connection to the declared scope.
- Boundary note: this domain is broader than the initial Agent Memory only pilot. Papers may have multiple topic tags; skill evolution is a distinct domain that overlaps memory and recursive self-improvement. Ontology classes should remain domain-neutral; topic tags and subdomain taxonomies can grow. Include 2025+ papers in the production corpus; the earlier 2023/2024 pilot is illustrative historical material, not automatically included in the target corpus.

## Data sources / discovery

Primary sources: OpenAlex, Semantic Scholar, arXiv. Maintain source-specific search queries, source IDs, retrieved timestamps and metadata snapshots. Use candidate deduplication (DOI/arXiv ID, then conservative title matching), title/abstract relevance gating and bounded citation expansion. Keep a document store for abstracts/full text and stable source locators.

## Incremental data pipeline (not CI/CD)

Use **scheduled incremental ETL / incremental ingestion** to:
1. Discover new and recently modified paper metadata in bounded time windows.
2. Reconcile identifiers and versions, then filter for relevance.
3. Compare content and extraction-version hashes; process only changed inputs.
4. Obtain a reviewed structured extraction record (manual ChatGPT-assisted workflow for personal use; optional LLM API adapter for automation).
5. Validate schema, evidence locators and provenance; stage before graph writes.
6. Idempotently upsert paper-specific contributions/evaluations, globally shared entities and edges.
7. Rebuild only changed nodes' normalized search text, sparse index entries and dense embeddings.
8. Record checkpoints, execution reports and a recoverable backup.

**CDC** (change data capture) usually tracks updates in an existing source database/log; this project primarily uses **incremental API ingestion** instead.

## Two extraction paths

### A. Manual / ChatGPT-assisted (intended personal workflow)

Provide paper text, PDFs or accessible paper URLs to ChatGPT for extraction and human review. Save reviewed JSON documents carrying paper identifier, source text locator, extraction date, extractor label and schema version. Import those records through the same validation and graph writer as automated records.

ChatGPT in a conversation does **not** have direct autonomous access to an offline Colab/Neo4j instance. A scheduled ChatGPT research report is not equivalent to a live automated database update; automated database updates require an accessible connected execution/write path.

### B. Optional API extraction (assignment scaffold)

Expose an OpenAI-compatible Chat Completions adapter with the same structured output contract. It is **disabled by default** and must never be represented as tested, enabled or deployed unless independently demonstrated.

The schedule entrypoint is also **disabled by default**. It is only a placeholder for a future trigger and uses the same extractor interface.

## Academic reporting

- **Implemented**: only files and interfaces actually committed to the repository.
- **Demonstrated**: only pathways that have been run and verified.
- **Proposed / not tested**: LLM API extraction, unattended scheduled updates and automatic Neo4j refresh until demonstrated.

No automatic LLM calls or timed ingestion tasks should execute merely from importing these modules.
