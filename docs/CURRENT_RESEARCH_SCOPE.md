# Current research scope

Updated2026-10-31 by explicit user decision. This scope supersedes six-domain discovery plans in historical pilot/review/handoff documents.

Time window: first publicly available on or after2025-01-01; upper date remains the collection command default or explicit --until.

| Domain key | Scope |
|---|---|
| agent_memory | Long-term agent memory; retrieval, update, consolidation and forgetting/eviction |
| context_management | Context compression/compaction and context management during long-horizon tasks |
| skill_evolution | Skill acquisition, procedural knowledge, reuse, update and self-improving skills |

These domains may overlap. Tool use, RL post-training and general recursive self-improvement are no longer independent discovery targets. They remain relevant when a paper's substantive focus addresses one of the three retained domains; incidental mentions do not suffice. Skill self-improvement stays included.

Active config: ../config/discovery_queries.json (resolved from repository root as config/discovery_queries.json);3domains/9existing query strings. This change narrows discovery keys, retaining each selected domain's existing query strings. It does not claim exhaustive recall of every subtopic.

Existing365candidate papers, annotations and historical six-domain results are retained. No existing record is deleted or retrospectively marked included by this configuration change. A subsequent scoped relevance pass is needed before further LLM extraction. New extraction/clustering work should use a selected in-scope subset rather than the entire historical pool. No recollection, paid LLM call, embeddings or graph materialization is triggered by changing this scope.
