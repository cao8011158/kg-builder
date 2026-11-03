# Retrieval extraction v0.3

This pilot stores a small paper-version record for retrieval over 2025 and later work on agent memory, context management, and skill evolution. It records author-claimed contributions, research problems, methods, and benchmarks. It does not assert that a result was independently reproduced.

The schema identifier is `research-kg-extraction/0.3-retrieval`. Each record requires `paper_id`, `version_id`, `paper_version` (`version_label`, `source_url`), `extractor_type`, `extractor_version`, `source_scope`, `status`, and all four arrays. A confirmed version uses a `vN` label; an unverified metadata snapshot uses `null`. The label and URL must match the stored `paper_versions` row on import.

```json
{
  "schema_version": "research-kg-extraction/0.3-retrieval",
  "paper_id": "arxiv:2501.12345",
  "version_id": "arxiv:2501.12345@v1",
  "paper_version": {"version_label": "v1", "source_url": "https://arxiv.org/abs/2501.12345v1"},
  "extractor_type": "assistant_manual",
  "extractor_version": "2026-10-31-v1",
  "source_scope": "abstract_only",
  "status": "draft_requires_review",
  "contributions": [{"key": "arxiv:2501.12345@v1#c1", "description": "Authors claim a memory system", "section": "abstract"}],
  "research_problems": [{"key": "problem:forgetting", "name": "Forgetting", "description": "Agents lose context"}],
  "methods": [{"key": "method:retrieval", "name": "Retrieval", "description": "Retrieve relevant memory", "role": "proposed"}],
  "benchmarks": [{"key": "benchmark:one", "name": "One", "description": "Task suite", "roles": ["introduced", "used"], "version_or_subset": null}]
}
```

Arrays may be empty where the source does not support an entry. Contributions require a version-scoped key (`version_id#...`). Entity keys must be unique within the extraction, including across arrays. Methods use `proposed`, `used`, or `involved`; benchmark roles include `introduced` and/or `used`. Every item may have a `section` and `source_url`. These are lightweight locators, not a completed evidence audit. Unknown fields, including the v0.2 `summary`, `findings`, `limitations`, `artifacts`, `metrics`, `evaluations`, `relations`, and `evidence_locators`, fail validation. Nonfinite numbers fail validation as well.

The importer validates the complete batch before writes, pins the paper/version and source URL to the local row, and uses a savepoint for atomic writes. It retains legacy and v0.2 validation and storage behavior. The optional API adapter is disabled by default; enabling it requires an API key and network access, and its result is still a draft requiring review. No API extraction is part of this implementation.

## Next steps

Curate source text and review individual draft records for the scoped corpus, then implement retrieval indexing and evaluate retrieval quality using fixed questions and source citations. Embeddings, graph retrieval, and semantic extraction have not been run as part of this schema change.
