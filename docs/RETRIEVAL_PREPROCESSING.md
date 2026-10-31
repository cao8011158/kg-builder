# Retrieval sidecar v1

This is a disposable SQLite index built from the master store and an optional private metadata collection. It never changes the master or the ontology/entity-resolution tables. It performs no network or LLM calls.

```bash
PYTHONPATH=src python -m kg_builder.retrieval_preprocess \
  --master /path/to/restored/live.sqlite3 \
  --enrichment /path/to/private/enrichment.json \
  --output /path/to/private/retrieval.sqlite3
```

The complete corpus is read once into an atomic replacement file. `get_paper(index_path, paper_id)` reads a single indexed paper and only its related rows. A changed master or enrichment input requires a new build. The index records build time, collection time, a computed master file SHA-256, the supplied input backup SHA-256, and the enrichment file SHA-256. A supplied `source_backup_sha256` must equal the actual master input file digest.

## Enrichment contract

```json
{
  "schema_version": "retrieval-enrichment/1",
  "collected_at": "2026-10-31T00:00:00Z",
  "source_backup_sha256": "64 hexadecimal characters matching the master file",
  "works": [{
    "paper_id": "arxiv:2501.00001",
    "provider": "openalex",
    "match_status": "matched",
    "match_basis": "existing_openalex_id",
    "work_id": "https://openalex.org/W123",
    "title": "Example",
    "abstract": "Existing OpenAlex abstract if present",
    "authors": [{"name": "Ada", "author_id": "https://openalex.org/A123"}],
    "cited_by_count": 0,
    "primary_topic": {"id": "https://openalex.org/T1", "display_name": "AI", "score": 0.7},
    "topics": [{"id": "https://openalex.org/T1", "display_name": "AI", "score": 0.7,
      "subfield": {"id": "S1", "display_name": "Artificial intelligence"},
      "field": {"id": "F1", "display_name": "Computer science"},
      "domain": {"id": "D1", "display_name": "Physical sciences"}}],
    "keywords": [{"id": "https://openalex.org/K1", "display_name": "memory", "score": 0.4}],
    "referenced_works": ["https://openalex.org/W2"],
    "related_works": ["https://openalex.org/W3"],
    "field_status": {"topics": "present"},
    "arxiv_status": "matched",
    "arxiv_categories": ["cs.AI"],
    "arxiv_primary_category": "cs.AI"
  }]
}
```

`match_status` and `arxiv_status`: `matched`, `not_found`, `error`, or `unrequested`. `match_basis`: `existing_openalex_id`, `doi`, `arxiv_url`, `title_authors`, or null. Matching is supplied by collection, never inferred from titles inside preprocessing. Errors may have an `error` string. For a matched work, present `topics: []` means confirmed empty; absent `topics` and absent `primary_topic` means unrequested. Missing `cited_by_count` stays SQL NULL, distinct from zero. Optional `master_file_sha256` is also checked against the input file. The collection JSON must not contain duplicate or unknown master paper IDs.

## Data and tasks

- `papers`, `versions`, `sources`, and `aliases` retain master IDs and version pointers. Available original arXiv/Semantic Scholar classes are separate `original_class` labels. Master candidate topic labels remain `candidate_topic`, with their source, and never count as standardized OpenAlex classifications.
- OpenAlex topic, primary topic, and keyword rows retain IDs, scores, origin, and nested field/subfield/domain payload. `references` and `related_work` are distinct edge types; their OpenAlex targets are external IDs, not automatically reconciled into local papers.
- Existing contribution objects retain extraction ID, version ID, extractor, source scope, draft/review status, and original object. Their descriptions are not treated as globally resolved scientific concepts.
- `tasks` records a `contribution_summary` only where no extracted contribution exists; `needs_text` flags papers without an abstract. A `classification` task is actionable only when OpenAlex classification is confirmed absent. Metadata errors and unrequested fields are explicitly deferred. These are work queues, not automatic LLM execution.

The master’s historical draft extraction count is included in the build report; contributions count individual objects and distinct papers separately.
