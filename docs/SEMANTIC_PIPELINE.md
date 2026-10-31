# Reviewed semantic sidecar

The master SQLite database is read with `mode=ro`; extraction IDs are selected explicitly. The sidecar stores mention text, source fingerprints, canonical entities, reviews, FTS5 text search and model/text keyed embedding cache. A changed extraction replaces its mentions in one transaction, so former memberships disappear. Contributions remain tied to their paper version and are searchable without a canonical entity.

Install the project and vector dependencies in the local environment. The default local embedding model is `BAAI/bge-small-en-v1.5`; no LLM call or semantic merge occurs in this pipeline.

```sh
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 ingest --master master.sqlite3 --extraction-id extr:ID1 --extraction-id extr:ID2
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 --cache-dir /local/model-cache cluster-problems --output problem-clusters.json --k 20 --min-similarity 0.65 --resolution 0.05 --seed 42
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 export-candidates --kind ResearchProblem --output problem-packet.json
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 export-candidates --kind Method --output method-packet.json
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 export-candidates --kind Benchmark --output benchmark-packet.json
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 import-review --input reviewed.json
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 search 'agent memory' --limit 10
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 entity rp:memory --limit 20
python scripts/semantic_pipeline.py --sidecar semantic.sqlite3 paper arxiv:2501.12345 --limit 20
```

Initial problem clustering returns proposed groups and pairwise edges, plus source and mention fingerprints. It assigns no identity. Inspect those groups manually, then use candidate packets to make explicit decisions. Future imports feed only new or changed pending mentions through the candidate lookup against existing canonical entities; do not run the initial clustering again. Method and Benchmark packets also search existing representatives. Similarity scores are suggestions, never merge thresholds. A packet contains the exact source item, original roles, benchmark version/subset, candidates, source and mention hashes, candidate revisions and a precondition token.

A review file follows `research-kg-semantic-review/1`:

```json
{
  "schema_version": "research-kg-semantic-review/1",
  "review_id": "review-2026-10-31-a",
  "entities": [
    {"entity_id": "rp:memory", "kind": "ResearchProblem", "name": "Memory retention", "description": "Agents retaining useful context", "aliases": ["Memory decay"], "name_origin": "reviewer_generated"}
  ],
  "assignments": [
    {"mention_id": "mention:FROM_PACKET", "model_id": "MODEL_ID_FROM_PACKET", "precondition": "PRECONDITION_FROM_PACKET", "candidate_revisions": {}, "decision": "new_entity", "entity_id": "rp:memory"}
  ]
}
```

Copy `mention_id`, `precondition`, `candidate_revisions`, and the packet's `model_id` exactly into each assignment. Choices are `new_entity`, `match`, and `pending`. `new_entity` refers to a declared entity in the same file; multiple mentions can map to it. `match` refers to an existing entity of the same kind and requires `target_revision` copied from that candidate (or fetched from a separate entity lookup if reviewed outside the candidate list). `pending` sets `entity_id` to null and leaves the mention for later review. Every declared entity needs at least one assignment. For a source-authored canonical name, at least one assigned mention must use that exact name; other assigned names must appear in `aliases` (for example, LoCoMo and LOCOMO). Method and Benchmark names are source-authored; ResearchProblem may use a reviewer generated name. Canonical IDs are stable and cannot be redeclared in a later review; aliases remain attached to the canonical entity. Benchmark roles and version/subset stay on each original mention, not canonical identity.

Imports reject unknown IDs, kind mismatches, duplicate IDs, stale source/mention/model preconditions, stale candidate or target revisions, and conflicting reused review IDs. An identical review is idempotent. A review transaction either applies every assignment or none. The SQLite commit is authoritative: if subsequent in-memory HNSW maintenance fails, import reports `index_status: rebuild_required` and the next dense query rebuilds the affected kind from SQLite. Retrying the identical review remains an idempotent `unchanged` response. A source update revokes older memberships, including those previously reviewed; a fresh packet and review are needed.

`get_entity` and `get_paper` return at most 100 mentions with `next_offset` for continuation. The paper view also includes at most 100 contributions and `contributions_next_offset`. `search` returns at most 100 results, expands each entity to at most 20 paper relations and includes `papers_truncated`. FTS5 BM25 and HNSW candidate ranks use reciprocal rank fusion. On first dense request for a kind in a process, its canonical vector index loads once from the sidecar (cold O(number of entities of that kind)); later detail reads use SQLite ID/relationship indexes, and a newly reviewed entity is upserted into its kind index. Sparse FTS, exact canonical name and same-kind HNSW candidates are combined before a bounded rank. The vector cache keys include model ID and text digest. A new model ID obtains fresh embeddings. Search ranks and clustering depend on the selected embedding model; human decisions remain the only identity authority.
