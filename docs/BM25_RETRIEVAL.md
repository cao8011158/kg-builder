# Frozen BM25 graph search

Install the optional `bm25` extra, then build a new directory from a native PostgreSQL graph run:

```powershell
uv sync --extra bm25
python scripts/bm25_retrieval.py build --run-id RUN_ID --index-dir PATH_TO_NEW_INDEX
python scripts/bm25_retrieval.py search --index-dir PATH_TO_INDEX --type Paper --limit 10 "quasar survey"
python scripts/bm25_retrieval.py info --index-dir PATH_TO_INDEX
```

The build reads `KG_GRAPH_DATABASE_URL` (then the GraphStore's usual fallback environment variables). Credentials do not appear in CLI arguments or the manifest. `build(run_id, index_dir)` and `BM25Index(index_dir).search(query, types=None, top_k=10)` are the Python API. Reuse one `BM25Index` instance for warm searches and call `close()` when done.

An index is a frozen snapshot. Build uses a read-only PostgreSQL `REPEATABLE READ` transaction and refuses to overwrite an existing directory. Rebuild to a new path when graph data changes. Its manifest holds the source run ID, PostgreSQL schema, Neo4j graph namespace (`neo4j_run_id`), optional snapshot hash, counts, BM25S version and tokenization settings. Results retain the publisher's exact node ID and type, the source run ID and graph namespace, scoring text, and available version, URL and extraction provenance. Only `Paper`, `ResearchProblem`, `Method`, `Benchmark`, `Contribution` and `ResearchArea` are indexed; `SourceItem` is not a search type.

Paper text is title plus the stored pinned version abstract. Entity text is name, description and aliases. Contribution text is its description. Area text is name plus description. BM25S 0.2.14 uses Lucene BM25 (`k1=1.5`, `b=0.75`) with lowercase English stopword tokenization and its default two-character word pattern. It does not translate Chinese queries into English. Type filtering happens before ranking. Each type has its own IDF statistics; mixed-type scores are useful for candidate discovery but are not calibrated across types. Empty and unknown-term queries return no hits. Search returns at most 4,000 text characters per hit and marks `text_truncated` when needed. This is a BM25-only seed route; it performs no dense retrieval, RRF, reranking, model call, or live database lookup during search.
