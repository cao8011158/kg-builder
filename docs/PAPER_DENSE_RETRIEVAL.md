# Paper dense retrieval

Prepare an index for a frozen PostgreSQL graph run:

```powershell
python -m kg_builder.paper_dense_cli --run-id RUN_ID --index-dir ABSOLUTE_INDEX_DIR prepare --batch-size 64
```

Query that index in the same process for repeated searches, or use the CLI:

```powershell
python -m kg_builder.paper_dense_cli --run-id RUN_ID --index-dir ABSOLUTE_INDEX_DIR search "research question" --top-k 10
```

The encoder is Voyage `voyage-4-large`, 1024 dimensions. Document text is the source title, a newline, and the abstract from `sources.version_id → paper_versions.version_id`. It never uses `papers.current_version_id`. Voyage receives `input_type=document` during preparation and `input_type=query` during search. The existing PostgreSQL `embedding_cache` is the canonical vector store, keyed by the model role identity and text hash. Completed batches persist immediately; a retry reuses them. The index directory contains only a frozen manifest and per-paper IDs, title, source/version provenance, and document hashes. It contains no vector copy.

`PaperDenseIndex` validates the frozen source and loads document vectors once to construct the existing in-memory `HNSWIndex`. Reuse an instance for many queries. Search returns cosine similarity and the original Paper node ID, graph namespace, title, source URL, source hash, version ID, and version label. Search computes or reuses a query-role vector; it does no RRF, BM25, graph expansion, or reranking. Existing `SemanticStore.search` and BM25 behavior are unchanged.

Install the `semantic` and `voyage` extras (`pip install -e '.[semantic,voyage]'`); HNSW comes from `semantic`. Preparation requires the existing PostgreSQL run, `VOYAGE_API_KEY`, and an absolute destination directory that does not contain an unrelated index. `--voyage-concurrency` defaults to 2. Each 64-document checkpoint can fill two of the model's default 32-document Voyage requests. A previously published manifest is accepted only when its run, model, namespace, snapshot, pinned source content, and all document cache rows still match. Missing pinned versions or conflicting pinned text for one Paper are rejected. A source change requires a new index directory. The CLI reports the model's API usage counters; SDK internal HTTP retries are bounded but are not separately observed.
