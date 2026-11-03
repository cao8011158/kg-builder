# Optional extraction and scheduled publishing

The optional `OpenAICompatibleExtractor` in `src/kg_builder/extraction_api.py` is disabled by default. It reads `LLM_API_KEY` when explicitly enabled and invoked.

The [incremental publishing command](POSTGRES_GRAPH.md#incremental-graph-publishing) consumes a prepared, approved snapshot batch. An external scheduler can call the same command through the default-disabled `scripts/scheduled_refresh.py` entrypoint.
