# Optional extraction and scheduled update scaffolds

**Status: untested, disabled, not an operational pipeline.**

- `src/kg_builder/extraction_api.py` defines `PaperInput`, `OpenAICompatibleExtractor` and a manual opt-in flag.
- `src/kg_builder/incremental_update.py` defines a shared extractor protocol and disabled schedule entrypoint.
- Both files are **interfaces/scaffolds**; the graph writer, review stage, candidate discovery, schema validation, job scheduler, embeddings and database integrations are **not implemented** here.
- API credentials are read from `LLM_API_KEY` only when the extractor is explicitly enabled and invoked; credentials must not be committed.
- For personal use, a manually reviewed extraction record can eventually feed the same validation and graph writer.
- For a report, describe these modules as **proposed / implemented interface, not tested**, rather than claiming operational automatic updating.
