# Deterministic corpus batch import

This historical SQLite corpus/semantic-sidecar import does not populate a new
PostgreSQL preparation run. New runs use the preparation CLI with `--run-id`.

`scripts/apply_corpus_batch.py` imports human supplied relevance decisions and v0.3 retrieval extractions. It does not call a model, decide relevance, fetch papers, or infer missing content. Keep the source manifest and reviewed JSON as auditable inputs; back up the master before applying a batch.

```bash
python scripts/apply_corpus_batch.py \
  --master data/research_knowledge.sqlite3 --sidecar data/semantic.sqlite3 \
  --input-manifest batch-input.json --reviews batch-reviews.json \
  --extractions batch-extractions.json --batch-id corpus-batch-1 \
  --report batch-report.json
# Review the report, then rerun the same command with --apply.
```

The manifest is a JSON array with one object per supplied paper: `paper_id`, `paper_version` (at least `version_id`, `version_label`, `source_url`; additional source metadata is allowed), and boolean `already_processed`. Every paper has exactly one review object with `paper_id`, `decision` (`include`, `exclude`, `pending`), canonical `topics` (from `agent_memory`, `context_management`, `skill_evolution`), `reason`, and `source_scope`; extra audit fields are retained in `payload_json`. Include reviews need at least one topic; exclude and pending reviews may use `[]`. Include and unprocessed papers need exactly one validated v0.3 extraction pinned to the same master paper/version/source and matching review `source_scope`. Excluded, pending, and reused papers have no extraction in the input. A reused record must already have a pinned v0.3 extraction in the master with `reviewed_for_retrieval_not_reproduced` or `reviewed` status. Missing or mismatched source metadata is an error; correct the source input and review it before retrying.

Without `--apply`, the master is opened read only and the sidecar is untouched. A valid dry run writes only the requested JSON report. With `--apply`, reviews and new extractions commit together in one master transaction. Repeating an identical batch preserves the original rows and timestamps. Changed data with an existing review ID or extraction identity is rejected; use an explicitly new identity for a revised review or extraction. Existing master rows remain present.

After the master commit, missing batch extraction IDs are imported into the semantic sidecar in its own transaction. The report's `stage` distinguishes `validated`, `master_committed`, `sidecar_failed`, and `sidecar_committed`; a sidecar failure writes `sidecar_failed` with an error and exits nonzero. Rerun the same `--apply` command to retry missing sidecar records without rewriting the master. There is no distributed transaction across the two databases. No sidecar is created for invalid inputs, and invalid input leaves an existing report alone.
