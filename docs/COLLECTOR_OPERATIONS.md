# Metadata collection operations and limits

## Run API collection (networked Work or Colab only)

```bash
python -m pip install -e .
python scripts/collect_papers.py --source openalex --per-query 5 --db data/research_knowledge.sqlite3
python scripts/collect_papers.py --source semantic_scholar --per-query 5 --db data/research_knowledge.sqlite3
python scripts/collect_papers.py --source arxiv --per-query 5 --db data/research_knowledge.sqlite3
```

Then inspect table counts, sample abstracts, source records and topic annotations before expanding:

```bash
python - <<'PY'
from sqlite3 import connect
c = connect('data/research_knowledge.sqlite3')
for t in ('papers','paper_versions','source_records','paper_topics','discovery_hits','extraction_records'):
    print(t, c.execute(f'SELECT count(*) FROM {t}').fetchone()[0])
print('integrity:', c.execute('PRAGMA integrity_check').fetchone()[0])
c.close()
PY
```

Collector outputs **candidate records**. Query matches are not an acceptance decision; relevance filtering and provenance checking are still needed. OpenAlex/S2/arXiv metadata can disagree or have partial dates. Duplicate DOI, preprint/accepted versions and citation expansion require further work.

### API limitations not yet tested

- OpenAlex: relevance search with date filter; initial implementation returns only first page of at most 100 per query. Contact or API token may be required depending on current API policy.
- Semantic Scholar: relevance search with year filter, first page; API key optional depending on rate limits; some papers lack exact publication dates. `year` does not necessarily establish the earliest public preprint date.
- arXiv: Atom API, last-submitted-date sort, first page; `id` may contain vN, otherwise version is unverified. Search grammar and overlap need live verification.
- All three may rate-limit; retries/backoff are implemented but not guaranteed sufficient. Query logs and checkpoints are a future enhancement.
- The requested publication window begins with **first public release 2025-01-01**, which metadata from a single source may not establish definitively.

### Version and dedup model

A stable `Paper` can link several `PaperVersion` records. If an upstream identifier gives no version, the store uses a clearly labelled `@unverified` snapshot, not a fake `v1`. Source records store original upstream payload where returned. Current-version selection should be verified against the authoritative source; the importer currently assigns it only for explicit version labels.

### Sensitive/large files

SQLite, scraped fulltext, credentials and manual review data are kept out of public Git. Store them in Drive with backups. Avoid downloading paywalled PDFs without rights. Do not run the optional LLM adapter or scheduled pipeline during this phase.

## Identity, versions, and offline reconciliation

The default `collect_papers.py --source all` queries OpenAlex and arXiv only. The optional Semantic Scholar adapter is selected only by explicitly passing `--source semantic_scholar`; historic records remain in SQLite. Metadata collection does not invoke the extraction API or scheduler.

`papers.current_version_id` names a source-confirmed numeric `vN` version only. A NULL value on a metadata-only paper is intentional. To display its best available snapshot, call `preferred_metadata_version(connection, paper_id)` and show the metadata status, rather than asserting a verified current version. New metadata snapshots retain distinct source/content combinations. Confirmed `v10` remains current when a later `v2` arrives. DOI and arXiv identity resolution uses explicit source IDs, DOI aliases and recognized arXiv identifiers; title similarity never merges records.

For historic databases, run `python scripts/reconcile_sqlite.py --input candidate.sqlite3 --input seed.sqlite3 --output reconciled.sqlite3 --report private-report.json` with new private output and report paths. The first input's paper IDs win in merged groups. Inputs are read-only; the output is built transactionally. The JSON report maps old paper/version IDs to canonical IDs and lists unresolved strong-ID conflicts. Every original row and payload is retained verbatim in `reconciliation_audit`, with relational extraction fields and active extraction JSON mapped to canonical IDs. `paper_redirects` persists the mapping; repeated reconciliation carries prior audit and redirect rows. Distinct metadata snapshots remain separate. Review unresolved conflicts before treating any ambiguous identities as one scholarly work. The tool copies the named ingestion tables; unrelated custom tables require a separate migration. Keep database and report private, outside Git.

## Validated extraction import (offline)

Install the declared `jsonschema>=4.23,<5` dependency and run `python scripts/import_reviewed.py --input reviewed.json --db research.sqlite3`. The CLI validates every JSON item before opening the database. An unversioned record follows the deliberately narrow legacy profile; an explicit `schema_version` must equal `research-kg-extraction/0.2-draft-pilot`. These are executable JSON Schema 2020-12 profiles packaged in `kg_builder.schemas`; they are candidate extraction contracts, not a frozen ontology. Known fields have typed constraints while supplementary fields may be retained. Nonfinite numbers, unknown versions, missing or duplicate evidence IDs, unresolved evidence refs, invalid relation kinds/endpoints, mismatched evaluation targets/metric units, cross-version contribution/evaluation keys, capture hash/URL mismatches, and a pinned PaperVersion differing from SQLite are rejected.

`ingest_extractions(connection, records)` validates the entire batch and writes it inside a savepoint. A later SQL failure rolls back every item. If a caller already has a transaction, the caller still controls commit or rollback. Reimporting an unchanged extraction preserves its stored ID, payload bytes, status and extraction timestamp. A changed record with the same natural key updates the existing ID inside the batch. Draft status and source scope remain as supplied; importing does not imply full-text verification or review promotion. The old `ingest_extraction` function calls this batch path for one record.

The offline reconciler carries `relevance_reviews` with paper redirects and original review rows in the audit. It retains original extraction JSON bytes when paper/version IDs do not change; when IDs change, it rewrites structured exact IDs and version-scoped keys/relation endpoints and validates declared v0.2 payloads. The reconciler rejects unfamiliar tables or added columns in supported tables before creating an output, so new data requires an explicit retention migration.
