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
