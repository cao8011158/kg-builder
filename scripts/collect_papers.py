"""Run bounded metadata discovery and SQLite upserts. No LLM calls."""
import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import sys
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from kg_builder.storage import connect, upsert_paper
from kg_builder.collectors import openalex_search, arxiv_search, semantic_scholar_search

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / 'config' / 'discovery_queries.json'
COUNT_TABLES = {'papers': 'papers', 'versions': 'paper_versions',
                'extractions': 'extraction_records', 'sources': 'source_records'}


def load_queries(config: Path) -> dict[str, list[str]]:
    return json.loads(config.read_text(encoding='utf-8'))['topic_queries']


def iso_date(value: str) -> date:
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise argparse.ArgumentTypeError('expected ISO date YYYY-MM-DD')
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('invalid calendar date') from exc


def bounded_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('expected an integer from 1 to 100') from exc
    if not 1 <= limit <= 100:
        raise argparse.ArgumentTypeError('expected an integer from 1 to 100')
    return limit


def counts(con):
    return {name: con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            for name, table in COUNT_TABLES.items()}


def safe_error(exc):
    # Exception strings may contain URLs and API keys.
    error = {'class': type(exc).__name__}
    if isinstance(exc, HTTPError):
        error['code'] = exc.code
    return error


def main(argv=None):
    raise SystemExit('Historical SQLite collector retired. Use scripts/prepare_pipeline.py --run-id RUN_ID --run-dir ARTIFACT_DIR run with KG_DATABASE_URL, or scripts/prepare_full_pipeline.py for pagination.')


if __name__ == '__main__':
    main()
