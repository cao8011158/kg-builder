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
    p = argparse.ArgumentParser()
    p.add_argument('--db', default='data/research_knowledge.sqlite3')
    p.add_argument('--source', choices=['openalex', 'semantic_scholar', 'arxiv', 'all'], default='all',
                   help='all queries OpenAlex and arXiv; Semantic Scholar requires explicit selection')
    p.add_argument('--per-query', type=bounded_limit, default=10)
    p.add_argument('--since', type=iso_date, default=date(2025, 1, 1))
    p.add_argument('--until', type=iso_date, default=datetime.now(timezone.utc).date())
    p.add_argument('--query-config', type=Path, default=DEFAULT_CONFIG)
    p.add_argument('--seed-json', default=None, help='Import web-discovered seed metadata instead of external APIs')
    p.add_argument('--manifest', type=Path, help='Write a JSON run manifest')
    args = p.parse_args(argv)
    if args.until < args.since:
        p.error('--until before --since')
    since, until = args.since.isoformat(), args.until.isoformat()
    # Validate input before creating or connecting to the database.
    queries = None if args.seed_json else load_queries(args.query_config)
    seeds = json.loads(Path(args.seed_json).read_text(encoding='utf-8')) if args.seed_json else None

    started = datetime.now(timezone.utc).isoformat()
    con = connect(args.db)
    before = counts(con)
    by_source = defaultdict(lambda: {'received': 0, 'stored': 0, 'date_filtered': 0,
                                     'unknown_date': 0, 'errors': 0})
    query_runs = []
    queries_ok = 0
    try:
        if seeds is not None:
            for row in seeds:
                upsert_paper(con, row, 'verified_web_page', row['arxiv_id'], 'manual_web_discovery')
                by_source['verified_web_page']['received'] += 1
                by_source['verified_web_page']['stored'] += 1
        else:
            for topic, terms in queries.items():
                for term in terms:
                    sources = ['openalex', 'arxiv'] if args.source == 'all' else [args.source]
                    for src in sources:
                        stats = {'source': src, 'topic': topic, 'query': term, 'received': 0,
                                 'stored': 0, 'date_filtered': 0, 'unknown_date': 0, 'errors': []}
                        query_runs.append(stats)
                        try:
                            collector = {
                                'openalex': lambda: openalex_search(term, args.per_query, since, until),
                                'semantic_scholar': lambda: semantic_scholar_search(term, args.per_query, year=since[:4] + '-'),
                                'arxiv': lambda: arxiv_search(term, args.per_query, since),
                            }[src]
                            for row in collector():
                                stats['received'] += 1
                                by_source[src]['received'] += 1
                                pub = row.get('published_at')
                                precise = pub and re.fullmatch(r'\d{4}-\d{2}-\d{2}', pub[:10])
                                if precise:
                                    try:
                                        precise = bool(date.fromisoformat(pub[:10]))
                                    except ValueError:
                                        precise = False
                                if precise and not (since <= pub[:10] <= until):
                                    stats['date_filtered'] += 1
                                    by_source[src]['date_filtered'] += 1
                                    continue
                                if not precise:
                                    stats['unknown_date'] += 1
                                    by_source[src]['unknown_date'] += 1
                                    year = row.get('year')
                                    if year is None or not (args.since.year <= int(year) <= args.until.year):
                                        stats['date_filtered'] += 1
                                        by_source[src]['date_filtered'] += 1
                                        continue
                                    row['first_release_date_status'] = 'unknown_candidate'
                                row['topics'] = [topic]
                                row['topic_basis'] = 'discovery_query_candidate'
                                source_id = row.pop('source_id')
                                upsert_paper(con, row, src, source_id, term)
                                stats['stored'] += 1
                                by_source[src]['stored'] += 1
                            queries_ok += 1
                        except Exception as exc:
                            error = safe_error(exc)
                            stats['errors'].append(error)
                            by_source[src]['errors'] += 1
                            print(f"WARN {src} {topic}: {error['class']}" +
                                  (f" HTTP {error['code']}" if 'code' in error else ''), file=sys.stderr)
        result = {'started_at': started, 'completed_at': datetime.now(timezone.utc).isoformat(),
                  'mode': 'seed' if seeds is not None else 'api_first_page',
                  'since': since, 'until': until, 'per_query': args.per_query,
                  'db_before': before, 'db_after': counts(con),
                  'by_source': dict(by_source), 'queries': query_runs,
                  'successful_queries': queries_ok,
                  'failed_queries': sum(bool(q['errors']) for q in query_runs)}
    finally:
        con.close()
    if args.manifest:
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        args.manifest.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if seeds is None and not queries_ok:
        raise SystemExit('All API discovery queries failed; no successful API collection occurred')
    return result


if __name__ == '__main__':
    main()
