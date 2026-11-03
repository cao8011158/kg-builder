"""Independent real-PostgreSQL source lookup plan/row evidence; synthetic data only."""
import json
import os
from pathlib import Path
import uuid
from psycopg import sql
from kg_builder.postgres import PreparationStore
from kg_builder.postgres_source import PostgresSourceRepository


def main():
    url = os.environ['KG_TEST_DATABASE_URL']
    store = PreparationStore('source-review-' + uuid.uuid4().hex, database_url=url)
    store.initialize()
    repo = PostgresSourceRepository(store)
    try:
        record = {'doi': '10.1234/target', 'title': 'Target', 'abstract': 'Target evidence'}
        with store.transaction() as conn:
            pid, _ = repo.upsert_paper(conn, record, 'openalex', 'target', 'fixture')
        results = []
        for extra in (0, 2000):
            with store.transaction() as conn:
                if extra:
                    conn.execute(sql.SQL('''INSERT INTO {} (paper_id,title,doi,created_at,updated_at)
                      SELECT 'unrelated:'||n,repeat('x',4096),'10.1234/unrelated'||n,'now','now'
                      FROM generate_series(1,%s) n''').format(store.source_table('papers')), (extra,))
                    conn.execute(sql.SQL('ANALYZE {}').format(store.source_table('papers')))
                observed = []
                original = conn.execute
                def trace(query, params=None, *args, **kwargs):
                    cur = original(query, params, *args, **kwargs)
                    rendered = query.as_string(conn) if isinstance(query, sql.Composable) else query
                    if rendered.lstrip().startswith('SELECT paper_id FROM'):
                        observed.append((query, params))
                    return cur
                conn.execute = trace
                actual, _ = repo.upsert_paper(conn, record, 'openalex', 'target', 'fixture')
                assert actual == pid
                conn.execute = original
                plans = []
                for query, params in observed:
                    doc = original(sql.SQL('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ') + query, params).fetchone()['QUERY PLAN'][0]
                    plans.append(doc)
                results.append({'unrelated_rows': extra, 'lookup_count': len(observed), 'plans': plans})
        assert results[0]['lookup_count'] == results[1]['lookup_count']
        report = {'engine': 'PostgreSQL', 'synthetic': True, 'operation': 'PostgresSourceRepository.upsert_paper', 'results': results}
        target = Path(os.environ.get('KG_TEST_EVIDENCE_DIR', '/tmp')) / 'source-locality.json'
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2))
        print(json.dumps({'source_locality': 'pass', 'lookup_count': results[1]['lookup_count'], 'unrelated_rows': 2000}))
    finally:
        with store.transaction() as conn:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(store.workflow_schema)))
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(store.source_schema)))
        store.close()

if __name__ == '__main__':
    main()
