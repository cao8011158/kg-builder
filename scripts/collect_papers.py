"""Run only metadata discovery and SQLite upserts. No LLM calls."""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.storage import connect,upsert_paper
from kg_builder.collectors import openalex_search,arxiv_search,semantic_scholar_search

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / 'config' / 'discovery_queries.json'

def load_queries(config: Path) -> dict[str, list[str]]:
    data = json.loads(config.read_text(encoding='utf-8'))
    return data['topic_queries']


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',default='data/research_knowledge.sqlite3')
    p.add_argument('--source',choices=['openalex','semantic_scholar','arxiv','all'],default='all')
    p.add_argument('--per-query',type=int,default=10)
    p.add_argument('--since',default='2025-01-01')
    p.add_argument('--until',default=datetime.now(timezone.utc).date().isoformat())
    p.add_argument('--query-config',type=Path,default=DEFAULT_CONFIG)
    p.add_argument('--seed-json',default=None,help='Import web-discovered seed metadata instead of external APIs')
    args=p.parse_args()
    con=connect(args.db)
    total=0
    queries_ok=0
    query_errors=[]
    if args.seed_json:
        for row in json.loads(Path(args.seed_json).read_text(encoding='utf-8')):
            upsert_paper(con,row,'verified_web_page',row['arxiv_id'],'manual_web_discovery')
            total+=1
    else:
        if args.until < args.since: raise ValueError('--until before --since')
        for topic,terms in load_queries(args.query_config).items():
            for term in terms:
                for src in (['openalex','semantic_scholar','arxiv'] if args.source=='all' else [args.source]):
                    try:
                        collector={'openalex':lambda:openalex_search(term,args.per_query,args.since,args.until),
                                   'semantic_scholar':lambda:semantic_scholar_search(term,args.per_query,year=args.since[:4]+'-'),
                                   'arxiv':lambda:arxiv_search(term,args.per_query,args.since)}[src]
                        for row in collector():
                            pub = row.get('published_at')
                            if pub:
                                if not (args.since <= pub[:10] <= args.until):
                                    continue
                            elif row.get('year') is None or not (int(args.since[:4]) <= int(row['year']) <= int(args.until[:4])):
                                continue
                            # Search hits are candidates, not automatically accepted papers.
                            row['topics']=[topic]
                            row['topic_basis']='discovery_query_candidate'
                            source_id=row.pop('source_id')
                            upsert_paper(con,row,src,source_id,term)
                            total+=1
                        queries_ok += 1
                    except Exception as ex:
                        query_errors.append({'source':src, 'topic':topic, 'query':term, 'error':str(ex)})
                        print(f'WARN {src} {topic}: {type(ex).__name__}: {ex}',file=sys.stderr)
    print(json.dumps({'records_seen':total,'papers':con.execute('SELECT COUNT(*) FROM papers').fetchone()[0],
      'versions':con.execute('SELECT COUNT(*) FROM paper_versions').fetchone()[0],
      'extractions':con.execute('SELECT COUNT(*) FROM extraction_records').fetchone()[0],
      'successful_queries':queries_ok,'failed_queries':len(query_errors),
      'errors':query_errors},indent=2))
    con.close()
    if not args.seed_json and not queries_ok:
        raise SystemExit('All API discovery queries failed; no successful API collection occurred')

if __name__=='__main__':main()
