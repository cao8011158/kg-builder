"""Independent read/identity audit over private corpus fixtures; no raw corpus is published."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3
from kg_builder.extraction_validation import validate_extraction_batch
from kg_builder.semantic_store import SemanticStore, digest
from kg_builder.embeddings import EmbeddingModel

p = argparse.ArgumentParser()
p.add_argument('task', type=Path)
p.add_argument('--through', type=int, required=True)
p.add_argument('--cache', type=Path, required=True)
a = p.parse_args()
root = a.task
load = lambda n: json.loads((root/n).read_text())
ro = lambda n: sqlite3.connect(f'file:{root/n}?mode=ro', uri=True)
master, baseline, sem = (ro(n) for n in ('master-working.sqlite3','master-baseline.sqlite3','semantic-working.sqlite3'))
for con in (master, baseline, sem):
    con.row_factory = sqlite3.Row
    assert con.execute('pragma integrity_check').fetchone()[0] == 'ok'
    assert not con.execute('pragma foreign_key_check').fetchall()
for table in ('papers','paper_versions'):
    old = [tuple(r) for r in baseline.execute(f'SELECT * FROM {table} ORDER BY 1')]
    now = [tuple(r) for r in master.execute(f'SELECT * FROM {table} ORDER BY 1')]
    assert now == old, f'Original metadata changed: {table}'
old_records = {r['extraction_id']:r['payload_json'] for r in baseline.execute('SELECT extraction_id,payload_json FROM extraction_records')}
for eid,raw in old_records.items():
    assert master.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',(eid,)).fetchone()[0] == raw
print('ORIGINAL_METADATA_AND_PAYLOADS_UNCHANGED',len(old_records))
for table_row in baseline.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
    table=table_row[0]
    assert table.replace('_','').isalnum()
    old_rows={tuple(r) for r in baseline.execute('SELECT * FROM '+table)}
    current_rows={tuple(r) for r in master.execute('SELECT * FROM '+table)}
    assert old_rows <= current_rows, f'Historical rows removed or changed: {table}'
    if table not in ('extraction_records','relevance_reviews'):assert old_rows==current_rows,table
print('ALL_HISTORICAL_MASTER_ROWS_PRESERVED')

seen=set(); includes=set(); total_new=0
for b in range(1,a.through+1):
    inp, reviews, ext = [load(f'batch{b}-{k}.json') for k in ('input','reviews','extractions')]
    validate_extraction_batch(ext)
    pin={x['paper_id']:x for x in inp}; rev={x['paper_id']:x for x in reviews}; extracted={x['paper_id']:x for x in ext}
    assert len(pin)==len(inp)==len(reviews)==len(rev)
    assert pin.keys()==rev.keys() and not seen.intersection(pin)
    seen.update(pin)
    expected={pid for pid,r in rev.items() if r['decision']=='include' and not pin[pid]['already_processed']}
    assert extracted.keys()==expected
    for pid,r in rev.items():
        row=master.execute('SELECT payload_json FROM relevance_reviews WHERE review_id=? AND paper_id=?',(f'scope-three-domain-batch{b}-20261101',pid)).fetchone()
        assert row and json.loads(row[0])==r
        if r['decision']=='include':includes.add(pid)
    for pid,x in extracted.items():
        assert x['version_id']==pin[pid]['paper_version']['version_id']
        assert x['paper_version']=={k:pin[pid]['paper_version'][k] for k in ('version_label','source_url')}
        row=master.execute('SELECT extraction_id,payload_json FROM extraction_records WHERE paper_id=? AND version_id=? AND extractor_version=?',(pid,x['version_id'],x['extractor_version'])).fetchone()
        assert row and json.loads(row['payload_json'])==x
        assert sem.execute('SELECT 1 FROM sources WHERE extraction_id=?',(row['extraction_id'],)).fetchone()
    total_new+=len(ext)
    print('BATCH',b,dict(Counter(x['decision'] for x in reviews)),'new',len(ext),'scopes',dict(Counter(x['source_scope'] for x in ext)))
# Later source evidence is append-only and preserves the original batch review.
for x in load('batch1-source-adjudication-reviews.json'):
    assert x['paper_id'] in seen
    row=master.execute('SELECT payload_json FROM relevance_reviews WHERE review_id=? AND paper_id=?',('scope-three-domain-batch1-source-adjudication-20261101',x['paper_id'])).fetchone()
    assert row and json.loads(row[0])==x and x['decision']=='exclude'
    print('SUPERSEDING_SOURCE_ADJUDICATION',x['paper_id'],x['decision'])
assert master.execute('SELECT COUNT(*) FROM extraction_records').fetchone()[0]==len(old_records)+total_new
assert {r[0] for r in sem.execute('SELECT paper_id FROM sources')}==includes
expected_mentions=expected_contributions=0
for s in sem.execute('SELECT * FROM sources'):
    row=master.execute('SELECT x.*,p.title,v.source_url,v.version_label FROM extraction_records x JOIN papers p ON p.paper_id=x.paper_id JOIN paper_versions v ON v.version_id=x.version_id WHERE extraction_id=?',(s['extraction_id'],)).fetchone()
    data=json.loads(row['payload_json'])
    source_hash=digest({'payload':data,'status':row['status'],'source_scope':row['source_scope'],'title':row['title'],'source_url':row['source_url'],'version_label':row['version_label']})
    assert s['source_hash']==source_hash
    expected_contributions+=len(data['contributions'])
    for field,kind in [('research_problems','ResearchProblem'),('methods','Method'),('benchmarks','Benchmark')]:
        for item in data[field]:
            expected_mentions+=1
            m=sem.execute('SELECT * FROM mentions WHERE extraction_id=? AND item_key=?',(s['extraction_id'],item['key'])).fetchone()
            assert m and m['kind']==kind and json.loads(m['source_json'])==item and m['mention_hash']==digest(item)
            target=sem.execute('SELECT kind FROM entities WHERE entity_id=?',(m['entity_id'],)).fetchone()
            assert m['decision']!='pending' and target and target[0]==kind
assert sem.execute('SELECT COUNT(*) FROM mentions').fetchone()[0]==expected_mentions
assert sem.execute('SELECT COUNT(*) FROM contributions').fetchone()[0]==expected_contributions
assert not sem.execute('SELECT 1 FROM entities e LEFT JOIN mentions m ON m.entity_id=e.entity_id WHERE m.mention_id IS NULL').fetchall()
for e in sem.execute("SELECT * FROM entities WHERE name_origin='source_authored'"):
    names={r[0] for r in sem.execute('SELECT name FROM mentions WHERE entity_id=?',(e['entity_id'],))}
    assert e['name'] in names
print('SEMANTIC_EXACT_SOURCE_ITEM_HASHES_PASS',expected_mentions,'mentions',expected_contributions,'contributions')
print('ENTITY_KINDS',dict(sem.execute('SELECT kind,count(*) FROM entities GROUP BY kind').fetchall()))
print('SHARED',[(r[0],r[1]) for r in sem.execute('SELECT entity_id,count(DISTINCT extraction_id) FROM mentions WHERE entity_id IN (?,?,?) GROUP BY entity_id',('method:sage','method:cat','benchmark:locomo'))])
plan=[tuple(r) for r in sem.execute('EXPLAIN QUERY PLAN SELECT * FROM mentions WHERE entity_id=? LIMIT 21',('method:sage',))]
assert any('mentions_entity' in r[3] for r in plan)
print('INDEXED_DETAIL_PLAN',plan)
store=SemanticStore(root/'semantic-working.sqlite3', model=EmbeddingModel(cache_dir=str(a.cache)))
try:
    for query,kind,eid in [('SAGE','Method','method:sage'),('CAT','Method','method:cat'),('LoCoMo','Benchmark','benchmark:locomo')]:
        result=store.search(query,kind=kind,limit=3)
        assert result['results'][0]['entity_id']==eid,(query,result)
        print('ACTUAL_HYBRID_QUERY',query,'TOP',eid)
    before=store.stats['detail_rows']; detail=store.get_entity('method:sage',limit=1)
    assert len(detail['mentions'])<=1 and store.stats['detail_rows']-before<=3
    print('BOUNDED_ENTITY_READ_ROWS',store.stats['detail_rows']-before)
finally:store.close()
if a.through==3:assert seen=={x['paper_id'] for x in load('corpus.json')} and len(seen)==365
print('AUDIT_PASS',len(seen),'candidates',len(includes),'included',total_new,'new_extractions')
