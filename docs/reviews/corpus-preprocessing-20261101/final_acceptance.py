"""Measure corpus retrieval, cache reuse and ID locality on a disposable SQLite backup."""
import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from kg_builder.semantic_store import SemanticStore
from kg_builder.embeddings import EmbeddingModel

spec=importlib.util.spec_from_file_location('pilot_meter',Path(__file__).parents[1]/'retrieval-pilot-20261031'/'independent_acceptance.py')
meter_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(meter_module)
p=argparse.ArgumentParser();p.add_argument('--sidecar',type=Path,required=True);p.add_argument('--cache',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
report={}
with tempfile.TemporaryDirectory() as td:
    path=Path(td)/'probe.sqlite3'
    src=sqlite3.connect(f'file:{a.sidecar.resolve()}?mode=ro',uri=True);dst=sqlite3.connect(path);src.backup(dst);src.close();dst.close()
    s=SemanticStore(path,model=EmbeddingModel(cache_dir=a.cache))
    try:
        report['actual_counts']={t:s.db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ['sources','mentions','entities','contributions']}
        assert s.db.execute('pragma integrity_check').fetchone()[0]=='ok' and not s.db.execute('pragma foreign_key_check').fetchall()
        assert s.export_candidates(limit=1)['mentions']==[]
        q='remember earlier interactions and preserve useful experience'
        cold=s.search(q,kind='Contribution',limit=3);before=dict(s.stats)
        warm=s.search(q,kind='Contribution',limit=3);after=dict(s.stats)
        assert cold==warm and len(warm['results'])==3
        assert after['dense_index_builds']==before['dense_index_builds'] and after['embeddings_computed']==before['embeddings_computed']
        report['warm_reuse']={'same_results':True,'new_index_builds':after['dense_index_builds']-before['dense_index_builds'],'new_embeddings':after['embeddings_computed']-before['embeddings_computed']}
        page=s.get_entity('benchmark:locomo',limit=1)
        assert len(page['mentions'])==1 and page['next_offset']==1
        assert len(s.get_entity('benchmark:locomo',limit=1,offset=1)['mentions'])==1
        report['pagination']={'first_page':1,'second_page':1,'next_offset':1}
        realdb=s.db;meter=meter_module.DBMeter(realdb);s.db=meter
        def measure(label):
            meter.reset();realdb.set_progress_handler(meter.tick,1);start=time.perf_counter()
            try:result=s.get_entity('benchmark:locomo',limit=3)
            finally:realdb.set_progress_handler(None,0)
            plans=[]
            for sql,args in meter.sql:plans.extend(tuple(r) for r in realdb.execute('EXPLAIN QUERY PLAN '+sql,args))
            assert not any('SCAN ' in r[3].upper() for r in plans)
            return {'label':label,'fetched_rows':meter.rows,'fetched_bytes':meter.bytes,'sqlite_vm_opcodes':meter.opcodes,'reply_bytes':len(json.dumps(result).encode()),'seconds':time.perf_counter()-start,'plans':plans}
        first=measure('actual_corpus_cold_detail');hot=measure('actual_corpus_warm_detail')
        realdb.executemany('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',[(f'locality-probe:{i}','Method','unrelated','z'*20000,'source_authored','[]',str(i)) for i in range(500)])
        realdb.commit();scaled=measure('actual_corpus_plus_500_unrelated_entities_10MB')
        for field in ['fetched_rows','fetched_bytes','reply_bytes']:assert first[field]==hot[field]==scaled[field]
        assert scaled['sqlite_vm_opcodes']<=hot['sqlite_vm_opcodes']+20
        meter.reset();assert s.get_entity('nonexistent-locality-probe') is None
        assert len(meter.sql)==1 and meter.rows==0
        report['locality']={'measurements':[first,hot,scaled],'missing_id_queries':1,'missing_id_fetched_rows':0,'internal_rows_bytes_and_opcodes_measured':True,'field_byte_budget':'record-count pagination, no arbitrary field-byte truncation'}
        s.db=realdb
    finally:
        if hasattr(s.db,'db'):s.db=s.db.db
        s.close()
a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
