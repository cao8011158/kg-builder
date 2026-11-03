"""Read real pilot via public APIs; measure actual SQLite rows/bytes/opcodes.

Run with --master, --original-master, --sidecar and --output. Source stores are
read-only; scale/rollback probes use temporary backups, never the original.
"""
import argparse,json,sqlite3,tempfile,time
from pathlib import Path
from kg_builder.semantic_store import SemanticStore
from kg_builder.embeddings import EmbeddingModel
from kg_builder.vector_index import HNSWIndex
import numpy as np

class CursorMeter:
    def __init__(self,cursor,meter):self.cursor,self.meter=cursor,meter
    def _record(self,rows):
        self.meter.rows+=len(rows)
        for row in rows:
            self.meter.bytes+=sum(len(v.encode()) if isinstance(v,str) else len(v) if isinstance(v,bytes) else 8 for v in row if v is not None)
        return rows
    def fetchall(self):return self._record(self.cursor.fetchall())
    def fetchone(self):
        row=self.cursor.fetchone()
        if row is not None:self._record([row])
        return row

class DBMeter:
    def __init__(self,db):self.db=db;self.reset()
    def reset(self):self.rows=0;self.bytes=0;self.sql=[];self.opcodes=0
    def tick(self):self.opcodes+=1;return 0
    def execute(self,sql,args=()):
        self.sql.append((sql,args));return CursorMeter(self.db.execute(sql,args),self)


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--master',required=True);ap.add_argument('--original-master',required=True)
    ap.add_argument('--sidecar',required=True);ap.add_argument('--cache-dir',required=True);ap.add_argument('--output',required=True)
    a=ap.parse_args();report={}
    old=sqlite3.connect(f'file:{Path(a.original_master).resolve()}?mode=ro',uri=True)
    master=sqlite3.connect(f'file:{Path(a.master).resolve()}?mode=ro',uri=True)
    originals=old.execute('SELECT extraction_id,payload_json FROM extraction_records').fetchall()
    assert len(originals)==40
    for eid,payload in originals:assert master.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',(eid,)).fetchone()[0]==payload
    assert master.execute('SELECT count(*) FROM extraction_records').fetchone()[0]==46
    assert master.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    report['master']={'historical_payloads_unchanged':len(originals),'current_extractions':46,'papers':master.execute('select count(*) from papers').fetchone()[0],'integrity':'ok'}
    old.close();master.close()
    with tempfile.TemporaryDirectory() as td:
        path=Path(td)/'sidecar.sqlite3';src=sqlite3.connect(a.sidecar);dst=sqlite3.connect(path);src.backup(dst);src.close();dst.close()
        s=SemanticStore(path,EmbeddingModel(cache_dir=a.cache_dir))
        assert s.db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        report['counts']={t:s.db.execute('select count(*) from '+t).fetchone()[0] for t in ['sources','mentions','entities','contributions']}
        assert report['counts']=={'sources':6,'mentions':37,'entities':26,'contributions':6}
        assert s.db.execute("SELECT count(*) FROM entities WHERE kind='ResearchProblem'").fetchone()[0]==7
        assert len(s.get_entity('benchmark:locomo')['mentions'])==3
        assert len(s.get_entity('method:a-mem')['mentions'])==3
        assert s.get_entity('method:mem0')['entity_id']!=s.get_entity('method:mem0-graph')['entity_id']
        page=s.get_entity('benchmark:locomo',limit=1);assert page['next_offset']==1
        assert len(s.get_entity('benchmark:locomo',limit=1,offset=1)['mentions'])==1
        assert s.get_paper('arxiv:2502.12110')['contributions']
        assert s.export_candidates()['mentions']==[]
        for q,kind,wanted in [('LOCOMO','Benchmark','benchmark:locomo'),('a-mem','Method','method:a-mem')]:
            found=s.search(q,kind=kind)['results'];assert found[0]['entity_id']==wanted
        q='remember earlier interactions'
        cold=s.search(q,kind='Contribution',limit=3);before=dict(s.stats)
        warm=s.search(q,kind='Contribution',limit=3);after=dict(s.stats)
        assert cold==warm and len(warm['results'])==3
        assert after['dense_index_builds']==before['dense_index_builds']
        assert after['embeddings_computed']==before['embeddings_computed']
        report['warm_reuse']={'same_results':True,'index_build_delta':after['dense_index_builds']-before['dense_index_builds'],'embedding_compute_delta':after['embeddings_computed']-before['embeddings_computed']}
        realdb=s.db;meter=DBMeter(realdb);s.db=meter
        def measure(label):
            meter.reset();realdb.set_progress_handler(meter.tick,1)
            started=time.perf_counter();result=s.get_entity('benchmark:locomo',limit=3)
            elapsed=time.perf_counter()-started;realdb.set_progress_handler(None,0)
            plans=[]
            for sql,params in meter.sql:
                plans.extend([tuple(row) for row in realdb.execute('EXPLAIN QUERY PLAN '+sql,params)])
            out={'label':label,'fetched_rows':meter.rows,'fetched_bytes':meter.bytes,'sqlite_vm_opcodes':meter.opcodes,'reply_bytes':len(json.dumps(result).encode()),'seconds':elapsed,'plans':plans}
            assert not any('SCAN ' in str(row).upper() for row in plans)
            return out
        baseline=measure('cold_detail');hot=measure('hot_detail')
        realdb.executemany('INSERT INTO entities VALUES(?,?,?,?,?,?,?)',[(f'unrelated:{i}','Method','unrelated','x'*20000,'source_authored','[]',str(i)) for i in range(500)])
        realdb.commit();scaled=measure('500_unrelated_entities_10MB')
        assert baseline['fetched_rows']==hot['fetched_rows']==scaled['fetched_rows']
        assert baseline['fetched_bytes']==hot['fetched_bytes']==scaled['fetched_bytes']
        assert baseline['reply_bytes']==scaled['reply_bytes']
        assert scaled['sqlite_vm_opcodes']<=hot['sqlite_vm_opcodes']+20
        meter.reset();assert s.get_entity('missing') is None;assert len(meter.sql)==1 and meter.rows==0
        report['locality']={'measurements':[baseline,hot,scaled],'missing_id_queries':len(meter.sql),'internal_reading_measured':True,'large_requested_single_record_byte_budget':'not enforced; limits bound record count, not arbitrary imported field length'}
        s.db=realdb;s.close()
    # Exact neighbors are used ONLY as an independent validation reference.
    rng=np.random.default_rng(42);v=rng.normal(size=(500,32)).astype(np.float32);v/=np.linalg.norm(v,axis=1)[:,None]
    index=HNSWIndex({f'v{i}':v[i] for i in range(len(v))},model_id='independent-synthetic-32d')
    recalls=[]
    for i in range(20):
        exact=set(np.argsort(-(v@v[i]))[:10].tolist())
        approx={int(hit['id'][1:]) for hit in index.search(v[i],k=10)}
        recalls.append(len(exact&approx)/10)
    report['ann_validation']={'vectors':500,'dimensions':32,'queries':20,'k':10,'mean_recall':float(np.mean(recalls)),'scope':'synthetic validation only; not corpus retrieval quality'}
    Path(a.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))

if __name__=='__main__':main()
