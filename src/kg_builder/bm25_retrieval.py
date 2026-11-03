"""Frozen, offline BM25 search over the six published graph node kinds."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import uuid

import bm25s
from bm25s.utils.corpus import JsonlCorpus

from .postgres_graph import GraphStore

TYPES = ('Paper', 'ResearchProblem', 'Method', 'Benchmark', 'Contribution', 'ResearchArea')
FORMAT = 'kg-bm25/1'
TOKENIZER = {'lower':True,'stopwords':'english','token_pattern':r'(?u)\b\w\w+\b'}
MAX_TEXT_CHARS = 4000


def _tokens(value):
    return bm25s.tokenize(value, **TOKENIZER, show_progress=False)


def _rows(db):
    # These scans belong only to the explicit, frozen build operation.
    for row in db.execute('''SELECT DISTINCT ON (s.paper_id) s.paper_id AS node_id,s.title,v.abstract,
        s.version_id,s.version_label,s.source_url FROM sources s
        JOIN paper_versions v ON v.version_id=s.version_id ORDER BY s.paper_id,s.extraction_id'''):
        yield 'Paper', {'node_id':row['node_id'], 'text':' '.join(filter(None,(row['title'],row['abstract']))),
            'title':row['title'], 'version_id':row['version_id'], 'version_label':row['version_label'],
            'source_url':row['source_url']}
    for row in db.execute("SELECT entity_id AS node_id,kind,name,description,aliases_json,revision FROM entities WHERE kind IN ('ResearchProblem','Method','Benchmark') ORDER BY kind,entity_id"):
        aliases=json.loads(row['aliases_json'])
        yield row['kind'], {'node_id':row['node_id'], 'text':' '.join(filter(None,(row['name'],row['description'],*aliases))),
            'name':row['name'],'description':row['description'],'aliases':aliases,'revision':row['revision']}
    for row in db.execute('''SELECT c.contribution_id AS node_id,c.description,c.extraction_id,s.paper_id,
        s.version_id,s.source_url FROM contributions c JOIN sources s ON s.extraction_id=c.extraction_id
        ORDER BY c.contribution_id'''):
        yield 'Contribution', {'node_id':row['node_id'],'text':row['description'],
            'description':row['description'],'extraction_id':row['extraction_id'],'paper_id':row['paper_id'],
            'version_id':row['version_id'],'source_url':row['source_url']}
    for row in db.execute('SELECT area_id AS node_id,name,description,revision FROM research_areas ORDER BY area_id'):
        yield 'ResearchArea', {'node_id':row['node_id'],'text':' '.join(filter(None,(row['name'],row['description']))),
            'name':row['name'],'description':row['description'],'revision':row['revision']}


def build(run_id: str, index_dir: str | Path, *, database_url: str | None = None) -> dict:
    """Publish a new index directory; never replace an existing target."""
    target=Path(index_dir).absolute()
    if target.exists():raise FileExistsError(target)
    target.parent.mkdir(parents=True,exist_ok=True)
    staging=target.parent / ('.'+target.name+'-'+uuid.uuid4().hex+'.tmp')
    staging.mkdir()
    try:
        documents={kind:[] for kind in TYPES}
        with GraphStore(run_id,database_url=database_url,readonly=True,initialize=False) as store:
            with store.db.transaction():
                store.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                meta={r['key']:r['value'] for r in store.db.execute("SELECT key,value FROM meta WHERE key IN ('neo4j_run_id','snapshot_hash','phase')")}
                for kind,row in _rows(store.db):documents[kind].append(row)
        manifest={'format':FORMAT,'run_id':run_id,'graph_namespace':meta.get('neo4j_run_id'),
            'source_schema':store.schema,'snapshot_hash':meta.get('snapshot_hash'),
            'bm25s_version':bm25s.__version__,'method':'lucene','k1':1.5,'b':0.75,'tokenizer':TOKENIZER,
            'counts':{kind:len(rows) for kind,rows in documents.items()}}
        for kind,rows in documents.items():
            if not rows:continue
            directory=staging / kind
            directory.mkdir()
            texts=[r['text'] for r in rows]
            retriever=bm25s.BM25(method='lucene',k1=1.5,b=0.75)
            tokens=_tokens(texts)
            if any(len(t) for t in tokens.ids):retriever.index(tokens,show_progress=False)
            else:
                manifest.setdefault('unsearchable_counts',{})[kind]=len(rows)
                continue
            retriever.save(str(directory))
            with (directory/'records.jsonl').open('w',encoding='utf-8') as stream:
                for row in rows:stream.write(json.dumps(row,ensure_ascii=False,sort_keys=True)+'\n')
            JsonlCorpus(str(directory/'records.jsonl'),show_progress=False).close()
        (staging/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True),encoding='utf-8')
        # Windows rename fails when target exists. No existing directory is removed.
        staging.rename(target)
        return manifest
    finally:
        if staging.exists():
            if staging.resolve().parent!=target.parent.resolve() or not staging.name.startswith('.'+target.name+'-'):
                raise RuntimeError('Unexpected staging path; cleanup refused')
            shutil.rmtree(staging)


class BM25Index:
    """Load once, reuse for warm queries; hydrate only hit records by JSONL offset."""
    def __init__(self,index_dir: str | Path, *, expected_run_id: str | None = None):
        path=Path(index_dir)
        manifest=json.loads((path/'manifest.json').read_text(encoding='utf-8'))
        if manifest.get('format')!=FORMAT:raise ValueError('Unsupported BM25 index format')
        if expected_run_id is not None and manifest.get('run_id')!=expected_run_id:raise ValueError('BM25 run identity differs')
        if manifest.get('bm25s_version')!=bm25s.__version__:raise ValueError('BM25S version differs')
        if manifest.get('method')!='lucene' or manifest.get('k1')!=1.5 or manifest.get('b')!=0.75 or manifest.get('tokenizer')!=TOKENIZER:
            raise ValueError('BM25 scoring configuration differs')
        self.manifest=manifest
        self.retrievers={}
        self.records={}
        try:
            for kind in TYPES:
                count=manifest['counts'][kind]
                if not count or kind in manifest.get('unsearchable_counts',{}):continue
                directory=path/kind
                self.retrievers[kind]=bm25s.BM25.load(str(directory),load_corpus=False)
                records=JsonlCorpus(str(directory/'records.jsonl'),show_progress=False,save_index=False)
                self.records[kind]=records
                if len(records)!=count:raise ValueError('BM25 corpus count differs')
        except BaseException:
            self.close()
            raise

    def close(self):
        for corpus in self.records.values():corpus.close()

    def __enter__(self):return self
    def __exit__(self,*_):self.close()

    def search(self,query: str, *, types=None, top_k: int = 10) -> list[dict]:
        if not isinstance(top_k,int) or isinstance(top_k,bool) or not 1<=top_k<=100:raise ValueError('top_k must be 1..100')
        selected=TYPES if types is None else (types,) if isinstance(types,str) else tuple(dict.fromkeys(types))
        if any(kind not in TYPES for kind in selected):raise ValueError('Unknown graph node type')
        if not query or not query.strip() or not selected:return []
        tokens=_tokens(query)
        if not tokens.ids or len(tokens.ids[0])==0:return []
        results=[]
        for kind in selected:
            retriever=self.retrievers.get(kind)
            if retriever is None:continue
            count=self.manifest['counts'][kind]
            ids,scores=retriever.retrieve(tokens,k=min(top_k,count),show_progress=False)
            for index,score in zip(ids[0],scores[0]):
                if score<=0:continue
                results.append((float(score),kind,int(index)))
        results.sort(key=lambda item:(-item[0],item[1],item[2]))
        hits=[]
        for score,kind,index in results[:top_k]:
            source=self.records[kind][index]
            record={key:source[key] for key in ('node_id','title','name','version_id','version_label',
                'source_url','extraction_id','paper_id','revision') if key in source}
            raw=source['text']
            record['text_truncated']=len(raw)>MAX_TEXT_CHARS
            record['text_length']=len(raw)
            record['text']=raw[:MAX_TEXT_CHARS]
            hits.append({**record,'type':kind,'score':score,
                'run_id':self.manifest['run_id'],'graph_namespace':self.manifest['graph_namespace']})
        return hits
