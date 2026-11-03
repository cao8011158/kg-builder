"""Offline BM25S ranking and persisted-index contract."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from contextlib import redirect_stdout
from io import StringIO

import bm25s
from bm25s.utils.corpus import JsonlCorpus

from kg_builder.bm25_retrieval import BM25Index, FORMAT, TYPES, _tokens


class BM25RetrievalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'index'
        self.path.mkdir()
        counts={kind:0 for kind in TYPES}
        docs={
            'Paper':[{'node_id':'paper:1','text':'quasar spectral survey','version_id':'v:1','source_url':'https://example.test/1'},
                     {'node_id':'paper:2','text':'climate dynamics','version_id':'v:2'}],
            'Method':[{'node_id':'entity:m','text':'quasar quasar quasar method','revision':'rev'}],
            'Contribution':[{'node_id':'contribution:1','text':'new quasar finding','paper_id':'paper:1'}],
        }
        for kind,rows in docs.items():
            counts[kind]=len(rows)
            directory=self.path/kind
            directory.mkdir()
            scorer=bm25s.BM25(method='lucene')
            scorer.index(_tokens([row['text'] for row in rows]),show_progress=False)
            scorer.save(str(directory))
            (directory/'records.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows),encoding='utf-8')
            JsonlCorpus(str(directory/'records.jsonl'),show_progress=False).close()
        (self.path/'manifest.json').write_text(json.dumps({'format':FORMAT,'run_id':'run:1',
            'graph_namespace':'graph:1','bm25s_version':bm25s.__version__,'method':'lucene',
            'k1':1.5,'b':0.75,'tokenizer':{'lower':True,'stopwords':'english','token_pattern':r'(?u)\b\w\w+\b'},
            'counts':counts}),encoding='utf-8')

    def test_type_filter_before_top_k_and_provenance(self):
        with BM25Index(self.path,expected_run_id='run:1') as index:
            self.assertEqual(index.search('quasar',types=['Paper'],top_k=1)[0]['node_id'],'paper:1')
            hit=index.search('quasar',types=['Contribution'],top_k=1)[0]
            self.assertEqual((hit['node_id'],hit['paper_id'],hit['graph_namespace']),
                             ('contribution:1','paper:1','graph:1'))
            self.assertEqual(index.search('unfindableterm'),[])
            self.assertEqual(index.search('the and'),[])
            self.assertEqual(index.search('quasar',types=['ResearchArea']),[])

    def test_reload_identity_and_limits(self):
        with BM25Index(self.path) as index:
            self.assertEqual(index.search('climate',top_k=1)[0]['node_id'],'paper:2')
            with self.assertRaises(ValueError):index.search('a',types=['SourceItem'])
            with self.assertRaises(ValueError):index.search('a',top_k=101)
        with self.assertRaises(ValueError):BM25Index(self.path,expected_run_id='other')
        manifest=self.path/'manifest.json'
        value=json.loads(manifest.read_text())
        value['format']='old'
        manifest.write_text(json.dumps(value))
        with self.assertRaises(ValueError):BM25Index(self.path)

    def test_cli_json_and_repeated_open_close(self):
        from scripts.bm25_retrieval import main
        for _ in range(2):
            output=StringIO()
            with redirect_stdout(output):main(['search','--index-dir',str(self.path),'--type','Paper','--limit','1','climate'])
            hits=json.loads(output.getvalue())
            self.assertEqual(hits[0]['node_id'],'paper:2')
            self.assertEqual(hits[0]['text_length'],len('climate dynamics'))
        output=StringIO()
        with redirect_stdout(output):main(['info','--index-dir',str(self.path)])
        self.assertEqual(json.loads(output.getvalue())['run_id'],'run:1')

    def test_fresh_process_cli_stdout_is_json(self):
        root=Path(__file__).resolve().parents[1]
        env=dict(os.environ)
        env['PYTHONPATH']=str(root/'src')
        commands=[['info','--index-dir',str(self.path)],
                  ['search','--index-dir',str(self.path),'--type','Paper','climate']]
        for command in commands:
            completed=subprocess.run([sys.executable,str(root/'scripts'/'bm25_retrieval.py'),*command],
                cwd=root,env=env,capture_output=True,text=True,check=True)
            parsed=json.loads(completed.stdout)
            if command[0]=='info':self.assertEqual(parsed['run_id'],'run:1')
            else:self.assertEqual(parsed[0]['node_id'],'paper:2')


if __name__=='__main__':unittest.main()
