import copy
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'src'),str(Path(__file__).resolve().parents[1]/'scripts')]
from kg_builder.storage import connect,upsert_paper,ingest_extraction
from kg_builder.entity_resolution import (validate_resolution_manifest,import_resolution_manifest,
                                          get_resolution_mentions)
from reconcile_sqlite import reconcile
from import_entity_resolution import main as cli_import


def digest(raw): return hashlib.sha256(raw.encode('utf-8')).hexdigest()


class ResolutionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.path=self.root/'source.db'
        self.con=connect(self.path);self.addCleanup(self.con.close)
        self.paper,self.version=upsert_paper(self.con,{'title':'Work','doi':'10.1234/work',
            'version_label':'v1','url':'https://example.org/v1'},'test','work','query')
        self.extraction={'paper_id':self.paper,'version_id':self.version,'summary':'draft',
            'methods':[{'name':'GRPO','description':'Local text'}],
            'research_problems':[{'name':'Problem'}],
            'artifacts':[{'key':'artifact:config','name':'Config','artifact_kind':'AGENT_SYSTEM',
                          'identity_scope':'paper_version_local_configuration',
                          'name_origin':'annotator_descriptive'}],
            'findings':[],'evidence_locators':[{'section':'§1','source_url':'https://example.org/v1'}]}
        self.eid=ingest_extraction(self.con,self.extraction)
        raw=self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                             (self.eid,)).fetchone()[0]
        self.hash=digest(raw)
        self.manifest={'schema_version':'research-kg-entity-resolution/0.1',
          'batch_id':'batch:manual-1','reviewer':'reviewer','reviewed_at':'2026-10-31T00:00:00Z',
          'source_db_sha256':'a'*64,'notes':'manual identity only',
          'entities':[{'entity_id':'concept:grpo','kind':'Method','name':'GRPO',
            'scope':'named_concept','scope_version_id':None,'aliases':['Group Relative Policy Optimization'],
            'identity_definition':'Algorithm explicitly named in source',
            'support':[{'extraction_id':self.eid,'source_payload_sha256':self.hash,
                        'evidence_locator_path':'/evidence_locators/0'}]}],
          'mentions':[{'extraction_id':self.eid,'kind':'Method','mention_path':'/methods/0',
            'source_payload_sha256':self.hash,'surface_name':'GRPO','source_key':None,
            'decision':'resolved','entity_id':'concept:grpo','rationale':'same named method',
            'evidence_refs':[],'evidence_locator_paths':['/evidence_locators/0']},
            {'extraction_id':self.eid,'kind':'ResearchProblem','mention_path':'/research_problems/0',
            'source_payload_sha256':self.hash,'surface_name':'Problem','source_key':None,
            'decision':'pending','entity_id':None,'rationale':'insufficient identity basis',
            'evidence_refs':[],'evidence_locator_paths':[]}]}

    def test_immutable_import_and_indexed_query(self):
        self.assertIs(validate_resolution_manifest(self.manifest),self.manifest)
        self.assertEqual(import_resolution_manifest(self.con,self.manifest),'batch:manual-1')
        records=get_resolution_mentions(self.con,'batch:manual-1',self.eid)
        self.assertEqual(len(records),2)
        self.assertEqual([(x['decision'],x['stale']) for x in records],[('resolved',False),('pending',False)])
        self.assertEqual(records[0]['entity']['entity_id'],'concept:grpo')
        first=self.con.execute('SELECT payload_json FROM entity_resolution_batches').fetchone()[0]
        self.assertEqual(import_resolution_manifest(self.con,self.manifest,raw_json=json.dumps(self.manifest,indent=2)),
                         'batch:manual-1')
        self.assertEqual(self.con.execute('SELECT payload_json FROM entity_resolution_batches').fetchone()[0],first)
        altered=copy.deepcopy(self.manifest);altered['notes']='different'
        with self.assertRaisesRegex(ValueError,'Immutable batch_id'):
            import_resolution_manifest(self.con,altered)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM entity_resolution_mentions').fetchone()[0],2)
        plan=self.con.execute('EXPLAIN QUERY PLAN SELECT * FROM entity_resolution_mentions '
            'WHERE batch_id=? AND extraction_id=?',('batch:manual-1',self.eid)).fetchall()
        self.assertTrue(any('sqlite_autoindex_entity_resolution_mentions_1' in r[3] for r in plan),plan)
        sql=[]
        self.con.set_trace_callback(sql.append)
        try:self.assertEqual(get_resolution_mentions(self.con,'unknown-batch',self.eid),[])
        finally:self.con.set_trace_callback(None)
        self.assertEqual(len(sql),3)
        self.assertIn('entity_resolution_mentions',sql[1])
        self.assertTrue(all('extraction_records' not in statement for statement in sql))
        def measured():
            steps=[0]
            def counter():steps[0]+=1
            self.con.set_progress_handler(counter,1)
            try: result=get_resolution_mentions(self.con,'batch:manual-1',self.eid)
            finally:self.con.set_progress_handler(None,0)
            return steps[0],len(json.dumps(result))
        cold_steps,cold_bytes=measured()
        for n in range(500):
            batch=f'unrelated:{n}'
            self.con.execute('INSERT INTO entity_resolution_batches VALUES(?,?,?,?,?,?)',
                             (batch,'other','other','now','0'*64,'{}'))
            self.con.execute('INSERT INTO entity_resolution_mentions VALUES(?,?,?,?,?,?,?,?)',
                (batch,self.eid,'ResearchProblem','/research_problems/0',self.hash,'pending',None,'{}'))
        self.con.commit()
        grown_steps,grown_bytes=measured()
        self.assertLessEqual(grown_steps,cold_steps+2)
        self.assertEqual(grown_bytes,cold_bytes)

    def test_atomic_invalid_second_and_sql_failure_in_outer_transaction(self):
        bad=copy.deepcopy(self.manifest);bad['mentions'][1]['source_payload_sha256']='0'*64
        self.con.execute('BEGIN')
        with self.assertRaisesRegex(ValueError,'hash mismatch'):
            import_resolution_manifest(self.con,bad)
        self.assertTrue(self.con.in_transaction)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM entity_resolution_batches').fetchone()[0],0)
        self.con.rollback()
        self.con.execute('''CREATE TRIGGER fail_second BEFORE INSERT ON entity_resolution_mentions
          WHEN NEW.mention_path='/research_problems/0' BEGIN SELECT RAISE(ABORT,'second failed'); END''')
        self.con.commit();self.con.execute('BEGIN')
        with self.assertRaisesRegex(sqlite3.DatabaseError,'second failed'):
            import_resolution_manifest(self.con,self.manifest)
        self.assertTrue(self.con.in_transaction)
        for table in ('entity_resolution_batches','entity_resolution_entities','entity_resolution_mentions'):
            self.assertEqual(self.con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0],0)
        self.con.rollback()

    def test_semantic_guards(self):
        tests=[('Cross-kind',lambda x:x['entities'][0].update(kind='Artifact',artifact_kind='AGENT_SYSTEM')),
               ('Artifact subtype',lambda x:(x['entities'][0].update(kind='Artifact',artifact_kind='MODEL'),
                 x['mentions'][0].update(kind='Artifact',mention_path='/artifacts/0',surface_name='Config',
                                         source_key='artifact:config'))),
               ('cannot resolve globally',lambda x:(x['entities'][0].update(kind='Artifact',artifact_kind='AGENT_SYSTEM'),
                 x['mentions'][0].update(kind='Artifact',mention_path='/artifacts/0',surface_name='Config',
                                         source_key='artifact:config'))),
               ('evidence',lambda x:x['mentions'][0].update(evidence_locator_paths=['/evidence_locators/99'])),
               ('Resolved mention needs',lambda x:x['mentions'][0].update(evidence_locator_paths=[])),
               ('Mention surface',lambda x:x['mentions'][0].update(surface_name='other')),
               ('Support payload hash',lambda x:x['entities'][0]['support'][0].update(source_payload_sha256='0'*64))]
        for message,change in tests:
            manifest=copy.deepcopy(self.manifest);change(manifest)
            with self.subTest(message=message),self.assertRaisesRegex(ValueError,message):
                import_resolution_manifest(self.con,manifest)
        invalid=copy.deepcopy(self.manifest);invalid['schema_version']='future'
        with self.assertRaisesRegex(ValueError,'Unknown'):validate_resolution_manifest(invalid)

    def test_cli_invalid_manifest_before_db_creation(self):
        input_path=self.root/'manifest.json';missing=self.root/'new.db'
        invalid=copy.deepcopy(self.manifest);invalid['schema_version']='future'
        input_path.write_text(json.dumps(invalid),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'Unknown'):
            cli_import(['--input',str(input_path),'--db',str(missing)])
        self.assertFalse(missing.exists())
        input_path.write_text('{"schema_version": NaN}',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'Nonfinite'):
            cli_import(['--input',str(input_path),'--db',str(missing)])
        self.assertFalse(missing.exists())

    def test_stale_mention_and_support_closure(self):
        # Second extraction points to an entity supported by the first one.
        second=copy.deepcopy(self.extraction)
        second['methods'][0]['name']='GRPO elsewhere'
        second['extractor_version']='second'
        second_eid=ingest_extraction(self.con,second)
        raw=self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',(second_eid,)).fetchone()[0]
        manifest=copy.deepcopy(self.manifest)
        manifest['mentions']=[dict(manifest['mentions'][0],extraction_id=second_eid,
                       source_payload_sha256=digest(raw),surface_name='GRPO elsewhere')]
        import_resolution_manifest(self.con,manifest)
        self.assertFalse(get_resolution_mentions(self.con,manifest['batch_id'],second_eid)[0]['stale'])
        self.con.execute('UPDATE extraction_records SET payload_json=? WHERE extraction_id=?',
                         (self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                          (self.eid,)).fetchone()[0]+' ',self.eid))
        self.con.commit()
        result=get_resolution_mentions(self.con,manifest['batch_id'],second_eid)[0]
        self.assertTrue(result['stale']);self.assertIsNone(result['entity'])
        self.assertEqual(result['decision'],'resolved')
        self.con.execute('UPDATE extraction_records SET payload_json=trim(payload_json) WHERE extraction_id=?',(self.eid,))
        self.con.execute('UPDATE extraction_records SET payload_json=payload_json||? WHERE extraction_id=?',
                         (' ',second_eid));self.con.commit()
        self.assertTrue(get_resolution_mentions(self.con,manifest['batch_id'],second_eid)[0]['stale'])

    def test_lookup_uses_one_snapshot_and_preserves_outer_transaction(self):
        second=copy.deepcopy(self.extraction)
        second['methods'][0]['name']='GRPO elsewhere'
        second['extractor_version']='second'
        second_eid=ingest_extraction(self.con,second)
        own_raw=self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                                 (second_eid,)).fetchone()[0]
        support_raw=self.con.execute('SELECT payload_json FROM extraction_records WHERE extraction_id=?',
                                     (self.eid,)).fetchone()[0]
        manifest=copy.deepcopy(self.manifest)
        manifest['mentions']=[dict(manifest['mentions'][0],extraction_id=second_eid,
            source_payload_sha256=digest(own_raw),surface_name='GRPO elsewhere')]
        import_resolution_manifest(self.con,manifest)
        self.con.execute('UPDATE extraction_records SET payload_json=? WHERE extraction_id=?',
                         (support_raw+' ',self.eid))
        self.con.commit()
        writer=sqlite3.connect(self.path)
        self.addCleanup(writer.close)
        support_eid=self.eid
        class CursorHook:
            def __init__(self,cursor,hook):self.cursor,self.hook=cursor,hook
            def fetchone(self):
                value=self.cursor.fetchone()
                self.hook()
                return value
        class ReaderHook:
            def __init__(self,con):self.con,self.fired=con,False
            def execute(self,sql,params=()):
                cursor=self.con.execute(sql,params)
                if ('SELECT payload_json FROM extraction_records' in sql and
                        params==(second_eid,) and not self.fired):
                    def switch():
                        self.fired=True
                        writer.execute('BEGIN')
                        writer.execute('UPDATE extraction_records SET payload_json=? WHERE extraction_id=?',
                                       (own_raw+' ',second_eid))
                        writer.execute('UPDATE extraction_records SET payload_json=? WHERE extraction_id=?',
                                       (support_raw,support_eid))
                        writer.commit()
                    return CursorHook(cursor,switch)
                return cursor
        hooked=ReaderHook(self.con)
        result=get_resolution_mentions(hooked,manifest['batch_id'],second_eid)[0]
        self.assertTrue(hooked.fired)
        self.assertTrue(result['stale'])
        self.assertIsNone(result['entity'])
        self.assertFalse(self.con.in_transaction)
        self.con.execute('BEGIN')
        get_resolution_mentions(self.con,manifest['batch_id'],second_eid)
        self.assertTrue(self.con.in_transaction)
        self.con.rollback()

    def test_reconcile_retention_repeat_and_remap_failure(self):
        import_resolution_manifest(self.con,self.manifest)
        self.con.close()
        first=reconcile([self.path],self.root/'copy.db',self.root/'report.json')
        self.assertEqual(first['counts']['entity_resolution_mentions'],2)
        self.assertEqual(first['counts']['entity_resolution_entities'],1)
        self.assertEqual(first['counts']['entity_resolution_batches'],1)
        repeated=reconcile([self.root/'copy.db'],self.root/'repeat.db',self.root/'repeat.json')
        self.assertEqual(repeated['counts']['entity_resolution_mentions'],2)
        output=sqlite3.connect(self.root/'repeat.db')
        self.assertEqual(output.execute("SELECT COUNT(*) FROM reconciliation_audit WHERE table_name='entity_resolution_mentions'").fetchone()[0],4)
        output.close()
        earlier=connect(self.root/'earlier.db')
        earlier_pid,_=upsert_paper(earlier,{'title':'Same','version_label':'v1',
                             'url':'https://example.org/v1'},'other','earlier','q')
        earlier.execute('UPDATE papers SET doi=? WHERE paper_id=?',('10.1234/work',earlier_pid))
        earlier.commit()
        earlier.close()
        with self.assertRaisesRegex(ValueError,'Annotated extraction ID or payload'):
            reconcile([self.root/'earlier.db',self.path],self.root/'blocked.db',self.root/'blocked.json')
        self.assertFalse((self.root/'blocked.db').exists())
        altered=self.root/'altered.db'
        shutil.copyfile(self.path,altered)
        changed=sqlite3.connect(altered)
        changed.execute('UPDATE entity_resolution_batches SET manifest_sha256=? WHERE batch_id=?',
                        ('f'*64,self.manifest['batch_id']))
        changed.commit();changed.close()
        with self.assertRaisesRegex(ValueError,'Conflicting immutable'):
            reconcile([self.path,altered],self.root/'conflict.db',self.root/'conflict.json')
        self.assertFalse((self.root/'conflict.db').exists())

    def test_reconcile_resolution_columns_in_different_order(self):
        import_resolution_manifest(self.con,self.manifest)
        self.con.close()
        reordered=self.root/'reordered.db'
        shutil.copyfile(self.path,reordered)
        db=sqlite3.connect(reordered)
        db.row_factory=sqlite3.Row
        orders={
            'entity_resolution_batches':('batch_id','reviewer','schema_version','reviewed_at','manifest_sha256','payload_json'),
            'entity_resolution_entities':('batch_id','entity_id','payload_json','scope','scope_version_id','kind'),
            'entity_resolution_mentions':('batch_id','extraction_id','payload_json','mention_path',
                                          'source_payload_sha256','decision','entity_id','kind'),
        }
        original={table:[dict(r) for r in db.execute(f'SELECT * FROM {table}')]
                  for table in orders}
        for table,order in orders.items():
            ddl=db.execute('SELECT sql FROM sqlite_master WHERE name=?',(table,)).fetchone()[0]
            body=ddl[ddl.index('(')+1:ddl.rindex(')')]
            chunks=[];depth=0;start=0
            for n,char in enumerate(body):
                if char=='(':depth+=1
                elif char==')':depth-=1
                elif char==',' and depth==0:
                    chunks.append(body[start:n].strip());start=n+1
            chunks.append(body[start:].strip())
            columns={part.split()[0]:part for part in chunks if part.split()[0] in order}
            constraints=[part for part in chunks if part.split()[0] not in columns]
            db.execute('CREATE TABLE reordered ('+','.join([columns[c] for c in order]+constraints)+')')
            fields=','.join(order)
            db.execute(f'INSERT INTO reordered({fields}) SELECT {fields} FROM {table}')
            db.execute(f'DROP TABLE {table}')
            db.execute(f'ALTER TABLE reordered RENAME TO {table}')
            db.commit()
        self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
        db.close()
        result=reconcile([reordered],self.root/'ordered-output.db',self.root/'ordered-report.json')
        out=sqlite3.connect(self.root/'ordered-output.db');out.row_factory=sqlite3.Row
        for table in orders:
            self.assertEqual([dict(r) for r in out.execute(f'SELECT * FROM {table}')],original[table])
        self.assertEqual(out.execute('PRAGMA foreign_key_check').fetchall(),[])
        self.assertEqual(len(get_resolution_mentions(out,self.manifest['batch_id'],self.eid)),2)
        out.close()
        self.assertEqual(result['counts']['entity_resolution_mentions'],2)
        repeated=reconcile([self.root/'ordered-output.db'],self.root/'ordered-again.db',
                           self.root/'ordered-again.json')
        self.assertEqual(repeated['counts']['entity_resolution_mentions'],2)

if __name__=='__main__': unittest.main()
