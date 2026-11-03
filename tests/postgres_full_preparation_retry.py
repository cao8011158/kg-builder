"""Native UUID-schema regression for bounded source retries; no external API."""
import json
from unittest.mock import patch
from urllib.error import URLError

from kg_builder import full_preparation as full
from tests.postgres_fixtures import PostgresCase
from tests.postgres_full_preparation import atom


class NativeSourceRetryTests(PostgresCase):
    def wf(self,fetch):
        return self.workflow(full.FullPreparationWorkflow,self.tmp.name,fetcher=fetch,client=object())
    def stream(self,w,source='arxiv-0'):
        db=w._db()
        try:return dict(db.execute('SELECT * FROM full_streams WHERE stream_id=%s',(source,)).fetchone())
        finally:w._release(db)
    def test_native_arxiv_retry_reacquires_gate_and_replays_durable_page(self):
        calls=[];tokens=[];gates=[]
        def fetch(source,plan,window,position):
            row=self.stream(w);tokens.append(row['lease_token']);gates.append(row['next_request_at'])
            calls.append(position)
            if len(calls)==1:raise URLError('sanitized only')
            return atom([1],2) if position=='0' else atom([2],2)
        w=self.wf(fetch)
        with patch.object(full,'_source_retry_delay',return_value=0):w._stream('arxiv-0')
        self.assertEqual(calls,['0','0','1'])
        self.assertEqual(len(set(tokens)),3)
        self.assertTrue(all(b-a>=3.049 for a,b in zip(gates,gates[1:])))
        row=self.stream(w);self.assertEqual(row['state'],'done');self.assertEqual(row['received'],2)
        db=w._db()
        try:
            pages=db.execute("SELECT position,state,applied_rows FROM full_pages WHERE stream_id='arxiv-0' ORDER BY position").fetchall()
            self.assertEqual([(r['position'],r['state'],r['applied_rows']) for r in pages],[('0','applied',1),('1','applied',1)])
        finally:w._release(db)
        # A completed stream resumes without refetching its durable pages.
        w._stream('arxiv-0');self.assertEqual(len(calls),3)
    def test_native_cap_preserves_cursor_and_cancellation_does_not_exhaust(self):
        calls=[]
        def fetch(*args):calls.append(args[-1]);raise URLError('no URL emitted')
        w=self.wf(fetch)
        with patch.object(full,'_source_retry_delay',return_value=0):
            with self.assertRaises(URLError):w._stream('openalex-0')
        row=self.stream(w,'openalex-0')
        self.assertEqual(calls,['*']*5);self.assertEqual(row['state'],'failed')
        self.assertEqual(row['position'],'*');self.assertEqual(row['received'],0)
        def stop(delay):w._stop_collection.set();return True
        with patch.object(w._stop_collection,'wait',side_effect=stop):
            with self.assertRaises(InterruptedError):w._stream('openalex-1')
        self.assertEqual(self.stream(w,'openalex-1')['state'],'open')
        db=w._db()
        try:self.assertNotEqual(db.execute('SELECT collection_state FROM run WHERE id=1').fetchone()['collection_state'],'sealed')
        finally:w._release(db)
