"""Offline source retry protocol tests; no network or production database."""
import asyncio
from contextlib import nullcontext
from datetime import datetime, timezone
from email.utils import format_datetime
import importlib.util
import json
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.error import HTTPError, URLError

import kg_builder.full_preparation as full

# A fixed HEAD copy can be selected to prove the regression fails before repair.
if os.environ.get('KG_RETRY_BASELINE'):
    spec=importlib.util.spec_from_file_location('kg_builder._retry_baseline',os.environ['KG_RETRY_BASELINE'])
    full=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(full)


class DB:
    """Bounded stream/page SQL protocol fixture, intentionally rejects unknown SQL."""
    def __init__(self,source='openalex'):
        self.stream=dict(state='open',source=source,window_index=0,position='*' if source=='openalex' else '0',
                         windows_json='[{}]',lease_until=None,lease_token=None,next_request_at=0,window_received=0)
        self.page=None
        self.row=None
        self.rowcount=1
        self.gates=[]
        self.leases=[]
        self.failures=[]
    def transaction(self):return nullcontext()
    def fetchone(self):return self.row
    def execute(self,query,args=()):
        query=' '.join(query.split())
        self.rowcount=1
        if query.startswith('SELECT * FROM full_streams'):self.row=dict(self.stream)
        elif query.startswith('SELECT state FROM full_streams'):self.row={'state':self.stream['state']}
        elif query.startswith('SELECT lease_token'):self.row=dict(self.stream)
        elif query.startswith('SELECT raw,'):self.row=self.page
        elif query.startswith('SELECT state,applied_rows'):self.row=self.page
        elif query.startswith("UPDATE full_streams SET state='processing'"):
            self.stream.update(state='processing',lease_token=args[0],lease_until=args[1]);self.leases.append(args[0])
        elif query.startswith('UPDATE full_streams SET next_request_at'):
            self.stream['next_request_at']=args[0];self.gates.append(args[0])
        elif query.startswith('INSERT INTO full_pages'):
            self.page=dict(raw=args[3],applied_rows=0,admitted=0,state='fetched')
        elif query.startswith("UPDATE full_pages SET state='applied'"):self.page['state']='applied'
        elif query.startswith('UPDATE full_streams SET window_index'):
            self.stream.update(window_index=args[0],position=args[1],state=args[2],lease_token=None,lease_until=None)
        elif query.startswith('UPDATE full_streams SET state=%s'):
            if args[-1]!=self.stream['lease_token']:self.rowcount=0
            else:
                self.stream.update(state=args[0],lease_token=None,lease_until=None,error=args[1]);self.failures.append(args[0])
        elif query.startswith("UPDATE full_streams SET state='failed'"):
            self.stream.update(state='failed',lease_token=None,lease_until=None,error=args[0]);self.failures.append('failed')
        else:raise AssertionError(query)
        return self


class RetryTests(unittest.TestCase):
    def workflow(self,db,fetcher):
        w=object.__new__(full.FullPreparationWorkflow)
        w.plan={'page_size':100};w.config={'lease_seconds':300}
        w._stop_collection=Mock(wraps=threading.Event())
        w._stop_collection.wait=Mock(return_value=False)
        w._db=lambda:db;w._release=lambda conn:None;w.fetcher=fetcher
        return w
    def raw(self,source):
        return b'{"results":[],"meta":{"count":0}}' if source=='openalex' else b'<feed xmlns:o="http://a9.com/-/spec/opensearch/1.1/"><o:totalResults>0</o:totalResults></feed>'
    def test_transient_page_recovers_and_reacquires_gate_and_lease(self):
        for source in ('arxiv','openalex'):
            with self.subTest(source=source):
                db=DB(source);fetch=Mock(side_effect=[URLError('secret URL'),TimeoutError(),self.raw(source)])
                w=self.workflow(db,fetch)
                with self.assertLogs(full.__name__,level='WARNING') as logs:w._stream(source+'-0')
                self.assertEqual(fetch.call_count,3)
                self.assertEqual(db.failures,['open','open'])
                self.assertEqual(len(set(db.leases)),3)
                self.assertEqual(db.stream['state'],'done')
                self.assertEqual(db.page['state'],'applied')
                self.assertNotIn('secret',str(logs.output))
                if source=='arxiv':
                    self.assertEqual(len(db.gates),3)
                    self.assertTrue(all(b-a>=3.049 for a,b in zip(db.gates,db.gates[1:])))
    def test_cap_five_preserves_cursor_and_failed_state(self):
        db=DB();fetch=Mock(side_effect=URLError('private'))
        w=self.workflow(db,fetch)
        with self.assertRaises(URLError):w._stream('openalex-0')
        self.assertEqual(fetch.call_count,5)
        self.assertEqual(db.failures,['open']*4+['failed'])
        self.assertEqual(db.stream['position'],'*');self.assertIsNone(db.page)
    def test_permanent_and_content_errors_never_retry(self):
        for error in (ValueError('bad'),HTTPError('private',400,'bad',{},None),HTTPError('private',403,'bad',{},None),HTTPError('private',404,'bad',{},None)):
            db=DB();fetch=Mock(side_effect=error);w=self.workflow(db,fetch)
            with self.assertRaises(type(error)):w._stream('openalex-0')
            self.assertEqual(fetch.call_count,1);self.assertEqual(db.stream['state'],'failed')
        db=DB();fetch=Mock(return_value=b'broken JSON');w=self.workflow(db,fetch)
        with self.assertRaises(ValueError):w._stream('openalex-0')
        self.assertEqual(fetch.call_count,1);self.assertIsNotNone(db.page)
    def test_http_transient_codes_and_retry_after(self):
        for code in (429,500,502,503,504):
            db=DB();error=HTTPError('private',code,'bad',{'Retry-After':'7'},None)
            fetch=Mock(side_effect=[error,self.raw('openalex')]);w=self.workflow(db,fetch)
            w._stream('openalex-0')
            w._stop_collection.wait.assert_called_once_with(7)
        with patch.object(full.time,'time',return_value=1000):
            error=HTTPError('private',503,'bad',{'Retry-After':format_datetime(datetime.fromtimestamp(1020,timezone.utc),usegmt=True)},None)
            self.assertEqual(full._source_retry_delay(error,1),20)
        self.assertEqual(full._source_retry_delay(URLError('x'),4),24.4)
        self.assertEqual(full._source_retry_delay(URLError('x'),5),30)
    def test_counter_resets_after_successful_page(self):
        db=DB();w=self.workflow(db,Mock())
        retry=full._RetrySourcePage('openalex',URLError('x'),0)
        w._consume=Mock(side_effect=[retry,True,retry,False]);db.stream['state']='done'
        w._stream('openalex-0')
        self.assertEqual([x.kwargs['attempt'] for x in w._consume.call_args_list],[1,2,1,2])
    def test_stop_during_backoff_raises_and_never_seals(self):
        db=DB();w=self.workflow(db,Mock(side_effect=URLError('x')))
        w._stop_collection.wait=Mock(return_value=True)
        with self.assertRaises(InterruptedError):w._stream('openalex-0')
        self.assertEqual(w.fetcher.call_count,1)
        self.assertNotEqual(db.stream['state'],'done')
    def test_saved_raw_page_does_not_fetch_or_reserve_gate(self):
        db=DB('arxiv');db.page=dict(raw=self.raw('arxiv'),applied_rows=0,admitted=0,state='fetched')
        w=self.workflow(db,Mock(side_effect=AssertionError('network')));w._stream('arxiv-0')
        w.fetcher.assert_not_called();self.assertEqual(db.gates,[])
    def test_both_fetch_adapters_disable_internal_retry(self):
        plan=dict(openalex_query='x',arxiv_query='x',page_size=100)
        for source in ('arxiv','openalex'):
            fetch=Mock(return_value=b'raw')
            full.fetch_page(source,plan,{'since':'2026-01-01','until':'2026-11-01'},'*',fetch=fetch)
            self.assertEqual(fetch.call_args.kwargs['max_retries'],1)
    def test_collect_stop_and_unfinished_stream_never_seal(self):
        for stopped in (True,False):
            db=MagicMock();db.execute.return_value.fetchone.return_value={'unfinished':1}
            w=self.workflow(db,Mock())
            w._stop_collection=threading.Event()
            w._stream=lambda stream: w._stop_collection.set() if stopped else None
            w._finish_empty=Mock()
            with self.assertRaises(InterruptedError):asyncio.run(w.collect())
            self.assertFalse(any("collection_state='sealed'" in str(call) for call in db.execute.call_args_list))
    def test_private_identity_adapter_forwards_attempt_and_clears_context(self):
        path=Path(__file__).resolve().parents[1]/'data/full-preparation-20261102/full_runtime.py'
        if not path.exists():self.skipTest('Private operational adapter is not part of checkout')
        spec=importlib.util.spec_from_file_location('_full_retry_adapter',path)
        adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
        w=object.__new__(adapter.IdentityQuarantineWorkflow);w._identity_context=threading.local()
        with patch.object(adapter.FullPreparationWorkflow,'_consume',side_effect=URLError('x')) as consume:
            with self.assertRaises(URLError):w._consume('arxiv-0',attempt=2)
            consume.assert_called_once_with('arxiv-0',attempt=2)
        self.assertIsNone(w._identity_context.stream_id)


if __name__=='__main__':unittest.main()
