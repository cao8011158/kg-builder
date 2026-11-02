"""Offline adapter contract checks: no tokenizer downloads or paid requests."""
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback
import types
import unittest
from unittest.mock import patch

import numpy as np

from kg_builder.embeddings import (
    EmbeddingModel, VoyageEmbeddingModel, create_embedding_model, embedding_model_from_id,
)


class FakeClient:
    def __init__(self, **kwargs):
        self.options = kwargs
        self.calls = []
        self.token_calls = []
        self.failure = None
        self.response = None
        self.total_tokens = 10

    def tokenize(self, texts, model):
        self.token_calls.append((list(texts), model))
        return [range(int(t.split(':')[1]) if t.startswith('tokens:') else len(t)) for t in texts]

    def embed(self, texts, **kwargs):
        self.calls.append((list(texts), kwargs))
        if self.failure:
            raise self.failure
        if self.response is not None:
            return types.SimpleNamespace(embeddings=self.response, total_tokens=self.total_tokens)
        vectors = []
        for text in texts:
            vector = [0.] * kwargs['output_dimension']
            vector[sum(text.encode()) % len(vector)] = 1.
            vectors.append(vector)
        return types.SimpleNamespace(embeddings=vectors, total_tokens=self.total_tokens)


class VoyageTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()

    def model(self, **kwargs):
        model = VoyageEmbeddingModel(**kwargs)
        model._get_client = lambda: self.client
        return model

    def assertUsage(self, model, expected):
        self.assertEqual({key: model.usage[key] for key in expected}, expected)

    def test_offline_construction_empty_and_factory(self):
        with patch.dict(sys.modules, {'voyageai': None}), patch.dict(os.environ, {}, clear=True):
            model = create_embedding_model('voyage')
            restored = embedding_model_from_id(model.model_id)
            self.assertEqual(restored.model_id, model.model_id)
            self.assertFalse(hasattr(restored._local, "client"))
            self.assertEqual(restored.encode([]).shape, (0, 1024))
            self.assertEqual(restored.encode_query([]).dtype, np.float32)
            with self.assertRaisesRegex(RuntimeError, 'VOYAGE_API_KEY'):
                restored.encode(['not cached'])

    def test_lazy_client_reads_environment_and_reuses_bounded_options(self):
        clients = []
        def construct(**kwargs):
            client = FakeClient(**kwargs)
            clients.append(client)
            return client
        with patch.dict(sys.modules, {'voyageai': types.SimpleNamespace(Client=construct)}):
            model = VoyageEmbeddingModel()
            self.assertEqual(clients, [])
            with patch.dict(os.environ, {'VOYAGE_API_KEY': 'test-secret'}):
                model.encode(['first'])
                model.encode_query(['second'])
        self.assertGreaterEqual(len(clients), 3)
        self.assertEqual(clients[0].options, {'api_key': 'test-secret', 'timeout': 30, 'max_retries': 2})
        self.assertNotIn('test-secret', model.model_id)

    def test_roles_order_and_float_contract(self):
        model = self.model(dimension=256, batch_size=2, max_concurrency=1)
        texts = ['one', 'two', 'three']
        document = model.encode(texts)
        query = model.encode_query(texts)
        self.assertEqual(document.shape, (3, 256))
        self.assertEqual(document.dtype, np.float32)
        np.testing.assert_array_equal(document, query)
        self.assertEqual([int(np.argmax(row)) for row in document], [sum(t.encode()) % 256 for t in texts])
        self.assertEqual([t for texts, _ in self.client.calls[:2] for t in texts], texts)
        self.assertEqual([opts['input_type'] for _, opts in self.client.calls], ['document', 'document', 'query', 'query'])
        for _, opts in self.client.calls:
            self.assertEqual(opts, dict(model='voyage-4-large', input_type=opts['input_type'],
                                       output_dimension=256, output_dtype='float', truncation=False))
        self.assertNotEqual(model.model_id, model.query_model_id)

    def test_token_budget_splits_and_overlong_later_input_prevents_all_requests(self):
        model = self.model(max_concurrency=1)
        model.encode(['tokens:31000'] * 8)
        self.assertEqual([len(texts) for texts, _ in self.client.calls], [3, 3, 2])
        self.client.calls.clear()
        model.encode(['tokens:30001'] * 4)
        self.assertEqual([len(texts) for texts, _ in self.client.calls], [3, 1])
        self.client.calls.clear()
        with self.assertRaisesRegex(ValueError, 'token limit'):
            model.encode(['fine'] * 40 + ['tokens:32001'])
        self.assertEqual(self.client.calls, [])

    def test_usage_tracks_sdk_calls_provider_tokens_and_unknown_usage(self):
        model = self.model(batch_size=2)
        self.assertUsage(model, {'sdk_requests': 0, 'total_tokens': None})
        model.encode([])
        self.assertUsage(model, {'sdk_requests': 0, 'total_tokens': None})
        model.encode(['a', 'b', 'c'])
        self.assertUsage(model, {'sdk_requests': 2, 'total_tokens': 20})
        model.encode_query(['query'])
        self.assertUsage(model, {'sdk_requests': 3, 'total_tokens': 30})
        snapshot = model.usage
        snapshot['total_tokens'] = 999
        self.assertEqual(model.usage['total_tokens'], 30)
        self.client.total_tokens = None
        model.encode(['missing usage'])
        self.assertUsage(model, {'sdk_requests': 4, 'total_tokens': None})
        self.client.total_tokens = 10
        model.encode(['reported again'])
        self.assertIsNone(model.usage['total_tokens'])
        failed = self.model()
        self.client.failure = RuntimeError('private details')
        with self.assertRaises(RuntimeError):
            failed.encode(['input'])
        self.assertUsage(failed, {'sdk_requests': 1, 'total_tokens': None})

    def test_reported_usage_survives_invalid_vectors_and_zero_is_not_missing(self):
        model = self.model()
        self.client.response = []
        self.client.total_tokens = 0
        with self.assertRaises(ValueError):
            model.encode(['input'])
        self.assertUsage(model, {'sdk_requests': 1, 'total_tokens': 0})

    def test_malformed_shape_count_nonfinite_and_norm_rejected(self):
        model = self.model(dimension=256)
        for response in ([], [[1.] * 255], [[1.] + [0.] * 255] * 2,
                         [[float('nan')] + [0.] * 255], [[0.] * 256], [[2.] + [0.] * 255]):
            with self.subTest(response_length=len(response)):
                self.client.response = response
                with self.assertRaises(ValueError):
                    model.encode(['input'])

    def test_later_failure_returns_no_partial_array_and_hides_provider_details(self):
        model = self.model(batch_size=1)
        original = self.client.embed
        def fail_second(texts, **kwargs):
            if texts == ['second']:
                raise RuntimeError('credential-secret and request text')
            return original(texts, **kwargs)
        self.client.embed = fail_second
        try:
            model.encode(['first', 'second'])
        except RuntimeError:
            rendered = traceback.format_exc()
        else:
            self.fail('partial success must not return')
        self.assertNotIn('credential-secret', rendered)
        self.assertIn('Voyage embedding request failed', rendered)

    def test_id_roundtrip_dimensions_and_strict_rejection(self):
        identities = set()
        for dimension in (256, 512, 1024, 2048):
            model = VoyageEmbeddingModel(dimension=dimension)
            identities.add(model.model_id)
            self.assertEqual(embedding_model_from_id(model.model_id).dimension, dimension)
        self.assertEqual(len(identities), 4)
        model = VoyageEmbeddingModel()
        invalid = [model.query_model_id, model.model_id.replace('float', 'int8'),
                   model.model_id.replace('1024', '768'), model.model_id + ':extra', '', None,
                   'fastembed-test:unknown:research-problem-text-v1:l2-v1']
        for identity in invalid:
            with self.assertRaises(ValueError):
                embedding_model_from_id(identity)
        with patch('kg_builder.embeddings.version', return_value='exact'):
            existing = EmbeddingModel()
            self.assertEqual(embedding_model_from_id(existing.model_id).model_id, existing.model_id)
            with self.assertRaises(ValueError):
                embedding_model_from_id(existing.model_id.replace('exact', 'older'))
            with self.assertRaises(ValueError):
                embedding_model_from_id('fastembed-exact:unknown:research-problem-text-v1:l2-v1')
            from fastembed import TextEmbedding
            with patch.object(TextEmbedding, 'list_supported_models', return_value=[{'model': 'other-supported'}]):
                identity = 'fastembed-exact:other-supported:research-problem-text-v1:l2-v1'
                self.assertEqual(embedding_model_from_id(identity).model_id, identity)

    def test_parallel_batches_are_bounded_ordered_and_share_role_limit(self):
        model = self.model(dimension=256, batch_size=1, max_concurrency=2)
        entered = threading.Event()
        release = threading.Event()
        lock = threading.Lock()
        active = peak = calls = outstanding = peak_outstanding = 0
        class TrackingExecutor(ThreadPoolExecutor):
            def submit(self, *args, **kwargs):
                nonlocal outstanding, peak_outstanding
                with lock:
                    outstanding += 1
                    peak_outstanding = max(peak_outstanding, outstanding)
                future = super().submit(*args, **kwargs)
                def finished(_):
                    nonlocal outstanding
                    with lock:
                        outstanding -= 1
                future.add_done_callback(finished)
                return future
        original = self.client.embed

        def blocking(texts, **kwargs):
            nonlocal active, peak, calls
            with lock:
                active += 1
                calls += 1
                peak = max(peak, active)
                if active == 2:
                    entered.set()
            try:
                if not release.wait(10):
                    raise AssertionError("test did not release blocked requests")
                return original(texts, **kwargs)
            finally:
                with lock:
                    active -= 1

        self.client.embed = blocking
        with patch('kg_builder.embeddings.ThreadPoolExecutor', TrackingExecutor), \
                ThreadPoolExecutor(max_workers=2) as callers:
            document = callers.submit(model.encode, ['a', 'b', 'c', 'd', 'e'])
            query = callers.submit(model.encode_query, ['f', 'g', 'h', 'i', 'j'])
            try:
                self.assertTrue(entered.wait(10))
                with lock:
                    self.assertEqual(active, 2)
                    self.assertEqual(calls, 2)
                self.assertEqual(model.usage['peak_concurrency'], 2)
            finally:
                release.set()
            for future, texts in ((document, 'abcde'), (query, 'fghij')):
                result = future.result(timeout=10)
                self.assertEqual([int(np.argmax(row)) for row in result],
                                 [ord(text) for text in texts])
        self.assertEqual(peak, 2)
        self.assertEqual(peak_outstanding, 2)
        self.assertEqual(outstanding, 0)
        self.assertEqual(model.usage['sdk_requests'], 10)
        self.assertEqual(model.usage['active_requests'], 0)
        self.assertEqual(model.encode_window_size, 2)
        self.assertGreaterEqual(model.usage['request_cumulative_seconds'],
                                model.usage['request_wall_union_seconds'])

    def test_official_clients_are_owned_by_their_creating_thread(self):
        clients = []
        barrier = threading.Barrier(2)
        class OwnedClient(FakeClient):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.owner = threading.get_ident()
                clients.append(self)
            def embed(self, texts, **kwargs):
                if self.owner != threading.get_ident():
                    raise AssertionError("SDK client crossed threads")
                barrier.wait(timeout=10)
                return super().embed(texts, **kwargs)
        with patch.dict(sys.modules, {'voyageai': types.SimpleNamespace(Client=OwnedClient)}), \
                patch.dict(os.environ, {'VOYAGE_API_KEY': 'test-key'}):
            model = VoyageEmbeddingModel(batch_size=1, max_concurrency=2)
            model.encode(['first', 'second'])
        self.assertEqual(len(clients), 3)  # caller tokenizer and two request workers
        self.assertEqual(len({client.owner for client in clients}), 3)

    def test_rate_limiter_reserves_retry_allowance_and_paces_with_fake_clock(self):
        model = self.model(batch_size=1, max_concurrency=1, max_retries=1,
                           requests_per_minute=4, tokens_per_minute=8)
        now = [0.0]
        waits = []
        model._clock = lambda: now[0]
        def advance(seconds):
            waits.append(seconds)
            now[0] += seconds
        model._sleep = advance
        model.encode(['aa', 'bb', 'cc'])
        self.assertEqual(waits, [60.0])
        self.assertEqual(model.usage['sdk_requests'], 3)
        self.assertEqual(model.usage['http_attempts_upper_bound'], 6)
        self.assertEqual(model.usage['reserved_input_tokens'], 12)
        self.assertEqual(model.usage['queue_seconds'], 60.0)
        with self.assertRaisesRegex(ValueError, 'token rate budget'):
            model.encode(['tokens:5'])
        self.assertEqual(model.usage['sdk_requests'], 3)
        # TPM also splits a batch before reservation, rather than waiting forever.
        model = self.model(batch_size=32, max_concurrency=1, max_retries=0,
                           requests_per_minute=None, tokens_per_minute=4)
        model._clock = lambda: now[0]
        model._sleep = advance
        self.client.calls.clear()
        model.encode(['aa', 'bb', 'cc'])
        self.assertEqual([len(texts) for texts, _ in self.client.calls], [2, 1])

    def test_identity_is_independent_of_pacing_and_concurrency(self):
        model = VoyageEmbeddingModel(max_concurrency=1, requests_per_minute=None,
                                     tokens_per_minute=None)
        self.assertEqual(model.model_id, VoyageEmbeddingModel().model_id)
        self.assertEqual(model.query_model_id, VoyageEmbeddingModel().query_model_id)
        configured = create_embedding_model('voyage', max_concurrency=3,
                                            requests_per_minute=300, tokens_per_minute=10000)
        self.assertEqual(configured.encode_window_size, 96)
        self.assertEqual(configured.requests_per_minute, 300)
        restored = embedding_model_from_id(configured.model_id, max_concurrency=3,
                                           requests_per_minute=300, tokens_per_minute=10000)
        self.assertEqual(restored.encode_window_size, 96)
        self.assertEqual(restored.tokens_per_minute, 10000)

    def test_configuration_and_input_validation(self):
        for kwargs in ({'dimension': True}, {'dimension': 768}, {'model_name': 'unknown'},
                       {'batch_size': 1001}, {'batch_size': False}, {'timeout': float('nan')},
                       {'timeout': 0}, {'timeout': 301}, {'max_retries': 6},
                       {'max_concurrency': 0}, {'max_concurrency': True}, {'max_concurrency': 65},
                       {'requests_per_minute': 0}, {'requests_per_minute': 2},
                       {'tokens_per_minute': False}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                VoyageEmbeddingModel(**kwargs)
        for texts in ('text', [1], None):
            with self.assertRaises(TypeError):
                self.model().encode(texts)
        with self.assertRaises(ValueError):
            create_embedding_model('unknown')


if __name__ == '__main__':
    unittest.main()
