"""Offline adapter contract checks: no tokenizer downloads or paid requests."""
import os
import sys
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
        model._client = self.client
        return model

    def test_offline_construction_empty_and_factory(self):
        with patch.dict(sys.modules, {'voyageai': None}), patch.dict(os.environ, {}, clear=True):
            model = create_embedding_model('voyage')
            restored = embedding_model_from_id(model.model_id)
            self.assertEqual(restored.model_id, model.model_id)
            self.assertIsNone(restored._client)
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
        self.assertEqual(len(clients), 1)
        self.assertEqual(clients[0].options, {'api_key': 'test-secret', 'timeout': 30, 'max_retries': 2})
        self.assertNotIn('test-secret', model.model_id)

    def test_roles_order_and_float_contract(self):
        model = self.model(dimension=256, batch_size=2)
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
        model = self.model()
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
        self.assertEqual(model.usage, {'sdk_requests': 0, 'total_tokens': None})
        model.encode([])
        self.assertEqual(model.usage, {'sdk_requests': 0, 'total_tokens': None})
        model.encode(['a', 'b', 'c'])
        self.assertEqual(model.usage, {'sdk_requests': 2, 'total_tokens': 20})
        model.encode_query(['query'])
        self.assertEqual(model.usage, {'sdk_requests': 3, 'total_tokens': 30})
        snapshot = model.usage
        snapshot['total_tokens'] = 999
        self.assertEqual(model.usage['total_tokens'], 30)
        self.client.total_tokens = None
        model.encode(['missing usage'])
        self.assertEqual(model.usage, {'sdk_requests': 4, 'total_tokens': None})
        self.client.total_tokens = 10
        model.encode(['reported again'])
        self.assertIsNone(model.usage['total_tokens'])
        failed = self.model()
        self.client.failure = RuntimeError('private details')
        with self.assertRaises(RuntimeError):
            failed.encode(['input'])
        self.assertEqual(failed.usage, {'sdk_requests': 1, 'total_tokens': None})

    def test_reported_usage_survives_invalid_vectors_and_zero_is_not_missing(self):
        model = self.model()
        self.client.response = []
        self.client.total_tokens = 0
        with self.assertRaises(ValueError):
            model.encode(['input'])
        self.assertEqual(model.usage, {'sdk_requests': 1, 'total_tokens': 0})

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

    def test_configuration_and_input_validation(self):
        for kwargs in ({'dimension': True}, {'dimension': 768}, {'model_name': 'unknown'},
                       {'batch_size': 1001}, {'batch_size': False}, {'timeout': float('nan')},
                       {'timeout': 0}, {'timeout': 301}, {'max_retries': 6}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                VoyageEmbeddingModel(**kwargs)
        for texts in ('text', [1], None):
            with self.assertRaises(TypeError):
                self.model().encode(texts)
        with self.assertRaises(ValueError):
            create_embedding_model('unknown')


if __name__ == '__main__':
    unittest.main()
