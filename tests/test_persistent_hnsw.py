import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from kg_builder.vector_index import HNSWIndex
from kg_builder.persistent_dense import PersistentDenseIndex


class HNSWSnapshotTests(unittest.TestCase):
    def test_load_windows_created_fixture(self):
        path = Path(__file__).parent / 'fixtures' / 'hnsw_windows_v1'
        with patch.object(HNSWIndex, 'upsert', side_effect=AssertionError('reinsert')):
            loaded = HNSWIndex.load(path, expected_model_id='fixture:windows-0.8')
        self.assertEqual(loaded.search([1., 0.], 1)[0]['id'], 'win:a')

    def test_round_trip_update_delete_and_no_reinsertion(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'index'
            source = HNSWIndex({'a': [1., 0.], 'b': [0., 1.]}, 'model:1')
            source.upsert('a', [0.8, 0.6])
            source.delete('b')
            source.save(path)
            with patch.object(HNSWIndex, 'upsert', side_effect=AssertionError('reinsert')):
                loaded = HNSWIndex.load(path, expected_model_id='model:1')
            self.assertEqual(loaded.search([1., 0.], 2)[0]['id'], 'a')
            self.assertAlmostEqual(loaded.search([1., 0.], 1)[0]['similarity'], 0.8, places=5)
            loaded.upsert('c', [0., 1.])
            self.assertEqual(loaded.search([0., 1.], 1)[0]['id'], 'c')

    def test_mismatch_and_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'index'
            HNSWIndex({'a': [1., 0.]}, 'model:1').save(path)
            with self.assertRaisesRegex(ValueError, 'model mismatch'):
                HNSWIndex.load(path, expected_model_id='model:2')
            (path / 'vectors.npy').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'Corrupt'):
                HNSWIndex.load(path)

    def test_persistent_wrapper_kind_filter_without_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            HNSWIndex({'Paper\0p': [1., 0.], 'Method\0m': [0., 1.]}, 'model').save(path / 'hnsw')
            from kg_builder.semantic_store import digest
            records = [{'kind': 'Paper', 'node_id': 'p', 'text_hash': 'a'},
                       {'kind': 'Method', 'node_id': 'm', 'text_hash': 'b'}]
            payload = json.dumps(records).encode()
            (path / 'records.json').write_bytes(payload)
            import hashlib
            metadata = {'format': 'kg-persistent-dense/1', 'run_id': 'r',
                'graph_namespace': 'graph:r', 'model_id': 'model', 'dimension': 2,
                'count': 2, 'kind_counts': {'Paper': 1, 'Method': 1},
                'records_hash': digest(records), 'records_sha256': hashlib.sha256(payload).hexdigest(),
                'hnsw_manifest_sha256': hashlib.sha256((path / 'hnsw' / 'manifest.json').read_bytes()).hexdigest()}
            (path / 'manifest.json').write_text(json.dumps(metadata), encoding='utf-8')
            with patch.object(HNSWIndex, 'upsert', side_effect=AssertionError('reinsert')):
                index = PersistentDenseIndex(path, expected_run_id='r', expected_graph_namespace='graph:r')
            self.assertEqual(index.search([1., 0.], types=['Method'], top_k=1)[0]['node_id'], 'm')
            with self.assertRaisesRegex(ValueError, 'model_id mismatch'):
                PersistentDenseIndex(path, expected_model_id='other')


if __name__ == '__main__':
    unittest.main()
