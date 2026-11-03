import tempfile
import unittest
from pathlib import Path

from kg_builder.incremental_update import prepare_incremental_snapshot, write_immutable_json
from kg_builder.snapshot_graph import canonical_hash, read_snapshot


def paper(pid, version='v1', *, doi=None, arxiv_id=None, text='Text'):
    url = 'https://example.org/' + pid
    row = {'paper_id': pid, 'version_id': pid + '@' + (version or 'unverified'),
           'title': pid, 'text': text,
           'paper_version': {'version_label': version, 'source_url': url},
           'doi': doi, 'arxiv_id': arxiv_id}
    row['source_hash'] = canonical_hash({
        'paper_id': pid, 'version_id': row['version_id'], 'title': pid, 'text': text,
        'version_label': version, 'source_url': url})
    return row


def snapshot(path, rows, *, policy='legacy-paper', model='model-a'):
    extractions = []
    vectors = []
    reviews = {}
    for row in rows:
        pid = row['paper_id']
        item = {'key': 'm', 'name': pid, 'description': row['text'], 'role': 'proposed'}
        extractions.append({
            'paper_id': pid, 'version_id': row['version_id'],
            'paper_version': row['paper_version'], 'status': 'reviewed',
            'research_problems': [], 'methods': [item],
            'benchmarks': [], 'contributions': []})
        vectors.append({'item_key': pid + '::m', 'kind': 'Method',
                        'text': pid + '\n' + row['text'], 'model_id': model,
                        'vector': [1.0, 0.0]})
        reviews[pid] = {'policy': 'item-v1', 'items': [{
            'field': 'methods', 'key': 'm', 'state': 'approved',
            'approved_payload_hash': canonical_hash(item)}]}
    result = {'schema_version': 'research-kg-prepared-snapshot/1',
              'run_id': path.stem, 'config': {'review_policy': policy},
              'papers': rows, 'extractions': extractions, 'vectors': vectors}
    if policy == 'item-v1':
        result['review_metadata'] = reviews
    result['snapshot_hash'] = canonical_hash(result)
    write_immutable_json(path, result)
    return result


class IncrementalSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.base, self.delta, self.output = (root / x for x in ('base.json', 'delta.json', 'out.json'))

    def test_replace_add_remove_and_replay(self):
        a = paper('a')
        b = paper('b')
        snapshot(self.base, [a, b], policy='item-v1')
        replacement = paper('a', 'v2', text='New')
        added = paper('c')
        snapshot(self.delta, [replacement, added], policy='item-v1')
        input_bytes = (self.base.read_bytes(), self.delta.read_bytes())
        first = prepare_incremental_snapshot(self.base, self.delta, self.output, remove_paper_ids=['b'])
        bytes_before = self.output.read_bytes()
        mtime = self.output.stat().st_mtime_ns
        self.assertEqual([p['paper_id'] for p in first['papers']], ['a', 'c'])
        self.assertEqual(first['incremental']['removed_paper_ids'], ['b'])
        self.assertEqual(set(first['review_metadata']), {'a', 'c'})
        self.assertEqual([row['item_key'] for row in first['vectors']], ['a::m', 'c::m'])
        self.assertEqual(first['vectors'][0]['text'], 'a\nNew')
        self.assertEqual(input_bytes, (self.base.read_bytes(), self.delta.read_bytes()))
        self.assertEqual(first['snapshot_hash'], read_snapshot(self.output)[0]['snapshot_hash'])
        self.assertEqual(first, prepare_incremental_snapshot(self.base, self.delta, self.output, remove_paper_ids=['b']))
        self.assertEqual((bytes_before, mtime), (self.output.read_bytes(), self.output.stat().st_mtime_ns))

    def test_preserves_unchanged_payload_and_review(self):
        unchanged = paper('a')
        snapshot(self.base, [unchanged], policy='item-v1')
        snapshot(self.delta, [], policy='item-v1')
        merged = prepare_incremental_snapshot(self.base, self.delta, self.output)
        self.assertEqual(merged['papers'][0], unchanged)
        self.assertEqual(merged['vectors'][0]['item_key'], 'a::m')
        self.assertEqual(merged['review_metadata']['a']['items'][0]['state'], 'approved')

    def test_rejects_downgrade_and_identity_collision(self):
        snapshot(self.base, [paper('a', 'v2', doi='10.1000/ONE')])
        snapshot(self.delta, [paper('a', 'v1')])
        with self.assertRaisesRegex(ValueError, 'downgrade'):
            prepare_incremental_snapshot(self.base, self.delta, self.output)
        self.delta.unlink()
        snapshot(self.delta, [paper('b', doi='https://doi.org/10.1000/one')])
        with self.assertRaisesRegex(ValueError, 'identity'):
            prepare_incremental_snapshot(self.base, self.delta, self.output)
        self.assertFalse(self.output.exists())

    def test_rejects_conflicting_same_version(self):
        snapshot(self.base, [paper('a', 'v1')])
        snapshot(self.delta, [paper('a', 'v1', text='Changed')])
        with self.assertRaisesRegex(ValueError, 'Conflicting payload'):
            prepare_incremental_snapshot(self.base, self.delta, self.output)
        self.assertFalse(self.output.exists())

    def test_rejects_mixed_policy_model_and_conflicting_output(self):
        snapshot(self.base, [paper('a')])
        snapshot(self.delta, [paper('b')], policy='item-v1')
        with self.assertRaisesRegex(ValueError, 'policies'):
            prepare_incremental_snapshot(self.base, self.delta, self.output)
        self.delta.unlink()
        snapshot(self.delta, [paper('b')], model='model-b')
        with self.assertRaisesRegex(ValueError, 'models'):
            prepare_incremental_snapshot(self.base, self.delta, self.output)
        self.delta.unlink()
        snapshot(self.delta, [paper('b')])
        write_immutable_json(self.output, {'different': True})
        with self.assertRaisesRegex(ValueError, 'different content'):
            prepare_incremental_snapshot(self.base, self.delta, self.output)

    def test_rejects_alias_and_implicit_removal(self):
        snapshot(self.base, [paper('a')])
        snapshot(self.delta, [])
        with self.assertRaisesRegex(ValueError, 'distinct'):
            prepare_incremental_snapshot(self.base, self.delta, self.base)
        with self.assertRaisesRegex(ValueError, 'unknown paper'):
            prepare_incremental_snapshot(self.base, self.delta, self.output, remove_paper_ids=['missing'])


if __name__ == '__main__':
    unittest.main()
