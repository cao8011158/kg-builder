import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import publish_incremental_graph as publish, scheduled_refresh


class IncrementalPublishTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.argv = [
            '--base-run-id', 'test-base',
            '--base-snapshot', str(self.directory / 'base.json'),
            '--delta-snapshot', str(self.directory / 'delta.json'),
            '--release-dir', str(self.directory),
        ]
        self.snapshot_hash = 'a' * 64
        self.release = self.directory / 'release.json'
        contexts = contextlib.ExitStack()
        self.addCleanup(contexts.close)
        contexts.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.prepare = contexts.enter_context(patch.object(
            publish, 'prepare_incremental_snapshot', return_value={'snapshot_hash': self.snapshot_hash},
        ))
        self.graph = contexts.enter_context(patch.object(publish.build_snapshot_graph, 'main'))
        self.audit = contexts.enter_context(patch(
            'kg_builder.graph_validation.validate_graph', return_value={'structural': {'passed': True}},
        ))
        self.index = contexts.enter_context(patch.object(
            publish, '_bm25_manifest',
            return_value={'graph_namespace': 'graph:test', 'snapshot_hash': self.snapshot_hash},
        ))
        self.status = {
            'phase': 'complete', 'completed_at': 1, 'index_status': 'verified',
            'neo4j_run_id': 'graph:test', 'snapshot_hash': self.snapshot_hash,
            'neo4j_projection': {'pending': 0},
        }
        self.graph.return_value = self.status

    def test_prepare_only_and_default_disabled_timer(self):
        self.assertEqual(publish.main([*self.argv, '--prepare-only'])['state'], 'prepared')
        with patch.dict('os.environ', {'ENABLE_SCHEDULED_KG_REFRESH': 'false'}):
            with self.assertRaisesRegex(RuntimeError, 'disabled'):
                scheduled_refresh.run_scheduled_refresh(self.argv)
        self.prepare.assert_called_once()
        self.graph.assert_not_called()
        self.audit.assert_not_called()
        self.index.assert_not_called()
        self.assertFalse(self.release.exists())

    def test_waiting_for_names_then_publish_and_replay_via_timer(self):
        self.graph.return_value = {'phase': 'grouped', 'name_tasks': {'leased': 1}}
        pending = publish.main(self.argv)
        self.assertEqual(pending['state'], 'waiting_for_names')
        self.assertFalse(self.release.exists())
        self.audit.assert_not_called()
        self.index.assert_not_called()

        self.graph.return_value = self.status
        published = publish.main(self.argv)
        self.assertEqual(published['state'], 'published')
        self.assertEqual(pending['run_id'], published['run_id'])
        self.assertEqual(json.loads(self.release.read_text(encoding='utf-8')), published)
        with patch.dict('os.environ', {'ENABLE_SCHEDULED_KG_REFRESH': 'true'}):
            self.assertEqual(scheduled_refresh.run_scheduled_refresh(self.argv), published)
        self.assertEqual(self.graph.call_count, 2)
        self.audit.assert_called_once()
        self.index.assert_called_once()

    def test_release_requires_projection_structural_validation_and_matching_index(self):
        self.graph.return_value = {**self.status, 'neo4j_projection': {'pending': 1}}
        with self.assertRaisesRegex(RuntimeError, 'undelivered'):
            publish.main(self.argv)
        self.audit.assert_not_called()

        self.graph.return_value = self.status
        self.audit.return_value = {'structural': {'passed': False, 'failure_counts': {'test': 1}}}
        with self.assertRaisesRegex(RuntimeError, 'structural validation'):
            publish.main(self.argv)
        self.index.assert_not_called()

        self.audit.return_value = {'structural': {'passed': True}}
        self.index.return_value = {'graph_namespace': 'graph:other', 'snapshot_hash': self.snapshot_hash}
        with self.assertRaisesRegex(ValueError, 'BM25 index differs'):
            publish.main(self.argv)
        self.assertFalse(self.release.exists())
