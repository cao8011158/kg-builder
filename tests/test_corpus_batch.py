import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'scripts')]
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION
from kg_builder.storage import connect, ingest_extractions, upsert_paper
from apply_corpus_batch import SidecarFailure, main, run


class CorpusBatchTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.master, self.sidecar = self.root / 'master.db', self.root / 'sidecar.db'
        con = connect(self.master)
        self.paper, self.version = upsert_paper(con, {'title': 'A', 'arxiv_id': '2501.12345',
            'version_label': 'v1', 'url': 'https://arxiv.org/abs/2501.12345v1'}, 'arxiv', '2501.12345', 'q')
        self.other, self.other_version = upsert_paper(con, {'title': 'Prior', 'url': 'https://example.org/prior'},
                                                        'source', 'prior', 'q')
        con.close()
        self.pin = {'version_id': self.version, 'version_label': 'v1',
                    'source_url': 'https://arxiv.org/abs/2501.12345v1'}
        self.manifest = [{'paper_id': self.paper, 'paper_version': self.pin, 'already_processed': False}]
        self.reviews = [{'paper_id': self.paper, 'decision': 'include', 'topics': ['agent_memory'],
                         'reason': 'Relevant abstract', 'source_scope': 'abstract_only_metadata_snapshot'}]
        self.extractions = [{'schema_version': RETRIEVAL_SCHEMA_VERSION, 'paper_id': self.paper,
            'version_id': self.version, 'paper_version': {k: self.pin[k] for k in ('version_label', 'source_url')},
            'extractor_type': 'assistant_manual', 'extractor_version': 'corpus-v1',
            'source_scope': 'abstract_only_metadata_snapshot', 'status': 'reviewed_for_retrieval_not_reproduced',
            'contributions': [], 'research_problems': [], 'methods': [], 'benchmarks': []}]

    def db(self, sql, args=()):
        import sqlite3
        with sqlite3.connect(self.master) as con:
            return con.execute(sql, args).fetchall()

    def test_dry_run_read_only_and_invalid_no_files(self):
        before = self.master.read_bytes()
        result = run(self.master, self.sidecar, self.manifest, self.reviews, self.extractions, 'b1')
        self.assertEqual(result['stage'], 'validated')
        self.assertEqual(self.master.read_bytes(), before)
        self.assertFalse(self.sidecar.exists())
        self.assertEqual(self.db('SELECT count(*) FROM relevance_reviews')[0][0], 0)
        invalid = copy.deepcopy(self.extractions)
        invalid[0]['benchmarks'] = [{'invented': 1}]
        with self.assertRaisesRegex(ValueError, 'schema'):
            run(self.master, self.sidecar, self.manifest, self.reviews, invalid, 'b1', True)
        self.assertFalse(self.sidecar.exists())
        self.assertEqual(self.db('SELECT count(*) FROM extraction_records')[0][0], 0)

    def test_apply_repeat_conflict_and_retention(self):
        first = run(self.master, self.sidecar, self.manifest, self.reviews, self.extractions, 'b1', True)
        self.assertEqual((first['master_new'], first['sidecar_imported']), (1, 1))
        previous = self.db('SELECT extraction_id,extracted_at,payload_json FROM extraction_records')[0]
        prior = self.db('SELECT paper_id FROM papers WHERE paper_id=?', (self.other,))
        second = run(self.master, self.sidecar, self.manifest, self.reviews, self.extractions, 'b1', True)
        self.assertEqual((second['master_new'], second['sidecar_imported']), (0, 0))
        self.assertEqual(previous, self.db('SELECT extraction_id,extracted_at,payload_json FROM extraction_records')[0])
        self.assertEqual(prior, self.db('SELECT paper_id FROM papers WHERE paper_id=?', (self.other,)))
        changed = copy.deepcopy(self.reviews)
        changed[0]['reason'] = 'Changed'
        with self.assertRaisesRegex(ValueError, 'Conflicting existing review'):
            run(self.master, self.sidecar, self.manifest, changed, self.extractions, 'b1', True)
        self.assertEqual(json.loads(self.db('SELECT payload_json FROM relevance_reviews')[0][0]), self.reviews[0])
        changed_extraction = copy.deepcopy(self.extractions)
        changed_extraction[0]['contributions'] = [{'key': self.version + '#c1', 'description': 'New'}]
        with self.assertRaisesRegex(ValueError, 'Conflicting existing extraction'):
            run(self.master, self.sidecar, self.manifest, self.reviews, changed_extraction, 'b1', True)
        other_manifest = [{'paper_id': self.other, 'paper_version': {'version_id': self.other_version,
            'version_label': None, 'source_url': 'https://example.org/prior'}, 'already_processed': False}]
        other_reviews = [{'paper_id': self.other, 'decision': 'exclude', 'topics': ['agent_memory'],
                          'reason': 'Out of scope', 'source_scope': 'abstract_only'}]
        with self.assertRaisesRegex(ValueError, 'different paper set'):
            run(self.master, self.sidecar, other_manifest, other_reviews, [], 'b1', True)

    def test_rollback_on_mid_batch_failure(self):
        con = connect(self.master)
        con.execute("CREATE TRIGGER reject_batch BEFORE INSERT ON extraction_records BEGIN SELECT RAISE(ABORT,'blocked'); END")
        con.commit(); con.close()
        with self.assertRaisesRegex(Exception, 'blocked'):
            run(self.master, self.sidecar, self.manifest, self.reviews, self.extractions, 'b1', True)
        self.assertEqual(self.db('SELECT count(*) FROM relevance_reviews')[0][0], 0)
        self.assertEqual(self.db('SELECT count(*) FROM extraction_records')[0][0], 0)
        self.assertFalse(self.sidecar.exists())

    def test_sidecar_failure_retry_and_reuse_gate(self):
        with patch('apply_corpus_batch.SemanticStore.ingest', side_effect=RuntimeError('sidecar unavailable')):
            with self.assertRaises(SidecarFailure) as error:
                run(self.master, self.sidecar, self.manifest, self.reviews, self.extractions, 'b1', True)
        self.assertEqual(error.exception.result['stage'], 'sidecar_failed')
        self.assertEqual(self.db('SELECT count(*) FROM extraction_records')[0][0], 1)
        result = run(self.master, self.sidecar, self.manifest, self.reviews, self.extractions, 'b1', True)
        self.assertEqual((result['master_new'], result['sidecar_imported']), (0, 1))
        reused = copy.deepcopy(self.manifest)
        reused[0]['already_processed'] = True
        self.assertEqual(run(self.master, self.sidecar, reused, self.reviews, [], 'b2')['extractions'], 0)
        con = connect(self.master)
        con.execute("UPDATE extraction_records SET status='draft_requires_review'")
        con.commit(); con.close()
        with self.assertRaisesRegex(ValueError, 'lacks reviewed'):
            run(self.master, self.sidecar, reused, self.reviews, [], 'b2')

    def test_cli_reports_failure_stage(self):
        paths = {}
        for name, data in [('input-manifest', self.manifest), ('reviews', self.reviews), ('extractions', self.extractions)]:
            paths[name] = self.root / (name + '.json')
            paths[name].write_text(json.dumps(data))
        report = self.root / 'report.json'
        args = ['--master', str(self.master), '--sidecar', str(self.sidecar),
                '--batch-id', 'b1', '--report', str(report)]
        for name, path in paths.items():
            args += ['--' + name, str(path)]
        self.assertEqual(main(args), 0)
        self.assertEqual(json.loads(report.read_text())['stage'], 'validated')
        self.assertFalse(self.sidecar.exists())
        bad_args = args.copy()
        bad_args[bad_args.index('--report') + 1] = str(self.master)
        self.assertEqual(main(bad_args), 1)

    def test_real_manifest_pin_metadata_and_excluded_empty_topics(self):
        manifest = copy.deepcopy(self.manifest)
        manifest[0]['paper_version']['version_status'] = 'source_confirmed'
        manifest.append({'paper_id': self.other, 'paper_version': {'version_id': self.other_version,
            'version_label': None, 'source_url': 'https://example.org/prior',
            'version_status': 'unverified_metadata_snapshot'}, 'already_processed': False})
        reviews = copy.deepcopy(self.reviews)
        reviews.append({'paper_id': self.other, 'decision': 'exclude', 'topics': [],
                        'reason': 'Not relevant', 'source_scope': 'abstract_only_metadata_snapshot'})
        result = run(self.master, self.sidecar, manifest, reviews, self.extractions, 'real-shape')
        self.assertEqual((result['papers'], result['reviews'], result['extractions']), (2, 2, 1))
        reviews[-1]['decision'] = 'pending'
        self.assertEqual(run(self.master, self.sidecar, manifest, reviews, self.extractions,
                             'pending-shape')['reviews'], 2)
        with self.assertRaisesRegex(ValueError, 'must differ'):
            run(self.master, self.master, manifest, reviews, self.extractions, 'same', True)


if __name__ == '__main__':
    unittest.main()
