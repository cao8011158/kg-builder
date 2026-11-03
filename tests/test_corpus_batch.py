"""Retired SQLite corpus entry points refuse before any storage side effect."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import apply_corpus_batch


class UnreadableInput:
    def __fspath__(self):
        raise AssertionError('Retired workflow inspected a path')

    def __iter__(self):
        raise AssertionError('Retired workflow inspected input data')


class RetiredCorpusBatchTests(unittest.TestCase):
    def test_imported_run_refuses_dry_and_apply_without_opening_sqlite(self):
        with patch('sqlite3.connect',side_effect=AssertionError('SQLite opened')) as connect:
            for apply in (False,True):
                with self.subTest(apply=apply),self.assertRaisesRegex(RuntimeError,'prepare_pipeline.py.*KG_DATABASE_URL'):
                    apply_corpus_batch.run('master.db','sidecar.db',[],[],[],'batch',apply=apply)
            connect.assert_not_called()

    def test_refusal_precedes_path_input_and_batch_validation(self):
        blocked=UnreadableInput()
        with patch('sqlite3.connect',side_effect=AssertionError('SQLite opened')) as connect:
            with self.assertRaisesRegex(RuntimeError,'workflow retired'):
                apply_corpus_batch.run(blocked,blocked,blocked,blocked,blocked,None,apply=True)
            connect.assert_not_called()

    def test_imported_run_preserves_existing_files_and_creates_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);master=root/'master.db';sidecar=root/'sidecar.db'
            master.write_bytes(b'historical master bytes')
            sidecar.write_bytes(b'historical sidecar bytes')
            before={p.name:p.read_bytes() for p in root.iterdir()}
            for target in (sidecar,root/'missing'/'new-sidecar.db'):
                with self.assertRaisesRegex(RuntimeError,'workflow retired'):
                    apply_corpus_batch.run(master,target,[],[],[],'batch',apply=True)
                self.assertEqual(before,{p.name:p.read_bytes() for p in root.iterdir()})

    def test_cli_refuses_before_reading_inputs_or_creating_outputs(self):
        with patch('sqlite3.connect',side_effect=AssertionError('SQLite opened')) as connect, \
             patch.object(Path,'read_text',side_effect=AssertionError('Input read')), \
             patch.object(Path,'write_text',side_effect=AssertionError('Output written')):
            with self.assertRaisesRegex(SystemExit,'prepare_pipeline.py.*KG_DATABASE_URL'):
                apply_corpus_batch.main(['--master','absent','--sidecar','absent','--report','report.json','--apply'])
            connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
