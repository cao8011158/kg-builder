"""Validate and atomically import reviewed/draft extraction JSON. No LLM calls."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.extraction_validation import validate_extraction_batch
from kg_builder.storage import connect, ingest_extractions


def load_batch(path):
    records=json.loads(Path(path).read_text(encoding='utf-8'),
                       parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f'Nonfinite JSON: {value}')))
    return validate_extraction_batch(records)


def main(argv=None):
    raise SystemExit('Historical SQLite import retired. Submit pinned v0.3 extraction tasks via scripts/prepare_pipeline.py --run-id RUN_ID --run-dir ARTIFACT_DIR submit --input FILE with KG_DATABASE_URL.')


if __name__=='__main__':
    main()
