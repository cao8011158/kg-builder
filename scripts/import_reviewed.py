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
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db',default='data/research_knowledge.sqlite3')
    p.add_argument('--input',required=True)
    args=p.parse_args(argv)
    records=load_batch(args.input)  # No database is opened for malformed JSON or schema.
    con=connect(args.db)
    try:
        ingest_extractions(con,records)
    finally:
        con.close()
    print('Imported',len(records),'reviewed or draft extraction records (statuses preserved)')


if __name__=='__main__':
    main()
