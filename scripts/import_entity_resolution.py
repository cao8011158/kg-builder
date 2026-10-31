"""Validate and import an immutable manual entity resolution manifest. Offline only."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.storage import connect
from kg_builder.entity_resolution import validate_resolution_manifest,import_resolution_manifest


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',required=True,type=Path)
    parser.add_argument('--db',required=True,type=Path)
    args=parser.parse_args(argv)
    raw=args.input.read_text(encoding='utf-8')
    manifest=json.loads(raw,parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f'Nonfinite JSON: {value}')))
    validate_resolution_manifest(manifest)  # Reject malformed input before opening SQLite.
    con=connect(args.db)
    try:
        batch_id=import_resolution_manifest(con,manifest,raw_json=raw)
    finally:
        con.close()
    print('Imported entity resolution batch',batch_id)

if __name__=='__main__':main()
