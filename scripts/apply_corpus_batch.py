"""Validate a supplied corpus batch; optionally commit reviewed decisions and extractions."""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from kg_builder.extraction_validation import RETRIEVAL_SCHEMA_VERSION, validate_extraction_batch
from kg_builder.semantic_store import SemanticStore
from kg_builder.storage import NOW, connect, ingest_extractions

TOPICS = {'agent_memory', 'context_management', 'skill_evolution'}


class SidecarFailure(RuntimeError):
    def __init__(self, result, cause):
        result.update(stage='sidecar_failed', error=f'{type(cause).__name__}: {cause}')
        self.result = result
        super().__init__(result['error'])


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'),
                      parse_constant=lambda x: (_ for _ in ()).throw(ValueError(f'Nonfinite JSON: {x}')))


def indexed(items, label):
    if not isinstance(items, list):
        raise ValueError(f'{label} must be an array')
    result = {}
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get('paper_id'), str) or not item['paper_id'].strip():
            raise ValueError(f'{label} item lacks paper_id')
        if item['paper_id'] in result:
            raise ValueError(f'Duplicate {label} paper_id: {item["paper_id"]}')
        result[item['paper_id']] = item
    return result


def prepare(master, manifest, reviews, extractions):
    sources = indexed(manifest, 'manifest')
    decisions = indexed(reviews, 'review')
    candidates = indexed(extractions, 'extraction')
    validate_extraction_batch(extractions)
    if sources.keys() != decisions.keys():
        raise ValueError('Review IDs must match manifest IDs exactly')
    selected = {}
    for pid, item in sources.items():
        pin = item.get('paper_version')
        if not isinstance(item.get('already_processed'), bool) or not isinstance(pin, dict) or not {'version_id', 'version_label', 'source_url'} <= pin.keys():
            raise ValueError(f'Incomplete manifest record: {pid}')
        vid, label, url = (pin[k] for k in ('version_id', 'version_label', 'source_url'))
        if not isinstance(vid, str) or not vid or (label is not None and not isinstance(label, str)) or (url is not None and not isinstance(url, str)):
            raise ValueError(f'Invalid version pin: {pid}')
        row = master.execute('SELECT paper_id,version_label,source_url FROM paper_versions WHERE version_id=?', (vid,)).fetchone()
        if row is None or tuple(row) != (pid, label, url):
            raise ValueError(f'Unknown or stale master version pin: {pid}')
        review = decisions[pid]
        topics = review.get('topics')
        if (review.get('decision') not in {'include', 'exclude', 'pending'} or not isinstance(topics, list)
                or (review['decision'] == 'include' and not topics)
                or any(not isinstance(topic, str) or topic not in TOPICS for topic in topics)
                or len(topics) != len(set(topics))):
            raise ValueError(f'Invalid decision or canonical topics: {pid}')
        if not isinstance(review.get('reason'), str) or not review['reason'].strip() or not isinstance(review.get('source_scope'), str) or not review['source_scope'].strip():
            raise ValueError(f'Missing review reason or source_scope: {pid}')
        if item['already_processed']:
            existing = master.execute('''SELECT payload_json,status FROM extraction_records
                WHERE paper_id=? AND version_id=?''', (pid, vid)).fetchall()
            if not any(status in {'reviewed', 'reviewed_for_retrieval_not_reproduced'}
                       and valid_reused(raw, pid, vid, label, url)
                       for raw, status in existing):
                raise ValueError(f'Reused paper lacks reviewed v0.3 pinned extraction: {pid}')
        expected = review['decision'] == 'include' and not item['already_processed']
        if (pid in candidates) != expected:
            raise ValueError(f'Extraction presence disagrees with review/reuse: {pid}')
        if expected:
            record = candidates[pid]
            if record['version_id'] != vid or record['paper_version'] != {'version_label': label, 'source_url': url}:
                raise ValueError(f'Extraction version pin mismatch: {pid}')
            if record['source_scope'] != review['source_scope']:
                raise ValueError(f'Extraction source_scope mismatch: {pid}')
            selected[pid] = record
    if candidates.keys() != selected.keys():
        raise ValueError('Extraction IDs must match included, unprocessed IDs exactly')
    return decisions, selected


def valid_reused(raw, pid, vid, label, url):
    try:
        data = json.loads(raw)
        validate_extraction_batch([data])
        return (data['schema_version'] == RETRIEVAL_SCHEMA_VERSION and data['paper_id'] == pid
                and data['version_id'] == vid and data['paper_version'] == {'version_label': label, 'source_url': url})
    except (ValueError, KeyError, TypeError):
        return False


def run(master_path, sidecar_path, manifest, reviews, extractions, batch_id, apply=False):
    if not batch_id or not batch_id.strip():
        raise ValueError('batch-id is required')
    master_path = Path(master_path)
    if master_path.resolve() == Path(sidecar_path).resolve():
        raise ValueError('Master and sidecar paths must differ')
    if not master_path.is_file():
        raise ValueError('Master database does not exist')
    db = sqlite3.connect(f'file:{master_path.resolve().as_posix()}?mode=ro', uri=True)
    try:
        decisions, selected = prepare(db, manifest, reviews, extractions)
        prior_ids = {row[0] for row in db.execute('SELECT paper_id FROM relevance_reviews WHERE review_id=?', (batch_id,))}
        if prior_ids and prior_ids != decisions.keys():
            raise ValueError('Batch ID already belongs to a different paper set')
        for pid, record in selected.items():
            row = db.execute('''SELECT payload_json FROM extraction_records WHERE paper_id=? AND version_id=?
                AND extractor_type=? AND extractor_version=?''',
                (pid, record['version_id'], record['extractor_type'], record['extractor_version'])).fetchone()
            if row and json.loads(row[0]) != record:
                raise ValueError(f'Conflicting existing extraction: {pid}')
        for pid, review in decisions.items():
            row = db.execute('SELECT payload_json FROM relevance_reviews WHERE review_id=? AND paper_id=?',
                             (batch_id, pid)).fetchone()
            if row and json.loads(row[0]) != review:
                raise ValueError(f'Conflicting existing review: {pid}')
    finally:
        db.close()
    result = {'batch_id': batch_id, 'stage': 'validated', 'papers': len(manifest),
              'extractions': len(selected), 'reviews': len(decisions), 'master_new': 0,
              'sidecar_imported': 0}
    if not apply:
        return result
    con = connect(master_path)
    try:
        con.execute('BEGIN IMMEDIATE')
        new_ids = []
        try:
            prior_ids = {row[0] for row in con.execute('SELECT paper_id FROM relevance_reviews WHERE review_id=?', (batch_id,))}
            if prior_ids and prior_ids != decisions.keys():
                raise ValueError('Batch ID already belongs to a different paper set')
            for pid, review in decisions.items():
                row = con.execute('SELECT payload_json FROM relevance_reviews WHERE review_id=? AND paper_id=?',
                                  (batch_id, pid)).fetchone()
                if row:
                    if json.loads(row[0]) != review:
                        raise ValueError(f'Conflicting existing review: {pid}')
                    continue
                con.execute('''INSERT INTO relevance_reviews VALUES(?,?,?,?,?,?,?,?,?)''',
                            (batch_id, pid, review['decision'], 'reviewed', review['source_scope'],
                             canonical(review['topics']), review['reason'], canonical(review), NOW()))
            for pid, record in selected.items():
                row = con.execute('''SELECT extraction_id,payload_json FROM extraction_records WHERE paper_id=?
                    AND version_id=? AND extractor_type=? AND extractor_version=?''',
                    (pid, record['version_id'], record['extractor_type'], record['extractor_version'])).fetchone()
                if row:
                    if json.loads(row[1]) != record:
                        raise ValueError(f'Conflicting existing extraction: {pid}')
                else:
                    new_ids.append(ingest_extractions(con, [record])[0])
            con.commit()
        except BaseException:
            con.rollback()
            raise
        result.update(stage='master_committed', master_new=len(new_ids))
        # Retry candidates also include records committed before a previous sidecar failure.
        ids = list(new_ids)
        for pid, record in selected.items():
            row = con.execute('''SELECT extraction_id FROM extraction_records WHERE paper_id=? AND version_id=?
                AND extractor_type=? AND extractor_version=?''',
                (pid, record['version_id'], record['extractor_type'], record['extractor_version'])).fetchone()
            if row[0] not in ids:
                ids.append(row[0])
    finally:
        con.close()
    if ids:
        try:
            sidecar = SemanticStore(sidecar_path)
            try:
                missing = [eid for eid in ids if sidecar.db.execute(
                    'SELECT 1 FROM sources WHERE extraction_id=?', (eid,)).fetchone() is None]
                if missing:
                    imported = sidecar.ingest(master_path, missing)
                    result['sidecar_imported'] = sum(r['status'] != 'unchanged' for r in imported)
            finally:
                sidecar.close()
        except Exception as exc:
            raise SidecarFailure(result, exc) from exc
    result['stage'] = 'sidecar_committed'
    return result


def main(argv=None):
    raise SystemExit('Historical SQLite corpus batch CLI retired. Use scripts/prepare_pipeline.py --run-id RUN_ID --run-dir ARTIFACT_DIR run with KG_DATABASE_URL.')


if __name__ == '__main__':
    raise SystemExit(main())
