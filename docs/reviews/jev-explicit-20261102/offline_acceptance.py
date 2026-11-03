"""Independent synthetic acceptance; all HTTP is intercepted, no original samples."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'tests')]
from kg_builder.decision_api import JevClient, judge_pairs, review_items, review_group_members
from kg_builder.preparation import PreparationWorkflow
from kg_builder.snapshot_graph import SnapshotGraph
from kg_builder.graph_validation import validate_graph
from test_preparation import source, draft, Embedding
from test_snapshot_graph import fixture, FakeEmbedding

packets = []
settings = {'positive': .76, 'negative': .2}

class Response:
    def __init__(self, value): self.value = value
    def __enter__(self): return self
    def __exit__(self, *args): pass

def transport(request, timeout):
    payload = json.loads(request.data)
    assert len(request.data) <= 60000
    assert len(json.dumps(payload['state'], ensure_ascii=False)) <= 40000
    assert len(payload['questions']) <= 24
    assert 'PRIVATE_SENTINEL' not in json.dumps(payload)
    packets.append(payload)
    answers = {}
    for key, question in payload['questions'].items():
        labels = list(question['criteria'])
        choice = labels[0]
        probability = settings['positive']
        if 'not_belongs' in labels:
            probability = settings['negative']
            choice = 'not_belongs' if probability >= .5 else 'belongs'
            distribution = {'belongs': 1 - probability, 'not_belongs': probability, 'insufficient': 0}
        else:
            distribution = {label: (probability if label == choice else (1 - probability) / (len(labels) - 1)) for label in labels}
        answers[key] = {'type': 'choice', 'choice': choice, 'confidence': .01, 'probabilities': distribution}
    return Response({'model': 'jev-1.14.0', 'answers': answers})

def body_size(packet): return len(json.dumps(packet, ensure_ascii=False).encode())

with patch.dict(os.environ, {'TYPESAFE_API_KEY': 'synthetic-unused-key'}), \
     patch('kg_builder.decision_api.urlopen', transport), \
     patch('kg_builder.decision_api.json.load', lambda response: response.value):
    client = JevClient(max_retries=0, max_concurrency=2)
    items = [dict(paper_id='PRIVATE_SENTINEL', field='benchmarks', key=f'k{i}',
                  input_hash='PRIVATE_SENTINEL', text='The suite evaluates recall.',
                  item=dict(name='Recall suite', description='Recall evaluation', roles=['used'],
                            version_or_subset='S', source_url='PRIVATE_SENTINEL')) for i in range(8)]
    results = review_items(items, client)
    assert all(r['passed'] and r['confidence'] == .01 for r in results)
    cold = packets[-1]
    assert len(cold['state']['sources']) == 1
    assert len(cold['questions']) == 8
    assert all(i['item']['roles'] == ['used'] for i in cold['state']['items'])
    review_items(items, client)
    assert packets[-1] == cold
    for item in items: item['item']['source_url'] = 'PRIVATE_SENTINEL' * 10000
    review_items(items, client)
    assert packets[-1] == cold
    settings['positive'] = .69
    assert not any(r['passed'] for r in review_items(items, client))
    settings['positive'] = .76
    # Check legacy mode separately without private sentinels in its intentionally historical wire.
    legacy = [dict(items[0], paper_id='legacy', input_hash='legacy', item={'name': 'M'})]
    assert not review_items(legacy, client, decision_metric='confidence')[0]['passed']
    a = dict(mention_id='PRIVATE_SENTINEL', name='Suite A', description='Recall evaluation',
             source={'version_or_subset': 'S', 'paper_id': 'PRIVATE_SENTINEL'})
    pairs = [dict(pair_id=f'p{i}', kind='Benchmark', left=a,
                  right=dict(a, mention_id=f'm{i}', name=f'Suite {i}')) for i in range(20)]
    output = judge_pairs(pairs, client)
    pair_packet = packets[-1]
    assert len(pair_packet['state']['entities']) == 21 and len(pair_packet['questions']) == 20
    assert all(e['version_or_subset'] == 'S' for e in pair_packet['state']['entities'])
    assert [r['pair_id'] for r in output] == [p['pair_id'] for p in pairs]
    start = len(packets)
    large = [dict(pairs[i % 20], pair_id=f'large{i}', right=dict(pairs[i % 20]['right'],
                  mention_id=f'large{i}', description='语义证据' * 1400 + str(i))) for i in range(25)]
    output = judge_pairs(large, client)
    assert [r['pair_id'] for r in output] == [p['pair_id'] for p in large]
    split_count = len(packets) - start
    assert split_count > 1
    for packet in packets[start:]:
        for entry in packet['state']['items']:
            assert packet['state']['entities'][entry['left']]['name'] == 'Suite A'
            assert packet['state']['entities'][entry['right']]['description'].startswith('语义证据')
    member = dict(membership_id='membership', group_id='group', kind='Benchmark',
                  name='Recall suite', description='Recall evaluation', member=a)
    for negative, expected in ((.7, False), (.700001, True)):
        settings['negative'] = negative
        assert review_group_members([member], client)[0]['excluded'] == expected
    settings['negative'] = .2
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        workflow = PreparationWorkflow(root / 'prep', target=1, client=client, embedding=Embedding())
        workflow.add_source(source(90001), 'arxiv', 'agent memory', 'agent_memory')
        workflow._scope_batch()
        task = workflow.claim('extract', 1, 'offline')[0]
        workflow.submit([dict(task_id=task['task_id'], lease_token=task['lease_token'],
                              input_hash=task['input_hash'], result=draft(task))])
        workflow._review_batch()
        assert workflow.status()['review']['ready'] == 1
        db = workflow._db()
        with db: db.execute("UPDATE run SET collection_state='sealed' WHERE id=1")
        db.close()
        asyncio.run(workflow.run(collect=False))
        assert workflow.snapshot(root / 'prepared.json')['counts']['ready'] == 1
        assert PreparationWorkflow(root / 'prep').decision_metric == 'probability'
        snapshot = root / 'synthetic.json'
        fixture(snapshot, n=3)
        job = SnapshotGraph(snapshot, root / 'graph', client=client, model=FakeEmbedding(), problem_resolution=.3)
        status = job.run()
        for task in job.claim('offline', 10):
            job.submit([dict(task_id=task['task_id'], lease_token=task['lease_token'],
                             input_hash=task['input_hash'], result=dict(name='Agent recall', description='Recall over time'))])
        status = job.run()
        assert status['phase'] == 'complete' and status['problem_resolution'] == .3
        job.close()
        validation = validate_graph(snapshot, root / 'graph', model=FakeEmbedding())
        assert validation['structural']['passed'], validation
        restored = SnapshotGraph(snapshot, root / 'graph', client=client, model=FakeEmbedding())
        assert restored.problem_resolution == .3 and restored.decision_metric == 'probability'
        assert restored.run()['phase'] == 'complete'
        restored.close()

protected = json.loads((ROOT.parents[1] / 'private/jev-context-20261102/protected-hashes.json').read_text())
assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest for path, digest in protected.items())
print(json.dumps(dict(passed=True, real_api_requests=0, original_sample_replays=0,
    original_artifacts_unchanged=len(protected), captured_mock_http_requests=len(packets),
    shared_eight_item_body_bytes=body_size(cold), a20_body_bytes=body_size(pair_packet),
    a20_objects=21, a20_questions=20, unicode_split_requests=split_count,
    synthetic_preparation_ready=1, synthetic_graph_complete=True, validator_passed=True,
    saved_resolution=.3), indent=2))
