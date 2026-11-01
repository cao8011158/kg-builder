import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from kg_builder.decision_api import JevClient, classify_scope, review_extractions, judge_pairs, review_groups


class FakeResponse:
    def __init__(self, body): self.body = body
    def __enter__(self): return self
    def __exit__(self, *args): pass


class FakeClient:
    def __init__(self, choices=None, confidence=.9):
        self.choices=choices or {}; self.confidence=confidence; self.calls=[]
    def decide(self, state, questions):
        self.calls.append((state, questions))
        answers={}
        for qid, q in questions.items():
            key=qid.split('_',1)[1]
            labels=list(q['criteria'])
            choice=self.choices.get(key,labels[0])
            probs={label: (1.0 if label==choice else 0.0) for label in labels}
            answers[qid]={'type':'choice','choice':choice,'probabilities':probs,'confidence':self.confidence}
        return {'model':'jev-1.14.0','usage':{'input_tokens':12,'output_tokens':4},'answers':answers,'attempts':1,'request_duration_s':.01}


class DecisionTests(unittest.TestCase):
    def test_native_request_and_audit(self):
        client=JevClient()
        questions={'scope':{'type':'choice','instructions':'Is it relevant?','criteria':{'yes':'Relevant','no':'Absent'}}}
        import json
        def fake_open(request,timeout):
            self.assertEqual(request.full_url,'https://api.typesafe.ai/v1/systemone')
            self.assertEqual(json.loads(request.data)['model'],'jev-1.14.0')
            self.assertEqual(timeout,120)
            return FakeResponse({'model':'jev-1.14.0','answers':{'scope':{'type':'choice','choice':'yes','probabilities':{'yes':.9,'no':.1},'confidence':.8}},'usage':{'input_tokens':1}})
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',fake_open),patch('kg_builder.decision_api.json.load',lambda response: fake_open_body):
            fake_open_body={'model':'jev-1.14.0','answers':{'scope':{'type':'choice','choice':'yes','probabilities':{'yes':.9,'no':.1},'confidence':.8}},'usage':{'input_tokens':1}}
            result=client.decide({'text':'evidence'},questions)
        self.assertEqual(result['attempts'],1)
        self.assertEqual(result['usage']['input_tokens'],1)
        self.assertEqual(result['request_id'],client.last_request_audit['request_id'])
        self.assertEqual(client.last_request_audit['status'],'validated')
        self.assertGreaterEqual(result['request_duration_s'],0)

    def test_reject_malformed_answers(self):
        client=JevClient()
        questions={'x':{'type':'choice','instructions':'Decide','criteria':{'a':'A','b':'B'}}}
        for answer in ({'type':'choice','choice':'c','probabilities':{'a':1,'b':0},'confidence':1},
                       {'type':'choice','choice':'a','probabilities':{'a':1},'confidence':1},
                       {'type':'choice','choice':'a','probabilities':{'a':1,'b':0},'confidence':float('nan')}):
            with self.subTest(answer=answer),patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',return_value=FakeResponse(None)),patch('kg_builder.decision_api.json.load',return_value={'model':'jev-1.14.0','answers':{'x':answer}}):
                with self.assertRaises(ValueError): client.decide('state',questions)

    def test_retry_is_bounded_and_audited(self):
        client=JevClient(max_retries=1)
        questions={'x':{'type':'choice','instructions':'Decide','criteria':{'a':'A','b':'B'}}}
        answer={'model':'jev-1.14.0','answers':{'x':{'type':'choice','choice':'a',
                'probabilities':{'a':1,'b':0},'confidence':1}}}
        failure=HTTPError('https://api.typesafe.ai/v1/systemone',429,'busy',{'Retry-After':'0.01'},None)
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',side_effect=[failure,FakeResponse(None)]),patch('kg_builder.decision_api.json.load',return_value=answer),patch('kg_builder.decision_api.time.sleep') as sleep:
            result=client.decide('state',questions)
        self.assertEqual(result['attempts'],2)
        self.assertGreaterEqual(result['total_elapsed_s'],result['request_duration_s'])
        sleep.assert_called_once_with(.01)
        with self.assertRaisesRegex(ValueError,'bounded request size'):
            client.decide('x'*40001,questions)
        with self.assertRaisesRegex(ValueError,'bounded payload size'):
            client.decide('state',{'x':{'type':'choice','instructions':'x'*61000,'criteria':{'a':'A','b':'B'}}})
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',side_effect=HTTPError('https://api.typesafe.ai/v1/systemone',403,'forbidden',{},None)):
            with self.assertRaisesRegex(RuntimeError,'403'):
                client.decide('state',questions)
        self.assertEqual(client.last_request_audit['status'],'http_error')
        self.assertEqual(client.last_request_audit['attempts'],1)

    def test_scope_and_reviews(self):
        paper={'paper_id':'p1','title':'Memory','text':'Supplied source'}
        yes=FakeClient({'agent':'yes','memory':'yes','context':'no','skills':'no'})
        result=classify_scope([paper],yes)[0]
        self.assertEqual((result['decision'],result['topics']),('include',['agent_memory']))
        self.assertEqual(len(yes.calls[0][1]),4)
        self.assertEqual(classify_scope([paper],FakeClient(confidence=.4))[0]['decision'],'pending')
        extraction={'contributions':[{'description':'claim'}],'research_problems':[],'methods':[],'benchmarks':[]}
        item={'paper_id':'p1','text':'Supplied source','extraction':extraction}
        self.assertTrue(review_extractions([item],FakeClient())[0]['passed'])
        self.assertFalse(review_extractions([item],FakeClient({'contributions_0':'contradicted'}))[0]['passed'])
        self.assertFalse(review_extractions([{'paper_id':'p1','text':'source','extraction':{}}],FakeClient())[0]['passed'])
        pair={'pair_id':'x','kind':'Method','left':{'name':'A'},'right':{'name':'B'}}
        self.assertEqual(judge_pairs([pair],FakeClient({'identity':'different'}))[0]['decision'],'different')
        group={'group_id':'g','kind':'Method','name':'A','description':'B','members':[]}
        self.assertFalse(review_groups([group],FakeClient({'coherence':'incoherent'}))[0]['passed'])

    def test_shared_audit_request_id_and_adaptive_payload_batch(self):
        client=FakeClient({'identity':'same'})
        pairs=[{'pair_id':str(i),'kind':'Method','left':{'name':'A'},'right':{'name':'B'}} for i in range(3)]
        results=judge_pairs(pairs,client)
        self.assertEqual(len(client.calls),1)
        self.assertEqual(results[1]['raw']['requests'][0]['request_id'],
                         results[2]['raw']['requests'][0]['request_id'])
        large=[{'pair_id':str(i),'kind':'Method','left':{'name':'A','description':'语'*12000},
                'right':{'name':'B'}} for i in range(2)]
        client=FakeClient({'identity':'same'})
        results=judge_pairs(large,client)
        self.assertEqual(len(client.calls),2)
        self.assertNotEqual(results[0]['raw']['requests'][0]['request_id'],
                            results[1]['raw']['requests'][0]['request_id'])

    def test_batches_bounded(self):
        client=FakeClient({'identity':'same'})
        pairs=[{'pair_id':str(i),'kind':'Method','left':{'name':'A'},'right':{'name':'B'}} for i in range(50)]
        self.assertEqual(len(judge_pairs(pairs,client)),50)
        self.assertEqual([len(q) for _,q in client.calls],[24,24,2])

    def test_long_field_review_splits_questions(self):
        client=FakeClient()
        item={'paper_id':'p', 'text':'source', 'extraction':{
            'contributions':[{'description':'source'} for _ in range(26)],
            'research_problems':[], 'methods':[], 'benchmarks':[]}}
        result=review_extractions([item],client)[0]
        self.assertTrue(result['passed'])
        self.assertEqual([len(q) for _,q in client.calls],[24,6])
        self.assertEqual(len(result['raw']['requests']),2)

    def test_every_question_targets_its_own_item_and_field(self):
        client=FakeClient({'agent':'yes','memory':'yes','context':'no','skills':'no'})
        papers=[{'paper_id':f'p{i}', 'title':f'Title {i}', 'text':f'Source {i}'} for i in range(2)]
        classify_scope(papers,client)
        self.assertEqual(len(client.calls),1)
        for qid, question in client.calls[0][1].items():
            index=int(qid[1]); instructions=question['instructions']
            self.assertIn(f'state.items[{index}].text',instructions)
            self.assertIn(f'ID "p{index}"',instructions)
            self.assertNotIn('state.items[INDEX]',instructions)
            self.assertNotIn(f'state.items[{1-index}].text',instructions)

        review=FakeClient()
        items=[{'paper_id':f'p{i}','text':f'Source {i}', 'extraction':{
            'contributions':[{'description':'claim'}], 'research_problems':[],
            'methods':[{'name':'method'}], 'benchmarks':[]}} for i in range(2)]
        review_extractions(items,review)
        for qid, question in review.calls[0][1].items():
            index=int(qid[1]); instructions=question['instructions']
            self.assertIn(f'state.items[{index}].text',instructions)
            self.assertIn(f'ID "p{index}"',instructions)
            suffix=qid.split('_',1)[1]
            field=suffix.removeprefix('adequacy_').rsplit('_',1)[0] if not suffix.startswith('adequacy_') else suffix.removeprefix('adequacy_')
            self.assertIn(f'state.items[{index}].extraction.{field}',instructions)
            if not suffix.startswith('adequacy_'):
                self.assertIn(f'state.items[{index}].extraction.{field}[0]',instructions)

    def test_pair_and_group_semantics(self):
        pairs=[{'pair_id':'a','kind':'ResearchProblem','left':{},'right':{}},
               {'pair_id':'b','kind':'Method','left':{},'right':{}}]
        client=FakeClient()
        judge_pairs(pairs,client)
        self.assertIn('concrete research objective or failure mode',client.calls[0][1]['i0_identity']['instructions'])
        self.assertIn('actual method or benchmark identity',client.calls[0][1]['i1_identity']['instructions'])
        self.assertIn('state.items[1].left',client.calls[0][1]['i1_identity']['instructions'])
        groups=[{'group_id':'a','kind':'ResearchProblem','members':[], 'name':'A','description':'B'},
                {'group_id':'b','kind':'Benchmark','members':[], 'name':'C','description':'D'}]
        client=FakeClient()
        review_groups(groups,client)
        self.assertIn('concrete research objective or failure mode',client.calls[0][1]['i0_coherence']['instructions'])
        self.assertIn('state.items[1].members',client.calls[0][1]['i1_coherence']['instructions'])

if __name__=='__main__': unittest.main()
