import os
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from kg_builder.decision_api import JevClient, classify_scope, review_extractions, review_items, review_completeness, judge_pairs, review_groups, review_group_members


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
    def test_member_review_strict_boundary_and_audit(self):
        item={'membership_id':'g:m','group_id':'g','kind':'ResearchArea',
              'name':'Agent memory','description':'Research on long horizon agent memory',
              'member':{'mention_id':'m','name':'Memory decay','description':'Agents forget',
                        'source':{'paper_id':'p'}}}
        for choice,confidence,excluded in [('not_belongs',.7,False),('not_belongs',.700001,True),
                                          ('belongs',1,False),('insufficient',1,False)]:
            with self.subTest(choice=choice,confidence=confidence):
                client=FakeClient({'membership':choice},confidence)
                result=review_group_members([item],client,decision_metric='confidence')[0]
                self.assertEqual(result['excluded'],excluded)
                self.assertEqual((result['choice'],result['confidence']),(choice,confidence))
                self.assertEqual(result['membership_id'],'g:m')
                self.assertIn('distinct research problems',client.calls[0][1]['i0_membership']['instructions'])
                self.assertIn('requests',result['raw'])
        method=dict(item,kind='Method')
        client=FakeClient({'membership':'not_belongs'})
        self.assertTrue(review_group_members([method],client)[0]['excluded'])
        self.assertIn('actual method or benchmark identity',client.calls[0][1]['i0_membership']['instructions'])
        with self.assertRaises(ValueError):review_group_members([item,item],client)
        class FailingClient:
            def decide(self,*args):raise RuntimeError('offline failure')
        with self.assertRaisesRegex(RuntimeError,'offline failure'):
            review_group_members([item],FailingClient())

    def test_shared_http_budget_and_per_thread_audit(self):
        client=JevClient(max_concurrency=2, max_retries=0)
        barrier=threading.Barrier(2, timeout=3)
        release=threading.Event()
        entered=[]
        lock=threading.Lock()
        questions={'x':{'type':'choice','instructions':'Decide','criteria':{'a':'A','b':'B'}}}
        def fake_open(request, timeout):
            marker=json.loads(request.data)['state']['marker']
            with lock:
                entered.append(marker)
                ordinal=len(entered)
            if ordinal<=2: barrier.wait()
            self.assertTrue(release.wait(3))
            if marker == 'bad':
                raise HTTPError(request.full_url, 403, 'forbidden', {}, None)
            return FakeResponse({'model':'jev-1.14.0','answers':{'x':{'type':'choice','choice':'a',
                'probabilities':{'a':1,'b':0},'confidence':1}}})
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',fake_open),patch('kg_builder.decision_api.json.load',lambda response:response.body):
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures=[executor.submit(client.decide, {'marker':marker}, questions)
                         for marker in ('good','bad','later')]
                deadline=time.monotonic()+3
                while len(entered)<2 and time.monotonic()<deadline: time.sleep(.001)
                self.assertEqual(len(entered),2)
                self.assertEqual(client.concurrency_stats['active'],2)
                release.set()
                outcomes=[]
                for future in futures:
                    try: outcomes.append(future.result(timeout=3))
                    except RuntimeError as exc: outcomes.append(exc)
        self.assertEqual(client.concurrency_stats['peak_active'],2)
        self.assertEqual(client.concurrency_stats['started'],3)
        self.assertEqual(client.concurrency_stats['finished'],3)
        self.assertGreaterEqual(client.concurrency_stats['peak_queued'],1)
        self.assertEqual(outcomes[0]['request_id'] != outcomes[2]['request_id'],True)
        self.assertEqual(outcomes[1].request_audit['status'],'http_error')
        self.assertNotEqual(outcomes[1].request_audit['request_id'],outcomes[0]['request_id'])

    def test_queue_time_excluded_from_http_duration(self):
        client=JevClient(max_concurrency=1, max_retries=0)
        entered=threading.Event(); release=threading.Event()
        questions={'x':{'type':'choice','instructions':'Decide','criteria':{'a':'A','b':'B'}}}
        def fake_open(request, timeout):
            if json.loads(request.data)['state']=='first':
                entered.set(); self.assertTrue(release.wait(3))
            return FakeResponse({'model':'jev-1.14.0','answers':{'x':{'type':'choice','choice':'a',
                'probabilities':{'a':1,'b':0},'confidence':1}}})
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',fake_open),patch('kg_builder.decision_api.json.load',lambda response:response.body):
            with ThreadPoolExecutor(max_workers=2) as executor:
                first=executor.submit(client.decide,'first',questions)
                self.assertTrue(entered.wait(3))
                second=executor.submit(client.decide,'second',questions)
                deadline=time.monotonic()+3
                while client.concurrency_stats['queued']<1 and time.monotonic()<deadline: time.sleep(.001)
                self.assertEqual(client.concurrency_stats['queued'],1)
                time.sleep(.04)
                release.set()
                first.result(timeout=3); result=second.result(timeout=3)
        self.assertGreater(result['queue_duration_s'],.03)
        self.assertLess(result['request_duration_s'],result['queue_duration_s'])
        self.assertEqual(len(result['http_intervals']),1)

    def test_dispatch_out_of_order_keeps_source_binding(self):
        barrier=threading.Barrier(2, timeout=3)
        class ConcurrentFake(FakeClient):
            max_concurrency=2
            def decide(self, state, questions):
                barrier.wait()
                if len(questions)==24: time.sleep(.03)
                return super().decide(state,questions)
        client=ConcurrentFake({'identity':'same'})
        pairs=[{'pair_id':str(i),'kind':'Method','left':{},'right':{}} for i in range(25)]
        results=judge_pairs(pairs,client)
        self.assertEqual([r['pair_id'] for r in results],[str(i) for i in range(25)])
        self.assertNotEqual(results[0]['raw']['requests'][0]['request_id'],results[24]['raw']['requests'][0]['request_id'])

    def test_concurrent_failure_retains_completed_request_audits(self):
        barrier=threading.Barrier(2, timeout=3)
        class FailingFake(FakeClient):
            max_concurrency=2
            def decide(self,state,questions):
                barrier.wait()
                if len(questions)==1:
                    error=RuntimeError('request failed')
                    error.request_audit={'request_id':'failed','status':'http_error'}
                    raise error
                return super().decide(state,questions)
        pairs=[{'pair_id':str(i),'kind':'Method','left':{},'right':{}} for i in range(25)]
        with self.assertRaisesRegex(RuntimeError,'request failed') as raised:
            judge_pairs(pairs,FailingFake())
        self.assertEqual(len(raised.exception.request_audits),2)
        self.assertEqual(raised.exception.decision_item_ids,['24'])

    def test_item_results_and_diagnostic_completeness(self):
        extraction={'contributions':[{'key':'stable','description':'claim'}], 'research_problems':[],
                    'methods':[], 'benchmarks':[]}
        result=review_extractions([{'paper_id':'p','text':'source','extraction':extraction}],
            FakeClient({'adequacy_contributions':'omission'}))[0]
        self.assertFalse(result['passed'])  # Retained paper-wide legacy field.
        self.assertEqual([(r['field'],r['key'],r['passed'],r['probability']) for r in result['item_results']],
                         [('contributions','stable',True,1.0)])
        self.assertEqual(result['completeness']['contributions']['choice'],'omission')
        item={'paper_id':'p','field':'methods','key':'m1','text':'source','item':{'name':'Method'},'input_hash':'abc'}
        reviewed=review_items([item],FakeClient())[0]
        self.assertEqual((reviewed['key'],reviewed['input_hash'],reviewed['passed']),('m1','abc',True))
        diagnostic=review_completeness([{'paper_id':'p','text':'source','extraction':extraction}],
            FakeClient({'adequacy_contributions':'omission'}))[0]
        self.assertEqual(diagnostic['completeness']['contributions']['choice'],'omission')
        self.assertEqual(len(diagnostic['completeness']),4)

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
                with self.assertRaises(ValueError) as raised: client.decide('state',questions)
                self.assertEqual(raised.exception.request_audit['status'],'invalid_response')
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',return_value=FakeResponse(None)),patch('kg_builder.decision_api.json.load',side_effect=ValueError('private response')):
            with self.assertRaisesRegex(ValueError,'Malformed System One response body') as raised:
                client.decide('state',questions)
            self.assertEqual(raised.exception.request_audit['status'],'invalid_response')

    def test_retry_is_bounded_and_audited(self):
        client=JevClient(max_retries=1)
        questions={'x':{'type':'choice','instructions':'Decide','criteria':{'a':'A','b':'B'}}}
        answer={'model':'jev-1.14.0','answers':{'x':{'type':'choice','choice':'a',
                'probabilities':{'a':1,'b':0},'confidence':1}}}
        failure=HTTPError('https://api.typesafe.ai/v1/systemone',429,'busy',{'Retry-After':'0.01'},None)
        with patch.dict(os.environ,{'TYPESAFE_API_KEY':'private'}),patch('kg_builder.decision_api.urlopen',side_effect=[failure,FakeResponse(None)]),patch('kg_builder.decision_api.json.load',return_value=answer),patch('kg_builder.decision_api.time.sleep') as sleep:
            result=client.decide('state',questions)
        self.assertEqual(result['attempts'],2)
        self.assertEqual(len(result['http_intervals']),2)
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
        self.assertEqual(classify_scope([paper],FakeClient(confidence=.4))[0]['decision'],'include')
        self.assertEqual(classify_scope([paper],FakeClient(confidence=.4),decision_metric='confidence')[0]['decision'],'pending')
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
        large=[{'pair_id':str(i),'kind':'Method','left':{'name':'A','description':'语'*8000},
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
            self.assertIn(f'state.sources[{index}].text',instructions)
            self.assertNotIn(f'ID "p{index}"',instructions)
            self.assertNotIn('state.items[INDEX]',instructions)
            self.assertNotIn(f'state.items[{1-index}].source',instructions)

        review=FakeClient()
        items=[{'paper_id':f'p{i}','text':f'Source {i}', 'extraction':{
            'contributions':[{'description':'claim'}], 'research_problems':[],
            'methods':[{'name':'method'}], 'benchmarks':[]}} for i in range(2)]
        review_extractions(items,review)
        for qid, question in review.calls[0][1].items():
            index=int(qid[1]); instructions=question['instructions']
            self.assertIn(f'state.sources[{index}].text',instructions)
            self.assertNotIn(f'ID "p{index}"',instructions)
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
        self.assertIn('Compare {} with {}',client.calls[0][1]['i1_identity']['instructions'])
        groups=[{'group_id':'a','kind':'ResearchProblem','members':[], 'name':'A','description':'B'},
                {'group_id':'b','kind':'Benchmark','members':[], 'name':'C','description':'D'}]
        client=FakeClient()
        review_groups(groups,client)
        self.assertIn('concrete research objective or failure mode',client.calls[0][1]['i0_coherence']['instructions'])
        self.assertIn('Compare every member in []',client.calls[0][1]['i1_coherence']['instructions'])


class ProbabilityProjectionTests(unittest.TestCase):
    class PacketClient(FakeClient):
        def __init__(self, probabilities=None, choices=None, confidence=.1):
            super().__init__(choices, confidence)
            self.distributions = probabilities or {}
        def decide(self, state, questions):
            response = super().decide(state, questions)
            for qid, answer in response['answers'].items():
                suffix = qid.split('_', 1)[1]
                if suffix in self.distributions:
                    answer['probabilities'] = self.distributions[suffix]
            return response

    def test_whole_group_member_questions_share_complete_group_without_anchor(self):
        members=[{'mention_id':f'm{n}','name':f'Variant {n}',
                  'description':f'Original member {n}','source':{'version_or_subset':'Lite' if n==0 else 'Verified'}}
                 for n in range(12)]
        items=[{'membership_id':f'g:m{n}','group_id':'g','kind':'Benchmark',
                'name':'Wrong first-member anchor','description':'Wrong anchor definition','member':member}
               for n,member in enumerate(members)]
        client=self.PacketClient()
        result=review_group_members(items,client,review_mode='whole-group-v1',group_members={'g':members})
        self.assertEqual(len(client.calls),1)
        state,questions=client.calls[0]
        self.assertEqual((len(state['candidate_groups']),len(state['candidate_groups'][0]['members']),len(questions)),(1,12,12))
        self.assertNotIn('Wrong first-member anchor',json.dumps((state,questions)))
        self.assertNotIn('m0',json.dumps((state,questions)))
        self.assertEqual(state['candidate_groups'][0]['members'][0]['version_or_subset'],'Lite')
        for n in range(12):
            prompt=questions[f'i{n}_membership']['instructions']
            self.assertIn('state.candidate_groups[0].members',prompt)
            self.assertIn(f'"name": "Variant {n}"',prompt)
            self.assertNotIn('state.entities[',prompt)
        self.assertTrue(all(row['review_mode']=='whole-group-v1' for row in result))

    def test_whole_group_split_keeps_every_member_and_isolates_groups(self):
        members=[{'mention_id':f'm{n}','name':f'Member {n}','description':f'Claim {n}',
                  'source':{'version_or_subset':'v1'}} for n in range(25)]
        other={'mention_id':'other','name':'Other','description':'Other claim','source':{}}
        items=[{'membership_id':f'g:{m["mention_id"]}','group_id':'g','kind':'Method',
                'name':'ignored','description':'ignored','member':m} for m in members]
        items.append({'membership_id':'h:other','group_id':'h','kind':'Benchmark',
                      'name':'ignored','description':'ignored','member':other})
        client=self.PacketClient()
        review_group_members(items,client,review_mode='whole-group-v1',group_members={'g':members,'h':[other]})
        self.assertEqual([len(q) for _,q in client.calls],[24,2])
        self.assertEqual(len(client.calls[0][0]['candidate_groups'][0]['members']),25)
        self.assertEqual(len(client.calls[1][0]['candidate_groups'][0]['members']),25)
        self.assertEqual(len(client.calls[1][0]['candidate_groups'][1]['members']),1)
        self.assertIn('state.candidate_groups[1].members',client.calls[1][1]['i1_membership']['instructions'])
        self.assertNotIn('Other',json.dumps(client.calls[0]))

    def test_whole_group_budget_fails_without_sending_subset(self):
        member={'mention_id':'m','name':'A','description':'X'*41000,'source':{}}
        item={'membership_id':'g:m','group_id':'g','kind':'Method',
              'name':'ignored','description':'ignored','member':member}
        client=self.PacketClient()
        with self.assertRaisesRegex(ValueError,'members=1 state_chars='):
            review_group_members([item],client,review_mode='whole-group-v1',group_members={'g':[member]})
        self.assertEqual(client.calls,[])

    def test_whole_group_preserves_path_shaped_target_evidence(self):
        literal=('The source literally says state.candidate_groups[GROUP].members, '
                 'state.items[INDEX], and TARGET_MEMBER.')
        member={'mention_id':'m','name':'A','description':literal,'source':{}}
        item={'membership_id':'g:m','group_id':'g','kind':'Method',
              'name':'ignored','description':'ignored','member':member}
        client=self.PacketClient()
        review_group_members([item],client,review_mode='whole-group-v1',group_members={'g':[member]})
        state,questions=client.calls[0]
        prompt=questions['i0_membership']['instructions']
        self.assertIn('state.candidate_groups[0].members? Target member:',prompt)
        self.assertIn(literal,prompt)
        self.assertEqual(state['candidate_groups'][0]['members'][0]['description'],literal)

    def test_explicit_questions_bind_semantic_objects_with_shared_state(self):
        a={'mention_id':'private-a','name':'Alpha','description':'first','source':{'version_or_subset':'v1'}}
        pairs=[{'pair_id':str(n),'kind':'Benchmark','left':a,
                'right':{'mention_id':str(n),'name':f'Beta {n}','description':f'variant {n}'}}
               for n in range(20)]
        client=self.PacketClient();judge_pairs(pairs,client)
        state,questions=client.calls[0]
        self.assertEqual((len(state['items']),len(state['entities']),len(questions)),(20,21,20))
        for n in range(20):
            prompt=questions[f'i{n}_identity']['instructions']
            self.assertIn('"version_or_subset": "v1"',prompt)
            self.assertIn(f'"name": "Beta {n}"',prompt)
            self.assertIn('using "Benchmark"',prompt)
            self.assertTrue(prompt.startswith('Compare '))
            self.assertNotIn('state.entities[',prompt)
            self.assertNotIn('private-',prompt)
        legacy=self.PacketClient();judge_pairs(pairs[:1],legacy,context_format='registry-v1')
        self.assertIn('state.entities[state.items[0].left]',legacy.calls[0][1]['i0_identity']['instructions'])

    def test_explicit_all_categories_and_boundaries(self):
        source={'paper_id':'private-paper','title':'Title','text':'evidence'}
        extraction={'contributions':[],'research_problems':[],'methods':[],'benchmarks':[]}
        group={'group_id':'private-group','kind':'Method','name':'Group Name','description':'Group definition',
               'members':[{'mention_id':'private-m','name':'Member Name','description':'Member claim'}]}
        member={'membership_id':'private-binding','group_id':'private-group','kind':'Method',
                'name':'Group Name','description':'Group definition','member':group['members'][0] | {'source':{'text':'member evidence'}}}
        calls=[]
        for action,records in ((classify_scope,[source]),
                               (review_items,[source | {'field':'methods','key':'private-key','input_hash':'private-hash','item':{'name':'Business item'}}]),
                               (review_extractions,[source | {'extraction':extraction}]),
                               (review_completeness,[source | {'extraction':extraction}]),
                               (review_groups,[group]),(review_group_members,[member])):
            client=self.PacketClient();action(records,client);calls.extend(client.calls)
        for state,questions in calls:
            packet=json.dumps((state,questions),ensure_ascii=False)
            self.assertNotIn('private-',packet)
            for question in questions.values():
                instruction=question['instructions']
                self.assertNotIn('state.sources[state.',instruction)
                self.assertNotIn('state.entities[state.',instruction)
                self.assertNotIn('state.groups[state.',instruction)
        self.assertIn('state.sources[0].text',calls[0][1]['i0_agent']['instructions'])
        self.assertIn('state.items[0].item',calls[1][1]['i0_support']['instructions'])
        self.assertIn('"name": "Member Name"',calls[-1][1]['i0_membership']['instructions'])
        self.assertIn('state.groups[0].description',calls[-1][1]['i0_membership']['instructions'])
        self.assertIn('state.sources[0].text',calls[-1][1]['i0_membership']['instructions'])
        second=member | {'membership_id':'private-binding-2',
                         'member':{'mention_id':'private-m2','name':'Another member','description':'Another claim','source':{'text':'member evidence'}}}
        shared=self.PacketClient();review_group_members([member,second],shared)
        state,questions=shared.calls[0]
        self.assertEqual(len(state['groups']),1)
        self.assertEqual(json.dumps({'state':state,'questions':questions}).count('Group definition'),1)
        self.assertIn('state.groups[0].description',questions['i1_membership']['instructions'])
        with self.assertRaisesRegex(ValueError,'context_format'):
            judge_pairs([],self.PacketClient(),context_format='unknown')

    def test_explicit_projection_does_not_rewrite_embedded_semantic_text(self):
        item={'pair_id':'p','kind':'Method',
              'left':{'name':'A','description':'literal state.items[0].kind in quoted evidence'},
              'right':{'name':'B'}}
        client=self.PacketClient();judge_pairs([item],client)
        self.assertIn('literal state.items[0].kind in quoted evidence',
                      client.calls[0][1]['i0_identity']['instructions'])

    def test_named_probability_gates_and_legacy(self):
        item={'paper_id':'p','field':'methods','key':'secret-key','input_hash':'secret-hash',
              'text':'Evidence', 'item':{'name':'Method','description':'Claim','roles':['used']}}
        high=self.PacketClient({'support':{'supported':.91,'contradicted':.04,'insufficient':.05}}, confidence=.02)
        approved=review_items([item],high)[0]
        self.assertTrue(approved['passed'])
        self.assertEqual((approved['probability'],approved['confidence'],approved['decision_metric']),(.91,.02,'probability'))
        low=self.PacketClient({'support':{'supported':.69,'contradicted':.2,'insufficient':.11}},confidence=.99)
        self.assertFalse(review_items([item],low)[0]['passed'])
        self.assertTrue(review_items([item],low,decision_metric='confidence')[0]['passed'])
        self.assertNotIn('probability',review_items([item],low,decision_metric='confidence')[0])
        pair={'pair_id':'private-pair','kind':'Benchmark','left':{'name':'A'},'right':{'name':'B'}}
        different=self.PacketClient({'identity':{'same':.15,'different':.8,'insufficient':.05}},
                                    {'identity':'different'},confidence=.01)
        self.assertEqual(judge_pairs([pair],different)[0]['decision'],'different')
        self.assertEqual(judge_pairs([pair],different,decision_metric='confidence')[0]['decision'],'pending')
        member={'membership_id':'private-membership','group_id':'private-group','kind':'Method',
                'name':'M','description':'Group',
                'member':{'mention_id':'private-mention','name':'N','description':'Claim','source':{'paper_id':'private-paper'}}}
        for negative, expected in ((.7,False),(.700001,True)):
            client=self.PacketClient({'membership':{'belongs':.1,'not_belongs':negative,'insufficient':.2}},
                                     {'membership':'not_belongs'},confidence=.01)
            self.assertEqual(review_group_members([member],client)[0]['excluded'],expected)
        other=self.PacketClient({'membership':{'belongs':.1,'not_belongs':.85,'insufficient':.05}},
                                {'membership':'belongs'},confidence=.99)
        self.assertFalse(review_group_members([member],other)[0]['excluded'])

    def test_entity_same_probability_gate_is_specific_to_kind_and_choice(self):
        pair={'pair_id':'p','kind':'Method','left':{'name':'A'},'right':{'name':'B'}}
        for kind,choice,probability,expected in (
            ('Method','same',.60,'same'),('Benchmark','same',.60,'same'),
            ('Method','same',.59,'pending'),('Method','different',.60,'pending'),
            ('Method','different',.70,'different'),('ResearchProblem','same',.60,'pending')):
            with self.subTest(kind=kind,choice=choice,probability=probability):
                values={'same':0.,'different':0.,'insufficient':0.}
                values[choice]=probability
                client=self.PacketClient({'identity':values},{'identity':choice},confidence=.99)
                self.assertEqual(judge_pairs([{**pair,'kind':kind}],client,same_min_probability=.6)[0]['decision'],expected)
        low=self.PacketClient({'identity':{'same':.59,'different':.4,'insufficient':.01}},
                              {'identity':'same'},confidence=.99)
        self.assertEqual(judge_pairs([pair],low,same_min_probability=.6)[0]['decision'],'pending')
        self.assertEqual(judge_pairs([pair],low,decision_metric='confidence',same_min_probability=.6)[0]['decision'],'same')
        self.assertEqual(judge_pairs([pair],self.PacketClient({'identity':{'same':.6,'different':.3,'insufficient':.1}},
                                              {'identity':'same'}))[0]['decision'],'pending')
        for invalid in (True,-.1,1.1,float('nan'),float('inf')):
            with self.subTest(invalid=invalid),self.assertRaises(ValueError):
                judge_pairs([pair],low,same_min_probability=invalid)

    def test_missing_invalid_named_probability_fails_closed(self):
        item={'paper_id':'p','field':'methods','key':'k','input_hash':'h','text':'source','item':{'name':'M'}}
        for distribution in ({'contradicted':.1}, {'supported':float('nan')}, {'supported':'0.9'}):
            with self.subTest(distribution=distribution):
                with self.assertRaisesRegex(ValueError,'named Choice probability'):
                    review_items([item],self.PacketClient({'support':distribution},confidence=1))
        with self.assertRaisesRegex(ValueError,'decision_metric'):
            review_items([item],FakeClient(),decision_metric='unknown')

    def test_semantic_packets_shared_and_local_bindings(self):
        sentinel='UNSENT_ADMIN_SENTINEL'
        items=[{'paper_id':'private-paper','field':'methods','key':f'private-{i}',
                'input_hash':sentinel,'text':'same source text',
                'item':{'name':f'Item {i}','description':'Claim','roles':['used'],
                        'section':sentinel,'source_url':sentinel,'extractor':sentinel}}
               for i in range(8)]
        client=self.PacketClient()
        output=review_items(items,client)
        self.assertEqual([r['key'] for r in output],[x['key'] for x in items])
        state,qs=client.calls[0]
        self.assertEqual((len(state['sources']),len(state['items']),len(qs)),(1,8,8))
        packet=json.dumps({'state':state,'questions':qs},ensure_ascii=False)
        self.assertEqual(packet.count('same source text'),1)
        for private in (sentinel,'private-paper','private-0','source_url','extractor'):
            self.assertNotIn(private,packet)
        items[7]['text']='different source text'
        client=self.PacketClient();review_items(items,client)
        self.assertEqual(len(client.calls[0][0]['sources']),2)
        self.assertEqual(client.calls[0][0]['items'][-1]['source'],1)
        items[7]['text']='same source text'; items[7]['paper_id']='other paper'
        client=self.PacketClient();review_items(items,client)
        self.assertEqual(len(client.calls[0][0]['sources']),2)
        a={'mention_id':'private-A','name':'A','description':'Shared object','paper_id':sentinel}
        pairs=[{'pair_id':f'private-pair-{i}','kind':'Method','left':a,
                'right':{'mention_id':f'private-B-{i}','name':'B','description':str(i),
                         'source_hash':sentinel}} for i in range(20)]
        client=self.PacketClient();out=judge_pairs(pairs,client)
        state,questions=client.calls[0]
        self.assertEqual((len(state['entities']),len(state['items']),len(questions)),(21,20,20))
        self.assertEqual(len({entry['left'] for entry in state['items']}),1)
        self.assertEqual([r['pair_id'] for r in out],[p['pair_id'] for p in pairs])
        self.assertNotIn(sentinel,json.dumps({'state':state,'questions':questions}))
        self.assertNotIn('private-A',json.dumps({'state':state,'questions':questions}))

    def test_scope_completeness_groups_and_legacy_wire(self):
        scope={'paper_id':'PRIVATE_SCOPE','title':'A source title','text':'Source evidence',
               'source_hash':'ADMIN_SENTINEL'}
        probabilities={'agent':{'yes':.91,'no':.04,'insufficient':.05},
                       'memory':{'yes':.84,'no':.1,'insufficient':.06},
                       'context':{'yes':.1,'no':.85,'insufficient':.05},
                       'skills':{'yes':.1,'no':.85,'insufficient':.05}}
        client=self.PacketClient(probabilities,{'agent':'yes','memory':'yes','context':'no','skills':'no'},confidence=.1)
        self.assertEqual(classify_scope([scope],client)[0]['decision'],'include')
        self.assertEqual(classify_scope([scope],client,decision_metric='confidence')[0]['decision'],'pending')
        state=client.calls[0][0]
        self.assertEqual(state['sources'],[{'title':'A source title','text':'Source evidence'}])
        self.assertNotIn('PRIVATE_SCOPE',json.dumps(client.calls[0]))
        self.assertEqual(client.calls[1][0]['items'][0]['paper_id'],'PRIVATE_SCOPE')
        extraction={'contributions':[{'name':'A','description':'Claim','key':'ADMIN_SENTINEL'}],
                    'research_problems':[],'methods':[],'benchmarks':[],'schema_version':'ADMIN_SENTINEL'}
        diagnostic={'paper_id':'PRIVATE_SCOPE','text':'Source evidence','extraction':extraction,
                    'extractor':'ADMIN_SENTINEL'}
        client=self.PacketClient({'adequacy_contributions':{'adequate':.95,'omission':.03,'insufficient':.02}},confidence=.01)
        self.assertTrue(review_completeness([diagnostic],client)[0]['completeness']['contributions']['passed'])
        self.assertTrue(review_extractions([diagnostic],client)[0]['item_results'][0]['passed'])
        for state,_ in client.calls:
            self.assertNotIn('ADMIN_SENTINEL',json.dumps(state))
            self.assertEqual(state['items'][0]['extraction']['contributions'][0]['name'],'A')
        group={'group_id':'PRIVATE_GROUP','kind':'Benchmark','name':'Benchmark','description':'Description',
               'members':[{'mention_id':'PRIVATE_MENTION','name':'Dataset','description':'Version one',
                           'version_or_subset':'v1','paper_id':'ADMIN_SENTINEL'}]}
        client=self.PacketClient({'coherence':{'coherent':.9,'incoherent':.05,'insufficient':.05}},confidence=.1)
        self.assertTrue(review_groups([group],client)[0]['passed'])
        state,questions=client.calls[0]
        self.assertEqual(state['entities'][state['items'][0]['members'][0]]['version_or_subset'],'v1')
        self.assertNotIn('PRIVATE_GROUP',json.dumps({'state':state,'questions':questions}))
        self.assertNotIn('ADMIN_SENTINEL',json.dumps({'state':state,'questions':questions}))

    def test_nested_source_benchmark_version_is_semantic(self):
        entity={'mention_id':'private-m','name':'Benchmark','description':'Evaluation suite',
                'source':{'version_or_subset':'SWE-bench Verified','quote':'Verified benchmark excerpt',
                          'key':'ADMIN_SENTINEL',
                          'paper_id':'PRIVATE_PAPER','source_hash':'ADMIN_SENTINEL'}}
        pair={'pair_id':'private-p','kind':'Benchmark','left':entity,
              'right':{'mention_id':'private-n','name':'Benchmark','description':'Other variant',
                       'source':{'version_or_subset':'SWE-bench Lite','key':'ADMIN_SENTINEL'}}}
        client=self.PacketClient();judge_pairs([pair],client)
        state,questions=client.calls[0]
        self.assertEqual([e['version_or_subset'] for e in state['entities']],
                         ['SWE-bench Verified','SWE-bench Lite'])
        self.assertEqual(state['entities'][0]['quote'],'Verified benchmark excerpt')
        self.assertNotIn('ADMIN_SENTINEL',json.dumps({'state':state,'questions':questions}))
        self.assertNotIn('PRIVATE_PAPER',json.dumps({'state':state,'questions':questions}))
        member={'membership_id':'private-membership','group_id':'private-g','kind':'Benchmark',
                'name':'Benchmark','description':'Collection', 'member':entity}
        client=self.PacketClient();review_group_members([member],client)
        state,questions=client.calls[0]
        self.assertEqual(state['entities'][0]['version_or_subset'],'SWE-bench Verified')
        self.assertNotIn('ADMIN_SENTINEL',json.dumps({'state':state,'questions':questions}))

    def test_projected_budget_and_oversized_semantic_item(self):
        admin='z'*50000
        items=[{'paper_id':'p','field':'methods','key':str(i),'input_hash':admin,
                'text':'source','item':{'name':'M','description':'claim','status':admin}} for i in range(8)]
        client=self.PacketClient();review_items(items,client)
        self.assertEqual(len(client.calls),1)
        self.assertLess(len(json.dumps(client.calls[0][0],ensure_ascii=False)),40000)
        huge=dict(items[0],item={'name':'M','description':'x'*40001})
        with self.assertRaisesRegex(ValueError,'bounded'):
            review_items([huge],self.PacketClient())
        # Split a large shared registry and rebuild item references against each packet.
        shared={'mention_id':'A','name':'A','description':'Y'*12000}
        pairs=[{'pair_id':str(i),'kind':'Method','left':shared,
                'right':{'mention_id':str(i),'name':'B','description':'Z'*12000}} for i in range(3)]
        client=self.PacketClient();judge_pairs(pairs,client)
        self.assertGreater(len(client.calls),1)
        for state,questions in client.calls:
            for item in state['items']:
                self.assertEqual(state['entities'][item['left']]['name'],'A')
                self.assertEqual(state['entities'][item['right']]['name'],'B')
            self.assertLessEqual(len(json.dumps(state,ensure_ascii=False)),40000)
            self.assertLessEqual(len(json.dumps({'model':'jev-1.14.0','state':state,'questions':questions},ensure_ascii=False).encode()),60000)

if __name__=='__main__': unittest.main()
