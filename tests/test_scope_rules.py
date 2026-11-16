"""Offline screening policy tests for the full preparation plan."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from kg_builder.decision_api import classify_scope_rules
from kg_builder.full_preparation import PLAN, validate_screening_policy


POLICY={'schema':'scope-rules/1',
        'inclusion_rules':[{'name':'battery','question':'Is grid battery degradation central?'}],
        'exclusion_rules':[{'name':'review','question':'Is this a review rather than original research?'}]}
PAPER={'paper_id':'p1','title':'Grid battery aging','text':'Measured grid battery degradation.'}


class RuleClient:
    def __init__(self,choices,probability=1):
        self.choices=choices;self.probability=probability;self.questions=[]

    def decide(self,state,questions):
        """Return named Jev choices for an offline source-grounding probe."""
        self.questions.extend(q['instructions'] for q in questions.values())
        answers={}
        for key in questions:
            name=key.split('_',2)[2]
            choice=self.choices[name]
            answers[key]={'type':'choice','choice':choice,'confidence':1,
                          'probabilities':{option:(self.probability if option==choice else 0)
                                           for option in ('yes','no','insufficient')}}
        return {'model':'jev-1.14.0','usage':{},'answers':answers,'attempts':1,'request_duration_s':0}


class ScopeRulesTests(unittest.TestCase):
    def test_different_domain_decisions_and_questions(self):
        cases=[({'battery':'yes','review':'no'},'include'),
               ({'battery':'yes','review':'yes'},'exclude'),
               ({'battery':'yes','review':'insufficient'},'pending'),
               ({'battery':'no','review':'insufficient'},'exclude'),
               ({'battery':'insufficient','review':'no'},'pending')]
        for choices,expected in cases:
            with self.subTest(choices=choices):
                client=RuleClient(choices)
                result=classify_scope_rules([PAPER],client,POLICY)[0]
                self.assertEqual(result['decision'],expected)
                self.assertEqual(len(client.questions),2)
                self.assertTrue(all('grid battery' in q.lower() or 'review' in q.lower() for q in client.questions))
                self.assertNotIn('LLM agents',' '.join(client.questions))
                self.assertEqual(set(result['rule_answers']),{'include_battery','exclude_review'})
        self.assertEqual(classify_scope_rules([PAPER],RuleClient({'battery':'yes','review':'no'},.6),POLICY)[0]['decision'],'pending')
        no_exclusions={**POLICY,'exclusion_rules':[]}
        self.assertEqual(classify_scope_rules([PAPER],RuleClient({'battery':'yes'}),no_exclusions)[0]['decision'],'include')

    def test_optional_policy_validation(self):
        legacy=json.loads(PLAN.read_text(encoding='utf-8'))
        validate_screening_policy(legacy)
        validate_screening_policy({**legacy,'screening_policy':POLICY})
        for invalid in ({}, {'schema':'scope-rules/1','inclusion_rules':[],'exclusion_rules':[]},
                        {**POLICY,'inclusion_rules':[{'name':'battery','question':'yes?'}],
                         'exclusion_rules':[{'name':'battery','question':'no?'}]},
                        {**POLICY,'inclusion_rules':[{'name':'battery','question':' '}]}):
            with self.subTest(invalid=invalid),self.assertRaises(ValueError):
                validate_screening_policy({**legacy,'screening_policy':invalid})


if __name__=='__main__':unittest.main()
