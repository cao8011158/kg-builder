"""Synthetic revision-audit tests; no PostgreSQL service is started."""
import json
import unittest

from kg_builder.graph_validation import _revision_contract
from kg_builder.postgres_graph import SCHEMA_FINGERPRINT, SCHEMA_REVISION
from kg_builder.semantic_store import digest


class Rows:
    def __init__(self, rows): self.rows = rows
    def __iter__(self): return iter(self.rows)
    def fetchone(self): return self.rows[0] if self.rows else None


class ParentGraph:
    def __init__(self, *, split=False, changed=False):
        self.split = split
        self.changed = changed
    def execute(self, query, params=None):
        statement = str(query)
        if 'to_regclass' in statement: return Rows([{'relation': 'schema_bootstrap'}])
        if 'schema_bootstrap' in statement:
            return Rows([{'run_id': 'parent', 'revision': SCHEMA_REVISION,
                          'ddl_fingerprint': SCHEMA_FINGERPRINT}])
        if 'meta' in statement:
            return Rows([{'key': k, 'value': v} for k, v in {
                'phase': 'complete', 'index_status': 'verified',
                'completed_at': '2026-11-04T00:00:00Z',
                'graph_policy': 'domain-v1', 'member_review_mode': 'whole-group-v1',
                'snapshot_hash': 'frozen', 'model_id': 'test-model'}.items()])
        if 'research_area_members' in statement:
            return Rows([{'area_id': 'area:old', 'mention_id': 'r1'}])
        if 'research_areas' in statement: return Rows([{'area_id': 'area:old'}])
        if 'sources' in statement:
            return Rows([{'extraction_id': 'extraction', 'source_hash': 'old' if self.changed else 'source'}])
        if 'mentions' in statement:
            return Rows([{'entity_id': 'entity:old', 'mention_id': mid,
                          'mention_hash': 'mention', 'extraction_id': 'extraction'}
                         for mid in (('m1', 'm2', 'r1') if self.split else ('m1', 'r1'))])
        if 'entities' in statement: return Rows([{'entity_id': 'entity:old', 'kind': 'Method'}])
        raise AssertionError(statement)


class RevisionContractTests(unittest.TestCase):
    def check(self, *, split=False, changed=False, entity_id='entity:new', extra_entity=False,
              mapping=None, lineage=None,
              snapshot_hash='frozen', delta_hash='delta', removed=None,
              child_model='test-model', vectors=None):
        entities = {entity_id: ('Method', ['m1'])}
        if split: entities['entity:second'] = ('Method', ['m2'])
        if extra_entity: entities['entity:old'] = ('Method', ['m3'])
        expected_lineage = {'entities': [
            {'old_id': 'entity:old', 'new_id': new_id,
             'relation': 'overlap' if split else 'inherited'}
            for new_id in entities if new_id != 'entity:old' or entity_id == 'entity:old'],
            'research_areas': [{'old_id': 'area:old', 'new_id': 'area:new',
                                'relation': 'inherited'}]}
        meta = {'base_run_id': 'parent', 'base_snapshot_hash': 'frozen',
                'delta_snapshot_hash': 'delta', 'removed_paper_ids': '[]',
                'model_id': child_model,
                'revision_node_ids': json.dumps(mapping if mapping is not None else
                                                ({'area:new': 'area:old'} if split else
                                                 {'entity:new': 'entity:old', 'area:new': 'area:old'})),
                'revision_lineage': json.dumps(lineage if lineage is not None else expected_lineage)}
        failures = []
        snapshot = {'snapshot_hash': 'combined', 'vectors': [] if vectors is None else vectors,
                    'incremental': {'base_snapshot_hash': snapshot_hash,
                                    'delta_snapshot_hash': delta_hash,
                                    'removed_paper_ids': [] if removed is None else removed}}
        mentions = {mid: {'extraction_id': 'extraction', 'mention_hash': 'mention'}
                    for mid in ('m1', 'm2', 'm3', 'r1')}
        sources = {'extraction': {'source_hash': 'source'}}
        result = _revision_contract(ParentGraph(split=split, changed=changed), meta,
                                    snapshot, 'current', entities,
                                    {'area:new': object()}, {('area:new', 'r1')},
                                    mentions, sources,
                                    lambda code, **context: failures.append(code))
        return failures, result

    def test_inherited_ids_are_applied_to_entities_and_areas(self):
        failures, (entities, areas, members, ids) = self.check()
        self.assertEqual(failures, [])
        self.assertEqual(set(entities), {'entity:old'})
        self.assertEqual(set(areas), {'area:old'})
        self.assertEqual(members, {('area:old', 'r1')})
        self.assertEqual(ids['entity:new'], 'entity:old')

    def test_split_is_lineage_only(self):
        failures, (entities, _, _, _) = self.check(split=True)
        self.assertEqual(failures, [])
        self.assertEqual(set(entities), {'entity:new', 'entity:second'})

    def test_invalid_inheritance_and_lineage_fail(self):
        failures, _ = self.check(split=True, mapping={'entity:new': 'entity:old',
                                                        'area:new': 'area:old'})
        self.assertIn('revision_node_mapping_mismatch', failures)
        failures, _ = self.check(lineage={'entities': [], 'research_areas': []})
        self.assertIn('revision_lineage_mismatch', failures)

    def test_changed_source_cannot_inherit_identity(self):
        failures, _ = self.check(changed=True)
        self.assertIn('revision_node_mapping_mismatch', failures)
        self.assertIn('revision_lineage_mismatch', failures)

    def test_revision_snapshot_hashes_are_bound(self):
        for hashes in ({'snapshot_hash': 'other'}, {'delta_hash': 'other'}):
            failures, _ = self.check(**hashes)
            self.assertIn('invalid_revision_snapshot_binding', failures)
        failures, _ = self.check(removed=['paper-1'])
        self.assertIn('invalid_revision_snapshot_binding', failures)

    def test_empty_revision_can_have_no_child_embedding_model(self):
        failures, _ = self.check(child_model='')
        self.assertEqual(failures, [])
        failures, _ = self.check(child_model='', vectors=[{'model_id': 'test-model'}])
        self.assertIn('invalid_revision_base_metadata', failures)
        failures, _ = self.check(vectors=[{'model_id': 'different-model'}])
        self.assertIn('invalid_revision_base_metadata', failures)

    def test_split_does_not_keep_ambiguous_baseline_id(self):
        alternate = 'entity:' + digest(['revision', 'combined', 'entity:old', 0])[:32]
        failures, (entities, _, _, ids) = self.check(
            split=True, entity_id='entity:old',
            mapping={'entity:old': alternate, 'area:new': 'area:old'})
        self.assertEqual(failures, [])
        self.assertEqual(set(entities), {alternate, 'entity:second'})
        self.assertEqual(ids['entity:old'], alternate)
        failures, _ = self.check(split=True, entity_id='entity:old',
                                 mapping={'area:new': 'area:old'})
        self.assertIn('revision_node_mapping_mismatch', failures)

    def test_new_default_cannot_collide_with_an_inherited_id(self):
        alternate = 'entity:' + digest(['revision', 'combined', 'entity:old', 0])[:32]
        failures, (entities, _, _, _) = self.check(
            extra_entity=True,
            mapping={'entity:new': 'entity:old', 'entity:old': alternate,
                     'area:new': 'area:old'})
        self.assertEqual(failures, [])
        self.assertEqual(set(entities), {'entity:old', alternate})
        failures, _ = self.check(extra_entity=True,
                                 mapping={'entity:new': 'entity:old', 'area:new': 'area:old'})
        self.assertIn('revision_node_mapping_mismatch', failures)
        self.assertIn('revision_node_id_collision', failures)

    def test_changed_research_problem_keeps_singleton_default_id(self):
        class ResearchProblemParent(ParentGraph):
            def execute(self, query, params=None):
                statement = str(query)
                if 'entities' in statement:
                    return Rows([{'entity_id': 'entity:rp', 'kind': 'ResearchProblem'}])
                if 'mentions' in statement:
                    return Rows([{'mention_id': 'r1', 'entity_id': 'entity:rp',
                                  'mention_hash': 'old', 'extraction_id': 'extraction'}])
                return super().execute(query, params)

        snapshot = {'snapshot_hash': 'combined', 'incremental': {
            'base_snapshot_hash': 'frozen', 'delta_snapshot_hash': 'delta',
            'removed_paper_ids': []}}
        meta = {'base_run_id': 'parent', 'base_snapshot_hash': 'frozen',
                'delta_snapshot_hash': 'delta', 'removed_paper_ids': '[]',
                'model_id': 'test-model',
                'revision_lineage': json.dumps({'entities': [], 'research_areas': []}),
                'revision_node_ids': '{}'}
        def audit(mapping):
            meta['revision_node_ids'] = json.dumps(mapping)
            failures = []
            _revision_contract(ResearchProblemParent(), meta, snapshot, 'current',
                               {'entity:rp': ('ResearchProblem', ['r1'])}, {}, set(),
                               {'r1': {'mention_hash': 'changed', 'extraction_id': 'extraction'}},
                               {'extraction': {'source_hash': 'source'}},
                               lambda code, **context: failures.append(code))
            return failures
        self.assertEqual(audit({}), [])
        self.assertIn('revision_node_mapping_mismatch',
                      audit({'entity:rp': 'entity:tampered'}))


if __name__ == '__main__': unittest.main()
