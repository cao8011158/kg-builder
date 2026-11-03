"""Pure lineage tests; these do not open PostgreSQL or call external models."""
import unittest

from kg_builder.snapshot_graph import revision_identity


class RevisionIdentityTests(unittest.TestCase):
    def test_mutual_overlap_inherits_even_when_default_id_changes(self):
        old = {'entity:old': ('Method', {'m1', 'm2'})}
        new = {'entity:new': ('Method', {'m1', 'm2', 'm3'})}
        ids, lineage = revision_identity(old, {}, new, {}, {'m1', 'm2'}, 'snapshot')
        self.assertEqual(ids, {'entity:new': 'entity:old'})
        self.assertEqual(lineage['entities'], [
            {'old_id': 'entity:old', 'new_id': 'entity:new', 'relation': 'inherited'}])

    def test_split_and_merge_have_lineage_without_id_inheritance(self):
        old = {'entity:old': ('Method', {'m1', 'm2'})}
        new = {'entity:a': ('Method', {'m1'}), 'entity:b': ('Method', {'m2'})}
        ids, lineage = revision_identity(old, {}, new, {}, {'m1', 'm2'}, 'snapshot')
        self.assertEqual(ids, {})
        self.assertEqual({r['relation'] for r in lineage['entities']}, {'overlap'})
        ids, lineage = revision_identity(new, {}, old, {}, {'m1', 'm2'}, 'snapshot')
        self.assertEqual(ids, {})
        self.assertEqual({r['relation'] for r in lineage['entities']}, {'overlap'})

    def test_changed_evidence_cannot_inherit(self):
        old = {'entity:old': ('Method', {'m1'})}
        new = {'entity:new': ('Method', {'m1'})}
        ids, lineage = revision_identity(old, {}, new, {}, set(), 'snapshot')
        self.assertEqual(ids, {})
        self.assertEqual(lineage['entities'], [])

    def test_area_ids_are_independent_from_entity_ids(self):
        old = {'area:old': ('ResearchArea', {'r1'})}
        new = {'area:new': ('ResearchArea', {'r1', 'r2'})}
        ids, lineage = revision_identity({}, old, {}, new, {'r1'}, 'snapshot')
        self.assertEqual(ids, {'area:new': 'area:old'})
        self.assertEqual(lineage['research_areas'][0]['relation'], 'inherited')

    def test_split_remaps_prior_published_default(self):
        old = {'entity:a': ('Method', {'m1', 'm2'})}
        new = {'entity:a': ('Method', {'m1'}), 'entity:b': ('Method', {'m2'})}
        ids, lineage = revision_identity(old, {}, new, {}, {'m1', 'm2'}, 'snapshot')
        self.assertIn('entity:a', ids)
        self.assertNotEqual(ids['entity:a'], 'entity:a')
        self.assertNotIn(ids['entity:a'], old)
        self.assertEqual({r['relation'] for r in lineage['entities']}, {'overlap'})

    def test_default_cannot_take_another_nodes_inherited_id(self):
        old = {'entity:a': ('Method', {'m1'})}
        new = {'entity:a': ('Method', {'m2'}), 'entity:b': ('Method', {'m1'})}
        ids, _ = revision_identity(old, {}, new, {}, {'m1'}, 'snapshot')
        self.assertEqual(ids['entity:b'], 'entity:a')
        self.assertNotEqual(ids['entity:a'], 'entity:a')
        self.assertNotEqual(ids['entity:a'], ids['entity:b'])

    def test_research_problem_source_projection_keeps_default_id(self):
        old={'entity:problem': ('ResearchProblem', {'r1'})}
        new={'entity:problem': ('ResearchProblem', {'r1'})}
        ids,lineage=revision_identity(old,{},new,{},set(),'snapshot')
        self.assertEqual(ids,{})
        self.assertEqual(lineage['entities'],[])


if __name__ == '__main__': unittest.main()
