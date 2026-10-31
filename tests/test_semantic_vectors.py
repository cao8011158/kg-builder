"""Real in-memory HNSW and Leiden behavior; encoder tests avoid model download."""
import unittest
from unittest.mock import patch

import numpy as np

from kg_builder.embeddings import EmbeddingModel
from kg_builder.problem_clustering import cluster_problem_vectors
from kg_builder.vector_index import HNSWIndex


def unit(x, y):
    v = np.array([x, y], dtype=np.float32)
    return v / np.linalg.norm(v)


class FakeEncoder:
    def embed(self, texts, batch_size):
        for text in texts:
            yield [float(len(text)), 4.0]


class EmbeddingTests(unittest.TestCase):
    def test_lazy_normalized_and_stable_identity(self):
        model = EmbeddingModel()
        self.assertIsNone(model._model)
        self.assertIn("BAAI/bge-small-en-v1.5", model.model_id)
        with patch("kg_builder.embeddings.version", return_value="test-version"):
            self.assertIn("fastembed-test-version:", model.model_id)
        self.assertEqual(model.encode([]).shape, (0, 0))
        self.assertIsNone(model._model)
        model._model = FakeEncoder()
        result = model.encode(["first", "second"])
        self.assertEqual(result.dtype, np.float32)
        self.assertEqual(result.shape, (2, 2))
        np.testing.assert_allclose(np.linalg.norm(result, axis=1), 1, atol=1e-6)
        self.assertEqual(model.encode([]).shape, (0, 2))

    def test_bad_encoder_result_rejected(self):
        class ZeroEncoder:
            def embed(self, texts, batch_size):
                yield [0.0, 0.0]
        model = EmbeddingModel()
        model._model = ZeroEncoder()
        with self.assertRaises(ValueError):
            model.encode(["foo"])


class IndexTests(unittest.TestCase):
    def test_upsert_delete_and_resize_stable_ids(self):
        vectors = {f"id:{i:03d}": unit(1, i / 10000) for i in range(35)}
        index = HNSWIndex(vectors, "model:v1")
        original_hnsw = index._index
        self.assertEqual(len(index.search(unit(1, 0), k=100)), 35)
        index.upsert("id:000", unit(0, 1))
        self.assertEqual(index.search(unit(0, 1), k=1)[0]["id"], "id:000")
        index.delete("id:000")
        index.delete("id:000")
        self.assertNotIn("id:000", [m["id"] for m in index.search(unit(0, 1), k=100)])
        index.upsert("new:stable", unit(0, 1))
        self.assertIs(index._index, original_hnsw)
        self.assertEqual(index.search(unit(0, 1), k=1)[0]["id"], "new:stable")
        self.assertEqual(len(index.search(unit(0, 1), k=100)), 35)

    def test_validation_empty_and_ties(self):
        empty = HNSWIndex({}, "model:v1")
        self.assertEqual(empty.search(unit(1, 0)), [])
        with self.assertRaises(ValueError):
            empty.search([0, 0])
        with self.assertRaises(ValueError):
            HNSWIndex({}, "")
        with self.assertRaises(ValueError):
            HNSWIndex({}, "model:v1", seed=True)
        index = HNSWIndex({"z": unit(1, 0), "a": unit(1, 0)}, "model:v1")
        self.assertEqual([m["id"] for m in index.search(unit(1, 0), 2)], ["a", "z"])
        for malformed in ([1.0, 0.0, 0.0], [float("nan"), 0.0], [2.0, 0.0], [0, 0]):
            with self.assertRaises(ValueError):
                index.upsert("bad", malformed)
        with self.assertRaises(ValueError):
            index.search(unit(1, 0), -1)
        with self.assertRaises(ValueError):
            index.search(unit(1, 0), True)

    def test_search_more_than_initial_ef(self):
        vectors = {f"id:{i:03d}": unit(np.cos(i * .02), np.sin(i * .02)) for i in range(160)}
        index = HNSWIndex(vectors, "model:v1")
        found = index.search(unit(1, 0), k=140)
        self.assertEqual(len(found), 140)
        self.assertEqual(len({match["id"] for match in found}), 140)
        self.assertGreaterEqual(index._index.ef, 140)


class ClusteringTests(unittest.TestCase):
    def test_degenerate_and_sparse(self):
        self.assertEqual(cluster_problem_vectors({})["clusters"], [])
        one = cluster_problem_vectors({"solo": unit(1, 0)})
        self.assertEqual(one["clusters"], [["solo"]])
        self.assertEqual(one["edges"], [])
        separated = cluster_problem_vectors({"a": unit(1, 0), "b": unit(-1, 0)})
        self.assertEqual(separated["clusters"], [["a"], ["b"]])
        self.assertEqual(separated["diagnostics"]["candidate_comparisons"], 2)

    def test_bridge_is_candidate_not_equivalence(self):
        vectors = {
            "a1": unit(1, 0), "a2": unit(1, .02), "a3": unit(1, -.02),
            "bridge": unit(1, 1),
            "b1": unit(0, 1), "b2": unit(.02, 1), "b3": unit(-.02, 1),
        }
        result = cluster_problem_vectors(vectors, k=4, min_similarity=.65, resolution=.2)
        edges = {(e["source"], e["target"]) for e in result["edges"]}
        self.assertIn(("a1", "bridge"), edges)
        self.assertIn(("b1", "bridge"), edges)
        self.assertNotIn(("a1", "b1"), edges)
        self.assertGreaterEqual(len(result["clusters"]), 2)
        self.assertEqual(sorted(sum(result["clusters"], [])), sorted(vectors))
        self.assertLessEqual(result["diagnostics"]["candidate_comparisons"], len(vectors) * 4)
        self.assertFalse(result["diagnostics"]["all_pairs_comparisons"])

    def test_mutual_topk_changes_edge_set(self):
        vectors = {"a": unit(1, 0), "b": unit(1, .01), "c": unit(1, -.1)}
        union = cluster_problem_vectors(vectors, k=1, min_similarity=.8)
        mutual = cluster_problem_vectors(vectors, k=1, min_similarity=.8, mutual=True)
        self.assertTrue({(e["source"], e["target"]) for e in mutual["edges"]}
                        <= {(e["source"], e["target"]) for e in union["edges"]})
        self.assertGreater(len(union["edges"]), len(mutual["edges"]))

    def test_candidates_bound_when_unrelated_points_added(self):
        small = {f"p:{i:03d}": unit(np.cos(i * .1), np.sin(i * .1)) for i in range(30)}
        large = {f"p:{i:03d}": unit(np.cos(i * .1), np.sin(i * .1)) for i in range(150)}
        for vectors in (small, large):
            result = cluster_problem_vectors(vectors, k=3, min_similarity=.99)
            self.assertLessEqual(result["diagnostics"]["candidate_comparisons"], 3 * len(vectors))
            self.assertFalse(result["diagnostics"]["all_pairs_comparisons"])


if __name__ == "__main__":
    unittest.main()
