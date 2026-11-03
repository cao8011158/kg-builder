"""Provisional candidate groups from a sparse research-problem similarity graph.

Only ResearchProblem vectors belong here. A cluster is a retrieval candidate,
not an equivalence decision. Each point queries HNSW for its own neighbors;
the graph retains exact-cosine edges above a provisional threshold. This
module never materializes an N by N similarity matrix.
"""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from .vector_index import HNSWIndex


def cluster_problem_vectors(
    vectors: dict[str, Sequence[float]], k: int = 20, min_similarity: float = 0.65,
    resolution: float = 0.05, seed: int = 42, mutual: bool = False,
) -> dict:
    if not isinstance(k, int) or isinstance(k, bool) or k < 1:
        raise ValueError("k must be a positive integer")
    if not np.isfinite(min_similarity) or not 0 < min_similarity <= 1:
        raise ValueError("min_similarity must be in (0, 1]")
    if not np.isfinite(resolution) or resolution <= 0:
        raise ValueError("resolution must be positive")
    if not isinstance(mutual, bool):
        raise ValueError("mutual must be boolean")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise ValueError("seed must be an integer")
    ids = sorted(vectors)
    index = HNSWIndex(vectors, model_id="research-problem-input-v1", seed=seed)
    directed: set[tuple[str, str]] = set()
    edge_scores: dict[tuple[str, str], float] = {}
    candidate_comparisons = 0
    for item_id in ids:
        # Ask for k+1 because the query point itself is normally returned.
        neighbors = index.search(vectors[item_id], k=min(len(ids), k + 1))
        selected = 0
        for neighbor in neighbors:
            other_id = neighbor["id"]
            if other_id == item_id:
                continue
            if selected == k:
                break
            selected += 1
            candidate_comparisons += 1
            similarity = neighbor["similarity"]  # exact cosine after HNSW candidate retrieval
            if similarity >= min_similarity and similarity > 0:
                directed.add((item_id, other_id))
                pair = tuple(sorted((item_id, other_id)))
                edge_scores[pair] = similarity
    edges = [
        {"source": source, "target": target, "similarity": score}
        for (source, target), score in sorted(edge_scores.items())
        if not mutual or ((source, target) in directed and (target, source) in directed)
    ]

    import igraph as ig
    import leidenalg

    positions = {item_id: i for i, item_id in enumerate(ids)}
    graph = ig.Graph(n=len(ids), edges=[(positions[e["source"]], positions[e["target"]]) for e in edges],
                     directed=False)
    if edges:
        partition = leidenalg.find_partition(
            graph, leidenalg.CPMVertexPartition, weights=[e["similarity"] for e in edges],
            resolution_parameter=resolution, seed=seed, n_iterations=-1,
        )
        clusters = sorted((sorted(ids[i] for i in community) for community in partition),
                          key=lambda cluster: cluster[0])
        objective = float(partition.quality())
    else:
        clusters = [[item_id] for item_id in ids]
        objective = 0.0
    return {
        "clusters": clusters,
        "edges": edges,
        "diagnostics": {
            "algorithm": "leiden-cpm", "candidate_index": "hnsw-cosine", "edge_metric": "exact_cosine",
            "objective": objective, "k": k, "min_similarity": min_similarity,
            "resolution": resolution, "seed": seed, "mutual": mutual,
            "point_count": len(ids), "edge_count": len(edges),
            "candidate_comparisons": candidate_comparisons,
            "all_pairs_comparisons": False,
        },
    }
