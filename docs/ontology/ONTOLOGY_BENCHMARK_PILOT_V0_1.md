# Ontology Benchmarking — Pilot v0.1

Date: 2026-10-30. Status: exploratory, **not** a final ontology decision.

## Goal

Evaluate three plausible scholarly KG designs against competency questions CQ-01 to CQ-07 before implementing a Neo4j ingestion pipeline. Domain: memory management in LLM agents.

## Existing references

- [ORKG contributions and comparisons](https://orkg.readthedocs.io/en/latest/client/contributions.html): paper-scoped contributions can be compared through structured properties.
- [NLPContributionGraph](https://ncg-task.github.io/data.html): extraction from contribution sentences through scientific terms and triples, organized as contribution information units.
- [OpenAIRE Graph](https://graph.openaire.eu/docs/next/data-model/): scholarly metadata, stable identifiers and source integration.
- [W3C PROV-O](https://www.w3.org/TR/prov-o/): trace the origin and derivation of assertions.

## Curated sources (8 real papers)

- [Generative Agents](https://arxiv.org/abs/2304.03442)
- [Reflexion](https://arxiv.org/abs/2303.11366)
- [MemoryBank](https://arxiv.org/abs/2305.10250)
- [MemGPT](https://arxiv.org/abs/2310.08560)
- [LoCoMo](https://aclanthology.org/2024.acl-long.747/)
- [A-MEM](https://arxiv.org/html/2502.12110)
- [Zep](https://arxiv.org/abs/2501.13956)
- [Mem0](https://arxiv.org/html/2504.19413)

A small **manually curated** fixture included 8 method entities, 10 paper-specific claims, and 14 numerical observations. Sources were paper abstracts, Mem0 Table 1 and Section 4.1, A-MEM Table 7, and the Zep abstract.

## Tested alternatives

| Variant | Representation | Nodes | Edges | Deterministic CQ checks |
|---|---|---:|---:|---:|
| Flat | Paper/Method/Problem/Benchmark/Metric; measurement on Method→Benchmark relationship | 25 | 36 | 7/7 |
| Hybrid | Adds paper-scoped Contribution and one Evaluation per observed score | 48 | 87 | 7/7 |
| Claim-rich | Adds individual Claim and shared text-locator Evidence nodes | 70 | 121 | 7/7 |

All variants answered the seven **hand-authored test queries** using in-memory Python navigation, but they differed in the number of entities and whether paper→observation and claim→evidence can be traversed as explicit graph paths. **7/7 is not a retrieval accuracy score.** No Neo4j query latency, BM25, vector retrieval, or automated LLM extraction was measured in this pilot.

## Provenance-critical test cases

1. Zep's LoCoMo multi-hop F1 19.37 appears in **Mem0 Table 1**; that score must be attributed to Mem0 as reporting paper, not to the paper that introduces Zep.
2. The A-MEM authors report LoCoMo multi-hop F1 15.02 under DeepSeek-R1-32B in **A-MEM Table 7**; Mem0 Table 1 lists A-MEM multi-hop F1 12.14. Different reporting papers/configurations are **not** interchangeable observations.
3. Mem0 Section 4.1 reports that its graph-memory extension does not improve multi-hop F1 relative to the base method; this is a paper-scoped claim with a specific text locator.

## Provisional decision

**Use Hybrid as the initial implementation candidate**:

- Shared nodes: Paper, Method, ResearchProblem, Benchmark, Metric.
- Paper-scoped Contribution node.
- Evaluation node for each paper-reported measurement (a preliminary granularity).
- Score, category, unit, setup label and evidence reference as Evaluation properties.
- Paper-scoped claim summaries and evidence locators as Contribution properties or separately stored documents. Do not merge two authors' claims into a globally shared fact.
- Reserve explicit Claim/Evidence nodes for claim-level disagreement analysis and many-to-many evidence reuse.

## Remaining decisions

- Whether to split Evaluation into an EvaluationRun with separate Measurement observations.
- Define numeric units, protocol compatibility and baseline role.
- Define source-versioned evidence references and stable identifiers.
- Confirm entity resolution and relation provenance rules.
- Run equivalent queries against an actual Neo4j instance and a separately verified annotation set.

The full **local** benchmark package contains the executable standard-library-only Python runner, JSON fixtures, generated results and extended report. This repository note summarizes that experiment; it does not claim that the local runner has been committed here.
