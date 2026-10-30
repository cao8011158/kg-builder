# Ontology v0.1: six-domain coverage review

Status: **design review only** (2026-10-31). This does not freeze the ontology or demonstrate a working Neo4j graph.

## 1. Requirements and scope

The corpus covers papers first available **2025-01-01 onward**, across six non-exclusive topics: agent memory; context compression/management; tool use; RL post-training (with related preference-optimization comparators); recursive self-improvement; and skill/self-improving-skill management. Paper-level `domain_tags` may contain multiple values.

The KG should extract reusable, paper-grounded summaries, concepts, relationships and key experimental results, with drill-down references to original text. Dense and sparse search should index normalized semantic text per node, excluding internal IDs and technical metadata from the embedding input.

## 2. Original seven-node coverage

| Node | Coverage | Key limitation |
|---|---|---|
| Paper | Strong across six domains | Needs provenance, DOI/arXiv version |
| Contribution | Strong | Must remain paper-scoped, not merged across authors |
| ResearchProblem | Strong | Keep separate from broad topic tags |
| Method | Broad but overloaded | An algorithm is not the same as a learned model, skill or callable tool |
| Benchmark | Strong | Distinguish evaluation protocol from a training dataset |
| Metric | Strong | Units, direction and calculation protocol matter |
| Evaluation | Strong with conditions | Need explicit evaluated target, reporting paper and setup to compare safely |

The seven-node design can **summarize** all six areas. It cannot **cleanly traverse** concrete model / skill / tool / version relationships without encoding those as strings or mislabeling them as Method.

## 3. Source-based stress cases (illustrative, not a scored benchmark)

| Domain | Representative 2025+ source | What seven nodes capture | Missing distinction |
|---|---|---|---|
| Agent Memory | Mem0, arXiv:2504.19413 | Method, Contribution, LoCoMo, Evaluation | Specific Mem0 implementation vs mechanism |
| Context Management | ACE, arXiv:2510.04618; ACM, arXiv:2607.23809 | Method and performance evaluation | Evolving playbook/context state as target object |
| Tool Use | ToolRL, NeurIPS 2025 | Reward-design method, task/benchmark, result | Tools used/executed vs learned method |
| RL Post-training | DeepSeek-R1, Nature 2025 | GRPO method, evaluations | Trained model/checkpoint is not itself an optimization method |
| Recursive Self-improvement | Darwin Gödel Machine, arXiv:2505.22954 | Method, coding benchmark, results | Code/agent revisions and parent-child lineage |
| Skill / Self-improving Skill | SkillWeaver, arXiv:2504.07079; MemSkill, arXiv:2602.02474 | Skill induction method, evaluations, claims | Produced/revised skill vs skill induction algorithm; skill library |

Sources:
- https://arxiv.org/abs/2504.19413
- https://arxiv.org/abs/2510.04618
- https://arxiv.org/abs/2607.23809
- https://proceedings.neurips.cc/paper_files/paper/2025/hash/97c5b2707228e7e3fb67e4ecc2e0e607-Abstract-Conference.html
- https://www.nature.com/articles/s41586-025-09422-z
- https://arxiv.org/abs/2505.22954
- https://arxiv.org/abs/2504.07079
- https://arxiv.org/abs/2602.02474

This review maps paper abstracts and selected publication descriptions to schema requirements. It does **not** claim full-text extraction, inter-annotator validation or an executed six-domain retrieval evaluation.

## 4. Minimal recommended extension: Artifact

Keep seven original node types and **provisionally add one**:

`Artifact {id, artifact_kind, name, version?, description?, source_ref?, search_text, embedding?, ...}`

`artifact_kind` candidate controlled values: `MODEL`, `AGENT_SYSTEM`, `TOOL`, `SKILL`, `SKILL_LIBRARY`, `CONTEXT_PLAYBOOK`, `PROMPT`, `CODE_IMPLEMENTATION`, `DATASET`. Values are ontology-controlled, not independent new labels in v0.2.

- **Method** = approach / procedure / algorithm, e.g., GRPO or skill discovery.
- **Artifact** = concrete entity generated, modified, applied or trained by a method, e.g., a model, reusable callable API skill, agent code version, or skill library.
- **Self-improved Skill** is *not* a separate node type: represent a skill artifact and, only when evidence exists, its revisions and improvements using provenance-backed relations.
- Do not create a node for each transient skill instance or run unless the paper identifies it sufficiently for stable reference or a competency question requires it.
- Do not create a generic artifact merely because the term “tool” or “skill” appears in a paper.
- Keep Benchmark as a benchmark/protocol entity; a training dataset may be `Artifact(kind=DATASET)` when useful.

## 5. Proposed relationship adjustments

| Source | Relationship | Target | Rationale |
|---|---|---|---|
| Paper | HAS_CONTRIBUTION | Contribution | Authorship/reporting scope |
| Contribution | ADDRESSES | ResearchProblem | Paper-grounded problem claim |
| Contribution | PROPOSES_METHOD | Method | Method proposal distinct from artifact |
| Contribution | INTRODUCES_ARTIFACT | Artifact | Created/released object |
| Method | USES_METHOD | Method | Technique dependency where explicit |
| Method | USES_ARTIFACT | Artifact | Employs concrete object, with source |
| Method | PRODUCES_ARTIFACT | Artifact | Creates a model/skill/etc. |
| Method | MODIFIES_ARTIFACT | Artifact | Describes operation; may add operation property |
| Artifact | IMPLEMENTS_METHOD | Method | Realization of technique |
| Artifact | DERIVED_FROM | Artifact | Version lineage, only with evidence |
| Paper | REPORTS_EVALUATION | Evaluation | Attribution of a reported result |
| Evaluation | EVALUATES_METHOD | Method | When evaluation target is a technique |
| Evaluation | EVALUATES_ARTIFACT | Artifact | When evaluation target is model/system/skill |
| Evaluation | USES_BENCHMARK | Benchmark | Benchmark used |
| Evaluation | MEASURES | Metric | Metric and values |
| Paper | CITES | Paper | Citation metadata |

Relationship direction and naming remain draft. Preserve provenance on paper-specific relation assertions; avoid turning a paper's reported improvement into an unconditional global `OUTPERFORMS` edge. `Evaluation` is a paper-reported, setup-scoped measurement, not an objective universal fact. Multiple metrics/configurations need separately identifiable measurements even if `Result` is not a node.

## 6. Design checks and candidate competency questions

1. Which RL post-training method was used to train a particular **model artifact**, and which paper reports its evaluations?
2. Which tool-use methods improve selection or application, and what kind of tool artifacts (if identified) do they use?
3. Which Agent Memory methods produce or consume **skill libraries**, and what evidence supports their reported gains?
4. Which named self-improving agent code version or skill revision **DERIVED_FROM** a preceding version, with source locator?
5. Which context management methods update a context playbook vs compress text, and how are they evaluated?
6. Which Skill-generation and Skill-verification methods are evaluated on the same benchmark?

The first seven memory-oriented competency questions are retained; these cross-domain questions expand the specification. All queries start from natural language and may use sparse/dense search to anchor graph traversal; tests should never assume end users know internal node IDs.

## 7. Deferred modeling decisions

- Do not yet add dedicated Tool, Skill, Model, Dataset, Claim, Evidence or Result labels.
- Keep Contribution paper-scoped; concise finding text can be stored on the contribution with evidence locator, with promotion to structured Claim only if needed.
- Use a single Evaluation node per comparable reported measurement/configuration as first approximation. Keep numerical values and metric units as properties; a full TrainingRun/Measurement ontology is not required for the first prototype.
- Use stable IDs and merge conservatively: paper-local unnamed artifacts do not merge globally just because their descriptions are similar.
- Validate the extension on source-grounded annotated examples and Neo4j queries before freezing v0.2.
