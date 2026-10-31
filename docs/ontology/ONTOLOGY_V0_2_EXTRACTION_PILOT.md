# Ontology v0.2 — source-grounded extraction pilot

Status: **candidate schema refinement, not frozen**. Six fixed arXiv v1 papers were read at selected method, setup, results and limitation sections on 2026-10-31. The private SQLite store contains 6 new Draft extractions, 11 paper-version-scoped contributions, 14 reported measurements and 34 source anchors. No Neo4j nodes, indexes, embeddings or Agent evaluation were created.

## Coverage and observed modeling requirements

| Domain anchor | Fixed primary source | Requirement exposed by actual source |
|---|---|---|
| Agent memory: A-Mem | https://arxiv.org/html/2502.12110v1 | Memory mechanism and deployed agent configuration differ; metrics must retain question category and backbone. |
| Context management: ACE | https://arxiv.org/html/2510.04618v1 | Evolving playbook is an Artifact; offline/online adaptation and availability of labels belong to evaluation setup. |
| Tool use: ToolRL | https://arxiv.org/html/2504.13958v1 | Reward-design method, used GRPO concept, trained model configuration and tool benchmark are different entities. |
| RL post-training: DAPO | https://arxiv.org/html/2503.14476v1 | avg@32 is not pass@32; progressive component addition is not an isolated-factor causal ablation. |
| Recursive self-improvement: DGM | https://arxiv.org/html/2505.22954v1 | Frozen foundation model, editable harness and fixed outer search loop must be distinguished; subset and full benchmark results differ. |
| Skill evolution: SkillWeaver | https://arxiv.org/html/2504.07079v1 | Executable skill library differs from induction method; exception-free execution does not guarantee semantic success. |

The nine node kinds remain sufficient for these examples. Findings and evidence remain structured contribution/extraction properties. Six selected examples do not prove exhaustive coverage or schema validity for all six domains.

## Candidate properties and identity boundaries

`id` below means a canonical graph identity after entity review. Current extraction keys are **candidates**, not completed global entity resolution. All nodes may later expose derived `search_text`; no dense or sparse index has been built. Do not embed internal IDs, hashes or operational logs as semantic content.

| Node | Candidate required properties | Optional / structured properties | Identity boundary |
|---|---|---|---|
| Paper | id, title, identity_status | DOI aliases, arXiv identity, first_release_date, date_evidence_status | One scholarly work; source IDs/aliases resolve identity, never title alone. |
| PaperVersion | id, paper_id, source_url, source_status | version_label, publication timestamp, capture hashes, source format | Confirmed source revision; keep unverified metadata snapshots distinct. HTML capture hash and abstract hash describe different bytes. |
| Contribution | id, paper_version_id, description, claim_status, evidence_refs | contribution_kind, findings, caveats, source_conflicts | Exact reporting version; never merge claims across papers just because wording is similar. |
| Method | id, name, description, resolution_status | variant, components, applicability | Reusable conceptual procedure, with source-attributed assertions about each use/variant. |
| ResearchProblem | id, name, description, resolution_status | assumptions, constraints, scope | Reviewed concept; temporary paper-derived keys are provisional, not global canonicalization. |
| Artifact | id, artifact_kind, name, name_origin, identity_scope, evidence_refs | version/revision if actually identified, repository URL, implementation/configuration, verification_status | Named shared artifact or explicitly scoped configuration. Annotator-created labels do not become author-defined named systems. |
| Benchmark | id, name, protocol, resolution_status | benchmark_version, dataset/task description, split definitions | Evaluation resource/protocol; subset, split and model configuration remain explicit measurement setup. |
| Metric | id, name, definition, unit, direction | aggregation, sampling protocol, normalization | Scoring semantics; do not merge unrelated metrics merely named accuracy/F1. |
| Evaluation | id, paper_version_id, target_ref, benchmark_ref, metric_ref, setup, evidence_refs | value or structured values, unit, uncertainty, reproduction_status | One reported measurement under one configuration. Missing numeric results are NULL, not zero. |

`Artifact.artifact_kind` candidates remain `MODEL`, `AGENT_SYSTEM`, `TOOL`, `SKILL`, `SKILL_LIBRARY`, `CONTEXT_PLAYBOOK`, `PROMPT`, `CODE_IMPLEMENTATION`, `DATASET`. Do not create every transient memory note, playbook bullet or rollout as a global Artifact.

## Attribution and evidence

Every source assertion must resolve to a PaperVersion and evidence references. A locator records source URL, section/table anchor, capture hash and actual inspection scope. The current records use `full_text_selected_sections_and_tables`; this does not mean every paragraph/appendix was checked or the authors' experiments reproduced.

`claim_status=author_reported` and `result_status=source_checked_author_reported_not_reproduced` preserve the distinction between reading a reported number and establishing its scientific truth. All new records remain `status=draft_requires_review`.

For a table, preserve row configuration, column metric/category and unit. Do not silently copy treatment hyperparameters into a control whose exact settings are unspecified. Reproducibility hashes pin captures; timestamps do not certify correctness.

## Candidate edge contracts and pilot boundaries

| Source kind | Predicate | Target kind | Condition |
|---|---|---|---|
| Paper | HAS_VERSION | PaperVersion | Source/version identity known or explicitly unverified snapshot. |
| Paper | CURRENT_VERSION | PaperVersion | Confirmed numeric source revision only; metadata-only paper may have no edge. |
| PaperVersion | HAS_CONTRIBUTION | Contribution | Contribution scoped to this version. |
| Contribution | PROPOSES_METHOD | Method | Paper actually proposes the procedure. |
| Contribution | INTRODUCES_ARTIFACT | Artifact | Explicitly attributed introduction; not every baseline or illustrative example. |
| Method | USES_METHOD | Method | Used conceptual component; variant details remain source-attributed. |
| Method | ADDRESSES | ResearchProblem | Explicit methodological connection supported by the source. |
| Method | USES_ARTIFACT | Artifact | Used resource, baseline or model component under the stated context. |
| Method | PRODUCES_ARTIFACT | Artifact | Reported generated/trained output, with evidence. |
| PaperVersion | REPORTS_EVALUATION | Evaluation | Reporting version is mandatory. |
| Evaluation | EVALUATES_ARTIFACT / EVALUATES_METHOD | Artifact / Method | Target is explicit, not inferred solely from paper title. |
| Evaluation | USES_BENCHMARK | Benchmark | Setup retains split/subset and protocol. |
| Evaluation | MEASURES | Metric | Numeric unit/aggregation agree with metric semantics. |
| Artifact | DERIVED_FROM | Artifact | Only if concrete parent/revision lineage is available; none fabricated from a general description of evolution. |

There is no unconditional global `OUTPERFORMS` assertion. Comparisons are obtained from compatible source-attributed evaluations. `Finding`, `Result` and `Evidence` are not additional core node kinds in this candidate schema.

## Problems deliberately kept visible

- SkillWeaver v1 §4.1 prose cites a different average improvement percentage from the conclusion and Table 1. The extraction retains the table's raw scores and a source-conflict finding, rather than choosing a universal corrected claim.
- DGM's 50-task Polyglot and full Polyglot measurements are distinct. Its outer archive/parent-selection process is fixed and foundation weights frozen in this version; do not relabel it as unconstrained weight-level recursive improvement.
- ACE offline adaptation with labels is not interchangeable with online adaptation without labels.
- A-Mem gains in the selected multi-hop condition do not imply superiority over every baseline in every question category.
- SkillWeaver Appendix D.2.1 shows how avoiding exceptions can conceal a nonfunctional skill. Artifact verification needs an explicit criterion, scope and failure evidence.

## Remaining work before freezing or graph materialization

1. Review cross-paper Method/Problem/Artifact aliases and candidate identities; preserve paper-local descriptive labels.
2. Agree on a machine-readable extraction schema and versioned migration strategy. The current importer validates the existing minimum contract and stores extra candidate fields as JSON; it is not a complete v0.2 schema validator.
3. Review additional examples including negative results, multi-metric configurations, revision changes and unresolved identity cases.
4. Implement graph writer constraints and positive/negative query checks only after schema approval. Do not report these as implemented by this pilot.
5. Preserve the additional `relevance_reviews` annotations when performing future database migrations. The offline reconciliation tool still supports its documented ingestion tables rather than arbitrary annotation tables.
