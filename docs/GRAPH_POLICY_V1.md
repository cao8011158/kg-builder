# Graph policy: domain-v1

The immutable preparation snapshot remains the input. Approved source payloads and original paper/version links are not rewritten. Collection, scope filtering and extraction are not repeated when rebuilding a graph.

## Two different operations

Methods and benchmarks use entity resolution: within-kind HNSW candidate neighborhoods, deduplicated candidate pairs, and Jev same/different decisions at the existing confidence >=0.7 threshold. Similarity does not establish identity. Confident same links form provisional components. A pending comparison does not block the whole component.

Research problems remain distinct source-grounded ResearchProblem nodes. Their whole-snapshot similarity graph is partitioned by Leiden CPM. Its actual clusters, rather than exact-identity links, become candidate research areas. An LLM summarizes each cluster of at least two members into a ResearchArea name and definition. Singleton problems remain searchable without forcing a synthetic area. Area nodes are distinct from their original problem nodes and expose member paper provenance.

## Second review is member exclusion

For either entity components or research areas, Jev reviews individual members in the context of the group name/definition. For Method/Benchmark, membership means the same actual entity; for ResearchArea, membership means the thematic field, not identical concrete research objectives.

A member is excluded only if Jev selects `not_belongs` AND its returned confidence is strictly greater than 0.7. Exactly 0.7, lower confidence, `belongs`, and `insufficient` are retained. No extra group-wide approval gate is added. Technical errors are recorded and stop a stage for recovery; they are not fabricated semantic answers.

Exclusion removes the group relationship, not the source item. Excluded methods/benchmarks and unmatched items materialize as independent source-authored nodes. Every original research problem remains a distinct searchable node even if it has no area. Uncertain identity relations remain audited and do not hide approved content from search. Explicit contradictory identity constraints are reported for a user decision rather than silently discarded.

## Persistence and evaluation

Each job pins its graph policy, input snapshot hash and configuration. New jobs use domain-v1; prior jobs retain their legacy behavior and are not rewritten. Candidate decisions, individual membership reviews and exclusions remain inspectable. Generation can use leased external workers or bounded API workers; this measured replay uses external generation and live Jev.

All original 15 source-retrieval queries can be repeated. Method/Benchmark identity reference cases still measure entity resolution. Old research-problem identity cases must not score research-area co-membership as entity equality: they are reported separately as inapplicable under the changed semantics. Area quality and original-problem coverage are separate results. Agent-authored proxy references are not human gold labels.

The previous unexplained missing-index-marker observation is recorded separately. It is not declared fixed by these semantic policy changes. Close/reopen and downloaded restore must verify the new graph; new issues are reported and discussed before further policy changes.
